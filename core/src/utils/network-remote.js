const { Buffer } = require('node:buffer');
const EventEmitter = require('node:events');
const process = require('node:process');
/**
 * Remote Transport 网络层 - 通过独立 Transport 容器收发 WSS 数据
 *
 * 说明：
 * - Bot Worker 仍负责 proto 编解码、登录/心跳、推送处理与业务逻辑
 * - Transport 仅负责 WSS 连接、GateMessage 封装、body 加密、请求-响应匹配、推送转发
 */

const WebSocket = require('ws');
const { CONFIG } = require('../config/config');
const { createScheduler } = require('../services/scheduler');
const { updateStatusFromLogin, updateStatusGold, updateStatusLevel } = require('../services/status');
const { recordOperation } = require('../services/stats');
const { types } = require('./proto');
const { toLong, toNum, syncServerTime, log, logWarn } = require('./utils');

// ============ 事件发射器 (用于推送通知) ============
const networkEvents = new EventEmitter();

// ============ 内部状态 ============
let transportWs = null;
let transportConnecting = null;
let transportReqId = 1;
const pendingTransport = new Map();

let gameReadyState = WebSocket.CLOSED;
let wsErrorState = { code: 0, at: 0, message: '' };

const networkScheduler = createScheduler('network_remote');

// ============ 用户状态 (登录后设置) ============
const userState = {
    gid: 0,
    name: '',
    level: 0,
    gold: 0,
    exp: 0,
    coupon: 0, // 点券(ID:1002)
    open_id: '',
    qq_friend_recommend_authorized: 0,
};

function getUserState() { return userState; }
function getWsErrorState() { return { ...wsErrorState }; }
function setWsErrorState(code, message) {
    wsErrorState = { code: Number(code) || 0, at: Date.now(), message: message || '' };
}
function clearWsErrorState() {
    wsErrorState = { code: 0, at: 0, message: '' };
}

function getAccountId() {
    const id = String(process.env.FARM_ACCOUNT_ID || '').trim();
    return id || 'default';
}

function getTransportRemoteUrl() {
    const env = String(process.env.TRANSPORT_REMOTE_URL || '').trim();
    if (env) return env;
    const cfg = CONFIG && CONFIG.transportRemoteUrl ? String(CONFIG.transportRemoteUrl).trim() : '';
    return cfg;
}

function getTransportSecret() {
    const env = String(process.env.TRANSPORT_SECRET || '').trim();
    if (env) return env;
    const cfg = CONFIG && CONFIG.transportSecret ? String(CONFIG.transportSecret).trim() : '';
    return cfg;
}

function normalizeRemoteWsUrl(baseUrl, accountId) {
    const raw = String(baseUrl || '').trim();
    if (!raw) throw new Error('TRANSPORT_REMOTE_URL is empty');
    const withProto = /^wss?:\/\//i.test(raw) ? raw : `ws://${raw}`;
    const url = new URL(withProto);
    if (!url.pathname || url.pathname === '/') url.pathname = '/ws';
    url.searchParams.set('accountId', String(accountId || 'default'));
    return url.toString();
}

function rejectAllPendingRequests(reason = '请求被中断') {
    const entries = Array.from(pendingTransport.entries());
    pendingTransport.clear();
    for (const [, item] of entries) {
        try {
            clearTimeout(item.timer);
            item.reject(new Error(reason));
        } catch {
            // ignore
        }
    }
    return entries.length;
}

function cleanup(reason = '网络清理') {
    rejectAllPendingRequests(`请求已中断: ${reason}`);
    networkScheduler.clearAll();
}

function sendTransportRequest(payload, timeoutMs = 10000) {
    if (!transportWs || transportWs.readyState !== WebSocket.OPEN) {
        return Promise.reject(new Error('transport 连接未打开'));
    }

    const id = transportReqId;
    transportReqId += 1;
    const request = { id, ...(payload || {}) };

    const effectiveTimeout = Math.max(200, Math.min(30000, Number(timeoutMs) || 10000));
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
            pendingTransport.delete(id);
            reject(new Error(`transport 请求超时 (id=${id})`));
        }, effectiveTimeout);

        pendingTransport.set(id, { resolve, reject, timer });

        try {
            transportWs.send(JSON.stringify(request));
        } catch (err) {
            clearTimeout(timer);
            pendingTransport.delete(id);
            reject(err);
        }
    });
}

function scheduleAutoReconnect() {
    if (!savedLoginCallback) return;
    networkScheduler.clear('auto_reconnect');
    networkScheduler.setTimeoutTask('auto_reconnect', 5000, () => {
        if (!savedLoginCallback) return;
        log('系统', '[Transport] 尝试自动重连...');
        reconnect(null);
    });
}

function handleTransportMessage(data) {
    const text = Buffer.isBuffer(data) ? data.toString('utf8') : String(data || '');
    let msg = null;
    try { msg = JSON.parse(text); } catch { msg = null; }
    if (!msg || typeof msg !== 'object') return;

    // response
    if (msg.id) {
        const pending = pendingTransport.get(msg.id);
        if (!pending) return;
        pendingTransport.delete(msg.id);
        clearTimeout(pending.timer);
        if (msg.ok) pending.resolve(msg.result);
        else pending.reject(new Error(String(msg.error || 'error')));
        return;
    }

    // push: game notify
    if (msg.type === 'notify' && msg.bodyHex) {
        try {
            const body = Buffer.from(String(msg.bodyHex), 'hex');
            handleNotify(body);
        } catch (e) {
            logWarn('推送', `notify 处理失败: ${e.message}`);
        }
        return;
    }

    // push: game ws state
    if (msg.type === 'ws_state') {
        const state = String(msg.state || '').toLowerCase();
        if (state === 'open') {
            gameReadyState = WebSocket.OPEN;
        } else if (state === 'close') {
            gameReadyState = WebSocket.CLOSED;
            cleanup('连接关闭');
            scheduleAutoReconnect();
        }
        return;
    }

    // push: ws error
    if (msg.type === 'ws_error') {
        const code = Number(msg.code || 0) || 0;
        const message = String(msg.message || '');
        if (code) setWsErrorState(code, message);
        networkEvents.emit('ws_error', { code, message });
    }
}

function ensureTransportConnected() {
    if (transportWs && transportWs.readyState === WebSocket.OPEN) return Promise.resolve();
    if (transportConnecting) return transportConnecting;

    const accountId = getAccountId();
    const remoteBase = getTransportRemoteUrl();
    const url = normalizeRemoteWsUrl(remoteBase, accountId);
    const secret = getTransportSecret();

    transportConnecting = new Promise((resolve, reject) => {
        const ws = new WebSocket(url, {
            headers: secret ? { 'x-transport-secret': secret } : {},
        });
        transportWs = ws;

        ws.on('open', () => {
            transportConnecting = null;
            resolve();
        });

        ws.on('message', (data) => {
            handleTransportMessage(data);
        });

        ws.on('close', () => {
            transportConnecting = null;
            transportWs = null;
            gameReadyState = WebSocket.CLOSED;
            rejectAllPendingRequests('transport 连接关闭');
            scheduleAutoReconnect();
        });

        ws.on('error', (err) => {
            transportConnecting = null;
            transportWs = null;
            gameReadyState = WebSocket.CLOSED;
            rejectAllPendingRequests('transport 连接错误');
            reject(err);
        });
    });

    return transportConnecting;
}

// ============ 推送处理 ============
const notifyHandlers = new Map();

// 被踢下线
notifyHandlers.set('Kickout', (eventBody) => {
    const notify = types.KickoutNotify.decode(eventBody);
    log('推送', `原因: ${notify.reason_message || '未知'}`);
    networkEvents.emit('kickout', {
        type: 'Kickout',
        reason: notify.reason_message || '未知',
    });
});

// 土地状态变化 (被放虫/放草/偷菜等)
notifyHandlers.set('LandsNotify', (eventBody) => {
    const notify = types.LandsNotify.decode(eventBody);
    const hostGid = toNum(notify.host_gid);
    const lands = notify.lands || [];
    if (lands.length > 0 && (hostGid === userState.gid || hostGid === 0)) {
        networkEvents.emit('landsChanged', lands);
    }
});

// 物品变化通知 (经验/金币等)
notifyHandlers.set('ItemNotify', (eventBody) => {
    const notify = types.ItemNotify.decode(eventBody);
    const items = notify.items || [];
    for (const itemChg of items) {
        const item = itemChg.item;
        if (!item) continue;
        const id = toNum(item.id);
        const count = toNum(item.count);
        const delta = toNum(itemChg.delta);

        // 仅使用 ID=1101 作为经验值标准
        if (id === 1101) {
            if (count > 0) userState.exp = count;
            else if (delta !== 0) userState.exp = Math.max(0, Number(userState.exp || 0) + delta);
            updateStatusLevel(userState.level, userState.exp);
        } else if (id === 1 || id === 1001) {
            if (count > 0) userState.gold = count;
            else if (delta !== 0) userState.gold = Math.max(0, Number(userState.gold || 0) + delta);
            updateStatusGold(userState.gold);
        } else if (id === 1002) {
            if (count > 0) userState.coupon = count;
            else if (delta !== 0) userState.coupon = Math.max(0, Number(userState.coupon || 0) + delta);
        }
    }
});

// 基本信息变化 (升级等)
notifyHandlers.set('BasicNotify', (eventBody) => {
    const notify = types.BasicNotify.decode(eventBody);
    if (!notify.basic) return;
    const oldLevel = userState.level;
    if (Object.prototype.hasOwnProperty.call(notify.basic, 'level')) {
        const nextLevel = toNum(notify.basic.level);
        if (Number.isFinite(nextLevel) && nextLevel > 0) userState.level = nextLevel;
    }
    let shouldUpdateGoldView = false;
    if (Object.prototype.hasOwnProperty.call(notify.basic, 'gold')) {
        const nextGold = toNum(notify.basic.gold);
        if (Number.isFinite(nextGold) && nextGold >= 0) {
            userState.gold = nextGold;
            shouldUpdateGoldView = true;
        }
    }
    if (Object.prototype.hasOwnProperty.call(notify.basic, 'exp')) {
        const exp = toNum(notify.basic.exp);
        if (Number.isFinite(exp) && exp >= 0) {
            userState.exp = exp;
            updateStatusLevel(userState.level, exp);
        }
    }
    if (shouldUpdateGoldView) {
        updateStatusGold(userState.gold);
    }
    if (userState.level !== oldLevel) {
        recordOperation('levelUp', 1);
    }
});

// 好友申请通知 (微信同玩)
notifyHandlers.set('FriendApplicationReceivedNotify', (eventBody) => {
    const notify = types.FriendApplicationReceivedNotify.decode(eventBody);
    const applications = notify.applications || [];
    if (applications.length > 0) {
        networkEvents.emit('friendApplicationReceived', applications);
    }
});

// 好友添加成功通知
notifyHandlers.set('FriendAddedNotify', (eventBody) => {
    const notify = types.FriendAddedNotify.decode(eventBody);
    const friends = notify.friends || [];
    if (friends.length > 0) {
        const names = friends.map(f => f.name || f.remark || `GID:${toNum(f.gid)}`).join(', ');
        log('好友', `新好友: ${names}`);
    }
});

// 商品解锁通知 (升级后解锁新种子等)
notifyHandlers.set('GoodsUnlockNotify', (eventBody) => {
    const notify = types.GoodsUnlockNotify.decode(eventBody);
    const goods = notify.goods_list || [];
    if (goods.length > 0) {
        networkEvents.emit('goodsUnlockNotify', goods);
    }
});

// 任务状态变化通知
notifyHandlers.set('TaskInfoNotify', (eventBody) => {
    const notify = types.TaskInfoNotify.decode(eventBody);
    if (notify.task_info) {
        networkEvents.emit('taskInfoNotify', notify.task_info);
    }
});

function handleNotify(eventMessageBytes) {
    if (!eventMessageBytes || eventMessageBytes.length === 0) return;
    try {
        const event = types.EventMessage.decode(eventMessageBytes);
        const type = event.message_type || '';
        const eventBody = event.body;

        for (const [key, handler] of notifyHandlers) {
            if (type.includes(key)) {
                try { handler(eventBody); } catch { }
                return;
            }
        }
    } catch (e) {
        logWarn('推送', `解码失败: ${e.message}`);
    }
}

// ============ 登录/心跳 ============
let savedLoginCallback = null;
let savedCode = null;
let lastHeartbeatResponse = Date.now();
let heartbeatMissCount = 0;

async function sendLogin(onLoginSuccess) {
    const body = types.LoginRequest.encode(types.LoginRequest.create({
        sharer_id: toLong(0),
        sharer_open_id: '',
        device_info: {
            client_version: CONFIG.clientVersion,
            sys_software: CONFIG.device_info.sys_software,
            network: CONFIG.device_info.network,
            cpu: CONFIG.device_info.cpu,
            memory: CONFIG.device_info.memory,
            device_id: CONFIG.device_info.device_id,
        },
        share_cfg_id: toLong(0),
        scene_id: CONFIG.loginSceneId,
        report_data: {
            callback: '', cd_extend_info: '', click_id: '', clue_token: '',
            minigame_channel: 'other-qq', minigame_platid: 1, req_id: '', trackid: '',
        },
    })).finish();

    try {
        const { body: replyBody } = await sendMsgAsync('gamepb.userpb.UserService', 'Login', body, 12000);
        const reply = types.LoginReply.decode(replyBody);
        if (reply.basic) {
            clearWsErrorState();
            userState.gid = toNum(reply.basic.gid);
            userState.name = reply.basic.name || '未知';
            userState.level = toNum(reply.basic.level);
            userState.gold = toNum(reply.basic.gold);
            userState.exp = toNum(reply.basic.exp);
            userState.open_id = reply.basic.open_id || '';
            userState.qq_friend_recommend_authorized = toNum(reply.qq_friend_recommend_authorized);

            updateStatusFromLogin({
                name: userState.name,
                level: userState.level,
                gold: userState.gold,
                exp: userState.exp,
            });

            log('系统', `登录成功: ${userState.name} (Lv${userState.level})`);

            console.warn('');
            console.warn('========== 登录成功 ==========');
            console.warn(`  GID:    ${userState.gid}`);
            console.warn(`  昵称:   ${userState.name}`);
            console.warn(`  等级:   ${userState.level}`);
            console.warn(`  金币:   ${userState.gold}`);
            if (reply.time_now_millis) {
                syncServerTime(toNum(reply.time_now_millis));
                console.warn(`  时间:   ${new Date(toNum(reply.time_now_millis)).toLocaleString()}`);
            }
            console.warn('===============================');
            console.warn('');
        }

        startHeartbeat();
        if (onLoginSuccess) onLoginSuccess();
    } catch (err) {
        log('登录', `失败: ${err.message}`);
        if (err.message && err.message.includes('code=')) {
            log('系统', '账号验证失败，即将停止运行...');
            networkScheduler.setTimeoutTask('login_error_exit', 1000, () => process.exit(0));
        }
    }
}

function startHeartbeat() {
    networkScheduler.clear('heartbeat_interval');
    lastHeartbeatResponse = Date.now();
    heartbeatMissCount = 0;

    networkScheduler.setIntervalTask('heartbeat_interval', CONFIG.heartbeatInterval, () => {
        if (!userState.gid) return;

        const timeSinceLastResponse = Date.now() - lastHeartbeatResponse;
        if (timeSinceLastResponse > 60000) {
            heartbeatMissCount++;
            logWarn('心跳', `连接可能已断开 (${Math.round(timeSinceLastResponse / 1000)}s 无响应, pending=${pendingTransport.size})`);
            if (heartbeatMissCount >= 2) {
                log('心跳', '尝试重连...');
                rejectAllPendingRequests('连接超时，已清理');
            }
        }

        const body = types.HeartbeatRequest.encode(types.HeartbeatRequest.create({
            gid: toLong(userState.gid),
            client_version: CONFIG.clientVersion,
        })).finish();
        sendMsgAsync('gamepb.userpb.UserService', 'Heartbeat', body, 7000).then(({ body: replyBody }) => {
            if (!replyBody) return;
            lastHeartbeatResponse = Date.now();
            heartbeatMissCount = 0;
            try {
                const reply = types.HeartbeatReply.decode(replyBody);
                if (reply.server_time) syncServerTime(toNum(reply.server_time));
            } catch { }
        }).catch(() => { });
    });
}

// ============ 对外 API ============
function connect(code, onLoginSuccess) {
    savedLoginCallback = onLoginSuccess;
    if (code) savedCode = code;

    const gameUrl = `${CONFIG.serverUrl}?platform=${CONFIG.platform}&os=${CONFIG.os}&ver=${CONFIG.clientVersion}&code=${savedCode}&openID=`;

    ensureTransportConnected().then(() => {
        return sendTransportRequest({ type: 'session_connect', url: gameUrl }, 15000);
    }).then(() => {
        gameReadyState = WebSocket.OPEN;
        sendLogin(onLoginSuccess);
    }).catch((err) => {
        logWarn('系统', `[Transport] 连接失败: ${err && err.message ? err.message : String(err)}`);
        gameReadyState = WebSocket.CLOSED;
        scheduleAutoReconnect();
    });
}

function reconnect(newCode) {
    cleanup('主动重连');
    if (newCode) savedCode = newCode;
    ensureTransportConnected().then(() => {
        return sendTransportRequest({ type: 'session_disconnect' }, 5000).catch(() => null);
    }).finally(() => {
        userState.gid = 0;
        connect(savedCode, savedLoginCallback);
    });
}

function getWs() {
    return {
        readyState: gameReadyState,
        close: () => {
            ensureTransportConnected()
                .then(() => sendTransportRequest({ type: 'session_disconnect' }, 5000))
                .catch(() => { })
                .finally(() => { gameReadyState = WebSocket.CLOSED; });
        },
    };
}

function sendMsg(serviceName, methodName, bodyBytes, callback) {
    if (gameReadyState !== WebSocket.OPEN) {
        if (callback) callback(new Error('连接未打开'));
        return false;
    }
    sendMsgAsync(serviceName, methodName, bodyBytes).then(({ body, meta }) => {
        if (callback) callback(null, body, meta);
    }).catch((err) => {
        if (callback) callback(err);
    });
    return true;
}

function sendMsgAsync(serviceName, methodName, bodyBytes, timeout = 10000) {
    const bytes = bodyBytes || Buffer.alloc(0);
    const bodyHex = Buffer.from(bytes).toString('hex');
    const payload = {
        type: 'rpc',
        serviceName,
        methodName,
        bodyHex,
        timeoutMs: timeout,
    };
    return ensureTransportConnected().then(() => {
        const rpcTimeout = Math.max(500, Math.min(30000, Number(timeout) || 10000));
        // 给 transport 留一点额外时间返回
        return sendTransportRequest(payload, rpcTimeout + 1000);
    }).then((result) => {
        const hex = result && result.bodyHex ? String(result.bodyHex) : '';
        const metaObj = result && result.meta ? result.meta : null;
        let meta = metaObj;
        try {
            if (metaObj && types && types.GateMeta && typeof types.GateMeta.fromObject === 'function') {
                meta = types.GateMeta.fromObject(metaObj);
            }
        } catch {
            meta = metaObj;
        }
        return {
            body: hex ? Buffer.from(hex, 'hex') : Buffer.alloc(0),
            meta,
        };
    });
}

module.exports = {
    connect, reconnect, cleanup,
    getWs,
    sendMsg, sendMsgAsync,
    getUserState,
    getWsErrorState,
    networkEvents,
};
