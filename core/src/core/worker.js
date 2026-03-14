const process = require('node:process');
/**
 * 子进程 Worker - 负责运行单个账号的挂机逻辑
 */
const { parentPort, workerData } = require('node:worker_threads');
const { CONFIG } = require('../config/config');
const { getLevelExpProgress } = require('../config/gameConfig');
const { getAutomation, getPreferredSeed, getConfigSnapshot, applyConfigSnapshot } = require('../models/store');
const { checkAndClaimEmails } = require('../services/email');
const { getEmailDailyState } = require('../services/email');
const { checkFarm, startFarmCheckLoop, stopFarmCheckLoop, refreshFarmCheckLoop, getLandsDetail, getAvailableSeeds, runFarmOperation, runFertilizerByConfig } = require('../services/farm');
const { checkFriends, startFriendCheckLoop, stopFriendCheckLoop, refreshFriendCheckLoop, getFriendsList, getGameFriendsByGids, getFriendLandsDetail, doFriendOperation } = require('../services/friend');
const { getInteractRecords } = require('../services/interact');
const { processInviteCodes } = require('../services/invite');
const { autoBuyOrganicFertilizer, buyFreeGifts, getFreeGiftDailyState } = require('../services/mall');
const { performDailyMonthCardGift, getMonthCardDailyState } = require('../services/monthcard');
const { performDailyOpenServerGift, getOpenServerDailyState } = require('../services/openserver');
const { performDailyVipGift, getVipDailyState } = require('../services/qqvip');
const { createScheduler, getSchedulerRegistrySnapshot } = require('../services/scheduler');
const { performDailyShare, getShareDailyState } = require('../services/share');
const { setInitialValues, resetSessionGains, recordOperation } = require('../services/stats');
const { initStatusBar, setStatusPlatform, statusData } = require('../services/status');
const { setRecordGoldExpHook } = require('../services/status');
const { cleanupTaskSystem, checkAndClaimTasks, getTaskClaimDailyState, getTaskDailyStateLikeApp, getGrowthTaskStateLikeApp } = require('../services/task');
const { sellAllFruits, getBag, getBagItems, openFertilizerGiftPacksSilently } = require('../services/warehouse');
const { connect, cleanup, getWs, getUserState, networkEvents, sendMsgAsync } = require('../utils/network');
const { loadProto, types, getRoot } = require('../utils/proto');
const { setLogHook, log, logWarn, toNum } = require('../utils/utils');
const { validateAutomation, validateIntervals, validateQuietHours } = require('../services/config-validator');

if (parentPort && workerData && workerData.accountId && !process.env.FARM_ACCOUNT_ID) {
    process.env.FARM_ACCOUNT_ID = String(workerData.accountId);
}

function sendToMaster(payload) {
    if (process.send) {
        process.send(payload);
        return;
    }
    if (parentPort) {
        parentPort.postMessage(payload);
    }
}

function onMasterMessage(handler) {
    if (process.send) {
        process.on('message', handler);
    }
    if (parentPort) {
        parentPort.on('message', handler);
    }
}

function exitWorker(code = 0) {
    if (parentPort) {
        try {
            parentPort.close();
        } catch {}
        return;
    }
    process.exit(code);
}

function pad2(n) {
    return String(n).padStart(2, '0');
}

function formatLocalDateTime24(date = new Date()) {
    const d = date instanceof Date ? date : new Date();
    const y = d.getFullYear();
    const m = pad2(d.getMonth() + 1);
    const day = pad2(d.getDate());
    const hh = pad2(d.getHours());
    const mm = pad2(d.getMinutes());
    const ss = pad2(d.getSeconds());
    return `${y}-${m}-${day} ${hh}:${mm}:${ss}`;
}

// 捕获日志发送给主进程
setLogHook((tag, msg, isWarn, meta) => {
    sendToMaster({
        type: 'log',
        data: {
            time: formatLocalDateTime24(new Date()),
            tag,
            msg,
            isWarn,
            meta: meta || {},
        }
    });
});

// 捕获金币经验变化
setRecordGoldExpHook((gold, exp) => {
    // 更新内部统计
    const { recordGoldExp } = require('../services/stats');
    recordGoldExp(gold, exp);

    // 发送给主进程
    sendToMaster({ type: 'stat_update', data: { gold, exp } });
});

let isRunning = false;
let loginReady = false;
let appliedConfigRevision = 0;
let unifiedSchedulerRunning = false;
let farmTaskRunning = false;
let friendTaskRunning = false;
let nextFarmRunAt = 0;
let nextFriendRunAt = 0;
let lastStatusHash = '';
let lastStatusSentAt = 0;
let onSellGain = null;
let onFarmHarvested = null;
let harvestSellRunning = false;
let onWsError = null;
let wsErrorHandledAt = 0;
let lastDailyRunDate = '';
let friendOpenIdsDiscoverRunning = false;
const workerScheduler = createScheduler('worker');
const INTERVAL_MAX_SEC = 86400;

function isDailyRoutineEnabled(auto) {
    const a = (auto && typeof auto === 'object') ? auto : {};
    return !!(a.email && a.free_gifts && a.share_reward && a.vip_gift && a.month_card && a.open_server_gift);
}

function getLocalDateKey() {
    const now = new Date();
    const y = now.getFullYear();
    const m = String(now.getMonth() + 1).padStart(2, '0');
    const d = String(now.getDate()).padStart(2, '0');
    return `${y}-${m}-${d}`;
}

async function runDailyRoutines(force = false) {
    if (!loginReady) return;
    const auto = getAutomation();
    try {
        if (auto.email) await checkAndClaimEmails(force);
        if (auto.share_reward) await performDailyShare(force);
        if (auto.month_card) await performDailyMonthCardGift(force);
        if (auto.open_server_gift) await performDailyOpenServerGift(force);
        if (auto.free_gifts) await buyFreeGifts(force);
        if (auto.vip_gift) await performDailyVipGift(force);
    } catch (e) {
        log('系统', `每日任务调度失败: ${e.message}`, { module: 'system', event: 'daily_routine', result: 'error' });
    }
}

function stopDailyRoutineTimer() {
    workerScheduler.clear('daily_routine_interval');
}

function startDailyRoutineTimer() {
    stopDailyRoutineTimer();
    lastDailyRunDate = getLocalDateKey();
    // 新账号登录后按当前设置强制执行一次领取
    runDailyRoutines(true).catch(() => null);
    workerScheduler.setIntervalTask('daily_routine_interval', 30 * 1000, () => {
        if (!loginReady) return;
        const today = getLocalDateKey();
        if (today === lastDailyRunDate) return;
        lastDailyRunDate = today;
        runDailyRoutines(true).catch(() => null);
    });
}

function stopFriendOpenIdsDiscoverTimer() {
    workerScheduler.clear('friend_open_ids_discover_interval');
}

async function discoverFriendOpenIdsFromVisitors() {
    if (!loginReady) return false;
    if (CONFIG.platform !== 'qq') return false;
    if (!CONFIG.friendOpenIdAutoDiscoverEnabled) return false;
    if (friendOpenIdsDiscoverRunning) return false;

    friendOpenIdsDiscoverRunning = true;
    try {
        const records = await getInteractRecords().catch(() => []);
        const list = Array.isArray(records) ? records : [];

        const state = getUserState();
        const selfGid = toNum(state && state.gid);
        const selfOpenId = String(state && state.open_id ? state.open_id : '').trim().toUpperCase();
        const isValidOpenId = (v) => /^[0-9A-F]{32}$/.test(String(v || '').trim().toUpperCase());

        const maxRecords = Math.max(1, Number(CONFIG.friendOpenIdAutoDiscoverMaxRecords || 50) || 50);
        const maxGids = Math.max(1, Number(CONFIG.friendOpenIdAutoDiscoverMaxGids || 50) || 50);
        const parseOpenIdFromUrl = (url) => {
            const text = String(url || '').trim();
            if (!text) return '';
            const match = text.match(/[0-9a-f]{32}/i);
            if (!match) return '';
            const oid = String(match[0] || '').trim().toUpperCase();
            return isValidOpenId(oid) ? oid : '';
        };
        const gids = [];
        const gidSeen = new Set();
        const openIds = [];
        const openIdSeen = new Set();

        for (const record of list.slice(0, maxRecords)) {
            const gid = toNum(record && (record.oprGid || record.opr_gid || record.visitorGid || record.visitor_gid));
            if (!gid || gid <= 0) continue;
            if (selfGid && gid === selfGid) continue;
            if (gidSeen.has(gid)) continue;
            gidSeen.add(gid);
            gids.push(gid);

            // 访客记录的 avatar_url 中通常包含对方 open_id（抓包数据示例: thirdqq.qlogo.cn/.../<OPEN_ID>/100）
            // 优先从 URL 解析，避免 GetGameFriends 不可用时无法补全。
            const url = record && (record.avatarUrl || record.avatar_url || record.opr_avatar_url || '');
            const oidFromUrl = parseOpenIdFromUrl(url);
            if (oidFromUrl && (!selfOpenId || oidFromUrl !== selfOpenId) && !openIdSeen.has(oidFromUrl)) {
                openIdSeen.add(oidFromUrl);
                openIds.push(oidFromUrl);
            }

            if (gids.length >= maxGids) break;
        }

        // 兜底：通过 gid 查询 open_id（部分环境 avatar_url 可能缺失）
        if (gids.length > 0 && openIds.length < maxGids) {
            const friends = await getGameFriendsByGids(gids).catch(() => []);
            const rawFriends = Array.isArray(friends) ? friends : [];
            for (const f of rawFriends) {
                const oid = String(f && f.open_id ? f.open_id : '').trim().toUpperCase();
                if (!isValidOpenId(oid)) continue;
                if (selfOpenId && oid === selfOpenId) continue;
                if (openIdSeen.has(oid)) continue;
                openIdSeen.add(oid);
                openIds.push(oid);
                if (openIds.length >= maxGids) break;
            }
        }

        if (openIds.length === 0) return false;

        // 仅发送“新增候选”，避免重复刷消息；主进程会做最终去重/持久化
        const snapshot = getConfigSnapshot() || {};
        const currentOpenIds = Array.isArray(snapshot.friendOpenIds) ? snapshot.friendOpenIds : [];
        const currentSet = new Set(currentOpenIds.map(v => String(v || '').trim().toUpperCase()).filter(Boolean));
        const newlyDiscovered = openIds.filter(oid => oid && !currentSet.has(oid));
        if (newlyDiscovered.length === 0) return false;

        sendToMaster({
            type: 'friend_open_ids_discovered',
            openIds: newlyDiscovered,
            source: 'interact_records.avatar_url+gid_lookup',
            at: Date.now(),
        });
        log('好友', `从访客记录补全 open_id: +${newlyDiscovered.length}`, {
            module: 'friend',
            event: 'friend_open_ids_discover',
            result: 'ok',
            candidates: newlyDiscovered.length,
        });
        return true;
    } catch (e) {
        logWarn('好友', `从访客记录补全 open_id 失败: ${e.message}`, {
            module: 'friend',
            event: 'friend_open_ids_discover',
            result: 'error',
        });
        return false;
    } finally {
        friendOpenIdsDiscoverRunning = false;
    }
}

function startFriendOpenIdsDiscoverTimer() {
    stopFriendOpenIdsDiscoverTimer();
    if (CONFIG.platform !== 'qq') return;
    if (!CONFIG.friendOpenIdAutoDiscoverEnabled) return;

    const intervalMs = Math.max(60_000, Number(CONFIG.friendOpenIdAutoDiscoverIntervalMs || (30 * 60 * 1000)) || (30 * 60 * 1000));
    workerScheduler.setIntervalTask('friend_open_ids_discover_interval', intervalMs, discoverFriendOpenIdsFromVisitors, {
        preventOverlap: true,
        runImmediately: true,
    });
}

function normalizeIntervalRangeSec(minSec, maxSec, fallbackSec) {
    const clampSec = (value, fallbackValue) => {
        const n = Number.parseInt(value, 10);
        const base = Number.isFinite(n) ? n : fallbackValue;
        return Math.max(1, Math.min(INTERVAL_MAX_SEC, base));
    };
    const fallback = clampSec(fallbackSec, 1);
    let min = clampSec(minSec, fallback);
    let max = clampSec(maxSec, fallback);
    if (min > max) [min, max] = [max, min];
    return { min, max };
}

function applyIntervalsToRuntime(intervals) {
    const data = (intervals && typeof intervals === 'object') ? intervals : {};

    const farmLegacy = Math.max(1, Number.parseInt(data.farm, 10) || 2);
    const farmRange = normalizeIntervalRangeSec(data.farmMin, data.farmMax, farmLegacy);
    CONFIG.farmCheckIntervalMin = farmRange.min * 1000;
    CONFIG.farmCheckIntervalMax = farmRange.max * 1000;
    CONFIG.farmCheckInterval = CONFIG.farmCheckIntervalMin;

    const friendLegacy = Math.max(1, Number.parseInt(data.friend, 10) || 10);
    const friendRange = normalizeIntervalRangeSec(data.friendMin, data.friendMax, friendLegacy);
    CONFIG.friendCheckIntervalMin = friendRange.min * 1000;
    CONFIG.friendCheckIntervalMax = friendRange.max * 1000;
    CONFIG.friendCheckInterval = CONFIG.friendCheckIntervalMin;
}

function randomIntervalMs(minMs, maxMs) {
    const minSec = Math.max(1, Math.floor(Math.max(1000, Number(minMs) || 1000) / 1000));
    const maxSec = Math.max(minSec, Math.floor(Math.max(1000, Number(maxMs) || minSec * 1000) / 1000));
    if (maxSec === minSec) return minSec * 1000;
    const sec = minSec + Math.floor(Math.random() * (maxSec - minSec + 1));
    return sec * 1000;
}

function resetUnifiedSchedule() {
    const farmMs = randomIntervalMs(
        CONFIG.farmCheckIntervalMin || CONFIG.farmCheckInterval || 2000,
        CONFIG.farmCheckIntervalMax || CONFIG.farmCheckInterval || 2000
    );
    const friendMs = randomIntervalMs(
        CONFIG.friendCheckIntervalMin || CONFIG.friendCheckInterval || 10000,
        CONFIG.friendCheckIntervalMax || CONFIG.friendCheckInterval || 10000
    );
    const now = Date.now();
    nextFarmRunAt = now + farmMs;
    nextFriendRunAt = now + friendMs;
}

async function runFarmTick(auto) {
    if (farmTaskRunning) return;
    farmTaskRunning = true;
    // 立即同步状态，告知前端开始巡查
    syncStatus();
    const farmMs = randomIntervalMs(
        CONFIG.farmCheckIntervalMin || CONFIG.farmCheckInterval || 2000,
        CONFIG.farmCheckIntervalMax || CONFIG.farmCheckInterval || 2000
    );
    try {
        if (auto.farm) await checkFarm();
        if (auto.task) await checkAndClaimTasks();
        if (auto.email) await checkAndClaimEmails();
        if (auto.fertilizer_gift) await openFertilizerGiftPacksSilently();
        if (auto.fertilizer_buy) await autoBuyOrganicFertilizer();
    } catch (e) {
        log('系统', `农场调度执行失败: ${e.message}`, { module: 'system', event: 'farm_tick', result: 'error' });
    } finally {
        nextFarmRunAt = Date.now() + farmMs;
        farmTaskRunning = false;
        // 完成后立即同步状态
        syncStatus();
    }
}

async function runFriendTick(auto) {
    if (friendTaskRunning) return;
    friendTaskRunning = true;
    // 立即同步状态，告知前端开始巡查
    syncStatus();
    const friendMs = randomIntervalMs(
        CONFIG.friendCheckIntervalMin || CONFIG.friendCheckInterval || 10000,
        CONFIG.friendCheckIntervalMax || CONFIG.friendCheckInterval || 10000
    );
    try {
        if (auto.friend_steal || auto.friend_help || auto.friend_bad) await checkFriends();
    } catch (e) {
        log('系统', `好友调度执行失败: ${e.message}`, { module: 'system', event: 'friend_tick', result: 'error' });
    } finally {
        nextFriendRunAt = Date.now() + friendMs;
        friendTaskRunning = false;
        // 完成后立即同步状态
        syncStatus();
    }
}

async function runUnifiedTick() {
    if (!unifiedSchedulerRunning || !loginReady) return;
    const now = Date.now();
    const dueFarm = now >= nextFarmRunAt;
    const dueFriend = now >= nextFriendRunAt;
    if (!dueFarm && !dueFriend) return;

    const auto = getAutomation();
    const tasks = [];
    if (dueFarm) tasks.push(runFarmTick(auto));
    if (dueFriend) tasks.push(runFriendTick(auto));
    await Promise.all(tasks);
}

function scheduleUnifiedNextTick() {
    if (!unifiedSchedulerRunning) return;
    workerScheduler.clear('unified_next_tick');
    if (!loginReady) return;

    const now = Date.now();
    const nextAt = Math.min(
        Number(nextFarmRunAt) || (now + 1000),
        Number(nextFriendRunAt) || (now + 1000)
    );
    const delayMs = Math.max(1000, nextAt - now); // 最低 1 秒

    workerScheduler.setTimeoutTask('unified_next_tick', delayMs, async () => {
        try {
            await runUnifiedTick();
        } finally {
            scheduleUnifiedNextTick();
        }
    });
}

function startUnifiedScheduler() {
    if (unifiedSchedulerRunning) return;
    unifiedSchedulerRunning = true;
    resetUnifiedSchedule();
    scheduleUnifiedNextTick();
}

function stopUnifiedScheduler() {
    unifiedSchedulerRunning = false;
    farmTaskRunning = false;
    friendTaskRunning = false;
    workerScheduler.clear('unified_next_tick');
}

function applyRuntimeConfig(snapshot, syncNow = false) {
    const prevAuto = getAutomation();
    const prevSnapshot = getConfigSnapshot() || {};
    const prevFriendOpenIds = Array.isArray(prevSnapshot.friendOpenIds) ? prevSnapshot.friendOpenIds : [];
    
    // 配置校验
    const validatedSnapshot = snapshot;
    if (snapshot && typeof snapshot === 'object') {
        try {
            if (snapshot.automation) {
                snapshot.automation = validateAutomation(snapshot.automation);
            }
            if (snapshot.intervals) {
                snapshot.intervals = validateIntervals(snapshot.intervals);
            }
            if (snapshot.friendQuietHours) {
                snapshot.friendQuietHours = validateQuietHours(snapshot.friendQuietHours);
            }
        } catch (e) {
            log('配置', `配置校验失败: ${e.message}`, { module: 'system', event: 'config_validate' });
        }
    }
    
    applyConfigSnapshot(validatedSnapshot || {}, { persist: false });
    const rev = Number((validatedSnapshot || {}).__revision || 0);
    if (rev > 0) appliedConfigRevision = rev;

    // 优先使用本次下发的间隔，避免 worker 内部 store 漂移导致回退默认值
    const incomingIntervals = (snapshot && snapshot.intervals && typeof snapshot.intervals === 'object')
        ? snapshot.intervals
        : null;
    if (incomingIntervals) {
        applyIntervalsToRuntime(incomingIntervals);
    }

    if (loginReady) {
        refreshFarmCheckLoop(200);
        refreshFriendCheckLoop(200);
        resetUnifiedSchedule();
        scheduleUnifiedNextTick();

        // open_id 变更后主动刷新一次好友列表（SyncAll），避免需要等待下一轮调度
        try {
            const nextSnapshot = getConfigSnapshot() || {};
            const nextFriendOpenIds = Array.isArray(nextSnapshot.friendOpenIds) ? nextSnapshot.friendOpenIds : [];
            const normalizeOpenIds = (list) => {
                const out = [];
                const seen = new Set();
                for (const item of (Array.isArray(list) ? list : [])) {
                    const v = String(item || '').trim().toUpperCase();
                    if (!v) continue;
                    if (seen.has(v)) continue;
                    seen.add(v);
                    out.push(v);
                }
                return out;
            };
            const a = normalizeOpenIds(prevFriendOpenIds);
            const b = normalizeOpenIds(nextFriendOpenIds);
            const setA = new Set(a);
            const setB = new Set(b);
            let changed = setA.size !== setB.size;
            if (!changed) {
                for (const v of setA) {
                    if (!setB.has(v)) {
                        changed = true;
                        break;
                    }
                }
            }
            if (changed) {
                workerScheduler.setTimeoutTask('friend_list_refresh_after_open_id_change', 800, () => {
                    if (!loginReady) return;
                    getFriendsList().catch(() => null);
                });
            }
        } catch {
            // ignore
        }

        // 保存设置后若“自动处理日常”开启，则立即执行一次
        const hasAutomationPayload = !!(snapshot && snapshot.automation && typeof snapshot.automation === 'object');
        if (hasAutomationPayload) {
            const nextAuto = getAutomation();
            const wasEnabled = isDailyRoutineEnabled(prevAuto);
            const nowEnabled = isDailyRoutineEnabled(nextAuto);
            if (!wasEnabled && nowEnabled) {
                // 保存设置时 /api/automation 可能触发多次 config_sync，这里做防抖且仅关->开触发
                workerScheduler.setTimeoutTask('daily_routine_immediate', 400, () => {
                    runDailyRoutines(true).catch(() => null);
                });
            }

            const prevFertilizerMode = String(prevAuto && prevAuto.fertilizer ? prevAuto.fertilizer : '').toLowerCase();
            const nextFertilizerMode = String(nextAuto && nextAuto.fertilizer ? nextAuto.fertilizer : '').toLowerCase();
            const fertilizerChanged = prevFertilizerMode !== nextFertilizerMode;
            if (fertilizerChanged && (nextFertilizerMode === 'both' || nextFertilizerMode === 'organic')) {
                // 保存设置时 /api/automation 可能连续触发多次 config_sync，这里做防抖为一次立即施肥
                workerScheduler.setTimeoutTask('fertilizer_immediate_after_save', 600, async () => {
                    if (!loginReady) return;
                    try {
                        await runFertilizerByConfig([]);
                    } catch (e) {
                        log('施肥', `保存配置后立即施肥失败: ${e.message}`, {
                            module: 'farm',
                            event: 'fertilize',
                            result: 'error',
                        });
                    }
                });
            }
        }
    }

    if (syncNow) syncStatus();
}

// 接收主进程指令
onMasterMessage(async (msg) => {
    try {
        if (msg.type === 'start') {
            await startBot(msg.config);
        } else if (msg.type === 'stop') {
            await stopBot({ reason: msg.reason });
        } else if (msg.type === 'api_call') {
            handleApiCall(msg);
        } else if (msg.type === 'config_sync') {
            applyRuntimeConfig(msg.config || {}, true);
        }
    } catch (e) {
        sendToMaster({ type: 'error', error: e.message });
    }
});

async function startBot(config) {
    if (isRunning) return;
    isRunning = true;

    const { code, platform, farmInterval, friendInterval } = config;

    CONFIG.platform = platform || 'qq';
    if (farmInterval) {
        CONFIG.farmCheckInterval = farmInterval;
        CONFIG.farmCheckIntervalMin = farmInterval;
        CONFIG.farmCheckIntervalMax = farmInterval;
    }
    if (friendInterval) {
        CONFIG.friendCheckInterval = friendInterval;
        CONFIG.friendCheckIntervalMin = friendInterval;
        CONFIG.friendCheckIntervalMax = friendInterval;
    }

    await loadProto();

    log('系统', '正在连接服务器...');

    // 加载保存的配置
    applyRuntimeConfig(getConfigSnapshot(), false);

    initStatusBar();
    setStatusPlatform(CONFIG.platform);

    if (onWsError) {
        networkEvents.off('ws_error', onWsError);
        onWsError = null;
    }
    onWsError = (payload) => {
        if ((Number(payload?.code) || 0) !== 400) return;
        const now = Date.now();
        if (now - wsErrorHandledAt < 4000) return;
        wsErrorHandledAt = now;
        log('系统', '连接被拒绝，可能需要更新 Code');
        sendToMaster({
            type: 'ws_error',
            code: 400,
            message: payload?.message || '',
        });
        if (isRunning) {
            workerScheduler.setTimeoutTask('ws_error_cleanup', 1000, () => {
                if (isRunning) cleanup();
            });
        }
    };
    networkEvents.on('ws_error', onWsError);

    networkEvents.on('kickout', onKickout);

    const onLoginSuccess = async () => {
        loginReady = true;
        if (onSellGain) {
            networkEvents.off('sell', onSellGain);
        }
        onSellGain = (deltaGold) => {
            const delta = Number(deltaGold || 0);
            if (!Number.isFinite(delta) || delta <= 0) return;
            recordOperation('sell', 1);
        };
        networkEvents.on('sell', onSellGain);

        if (onFarmHarvested) {
            networkEvents.off('farmHarvested', onFarmHarvested);
        }
        onFarmHarvested = async () => {
            if (harvestSellRunning) return;
            if (!getAutomation().sell) return;
            harvestSellRunning = true;
            try {
                await sellAllFruits();
            } catch (e) {
                log('仓库', `收获后自动出售失败: ${e.message}`, { module: 'warehouse', event: 'sell_after_harvest', result: 'error' });
            } finally {
                harvestSellRunning = false;
            }
        };
        networkEvents.on('farmHarvested', onFarmHarvested);

        // 登录后主动拉一次背包，初始化点券(ID:1002)数量
        try {
            const bagReply = await getBag();
            const items = getBagItems(bagReply);
            let coupon = 0;
            for (const it of (items || [])) {
                if (toNum(it && it.id) === 1002) {
                    coupon = toNum(it.count);
                    break;
                }
            }
            const state = getUserState();
            state.coupon = Math.max(0, coupon);
        } catch {
            // ignore
        }
        // 登录成功后，以当前金币/经验/点券作为统计基线，并清空会话增量
        const latest = getUserState();
        setInitialValues(Number(latest.gold || 0), Number(latest.exp || 0), Number(latest.coupon || 0));
        resetSessionGains();

        // 登录成功后启动各模块
        await processInviteCodes();
        if (getAutomation().fertilizer_gift) {
            await openFertilizerGiftPacksSilently().catch(() => 0);
        }
        startFarmCheckLoop({ externalScheduler: true });
        startFriendCheckLoop({ externalScheduler: true });
        startUnifiedScheduler();
        // 每日礼包/任务改为跨日调度，不在农场轮询内执行
        startDailyRoutineTimer();
        // 从访客记录补全 QQ open_id（用于 SyncAll）
        startFriendOpenIdsDiscoverTimer();

        // 立即发送一次状态
        syncStatus();
    };

    connect(code, onLoginSuccess);

    // 启动定时状态同步
    workerScheduler.setIntervalTask('status_sync', 3000, syncStatus, { preventOverlap: true });
}

async function stopBot(options = {}) {
    if (!isRunning) return exitWorker(0);
    isRunning = false;
    loginReady = false;
    const reason = (options && typeof options === 'object' && !Array.isArray(options) && options.reason !== undefined)
        ? String(options.reason || '')
        : '';
    const persistSessionOnShutdownEnv = String(process.env.TRANSPORT_PERSIST_SESSION_ON_SHUTDOWN || '').trim().toLowerCase();
    const persistSessionOnShutdown = (persistSessionOnShutdownEnv === '1'
        || persistSessionOnShutdownEnv === 'true'
        || persistSessionOnShutdownEnv === 'yes'
        || persistSessionOnShutdownEnv === 'on');
    const persistRemoteSession = reason === 'shutdown' && persistSessionOnShutdown;
    stopUnifiedScheduler();
    networkEvents.off('kickout', onKickout);
    if (onWsError) {
        networkEvents.off('ws_error', onWsError);
        onWsError = null;
    }
    if (onSellGain) {
        networkEvents.off('sell', onSellGain);
        onSellGain = null;
    }
    if (onFarmHarvested) {
        networkEvents.off('farmHarvested', onFarmHarvested);
        onFarmHarvested = null;
    }
    stopFarmCheckLoop();
    stopFriendCheckLoop();
    stopDailyRoutineTimer();
    cleanupTaskSystem();
    workerScheduler.clearAll();
    cleanup();
    const ws = getWs();
    if (ws && typeof ws.close === 'function') {
        try {
            // 本地直连: ws 为真实 WebSocket 实例；remote transport: ws 为 wrapper 对象
            if (typeof ws.send === 'function') {
                ws.close();
            } else {
                const ret = ws.close({
                    disconnectSession: !persistRemoteSession,
                    timeoutMs: persistRemoteSession ? 0 : 1500,
                });
                if (ret && typeof ret.then === 'function') {
                    await ret.catch(() => null);
                }
            }
        } catch { }
    }
    exitWorker(0);
}

function onKickout(payload) {
    const reason = payload && payload.reason ? payload.reason : '未知';
    log('系统', `检测到踢下线，准备自动停止账号。原因: ${reason}`);
    sendToMaster({ type: 'account_kicked', reason });
    workerScheduler.setTimeoutTask('kickout_stop', 200, () => {
        stopBot().catch(() => exitWorker(0));
    });
}

function isPlainObject(value) {
    return !!value && typeof value === 'object' && !Array.isArray(value);
}

function resolveProtoType(typeRef) {
    const ref = String(typeRef || '').trim();
    if (!ref) return null;

    // Prefer named mappings (e.g. "SyncAllRequest")
    if (types && types[ref]) return types[ref];

    // Fallback to full name lookup (e.g. "gamepb.friendpb.SyncAllRequest")
    const root = (typeof getRoot === 'function') ? getRoot() : null;
    if (root && typeof root.lookupType === 'function') {
        try {
            return root.lookupType(ref);
        } catch {
            // ignore lookup failures
        }
    }
    return null;
}

function collectUnknownProtoFields(messageType, value, path = '', depth = 0) {
    if (!messageType || !messageType.fields || !isPlainObject(value)) return [];
    if (depth > 16) return [];

    const unknown = [];
    const prefix = path ? `${path}.` : '';
    const fields = messageType.fields || {};

    for (const key of Object.keys(value)) {
        const field = fields[key];
        if (!field) {
            unknown.push(`${prefix}${key}`);
            continue;
        }

        const resolved = field.resolvedType;
        if (!resolved || !resolved.fields) continue; // not a message type

        const childPath = `${prefix}${key}`;
        const v = value[key];

        if (field.repeated) {
            if (Array.isArray(v)) {
                for (let i = 0; i < v.length; i += 1) {
                    unknown.push(...collectUnknownProtoFields(resolved, v[i], `${childPath}[${i}]`, depth + 1));
                }
            }
            continue;
        }

        if (field.map) {
            if (isPlainObject(v)) {
                for (const [mk, mv] of Object.entries(v)) {
                    unknown.push(...collectUnknownProtoFields(resolved, mv, `${childPath}.${mk}`, depth + 1));
                }
            }
            continue;
        }

        unknown.push(...collectUnknownProtoFields(resolved, v, childPath, depth + 1));
    }

    return unknown;
}

async function wssDebugCall(rawPayload) {
    const payload = (rawPayload && typeof rawPayload === 'object') ? rawPayload : {};
    const serviceName = String(payload.serviceName || payload.service_name || payload.service || '').trim();
    const methodName = String(payload.methodName || payload.method_name || payload.method || '').trim();
    if (!serviceName) throw new Error('Missing serviceName');
    if (!methodName) throw new Error('Missing methodName');

    const requestTypeRef = String(payload.requestType || payload.request_type || payload.type || '').trim();
    if (!requestTypeRef) throw new Error('Missing requestType');
    const responseTypeRef = String(payload.responseType || payload.response_type || '').trim();

    const request = (payload.request !== undefined) ? payload.request
        : (payload.body !== undefined) ? payload.body
            : (payload.message !== undefined) ? payload.message
                : {};
    const requestBody = (request && typeof request === 'object') ? request : {};

    const strict = payload.strict === undefined ? true : !!payload.strict;
    const decodeResponse = payload.decodeResponse === undefined ? true : !!payload.decodeResponse;

    const timeoutMsRaw = payload.timeoutMs !== undefined ? payload.timeoutMs : payload.timeout;
    let timeoutMs = Number.parseInt(timeoutMsRaw, 10);
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) timeoutMs = 10000;
    timeoutMs = Math.max(500, Math.min(30000, timeoutMs));

    const reqType = resolveProtoType(requestTypeRef);
    if (!reqType) throw new Error(`Unknown requestType: ${requestTypeRef}`);

    if (strict) {
        const unknownFields = collectUnknownProtoFields(reqType, requestBody);
        if (unknownFields.length > 0) {
            throw new Error(`Unknown proto fields: ${unknownFields.join(', ')}`);
        }
    }

    const reqMessage = reqType.fromObject ? reqType.fromObject(requestBody) : reqType.create(requestBody);
    const reqBytes = reqType.encode(reqMessage).finish();

    const { body: replyBody, meta } = await sendMsgAsync(serviceName, methodName, reqBytes, timeoutMs);

    const ret = {
        service_name: serviceName,
        method_name: methodName,
        request_type: reqType.fullName || requestTypeRef,
        response_type: responseTypeRef || '',
        request_hex: Buffer.from(reqBytes).toString('hex'),
        response_hex: Buffer.from(replyBody || []).toString('hex'),
        meta: null,
        response: null,
        response_decode_error: null,
    };

    try {
        if (types && types.GateMeta && meta) {
            ret.meta = types.GateMeta.toObject(meta, { longs: String, enums: String, bytes: String, defaults: true });
        } else {
            ret.meta = meta || null;
        }
    } catch {
        ret.meta = meta || null;
    }

    if (decodeResponse && responseTypeRef) {
        const repType = resolveProtoType(responseTypeRef);
        if (!repType) {
            ret.response_decode_error = `Unknown responseType: ${responseTypeRef}`;
            return ret;
        }
        try {
            const repMessage = repType.decode(replyBody || Buffer.alloc(0));
            ret.response = repType.toObject(repMessage, { longs: String, enums: String, bytes: String, defaults: true });
        } catch (e) {
            ret.response_decode_error = e && e.message ? e.message : String(e || 'decode error');
        }
    }

    return ret;
}

// 处理来自 Admin 面板的直接调用请求 (如: 购买种子、开关设置等)
async function handleApiCall(msg) {
    const { id, method, args } = msg;
    let result = null;
    let error = null;

    try {
        switch (method) {
            case 'getLands':
                result = await getLandsDetail();
                break;
            case 'getFriends':
                result = await getFriendsList();
                break;
            case 'getInteractRecords':
                result = await getInteractRecords();
                break;
            case 'getFriendBlacklist':
                result = require('../models/store').getFriendBlacklist();
                break;
            case 'getFriendLands':
                result = await getFriendLandsDetail(args[0]);
                break;
            case 'doFriendOp':
                result = await doFriendOperation(args[0], args[1]);
                break;
            case 'getSeeds':
                result = await getAvailableSeeds();
                break;
            case 'getBag':
                result = await require('../services/warehouse').getBagDetail();
                break;
            case 'getBagSeeds':
                result = await require('../services/warehouse').getBagSeeds();
                break;
            case 'setAutomation': {
                const payload = args && args[0] ? args[0] : {};
                applyRuntimeConfig({ automation: { [payload.key]: payload.value } }, true);
                result = getAutomation();
                break;
            }
            case 'doFarmOp':
                result = await runFarmOperation(args[0], { automated: false }); // opType
                break;
            case 'getAnalytics': {
                const { getPlantRankings } = require('../services/analytics');
                result = getPlantRankings(args[0]); // sortBy
                break;
            }
            case 'getDailyGiftOverview':
                result = await getDailyGiftOverview();
                break;
            case 'getSchedulers':
                result = getSchedulerRegistrySnapshot();
                break;
            case 'wssDebug':
            case 'wss_debug':
                result = await wssDebugCall(args && args[0] ? args[0] : {});
                break;
            default:
                error = 'Unknown method';
        }
    } catch (e) {
        error = e.message;
    }

    sendToMaster({ type: 'api_response', id, result, error });
}

async function getDailyGiftOverview() {
    const auto = getAutomation() || {};
    const task = getTaskDailyStateLikeApp
        ? await getTaskDailyStateLikeApp()
        : (getTaskClaimDailyState ? getTaskClaimDailyState() : { doneToday: false, lastClaimAt: 0 });
    const growthTask = getGrowthTaskStateLikeApp
        ? await getGrowthTaskStateLikeApp()
        : { doneToday: false, completedCount: 0, totalCount: 0, tasks: [] };
    const email = getEmailDailyState ? getEmailDailyState() : { doneToday: false, lastCheckAt: 0 };
    const free = getFreeGiftDailyState ? getFreeGiftDailyState() : { doneToday: false, lastClaimAt: 0 };
    const share = getShareDailyState ? getShareDailyState() : { doneToday: false, lastClaimAt: 0 };
    const vip = getVipDailyState ? getVipDailyState() : { doneToday: false, lastClaimAt: 0 };
    const month = getMonthCardDailyState ? getMonthCardDailyState() : { doneToday: false, lastClaimAt: 0 };
    const openServer = getOpenServerDailyState ? getOpenServerDailyState() : { doneToday: false, lastClaimAt: 0, lastCheckAt: 0 };

    return {
        date: new Date().toISOString().slice(0, 10),
        growth: {
            key: 'growth_task',
            label: '成长任务',
            doneToday: !!growthTask.doneToday,
            completedCount: Number(growthTask.completedCount || 0),
            totalCount: Number(growthTask.totalCount || 0),
            tasks: Array.isArray(growthTask.tasks) ? growthTask.tasks : [],
        },
        gifts: [
            {
                key: 'task_claim',
                label: '每日任务',
                enabled: !!auto.task,
                doneToday: !!task.doneToday,
                lastAt: Number(task.lastClaimAt || 0),
                completedCount: Number(task.completedCount || 0),
                totalCount: Number(task.totalCount || 3),
            },
            { key: 'email_rewards', label: '邮箱奖励', enabled: !!auto.email, doneToday: !!email.doneToday, lastAt: Number(email.lastCheckAt || 0) },
            { key: 'mall_free_gifts', label: '商城免费礼包', enabled: !!auto.free_gifts, doneToday: !!free.doneToday, lastAt: Number(free.lastClaimAt || 0) },
            { key: 'daily_share', label: '分享礼包', enabled: !!auto.share_reward, doneToday: !!share.doneToday, lastAt: Number(share.lastClaimAt || 0) },
            {
                key: 'vip_daily_gift',
                label: '会员礼包',
                enabled: !!auto.vip_gift,
                doneToday: !!vip.doneToday,
                lastAt: Number(vip.lastClaimAt || vip.lastCheckAt || 0),
                hasGift: Object.prototype.hasOwnProperty.call(vip, 'hasGift') ? !!vip.hasGift : undefined,
                canClaim: Object.prototype.hasOwnProperty.call(vip, 'canClaim') ? !!vip.canClaim : undefined,
                result: vip.result || '',
            },
            {
                key: 'month_card_gift',
                label: '月卡礼包',
                enabled: !!auto.month_card,
                doneToday: !!month.doneToday,
                lastAt: Number(month.lastClaimAt || month.lastCheckAt || 0),
                hasCard: Object.prototype.hasOwnProperty.call(month, 'hasCard') ? !!month.hasCard : undefined,
                hasClaimable: Object.prototype.hasOwnProperty.call(month, 'hasClaimable') ? !!month.hasClaimable : undefined,
                result: month.result || '',
            },
            {
                key: 'open_server_gift',
                label: '开服红包',
                enabled: !!auto.open_server_gift,
                doneToday: !!openServer.doneToday,
                lastAt: Number(openServer.lastClaimAt || openServer.lastCheckAt || 0),
                hasClaimable: Object.prototype.hasOwnProperty.call(openServer, 'hasClaimable') ? !!openServer.hasClaimable : undefined,
                result: openServer.result || '',
            },
        ],
    };
}

function syncStatus() {
    if (!process.send && !parentPort) return;

    const userState = getUserState();
    const ws = getWs();
    const connected = !!(loginReady && ws && ws.readyState === 1);

    let expProgress = null;
    const level = (userState.level ?? statusData.level ?? 0);
    const exp = (userState.exp ?? statusData.exp ?? 0);

    if (level > 0 && exp >= 0) {
        expProgress = getLevelExpProgress(level, exp);
    }

    const limits = require('../services/friend').getOperationLimits();
    const fullStats = require('../services/stats').getStats(statusData, userState, connected, limits);
    const nowMs = Date.now();
    const farmRemainSec = Math.max(0, Math.ceil((Number(nextFarmRunAt || 0) - nowMs) / 1000));
    const friendRemainSec = Math.max(0, Math.ceil((Number(nextFriendRunAt || 0) - nowMs) / 1000));
    // 等待巡查：倒计时为0且不在巡查中
    const farmWaiting = farmRemainSec <= 0 && !farmTaskRunning;
    const friendWaiting = friendRemainSec <= 0 && !friendTaskRunning;
    fullStats.nextChecks = {
        farmRemainSec,
        friendRemainSec,
        farmInspecting: farmTaskRunning,
        friendInspecting: friendTaskRunning,
        farmWaiting,
        friendWaiting,
    };

    fullStats.automation = getAutomation();
    fullStats.preferredSeed = getPreferredSeed();
    fullStats.levelProgress = expProgress;
    fullStats.configRevision = appliedConfigRevision;
    const hash = JSON.stringify(fullStats);
    const now = Date.now();
    if (hash !== lastStatusHash || now - lastStatusSentAt > 8000) {
        lastStatusHash = hash;
        lastStatusSentAt = now;
        sendToMaster({ type: 'status_sync', data: fullStats });
    }
}
