const http = require('node:http');
const { Buffer } = require('node:buffer');
const { URL } = require('node:url');

const WebSocket = require('ws');
const { WebSocketServer } = require('ws');

const { CONFIG } = require('../config/config');
const { createModuleLogger } = require('../services/logger');
const { loadProto, types } = require('../utils/proto');
const { toLong, toNum } = require('../utils/utils');
const cryptoWasm = require('../utils/crypto-wasm');

const logger = createModuleLogger('transport');

function isPlainObject(value) {
    return !!value && typeof value === 'object' && !Array.isArray(value);
}

function safeJsonParse(text) {
    try {
        return JSON.parse(text);
    } catch {
        return null;
    }
}

function normalizeAccountId(input) {
    const id = String(input || '').trim();
    return id || 'default';
}

function normalizeHex(input) {
    const hex = String(input || '').trim();
    if (!hex) return '';
    if (!/^[0-9a-f]+$/i.test(hex)) return '';
    if (hex.length % 2 !== 0) return '';
    return hex.toLowerCase();
}

function resolveTransportSecret() {
    const env = String(process.env.TRANSPORT_SECRET || '').trim();
    if (env) return env;
    const fromCfg = CONFIG && CONFIG.transportSecret ? String(CONFIG.transportSecret).trim() : '';
    return fromCfg;
}

function resolveListenPort() {
    const env = Number.parseInt(String(process.env.TRANSPORT_PORT || ''), 10);
    if (Number.isFinite(env) && env > 0) return env;
    const cfg = Number.parseInt(String(CONFIG.transportListenPort || ''), 10);
    if (Number.isFinite(cfg) && cfg > 0) return cfg;
    return 3100;
}

function toMetaPlain(meta) {
    if (!meta) return null;
    if (types && types.GateMeta) {
        try {
            return types.GateMeta.toObject(meta, { longs: String, enums: String, bytes: String, defaults: true });
        } catch {
            // fallthrough
        }
    }
    // protobufjs message is also a plain object-ish
    return meta;
}

class TransportSession {
    constructor(accountId) {
        this.accountId = accountId;
        this.ws = null;
        this.clientSeq = 1;
        this.serverSeq = 0;
        this.pending = new Map();
        this.connecting = null;
        this.lastUrl = '';
        this.clients = new Set();
        this.wsErrorState = { code: 0, at: 0, message: '' };
        this.userGid = 0;
        this.heartbeatTimer = null;
        this.heartbeatInFlight = false;
    }

    get readyState() {
        if (!this.ws) return WebSocket.CLOSED;
        return this.ws.readyState;
    }

    setWsError(code, message) {
        this.wsErrorState = { code: Number(code) || 0, at: Date.now(), message: String(message || '') };
    }

    clearWsError() {
        this.wsErrorState = { code: 0, at: 0, message: '' };
    }

    subscribe(client) {
        this.clients.add(client);
    }

    unsubscribe(client) {
        this.clients.delete(client);
    }

    startHeartbeat() {
        if (this.heartbeatTimer) return;
        const rawInterval = Number(CONFIG.heartbeatInterval || 25000);
        const intervalMs = Math.max(5000, Math.min(60000, Number.isFinite(rawInterval) ? rawInterval : 25000));

        this.heartbeatTimer = setInterval(() => {
            // 仅在没有 bot client 在线时，由 transport 兜底心跳，避免重复心跳
            if (this.clients.size > 0) return;
            if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
            if (!this.userGid) return;
            if (this.heartbeatInFlight) return;
            if (!types || !types.HeartbeatRequest) return;

            this.heartbeatInFlight = true;
            const body = types.HeartbeatRequest.encode(types.HeartbeatRequest.create({
                gid: toLong(this.userGid),
                client_version: CONFIG.clientVersion,
            })).finish();

            this.rpc('gamepb.userpb.UserService', 'Heartbeat', body, 7000)
                .catch((e) => {
                    logger.warn('heartbeat failed', { accountId: this.accountId, error: e && e.message ? e.message : String(e) });
                })
                .finally(() => {
                    this.heartbeatInFlight = false;
                });
        }, intervalMs);

        if (this.heartbeatTimer && typeof this.heartbeatTimer.unref === 'function') {
            this.heartbeatTimer.unref();
        }
    }

    stopHeartbeat() {
        if (this.heartbeatTimer) {
            try { clearInterval(this.heartbeatTimer); } catch { }
        }
        this.heartbeatTimer = null;
        this.heartbeatInFlight = false;
    }

    broadcast(payload) {
        const text = JSON.stringify(payload);
        for (const client of Array.from(this.clients)) {
            if (!client || client.readyState !== WebSocket.OPEN) {
                this.clients.delete(client);
                continue;
            }
            try {
                client.send(text);
            } catch {
                // ignore
            }
        }
    }

    rejectAllPending(reason = '请求被中断') {
        const entries = Array.from(this.pending.entries());
        this.pending.clear();
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

    async connect(url) {
        const nextUrl = String(url || '').trim();
        if (!nextUrl) throw new Error('Missing url');

        // 已连接且 URL 相同
        if (this.ws && this.ws.readyState === WebSocket.OPEN && this.lastUrl === nextUrl) {
            return { readyState: this.ws.readyState };
        }

        // 已在连接中复用 promise
        if (this.connecting) return this.connecting;

        // 若已存在连接但 URL 不同，先断开
        if (this.ws && this.ws.readyState === WebSocket.OPEN && this.lastUrl !== nextUrl) {
            this.disconnect('url_changed');
        }

        this.lastUrl = nextUrl;
        this.connecting = new Promise((resolve, reject) => {
            let settled = false;
            const ws = new WebSocket(nextUrl, {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF WindowsWechat(0x63090a13)',
                    'Origin': 'https://gate-obt.nqf.qq.com',
                },
            });
            this.ws = ws;
            ws.binaryType = 'arraybuffer';

            const finalize = () => {
                this.connecting = null;
            };

            ws.on('open', () => {
                if (settled) return;
                settled = true;
                this.clearWsError();
                this.broadcast({ type: 'ws_state', state: 'open', readyState: WebSocket.OPEN });
                finalize();
                resolve({ readyState: WebSocket.OPEN });
            });

            ws.on('message', (data) => {
                this.handleGameMessage(data);
            });

            ws.on('close', (code) => {
                this.broadcast({ type: 'ws_state', state: 'close', readyState: WebSocket.CLOSED, code: Number(code || 0) });
                this.rejectAllPending(`连接关闭(code=${code})`);
                try {
                    ws.removeAllListeners();
                } catch { }
                if (this.ws === ws) this.ws = null;
                this.stopHeartbeat();
                this.userGid = 0;
                if (!settled) {
                    settled = true;
                    finalize();
                    reject(new Error(`连接关闭(code=${Number(code || 0)})`));
                }
            });

            ws.on('error', (err) => {
                const message = err && err.message ? String(err.message) : '';
                const match = message.match(/Unexpected server response:\\s*(\\d+)/i);
                if (match) {
                    const code = Number.parseInt(match[1], 10) || 0;
                    if (code) {
                        this.setWsError(code, message);
                        this.broadcast({ type: 'ws_error', code, message });
                    }
                }
                // connect phase error
                if (!settled && this.connecting) {
                    settled = true;
                    finalize();
                    reject(new Error(message || 'ws_error'));
                }
            });
        });

        return this.connecting;
    }

    disconnect(reason = 'disconnect') {
        this.rejectAllPending(`请求已中断: ${reason}`);
        this.stopHeartbeat();
        const ws = this.ws;
        this.ws = null;
        this.connecting = null;
        this.clientSeq = 1;
        this.serverSeq = 0;
        this.userGid = 0;
        try {
            if (ws) {
                ws.removeAllListeners();
                ws.close();
            }
        } catch { }
        this.broadcast({ type: 'ws_state', state: 'close', readyState: WebSocket.CLOSED, reason });
    }

    async encodeGateMessage(serviceName, methodName, bodyBytes, clientSeqValue) {
        let finalBody = bodyBytes || Buffer.alloc(0);
        try {
            finalBody = await cryptoWasm.encryptBuffer(finalBody);
        } catch (e) {
            logger.warn('encrypt failed, sending plain body', { accountId: this.accountId, error: e && e.message ? e.message : String(e) });
        }

        const msg = types.GateMessage.create({
            meta: {
                service_name: serviceName,
                method_name: methodName,
                message_type: 1,
                client_seq: toLong(clientSeqValue),
                server_seq: toLong(this.serverSeq),
            },
            body: finalBody,
        });
        return types.GateMessage.encode(msg).finish();
    }

    async rpc(serviceName, methodName, bodyBytes, timeoutMs = 10000) {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
            throw new Error('连接未打开');
        }
        const seq = this.clientSeq;
        this.clientSeq += 1;

        const encoded = await this.encodeGateMessage(serviceName, methodName, bodyBytes, seq);

        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                this.pending.delete(seq);
                reject(new Error(`请求超时: ${methodName} (seq=${seq}, pending=${this.pending.size})`));
            }, Math.max(200, Math.min(30000, Number(timeoutMs) || 10000)));

            this.pending.set(seq, { resolve, reject, timer });

            try {
                this.ws.send(encoded);
            } catch (err) {
                clearTimeout(timer);
                this.pending.delete(seq);
                reject(err);
            }
        });
    }

    handleGameMessage(data) {
        try {
            const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
            const msg = types.GateMessage.decode(buf);
            const meta = msg.meta;
            if (!meta) return;

            if (meta.server_seq) {
                const seq = toNum(meta.server_seq);
                if (seq > this.serverSeq) this.serverSeq = seq;
            }

            const msgType = meta.message_type;
            if (msgType === 3) {
                // notify: msg.body is EventMessage
                this.broadcast({
                    type: 'notify',
                    meta: toMetaPlain(meta),
                    bodyHex: Buffer.from(msg.body || []).toString('hex'),
                });
                return;
            }

            if (msgType === 2) {
                const errorCode = toNum(meta.error_code);
                const clientSeqVal = toNum(meta.client_seq);

                // 登录成功后缓存 gid，用于在 bot 容器断开时由 transport 兜底心跳
                if (errorCode === 0
                    && String(meta.service_name || '') === 'gamepb.userpb.UserService'
                    && String(meta.method_name || '') === 'Login'
                    && types
                    && types.LoginReply
                    && msg.body
                    && msg.body.length > 0) {
                    try {
                        const reply = types.LoginReply.decode(msg.body);
                        const gid = toNum(reply && reply.basic && reply.basic.gid);
                        if (gid > 0) {
                            this.userGid = gid;
                            this.startHeartbeat();
                        }
                    } catch (e) {
                        logger.warn('decode login reply failed', { accountId: this.accountId, error: e && e.message ? e.message : String(e) });
                    }
                }

                const cb = this.pending.get(clientSeqVal);
                if (cb) {
                    this.pending.delete(clientSeqVal);
                    clearTimeout(cb.timer);
                    if (errorCode !== 0) {
                        cb.reject(new Error(`${meta.service_name}.${meta.method_name} 错误: code=${errorCode} ${meta.error_message || ''}`));
                    } else {
                        cb.resolve({ body: msg.body, meta });
                    }
                }
            }
        } catch (err) {
            logger.warn('gate decode failed', { accountId: this.accountId, error: err && err.message ? err.message : String(err) });
        }
    }
}

async function startTransportServer() {
    await loadProto();

    const port = resolveListenPort();
    const secret = resolveTransportSecret();
    if (!secret) {
        logger.warn('TRANSPORT_SECRET is empty, transport is unauthenticated (LAN only recommended)');
    }

    const sessions = new Map();
    const getSession = (accountId) => {
        const id = normalizeAccountId(accountId);
        let session = sessions.get(id);
        if (!session) {
            session = new TransportSession(id);
            sessions.set(id, session);
        }
        return session;
    };

    const server = http.createServer((req, res) => {
        const url = req && req.url ? String(req.url) : '/';
        if (url.startsWith('/health')) {
            res.writeHead(200, { 'content-type': 'application/json' });
            res.end(JSON.stringify({ ok: true }));
            return;
        }
        res.writeHead(404, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ ok: false, error: 'not_found' }));
    });

    const wss = new WebSocketServer({ server, path: '/ws' });

    wss.on('connection', (client, req) => {
        const rawSecret = String(req.headers['x-transport-secret'] || '').trim();
        if (secret && rawSecret !== secret) {
            try { client.close(1008, 'unauthorized'); } catch { }
            return;
        }

        let accountId = 'default';
        try {
            const base = `http://localhost${req.url || ''}`;
            const parsed = new URL(base);
            accountId = normalizeAccountId(parsed.searchParams.get('accountId'));
        } catch {
            accountId = 'default';
        }

        const session = getSession(accountId);
        session.subscribe(client);
        logger.info('client connected', { accountId });

        client.on('close', () => {
            session.unsubscribe(client);
            logger.info('client disconnected', { accountId });
        });

        client.on('message', async (data) => {
            const text = Buffer.isBuffer(data) ? data.toString('utf8') : String(data || '');
            const msg = safeJsonParse(text);
            if (!isPlainObject(msg)) return;

            const id = Number.isFinite(Number(msg.id)) ? Number(msg.id) : 0;
            const type = String(msg.type || '').trim();

            const reply = (ok, result, error) => {
                if (!id) return;
                const payload = { id, ok: !!ok };
                if (ok) payload.result = result === undefined ? null : result;
                else payload.error = String(error || 'error');
                try { client.send(JSON.stringify(payload)); } catch { }
            };

            try {
                if (type === 'session_connect') {
                    const url = String(msg.url || '').trim();
                    const info = await session.connect(url);
                    reply(true, { ...info, wsErrorState: session.wsErrorState }, null);
                    return;
                }

                if (type === 'session_disconnect') {
                    session.disconnect('requested');
                    reply(true, { readyState: session.readyState }, null);
                    return;
                }

                if (type === 'get_state') {
                    reply(true, { readyState: session.readyState, wsErrorState: session.wsErrorState }, null);
                    return;
                }

                if (type === 'rpc') {
                    const serviceName = String(msg.serviceName || msg.service_name || msg.service || '').trim();
                    const methodName = String(msg.methodName || msg.method_name || msg.method || '').trim();
                    if (!serviceName) throw new Error('Missing serviceName');
                    if (!methodName) throw new Error('Missing methodName');
                    const bodyHex = normalizeHex(msg.bodyHex || msg.body_hex || msg.body || '');
                    const timeoutMs = Number(msg.timeoutMs || msg.timeout || 10000) || 10000;

                    const bodyBytes = bodyHex ? Buffer.from(bodyHex, 'hex') : Buffer.alloc(0);
                    const { body, meta } = await session.rpc(serviceName, methodName, bodyBytes, timeoutMs);
                    reply(true, {
                        bodyHex: Buffer.from(body || []).toString('hex'),
                        meta: toMetaPlain(meta),
                    }, null);
                    return;
                }

                // Unknown request type
                reply(false, null, `Unknown request type: ${type || '(empty)'}`);
            } catch (e) {
                reply(false, null, e && e.message ? e.message : String(e || 'error'));
            }
        });
    });

    await new Promise((resolve, reject) => {
        const onError = (err) => {
            server.off('listening', onListening);
            reject(err);
        };
        const onListening = () => {
            server.off('error', onError);
            resolve();
        };
        server.once('error', onError);
        server.once('listening', onListening);
        server.listen(port, '0.0.0.0');
    });
    logger.info('transport listening', { port });

    return {
        port,
        close: () => {
            try { wss.close(); } catch { }
            try { server.close(); } catch { }
            for (const session of sessions.values()) {
                try { session.disconnect('server_close'); } catch { }
            }
        },
    };
}

module.exports = { startTransportServer };
