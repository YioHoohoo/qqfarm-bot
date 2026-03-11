/**
 * WebSocket 网络层入口
 * - 默认：本地直连游戏服务器 (network-local)
 * - 可选：通过 Transport 容器转发 (network-remote)
 */

const process = require('node:process');

function shouldUseRemoteTransport() {
    const mode = String(process.env.TRANSPORT_MODE || '').trim().toLowerCase();
    if (mode === 'local') return false;
    if (mode === 'remote') return true;
    const url = String(process.env.TRANSPORT_REMOTE_URL || '').trim();
    return !!url;
}

module.exports = shouldUseRemoteTransport()
    ? require('./network-remote')
    : require('./network-local');
