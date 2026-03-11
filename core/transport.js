/**
 * Transport 服务 - 仅负责 WSS 连接/加密/收发 (供 Bot 容器远程调用)
 */

const process = require('node:process');

const { startTransportServer } = require('./src/transport/server');
const { createModuleLogger } = require('./src/services/logger');

const logger = createModuleLogger('transport_main');

startTransportServer().catch((err) => {
    logger.error('transport bootstrap failed', { error: err && err.message ? err.message : String(err) });
    process.exit(1);
});

