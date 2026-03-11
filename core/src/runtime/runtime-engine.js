const { fork } = require('node:child_process')
const path = require('node:path')
const process = require('node:process');
const { Worker } = require('node:worker_threads')
const { CONFIG } = require('../config/config')
const store = require('../models/store')
const { sendPushooMessage } = require('../services/push')
const { MiniProgramLoginSession } = require('../services/qrlogin')
const { createDataProvider } = require('./data-provider')
const { createReloginReminderService } = require('./relogin-reminder')
const { createRuntimeState } = require('./runtime-state')
const { createWorkerManager } = require('./worker-manager')

const OPERATION_KEYS = ['harvest', 'water', 'weed', 'bug', 'fertilize', 'plant', 'steal', 'helpWater', 'helpWeed', 'helpBug', 'taskClaim', 'sell', 'upgrade']

function createRuntimeEngine(options = {}) {
  const processRef = options.processRef || process
  const mainEntryPath = options.mainEntryPath || path.join(__dirname, '../../client.js')
  const workerScriptPath = options.workerScriptPath || path.join(__dirname, '../core/worker.js')
  const runtimeMode = String(options.runtimeMode || processRef.env.FARM_RUNTIME_MODE || 'thread').toLowerCase()
  const onStatusSync = typeof options.onStatusSync === 'function' ? options.onStatusSync : null
  const onLog = typeof options.onLog === 'function' ? options.onLog : null
  const onAccountLog = typeof options.onAccountLog === 'function' ? options.onAccountLog : null
  const startAdminServer = typeof options.startAdminServer === 'function' ? options.startAdminServer : null

  const workerControls = { startWorker: null, restartWorker: null }
  const runtimeState = createRuntimeState({
    store,
    operationKeys: OPERATION_KEYS,
  })
  const {
    workers,
    globalLogs: GLOBAL_LOGS,
    accountLogs: ACCOUNT_LOGS,
    runtimeEvents,
    nextConfigRevision,
    buildConfigSnapshotForAccount,
    log,
    addAccountLog,
    normalizeStatusForPanel,
    buildDefaultStatus,
    filterLogs,
  } = runtimeState

  const reloginReminder = createReloginReminderService({
    store,
    miniProgramLoginSession: MiniProgramLoginSession,
    sendPushooMessage,
    log,
    addAccountLog,
    getAccounts: store.getAccounts,
    addOrUpdateAccount: store.addOrUpdateAccount,
    resolveWorkerControls: () => workerControls,
  })

  const {
    getOfflineAutoDeleteMs,
    triggerOfflineReminder,
  } = reloginReminder

  const { startWorker, stopWorker, restartWorker, callWorkerApi } = createWorkerManager({
    fork,
    WorkerThread: Worker,
    runtimeMode,
    processRef,
    mainEntryPath,
    workerScriptPath,
    workers,
    globalLogs: GLOBAL_LOGS,
    log,
    addAccountLog,
    normalizeStatusForPanel,
    buildConfigSnapshotForAccount,
    getOfflineAutoDeleteMs,
    triggerOfflineReminder,
    addOrUpdateAccount: store.addOrUpdateAccount,
    deleteAccount: store.deleteAccount,
    upsertFriendBlacklist: (accountId, gid) => {
      const id = String(accountId || '').trim()
      const friendGid = Number(gid)
      if (!id || !Number.isFinite(friendGid) || friendGid <= 0) return false
      const current = store.getFriendBlacklist ? store.getFriendBlacklist(id) : []
      const list = Array.isArray(current) ? current : []
      if (list.includes(friendGid)) return false
      if (store.setFriendBlacklist) {
        store.setFriendBlacklist(id, [...list, friendGid])
        return true
      }
      return false
    },
    mergeFriendOpenIdsDiscovered: (accountId, payload) => {
      const id = String(accountId || '').trim()
      if (!id) return { ok: false, addedOpenIds: [], openIdsChanged: false }

      const body = (payload && typeof payload === 'object') ? payload : {}
      const inputOpenIds = body.openIds || body.open_ids || body.openIdList || body.open_id_list || []
      const src = Array.isArray(inputOpenIds) ? inputOpenIds : []
      const addedOpenIds = []
      const candidates = []
      const seen = new Set()
      for (const item of src) {
        const v = String(item || '').trim().toUpperCase()
        if (!/^[0-9A-F]{32}$/.test(v)) continue
        if (seen.has(v)) continue
        seen.add(v)
        candidates.push(v)
      }
      if (candidates.length === 0) return { ok: true, addedOpenIds, openIdsChanged: false }

      const currentOpenIds = store.getFriendOpenIds ? store.getFriendOpenIds(id) : []
      const current = Array.isArray(currentOpenIds) ? currentOpenIds : []
      const set = new Set(current)

      for (const openId of candidates) {
        if (set.has(openId)) continue
        set.add(openId)
        addedOpenIds.push(openId)
      }

      if (addedOpenIds.length === 0) {
        return { ok: true, addedOpenIds, openIdsChanged: false }
      }

      const nextOpenIds = [...current, ...addedOpenIds]
      store.applyConfigSnapshot({ friendOpenIds: nextOpenIds }, { accountId: id })
      return { ok: true, addedOpenIds, openIdsChanged: true }
    },
    syncFriendOpenIdsProbe: (accountId, payload) => {
      const id = String(accountId || '').trim()
      if (!id) return { ok: false, removedOpenIds: [], openIdsChanged: false, missChanged: false }

      const normalizeOpenIdList = (input) => {
        const src = Array.isArray(input) ? input : []
        const seen = new Set()
        const out = []
        for (const item of src) {
          const v = String(item || '').trim().toUpperCase()
          if (!/^[0-9A-F]{32}$/.test(v)) continue
          if (seen.has(v)) continue
          seen.add(v)
          out.push(v)
        }
        return out
      }

      const shallowEqualNumberMap = (a, b) => {
        const aa = (a && typeof a === 'object') ? a : {}
        const bb = (b && typeof b === 'object') ? b : {}
        const aKeys = Object.keys(aa)
        const bKeys = Object.keys(bb)
        if (aKeys.length !== bKeys.length) return false
        for (const k of aKeys) {
          if (!Object.prototype.hasOwnProperty.call(bb, k)) return false
          if (Number(aa[k]) !== Number(bb[k])) return false
        }
        return true
      }

      const body = (payload && typeof payload === 'object') ? payload : {}
      const requestedOpenIds = normalizeOpenIdList(body.requestedOpenIds)
      const missingOpenIds = normalizeOpenIdList(body.missingOpenIds)
      if (requestedOpenIds.length === 0) return { ok: true, removedOpenIds: [], openIdsChanged: false, missChanged: false }

      const currentOpenIds = store.getFriendOpenIds ? store.getFriendOpenIds(id) : []
      const currentSet = new Set(Array.isArray(currentOpenIds) ? currentOpenIds : [])
      if (currentSet.size === 0) return { ok: true, removedOpenIds: [], openIdsChanged: false, missChanged: false }

      const effectiveRequested = requestedOpenIds.filter(v => currentSet.has(v))
      if (effectiveRequested.length === 0) return { ok: true, removedOpenIds: [], openIdsChanged: false, missChanged: false }

      const missingSet = new Set(missingOpenIds.filter(v => currentSet.has(v)))

      const prevMisses = store.getFriendOpenIdMisses ? store.getFriendOpenIdMisses(id) : {}
      const nextMisses = {}
      for (const [k, v] of Object.entries(prevMisses || {})) {
        if (!currentSet.has(k)) continue
        const n = Number.parseInt(String(v), 10)
        if (!Number.isFinite(n) || n <= 0) continue
        nextMisses[k] = Math.min(999, n)
      }

      for (const openId of effectiveRequested) {
        if (missingSet.has(openId)) {
          nextMisses[openId] = Math.min(999, (nextMisses[openId] || 0) + 1)
        }
        else {
          delete nextMisses[openId] // reset consecutive misses
        }
      }

      const removedOpenIds = []
      for (const [openId, count] of Object.entries(nextMisses)) {
        if (!currentSet.has(openId)) continue
        if (Number(count) >= Number(CONFIG.friendOpenIdMissPruneThreshold || 3)) removedOpenIds.push(openId)
      }

      const removedSet = new Set(removedOpenIds)
      const nextOpenIds = removedOpenIds.length > 0
        ? (Array.isArray(currentOpenIds) ? currentOpenIds.filter(v => !removedSet.has(v)) : [])
        : (Array.isArray(currentOpenIds) ? currentOpenIds : [])
      for (const openId of removedSet) delete nextMisses[openId]

      const openIdsChanged = nextOpenIds.length !== (Array.isArray(currentOpenIds) ? currentOpenIds.length : 0)
      const missChanged = !shallowEqualNumberMap(prevMisses, nextMisses)

      if (openIdsChanged || missChanged) {
        const snapshot = openIdsChanged
          ? { friendOpenIds: nextOpenIds, friendOpenIdMisses: nextMisses }
          : { friendOpenIdMisses: nextMisses }
        store.applyConfigSnapshot(snapshot, { accountId: id })
      }

      return { ok: true, removedOpenIds, openIdsChanged, missChanged }
    },
    broadcastConfigToWorkers,
    onStatusSync: (accountId, status, accountName) => {
      runtimeEvents.emit('status', { accountId, status, accountName })
      if (onStatusSync) onStatusSync(accountId, status, accountName)
    },
    onWorkerLog: (entry, accountId, accountName) => {
      runtimeEvents.emit('worker_log', { entry, accountId, accountName })
      if (onLog) onLog(entry, accountId, accountName)
    },
  })
  workerControls.startWorker = startWorker
  workerControls.restartWorker = restartWorker

  const dataProvider = createDataProvider({
    workers,
    globalLogs: GLOBAL_LOGS,
    accountLogs: ACCOUNT_LOGS,
    store,
    getAccounts: store.getAccounts,
    callWorkerApi,
    buildDefaultStatus,
    normalizeStatusForPanel,
    filterLogs,
    addAccountLog,
    nextConfigRevision,
    broadcastConfigToWorkers,
    startWorker,
    stopWorker,
    restartWorker,
  })

  runtimeEvents.on('log', (entry) => {
    if (onLog) onLog(entry, entry && entry.accountId ? entry.accountId : '', entry && entry.accountName ? entry.accountName : '')
  })
  runtimeEvents.on('account_log', (entry) => {
    if (onAccountLog) onAccountLog(entry)
  })

  function broadcastConfigToWorkers(targetAccountId = '') {
    const targetId = String(targetAccountId || '').trim()
    for (const [accId, worker] of Object.entries(workers)) {
      if (targetId && String(accId) !== targetId) continue
      const snapshot = buildConfigSnapshotForAccount(accId)
      try {
        worker.process.send({ type: 'config_sync', config: snapshot })
      }
      catch {
        // ignore IPC failures for exited workers
      }
    }
  }

  function startAllAccounts() {
    const accounts = (store.getAccounts().accounts || [])
    if (accounts.length > 0) {
      log('系统', `发现 ${accounts.length} 个账号，正在启动...`)
      accounts.forEach(acc => startWorker(acc))
    }
    else {
      log('系统', '未发现账号，请访问管理面板添加账号')
    }
  }

  async function start(options = {}) {
    const shouldStartAdminServer = options.startAdminServer !== false
    const shouldAutoStartAccounts = options.autoStartAccounts !== false

    if (shouldStartAdminServer && startAdminServer) {
      startAdminServer(dataProvider)
    }

    if (shouldAutoStartAccounts) {
      startAllAccounts()
    }
  }

  function stopAllAccounts() {
    for (const accountId of Object.keys(workers)) {
      stopWorker(accountId, { reason: 'shutdown' })
    }
  }

  return {
    store,
    runtimeEvents,
    workers,
    dataProvider,
    start,
    startAllAccounts,
    stopAllAccounts,
    broadcastConfigToWorkers,
    startWorker,
    stopWorker,
    restartWorker,
    callWorkerApi,
    log,
    addAccountLog,
  }
}

module.exports = {
  createRuntimeEngine,
}
