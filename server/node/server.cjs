const express = require('express');
const app = express();
const http = require('http');
const https = require('https');
const path = require('path');
const compression = require('compression');
const htmlparser = require('node-html-parser');
const fsSync = require('fs');
const {
    existsSync,
    mkdirSync,
    readFileSync,
    renameSync,
    writeFileSync,
    readdirSync,
    unlinkSync,
    openSync,
    closeSync,
    fsyncSync,
    createReadStream,
    createWriteStream,
} = fsSync;
const fs = require('fs/promises')
const nodeCrypto = require('crypto')
const { createSessionLock } = require('./runtime/session-lock.cjs')
const zlib = require('zlib')
const v8 = require('v8')
const rateLimit = require('express-rate-limit')
const WRITER_EPOCH_HEADER = 'x-writer-epoch'
const sessionLock = createSessionLock()

function getSessionLockEpoch() {
    return sessionLock.epoch();
}

const Vips = require('wasm-vips')
let _vipsPromise = null
const getVips = () => {
    if (!_vipsPromise) {
        _vipsPromise = Vips().catch(err => {
            _vipsPromise = null
            throw err
        })
    }
    return _vipsPromise
}
const { kvGet, kvGetAsync, kvWriteToFile, kvSet, kvSetFromFile, kvDel, kvList,
        kvDelPrefix, kvListWithSizes, kvListSelectedWithSizes, kvSize, kvGetUpdatedAt, kvGetDatabaseRevision, kvGetPluginStoragePublicationRevision, kvCopyValue, clearEntities, checkpointWal,
        kvClearDeletion, kvRecordDeletion, kvListModifiedSince, kvGetDeletedSince, kvCleanupOldDeletions,
        kvGetListEpoch, kvBumpListEpoch,
        gcChunks, reclaimableChunkBytes, isDbBlobChunked, snapshotFootprints, createKvSnapshot,
        kvGetSnapshotSourceToken,
        withPluginStorageQuotaPlan,
        isLegacyHexMigrationComplete, markLegacyHexMigrationComplete,
        publishLegacyHexMigrationMarker,
        db: sqliteDb } = require('./db/db.cjs');
const { CHUNK_MARKER } = require('./db/chunkStore.cjs');
const { buildListResponse } = require('./db/listDelta.cjs');
const {
    assetDir,
    migrationMarkerPath: assetMigrationMarker,
    legacyHashIdentityMarkerPath,
    createAssetStore,
    ensureAssetDir,
    isSafeAssetName,
    portableAssetNameKey,
    isPortableAssetName,
    runtimeAssetFileDisposition,
    runtimeAssetFileDispositions,
    withAssetFileMutationAdmission,
    assetPathFor,
    isLegacyHashAsset,
    markLegacyHashAsset,
    clearLegacyHashAsset,
    reconcileLegacyHashAssetIdentity,
    writeAssetFile,
    writeAssetFileIfChanged,
    writeAssetFileFromFile,
    readAssetFile,
    assetFileMtimeMs,
    deleteAssetFile,
    listAssetFiles,
    sumAssetFsBytes,
    swapAssetDirectoryFromStaging,
    swapDirectoryFromStaging,
    migrateAssetRowsToFilesystem,
    verifyAssetHash,
} = require('./assets/assetStore.cjs');
const {
    collectReferencedAssetKeys,
    createAssetGcCandidateStore,
    planAssetGc,
} = require('./assets/assetGc.cjs');
const {
    writeImportJournal,
    readImportJournal,
    clearImportJournal,
    fsyncDirectoryTree,
    recoverImportSwap,
} = require('./backup/importJournal.cjs');
const { createImportBarrier } = require('./backup/importBarrier.cjs');
const {
    acquireAssetMaintenanceLockSync,
    isAssetMaintenanceLockedError,
    releaseAssetMaintenanceLockHandle,
    sameAssetDirectoryIdentitySync,
} = require('./assets/assetMaintenanceLock.cjs');
const {
    logger, installProcessHandlers, expressErrorMiddleware,
} = require('./runtime/logs.cjs');
const {
    registerStorageCapacityRoute,
    registerStorageListSizesRoute,
    registerLogRoutes,
} = require('./runtime/observability.cjs');
const {
    registerAssetCleanupRoute,
    registerMaintenanceRoutes,
} = require('./db/maintenanceRoutes.cjs');
const {
    registerSelfUpdateRoutes,
} = require('./runtime/selfUpdate.cjs');
const {
    registerProxyRoutes,
    checkProxyAuth,
    setupProxyStreamWebSocket,
    startProxyStreamJobGc,
} = require('./runtime/proxy.cjs');
const { createRequestLogs } = require('./runtime/request-logs.cjs');
const { createRequestTracer, isRequestTracingEnabled } = require('./runtime/request-trace.cjs');
const { applyPatchAtomic } = require('./db/atomicJsonPatch.cjs');
const { createGenerationMemo } = require('./db/generationMemo.cjs');
const { createRevisionBoundCache } = require('./db/revisionBoundCache.cjs');
const { createPluginStorageManifestCache } = require('./plugin-storage/pluginStorageManifestCache.cjs');
const {
    registerPluginStorageClearRoute,
    registerPluginStorageStateRoutes,
    registerPluginStorageManagementRoutes,
    registerPluginStorageBatchRoute,
    registerPluginStorageMutationRoute,
    registerPluginStorageTransitionRoutes,
} = require('./plugin-storage/pluginStorageRoutes.cjs');
const {
    registerBackupExportRoutes,
    registerBackupImportRoutes,
    registerServerBackupRoutes,
    registerSaveFolderMigrationRoutes,
    registerSnapshotRestoreRoute,
    registerBackupConfigurationRoutes,
    startPartialExportJobGc,
} = require('./backup/backupRoutes.cjs');
const {
    DbCachePersistenceGuardError,
    commitPreparedDbCachePersistence,
    findStubFlagLossChats,
    persistDbCacheGenerationSync,
    prepareDbCachePersistence,
    runEmergencyDbFlush,
} = require('./db/dbCachePersistence.cjs');
const {
    decodeRisuSave,
    decodeAuthoritativeRisuSave,
    encodeRisuSaveLegacy,
    calculateHash,
    normalizeJSON,
    hasRemoteBlocks,
    magicHeader,
    magicRisuSaveHeader,
    parseCachedHashesHeader,
    sha256Hex,
} = require('./utils.cjs');
const {
    computeBufferEtag,
    parseDbCacheInventory,
    prepareDatabaseReadPayload,
    encodeCachedDbReadEnvelope,
    createDatabaseSegmentMemo,
} = require('./db/dbCachedRead.cjs');
const {
    createChatRowStore,
    chatRowKey,
    parseChatRowKey,
    hasChatPayloads,
    isCanonicalRawChatRow,
    findDuplicateChaIds,
    findDuplicateChatIds,
    validateDatabaseShape,
} = require('./chat/chatRows.cjs');
const {
    CHARACTER_DEFAULTS_MARKER_KEY,
    CHARACTER_DEFAULTS_MARKER_VALUE,
    applyDatabaseCharacterDefaults,
} = require('./chat/characterDefaults.cjs');
const {
    CHAT_DELTA_CONTENT_TYPE,
    ChatDeltaValidationError,
} = require('./chat/chatDelta.cjs');
const { streamRisuSaveToFile } = require('./backup/streamRisuSave.cjs');
const {
    MCP_TOOL_CALL_CACHE_PREFIX,
    mcpToolCallStorageKey,
    parseMcpToolCallSnapshotKey,
    parseMcpToolCallStorageKey,
    scanMcpToolCallIdsFromFile,
} = require('./backup/mcpToolCallRecovery.cjs');
const { validateJsonSource } = require('./backup/streamJsonToMsgpack.cjs');
const {
    pluginStorageViewerDisplaySize,
    pluginStorageViewerDisplaySizeFromMetadata,
} = require('./plugin-storage/pluginStorageViewerFacets.cjs');
const {
    readBlockRisuSaveTopLevelFields,
    streamBackupRisuSaveToFile,
} = require('./backup/streamBackupRisuSave.cjs');
const {
    DECODED_SPOOL_FILE_PREFIXES,
    decodeBoundedLegacyRisuSave,
    inspectRisuSaveSource,
    readRisuSaveTopLevelFields,
    shouldStreamRisuSave,
    walkRisuSave,
} = require('./backup/streamRisuLoad.cjs');

async function readBackupRisuSaveTopLevelFields(input, requestedKeys, options = {}) {
    const inspection = await inspectRisuSaveSource(input);
    if (inspection.format === 'risusave') {
        return readBlockRisuSaveTopLevelFields(input, requestedKeys, options);
    }
    return readRisuSaveTopLevelFields(input, requestedKeys, {
        ...options,
        inspection,
    });
}

function risuSavePreparationRefusal(error) {
    if (error?.risuSavePreparationLimit === true) {
        return {
            status: error.status ?? 413,
            body: {
                error: error.message,
                code: error.code,
                limit: error.limit,
                actual: error.actual,
                retryable: false,
                commitOutcome: 'not-committed',
                commitOutcomeUnknown: false,
            },
        };
    }
    return null;
}
const {
    IMPORT_IO_PAGE_BYTES,
    SAVE_FOLDER_IMPORT_STAGE_PREFIX,
    ImportIngressError,
    finiteByteLimit,
    throwIfAborted: throwIfImportAborted,
    copyFileToSpool,
    readFileToBufferBounded,
    validateJsonFileStreaming,
} = require('./backup/importSpool.cjs');
const {
    PLUGIN_SAVE_PREFIX,
    PLUGIN_SAVE_META_PREFIX,
    PLUGIN_STORAGE_FOLDED_MARKER,
    assertArchiveSafePluginSaveStorageKey,
    PLUGIN_STORAGE_GENERATION_FIELD,
    PLUGIN_STORAGE_MANIFEST_KEY,
    createPluginStorageManifest,
    parsePluginStorageManifest,
    decodePluginSaveStorageKey,
    decodeManifestPluginSaveStorageKey,
    encodePluginSaveStorageKey,
    isHashedPluginSaveStorageKey,
    mergePluginStorageKeyMappings,
    pluginSaveStorageKeyMappingComponent,
} = require('./plugin-storage/pluginSaveKeys.cjs');
const {
    encodeBackupEntryHeader,
} = require('./backup/backupEntryFormat.cjs');
const {
    PluginStorageValidationError,
    PLUGIN_STORAGE_LOSSLESS_CODEC,
    PLUGIN_STORAGE_LOSSLESS_MAGIC,
    assertPluginStorageRow,
    createPluginStorageOwnerScanner,
    decodeValidatedPluginStorageKey,
    encodeValidatedPluginStorageKey,
    isPluginStorageValidationError,
    parsePluginStorageJsonBuffer,
    pluginStorageCodecForBuffer,
    serializeLosslessPluginStorageRow,
    serializePluginStorageRow,
    snapshotPluginStorageRecord,
    validatePluginStorageRow,
} = require('./plugin-storage/pluginStorageJson.cjs');
const {
    PLUGIN_VALUE_MAX_BYTES,
    PLUGIN_STORAGE_MAX_BYTES,
    PluginStorageLimitError,
} = require('./plugin-storage/pluginStorageLimits.cjs');
const {
    BUFFERED_INGRESS_POLICY,
    createBufferedIngressLimits,
    createInFlightByteBudget,
    createRoutePolicyResolver,
    createBufferedIngressMiddleware,
    isStreamedIngress,
    sendClientUpgradeRequired,
} = require('./chat/bufferedIngress.cjs');
const {
    ADMITTED_INGRESS_SPOOL,
    ADMITTED_INGRESS_SPOOL_PREFIX,
    ADMITTED_WRITE_STAGE_PREFIX,
    createAdmittedIngressSpoolMiddleware,
    disposeAdmittedIngressSpool,
    isAdmittedSpoolPressureError,
    sendRetryableSpoolRefusal,
} = require('./chat/admittedIngressSpool.cjs');
const { prepareFileChunkPlan } = require('./db/chunkPlan.cjs');
const { readClientBuildStamp } = require('./runtime/buildStamp.cjs');
const {
    SPOOL_OWNER_ID_FILENAME,
    readOrCreatePersistentUuid,
    resolveOwnedSpoolDir,
    claimOwnedSpoolNamespaceSync,
    ensureOwnedSpoolDirSync,
    openPinnedOwnedSpoolDirSync,
    withQuarantinedOwnedSpoolDirSync,
} = require('./backup/spoolOwnership.cjs');
const {
    CHAT_BACKUP_DIRNAME,
    createChatBackupStore,
    migrateLegacyChatBackups,
    resolveChatBackupDir,
    resolveChatBackupMaxBytes,
    resolveChatBackupMaxUncompressedBytes,
    isDestructiveBackupReason,
} = require('./chat/chatBackups.cjs');
const {
    RECOVERY_PATH_STARTUP_QUARANTINE_NAME,
    acquireRecoveryPathStateLockSync,
    canonicalizePathWithExistingPrefixSync,
    clearRecoveryPathStartupQuarantineSync,
    publishRecoveryPathMarkerSetSync,
    publishRecoveryPathStartupQuarantineSync,
    readRecoveryPathMarkerTargetsSync,
    readRecoveryPathStartupQuarantineSync,
    recoveryPathKeepEntries,
} = require('./recoveryPathMarkers.cjs');
const { spawn } = require('child_process');
const { Transform } = require('stream');

function fatalFlushPendingDatabase() {
    try {
        return runEmergencyDbFlush({
            log: message => console.error(message),
            isImportInProgress: () => importInProgress || importBarrier.isHeld(),
            isInTransaction: () => sqliteDb.inTransaction,
            hasPendingWork: () => Boolean(
                saveTimers[DB_HEX_KEY] || dbPersistRetryPending
            ),
            peekCachedDb: () => peekDbCacheValue(DB_HEX_KEY),
            getCacheMetadata: () => dbCache.metadata(DB_HEX_KEY),
            kvGetDatabaseRevision,
            persist: ({ cachedDb, cacheMetadata }) => persistDbCacheGenerationSync({
                ...dbCachePersistenceOptions({
                    filePath: DB_HEX_KEY,
                    decodedKey: DB_BLOB_KEY,
                    generation: dbDerivedValueMemo.generation(DB_HEX_KEY),
                    cachedDb,
                    cacheMetadata,
                }),
                chatRowsToDelete: [],
            }),
        });
    } catch (error) {
        try {
            console.error(
                `[FatalFlush] failed: ${error instanceof Error ? error.message : String(error)}`,
            );
        } catch {}
        return { status: 'failed', error };
    }
}

// Install process-level error handlers before any other init so early crashes get logged.
installProcessHandlers({ onFatalExit: fatalFlushPendingDatabase });
const expectedClientBuild = readClientBuildStamp({ log: logger });
if (expectedClientBuild) {
    logger.info(
        `[Build] Client build admission enabled for ${expectedClientBuild.version} (${expectedClientBuild.stamp})`,
    );
}

// Node.js version check
const [nodeMajor] = process.version.slice(1).split('.').map(Number);
if (nodeMajor < 24) {
    logger.warn(`[Server] Node.js ${process.version} is below the recommended version (v24.x). Consider upgrading for best compatibility.`);
}

// Configuration flags for patch-based sync
const enablePatchSync = true;
// Emergency escape hatch for remote plain-HTTP deployments. Only these exact
// values allow the client to boot outside a browser secure context.
const allowInsecureContext = process.env.POCKETRISU_ALLOW_INSECURE_CONTEXT === '1'
    || process.env.POCKETRISU_ALLOW_INSECURE_CONTEXT === 'true';
const HUB_HOSTING_MODE = ['true', '1'].includes(String(process.env.POCKETRISU_HUB_HOSTING ?? '').trim().toLowerCase());

function isHubHostingMode() {
    return HUB_HOSTING_MODE;
}

const ASSET_GC_DEFAULT_GRACE_MS = 7 * 24 * 60 * 60 * 1000;
const ASSET_GC_DEFAULT_START_DELAY_MS = 30 * 1000;
const ASSET_GC_DEFAULT_INTERVAL_MS = 24 * 60 * 60 * 1000;

function nonNegativeDurationEnv(name, fallback) {
    const raw = process.env[name];
    if (raw === undefined || raw === '') return fallback;
    const parsed = Number(raw);
    return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}

const ASSET_GC_GRACE_MS = nonNegativeDurationEnv(
    'POCKETRISU_ASSET_GC_GRACE_MS',
    ASSET_GC_DEFAULT_GRACE_MS,
);
const ASSET_GC_START_DELAY_MS = nonNegativeDurationEnv(
    'POCKETRISU_ASSET_GC_START_DELAY_MS',
    ASSET_GC_DEFAULT_START_DELAY_MS,
);
const ASSET_GC_INTERVAL_MS = Math.max(1_000, nonNegativeDurationEnv(
    'POCKETRISU_ASSET_GC_INTERVAL_MS',
    ASSET_GC_DEFAULT_INTERVAL_MS,
));
const ASSET_GC_AUTO_ENABLED = process.env.POCKETRISU_ASSET_GC_AUTO === '1'
    || (process.env.NODE_ENV !== 'test' && process.env.POCKETRISU_ASSET_GC_AUTO !== '0');
const assetGcCandidateStore = createAssetGcCandidateStore(sqliteDb);

const dbDerivedValueMemo = createGenerationMemo();
const dbSegmentMemo = createDatabaseSegmentMemo();
let preserveDbSegmentMemoOnCacheMutation = false;
// Atomic JSON patches preserve untouched object identities. Reuse only those
// branch hashes across the explicitly marked copy-on-write cache handoff.
let preserveDbHashMemoOnCacheMutation = false;
let dbCompositionalHashMemo = new WeakMap();
const DB_BLOB_KEY = 'database/database.bin';
const DB_HEX_KEY = Buffer.from(DB_BLOB_KEY, 'utf-8').toString('hex');
const rawBootByteLengthStatement = (() => {
    try {
        return sqliteDb.prepare('SELECT LENGTH(value) AS byte_length FROM kv WHERE key = ?');
    } catch {
        return null;
    }
})();

function readRawBootByteLengthHint() {
    try {
        const byteLength = rawBootByteLengthStatement?.get(DB_BLOB_KEY)?.byte_length;
        if (!Number.isSafeInteger(byteLength) || byteLength < 0) return null;
        if (byteLength !== CHUNK_MARKER.length || !isDbBlobChunked()) return byteLength;
        const logicalByteLength = kvSize(DB_BLOB_KEY);
        return Number.isSafeInteger(logicalByteLength) && logicalByteLength >= 0
            ? logicalByteLength
            : null;
    } catch {
        return null;
    }
}
// A successful patch needs the same canonical bytes twice: immediately for its
// ETag and later for the debounced write. Keep exactly one generation-bound
// copy, then release it on persist completion or any cache invalidation.
const DB_CANONICAL_ENCODING_MEMO_NAME = 'canonical-encoding';
const DB_CANONICAL_ENCODING_TEST_STATS_PATH = process.env.NODE_ENV === 'test'
    ? String(process.env.POCKETRISU_TEST_DB_CANONICAL_ENCODING_STATS_PATH ?? '').trim() || null
    : null;
const dbCanonicalEncodingTestStats = {
    fullEncodes: 0,
    retained: false,
    retainedGeneration: null,
    retainedRevision: null,
    releases: {},
};
const DB_CACHE_MAX_ENTRIES = 8;
const DB_CACHE_MAX_ESTIMATED_BYTES = 1024 * 1024 * 1024;
const DB_CACHE_MAX_ENTRY_ESTIMATED_BYTES = 512 * 1024 * 1024;
const DB_CACHE_HEAP_PRESSURE_RATIO = 0.80;
const DB_CACHE_HEAP_LIMIT = v8.getHeapStatistics().heap_size_limit;

// In-memory database cache for patch-based sync. Entries store the STRIPPED
// (stubs-only) view and are reusable only for the exact SQLite row revision.
// Clean entries are LRU/size/heap-pressure evictable; acknowledged dirty patch
// state is pinned until persistence succeeds or an explicit invalidation wins.
const dbCache = createRevisionBoundCache({
    maxEntries: DB_CACHE_MAX_ENTRIES,
    maxEstimatedBytes: DB_CACHE_MAX_ESTIMATED_BYTES,
    maxEntryEstimatedBytes: DB_CACHE_MAX_ENTRY_ESTIMATED_BYTES,
    isUnderMemoryPressure: () => (
        process.memoryUsage().heapUsed >= DB_CACHE_HEAP_LIMIT * DB_CACHE_HEAP_PRESSURE_RATIO
    ),
    onMutation: (filePath, reason) => {
        releaseDbCacheCanonicalEncoding(filePath, `cache-${reason}`);
        if (reason === 'replace') dbDerivedValueMemo.bump(filePath);
        else dbDerivedValueMemo.deleteKey(filePath);
        if (filePath === DB_HEX_KEY && !preserveDbHashMemoOnCacheMutation) {
            dbCompositionalHashMemo = new WeakMap();
        }
        if (filePath === DB_HEX_KEY && !preserveDbSegmentMemoOnCacheMutation) {
            dbSegmentMemo.clear();
        }
    },
});
const pluginStorageManifestCache = createPluginStorageManifestCache({
    getRevision: kvGetPluginStoragePublicationRevision,
    readState: () => readPluginStorageManifestStateUncached(kvGet),
});

class DatabaseCacheRevisionConflict extends Error {
    constructor() {
        super('The authoritative database changed outside the decoded cache lifecycle');
        this.name = 'DatabaseCacheRevisionConflict';
        this.code = 'DATABASE_CACHE_REVISION_CONFLICT';
    }
}

let dbCachePruneScheduled = false;
let saveTimers = {};
let dbPersistRetryPending = false;
const pendingChatRowDeletions = new Set();
const SAVE_INTERVAL = 5000;

const chatRowStore = createChatRowStore({
    db: sqliteDb,
    kvGet,
    kvGetAsync,
    kvSet,
    kvSetFromFile,
    kvDel,
    kvList,
    kvListWithSizes,
    kvWriteToFile,
    kvSize,
    kvGetUpdatedAt,
    chatDeltaCompactMaxOperations: Number.isSafeInteger(Number(
        process.env.POCKETRISU_CHAT_DELTA_COMPACT_MAX_OPERATIONS,
    )) && Number(process.env.POCKETRISU_CHAT_DELTA_COMPACT_MAX_OPERATIONS) > 0
        ? Number(process.env.POCKETRISU_CHAT_DELTA_COMPACT_MAX_OPERATIONS)
        : 64,
    chatDeltaCompactMaxBytes: Number.isSafeInteger(Number(
        process.env.POCKETRISU_CHAT_DELTA_COMPACT_MAX_BYTES,
    )) && Number(process.env.POCKETRISU_CHAT_DELTA_COMPACT_MAX_BYTES) > 0
        ? Number(process.env.POCKETRISU_CHAT_DELTA_COMPACT_MAX_BYTES)
        : 1024 * 1024,
    chatDeltaCompactionFailpoint: process.env.NODE_ENV === 'test'
        && process.env.POCKETRISU_TEST_CHAT_DELTA_COMPACTION_FAILPOINT
        ? (stage) => {
            if (stage === process.env.POCKETRISU_TEST_CHAT_DELTA_COMPACTION_FAILPOINT) {
                throw new Error(`Injected chat delta compaction failure at ${stage}`);
            }
        }
        : null,
});

// ETag for database.bin
let dbEtag = null;

function getDbEtag() {
    return dbEtag;
}

function setDbEtag(next) {
    dbEtag = next;
}

function ensurePluginStorageTransitionDbEtag(rawDatabase) {
    const currentEtag = dbEtag ?? computeBufferEtag(rawDatabase);
    dbEtag = currentEtag;
    return currentEtag;
}

function publishPluginStorageTransitionDbState(req, targetDb, resultEtag) {
    invalidateDbCache();
    dbEtag = resultEtag;
    rememberSessionPluginStorageState(req, targetDb);
}

function computeDatabaseEtagFromObject(databaseObject) {
    return computeBufferEtag(Buffer.from(encodeRisuSaveLegacy(databaseObject)));
}

function publishDbCanonicalEncodingTestStats() {
    if (!DB_CANONICAL_ENCODING_TEST_STATS_PATH) return;
    const statsPath = path.resolve(process.cwd(), DB_CANONICAL_ENCODING_TEST_STATS_PATH);
    writeFileSync(statsPath, JSON.stringify(dbCanonicalEncodingTestStats), 'utf-8');
}

function noteDbCanonicalEncodingRetained(generation, revision) {
    if (!DB_CANONICAL_ENCODING_TEST_STATS_PATH) return;
    dbCanonicalEncodingTestStats.fullEncodes += 1;
    dbCanonicalEncodingTestStats.retained = true;
    dbCanonicalEncodingTestStats.retainedGeneration = generation;
    dbCanonicalEncodingTestStats.retainedRevision = revision;
    publishDbCanonicalEncodingTestStats();
}

function releaseDbCacheCanonicalEncoding(
    filePath,
    reason,
    expectedGeneration = undefined,
) {
    const released = dbDerivedValueMemo.deleteValue(
        filePath,
        DB_CANONICAL_ENCODING_MEMO_NAME,
        expectedGeneration,
    );
    if (!released || !DB_CANONICAL_ENCODING_TEST_STATS_PATH) return released;
    dbCanonicalEncodingTestStats.retained = false;
    dbCanonicalEncodingTestStats.retainedGeneration = null;
    dbCanonicalEncodingTestStats.retainedRevision = null;
    dbCanonicalEncodingTestStats.releases[reason]
        = (dbCanonicalEncodingTestStats.releases[reason] ?? 0) + 1;
    publishDbCanonicalEncodingTestStats();
    return true;
}

function retainDbCacheCanonicalEncoding(filePath) {
    const databaseObject = peekDbCacheValue(filePath);
    const metadata = dbCache.metadata(filePath);
    const generation = dbDerivedValueMemo.generation(filePath);
    if (!databaseObject || !metadata) {
        throw new DatabaseCacheRevisionConflict();
    }
    const retained = dbDerivedValueMemo.getOrCompute(
        filePath,
        DB_CANONICAL_ENCODING_MEMO_NAME,
        () => {
            const value = {
                bytes: Buffer.from(encodeRisuSaveLegacy(databaseObject)),
                databaseObject,
                generation,
                revision: metadata.revision,
            };
            noteDbCanonicalEncodingRetained(generation, metadata.revision);
            return value;
        },
    );
    if (retained.databaseObject !== databaseObject
        || retained.generation !== generation
        || retained.revision !== metadata.revision) {
        releaseDbCacheCanonicalEncoding(filePath, 'binding-conflict', generation);
        throw new DatabaseCacheRevisionConflict();
    }
    return retained;
}

// Keep every cache replacement/eviction behind these helpers: derived values
// are valid only for the exact mutation generation in which they were built.
function scheduleDbCachePrune() {
    if (dbCachePruneScheduled) return;
    dbCachePruneScheduled = true;
    setImmediate(() => {
        dbCachePruneScheduled = false;
        dbCache.prune();
    });
}

function getDbCacheValue(filePath) {
    const value = dbCache.get(filePath);
    if (value !== undefined) scheduleDbCachePrune();
    return value;
}

function getCurrentDatabaseCacheValue(filePath, { allowDirty = false } = {}) {
    const revision = kvGetDatabaseRevision();
    const retained = dbCache.metadata(filePath);
    if (retained?.dirty && retained.revision !== revision) {
        releaseDbCacheCanonicalEncoding(filePath, 'external-revision-conflict');
        throw new DatabaseCacheRevisionConflict();
    }
    const value = dbCache.getForRevision(filePath, revision, { allowDirty });
    if (value !== undefined) scheduleDbCachePrune();
    return value;
}

function peekDbCacheValue(filePath) {
    return dbCache.peek(filePath);
}

function replaceDbCacheValue(filePath, value, metadata = {}) {
    const preserveSegmentMemo = filePath === DB_HEX_KEY
        && metadata.preserveSegmentMemo === true;
    const preserveHashMemo = filePath === DB_HEX_KEY
        && metadata.preserveHashMemo === true;
    if (filePath === DB_HEX_KEY && !preserveSegmentMemo) dbSegmentMemo.clear();
    if (preserveSegmentMemo) dbSegmentMemo.preserveForNextRevision();
    const previousPreserve = preserveDbSegmentMemoOnCacheMutation;
    const previousHashPreserve = preserveDbHashMemoOnCacheMutation;
    preserveDbSegmentMemoOnCacheMutation = preserveSegmentMemo;
    preserveDbHashMemoOnCacheMutation = preserveHashMemo;
    try {
        dbCache.set(filePath, value, metadata);
    } finally {
        preserveDbSegmentMemoOnCacheMutation = previousPreserve;
        preserveDbHashMemoOnCacheMutation = previousHashPreserve;
    }
    scheduleDbCachePrune();
}

function markDbCacheClean(filePath, metadata = {}) {
    dbCache.markClean(filePath, metadata);
    scheduleDbCachePrune();
}

function deleteDbCacheValue(filePath) {
    dbCache.delete(filePath);
}

function invalidateDbCacheEntry(filePath) {
    deleteDbCacheValue(filePath);
    if (saveTimers[filePath]) {
        clearTimeout(saveTimers[filePath]);
        delete saveTimers[filePath];
    }
}

function getDbCacheHash(filePath) {
    return dbDerivedValueMemo.getOrCompute(
        filePath,
        'hash',
        () => calculateHash(
            peekDbCacheValue(filePath),
            filePath === DB_HEX_KEY ? dbCompositionalHashMemo : undefined,
        ).toString(16),
    );
}

function getDbCacheEtag(filePath, { retainCanonicalEncoding = false } = {}) {
    const canonicalEncoding = retainCanonicalEncoding
        ? retainDbCacheCanonicalEncoding(filePath)
        : null;
    return dbDerivedValueMemo.getOrCompute(
        filePath,
        'etag',
        () => canonicalEncoding
            ? computeBufferEtag(canonicalEncoding.bytes)
            : computeDatabaseEtagFromObject(peekDbCacheValue(filePath)),
    );
}

function seedDbCacheEtag(filePath, etag) {
    dbDerivedValueMemo.seed(filePath, 'etag', etag);
}

const STORAGE_QUEUE_DIAG_ENABLED = process.env.POCKETRISU_QUEUE_DIAG === 'true';
const STORAGE_QUEUE_DIAG_SAMPLE_LIMIT = 512;
const storageQueueDiagByLabel = STORAGE_QUEUE_DIAG_ENABLED ? new Map() : null;

function recordStorageQueueDiag(label, waitMs, holdMs) {
    let stats = storageQueueDiagByLabel.get(label);
    if (!stats) {
        stats = {
            count: 0,
            waitTotalMs: 0,
            waitMaxMs: 0,
            holdTotalMs: 0,
            holdMaxMs: 0,
            samples: [],
        };
        storageQueueDiagByLabel.set(label, stats);
    }
    stats.count++;
    stats.waitTotalMs += waitMs;
    stats.waitMaxMs = Math.max(stats.waitMaxMs, waitMs);
    stats.holdTotalMs += holdMs;
    stats.holdMaxMs = Math.max(stats.holdMaxMs, holdMs);
    const sample = { waitMs, holdMs };
    if (stats.samples.length < STORAGE_QUEUE_DIAG_SAMPLE_LIMIT) {
        stats.samples.push(sample);
    } else {
        const replacement = Math.floor(Math.random() * stats.count);
        if (replacement < STORAGE_QUEUE_DIAG_SAMPLE_LIMIT) {
            stats.samples[replacement] = sample;
        }
    }
}

function storageQueueDiagPercentile(sortedValues, percentile) {
    if (sortedValues.length === 0) return 0;
    const index = Math.min(
        sortedValues.length - 1,
        Math.max(0, Math.ceil(sortedValues.length * percentile) - 1),
    );
    return sortedValues[index];
}

function storageQueueDiagSnapshot() {
    const labels = Object.create(null);
    for (const [label, stats] of [...storageQueueDiagByLabel.entries()]
        .sort(([left], [right]) => left.localeCompare(right))) {
        const waitSample = stats.samples.map(sample => sample.waitMs).sort((a, b) => a - b);
        const holdSample = stats.samples.map(sample => sample.holdMs).sort((a, b) => a - b);
        labels[label] = {
            count: stats.count,
            sampleCount: stats.samples.length,
            waitMs: {
                total: stats.waitTotalMs,
                max: stats.waitMaxMs,
                p50: storageQueueDiagPercentile(waitSample, 0.5),
                p95: storageQueueDiagPercentile(waitSample, 0.95),
            },
            holdMs: {
                total: stats.holdTotalMs,
                max: stats.holdMaxMs,
                p50: storageQueueDiagPercentile(holdSample, 0.5),
                p95: storageQueueDiagPercentile(holdSample, 0.95),
            },
        };
    }
    return {
        enabled: true,
        sampleLimit: STORAGE_QUEUE_DIAG_SAMPLE_LIMIT,
        labels,
    };
}

function logStorageQueueDiagSummary() {
    const { labels } = storageQueueDiagSnapshot();
    for (const [label, stats] of Object.entries(labels)) {
        const wait = stats.waitMs;
        const hold = stats.holdMs;
        console.log(
            `[QueueDiag] ${label} count=${stats.count} `
            + `wait_ms(total=${wait.total.toFixed(3)},max=${wait.max.toFixed(3)},p50=${wait.p50.toFixed(3)},p95=${wait.p95.toFixed(3)}) `
            + `hold_ms(total=${hold.total.toFixed(3)},max=${hold.max.toFixed(3)},p50=${hold.p50.toFixed(3)},p95=${hold.p95.toFixed(3)})`,
        );
    }
}

let storageOperationQueue = Promise.resolve();
function queueStorageOperation(operation, label = 'unlabeled') {
    // Preserve the original callback and promise chain when diagnostics are off.
    if (!STORAGE_QUEUE_DIAG_ENABLED) {
        const operationRun = storageOperationQueue.then(operation, operation);
        storageOperationQueue = operationRun.catch(() => {});
        return operationRun;
    }
    const enqueuedAt = performance.now();
    const diagLabel = typeof label === 'string' && label.length > 0 ? label : 'unlabeled';
    const timedOperation = async (value) => {
        const startedAt = performance.now();
        try {
            return await operation(value);
        } finally {
            recordStorageQueueDiag(
                diagLabel,
                startedAt - enqueuedAt,
                performance.now() - startedAt,
            );
        }
    };
    const operationRun = storageOperationQueue.then(timedOperation, timedOperation);
    storageOperationQueue = operationRun.catch(() => {});
    return operationRun;
}

let importInProgress = false;

function getImportInProgress() {
    return importInProgress;
}

function setImportInProgress(next) {
    importInProgress = next;
}

const IMPORT_BARRIER_DRAIN_TEST_GATE_DIR = process.env.NODE_ENV === 'test'
    ? String(process.env.POCKETRISU_IMPORT_BARRIER_DRAIN_TEST_GATE_DIR ?? '').trim() || null
    : null;
let importBarrierAcquireFailures = process.env.NODE_ENV === 'test'
    ? Math.max(0, Number.parseInt(process.env.POCKETRISU_TEST_IMPORT_BARRIER_ACQUIRE_FAILURES ?? '0', 10) || 0)
    : 0;

async function drainStorageMutationsForImport() {
    await queueStorageOperation(async () => {
        if (!IMPORT_BARRIER_DRAIN_TEST_GATE_DIR) return;
        const holdPath = path.join(IMPORT_BARRIER_DRAIN_TEST_GATE_DIR, 'hold');
        if (!existsSync(holdPath)) return;
        await fs.mkdir(IMPORT_BARRIER_DRAIN_TEST_GATE_DIR, { recursive: true });
        await fs.writeFile(path.join(IMPORT_BARRIER_DRAIN_TEST_GATE_DIR, 'entered'), 'draining', 'utf-8');
        const releasePath = path.join(IMPORT_BARRIER_DRAIN_TEST_GATE_DIR, 'release');
        // This queue boundary must finish even if the importing peer leaves;
        // abandoning it would let a later transaction overtake older writes.
        while (existsSync(holdPath) && !existsSync(releasePath)) {
            await new Promise((resolve) => setTimeout(resolve, 10));
        }
    });
    if (importBarrierAcquireFailures > 0) {
        importBarrierAcquireFailures--;
        throw new ImportIngressError('Import barrier acquisition failed before publication', {
            code: 'IMPORT_BARRIER_ACQUIRE_FAILED',
            statusCode: 500,
            retryable: true,
        });
    }
}
// Imports keep one raw transaction open across streamed decompression, msgpack
// walking and directory swaps. The barrier drains this queue before an import
// begins, so mutations either land entirely before BEGIN or are refused —
// never acknowledged and then discarded by the import's ROLLBACK.
const importBarrier = createImportBarrier({
    drainMutations: drainStorageMutationsForImport,
});

function getImportBarrier() {
    return importBarrier;
}

class ImportInProgressError extends Error {
    constructor() {
        super('An import is in progress; the write was not applied');
        this.name = 'ImportInProgressError';
        this.importInProgress = true;
    }
}

// Every KV/chat-row/asset mutation must run through this, not through
// queueStorageOperation directly. The barrier check has to happen inside the
// queued callback: the serial FIFO order is what makes the boundary airtight.
function queueStorageMutation(operation, label = 'unlabeled') {
    return queueStorageOperation(() => {
        if (importBarrier.isHeld()) throw new ImportInProgressError();
        return operation();
    }, label);
}

// ─── SQLite durability policy ───────────────────────────────────────────────
// The database module opens in FULL so early migrations and invalid/missing
// configuration fail safe. Self-hosted administrators can explicitly trade a
// bounded power-loss window for fewer commit-time fsyncs; hub mode is always
// server-admin managed through POCKETRISU_SQLITE_DURABILITY_MODE.
const SQLITE_DURABILITY_CONFIG_KEY = 'config/sqlite-durability-mode';
const SQLITE_DURABILITY_ENV_KEY = 'POCKETRISU_SQLITE_DURABILITY_MODE';
const SQLITE_MAINTENANCE_CHECKPOINT_INTERVAL_MS = 5 * 60 * 1000;
const SQLITE_CHECKPOINT_RETRY_MS = 10 * 1000;
const SQLITE_FOREGROUND_CHECKPOINT_RETRY_MS = 25;
const SQLITE_FOREGROUND_CHECKPOINT_DEADLINE_MS = 3 * 1000;
const SQLITE_DURABILITY_PROFILES = Object.freeze({
    durable: Object.freeze({
        synchronous: 'FULL',
        checkpointIntervalMs: null,
        powerLossWindowMs: 0,
    }),
    balanced: Object.freeze({
        synchronous: 'NORMAL',
        checkpointIntervalMs: 60 * 1000,
        powerLossWindowMs: 60 * 1000,
    }),
    performance: Object.freeze({
        synchronous: 'NORMAL',
        checkpointIntervalMs: 5 * 60 * 1000,
        powerLossWindowMs: 5 * 60 * 1000,
    }),
});

function normalizeSqliteDurabilityMode(value) {
    const normalized = String(value ?? '').trim().toLowerCase();
    return Object.prototype.hasOwnProperty.call(SQLITE_DURABILITY_PROFILES, normalized)
        ? normalized
        : null;
}

const sqliteDurabilityEnvRaw = String(process.env[SQLITE_DURABILITY_ENV_KEY] ?? '').trim();
const sqliteDurabilityEnvMode = normalizeSqliteDurabilityMode(sqliteDurabilityEnvRaw);
const sqliteDurabilityManaged = HUB_HOSTING_MODE || sqliteDurabilityEnvRaw.length > 0;

function readPersistedSqliteDurabilityMode() {
    try {
        const raw = kvGet(SQLITE_DURABILITY_CONFIG_KEY);
        return raw ? normalizeSqliteDurabilityMode(Buffer.from(raw).toString('utf-8')) : null;
    } catch {
        return null;
    }
}

let sqliteDurabilityMode = sqliteDurabilityManaged
    ? (sqliteDurabilityEnvMode || 'durable')
    : (readPersistedSqliteDurabilityMode() || 'durable');
let sqliteDurabilityTimer = null;
let sqliteDurabilitySchedulerStarted = false;
let lastWalCheckpointAttempt = null;
let lastSuccessfulWalCheckpointAt = null;
let lastMaintenanceWalCheckpointAt = Date.now();

if (sqliteDurabilityEnvRaw && !sqliteDurabilityEnvMode) {
    logger.warn(
        `[SQLite] Invalid ${SQLITE_DURABILITY_ENV_KEY}=${JSON.stringify(sqliteDurabilityEnvRaw)}; `
        + 'using durable mode',
    );
}

function sqliteDurabilityProfile() {
    return SQLITE_DURABILITY_PROFILES[sqliteDurabilityMode];
}

function applySqliteDurabilityMode() {
    sqliteDb.pragma(`synchronous = ${sqliteDurabilityProfile().synchronous}`);
}

function isSqliteDurabilityManaged() {
    return sqliteDurabilityManaged;
}

function persistSqliteDurabilityMode(nextMode) {
    const previousMode = sqliteDurabilityMode;
    // Persist the operator choice through a FULL commit before applying
    // a requested downgrade. This also makes every earlier NORMAL-mode
    // transaction durable before the endpoint acknowledges the change.
    sqliteDb.pragma('synchronous = FULL');
    try {
        kvSet(SQLITE_DURABILITY_CONFIG_KEY, Buffer.from(nextMode, 'utf-8'));
        sqliteDurabilityMode = nextMode;
        applySqliteDurabilityMode();
    } catch (error) {
        sqliteDurabilityMode = previousMode;
        applySqliteDurabilityMode();
        throw error;
    }
}

function normalizeWalCheckpointResult(rawResult, mode, reason) {
    const row = Array.isArray(rawResult) && rawResult[0] ? rawResult[0] : {};
    const busy = Number(row.busy ?? 1);
    const result = {
        mode,
        reason,
        complete: busy === 0,
        busy,
        logFrames: Number(row.log ?? -1),
        checkpointedFrames: Number(row.checkpointed ?? -1),
        attemptedAt: Date.now(),
    };
    lastWalCheckpointAttempt = result;
    if (result.complete) {
        lastSuccessfulWalCheckpointAt = result.attemptedAt;
        if (mode === 'TRUNCATE') lastMaintenanceWalCheckpointAt = result.attemptedAt;
    }
    return result;
}

function runTrackedWalCheckpoint(mode, reason) {
    return normalizeWalCheckpointResult(checkpointWal(mode), mode, reason);
}

function runTrackedWalCheckpointWithoutBusyWait(mode, reason) {
    const busyTimeout = Number(sqliteDb.pragma('busy_timeout', { simple: true }));
    sqliteDb.pragma('busy_timeout = 0');
    try {
        return runTrackedWalCheckpoint(mode, reason);
    } finally {
        sqliteDb.pragma(`busy_timeout = ${busyTimeout}`);
    }
}

async function runTrackedWalCheckpointWithBusyRetry(mode, reason, {
    deadlineMs = SQLITE_FOREGROUND_CHECKPOINT_DEADLINE_MS,
} = {}) {
    const deadline = Date.now() + deadlineMs;
    let checkpoint;
    for (;;) {
        // The connection normally waits up to five seconds inside a busy
        // checkpoint. Foreground retries need the async deadline to remain in
        // control, so each synchronous attempt temporarily disables that wait
        // and restores it before yielding to any other work.
        checkpoint = runTrackedWalCheckpointWithoutBusyWait(mode, reason);
        if (checkpoint.complete) return checkpoint;
        const remainingMs = deadline - Date.now();
        if (remainingMs <= 0) return checkpoint;
        await new Promise((resolve) => setTimeout(
            resolve,
            Math.min(SQLITE_FOREGROUND_CHECKPOINT_RETRY_MS, remainingMs),
        ));
    }
}

function sqliteDurabilityState() {
    const profile = sqliteDurabilityProfile();
    return {
        mode: sqliteDurabilityMode,
        managed: sqliteDurabilityManaged,
        managedBy: sqliteDurabilityManaged
            ? (sqliteDurabilityEnvRaw ? 'environment' : 'hub')
            : null,
        synchronous: profile.synchronous,
        checkpointIntervalMs: profile.checkpointIntervalMs,
        maintenanceCheckpointIntervalMs: SQLITE_MAINTENANCE_CHECKPOINT_INTERVAL_MS,
        powerLossWindowMs: profile.powerLossWindowMs,
        lastSuccessfulCheckpointAt: lastSuccessfulWalCheckpointAt,
        lastCheckpoint: lastWalCheckpointAttempt,
    };
}

function sqliteCheckpointDelay(intervalMs) {
    if (!HUB_HOSTING_MODE) return intervalMs;
    // Stagger independently hosted tenant processes so a shared volume does not
    // receive a synchronized flush burst every minute.
    return Math.max(1000, Math.round(intervalMs * (0.9 + Math.random() * 0.2)));
}

function scheduleSqliteDurabilityCheckpoint(delayMs = null) {
    if (!sqliteDurabilitySchedulerStarted) return;
    if (sqliteDurabilityTimer) clearTimeout(sqliteDurabilityTimer);
    const profile = sqliteDurabilityProfile();
    const interval = profile.checkpointIntervalMs
        ?? SQLITE_MAINTENANCE_CHECKPOINT_INTERVAL_MS;
    sqliteDurabilityTimer = setTimeout(async () => {
        sqliteDurabilityTimer = null;
        let retry = false;
        try {
            const now = Date.now();
            const mode = now - lastMaintenanceWalCheckpointAt
                >= SQLITE_MAINTENANCE_CHECKPOINT_INTERVAL_MS
                ? 'TRUNCATE'
                : profile.synchronous === 'NORMAL' ? 'FULL' : 'TRUNCATE';
            const result = await queueStorageMutation(() => (
                runTrackedWalCheckpoint(mode, 'scheduled')
            ));
            retry = !result.complete;
            if (!result.complete) {
                logger.warn(`[SQLite] Scheduled ${mode} checkpoint was busy; retrying`);
            }
        } catch (error) {
            retry = true;
            if (!isImportInProgressError(error)) {
                logger.warn('[SQLite] Scheduled durability checkpoint failed:', error?.message || error);
            }
        } finally {
            scheduleSqliteDurabilityCheckpoint(
                retry ? SQLITE_CHECKPOINT_RETRY_MS : null,
            );
        }
    }, sqliteCheckpointDelay(delayMs ?? interval));
    sqliteDurabilityTimer.unref?.();
}

function startSqliteDurabilityCheckpointScheduler() {
    sqliteDurabilitySchedulerStarted = true;
    lastMaintenanceWalCheckpointAt = Date.now();
    scheduleSqliteDurabilityCheckpoint();
}

function rescheduleSqliteDurabilityCheckpoint() {
    if (sqliteDurabilitySchedulerStarted) scheduleSqliteDurabilityCheckpoint();
}

// db.cjs deliberately started in FULL. This is the only startup point that may
// downgrade it, and only after an explicit valid persisted/admin choice exists.
applySqliteDurabilityMode();

// Imports hold a raw transaction outside the storage queue. Wait before
// entering the queue, then re-check from inside it: if an import won the race,
// retry after that holder releases. If the read wins, the import's queue drain
// stays behind it and cannot open its transaction until the read completes.
function throwIfSignalAborted(signal) {
    if (!signal?.aborted) return;
    if (signal.reason !== undefined) throw signal.reason;
    const error = new Error('The operation was aborted');
    error.name = 'AbortError';
    throw error;
}

async function queueStorageReadAfterImports(operation, signal = null) {
    while (true) {
        throwIfSignalAborted(signal);
        await importBarrier.waitUntilIdle(signal);
        throwIfSignalAborted(signal);
        const attempt = await queueStorageOperation(async () => {
            throwIfSignalAborted(signal);
            if (importBarrier.isHeld()) return { retry: true };
            return { retry: false, value: await operation() };
        });
        if (!attempt.retry) return attempt.value;
    }
}

function isImportInProgressError(error) {
    return Boolean(error && error.importInProgress === true);
}

// 503 + Retry-After: the client may safely reissue the same write once the
// import finishes. Anything else would let the caller treat a dropped write as
// applied.
function sendImportBusy(res) {
    if (res.headersSent) return;
    res.setHeader('Retry-After', '5');
    res.status(503).json({
        error: 'An import is in progress; retry this write after it completes',
        code: 'IMPORT_IN_PROGRESS',
        retryAfter: 5,
        retryable: true,
        commitOutcome: 'not-committed',
        commitOutcomeUnknown: false,
    });
}


// Test-only boundaries for the per-entry bulk-write contract. The suffix is
// the zero-based request index, for example `before-asset-publish:1`.
const bulkWriteFailpoints = new Set(process.env.NODE_ENV === 'test'
    ? String(process.env.POCKETRISU_TEST_BULK_WRITE_FAILPOINT ?? '')
        .split(',')
        .map(value => value.trim())
        .filter(Boolean)
    : []);
const bulkWriteTestGateDir = process.env.NODE_ENV === 'test'
    ? String(process.env.POCKETRISU_TEST_BULK_WRITE_GATE_DIR ?? '').trim()
    : '';

function hitBulkWriteFailpoint(boundary, index) {
    if (!bulkWriteFailpoints.has(`${boundary}:${index}`)
        && !bulkWriteFailpoints.has(`${boundary}:*`)) return;
    const error = new Error(`Injected bulk write failure at ${boundary}:${index}`);
    error.code = 'POCKETRISU_TEST_BULK_WRITE_FAILURE';
    throw error;
}

async function waitAtBulkWriteValidationTestGate() {
    if (!bulkWriteTestGateDir) return;
    const holdPath = path.join(bulkWriteTestGateDir, 'hold');
    if (!existsSync(holdPath)) return;
    await fs.mkdir(bulkWriteTestGateDir, { recursive: true });
    await fs.writeFile(
        path.join(bulkWriteTestGateDir, 'entered'),
        'before-queued-validation',
        'utf-8',
    );
    const releasePath = path.join(bulkWriteTestGateDir, 'release');
    while (existsSync(holdPath) && !existsSync(releasePath)) {
        await new Promise(resolve => setTimeout(resolve, 10));
    }
}

const pluginStorageOwnershipReadFailpoint = process.env.NODE_ENV === 'test'
    ? String(process.env.POCKETRISU_TEST_PLUGIN_OWNERSHIP_READ_FAILPOINT ?? '').trim()
    : '';
const pluginStorageOwnershipStatsPath = process.env.NODE_ENV === 'test'
    ? String(process.env.POCKETRISU_TEST_PLUGIN_OWNERSHIP_STATS_PATH ?? '').trim()
    : '';
const SNAPSHOT_RESTORE_TEST_GATE_DIR = process.env.NODE_ENV === 'test'
    ? String(process.env.POCKETRISU_SNAPSHOT_RESTORE_TEST_GATE_DIR ?? '').trim() || null
    : null;
const STREAM_LOAD_TEST_GATE_DIR = process.env.NODE_ENV === 'test'
    ? String(process.env.POCKETRISU_STREAM_LOAD_TEST_GATE_DIR ?? '').trim() || null
    : null;
const STREAM_LOAD_TEST_GATE_PHASE = process.env.NODE_ENV === 'test'
    ? String(process.env.POCKETRISU_STREAM_LOAD_TEST_GATE_PHASE ?? '').trim() || null
    : null;
const importRecoveryFailpoint = process.env.NODE_ENV === 'test'
    ? String(process.env.POCKETRISU_TEST_IMPORT_RECOVERY_FAILPOINT ?? '').trim()
    : '';

function throwIfStreamingRestoreAborted(shouldAbort) {
    if (typeof shouldAbort !== 'function' || !shouldAbort()) return;
    const error = new Error('Streaming Risu load cancelled');
    error.code = 'RISU_STREAM_ABORTED';
    throw error;
}

async function waitAtSnapshotRestoreTestGate(shouldAbort) {
    if (!SNAPSHOT_RESTORE_TEST_GATE_DIR) return;
    const holdPath = path.join(SNAPSHOT_RESTORE_TEST_GATE_DIR, 'hold');
    if (!existsSync(holdPath)) return;
    await fs.mkdir(SNAPSHOT_RESTORE_TEST_GATE_DIR, { recursive: true });
    await fs.writeFile(
        path.join(SNAPSHOT_RESTORE_TEST_GATE_DIR, 'entered'),
        'before-folded-delete',
        'utf-8',
    );
    const releasePath = path.join(SNAPSHOT_RESTORE_TEST_GATE_DIR, 'release');
    while (existsSync(holdPath) && !existsSync(releasePath)) {
        throwIfStreamingRestoreAborted(shouldAbort);
        await new Promise((resolve) => setTimeout(resolve, 10));
    }
    throwIfStreamingRestoreAborted(shouldAbort);
}

let streamLoadTestGateEntered = false;
async function waitAtStreamLoadTestGate(phase) {
    if (!STREAM_LOAD_TEST_GATE_DIR
        || STREAM_LOAD_TEST_GATE_PHASE !== phase
        || streamLoadTestGateEntered) return;
    const holdPath = path.join(STREAM_LOAD_TEST_GATE_DIR, 'hold');
    if (!existsSync(holdPath)) return;
    streamLoadTestGateEntered = true;
    await fs.mkdir(STREAM_LOAD_TEST_GATE_DIR, { recursive: true });
    await fs.writeFile(
        path.join(STREAM_LOAD_TEST_GATE_DIR, 'entered'),
        phase,
        'utf-8',
    );
    const releasePath = path.join(STREAM_LOAD_TEST_GATE_DIR, 'release');
    while (existsSync(holdPath) && !existsSync(releasePath)) {
        await new Promise(resolve => setTimeout(resolve, 10));
    }
}


const PLUGIN_STORAGE_BATCH_MAX_OPERATIONS = 128;
const PLUGIN_STORAGE_BATCH_MAX_BODY_BYTES = 16 * 1024 * 1024;
const PLUGIN_STORAGE_BATCH_STREAM_MAX_METADATA_BYTES = 1024 * 1024;
const PLUGIN_STORAGE_BATCH_STREAM_MAX_PAYLOAD_BYTES = Math.max(
    PLUGIN_VALUE_MAX_BYTES,
    PLUGIN_STORAGE_MAX_BYTES,
);
const PLUGIN_STORAGE_TRANSITION_STREAM_MAX_ENTRIES = 100_000;
const PLUGIN_STORAGE_TRANSITION_STREAM_MAX_METADATA_BYTES = 64 * 1024 * 1024;
const PLUGIN_STORAGE_TRANSITION_STREAM_MAX_PAYLOAD_BYTES = Math.max(
    PLUGIN_VALUE_MAX_BYTES,
    PLUGIN_STORAGE_MAX_BYTES,
);
const PLUGIN_STORAGE_UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

// Captures run inside the endpoint's storage operation. Reconcile enters the
// same queue through runStorageOperation, so neither can observe half-written
// backup state or race a chat-row overwrite.
function observeChatBackupTestEvent(event) {
    if (process.env.NODE_ENV !== 'test'
        || event?.event !== 'reachability-inventory-complete') return;
    const configured = String(
        process.env.POCKETRISU_TEST_CHAT_BACKUP_REACHABILITY_GATE_DIR ?? '',
    ).trim();
    if (!configured) return;
    const gateDir = path.resolve(configured);
    const holdPath = path.join(gateDir, 'hold');
    if (!existsSync(holdPath)) return;
    mkdirSync(gateDir, { recursive: true });
    writeFileSync(path.join(gateDir, 'entered'), String(event.totalCandidates), 'utf8');
    const releasePath = path.join(gateDir, 'release');
    const deadline = Date.now() + 30_000;
    const sleeper = new Int32Array(new SharedArrayBuffer(4));
    while (existsSync(holdPath) && !existsSync(releasePath)) {
        if (Date.now() >= deadline) {
            throw new Error('Timed out at chat-backup reachability test gate');
        }
        Atomics.wait(sleeper, 0, 0, 10);
    }
}

const chatBackupStore = createChatBackupStore({
    getChatBackupsRoot: () => chatBackupsDir,
    getChatBackupsReadRoots: () => {
        const required = new Set(chatBackupRequiredReadRoots.map(root => path.resolve(root)));
        return chatBackupReadRoots.map(root => ({
            root,
            required: required.has(path.resolve(root)),
        }));
    },
    logger,
    inspectChatRow: (chaId, chatId) => (
        chatRowStore.inspectChatRowForBackup(chaId, chatId)
    ),
    readChatRowRaw: (chaId, chatId) => chatRowStore.readChatRowRaw(chaId, chatId),
    repairChatRowMetadata: (rowState, coldStorage, messageCount) => (
        chatRowStore.repairChatRowMetadata(rowState, coldStorage, messageCount)
    ),
    readChatRowRawWithMetadata: (chaId, chatId) => (
        chatRowStore.readChatRowRawWithMetadata(chaId, chatId)
    ),
    streamChatRowRawToFile: (chaId, chatId, filePath) => (
        chatRowStore.streamChatRowRawToFile(chaId, chatId, filePath)
    ),
    getByteBudget: () => resolveChatBackupMaxBytes({ kvGet }),
    getUncompressedByteBudget: () => resolveChatBackupMaxUncompressedBytes({ kvGet }),
    runStorageOperation: queueStorageOperation,
    diagnostics: process.env.NODE_ENV === 'test'
        ? { onEvent: observeChatBackupTestEvent }
        : null,
});

function listChatBackupChats() {
    return chatBackupStore.listChatBackupChats();
}

function listChatBackups(chaId, chatId) {
    return chatBackupStore.listChatBackups(chaId, chatId);
}

function readChatBackup(chaId, chatId, versionId) {
    return chatBackupStore.readChatBackup(chaId, chatId, versionId);
}

function captureOrphanChatPreImage(identity) {
    return chatBackupStore.captureChatPreImage({
        ...identity,
        reason: 'orphan-sweep',
        force: true,
        required: true,
    });
}

const DB_CACHE_TEST_DIAGNOSTICS = process.env.NODE_ENV === 'test';
const CHAT_EXTERNALIZATION_MARKER_KEY = 'migration/chats-externalized';
const CHAT_EXTERNALIZATION_MARKER_VALUE = Buffer.from('done', 'utf-8');

// ─── Persist failure tracking (Stage 1 visibility) ───────────────────────────
// Debounced failures surface on the next patch; structural-patch failures
// surface on the current response. Cleared on the next successful persist.
let lastPersistFailure = null;

function recordPersistFailure(error, source) {
    const message = String(error?.message || error || 'unknown error');
    const attemptedSize = typeof error?.attemptedSize === 'number' ? error.attemptedSize : null;
    // Preserve timestamp when the failure is identical to the last one — every
    // debounce cycle re-records the same failure, and clients dedupe by ts.
    // Without this guard a fresh ts every 5s would re-fire the toast.
    if (lastPersistFailure
        && lastPersistFailure.source === source
        && lastPersistFailure.message === message
        && lastPersistFailure.attemptedSize === attemptedSize) {
        return;
    }
    lastPersistFailure = {
        timestamp: Date.now(),
        message,
        attemptedSize,
        source,
    };
}

function clearPersistFailure() {
    lastPersistFailure = null;
}

function currentPersistWarning() {
    return lastPersistFailure;
}

// ─── Server-side database backup (DB-only snapshots) ────────────────────────
//
// Snapshots live as `database/dbbackup-{ts}.bin` keys inside the kv table.
// They're created on every successful persist (with a cooldown) and rotated
// to fit user-configured count/size limits — see SNAPSHOT_LIMIT_* below.
const SNAPSHOT_LIMIT_COUNT_KEY = 'config/snapshot-max-count';
const SNAPSHOT_LIMIT_BYTES_KEY = 'config/snapshot-max-bytes';
const SNAPSHOT_LIMIT_DEFAULT_COUNT = 20;
const SNAPSHOT_LIMIT_DEFAULT_BYTES = 500 * 1024 * 1024; // 500 MB
// Safety bounds to keep a stray PUT from making the system unusable.
const SNAPSHOT_LIMIT_MIN_COUNT = 1;
const SNAPSHOT_LIMIT_MAX_COUNT = 100;
const SNAPSHOT_LIMIT_MIN_BYTES = 10 * 1024 * 1024;        // 10 MB
const SNAPSHOT_LIMIT_MAX_BYTES = 50 * 1024 * 1024 * 1024; // 50 GB
// Hub-mode snapshot byte cap. POCKETRISU_HUB_SNAPSHOT_CAP_MB (in MB) replaces
// the tenant-stored value everywhere the cap is read (endpoints and trim
// rotation); unset/invalid falls back to the 500 MB default, clamped to the
// same safety bounds as a PUT. null outside hub mode.
const HUB_SNAPSHOT_CAP_BYTES = (() => {
    if (!HUB_HOSTING_MODE) return null;
    const mb = Number(process.env.POCKETRISU_HUB_SNAPSHOT_CAP_MB);
    if (!Number.isFinite(mb) || mb <= 0) return SNAPSHOT_LIMIT_DEFAULT_BYTES;
    const bytes = Math.floor(mb * 1024 * 1024);
    return Math.min(SNAPSHOT_LIMIT_MAX_BYTES, Math.max(SNAPSHOT_LIMIT_MIN_BYTES, bytes));
})();
const BACKUP_INTERVAL_MS = process.env.POCKETRISU_BACKUP_INTERVAL_MS
    ? Number(process.env.POCKETRISU_BACKUP_INTERVAL_MS)
    : 5 * 60 * 1000; // 5 minutes (override for tests to force snapshot creation)
// A plugin publication can commit after an ordinary database/chat snapshot has
// consumed the cooldown. Keep that later recovery obligation durable so a
// restart cannot lose the deferred snapshot. The marker is replaced inside the
// same SQLite transaction as each logical plugin mutation/transition, then
// cleared atomically with the snapshot that folded that exact-or-later state.
const PLUGIN_RECOVERY_SNAPSHOT_DIRTY_KEY = 'config/plugin-storage-recovery-dirty';
const PLUGIN_RECOVERY_SNAPSHOT_TEST_GATE_DIR
    = process.env.POCKETRISU_PLUGIN_RECOVERY_SNAPSHOT_TEST_GATE_DIR || null;
const SNAPSHOT_ASSEMBLY_TEST_GATE_DIR
    = process.env.POCKETRISU_SNAPSHOT_ASSEMBLY_TEST_GATE_DIR || null;
const SNAPSHOT_SOURCE_TOKEN_MAX_RETRIES = 2;
const SNAPSHOT_TEST_STATS_PATH = process.env.NODE_ENV === 'test'
    ? String(process.env.POCKETRISU_TEST_SNAPSHOT_STATS_PATH ?? '').trim() || null
    : null;
const snapshotTestStats = {
    metadataProbes: 0,
    databaseBodySpools: 0,
    assemblies: 0,
    tokenMismatches: 0,
    publications: 0,
};
const PLUGIN_RECOVERY_SNAPSHOT_RETRY_MS = Math.min(
    5000,
    Math.max(100, Number.isFinite(BACKUP_INTERVAL_MS) ? BACKUP_INTERVAL_MS : 1000),
);
let lastBackupTime = null;
let backupCreationInFlight = false;
let pluginRecoverySnapshotTimer = null;
let pluginRecoverySnapshotRun = null;
let deferredBackupPending = false;

function publishSnapshotTestStats() {
    if (!SNAPSHOT_TEST_STATS_PATH) return;
    const statsPath = path.resolve(process.cwd(), SNAPSHOT_TEST_STATS_PATH);
    writeFileSync(statsPath, JSON.stringify(snapshotTestStats), 'utf-8');
}

function newPluginRecoverySnapshotToken() {
    return Buffer.from(nodeCrypto.randomUUID(), 'utf-8');
}

function markPluginRecoverySnapshotDirty(token) {
    kvSet(PLUGIN_RECOVERY_SNAPSHOT_DIRTY_KEY, token);
}

function clearCapturedPluginRecoverySnapshotDirty(capturedToken) {
    if (!capturedToken) return;
    const currentToken = kvGet(PLUGIN_RECOVERY_SNAPSHOT_DIRTY_KEY);
    if (currentToken?.equals(capturedToken)) {
        kvDel(PLUGIN_RECOVERY_SNAPSHOT_DIRTY_KEY);
    }
}

async function waitAtSnapshotTestGate(gateDir, state) {
    if (!gateDir) return;
    const holdPath = path.join(gateDir, 'hold');
    if (!existsSync(holdPath)) return;
    await fs.mkdir(gateDir, { recursive: true });
    await fs.writeFile(
        path.join(gateDir, 'entered'),
        state,
        'utf-8',
    );
    const releasePath = path.join(gateDir, 'release');
    while (existsSync(holdPath) && !existsSync(releasePath)) {
        await new Promise((resolve) => setTimeout(resolve, 10));
    }
}

async function waitAtPluginRecoverySnapshotTestGate() {
    return waitAtSnapshotTestGate(
        PLUGIN_RECOVERY_SNAPSHOT_TEST_GATE_DIR,
        'before-publication',
    );
}

async function waitAtSnapshotAssemblyTestGate() {
    return waitAtSnapshotTestGate(SNAPSHOT_ASSEMBLY_TEST_GATE_DIR, 'assembling-pinned-source');
}

function readSnapshotConfigInt(key, fallback, min, max) {
    try {
        const raw = kvGet(key);
        if (!raw) return fallback;
        const n = parseInt(Buffer.from(raw).toString('utf-8').trim(), 10);
        if (!Number.isFinite(n)) return fallback;
        return Math.min(max, Math.max(min, n));
    } catch { return fallback; }
}

function getSnapshotLimits() {
    return {
        maxCount: readSnapshotConfigInt(
            SNAPSHOT_LIMIT_COUNT_KEY, SNAPSHOT_LIMIT_DEFAULT_COUNT,
            SNAPSHOT_LIMIT_MIN_COUNT, SNAPSHOT_LIMIT_MAX_COUNT,
        ),
        maxBytes: HUB_SNAPSHOT_CAP_BYTES ?? readSnapshotConfigInt(
            SNAPSHOT_LIMIT_BYTES_KEY, SNAPSHOT_LIMIT_DEFAULT_BYTES,
            SNAPSHOT_LIMIT_MIN_BYTES, SNAPSHOT_LIMIT_MAX_BYTES,
        ),
    };
}

// Walk newest → oldest; keep within both limits, delete the rest. The most
// recent snapshot is always kept (even if it alone exceeds the byte limit) so
// we never end up with zero backups after a config change.
function trimSnapshotsToLimits() {
    const { maxCount, maxBytes } = getSnapshotLimits();
    const keys = kvList(DB_BACKUP_PREFIX)
        .map((key) => {
            const tsRaw = parseInt(key.slice(DB_BACKUP_PREFIX.length, -4), 10);
            return { key, ts: Number.isFinite(tsRaw) ? tsRaw : 0 };
        })
        .sort((a, b) => b.ts - a.ts)
        .map(entry => entry.key);
    let removed = 0;

    // Exclusive footprint is "what deleting this manifest frees". Removing a
    // sibling can make shared chunks exclusive, so recalculate after each trim.
    while (keys.length > 1) {
        const costs = new Map(
            snapshotFootprints(DB_BACKUP_PREFIX).map((entry) => [entry.key, entry.size]),
        );
        const totalBytes = keys.reduce((sum, key) => sum + (costs.get(key) ?? 0), 0);
        if (keys.length <= maxCount && totalBytes <= maxBytes) break;
        kvDel(keys.pop());
        removed++;
    }
    return { kept: keys.length, removed };
}

function warnAndPreserveMissingChatRow(source, chaId, chatId) {
    // The referenced payload is already lost. Recovery-oriented backups keep
    // the remaining database usable by retaining its metadata-only stub.
    logger.warn(
        `[${source}] Missing referenced chat row ${chaId}/${chatId}; preserving bare stub`
    );
}

function snapshotSourceTokensEqual(left, right) {
    return JSON.stringify(left) === JSON.stringify(right);
}

function runAutomaticSnapshotStoragePhase(storageAlreadyExclusive, operation, label) {
    return storageAlreadyExclusive ? operation() : queueStorageMutation(operation, label);
}

async function captureAutomaticSnapshotSource(storageAlreadyExclusive) {
    return runAutomaticSnapshotStoragePhase(storageAlreadyExclusive, async () => {
        await flushPendingDb({ scheduleSnapshot: false });
        if (dbPersistRetryPending) return null;

        const snapshot = createKvSnapshot();
        try {
            const sourceToken = snapshot.kvGetSnapshotSourceToken();
            snapshotTestStats.metadataProbes += 1;
            publishSnapshotTestStats();
            if (!Number.isSafeInteger(sourceToken.databaseSize)
                || sourceToken.databaseSize <= 0) {
                snapshot.close();
                return null;
            }
            return { snapshot, sourceToken };
        } catch (error) {
            snapshot.close();
            throw error;
        }
    }, 'snapshot-capture');
}

async function assembleAutomaticSnapshotSource(captured) {
    const { snapshot } = captured;
    const spoolRow = (key) => spoolBackupSnapshotRow(snapshot, key);
    const spoolChatRow = (key) => spoolLogicalChatSnapshotRow(snapshot, key);
    let assemblyGateReached = false;
    const databaseSource = await spoolBackupSnapshotRow(snapshot, DB_BLOB_KEY, {
        onChunk: async () => {
            if (assemblyGateReached) return;
            assemblyGateReached = true;
            await waitAtSnapshotAssemblyTestGate();
        },
    });
    if (!databaseSource) return null;
    snapshotTestStats.databaseBodySpools += 1;
    publishSnapshotTestStats();

    const finalPath = path.join(
        databaseSpoolDir,
        `${DATABASE_SPOOL_FILE_PREFIX}${process.pid}-${nodeCrypto.randomUUID()}`,
    );
    const filePath = `${finalPath}.tmp`;
    try {
        const databaseState = await readBackupRisuSaveTopLevelFields(
            databaseSource,
            ['optimizePluginMemory', PLUGIN_STORAGE_GENERATION_FIELD],
            {
                tempDir: requireDatabaseSpoolDirSync(),
                readRemoteRowSize: (name) => snapshot.kvSize(
                    `remotes/${name}.local.bin`,
                ),
                readRemoteRowSource: (name) => spoolRow(
                    `remotes/${name}.local.bin`,
                ),
            },
        );
        const ownedPluginStorage = resolveOwnedPluginStorageRows(databaseState, snapshot);
        const pluginStorage = {
            valueRows: ownedPluginStorage.valueRows,
            metaRows: ownedPluginStorage.metaRows,
            readRowSource: spoolRow,
        };
        const mcpToolCalls = {
            rows: listMcpToolCallBackupEntries(snapshot).map((entry) => ({
                key: parseMcpToolCallStorageKey(entry.key).suffix,
                source: entry.key,
            })),
            readRowSource: spoolRow,
        };
        const result = await streamBackupRisuSaveToFile({
            databaseSource,
            filePath,
            readChatRowSource: (chaId, chatId) => spoolChatRow(
                chatRowKey(chaId, chatId),
            ),
            readRemoteRowSource: (name) => spoolRow(
                `remotes/${name}.local.bin`,
            ),
            readRemoteRowSize: (name) => snapshot.kvSize(
                `remotes/${name}.local.bin`,
            ),
            pluginStorage,
            mcpToolCalls,
            markPluginStorageFolded: true,
            canonicalJsonEncoding: true,
            tempDir: requireDatabaseSpoolDirSync(),
            onMissingChatRow: (chaId, chatId) => {
                warnAndPreserveMissingChatRow('Snapshot', chaId, chatId);
            },
        });
        snapshotTestStats.assemblies += 1;
        publishSnapshotTestStats();
        return result;
    } catch (error) {
        await fs.unlink(filePath).catch(() => {});
        throw error;
    } finally {
        await databaseSource.cleanup();
    }
}

async function publishAutomaticSnapshot(
    captured,
    backupDbSpool,
    backupKey,
    storageAlreadyExclusive,
) {
    return runAutomaticSnapshotStoragePhase(storageAlreadyExclusive, () => {
        const currentToken = kvGetSnapshotSourceToken();
        if (!snapshotSourceTokensEqual(captured.sourceToken, currentToken)) {
            snapshotTestStats.tokenMismatches += 1;
            publishSnapshotTestStats();
            return { published: false, mismatch: true };
        }
        sqliteDb.transaction(() => {
            kvSetFromFile(backupKey, backupDbSpool.filePath);
            clearCapturedPluginRecoverySnapshotDirty(
                captured.sourceToken.recoveryDirtyToken
                    ? Buffer.from(captured.sourceToken.recoveryDirtyToken, 'base64url')
                    : null,
            );
        })();
        lastBackupTime = Date.now();
        trimSnapshotsToLimits();
        snapshotTestStats.publications += 1;
        publishSnapshotTestStats();
        return { published: true, mismatch: false };
    }, 'snapshot-publish');
}

async function publishAutomaticSnapshotAfterImports(
    captured,
    backupDbSpool,
    backupKey,
    storageAlreadyExclusive,
) {
    for (;;) {
        try {
            return await publishAutomaticSnapshot(
                captured,
                backupDbSpool,
                backupKey,
                storageAlreadyExclusive,
            );
        } catch (error) {
            if (storageAlreadyExclusive || !isImportInProgressError(error)) throw error;
            // The pinned assembly remains private. Let the destructive
            // replacement finish, then compare its committed source token with
            // the captured one instead of publishing or silently abandoning
            // the consistency proof.
            await importBarrier.waitUntilIdle();
        }
    }
}

async function createBackupAndRotate({ storageAlreadyExclusive = false } = {}) {
    const now = Date.now();
    if (lastBackupTime && now - lastBackupTime < BACKUP_INTERVAL_MS) {
        return {
            created: false,
            retryAfterMs: BACKUP_INTERVAL_MS - (now - lastBackupTime),
        };
    }
    if (backupCreationInFlight) {
        return { created: false, retryAfterMs: 50, reschedule: true };
    }

    backupCreationInFlight = true;
    try {
        for (let attempt = 0; attempt < SNAPSHOT_SOURCE_TOKEN_MAX_RETRIES; attempt++) {
            let captured = null;
            let backupDbSpool = null;
            try {
                captured = await captureAutomaticSnapshotSource(storageAlreadyExclusive);
                if (!captured) {
                    return { created: false, retryAfterMs: PLUGIN_RECOVERY_SNAPSHOT_RETRY_MS };
                }
                backupDbSpool = await assembleAutomaticSnapshotSource(captured);
                captured.snapshot.close();
                captured.snapshot = null;
                if (!backupDbSpool) {
                    return { created: false, retryAfterMs: PLUGIN_RECOVERY_SNAPSHOT_RETRY_MS };
                }

                // Test-only gate after the pinned source has been fully consumed
                // and before publication re-enters the mutation queue.
                await waitAtPluginRecoverySnapshotTestGate();
                const backupKey = `${DB_BACKUP_PREFIX}${(Date.now() / 100).toFixed()}.bin`;
                const publication = await publishAutomaticSnapshotAfterImports(
                    captured,
                    backupDbSpool,
                    backupKey,
                    storageAlreadyExclusive,
                );
                if (publication.published) {
                    return { created: true, retryAfterMs: 0 };
                }
                if (attempt + 1 >= SNAPSHOT_SOURCE_TOKEN_MAX_RETRIES) {
                    return { created: false, retryAfterMs: 50, reschedule: true };
                }
            } finally {
                captured?.snapshot?.close();
                if (backupDbSpool) {
                    await fs.unlink(backupDbSpool.filePath).catch(() => {});
                }
            }
        }
        return { created: false, retryAfterMs: 50, reschedule: true };
    } catch (error) {
        if (!isImportInProgressError(error)) {
            logger.error(
                `[Snapshot] Failed to create database snapshot using spool ${databaseSpoolDir}:`,
                error,
            );
        }
        return {
            created: false,
            retryAfterMs: PLUGIN_RECOVERY_SNAPSHOT_RETRY_MS,
            reschedule: isImportInProgressError(error),
        };
    } finally {
        backupCreationInFlight = false;
    }
}

function pluginRecoverySnapshotDelay(extraDelayMs = 0) {
    const cooldownDelay = lastBackupTime
        ? Math.max(0, BACKUP_INTERVAL_MS - (Date.now() - lastBackupTime))
        : 0;
    return Math.max(0, cooldownDelay, extraDelayMs);
}

function schedulePluginRecoverySnapshot(extraDelayMs = 0) {
    if (!kvGet(PLUGIN_RECOVERY_SNAPSHOT_DIRTY_KEY)) return;
    if (pluginRecoverySnapshotTimer || pluginRecoverySnapshotRun) return;
    pluginRecoverySnapshotTimer = setTimeout(() => {
        pluginRecoverySnapshotTimer = null;
        let retryAfterMs = 0;
        pluginRecoverySnapshotRun = (async () => {
            try {
                if (!kvGet(PLUGIN_RECOVERY_SNAPSHOT_DIRTY_KEY)) return;
                const result = await createBackupAndRotate();
                retryAfterMs = result?.retryAfterMs ?? PLUGIN_RECOVERY_SNAPSHOT_RETRY_MS;
            } catch (error) {
                retryAfterMs = PLUGIN_RECOVERY_SNAPSHOT_RETRY_MS;
                if (isImportInProgressError(error)) {
                    // Do not spin while an import owns the SQLite connection.
                    // Its replacement state is the next safe recovery point.
                    await importBarrier.waitUntilIdle();
                } else {
                    logger.error('[Plugin storage] Deferred recovery snapshot failed:', error);
                }
            } finally {
                pluginRecoverySnapshotRun = null;
                if (kvGet(PLUGIN_RECOVERY_SNAPSHOT_DIRTY_KEY)) {
                    schedulePluginRecoverySnapshot(retryAfterMs);
                }
            }
        })();
    }, pluginRecoverySnapshotDelay(extraDelayMs));
    pluginRecoverySnapshotTimer.unref?.();
}

function scheduleBackupAndRotate() {
    if (deferredBackupPending) return;
    deferredBackupPending = true;
    setImmediate(async () => {
        try {
            while (true) {
                await importBarrier.waitUntilIdle();
                const result = await createBackupAndRotate();
                if (!result?.reschedule) break;
                await new Promise((resolve) => setTimeout(
                    resolve,
                    Math.max(10, result.retryAfterMs ?? 50),
                ));
            }
        } catch (error) {
            logger.warn('[Snapshot] Deferred snapshot scheduling failed:', error);
        } finally {
            deferredBackupPending = false;
        }
    });
}

async function flushPendingDb({ scheduleSnapshot = true } = {}) {
    let persisted = false;
    if (saveTimers[DB_HEX_KEY] || dbPersistRetryPending) {
        if (saveTimers[DB_HEX_KEY]) clearTimeout(saveTimers[DB_HEX_KEY]);
        delete saveTimers[DB_HEX_KEY];
        if (peekDbCacheValue(DB_HEX_KEY)) {
            try {
                await persistDbCache(DB_HEX_KEY, 'database/database.bin');
                dbPersistRetryPending = false;
                clearPersistFailure();
                persisted = true;
                if (scheduleSnapshot) scheduleBackupAndRotate();
            } catch (error) {
                // Retain an actionable pending state after consuming the timer.
                // A plugin recovery retry must reattempt this database persist,
                // never snapshot dbCache and clear its token over stale live bytes.
                dbPersistRetryPending = Boolean(peekDbCacheValue(DB_HEX_KEY));
                throw error;
            }
        } else {
            // Integrity guards deliberately invalidate malformed cache state.
            // That state is superseded by authoritative live bytes, not retryable.
            dbPersistRetryPending = false;
        }
    }
    return persisted;
}

function invalidateDbCache() {
    invalidateDbCacheEntry(DB_HEX_KEY);
    pluginStorageManifestCache.clear('database-publication-invalidation');
    dbPersistRetryPending = false;
    pendingChatRowDeletions.clear();
    dbEtag = null;
}

function invalidateAllDbCaches() {
    const filePaths = new Set([...dbCache.keys(), ...Object.keys(saveTimers)]);
    filePaths.add(DB_HEX_KEY);
    for (const filePath of filePaths) invalidateDbCacheEntry(filePath);
    pluginStorageManifestCache.clear('destructive-publication-invalidation');
    dbPersistRetryPending = false;
    pendingChatRowDeletions.clear();
    dbEtag = null;
}

// ─── Remote-block migration ─────────────────────────────────────────────────
//
// Background: upstream RisuAI (and very early NodeOnly versions) split each
// character's data out of database.bin into a separate `remotes/<chaId>.local.bin`
// file. The main database.bin then carries a REMOTE pointer block instead of the
// character payload. The server-side RisuSaveDecoder used to skip those blocks
// outright, so any decode pass — /api/read, /api/chat-content fallback, chat
// store init — saw the character as missing and lost its chats.
//
// NodeOnly never wanted this split (`disableRemoteSaving` is hardcoded to
// true), so we one-shot convert any leftover REMOTE blocks to inline raw blocks
// the first time a server with such data boots. The reencoded database.bin is
// stored in legacy msgpack format, which has no block structure at all — so
// the REMOTE code path becomes unreachable for future decodes.
//
// Idempotent via a KV marker. The marker lives in KV (not on disk) so a backup
// import — which wipes most KV prefixes and INSERTs a new database.bin — naturally
// clears it, letting the new contents be re-evaluated.

const REMOTE_MIGRATION_MARKER_KEY = 'migration/disable-remote-saving';
const REMOTE_MIGRATION_MARKER_VALUE = Buffer.from('done', 'utf-8');

function isRemoteMigrationDone() {
    const value = kvGet(REMOTE_MIGRATION_MARKER_KEY);
    return value !== null && value.length > 0;
}

function markRemoteMigrationDone() {
    kvSet(REMOTE_MIGRATION_MARKER_KEY, REMOTE_MIGRATION_MARKER_VALUE);
}

async function decodeAuthoritativeDatabase(raw, options = {}) {
    return decodeAuthoritativeRisuSave(raw, {
        resolveRemote: async (name) => {
            const value = kvGet(`remotes/${name}.local.bin`);
            return value || null;
        },
        ...options,
    });
}

/**
 * Convert any leftover REMOTE blocks in database.bin into inline raw blocks.
 * Safe to call repeatedly: idempotent via KV marker.
 */
async function migrateRemoteBlocksIfNeeded() {
    if (isRemoteMigrationDone()) return { ran: false, reason: 'already-done' };

    const raw = kvGet('database/database.bin');
    if (!raw) {
        markRemoteMigrationDone();
        return { ran: false, reason: 'no-database' };
    }

    if (!hasRemoteBlocks(raw)) {
        markRemoteMigrationDone();
        return { ran: false, reason: 'no-remote-blocks' };
    }

    logger.info('[Migration] REMOTE blocks detected in database.bin; converting to inline format');

    // Pre-migration backup so a botched migration can be rolled back manually.
    // Use a dedicated prefix — `database/dbbackup-` is on a 20-snapshot rotation
    // whose timestamp parser would assign this entry ts=0 (because of the
    // non-numeric suffix), making it the first to evict. The migration safety
    // net must outlive ordinary backup churn.
    const backupKey = `migration-backup/pre-remote-fix-${Date.now()}.bin`;
    kvCopyValue('database/database.bin', backupKey);

    const dbObj = await decodeAuthoritativeDatabase(raw);

    const reEncoded = encodeRisuSaveLegacy(dbObj, 'compression');

    // Single transaction so swap + marker move together.
    // remotes/ files are intentionally NOT deleted here: pre-migration
    // dbbackup-* snapshots and the migration-backup we just wrote both
    // only carry database.bin (kvCopyValue is single-key). If a user later
    // restores one of those snapshots — which holds REMOTE pointers —
    // resolveRemote needs the remotes/<id>.local.bin payloads to still
    // exist, otherwise every REMOTE-pointed character drops on the next
    // decode and the backup is effectively dead. The orphans don't grow
    // (NodeOnly's disableRemoteSaving = true on writes), so leaving them
    // costs a few MB of disk for full backup recoverability.
    sqliteDb.transaction(() => {
        kvSet('database/database.bin', Buffer.from(reEncoded));
        markRemoteMigrationDone();
    })();

    // Reset in-memory caches whose contents were derived from the pre-migration
    // bytes — next reader recomputes from the migrated database.bin.
    invalidateDbCache();
    dbEtag = null;

    const characterCount = Array.isArray(dbObj.characters) ? dbObj.characters.length : 0;
    logger.info(`[Migration] Remote-block migration complete. Inlined ${characterCount} character(s); pre-migration backup at ${backupKey}`);
    return { ran: true, characterCount, backupKey };
}

/**
 * Prove that the live monolith is decodable before any ordinary boot
 * migration is allowed to publish markers, backups, or rewritten rows.
 *
 * This deliberately has no storage writes. REMOTE-backed legacy databases
 * are resolved from their existing rows so a valid pre-migration source does
 * not get mistaken for corruption, while optimized plugin JSON receives the
 * same strict validation used by ingest.
 */
async function preflightBootDatabase(raw) {
    const decoded = await decodeAuthoritativeDatabase(raw);
    const normalized = normalizeJSON(decoded);
    validateDatabaseShape(normalized);
    snapshotOptimizedPluginStorageFields(decoded);
}

async function ingestDatabase(raw, {
    createBackup = false,
    skipLiveRemoteMigration = false,
} = {}) {
    const migration = skipLiveRemoteMigration
        ? { ran: false }
        : await migrateRemoteBlocksIfNeeded();
    const source = migration.ran ? kvGet('database/database.bin') : raw;
    if (Buffer.isBuffer(source) || source instanceof Uint8Array) {
        const inspection = await inspectRisuSaveSource(source);
        if (await shouldStreamRisuSave(source, { inspection })) {
            const result = await ingestDatabaseStreaming(source, { inspection });
            if (createBackup) {
                await createBackupAndRotate({ storageAlreadyExclusive: importInProgress });
            }
            return result;
        }
    }
    const decoded = Buffer.isBuffer(source) || source instanceof Uint8Array
        ? await decodeAuthoritativeDatabase(source)
        : source;
    const dbObj = normalizeJSON(decoded);
    validateDatabaseShape(dbObj);
    const strictPluginStorage = snapshotOptimizedPluginStorageFields(decoded);
    if (strictPluginStorage) {
        dbObj.pluginCustomStorage = strictPluginStorage.values;
        if (strictPluginStorage.hasMeta) dbObj.pluginStorageMeta = strictPluginStorage.meta;
    }

    // Plugin rows commit before chat ingestion rewrites database.bin. If the
    // process stops between those steps, the inline monolith remains the
    // authoritative copy and a later pass overwrites any partial/stale rows.
    externalizePluginStorageIfNeeded(dbObj);

    const result = await chatRowStore.ingestFullDatabase(dbObj, {
        restoreColdStorageCharacters: (dbObj) => {
            const coldRestoreResult = restoreColdStorageCharactersInDb(dbObj);
            if (coldRestoreResult.failed > 0) {
                logger.error(`[ColdStorage] ${coldRestoreResult.failed} character(s) could not be restored and were converted to safe blank characters. Cold storage KV data is preserved.`);
                for (const name of coldRestoreResult.failedNames) {
                    logger.error(`[ColdStorage]   - "${name}"`);
                }
            }
            return coldRestoreResult;
        },
    });
    logDuplicateCharacterIdReassignments(result);
    if (createBackup) {
        await createBackupAndRotate({ storageAlreadyExclusive: importInProgress });
    }
    return result;
}

function logColdStorageRestoreFailures(result) {
    if (!result || result.failed <= 0) return;
    logger.error(`[ColdStorage] ${result.failed} character(s) could not be restored and were converted to safe blank characters. Cold storage KV data is preserved.`);
    for (const name of result.failedNames) {
        logger.error(`[ColdStorage]   - "${name}"`);
    }
}

function logDuplicateCharacterIdReassignments(result) {
    const count = result?.stats?.reassignedDuplicateChaIds ?? 0;
    if (count > 0) {
        logger.warn(`[ChatRows] Reassigned ${count} duplicate character ID(s) before externalizing chats`);
    }
}

async function ingestDatabaseStreaming(source, {
    inspection = null,
    shouldAbort,
    signal,
    maxDecodedBytes,
    diskHeadroomBytes,
    availableDiskBytes,
    onDecodedChunk,
} = {}) {
    const streamedPluginValueKeys = new Set();
    const streamedPluginMetaKeys = new Set();
    const streamedPluginRawKeys = new Set();
    let foldedPluginStorage = false;
    const result = await chatRowStore.ingestStreamingDatabase(source, {
        inspection: inspection ?? await inspectRisuSaveSource(source),
        tempDir: requireDatabaseSpoolDirSync(),
        shouldAbort,
        signal,
        maxDecodedBytes,
        diskHeadroomBytes,
        availableDiskBytes,
        onDecodedChunk,
        onDecodedSourcePrepared: () => waitAtStreamLoadTestGate('decoded'),
        onTraversalProgress: () => waitAtStreamLoadTestGate('traversal'),
        onPluginStorageFolded: async () => {
            foldedPluginStorage = true;
            // The marker is decoded before the walker emits any target rows.
            // Prove the current publication only at that point: an unmarked
            // import must not read large live ownership bodies at all. The
            // async proof releases each decoded row before yielding, remains
            // cancellable, and completes before a same-key target can replace
            // the old bytes that established deletion authority.
            const priorOwnership = await proveStrictPluginStorageOwnershipBoundary({
                shouldAbort,
            });
            await waitAtSnapshotRestoreTestGate(shouldAbort);
            throwIfStreamingRestoreAborted(shouldAbort);
            deleteOwnedPluginStorageRows(priorOwnership);
        },
        onPluginStorageEntry: ({ field, key, value }) => {
            const prefix = field === 'pluginStorageMeta'
                ? PLUGIN_SAVE_META_PREFIX
                : PLUGIN_SAVE_PREFIX;
            const storageKey = encodeValidatedPluginStorageKey(key, prefix);
            kvSet(storageKey, serializePluginStorageRow(storageKey, value));
            (prefix === PLUGIN_SAVE_META_PREFIX
                ? streamedPluginMetaKeys
                : streamedPluginValueKeys).add(storageKey);
            streamedPluginRawKeys.add(key);
        },
        onPluginStorageComplete: ({ dbObj, pluginStats }) => {
            if (!foldedPluginStorage && !pluginStats?.changed) return;
            const generation = foldedPluginStorage
                && typeof dbObj[PLUGIN_STORAGE_GENERATION_FIELD] === 'string'
                && dbObj[PLUGIN_STORAGE_GENERATION_FIELD].length > 0
                ? dbObj[PLUGIN_STORAGE_GENERATION_FIELD]
                : nodeCrypto.randomUUID();
            dbObj[PLUGIN_STORAGE_GENERATION_FIELD] = generation;
            writePluginStorageManifest(createPluginStorageManifest(
                generation,
                streamedPluginValueKeys,
                streamedPluginMetaKeys,
                mergePluginStorageKeyMappings(
                    null,
                    streamedPluginRawKeys,
                    streamedPluginValueKeys,
                    streamedPluginMetaKeys,
                ),
            ));
        },
        onMcpToolCallsFolded: () => {
            kvDelPrefix(MCP_TOOL_CALL_CACHE_PREFIX);
        },
        onMcpToolCallEntry: ({ key, callId, value }) => {
            if (!parseMcpToolCallStorageKey(key)) {
                throw new TypeError(`Invalid remembered MCP tool-call key: ${key}`);
            }
            kvSet(key, serializeMcpToolCallPayload(key, callId, value));
        },
        restoreColdStorageCharacters: (dbObj) => {
            const coldRestoreResult = restoreColdStorageCharactersInDb(dbObj);
            logColdStorageRestoreFailures(coldRestoreResult);
            return coldRestoreResult;
        },
    });
    logDuplicateCharacterIdReassignments(result);
    return result;
}

function normalizeDecodedDatabaseForRead(rawDecoded) {
    const decoded = normalizeJSON(rawDecoded);
    validateDatabaseShape(decoded);
    const strictPluginStorage = snapshotOptimizedPluginStorageFields(rawDecoded);
    if (strictPluginStorage) {
        decoded.pluginCustomStorage = strictPluginStorage.values;
        if (strictPluginStorage.hasMeta) decoded.pluginStorageMeta = strictPluginStorage.meta;
    }
    return decoded;
}

async function loadStrippedDatabase(raw, source) {
    const inspection = await inspectRisuSaveSource(raw);
    if (kvGet(CHAT_EXTERNALIZATION_MARKER_KEY) === null
        && await shouldStreamRisuSave(raw, { inspection })) {
        logger.warn(`[${source}] Large supported database.bin found; externalizing through the streaming ingest path`);
        return (await ingestDatabaseStreaming(raw, { inspection })).strippedDb;
    }
    const rawDecoded = await decodeAuthoritativeDatabase(raw);
    const decoded = normalizeDecodedDatabaseForRead(rawDecoded);
    const hasChats = hasChatPayloads(decoded);
    const hasPluginStorage = hasExternalizablePluginStorage(decoded);
    if (!hasChats && !hasPluginStorage) return decoded;
    if (hasChats) {
        logger.warn(`[${source}] Chat payload found in database.bin; externalizing defensively`);
    }
    if (hasPluginStorage) {
        logger.warn(`[${source}] Folded plugin storage found in database.bin; externalizing defensively`);
    }
    return (await ingestDatabase(decoded)).strippedDb;
}

async function prepareLiveDatabaseRead(source, { includeFullBlob = true } = {}) {
    for (let attempt = 0; attempt < 3; attempt++) {
        const selectedRevision = kvGetDatabaseRevision();
        if (selectedRevision === null) {
            const retained = dbCache.metadata(DB_HEX_KEY);
            if (retained?.dirty) throw new DatabaseCacheRevisionConflict();
            dbCache.getForRevision(DB_HEX_KEY, null);
            dbEtag = null;
            return null;
        }

        let cacheStatus = 'hit';
        let strippedDatabase = dbCache.getForRevision(DB_HEX_KEY, selectedRevision);
        if (strippedDatabase) scheduleDbCachePrune();
        if (!strippedDatabase) {
            const retained = dbCache.metadata(DB_HEX_KEY);
            if (retained?.dirty) {
                throw new Error('Cannot replace acknowledged dirty database cache state during a read');
            }

            const raw = await kvGetAsync(DB_BLOB_KEY);
            if (raw === null) continue;
            // The storage queue remains held across the async single-flight read.
            // If defensive migration or an out-of-process writer changes the row
            // during later decoding, retry against the newly authoritative revision.
            if (kvGetDatabaseRevision() !== selectedRevision) continue;
            strippedDatabase = await loadStrippedDatabase(raw, source);
            if (kvGetDatabaseRevision() !== selectedRevision) continue;
            replaceDbCacheValue(DB_HEX_KEY, strippedDatabase, {
                revision: selectedRevision,
                estimatedBytes: raw.length,
                dirty: false,
            });
            cacheStatus = 'miss';
        }

        let fullBlob;
        let etag;
        if (includeFullBlob) {
            const prepared = prepareDatabaseReadPayload(strippedDatabase);
            fullBlob = prepared.fullBlob;
            etag = prepared.etag;
            if (dbCache.has(DB_HEX_KEY)) seedDbCacheEtag(DB_HEX_KEY, etag);
        } else if (dbCache.has(DB_HEX_KEY)) {
            etag = getDbCacheEtag(DB_HEX_KEY);
        } else {
            etag = computeDatabaseEtagFromObject(strippedDatabase);
        }
        dbEtag = etag;
        return { strippedDatabase, fullBlob, etag, cacheStatus, revision: selectedRevision };
    }
    throw new Error('Database changed repeatedly while preparing an authoritative read');
}

async function loadPatchCache(filePath, decodedKey) {
    if (decodedKey !== DB_BLOB_KEY) {
        const cached = getDbCacheValue(filePath);
        if (cached) return cached;
        const fileContent = kvGet(decodedKey);
        const decoded = fileContent
            ? normalizeJSON(await decodeRisuSave(fileContent))
            : {};
        replaceDbCacheValue(filePath, decoded, {
            revision: null,
            estimatedBytes: fileContent?.length ?? 0,
            dirty: false,
        });
        return decoded;
    }

    for (let attempt = 0; attempt < 3; attempt++) {
        const revision = kvGetDatabaseRevision();
        const retained = dbCache.metadata(filePath);
        if (retained?.dirty) {
            if (retained.revision !== revision) {
                releaseDbCacheCanonicalEncoding(filePath, 'external-revision-conflict');
                throw new DatabaseCacheRevisionConflict();
            }
            return peekDbCacheValue(filePath);
        }
        const cached = dbCache.getForRevision(filePath, revision);
        if (cached) {
            scheduleDbCachePrune();
            return cached;
        }

        const fileContent = kvGet(decodedKey);
        if (fileContent === null) {
            if (kvGetDatabaseRevision() !== revision) continue;
            const empty = {};
            replaceDbCacheValue(filePath, empty, {
                revision: null,
                estimatedBytes: 0,
                dirty: false,
            });
            return empty;
        }
        if (kvGetDatabaseRevision() !== revision) continue;
        const decoded = await loadStrippedDatabase(fileContent, 'Patch');
        if (kvGetDatabaseRevision() !== revision) continue;
        replaceDbCacheValue(filePath, decoded, {
            revision,
            estimatedBytes: fileContent.length,
            dirty: false,
        });
        return decoded;
    }
    throw new Error('Database changed repeatedly while preparing a patch');
}

async function migrateChatsToRowsIfNeeded() {
    if (kvGet(CHAT_EXTERNALIZATION_MARKER_KEY) !== null) return;
    const raw = kvGet('database/database.bin');
    if (!raw) {
        kvSet(CHAT_EXTERNALIZATION_MARKER_KEY, CHAT_EXTERNALIZATION_MARKER_VALUE);
        logger.info('[Migration] Chat externalization marker initialized (no database present)');
        return;
    }

    const backupKey = `migration-backup/pre-chat-externalization-${Date.now()}.bin`;
    kvCopyValue('database/database.bin', backupKey);
    logger.info(`[Migration] Externalizing chats from database.bin; safety backup at ${backupKey}`);
    const result = await ingestDatabase(raw);
    logger.info(
        `[Migration] Chat externalization complete: ${result.stats.chats} chat row(s), `
        + `${result.stats.deletedStale} stale row(s) removed`
    );
}

async function migrateCharacterDefaultsIfNeeded() {
    if (kvGet(CHARACTER_DEFAULTS_MARKER_KEY) !== null) return;
    const raw = kvGet('database/database.bin');
    if (!raw) {
        kvSet(CHARACTER_DEFAULTS_MARKER_KEY, CHARACTER_DEFAULTS_MARKER_VALUE);
        logger.info('[Migration] Character-defaults marker initialized (no database present)');
        return;
    }

    const rawDecoded = await decodeAuthoritativeDatabase(raw);
    const strictPluginStorage = snapshotOptimizedPluginStorageFields(rawDecoded);
    const decoded = normalizeJSON(rawDecoded);
    validateDatabaseShape(decoded);
    if (strictPluginStorage) {
        decoded.pluginCustomStorage = strictPluginStorage.values;
        if (strictPluginStorage.hasMeta) decoded.pluginStorageMeta = strictPluginStorage.meta;
    }
    applyDatabaseCharacterDefaults(decoded, nodeCrypto.randomUUID);
    const reEncoded = Buffer.from(encodeRisuSaveLegacy(decoded));
    const backupKey = `migration-backup/pre-character-defaults-${Date.now()}.bin`;

    sqliteDb.transaction(() => {
        kvCopyValue('database/database.bin', backupKey);
        kvSet('database/database.bin', reEncoded);
        kvSet(CHARACTER_DEFAULTS_MARKER_KEY, CHARACTER_DEFAULTS_MARKER_VALUE);
    })();

    // The authoritative re-encode can change RisuSave framing, raw ETag, and
    // stored size while preserving the logical database. Readers revalidate
    // those byte-derived values on their next request.
    invalidateDbCache();
    dbEtag = null;
    logger.info(`[Migration] Character defaults persisted; safety backup at ${backupKey}`);
}

// Stub metadata fields a JSON Patch may legitimately touch on a `chats[i]`
// entry. Anything else is a chat-internal field — those live in chat rows, not
// in dbCache, and should never appear in a /api/patch payload. Keep in
// sync with chatToStub on both server and client.
const STUB_METADATA_FIELDS = new Set(['id', 'name', '_stub', 'lastDate', 'folderId', 'modules']);

// Only add/replace/remove are produced by the legitimate patcher. move/copy
// could alias _stub or other chat-internal fields through `from`, bypassing
// the path-based field allowlist. Reject those op types outright on chat
// paths. test ops can also reveal/manipulate state; deny for symmetry.
const ALLOWED_CHAT_OP_TYPES = new Set(['add', 'replace', 'remove']);

const CHAT_FIELD_PATH_RE = /^\/characters\/\d+\/chats\/\d+\/([^/]+)/;

/**
 * Detect JSON Patch ops that mutate chat-internal fields (anything beyond
 * STUB_METADATA_FIELDS). Such ops are the loss vector: applying them to
 * dbCache leaves a metadata-only chat without `_stub`, which then gets
 * persisted as-is.
 *
 * Whole-chat ops (path = `/characters/N/chats/M` or `/characters/N/chats`)
 * are allowed — those replace/add/remove chat slots wholesale and the
 * persist guard takes care of validating the resulting state.
 *
 * The `_stub` field gets stricter treatment than other allowed fields: only
 * `add`/`replace` with literal value `true` is permitted. Any op that could
 * remove the flag or set it to a falsy value is itself the loss mechanism
 * so it must be blocked at the patch boundary, not just at persistence.
 *
 * `move`/`copy` ops are rejected wholesale on chat-internal paths because
 * the field-name allowlist on `path` alone can't catch a `from` that points
 * at `_stub` or another chat-internal field. Both `path` and `from` are
 * checked when present.
 */
function findChatInternalFieldOps(patch) {
    if (!Array.isArray(patch)) return [];
    const violations = [];
    for (const op of patch) {
        if (!op || typeof op !== 'object' || typeof op.path !== 'string') continue;

        const pathMatch = op.path.match(CHAT_FIELD_PATH_RE);
        const fromMatch = typeof op.from === 'string' ? op.from.match(CHAT_FIELD_PATH_RE) : null;
        if (!pathMatch && !fromMatch) continue;

        if (!ALLOWED_CHAT_OP_TYPES.has(op.op)) {
            violations.push({
                op: op.op,
                path: op.path,
                field: (pathMatch && pathMatch[1]) || (fromMatch && fromMatch[1]) || '',
                reason: 'disallowed op type on chat field',
            });
            continue;
        }

        if (pathMatch) {
            const field = pathMatch[1];
            if (!STUB_METADATA_FIELDS.has(field)) {
                violations.push({ op: op.op, path: op.path, field });
                continue;
            }
            if (field === '_stub') {
                if (op.op === 'remove') {
                    violations.push({ op: op.op, path: op.path, field, reason: 'remove _stub' });
                } else if ((op.op === 'add' || op.op === 'replace') && op.value !== true) {
                    violations.push({ op: op.op, path: op.path, field, reason: 'non-true _stub value' });
                }
            }
        }
    }
    return violations;
}

function duplicateChatIdSample(duplicates) {
    return duplicates.slice(0, 3).map(duplicate => {
        const characterLabel = duplicate.chaId ?? `character[${duplicate.characterIndex}]`;
        return `${characterLabel}/${duplicate.chatId}`;
    }).join(', ');
}

function diffReferencedChatRowKeys(oldStrippedDb, newStrippedDb) {
    const oldKeys = chatRowStore.referencedChatRowKeys(oldStrippedDb);
    const newKeys = chatRowStore.referencedChatRowKeys(newStrippedDb);
    let changed = oldKeys.size !== newKeys.size;
    for (const key of oldKeys) {
        if (!newKeys.has(key)) changed = true;
    }
    for (const key of newKeys) {
        if (!oldKeys.has(key)) changed = true;
    }
    return { changed, newKeys, oldKeys };
}

function trackPendingChatRowDeletions({ oldKeys, newKeys }) {
    for (const key of oldKeys) {
        if (!newKeys.has(key)) pendingChatRowDeletions.add(key);
    }
    for (const key of newKeys) {
        pendingChatRowDeletions.delete(key);
    }
}

async function captureChatDeletionPreImages(chatRowKeys) {
    for (const key of chatRowKeys) {
        const identity = chatRowStore.parseChatRowKey(key);
        if (!identity) {
            throw new Error(`Could not decode pending chat-row deletion key: ${key}`);
        }
        const result = await chatBackupStore.captureChatPreImage({
            ...identity,
            reason: 'delete-chat',
            force: true,
            required: true,
        });
        if (result !== 'captured' && result !== 'skipped-no-row') {
            throw new Error(
                `Required chat deletion pre-image was not captured for ${identity.chaId}/${identity.chatId}: ${result}`
            );
        }
    }
}

/**
 * Persist the stubs-only patch cache.
 */
async function persistDbCache(filePath, decodedKey) {
    const generation = dbDerivedValueMemo.generation(filePath);
    let succeeded = false;
    try {
        await persistDbCacheGeneration(filePath, decodedKey, generation);
        succeeded = true;
    } finally {
        if (decodedKey === DB_BLOB_KEY) {
            releaseDbCacheCanonicalEncoding(
                filePath,
                succeeded ? 'persist-success' : 'persist-error',
                generation,
            );
        }
    }
}

function dbCachePersistenceOptions({
    filePath,
    decodedKey,
    generation,
    cachedDb,
    cacheMetadata,
}) {
    const assertCurrent = () => {
        if (decodedKey !== DB_BLOB_KEY) return;
        if (cacheMetadata?.revision !== kvGetDatabaseRevision()
            || dbDerivedValueMemo.generation(filePath) !== generation
            || peekDbCacheValue(filePath) !== cachedDb
            || dbCache.metadata(filePath)?.revision !== cacheMetadata?.revision) {
            throw new DatabaseCacheRevisionConflict();
        }
    };
    return {
        cachedDb,
        decodedKey,
        assertCurrent,
        findDuplicateChatIds,
        preparePluginStorageExternalization,
        retainCanonicalEncoding: () => retainDbCacheCanonicalEncoding(filePath),
        encodeRisuSaveLegacy,
        sqliteDb,
        writePluginStorageRows,
        writePluginStorageManifest,
        kvSet,
        kvDel,
        kvGetDatabaseRevision,
    };
}

function handleDbCachePersistenceGuard(error) {
    if (!(error instanceof DbCachePersistenceGuardError)) return;
    recordPersistFailure(error, error.guard === 'stub-flag-loss'
        ? 'persistDbCache:stub-flag-loss'
        : 'persistDbCache:duplicate-chat-ids');
    invalidateDbCache();
}

async function persistDbCacheGeneration(filePath, decodedKey, generation) {
    const cachedDb = peekDbCacheValue(filePath);
    if (!cachedDb) return;
    const cacheMetadata = dbCache.metadata(filePath);
    const persistenceOptions = dbCachePersistenceOptions({
        filePath,
        decodedKey,
        generation,
        cachedDb,
        cacheMetadata,
    });
    let prepared;
    try {
        prepared = prepareDbCachePersistence(persistenceOptions);
    } catch (error) {
        handleDbCachePersistenceGuard(error);
        throw error;
    }
    const referencedChatRows = decodedKey === 'database/database.bin'
        ? chatRowStore.referencedChatRowKeys(prepared.strippedDb)
        : new Set();
    const chatRowsToDelete = decodedKey === 'database/database.bin'
        ? [...pendingChatRowDeletions].filter(key => !referencedChatRows.has(key))
        : [];
    await captureChatDeletionPreImages(chatRowsToDelete);
    const committed = commitPreparedDbCachePersistence({
        ...persistenceOptions,
        prepared,
        chatRowsToDelete,
    });
    if (decodedKey === 'database/database.bin') {
        replaceDbCacheValue(filePath, committed.strippedDb, {
            revision: committed.committedRevision,
            estimatedBytes: committed.data.length,
            dirty: false,
            preserveSegmentMemo: true,
        });
        pendingChatRowDeletions.clear();
    }
}

function shouldCompress(req, res) {
    // Proxy/hub-proxy: pass through external responses without compression.
    // Original upstream server has no compression middleware at all,
    // so proxy responses were never compressed in the first place.
    const url = req.originalUrl || req.url;
    if (url.startsWith('/proxy')
        || url.startsWith('/hub-proxy')
        || url.startsWith('/api/backup/export')
        || url.startsWith('/api/backup/server/download/')
        || url.startsWith('/api/plugin-storage/state/raw')) {
        return false;
    }

    const contentType = String(res.getHeader('Content-Type') || '').toLowerCase();
    if (contentType.includes('text/event-stream')) {
        return false;
    }
    // NDJSON endpoints (backup import/restore, inlay bulk compression) emit
    // small per-line events and rely on real-time flushes — keepalive
    // heartbeats in particular must reach reverse proxies before their
    // response timeout fires. gzip would buffer those lines until enough
    // bytes accumulated for an efficient compression block, defeating the
    // 502-avoidance the streaming endpoints were built for. compressible's
    // mime-db happens not to list application/x-ndjson today (so this is
    // a no-op in practice) but a future dep upgrade could flip it on.
    if (contentType.includes('application/x-ndjson')) {
        return false;
    }
    // Already-compressed media formats: gzip adds CPU cost with ~0% size gain
    if (contentType.startsWith('image/') || contentType.startsWith('video/') || contentType.startsWith('audio/')) {
        return false;
    }
    if (contentType.includes('application/octet-stream')) {
        return true;
    }
    return compression.filter(req, res);
}

app.use(compression({
    filter: shouldCompress,
}));
if (isRequestTracingEnabled()) {
    console.warn(
        '[RequestTrace] Request tracing is active because TRACE_REQUEST_FOR_DEBUG=true. '
        + 'Trace files may contain sensitive data and request tracing is not recommended '
        + 'outside debugging sessions.',
    );
    const requestTracer = createRequestTracer({
        traceDir: path.join(process.cwd(), 'save', 'trace'),
        maxTraces: 500,
        isStreamingRequest: req => Boolean(req[ADMITTED_INGRESS_SPOOL])
            || isStreamedIngress(req),
        onError: (context, error) => logger.warn(
            `[RequestTrace] Failed while ${context}: ${error?.stack || error}`,
        ),
    });
    app.use(requestTracer.middleware);
}
// Vite 산출물은 해시 파일명이므로 /assets는 장기 캐시 안전
app.use('/assets', express.static(path.join(process.cwd(), 'dist/assets'), {
    maxAge: '1y',
    immutable: true,
}));
app.use(express.static(path.join(process.cwd(), 'dist'), {index: false, maxAge: 0}));
const bufferedIngressLimits = createBufferedIngressLimits({
    pluginValueMaxBytes: PLUGIN_VALUE_MAX_BYTES,
    pluginStorageMaxBytes: PLUGIN_STORAGE_MAX_BYTES,
    pluginBatchMaxBytes: PLUGIN_STORAGE_BATCH_MAX_BODY_BYTES,
});
const bufferedIngressBudget = createInFlightByteBudget(bufferedIngressLimits.global);
app.use((req, res, next) => {
    if (req.path.startsWith('/api/')) {
        res.setHeader(WRITER_EPOCH_HEADER, sessionLock.epoch());
    }
    next();
});
app.use(createBufferedIngressMiddleware({
    resolvePolicy: createRoutePolicyResolver(bufferedIngressLimits),
    budget: bufferedIngressBudget,
    authenticate: (req, res, allowExpired) => checkAuth(
        req,
        res,
        false,
        { allowExpired },
    ),
    authenticateCookie: (req, res) => checkSessionCookieAuth(req, res),
    writerState: (req) => sessionLock.peek(
        typeof req.headers['x-session-id'] === 'string'
            ? req.headers['x-session-id']
            : '',
        typeof req.headers[WRITER_EPOCH_HEADER] === 'string'
            ? req.headers[WRITER_EPOCH_HEADER]
            : '',
    ),
    expectedClientBuild,
}));
// Revalidate or lazily establish the process-pinned installation spool on every
// request. This is intentionally non-blocking for routes that do not need it;
// spool consumers retain their retryable failure contracts, while a repaired
// custom root becomes usable without restarting the process.
app.use((_req, _res, next) => {
    ensureDatabaseSpoolDirSync();
    next();
});
app.use(createAdmittedIngressSpoolMiddleware({
    policySymbol: BUFFERED_INGRESS_POLICY,
    spoolDir: () => requireDatabaseSpoolDirSync(),
    disabled: process.env.NODE_ENV === 'test'
        && process.env.POCKETRISU_TEST_DISABLE_ADMITTED_SPOOL === '1',
    globalBudgetBytes: bufferedIngressLimits.global,
}));
app.use((req, res, next) => {
    if (req.path === '/api/db/read-cached') return next();
    if (req[ADMITTED_INGRESS_SPOOL]) return next();
    const policy = req[BUFFERED_INGRESS_POLICY];
    const parser = express.json({
        type: ['application/json', CHAT_DELTA_CONTENT_TYPE],
        limit: policy?.bodyKind === 'json'
            ? policy.maxBytes
            : bufferedIngressLimits.json,
    });
    return parser(req, res, next);
});
app.use((req, res, next) => {
    // These endpoints consume the request stream directly and must never be
    // pre-buffered by the generic octet-stream parser.
    if (req[ADMITTED_INGRESS_SPOOL]) return next();
    if (isStreamedIngress(req)) return next();
    const isPluginStorageBatch = req.path === '/api/plugin-storage/batch';
    const isBufferedPluginMutationSet = req.path === '/api/plugin-storage/mutate'
        && req.headers['x-plugin-storage-operation'] === 'set';
    const isPluginManifestMutation = req.path === '/api/plugin-storage/mutate'
        && req.headers['x-plugin-storage-operation'] === undefined;
    let pluginLegacyWrite = false;
    if (req.path === '/api/write') {
        const encodedPath = req.headers['file-path'];
        if (typeof encodedPath === 'string' && /^[0-9a-fA-F]+$/.test(encodedPath)) {
            pluginLegacyWrite = Buffer.from(encodedPath, 'hex')
                .toString('utf-8')
                .startsWith(PLUGIN_SAVE_PREFIX);
        }
    }
    const admissionPolicy = req[BUFFERED_INGRESS_POLICY];
    const parser = express.raw({
        type: 'application/octet-stream',
        limit: admissionPolicy?.bodyKind === 'raw'
            ? admissionPolicy.maxBytes
            : isPluginStorageBatch
            ? PLUGIN_STORAGE_BATCH_MAX_BODY_BYTES
            : (pluginLegacyWrite || isBufferedPluginMutationSet)
                ? PLUGIN_VALUE_MAX_BYTES
                : isPluginManifestMutation
                    ? PLUGIN_STORAGE_MAX_BYTES
                : '2gb',
    });
    return parser(req, res, (error) => {
        if (!error) return next();
        const tooLarge = error.type === 'entity.too.large';
        if (pluginLegacyWrite && tooLarge) {
            return res.status(413).json({
                error: `Plugin value exceeds the ${PLUGIN_VALUE_MAX_BYTES}-byte per-value limit. Split the value into smaller records.`,
                code: 'PLUGIN_VALUE_TOO_LARGE',
                limit: PLUGIN_VALUE_MAX_BYTES,
                retryable: false,
                commitOutcome: 'not-committed',
                commitOutcomeUnknown: false,
            });
        }
        if (isBufferedPluginMutationSet && tooLarge) {
            return res.status(413).json({
                success: false,
                outcome: 'not-committed',
                operation: 'set',
                error: `Plugin value exceeds the ${PLUGIN_VALUE_MAX_BYTES}-byte per-value limit. Split the value into smaller records.`,
                code: 'PLUGIN_VALUE_TOO_LARGE',
                limit: PLUGIN_VALUE_MAX_BYTES,
                actual: Number(req.headers['content-length']) || PLUGIN_VALUE_MAX_BYTES + 1,
                retryable: false,
            });
        }
        if (!isPluginStorageBatch) return next(error);
        return res.status(tooLarge ? 413 : 400).json({
            success: false,
            outcome: 'not-committed',
            operation: 'batch',
            error: tooLarge
                ? 'Plugin storage batch body exceeds the 16 MiB limit.'
                : 'Plugin storage batch body could not be read.',
            code: 'INVALID_PLUGIN_STORAGE_BATCH',
            retryable: false,
        });
    });
});
app.use((req, res, next) => {
    const policy = req[BUFFERED_INGRESS_POLICY];
    return express.text({
        limit: policy?.bodyKind === 'text'
            ? policy.maxBytes
            : bufferedIngressLimits.json,
    })(req, res, next);
});
const { pipeline, finished } = require('stream/promises')
const sslPath = path.join(process.cwd(), 'server/node/ssl/certificate');

let password = ''

// Ensure /save/ exists for password file and migration source
const savePath = path.join(process.cwd(), "save")
if(!existsSync(savePath)){
    mkdirSync(savePath)
}

// Keep analytics identity valid, but do not use it for filesystem ownership:
// it is routinely copied with operator templates and cloned save directories.
const instanceIdPath = path.join(savePath, '__instance_id')
const instanceId = readOrCreatePersistentUuid(instanceIdPath)
// The spool owner is a separate, atomically initialized installation token.
// Restarts reuse it for own-orphan cleanup; copied analytics ids do not collide.
let databaseSpoolOwnerId = readOrCreatePersistentUuid(
    path.join(savePath, SPOOL_OWNER_ID_FILENAME),
)

const DATABASE_SPOOL_FILE_PREFIX = '.database-risudat-';
const BACKUP_IMPORT_SPOOL_FILE_PREFIX = '.backup-import-';
const BACKUP_ENTRY_STAGE_PREFIX = '.backup-entry-stage-';
const PARTIAL_EXPORT_JOB_PREFIX = '.partial-export-';
const FULL_EXPORT_PIN_PREFIX = '.full-export-';
const SERVER_BACKUP_TEMP_PREFIX = '.risu-backup-save-';
const DRAFT_PREFIX = 'drafts/';
const PLUGIN_VALUE_SPOOL_FILE_PREFIX = '.plugin-value-';
const PLUGIN_BATCH_VALUE_SPOOL_FILE_PREFIX = '.plugin-batch-value-';
const PLUGIN_RECOVERY_DOWNLOAD_SPOOL_FILE_PREFIX = '.plugin-recovery-download-';
const PLUGIN_TRANSITION_STAGE_PREFIX = '.plugin-transition-stage-';
const PLUGIN_TRANSITION_STAGE_MAX_AGE_MS = 24 * 60 * 60 * 1000;
// A mode transition must accept every row that optimized storage itself can
// legally publish. Preserve the historical 32 MiB staging floor when an
// operator configures a smaller publication quota; the atomic finalize step
// remains authoritative for that quota.
const PLUGIN_TRANSITION_MAX_ROW_BYTES = Math.max(
    32 * 1024 * 1024,
    PLUGIN_VALUE_MAX_BYTES,
);
// POCKETRISU_SPOOL_DIR relocates the shared spool root. Each installation gets
// a stable child namespace so boot cleanup can prove ownership without an
// unsafe age heuristic or touching another live instance's files.
const configuredDatabaseSpoolDir = String(process.env.POCKETRISU_SPOOL_DIR ?? '').trim();
const databaseSpoolRootDir = configuredDatabaseSpoolDir
    ? path.resolve(configuredDatabaseSpoolDir)
    : path.join(savePath, '.spool');
let databaseSpoolOwnedPath = resolveOwnedSpoolDir(databaseSpoolRootDir, databaseSpoolOwnerId);
let databaseSpoolDir = null;
// Filesystem asset pins stay on the save volume so an export can take one
// coherent SQLite/filesystem cut without relying on cross-device links.
const partialExportSpoolDir = path.join(savePath, '.partial-export-spool');
let databaseSpoolReady = false;
let databaseSpoolNamespaceClaimed = false;
let databaseSpoolHandle = null;
const pluginTransitionStageDir = path.join(savePath, '.plugin-transition-staging');

function getDatabaseSpoolDir() {
    return databaseSpoolDir;
}

function getPluginTransitionStageDir() {
    return pluginTransitionStageDir;
}

function ensureDatabaseSpoolDirSync() {
    try {
        if (databaseSpoolHandle) {
            if (!fsSync.fstatSync(databaseSpoolHandle.descriptor).isDirectory()) {
                throw new Error('Pinned database spool descriptor is no longer a directory');
            }
            databaseSpoolReady = true;
            return true;
        }
        if (!databaseSpoolNamespaceClaimed) {
            const claimed = claimOwnedSpoolNamespaceSync(savePath, databaseSpoolRootDir);
            databaseSpoolOwnerId = claimed.ownerId;
            databaseSpoolOwnedPath = claimed.spoolDir;
            databaseSpoolNamespaceClaimed = true;
        }
        ensureOwnedSpoolDirSync(databaseSpoolRootDir, databaseSpoolOwnedPath);
        const opened = openPinnedOwnedSpoolDirSync(databaseSpoolOwnedPath);
        if (!opened) {
            const error = new Error('Descriptor-relative database spool access is unavailable');
            error.code = 'ENOTSUP';
            throw error;
        }
        databaseSpoolHandle = opened;
        databaseSpoolDir = opened.pinnedPath;
        databaseSpoolReady = true;
        return true;
    } catch {
        databaseSpoolReady = false;
        return false;
    }
}
function requireDatabaseSpoolDirSync() {
    if (ensureDatabaseSpoolDirSync()) return databaseSpoolDir;
    const error = new Error(`The configured database spool is unavailable: ${databaseSpoolOwnedPath}`);
    error.code = 'ENOENT';
    throw error;
}
try {
    const claimed = claimOwnedSpoolNamespaceSync(savePath, databaseSpoolRootDir);
    databaseSpoolOwnerId = claimed.ownerId;
    databaseSpoolOwnedPath = claimed.spoolDir;
    databaseSpoolNamespaceClaimed = true;
} catch (error) {
    databaseSpoolReady = false;
    logger.error(`[Backup] Could not claim database spool namespace ${databaseSpoolOwnedPath}:`, error);
}
try {
    mkdirSync(partialExportSpoolDir, { recursive: true, mode: 0o700 });
} catch (error) {
    logger.error(`[Backup] Could not create partial export spool directory ${partialExportSpoolDir}:`, error);
}
try {
    mkdirSync(pluginTransitionStageDir, { recursive: true, mode: 0o700 });
} catch (error) {
    logger.error(`[PluginStorage] Could not create transition stage directory ${pluginTransitionStageDir}:`, error);
}
try {
    for (const entry of readdirSync(partialExportSpoolDir, { withFileTypes: true })) {
        if (!entry.name.startsWith(PARTIAL_EXPORT_JOB_PREFIX)
            && !entry.name.startsWith(FULL_EXPORT_PIN_PREFIX)) continue;
        fsSync.rmSync(path.join(partialExportSpoolDir, entry.name), {
            recursive: true,
            force: true,
        });
    }
} catch (error) {
    logger.warn('[Backup] Could not sweep partial export spool directory:', error);
}
function sweepDatabaseSpoolDirectory(sweepDir) {
    for (const entry of readdirSync(sweepDir, { withFileTypes: true })) {
        if (
            entry.name.startsWith(PARTIAL_EXPORT_JOB_PREFIX)
            && entry.name !== path.basename(partialExportSpoolDir)
        ) {
            try {
                fsSync.rmSync(path.join(sweepDir, entry.name), {
                    recursive: true,
                    force: true,
                });
            } catch (error) {
                logger.warn(`[Backup] Could not remove orphaned partial export ${entry.name}:`, error);
            }
            continue;
        }
        if (entry.isDirectory() && entry.name.startsWith(SAVE_FOLDER_IMPORT_STAGE_PREFIX)) {
            try {
                fsSync.rmSync(path.join(sweepDir, entry.name), {
                    recursive: true,
                    force: true,
                });
            } catch (error) {
                logger.warn(`[Backup] Could not remove orphaned save-folder import ${entry.name}:`, error);
            }
            continue;
        }
        if (entry.isDirectory() && entry.name.startsWith(BACKUP_ENTRY_STAGE_PREFIX)) {
            try {
                fsSync.rmSync(path.join(sweepDir, entry.name), {
                    recursive: true,
                    force: true,
                });
            } catch (error) {
                logger.warn(`[Backup] Could not remove orphaned backup-entry stage ${entry.name}:`, error);
            }
            continue;
        }
        if (!entry.isFile() || !(
            entry.name.startsWith(DATABASE_SPOOL_FILE_PREFIX)
            || entry.name.startsWith(BACKUP_IMPORT_SPOOL_FILE_PREFIX)
            || entry.name.startsWith(PLUGIN_VALUE_SPOOL_FILE_PREFIX)
            || entry.name.startsWith(PLUGIN_BATCH_VALUE_SPOOL_FILE_PREFIX)
            || entry.name.startsWith(PLUGIN_RECOVERY_DOWNLOAD_SPOOL_FILE_PREFIX)
            || entry.name.startsWith(ADMITTED_INGRESS_SPOOL_PREFIX)
            || DECODED_SPOOL_FILE_PREFIXES.some(prefix => entry.name.startsWith(prefix))
        )) continue;
        try {
            unlinkSync(path.join(sweepDir, entry.name));
        } catch (error) {
            logger.warn(`[Backup] Could not remove orphaned spool file ${entry.name}:`, error);
        }
    }
    for (const entry of readdirSync(sweepDir, { withFileTypes: true })) {
        if (!entry.isDirectory() || !entry.name.startsWith(ADMITTED_WRITE_STAGE_PREFIX)) {
            continue;
        }
        try {
            fsSync.rmSync(path.join(sweepDir, entry.name), {
                recursive: true,
                force: true,
            });
        } catch (error) {
            logger.warn(`[Backup] Could not remove orphaned admitted-write stage ${entry.name}:`, error);
        }
    }
}

if (databaseSpoolNamespaceClaimed) {
    try {
        withQuarantinedOwnedSpoolDirSync(
            databaseSpoolRootDir,
            databaseSpoolOwnedPath,
            sweepDatabaseSpoolDirectory,
        );
        databaseSpoolReady = ensureDatabaseSpoolDirSync();
    } catch (error) {
        databaseSpoolReady = false;
        logger.warn(`[Backup] Could not sweep database spool directory ${databaseSpoolOwnedPath}:`, error);
    }
}

function pluginTransitionStageMetaPath(transitionId) {
    if (!PLUGIN_STORAGE_UUID_PATTERN.test(transitionId)) {
        throw new TypeError('Plugin transition id must be a canonical UUID');
    }
    return path.join(
        pluginTransitionStageDir,
        `${PLUGIN_TRANSITION_STAGE_PREFIX}${transitionId}.json`,
    );
}

function pluginTransitionStageRowPath(transitionId, index) {
    if (!Number.isSafeInteger(index) || index < 0 || index > 100_000) {
        throw new TypeError('Invalid plugin transition row index');
    }
    return path.join(
        pluginTransitionStageDir,
        `${PLUGIN_TRANSITION_STAGE_PREFIX}${transitionId}-${index}.row`,
    );
}

function readPluginTransitionStage(transitionId) {
    try {
        const value = JSON.parse(readFileSync(
            pluginTransitionStageMetaPath(transitionId),
            'utf-8',
        ));
        if (value?.version !== 1 || value.transitionId !== transitionId) return null;
        return value;
    } catch (error) {
        if (error?.code === 'ENOENT') return null;
        throw error;
    }
}

function writePluginTransitionStage(stage) {
    const metaPath = pluginTransitionStageMetaPath(stage.transitionId);
    const temporaryPath = `${metaPath}.${process.pid}.${nodeCrypto.randomUUID()}.tmp`;
    try {
        writeFileSync(temporaryPath, JSON.stringify(stage), { encoding: 'utf-8', flag: 'wx' });
        const fileDescriptor = openSync(temporaryPath, 'r');
        try {
            fsyncSync(fileDescriptor);
        } finally {
            closeSync(fileDescriptor);
        }
        renameSync(temporaryPath, metaPath);
        fsyncPluginTransitionStageDirectory();
    } catch (error) {
        try { unlinkSync(temporaryPath); } catch {}
        throw error;
    }
}

function fsyncPluginTransitionStageDirectory() {
    // POSIX requires the containing directory to be synced for a newly
    // created or renamed entry to survive power loss. Some platforms reject
    // opening directories; the row/receipt file itself is still synced there.
    let directoryDescriptor = null;
    try {
        directoryDescriptor = openSync(pluginTransitionStageDir, 'r');
        fsyncSync(directoryDescriptor);
    } catch {}
    finally {
        if (directoryDescriptor !== null) closeSync(directoryDescriptor);
    }
}

function pluginTransitionStageBelongsToRequest(stage, req) {
    if (!stage) return false;
    const requestSessionId = typeof req.headers['x-session-id'] === 'string'
        ? req.headers['x-session-id']
        : null;
    return stage.sessionId === requestSessionId;
}

function removePluginTransitionStage(stage) {
    if (!stage) return;
    const prefix = `${PLUGIN_TRANSITION_STAGE_PREFIX}${stage.transitionId}`;
    for (const entry of readdirSync(pluginTransitionStageDir, { withFileTypes: true })) {
        if (!entry.isFile() || !entry.name.startsWith(prefix)) continue;
        const suffix = entry.name.slice(prefix.length);
        if (suffix !== '.json' && !suffix.startsWith('-') && !suffix.startsWith('.json.')) continue;
        try { unlinkSync(path.join(pluginTransitionStageDir, entry.name)); } catch {}
    }
    fsyncPluginTransitionStageDirectory();
}

function removePluginTransitionStageRows(stage) {
    if (!stage) return;
    const prefix = `${PLUGIN_TRANSITION_STAGE_PREFIX}${stage.transitionId}-`;
    for (const entry of readdirSync(pluginTransitionStageDir, { withFileTypes: true })) {
        if (!entry.isFile() || !entry.name.startsWith(prefix)) continue;
        try { unlinkSync(path.join(pluginTransitionStageDir, entry.name)); } catch {}
    }
    fsyncPluginTransitionStageDirectory();
}

async function findActivePluginTransition(req, excludeTransitionId = null) {
    const requestSessionId = typeof req.headers['x-session-id'] === 'string'
        ? req.headers['x-session-id']
        : null;
    for (const entry of readdirSync(pluginTransitionStageDir, { withFileTypes: true })) {
        if (!entry.isFile()) continue;
        const match = entry.name.match(
            /^\.plugin-transition-stage-([0-9a-f-]{36})\.json$/,
        );
        if (!match || match[1] === excludeTransitionId) continue;
        const stage = await refreshPluginTransitionStageState(
            readPluginTransitionStage(match[1]),
        );
        if (!stage || (stage.state !== 'uploading' && stage.state !== 'ready')) continue;
        // A page reload creates a new writer session. Once that session owns
        // the server lock, an unpublished stage from the displaced page can
        // no longer be finalized and must not block recovery forever.
        if (stage.sessionId !== requestSessionId) {
            removePluginTransitionStage(stage);
            continue;
        }
        return stage;
    }
    return null;
}

function sweepStalePluginTransitionStages() {
    if (!ensureDatabaseSpoolDirSync()) return;
    try {
        const now = Date.now();
        for (const entry of readdirSync(pluginTransitionStageDir, { withFileTypes: true })) {
            if (!entry.isFile()
                || !entry.name.startsWith(PLUGIN_TRANSITION_STAGE_PREFIX)
                || !entry.name.endsWith('.json')) continue;
            const match = entry.name.match(
                /^\.plugin-transition-stage-([0-9a-f-]{36})\.json$/,
            );
            if (!match || !PLUGIN_STORAGE_UUID_PATTERN.test(match[1])) continue;
            const stage = readPluginTransitionStage(match[1]);
            if (!stage
                || stage.state === 'aborted'
                || now - Number(stage.updatedAt ?? stage.createdAt ?? 0)
                    > PLUGIN_TRANSITION_STAGE_MAX_AGE_MS) {
                removePluginTransitionStage(stage ?? { transitionId: match[1], rows: [] });
            }
        }
        for (const entry of readdirSync(pluginTransitionStageDir, { withFileTypes: true })) {
            if (!entry.isFile() || !entry.name.startsWith(PLUGIN_TRANSITION_STAGE_PREFIX)) continue;
            if (entry.name.endsWith('.json')) continue;
            const match = entry.name.match(
                /^\.plugin-transition-stage-([0-9a-f-]{36})-/,
            );
            const stage = match && PLUGIN_STORAGE_UUID_PATTERN.test(match[1])
                ? readPluginTransitionStage(match[1])
                : null;
            if (!stage || stage.state === 'committed' || stage.state === 'aborted') {
                try { unlinkSync(path.join(pluginTransitionStageDir, entry.name)); } catch {}
            }
        }
    } catch (error) {
        logger.warn('[PluginStorageTransition] Could not sweep stale stages:', error);
    }
}

// Server-side backup directory (outside save/ to avoid bloating updater copies).
// Configurable at runtime via the kv key `config/server-backup-path`. When the
// user changes the path the old directory is left in place (existing backups
// stay where they were); only future backups land at the new path.
const DEFAULT_BACKUPS_DIR = path.join(process.cwd(), "backups");
const BACKUP_PATH_CONFIG_KEY = 'config/server-backup-path';
// Plaintext marker the updater reads to preserve a custom in-tree backup dir
// during in-place updates. KV lives inside the SQLite DB so the updater (which
// runs without npm deps) can't read it; this marker bridges that gap.
const BACKUP_PATH_MARKER = path.join(savePath, '__backup_path');
const CHAT_BACKUP_PATH_MARKER = path.join(savePath, '__chat_backup_path');
function withRecoveryPathInterprocessLockSync(purpose, operation) {
    const interprocessLock = acquireRecoveryPathStateLockSync(savePath, { purpose });
    let releaseLock = true;
    try {
        return operation();
    } catch (error) {
        if (error?.retainRecoveryPathStateLock === true) releaseLock = false;
        throw error;
    } finally {
        if (releaseLock) interprocessLock.release();
    }
}

function readBackupsDirConfig() {
    try {
        const raw = kvGet(BACKUP_PATH_CONFIG_KEY);
        if (!raw) return DEFAULT_BACKUPS_DIR;
        const text = Buffer.from(raw).toString('utf-8').trim();
        return text || DEFAULT_BACKUPS_DIR;
    } catch { return DEFAULT_BACKUPS_DIR; }
}

function publishConfiguredUpdaterPathMarker(markerPath, absPath) {
    let existingTargets = [];
    try { existingTargets = readRecoveryPathMarkerTargetsSync(markerPath); }
    catch {
        // Missing/malformed legacy metadata is repaired from the authoritative
        // configured root. Once a valid set exists, later publications retain
        // it so archives deliberately left at prior roots remain protected.
    }
    return publishUpdaterPathMarkerSet(markerPath, [...existingTargets, absPath]);
}

function publishUpdaterPathMarkerSet(markerPath, targetPaths) {
    const identityTargets = [];
    for (const targetPath of targetPaths) {
        const absolute = path.resolve(targetPath);
        identityTargets.push(absolute);
        try {
            if (process.env.NODE_ENV === 'test') {
                const injected = JSON.parse(String(
                    process.env.POCKETRISU_TEST_RECOVERY_CANONICALIZE_FAIL_PATHS ?? '[]',
                ));
                if (Array.isArray(injected) && injected.includes(absolute)) {
                    const error = new Error(`Injected inaccessible recovery path: ${absolute}`);
                    error.code = 'EACCES';
                    throw error;
                }
            }
            identityTargets.push(canonicalizePathWithExistingPrefixSync(absolute));
        } catch (error) {
            // Marker publication is conservative and may retain offline UNC,
            // removable-drive, or permission-denied historical roots. Preserve
            // their authoritative lexical identities so startup can continue;
            // updater consumption still canonicalizes every entry and refuses
            // destructive replacement if an identity remains ambiguous.
            logger.warn(
                `[RecoveryPath] Could not canonicalize ${absolute} while publishing preservation metadata; retaining its lexical identity:`,
                error?.message || error,
            );
        }
    }
    return publishRecoveryPathMarkerSetSync(markerPath, identityTargets, {
        onStage: recoveryPathPublicationFaultHandler(markerPath),
    });
}

function recoveryPathPublicationFaultHandler(targetPath) {
    return (stage) => {
        if (process.env.NODE_ENV !== 'test') return;
        const faultDirectory = String(
            process.env.POCKETRISU_TEST_RECOVERY_PATH_MARKER_FAULT_DIR ?? '',
        ).trim();
        if (!faultDirectory) return;
        const faultPath = path.join(
            path.resolve(faultDirectory),
            `${path.basename(targetPath)}.${stage}`,
        );
        if (existsSync(faultPath)) {
            const kind = path.basename(targetPath) === RECOVERY_PATH_STARTUP_QUARANTINE_NAME
                ? 'startup quarantine'
                : 'marker';
            throw new Error(`Injected recovery-path ${kind} publication failure at ${stage}`);
        }
    };
}

function waitAtRecoveryPathStartupTestGateSync(stage) {
    if (process.env.NODE_ENV !== 'test') return;
    const configured = String(
        process.env.POCKETRISU_TEST_RECOVERY_PATH_STARTUP_GATE_DIR ?? '',
    ).trim();
    if (!configured) return;
    const gateDir = path.resolve(configured);
    let selectedStage;
    try { selectedStage = readFileSync(path.join(gateDir, 'stage'), 'utf8').trim(); }
    catch { return; }
    if (selectedStage !== stage || !existsSync(path.join(gateDir, 'hold'))) return;
    writeFileSync(path.join(gateDir, 'entered'), stage, 'utf8');
    const sleeper = new Int32Array(new SharedArrayBuffer(4));
    while (existsSync(path.join(gateDir, 'hold'))
        && !existsSync(path.join(gateDir, 'release'))) {
        Atomics.wait(sleeper, 0, 0, 10);
    }
}

function recoveryPathPlatform() {
    if (process.env.NODE_ENV === 'test'
        && process.env.POCKETRISU_TEST_RECOVERY_PLATFORM === 'win32') {
        return 'win32';
    }
    return process.platform;
}

function isManagedBackupPath(absPath) {
    try {
        recoveryPathKeepEntries(process.cwd(), absPath, 'Backup path', {
            platform: recoveryPathPlatform(),
        });
        return false;
    } catch {
        return true;
    }
}

let backupsDir;
let chatBackupsDir;
let chatBackupReadRoots = [];
let chatBackupRequiredReadRoots = [];
let serverBackupReadRoots = [];

function getBackupsDir() {
    return backupsDir;
}

function setBackupsDirResolved(resolved) {
    backupsDir = resolved;
}

function getChatBackupsDir() {
    return chatBackupsDir;
}
// Publish the configured root before creating, sweeping, or otherwise relying
// on it. If the configured destination is temporarily unavailable and runtime
// falls back to the default, the marker intentionally remains conservative and
// still preserves the configured in-tree root.
// Backup and chat metadata share one admission: no updater or second server can
// observe the old split-publication window between these durable writes.
waitAtRecoveryPathStartupTestGateSync('before-startup-lock-acquire');
withRecoveryPathInterprocessLockSync('server startup recovery-marker publication', () => {
    // Select the authoritative roots only after acquiring the same admission
    // used by PUT transitions. Carry these exact values into marker publication
    // and all subsequent startup work.
    backupsDir = readBackupsDirConfig();
    chatBackupsDir = resolveChatBackupDir({ savePath });
    let previousQuarantine;
    try {
        previousQuarantine = readRecoveryPathStartupQuarantineSync(savePath);
    } catch (error) {
        error.retainRecoveryPathStateLock = true;
        throw error;
    }
    const plannedTargets = {};
    for (const [markerName, markerPath, authoritativeRoot] of [
        ['__backup_path', BACKUP_PATH_MARKER, backupsDir],
        ['__chat_backup_path', CHAT_BACKUP_PATH_MARKER, chatBackupsDir],
    ]) {
        const historicalTargets = previousQuarantine?.markers?.[markerName] ?? [];
        try {
            historicalTargets.push(...readRecoveryPathMarkerTargetsSync(markerPath));
        } catch {
            // A valid quarantine is authoritative recovery history after a
            // partial startup. Without one, legacy missing/malformed metadata
            // is repaired conservatively from the current configured root.
        }
        plannedTargets[markerName] = [...historicalTargets, authoritativeRoot];
    }
    let quarantinedTargets;
    try {
        quarantinedTargets = publishRecoveryPathStartupQuarantineSync(
            savePath,
            plannedTargets,
            {
                onStage: recoveryPathPublicationFaultHandler(
                    path.join(savePath, RECOVERY_PATH_STARTUP_QUARANTINE_NAME),
                ),
            },
        );
    } catch (error) {
        // No marker is changed before this record is durable. If its atomic
        // publication is uncertain, retain the exact token-owned lock rather
        // than allowing an updater to guess whether fail-closed state exists.
        error.retainRecoveryPathStateLock = true;
        throw error;
    }
    try {
        publishUpdaterPathMarkerSet(
            BACKUP_PATH_MARKER,
            quarantinedTargets.__backup_path,
        );
        waitAtRecoveryPathStartupTestGateSync('after-backup-before-chat-marker');
        publishUpdaterPathMarkerSet(
            CHAT_BACKUP_PATH_MARKER,
            quarantinedTargets.__chat_backup_path,
        );
        // Captures always target chatBackupsDir. Historical marker roots remain
        // readable until their files have been merged successfully, including
        // when a conflicting or interrupted migration leaves a source behind.
        serverBackupReadRoots = [...quarantinedTargets.__backup_path];
        const serverChatBackupReadRoots = serverBackupReadRoots.map(
            root => path.join(root, CHAT_BACKUP_DIRNAME),
        );
        chatBackupRequiredReadRoots = [
            chatBackupsDir,
            ...quarantinedTargets.__chat_backup_path,
            // A derived legacy tree that is present at startup is now a known
            // history root. If it later disappears, destructive reachability
            // scans must fail closed rather than reinterpret it as empty.
            ...serverChatBackupReadRoots.filter(root => existsSync(root)),
        ];
        chatBackupReadRoots = [
            ...chatBackupRequiredReadRoots,
            ...serverChatBackupReadRoots,
        ];
        clearRecoveryPathStartupQuarantineSync(savePath);
    } catch (publicationError) {
        throw new AggregateError(
            [publicationError],
            'Startup recovery-marker publication failed; durable recovery history remains quarantined fail-closed',
        );
    }
});
if(!HUB_HOSTING_MODE && !existsSync(backupsDir)){
    try {
        if (process.env.NODE_ENV === 'test') {
            const injectedUnavailable = JSON.parse(String(
                process.env.POCKETRISU_TEST_RECOVERY_UNAVAILABLE_PATHS ?? '[]',
            ));
            if (Array.isArray(injectedUnavailable)
                && injectedUnavailable.includes(path.resolve(backupsDir))) {
                const error = new Error(`Injected unavailable backup root: ${backupsDir}`);
                error.code = 'EACCES';
                throw error;
            }
        }
        mkdirSync(backupsDir, { recursive: true });
    }
    catch { backupsDir = DEFAULT_BACKUPS_DIR; mkdirSync(backupsDir, { recursive: true }); }
}
function sweepServerBackupTemps(directory) {
    try {
        for (const entry of readdirSync(directory, { withFileTypes: true })) {
            if (!entry.isFile() || !entry.name.startsWith(SERVER_BACKUP_TEMP_PREFIX)) continue;
            try {
                unlinkSync(path.join(directory, entry.name));
            } catch (error) {
                logger.warn(`[Backup] Could not remove orphaned server backup temp ${entry.name}:`, error);
            }
        }
    } catch (error) {
        if (error?.code !== 'ENOENT') {
            logger.warn(`[Backup] Could not sweep server backup directory ${directory}:`, error);
        }
    }
}
sweepServerBackupTemps(backupsDir);
try {
    mkdirSync(chatBackupsDir, { recursive: true });
} catch (error) {
    // Capture/reconcile remain best-effort. In particular, an invalid operator
    // override must not make the authoritative database unavailable.
    logger.error('[ChatBackups] Could not create the chat-backup directory:', error?.message || error);
}
for (const historicalRoot of [
    path.join(path.resolve(backupsDir), CHAT_BACKUP_DIRNAME),
    ...chatBackupReadRoots,
]) {
    migrateLegacyChatBackups({
        legacyRoot: historicalRoot,
        destinationRoot: chatBackupsDir,
        logger,
    });
}
const BACKUP_FILENAME_REGEX = /^risu-backup-\d+\.bin$/;
const CHAT_BACKUP_VERSION_ID_REGEX = /^v-\d+-\d+-[a-z0-9_-]{1,24}$/;

const passwordPath = path.join(process.cwd(), 'save', '__password')
if(existsSync(passwordPath)){
    password = readFileSync(passwordPath, 'utf-8')
}

// ── NodeOnly: server-side JWT (HMAC-SHA256) ─────────────────────────────────
// Upstream uses client-side ECDSA JWT via crypto.subtle, which requires
// Secure Context (HTTPS or localhost). NodeOnly serves over HTTP,
// so we moved JWT signing/verification to the server using HMAC-SHA256.
// If upstream changes its auth flow, this section needs manual sync.
// Related: createServerJwt(), checkAuth(), /api/login, /api/token/refresh
const jwtSecretPath = path.join(savePath, '__jwt_secret')
let jwtSecret
if (existsSync(jwtSecretPath)) {
    jwtSecret = readFileSync(jwtSecretPath, 'utf-8').trim()
} else {
    jwtSecret = nodeCrypto.randomBytes(64).toString('hex')
    writeFileSync(jwtSecretPath, jwtSecret, 'utf-8')
}

const inlayDir = path.join(savePath, 'inlays')
const inlayMigrationMarker = path.join(inlayDir, '.migrated_to_fs')
const INLAY_CANONICAL_ROOT_NAME = '.inlay-objects-v1'
const INLAY_CANONICAL_PAYLOAD_DIR_NAME = 'payload'
const INLAY_CANONICAL_SIDECAR_DIR_NAME = 'sidecar'
const INLAY_CANONICAL_ID_MARKER = 'i'
const INLAY_CANONICAL_EXT_MARKER = 'e'
const INLAY_CANONICAL_PAYLOAD_FILE = 'data'
const INLAY_CANONICAL_SIDECAR_FILE = 'meta.json'
const INLAY_CANONICAL_HEX_CHUNK_LENGTH = 120
const inlayCanonicalRoot = path.join(inlayDir, INLAY_CANONICAL_ROOT_NAME)
const inlayCanonicalPayloadDir = path.join(
    inlayCanonicalRoot,
    INLAY_CANONICAL_PAYLOAD_DIR_NAME,
)
const inlayCanonicalSidecarDir = path.join(
    inlayCanonicalRoot,
    INLAY_CANONICAL_SIDECAR_DIR_NAME,
)
const INLAY_TEMP_PREFIX = '.inlay-publish-'
const INLAY_TEMP_NAME_PATTERN = /^\.inlay-publish-\d+-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}-(?:payload|sidecar)$/i
const inlayPublishFailpoint = process.env.NODE_ENV === 'test'
    ? String(process.env.POCKETRISU_TEST_INLAY_PUBLISH_FAILPOINT ?? '').trim()
    : ''
const inlayPublishTestGateDir = process.env.NODE_ENV === 'test'
    ? String(process.env.POCKETRISU_TEST_INLAY_PUBLISH_GATE_DIR ?? '').trim() || null
    : null
const inlayPublishTestGateStage = process.env.NODE_ENV === 'test'
    ? String(process.env.POCKETRISU_TEST_INLAY_PUBLISH_GATE_STAGE ?? '').trim()
    : ''
const IMPORT_JOURNAL_PATH = path.join(savePath, 'import_journal.json')
const IMPORT_JOURNAL_MARKER_KEY = 'import_journal/marker'
const hexRegex = /^[0-9a-fA-F]+$/;
const DEFAULT_BACKUP_IMPORT_MAX_BYTES = 2 * 1024 * 1024 * 1024;
// Large restore is an explicit, authenticated recovery action.  Keep ordinary
// API callers behind the conservative soft limits while allowing the UI (after
// its destructive-restore confirmations) and trusted server backups to use all
// safely representable space that the disk preflight can admit.
const DEFAULT_LARGE_RESTORE_MAX_BYTES = Math.floor(Number.MAX_SAFE_INTEGER / 2);
const DEFAULT_LEGACY_DATABASE_IMPORT_MAX_BYTES = 64 * 1024 * 1024;
const BACKUP_IMPORT_MAX_BYTES = finiteByteLimit(
    process.env.RISU_BACKUP_IMPORT_MAX_BYTES,
    DEFAULT_BACKUP_IMPORT_MAX_BYTES,
);
const LEGACY_DATABASE_IMPORT_MAX_BYTES = finiteByteLimit(
    process.env.RISU_LEGACY_DATABASE_IMPORT_MAX_BYTES,
    DEFAULT_LEGACY_DATABASE_IMPORT_MAX_BYTES,
    { max: BACKUP_IMPORT_MAX_BYTES },
);
const SAVE_FOLDER_IMPORT_MAX_ENTRIES = finiteByteLimit(
    process.env.RISU_SAVE_FOLDER_IMPORT_MAX_ENTRIES,
    100_000,
    { max: 1_000_000 },
);
const BACKUP_IMPORT_MAX_ENTRIES = finiteByteLimit(
    process.env.RISU_BACKUP_IMPORT_MAX_ENTRIES,
    100_000,
    { max: 1_000_000 },
);
const LARGE_RESTORE_MAX_BYTES = finiteByteLimit(
    process.env.RISU_LARGE_RESTORE_MAX_BYTES,
    DEFAULT_LARGE_RESTORE_MAX_BYTES,
    { max: DEFAULT_LARGE_RESTORE_MAX_BYTES },
);
const LARGE_RESTORE_MAX_ENTRIES = finiteByteLimit(
    process.env.RISU_LARGE_RESTORE_MAX_ENTRIES,
    Number.MAX_SAFE_INTEGER,
);
const IMPORT_BUFFERED_ENTRY_MAX_BYTES = finiteByteLimit(
    process.env.RISU_IMPORT_BUFFERED_ENTRY_MAX_BYTES,
    32 * 1024 * 1024,
    { max: BACKUP_IMPORT_MAX_BYTES },
);
// Minimum free disk space headroom multiplier: require 2× the backup size to be free
const BACKUP_DISK_HEADROOM = 2;
function recoverPendingImportSwap(source) {
    const journal = readImportJournal(IMPORT_JOURNAL_PATH);
    if (!journal) return null;

    const touchesLiveAssets = journal.dirs.some((entry) => (
        sameAssetDirectoryIdentitySync(entry.liveDir, assetDir, fsSync)
    ));
    const assetMaintenanceLock = touchesLiveAssets
        ? acquireAssetMaintenanceLockSync(assetDir, {
            purpose: `import-journal recovery (${source})`,
        })
        : null;

    let recoveryComplete = false;
    try {
        if (importRecoveryFailpoint === 'after-lock-acquired') {
            const error = new Error('Injected import-journal recovery failure after lock acquisition');
            error.code = 'IMPORT_RECOVERY_INJECTED_FAILURE';
            throw error;
        }
        const markerValue = kvGet(IMPORT_JOURNAL_MARKER_KEY);
        const markerPresent = markerValue !== null
            && Buffer.from(markerValue).toString('utf-8') === journal.id;
        const summary = recoverImportSwap({ journal, markerPresent, fs: fsSync });
        logger.warn(
            `[Import Recovery] ${source}: ${summary.action} ${summary.directories} `
            + `directory swap(s) for journal ${journal.id} `
            + `(phase=${journal.phase}, markerPresent=${markerPresent})`
        );

        // Once backups have been finalized, marker deletion must not make a
        // repeated recovery interpret the imported live directories as uncommitted.
        if (summary.action === 'finalized' && journal.phase !== 'committed') {
            writeImportJournal(IMPORT_JOURNAL_PATH, { ...journal, phase: 'committed' });
        }
        if (markerValue !== null) kvDel(IMPORT_JOURNAL_MARKER_KEY);
        clearImportJournal(IMPORT_JOURNAL_PATH);
        recoveryComplete = true;
        return summary;
    } finally {
        // An unresolved live recovery must keep exclusion in this process.
        // Releasing here would admit writes/dedup against a partially restored
        // directory. Startup can recover the retained same-host owner after
        // this process exits, then retry the durable journal.
        if (recoveryComplete) {
            releaseAssetMaintenanceLockHandle(assetMaintenanceLock);
        }
    }
}

const INLAY_LEGACY_NAME_MAX_BYTES = 255;
const INLAY_LEGACY_SIDECAR_SUFFIX = '.meta.json';

function isWellFormedUtf8Text(value) {
    return typeof value === 'string'
        && Buffer.from(value, 'utf-8').toString('utf-8') === value;
}

function isSafeInlayId(id) {
    return isWellFormedUtf8Text(id) &&
        id.length > 0 &&
        !id.includes('\0') &&
        !id.includes('/') &&
        !id.includes('\\') &&
        id !== '.' &&
        id !== '..' &&
        Buffer.byteLength(id, 'utf-8')
            + Buffer.byteLength(INLAY_LEGACY_SIDECAR_SUFFIX, 'utf-8')
            <= INLAY_LEGACY_NAME_MAX_BYTES;
}

function normalizedInlayTupleExtension(id, ext) {
    if (!isSafeInlayId(id)) return null;
    const normalizedExt = normalizeInlayExt(ext);
    if (!isWellFormedUtf8Text(normalizedExt)
        || Buffer.byteLength(id, 'utf-8')
            + 1
            + Buffer.byteLength(normalizedExt, 'utf-8')
            > INLAY_LEGACY_NAME_MAX_BYTES) return null;
    return normalizedExt;
}

function isSafeInlayTuple(id, ext) {
    return normalizedInlayTupleExtension(id, ext) !== null;
}

function invalidInlayTupleError(id, ext = null) {
    const error = new Error(
        ext === null
            ? 'Invalid inlay ID or ID exceeds the portable filename limit'
            : 'Invalid inlay ID/extension tuple or tuple exceeds the portable filename limit',
    );
    error.code = 'INVALID_INLAY_TUPLE';
    error.statusCode = 400;
    error.id = id;
    error.ext = ext;
    return error;
}

function assertSafeInlayTuple(id, ext) {
    if (arguments.length === 1) {
        if (!isSafeInlayId(id)) throw invalidInlayTupleError(id);
        return null;
    }
    const normalizedExt = normalizedInlayTupleExtension(id, ext);
    if (normalizedExt === null) throw invalidInlayTupleError(id, ext);
    return normalizedExt;
}

const MAX_INLAY_DELETE_BATCH = 1000;
const INLAY_REFERENCE_PATTERN = /\{\{(?:inlay|inlayed|inlayeddata)::(.+?)\}\}/g;

function addInlayReferencesFromText(text, refCounts) {
    if (typeof text !== 'string') return;
    const regex = new RegExp(INLAY_REFERENCE_PATTERN.source, 'g');
    let match;
    while ((match = regex.exec(text)) !== null) {
        const id = match[1];
        refCounts.set(id, (refCounts.get(id) ?? 0) + 1);
    }
}

function addInlayReferencesFromChat(chat, refCounts) {
    if (!Array.isArray(chat?.message)) return 0;
    let totalMessages = 0;
    for (const message of chat.message) {
        if (!message || typeof message !== 'object') continue;
        totalMessages++;
        addInlayReferencesFromText(message.data, refCounts);
        if (Array.isArray(message.swipes)) {
            for (const swipe of message.swipes) {
                addInlayReferencesFromText(swipe, refCounts);
            }
        }
    }
    return totalMessages;
}

/**
 * Count references from server-authoritative chat rows. This deliberately scans
 * every physical chat row, including a recently staged row whose stub has not
 * committed yet. Being conservative can temporarily retain an orphan, while
 * omitting that row could permanently delete media from a newly created chat.
 */
async function scanAuthoritativeInlayReferences() {
    const refCounts = new Map();
    let totalMessages = 0;
    let totalHistoryVersions = 0;

    for (const key of chatRowStore.listAllChatRowKeys()) {
        const identity = chatRowStore.parseChatRowKey(key);
        if (!identity) continue;
        const chat = await chatRowStore.readChatRow(identity.chaId, identity.chatId);
        if (!chat) continue;
        if (isColdStorageChat(chat) && !restoreColdStorageChat(chat)) {
            throw new Error(`Cannot verify inlay references in cold-storage chat ${key}`);
        }
        totalMessages += addInlayReferencesFromChat(chat, refCounts);
    }

    const history = await chatBackupStore.scanChatBackupVersions(async (raw, identity) => {
        let chat;
        try {
            chat = await decodeAuthoritativeRisuSave(raw);
        } catch (error) {
            throw new Error(
                `Cannot decode retained chat backup ${identity.chaId}/${identity.chatId}`
                + `/${identity.versionId}: ${error?.message || error}`,
                { cause: error },
            );
        }
        if (isColdStorageChat(chat) && !restoreColdStorageChat(chat)) {
            throw new Error(
                `Cannot verify inlay references in cold-storage chat backup `
                + `${identity.chaId}/${identity.chatId}/${identity.versionId}`,
            );
        }
        totalMessages += addInlayReferencesFromChat(chat, refCounts);
    });
    totalHistoryVersions = history.totalVersions;

    return {
        scannedAt: Date.now(),
        totalMessages,
        totalHistoryVersions,
        refCounts: Object.fromEntries([...refCounts.entries()].sort(([left], [right]) => (
            left.localeCompare(right)
        ))),
    };
}

function validateInlayDeleteRequest(body) {
    if (!body || typeof body !== 'object' || Array.isArray(body)) return null;
    if (!Array.isArray(body.ids)
        || body.ids.length === 0
        || body.ids.length > MAX_INLAY_DELETE_BATCH) return null;
    const ids = [...new Set(body.ids)];
    if (ids.length === 0 || ids.some((id) => !isSafeInlayId(id))) return null;

    const clientProtected = body.clientProtectedIds ?? [];
    if (!Array.isArray(clientProtected)
        || clientProtected.length > MAX_INLAY_DELETE_BATCH
        || clientProtected.some((id) => typeof id !== 'string')) return null;
    const requested = new Set(ids);
    return {
        ids,
        clientProtectedIds: new Set(clientProtected.filter((id) => requested.has(id))),
    };
}

async function deleteUnreferencedInlays(ids, clientProtectedIds = new Set()) {
    const scan = await scanAuthoritativeInlayReferences();
    const referencedIds = ids.filter((id) => (
        clientProtectedIds.has(id) || (scan.refCounts[id] ?? 0) > 0
    ));
    const referenced = new Set(referencedIds);
    const removedIds = [];

    for (const id of ids) {
        if (referenced.has(id)) continue;
        await deleteInlayFile(id);
        kvDel(`inlay/${id}`);
        kvDel(`inlay_thumb/${id}`);
        kvDel(`inlay_info/${id}`);
        kvDel(`inlay_meta/${id}`);
        removedIds.push(id);
    }

    return { removedIds, referencedIds, scannedAt: scan.scannedAt };
}

function normalizeInlayExt(ext) {
    if (typeof ext !== 'string') return 'bin';
    const normalized = ext.trim().toLowerCase().replace(/^\.+/, '').replace(/[\/\\\0]/g, '');
    return normalized || 'bin';
}

const resolvedInlayDir = path.resolve(inlayDir) + path.sep;

function assertInsideInlayDir(filePath) {
    if (!path.resolve(filePath).startsWith(resolvedInlayDir)) {
        throw new Error(`Path escapes inlay directory: ${filePath}`);
    }
}

function encodeInlayPhysicalComponent(value) {
    const encoded = Buffer.from(value, 'utf-8');
    if (encoded.toString('utf-8') !== value) {
        throw new Error('Inlay physical names require well-formed UTF-8 text');
    }
    return encoded.toString('hex');
}

function decodeInlayPhysicalComponent(value) {
    if (typeof value !== 'string'
        || value.length === 0
        || value.length % 2 !== 0
        || !/^[0-9a-f]+$/.test(value)) return null;
    const decoded = Buffer.from(value, 'hex').toString('utf-8');
    return encodeInlayPhysicalComponent(decoded) === value ? decoded : null;
}

function chunkInlayPhysicalComponent(value) {
    const encoded = encodeInlayPhysicalComponent(value);
    const chunks = [];
    for (let offset = 0; offset < encoded.length; offset += INLAY_CANONICAL_HEX_CHUNK_LENGTH) {
        chunks.push(encoded.slice(offset, offset + INLAY_CANONICAL_HEX_CHUNK_LENGTH));
    }
    return chunks;
}

function decodeInlayPhysicalChunks(chunks) {
    if (!Array.isArray(chunks) || chunks.length === 0) return null;
    for (let index = 0; index < chunks.length; index++) {
        const chunk = chunks[index];
        const expectedLength = index === chunks.length - 1
            ? chunk.length
            : INLAY_CANONICAL_HEX_CHUNK_LENGTH;
        if (typeof chunk !== 'string'
            || chunk.length === 0
            || chunk.length % 2 !== 0
            || chunk.length > INLAY_CANONICAL_HEX_CHUNK_LENGTH
            || chunk.length !== expectedLength
            || !/^[0-9a-f]+$/.test(chunk)) return null;
    }
    return decodeInlayPhysicalComponent(chunks.join(''));
}

function canonicalInlayPaths(baseDir, id, ext = null) {
    const normalizedExt = ext === null
        ? assertSafeInlayTuple(id)
        : assertSafeInlayTuple(id, ext);
    const root = path.join(baseDir, INLAY_CANONICAL_ROOT_NAME);
    const idChunks = chunkInlayPhysicalComponent(id);
    const sidecarPath = path.join(
        root,
        INLAY_CANONICAL_SIDECAR_DIR_NAME,
        INLAY_CANONICAL_ID_MARKER,
        ...idChunks,
        INLAY_CANONICAL_SIDECAR_FILE,
    );
    const payloadPath = ext === null
        ? null
        : path.join(
            root,
            INLAY_CANONICAL_PAYLOAD_DIR_NAME,
            INLAY_CANONICAL_ID_MARKER,
            ...idChunks,
            INLAY_CANONICAL_EXT_MARKER,
            ...chunkInlayPhysicalComponent(normalizedExt),
            INLAY_CANONICAL_PAYLOAD_FILE,
        );
    if (baseDir === inlayDir) {
        assertInsideInlayDir(sidecarPath);
        if (payloadPath) assertInsideInlayDir(payloadPath);
    }
    return { root, payloadPath, sidecarPath };
}

function ensurePortableInlayDirectory(directory, baseDir) {
    const relative = path.relative(baseDir, directory);
    if (!relative || relative.startsWith('..') || path.isAbsolute(relative)) {
        if (relative === '') return;
        throw new Error(`Inlay canonical directory escapes namespace: ${directory}`);
    }
    let current = baseDir;
    for (const segment of relative.split(path.sep)) {
        const entries = readdirSync(current, { withFileTypes: true });
        const aliases = entries.filter((entry) => entry.name.toLowerCase() === segment.toLowerCase());
        const collision = aliases.find((entry) => entry.name !== segment || !entry.isDirectory());
        if (collision) {
            const error = new Error(
                `Inlay physical directory collision: ${segment} conflicts with ${collision.name}`,
            );
            error.code = 'INLAY_PHYSICAL_COLLISION';
            throw error;
        }
        const next = path.join(current, segment);
        if (aliases.length === 0) {
            mkdirSync(next, { recursive: false });
            fsyncDirectoryPathSync(current);
        }
        current = next;
    }
}

function getInlayFilePath(id, ext) {
    return canonicalInlayPaths(inlayDir, id, ext).payloadPath;
}

function getInlaySidecarPath(id) {
    return canonicalInlayPaths(inlayDir, id).sidecarPath;
}

function getLegacyInlayFilePath(id, ext) {
    const normalizedExt = assertSafeInlayTuple(id, ext);
    const p = path.join(inlayDir, `${id}.${normalizedExt}`);
    assertInsideInlayDir(p);
    return p;
}

function getLegacyInlaySidecarPath(id) {
    assertSafeInlayTuple(id);
    const p = path.join(inlayDir, `${id}.meta.json`);
    assertInsideInlayDir(p);
    return p;
}

function assertExactPortableInlayTarget(directory, name) {
    let entries;
    try {
        entries = readdirSync(directory, { withFileTypes: true });
    } catch (error) {
        if (error?.code === 'ENOENT') return;
        throw error;
    }
    const portableName = name.toLowerCase();
    const aliases = entries.filter((entry) => entry.name.toLowerCase() === portableName);
    const collision = aliases.find((entry) => entry.name !== name || !entry.isFile());
    if (collision) {
        const error = new Error(
            `Inlay physical target collision: ${name} conflicts with ${collision.name}`,
        );
        error.code = 'INLAY_PHYSICAL_COLLISION';
        throw error;
    }
}

function assertCanonicalInlayNamespace(baseDir) {
    const rootEntries = readdirSync(baseDir, { withFileTypes: true });
    const rootAlias = rootEntries.find((entry) => (
        entry.name.toLowerCase() === INLAY_CANONICAL_ROOT_NAME
        && (entry.name !== INLAY_CANONICAL_ROOT_NAME || !entry.isDirectory())
    ));
    if (rootAlias) {
        const error = new Error(
            `Inlay canonical namespace collision: ${rootAlias.name}`,
        );
        error.code = 'INLAY_PHYSICAL_COLLISION';
        throw error;
    }
    const root = path.join(baseDir, INLAY_CANONICAL_ROOT_NAME);
    if (!existsSync(root)) {
        mkdirSync(root, { recursive: false });
        fsyncDirectoryPathSync(baseDir);
    }
    for (const name of [INLAY_CANONICAL_PAYLOAD_DIR_NAME, INLAY_CANONICAL_SIDECAR_DIR_NAME]) {
        const entries = readdirSync(root, { withFileTypes: true });
        const alias = entries.find((entry) => (
            entry.name.toLowerCase() === name
            && (entry.name !== name || !entry.isDirectory())
        ));
        if (alias) {
            const error = new Error(
                `Inlay canonical namespace collision: ${alias.name}`,
            );
            error.code = 'INLAY_PHYSICAL_COLLISION';
            throw error;
        }
        const directory = path.join(root, name);
        if (!existsSync(directory)) {
            mkdirSync(directory, { recursive: false });
            fsyncDirectoryPathSync(root);
        }
    }
}

function assertCanonicalInlayWriteTargets(baseDir, id, ext = null) {
    const paths = canonicalInlayPaths(baseDir, id, ext);
    const canonicalRoot = path.join(baseDir, INLAY_CANONICAL_ROOT_NAME);
    ensurePortableInlayDirectory(path.dirname(paths.sidecarPath), canonicalRoot);
    assertExactPortableInlayTarget(
        path.dirname(paths.sidecarPath),
        path.basename(paths.sidecarPath),
    );
    if (paths.payloadPath) {
        ensurePortableInlayDirectory(path.dirname(paths.payloadPath), canonicalRoot);
        assertExactPortableInlayTarget(
            path.dirname(paths.payloadPath),
            path.basename(paths.payloadPath),
        );
    }
}

async function ensureInlayDir() {
    await fs.mkdir(inlayDir, { recursive: true });
    assertCanonicalInlayNamespace(inlayDir);
}

function ensureInlayDirSync() {
    if (!existsSync(inlayDir)) {
        mkdirSync(inlayDir, { recursive: true });
    }
    assertCanonicalInlayNamespace(inlayDir);
}

async function fsyncDirectoryPath(directory) {
    let directoryHandle;
    try {
        directoryHandle = await fs.open(directory, 'r');
        await directoryHandle.sync();
    } catch {
        // Some platforms do not allow directory handles to be opened or synced.
        // The staged file itself is still synced before every atomic rename.
    } finally {
        await directoryHandle?.close().catch(() => {});
    }
}

async function fsyncInlayDirectory() {
    return fsyncDirectoryPath(inlayDir);
}

function fsyncDirectoryPathSync(directory) {
    let directoryDescriptor;
    try {
        directoryDescriptor = openSync(directory, 'r');
        fsyncSync(directoryDescriptor);
    } catch {
        // Directory fsync is unavailable on some platforms.
    } finally {
        if (directoryDescriptor !== undefined) {
            try { closeSync(directoryDescriptor); } catch {}
        }
    }
}

function fsyncInlayDirectorySync() {
    return fsyncDirectoryPathSync(inlayDir);
}

function newInlayTempPath(label) {
    const tempPath = path.join(
        inlayDir,
        `${INLAY_TEMP_PREFIX}${process.pid}-${nodeCrypto.randomUUID()}-${label}`,
    );
    assertInsideInlayDir(tempPath);
    return tempPath;
}

function isInlayTemporaryFileName(name) {
    return typeof name === 'string' && INLAY_TEMP_NAME_PATTERN.test(name);
}

async function writeDurableInlayTempFile(filePath, value) {
    let handle;
    try {
        handle = await fs.open(filePath, 'wx', 0o600);
        await handle.writeFile(value);
        await handle.sync();
    } finally {
        await handle?.close().catch(() => {});
    }
}

async function copyDurableInlayTempFile(sourcePath, filePath) {
    let source;
    let destination;
    try {
        source = await fs.open(sourcePath, 'r');
        const stat = await source.stat();
        if (!stat.isFile()) throw new Error('Inlay spool source must be a regular file');
        destination = await fs.open(filePath, 'wx', 0o600);
        const page = Buffer.allocUnsafe(256 * 1024);
        let position = 0;
        while (position < stat.size) {
            const length = Math.min(page.length, stat.size - position);
            const result = await source.read(page, 0, length, position);
            if (result.bytesRead !== length) throw new Error('Inlay spool changed during publication');
            let written = 0;
            while (written < length) {
                const output = await destination.write(page, written, length - written);
                if (output.bytesWritten <= 0) {
                    throw new Error('Inlay spool publication made no progress');
                }
                written += output.bytesWritten;
            }
            position += length;
        }
        await destination.sync();
    } finally {
        await source?.close().catch(() => {});
        await destination?.close().catch(() => {});
    }
}

function writeDurableInlayTempFileSync(filePath, value) {
    let descriptor;
    try {
        descriptor = openSync(filePath, 'wx', 0o600);
        writeFileSync(descriptor, value);
        fsyncSync(descriptor);
    } finally {
        if (descriptor !== undefined) {
            try { closeSync(descriptor); } catch {}
        }
    }
}

async function reachInlayPublishTestBoundary(stage, id) {
    if (inlayPublishFailpoint === stage) {
        throw new Error(`Injected inlay publication failure at ${stage}`);
    }
    if (!inlayPublishTestGateDir || inlayPublishTestGateStage !== stage) return;
    const holdPath = path.join(inlayPublishTestGateDir, 'hold');
    if (!existsSync(holdPath)) return;
    await fs.mkdir(inlayPublishTestGateDir, { recursive: true });
    await fs.writeFile(
        path.join(inlayPublishTestGateDir, 'entered'),
        JSON.stringify({ stage, id }),
        'utf-8',
    );
    const releasePath = path.join(inlayPublishTestGateDir, 'release');
    while (existsSync(holdPath) && !existsSync(releasePath)) {
        await new Promise((resolve) => setTimeout(resolve, 10));
    }
}

function inlaySidecarValue(id, info) {
    return Buffer.from(JSON.stringify({
        ext: normalizeInlayExt(info?.ext),
        name: typeof info?.name === 'string' ? info.name : id,
        type: typeof info?.type === 'string' ? info.type : 'image',
        height: typeof info?.height === 'number' ? info.height : undefined,
        width: typeof info?.width === 'number' ? info.width : undefined,
    }));
}

async function reconcileInterruptedInlayPublications() {
    await ensureInlayDir();
    const entries = await fs.readdir(inlayDir, { withFileTypes: true });
    let removedTemporaryFile = false;
    for (const entry of entries) {
        if (!entry.isFile() || !isInlayTemporaryFileName(entry.name)) continue;
        await fs.unlink(path.join(inlayDir, entry.name)).catch((error) => {
            if (error?.code !== 'ENOENT') throw error;
        });
        removedTemporaryFile = true;
    }
    if (removedTemporaryFile) await fsyncInlayDirectory();
}

function getMimeFromExt(ext, buffer) {
    return ASSET_EXT_MIME[normalizeInlayExt(ext)] || detectMime(buffer);
}

function decodeDataUri(dataUri) {
    if (typeof dataUri !== 'string' || !dataUri.startsWith('data:')) {
        throw new Error('Invalid data URI');
    }
    const commaIdx = dataUri.indexOf(',');
    if (commaIdx === -1) {
        throw new Error('Malformed data URI');
    }
    const meta = dataUri.substring(5, commaIdx);
    return {
        buffer: Buffer.from(dataUri.substring(commaIdx + 1), 'base64'),
        mime: meta.split(';')[0] || 'application/octet-stream',
    };
}

function encodeDataUri(buffer, mime) {
    return `data:${mime || 'application/octet-stream'};base64,${Buffer.from(buffer).toString('base64')}`;
}

function parseInlaySidecarData(raw, id) {
    try {
        const parsed = JSON.parse(Buffer.isBuffer(raw) ? raw.toString('utf-8') : raw);
        const ext = normalizedInlayTupleExtension(id, parsed?.ext);
        if (ext === null) return null;
        return {
            ext,
            name: typeof parsed?.name === 'string' ? parsed.name : id,
            type: typeof parsed?.type === 'string' ? parsed.type : 'image',
            height: typeof parsed?.height === 'number' ? parsed.height : undefined,
            width: typeof parsed?.width === 'number' ? parsed.width : undefined,
        };
    } catch {
        return null;
    }
}

async function exactRegularFileExists(filePath) {
    try {
        const entries = await fs.readdir(path.dirname(filePath), { withFileTypes: true });
        return entries.some((entry) => entry.name === path.basename(filePath) && entry.isFile());
    } catch (error) {
        if (error?.code === 'ENOENT') return false;
        throw error;
    }
}

function exactRegularFileExistsSync(filePath) {
    try {
        return readdirSync(path.dirname(filePath), { withFileTypes: true })
            .some((entry) => entry.name === path.basename(filePath) && entry.isFile());
    } catch (error) {
        if (error?.code === 'ENOENT') return false;
        throw error;
    }
}

async function readSidecarFileState(filePath, id) {
    if (!await exactRegularFileExists(filePath)) {
        return { exists: false, info: null, filePath };
    }
    try {
        return {
            exists: true,
            info: parseInlaySidecarData(await fs.readFile(filePath), id),
            filePath,
        };
    } catch (error) {
        if (error?.code === 'ENOENT') return { exists: false, info: null, filePath };
        return { exists: true, info: null, filePath };
    }
}

function readSidecarFileStateSync(filePath, id) {
    if (!exactRegularFileExistsSync(filePath)) {
        return { exists: false, info: null, filePath };
    }
    try {
        return {
            exists: true,
            info: parseInlaySidecarData(readFileSync(filePath), id),
            filePath,
        };
    } catch (error) {
        if (error?.code === 'ENOENT') return { exists: false, info: null, filePath };
        return { exists: true, info: null, filePath };
    }
}

async function readLegacyInlaySidecarState(id, seen = new Set()) {
    const filePath = getLegacyInlaySidecarPath(id);
    const own = await readSidecarFileState(filePath, id);
    if (!own.exists || seen.has(id)) return own;
    const claimantId = `${id}.meta`;
    if (isSafeInlayId(claimantId)) {
        const nextSeen = new Set(seen);
        nextSeen.add(id);
        const claimant = await readLegacyInlaySidecarState(claimantId, nextSeen);
        if (claimant.info?.ext === 'json') {
            return { exists: true, info: null, filePath, claimedAsPayload: true };
        }
    }
    return own;
}

function readLegacyInlaySidecarStateSync(id, seen = new Set()) {
    const filePath = getLegacyInlaySidecarPath(id);
    const own = readSidecarFileStateSync(filePath, id);
    if (!own.exists || seen.has(id)) return own;
    const claimantId = `${id}.meta`;
    if (isSafeInlayId(claimantId)) {
        const nextSeen = new Set(seen);
        nextSeen.add(id);
        const claimant = readLegacyInlaySidecarStateSync(claimantId, nextSeen);
        if (claimant.info?.ext === 'json') {
            return { exists: true, info: null, filePath, claimedAsPayload: true };
        }
    }
    return own;
}

async function readInlaySidecarState(id) {
    const canonical = await readSidecarFileState(getInlaySidecarPath(id), id);
    if (canonical.exists) return { ...canonical, canonical: true };
    const legacy = await readLegacyInlaySidecarState(id);
    return { ...legacy, canonical: false };
}

function readInlaySidecarStateSync(id) {
    const canonical = readSidecarFileStateSync(getInlaySidecarPath(id), id);
    if (canonical.exists) return { ...canonical, canonical: true };
    const legacy = readLegacyInlaySidecarStateSync(id);
    return { ...legacy, canonical: false };
}

async function readInlaySidecar(id) {
    if (!isSafeInlayId(id)) return null;
    return (await readInlaySidecarState(id)).info;
}

function parseCanonicalInlayPayloadPath(filePath, payloadDir = inlayCanonicalPayloadDir) {
    const relative = path.relative(payloadDir, filePath);
    if (!relative || relative.startsWith('..') || path.isAbsolute(relative)) return null;
    const segments = relative.split(path.sep);
    if (segments[0] !== INLAY_CANONICAL_ID_MARKER
        || segments.at(-1) !== INLAY_CANONICAL_PAYLOAD_FILE) return null;
    const extMarker = segments.indexOf(INLAY_CANONICAL_EXT_MARKER, 2);
    if (extMarker < 2 || extMarker >= segments.length - 2) return null;
    const id = decodeInlayPhysicalChunks(segments.slice(1, extMarker));
    const ext = decodeInlayPhysicalChunks(segments.slice(extMarker + 1, -1));
    if (!isSafeInlayTuple(id, ext) || normalizeInlayExt(ext) !== ext) return null;
    return { id, ext };
}

function parseCanonicalInlaySidecarPath(filePath, sidecarDir = inlayCanonicalSidecarDir) {
    const relative = path.relative(sidecarDir, filePath);
    if (!relative || relative.startsWith('..') || path.isAbsolute(relative)) return null;
    const segments = relative.split(path.sep);
    if (segments[0] !== INLAY_CANONICAL_ID_MARKER
        || segments.at(-1) !== INLAY_CANONICAL_SIDECAR_FILE) return null;
    const id = decodeInlayPhysicalChunks(segments.slice(1, -1));
    return isSafeInlayId(id) ? { id } : null;
}

async function listRegularFilesRecursive(directory) {
    const files = [];
    const pending = [directory];
    while (pending.length > 0) {
        const current = pending.pop();
        let entries;
        try {
            entries = await fs.readdir(current, { withFileTypes: true });
        } catch (error) {
            if (error?.code === 'ENOENT') continue;
            throw error;
        }
        for (const entry of entries) {
            const entryPath = path.join(current, entry.name);
            if (entry.isDirectory()) pending.push(entryPath);
            else if (entry.isFile()) files.push(entryPath);
        }
    }
    return files;
}

function listRegularFilesRecursiveSync(directory) {
    const files = [];
    const pending = [directory];
    while (pending.length > 0) {
        const current = pending.pop();
        let entries;
        try {
            entries = readdirSync(current, { withFileTypes: true });
        } catch (error) {
            if (error?.code === 'ENOENT') continue;
            throw error;
        }
        for (const entry of entries) {
            const entryPath = path.join(current, entry.name);
            if (entry.isDirectory()) pending.push(entryPath);
            else if (entry.isFile()) files.push(entryPath);
        }
    }
    return files;
}

function parseLegacyInlayPayloadName(name) {
    if (typeof name !== 'string'
        || name === '.migrated_to_fs'
        || isInlayTemporaryFileName(name)
        || name.endsWith('.meta.json')) return null;
    const dot = name.lastIndexOf('.');
    if (dot <= 0 || dot === name.length - 1) return null;
    const id = name.slice(0, dot);
    const rawExt = name.slice(dot + 1);
    if (!isSafeInlayTuple(id, rawExt) || normalizeInlayExt(rawExt) !== rawExt) return null;
    return { id, ext: rawExt };
}

async function listCanonicalInlayPayloads(id = null) {
    return (await listRegularFilesRecursive(inlayCanonicalPayloadDir))
        .map((filePath) => ({ filePath, parsed: parseCanonicalInlayPayloadPath(filePath) }))
        .filter(({ parsed }) => parsed && (id === null || parsed.id === id))
        .map(({ filePath, parsed }) => ({
            ...parsed,
            filePath,
            canonical: true,
        }))
        .sort((left, right) => left.filePath.localeCompare(right.filePath));
}

function listCanonicalInlayPayloadsSync(id = null) {
    return listRegularFilesRecursiveSync(inlayCanonicalPayloadDir)
        .map((filePath) => ({ filePath, parsed: parseCanonicalInlayPayloadPath(filePath) }))
        .filter(({ parsed }) => parsed && (id === null || parsed.id === id))
        .map(({ filePath, parsed }) => ({
            ...parsed,
            filePath,
            canonical: true,
        }))
        .sort((left, right) => left.filePath.localeCompare(right.filePath));
}

async function listCanonicalInlaySidecars() {
    return Promise.all((await listRegularFilesRecursive(inlayCanonicalSidecarDir))
        .map(async (filePath) => {
            const parsed = parseCanonicalInlaySidecarPath(filePath);
            if (!parsed) return null;
            let info = null;
            try {
                info = parseInlaySidecarData(await fs.readFile(filePath), parsed.id);
            } catch {}
            return { ...parsed, filePath, info, canonical: true };
        }))
        .then((states) => states.filter(Boolean));
}

async function legacyPayloadCandidateFromInfo(id, info) {
    if (!info) return null;
    const filePath = getLegacyInlayFilePath(id, info.ext);
    if (filePath === getLegacyInlaySidecarPath(id)
        || !await exactRegularFileExists(filePath)) return null;
    if (path.basename(filePath).endsWith('.meta.json')) {
        const possibleSidecarId = path.basename(filePath).slice(0, -'.meta.json'.length);
        if (isSafeInlayId(possibleSidecarId)) {
            const sidecar = await readLegacyInlaySidecarState(possibleSidecarId);
            if (sidecar.info && sidecar.filePath === filePath) return null;
        }
    }
    return { id, ext: normalizeInlayExt(info.ext), filePath, canonical: false };
}

function legacyPayloadCandidateFromInfoSync(id, info) {
    if (!info) return null;
    const filePath = getLegacyInlayFilePath(id, info.ext);
    if (filePath === getLegacyInlaySidecarPath(id)
        || !exactRegularFileExistsSync(filePath)) return null;
    if (path.basename(filePath).endsWith('.meta.json')) {
        const possibleSidecarId = path.basename(filePath).slice(0, -'.meta.json'.length);
        if (isSafeInlayId(possibleSidecarId)) {
            const sidecar = readLegacyInlaySidecarStateSync(possibleSidecarId);
            if (sidecar.info && sidecar.filePath === filePath) return null;
        }
    }
    return { id, ext: normalizeInlayExt(info.ext), filePath, canonical: false };
}

async function listLegacyFallbackPayloads(id = null) {
    let entries;
    try {
        entries = await fs.readdir(inlayDir, { withFileTypes: true });
    } catch (error) {
        if (error?.code === 'ENOENT') return [];
        throw error;
    }
    return entries
        .filter((entry) => entry.isFile())
        .map((entry) => ({ entry, parsed: parseLegacyInlayPayloadName(entry.name) }))
        .filter(({ parsed }) => parsed && (id === null || parsed.id === id))
        .map(({ entry, parsed }) => ({
            ...parsed,
            filePath: path.join(inlayDir, entry.name),
            canonical: false,
        }))
        .sort((left, right) => left.filePath.localeCompare(right.filePath));
}

function listLegacyFallbackPayloadsSync(id = null) {
    let entries;
    try {
        entries = readdirSync(inlayDir, { withFileTypes: true });
    } catch (error) {
        if (error?.code === 'ENOENT') return [];
        throw error;
    }
    return entries
        .filter((entry) => entry.isFile())
        .map((entry) => ({ entry, parsed: parseLegacyInlayPayloadName(entry.name) }))
        .filter(({ parsed }) => parsed && (id === null || parsed.id === id))
        .map(({ entry, parsed }) => ({
            ...parsed,
            filePath: path.join(inlayDir, entry.name),
            canonical: false,
        }))
        .sort((left, right) => left.filePath.localeCompare(right.filePath));
}

async function listLegacyInlayPayloads(id) {
    const sidecar = await readLegacyInlaySidecarState(id);
    const evidenced = await legacyPayloadCandidateFromInfo(id, sidecar.info);
    const fallback = await listLegacyFallbackPayloads(id);
    const byPath = new Map(fallback.map((entry) => [entry.filePath, entry]));
    if (evidenced) byPath.set(evidenced.filePath, evidenced);
    return [...byPath.values()].sort((left, right) => left.filePath.localeCompare(right.filePath));
}

function listLegacyInlayPayloadsSync(id) {
    const sidecar = readLegacyInlaySidecarStateSync(id);
    const evidenced = legacyPayloadCandidateFromInfoSync(id, sidecar.info);
    const fallback = listLegacyFallbackPayloadsSync(id);
    const byPath = new Map(fallback.map((entry) => [entry.filePath, entry]));
    if (evidenced) byPath.set(evidenced.filePath, evidenced);
    return [...byPath.values()].sort((left, right) => left.filePath.localeCompare(right.filePath));
}

async function listAllInlayPayloadsForId(id) {
    return [
        ...await listCanonicalInlayPayloads(id),
        ...await listLegacyInlayPayloads(id),
    ];
}

function listAllInlayPayloadsForIdSync(id) {
    return [
        ...listCanonicalInlayPayloadsSync(id),
        ...listLegacyInlayPayloadsSync(id),
    ];
}

async function resolveInlayPayload(id) {
    if (!isSafeInlayId(id)) return null;
    const canonicalSidecar = await readSidecarFileState(getInlaySidecarPath(id), id);
    const canonicalPayloads = await listCanonicalInlayPayloads(id);
    const legacySidecar = await readLegacyInlaySidecarState(id);

    if (canonicalSidecar.info) {
        const preferred = canonicalPayloads.find((entry) => entry.ext === canonicalSidecar.info.ext);
        if (preferred) return preferred;
        if (canonicalPayloads.length > 0) return canonicalPayloads[0];
        const legacy = await legacyPayloadCandidateFromInfo(id, canonicalSidecar.info);
        if (legacy) return legacy;
    } else if (!canonicalSidecar.exists && legacySidecar.info) {
        // A deployed legacy sidecar remains the commit point until the canonical
        // sidecar is published. This keeps a crash after payload rename from
        // exposing an incomplete replacement.
        const legacy = await legacyPayloadCandidateFromInfo(id, legacySidecar.info);
        if (legacy) return legacy;
        if (canonicalPayloads.length > 0) return canonicalPayloads[0];
    } else if (canonicalPayloads.length > 0) {
        return canonicalPayloads[0];
    }

    const legacyFallbacks = await listLegacyFallbackPayloads(id);
    return legacyFallbacks[0] || null;
}

function resolveInlayPayloadSync(id) {
    if (!isSafeInlayId(id)) return null;
    const canonicalSidecar = readSidecarFileStateSync(getInlaySidecarPath(id), id);
    const canonicalPayloads = listCanonicalInlayPayloadsSync(id);
    const legacySidecar = readLegacyInlaySidecarStateSync(id);

    if (canonicalSidecar.info) {
        const preferred = canonicalPayloads.find((entry) => entry.ext === canonicalSidecar.info.ext);
        if (preferred) return preferred;
        if (canonicalPayloads.length > 0) return canonicalPayloads[0];
        const legacy = legacyPayloadCandidateFromInfoSync(id, canonicalSidecar.info);
        if (legacy) return legacy;
    } else if (!canonicalSidecar.exists && legacySidecar.info) {
        const legacy = legacyPayloadCandidateFromInfoSync(id, legacySidecar.info);
        if (legacy) return legacy;
        if (canonicalPayloads.length > 0) return canonicalPayloads[0];
    } else if (canonicalPayloads.length > 0) {
        return canonicalPayloads[0];
    }

    return listLegacyFallbackPayloadsSync(id)[0] || null;
}

async function resolveInlayFilePath(id) {
    return (await resolveInlayPayload(id))?.filePath || null;
}

function resolveInlayFilePathSync(id) {
    return resolveInlayPayloadSync(id)?.filePath || null;
}

async function resolveInlaySidecarPath(id) {
    const state = await readInlaySidecarState(id);
    return state.info ? state.filePath : null;
}

async function readInlayFile(id) {
    const payload = await resolveInlayPayload(id);
    if (!payload) return null;
    const { filePath, ext } = payload;
    const buffer = await fs.readFile(filePath);
    const stat = await fs.stat(filePath);
    return {
        buffer,
        ext,
        filePath,
        mtimeMs: stat.mtimeMs,
        mime: getMimeFromExt(ext, buffer),
    };
}

async function writeInlaySidecar(id, info) {
    const normalizedExt = assertSafeInlayTuple(id, info?.ext);
    const normalizedInfo = { ...(info || {}), ext: normalizedExt };
    await ensureInlayDir();
    assertCanonicalInlayWriteTargets(inlayDir, id);
    const temporaryPath = newInlayTempPath('sidecar');
    try {
        await writeDurableInlayTempFile(temporaryPath, inlaySidecarValue(id, normalizedInfo));
        const destinationPath = getInlaySidecarPath(id);
        await fs.rename(temporaryPath, destinationPath);
        await fsyncDirectoryPath(path.dirname(destinationPath));
        const legacy = await readLegacyInlaySidecarState(id);
        if (legacy.info) {
            try {
                await fs.unlink(legacy.filePath);
                await fsyncInlayDirectory();
            } catch (error) {
                if (error?.code !== 'ENOENT') {
                    logger.warn(
                        `[InlayFS] Failed to remove legacy sidecar for ${id}:`,
                        error?.message || error,
                    );
                }
            }
        }
    } finally {
        await fs.unlink(temporaryPath).catch(() => {});
    }
}

function writeInlaySidecarSync(id, info) {
    const normalizedExt = assertSafeInlayTuple(id, info?.ext);
    const normalizedInfo = { ...(info || {}), ext: normalizedExt };
    ensureInlayDirSync();
    assertCanonicalInlayWriteTargets(inlayDir, id);
    const temporaryPath = newInlayTempPath('sidecar');
    try {
        writeDurableInlayTempFileSync(temporaryPath, inlaySidecarValue(id, normalizedInfo));
        const destinationPath = getInlaySidecarPath(id);
        renameSync(temporaryPath, destinationPath);
        fsyncDirectoryPathSync(path.dirname(destinationPath));
        const legacy = readLegacyInlaySidecarStateSync(id);
        if (legacy.info) {
            try {
                unlinkSync(legacy.filePath);
                fsyncInlayDirectorySync();
            } catch (error) {
                if (error?.code !== 'ENOENT') {
                    logger.warn(
                        `[InlayFS] Failed to remove legacy sidecar for ${id}:`,
                        error?.message || error,
                    );
                }
            }
        }
    } finally {
        try { unlinkSync(temporaryPath); } catch {}
    }
}

async function removeObsoleteInlayFiles(id, destinationPath) {
    const obsolete = (await listAllInlayPayloadsForId(id))
        .filter((entry) => entry.filePath !== destinationPath);
    const touchedDirectories = new Set();
    for (const entry of obsolete) {
        try {
            await fs.unlink(entry.filePath);
            touchedDirectories.add(path.dirname(entry.filePath));
        } catch (error) {
            if (error?.code !== 'ENOENT') throw error;
        }
    }
    const legacySidecar = await readLegacyInlaySidecarState(id);
    if (legacySidecar.info) {
        try {
            await fs.unlink(legacySidecar.filePath);
            touchedDirectories.add(path.dirname(legacySidecar.filePath));
        } catch (error) {
            if (error?.code !== 'ENOENT') throw error;
        }
    }
    for (const directory of touchedDirectories) await fsyncDirectoryPath(directory);
}

function removeObsoleteInlayFilesSync(id, destinationPath) {
    const obsolete = listAllInlayPayloadsForIdSync(id)
        .filter((entry) => entry.filePath !== destinationPath);
    const touchedDirectories = new Set();
    for (const entry of obsolete) {
        try {
            unlinkSync(entry.filePath);
            touchedDirectories.add(path.dirname(entry.filePath));
        } catch (error) {
            if (error?.code !== 'ENOENT') throw error;
        }
    }
    const legacySidecar = readLegacyInlaySidecarStateSync(id);
    if (legacySidecar.info) {
        try {
            unlinkSync(legacySidecar.filePath);
            touchedDirectories.add(path.dirname(legacySidecar.filePath));
        } catch (error) {
            if (error?.code !== 'ENOENT') throw error;
        }
    }
    for (const directory of touchedDirectories) fsyncDirectoryPathSync(directory);
}

async function writeInlayFile(id, ext, buffer, info = null) {
    const normalizedExt = assertSafeInlayTuple(id, ext);
    await ensureInlayDir();
    assertCanonicalInlayWriteTargets(inlayDir, id, normalizedExt);
    const destinationPath = getInlayFilePath(id, normalizedExt);
    const sidecarPath = getInlaySidecarPath(id);
    const previousPath = await resolveInlayFilePath(id);
    const payloadTemporaryPath = newInlayTempPath('payload');
    const sidecarTemporaryPath = newInlayTempPath('sidecar');
    const sidecarValue = inlaySidecarValue(id, {
        ...(info || {}),
        ext: normalizedExt,
    });
    let payloadPublished = false;
    let sidecarPublished = false;
    try {
        // Stage and sync both files before changing any reader-visible path.
        // ENOSPC and encoding/write failures therefore leave the old inlay
        // completely untouched.
        await writeDurableInlayTempFile(payloadTemporaryPath, Buffer.from(buffer));
        await writeDurableInlayTempFile(sidecarTemporaryPath, sidecarValue);
        await reachInlayPublishTestBoundary('before-payload-publish', id);

        // Publish the payload first while the prior sidecar and prior-extension
        // payload remain authoritative. The sidecar rename below is the commit
        // point for extension-changing replacements.
        await fs.rename(payloadTemporaryPath, destinationPath);
        payloadPublished = true;
        await fsyncDirectoryPath(path.dirname(destinationPath));
        await reachInlayPublishTestBoundary('after-payload-publish', id);

        await fs.rename(sidecarTemporaryPath, sidecarPath);
        sidecarPublished = true;
        await fsyncDirectoryPath(path.dirname(sidecarPath));

        // Only a committed sidecar can make the prior extension obsolete.
        // Failures here retain an extra recoverable copy rather than removing
        // the only valid one.
        try {
            await removeObsoleteInlayFiles(id, destinationPath);
        } catch (error) {
            logger.warn(`[InlayFS] Failed to remove obsolete files for ${id}:`, error?.message || error);
        }
    } catch (error) {
        // If an extension-changing replacement did not reach its sidecar commit
        // point, roll back its newly visible orphan. The prior sidecar-selected
        // source remains untouched. Same-extension rename is already atomic and
        // therefore still leaves one complete payload.
        if (payloadPublished && !sidecarPublished && previousPath !== destinationPath) {
            try {
                await fs.unlink(destinationPath);
                await fsyncDirectoryPath(path.dirname(destinationPath));
            } catch (rollbackError) {
                if (rollbackError?.code !== 'ENOENT') {
                    logger.warn(
                        `[InlayFS] Failed to roll back unpublished payload for ${id}:`,
                        rollbackError?.message || rollbackError,
                    );
                }
            }
        }
        throw error;
    } finally {
        await fs.unlink(payloadTemporaryPath).catch(() => {});
        await fs.unlink(sidecarTemporaryPath).catch(() => {});
    }
    kvClearDeletion(`inlay/${id}`);
}

async function writeInlayFileFromFile(id, ext, sourcePath, info = null) {
    const normalizedExt = assertSafeInlayTuple(id, ext);
    await ensureInlayDir();
    assertCanonicalInlayWriteTargets(inlayDir, id, normalizedExt);
    const destinationPath = getInlayFilePath(id, normalizedExt);
    const sidecarPath = getInlaySidecarPath(id);
    const previousPath = await resolveInlayFilePath(id);
    const payloadTemporaryPath = newInlayTempPath('payload');
    const sidecarTemporaryPath = newInlayTempPath('sidecar');
    const sidecarValue = inlaySidecarValue(id, {
        ...(info || {}),
        ext: normalizedExt,
    });
    let payloadPublished = false;
    let sidecarPublished = false;
    try {
        await copyDurableInlayTempFile(sourcePath, payloadTemporaryPath);
        await writeDurableInlayTempFile(sidecarTemporaryPath, sidecarValue);
        await reachInlayPublishTestBoundary('before-payload-publish', id);
        await fs.rename(payloadTemporaryPath, destinationPath);
        payloadPublished = true;
        await fsyncDirectoryPath(path.dirname(destinationPath));
        await reachInlayPublishTestBoundary('after-payload-publish', id);
        await fs.rename(sidecarTemporaryPath, sidecarPath);
        sidecarPublished = true;
        await fsyncDirectoryPath(path.dirname(sidecarPath));
        try {
            await removeObsoleteInlayFiles(id, destinationPath);
        } catch (error) {
            logger.warn(`[InlayFS] Failed to remove obsolete files for ${id}:`, error?.message || error);
        }
    } catch (error) {
        if (payloadPublished && !sidecarPublished && previousPath !== destinationPath) {
            try {
                await fs.unlink(destinationPath);
                await fsyncDirectoryPath(path.dirname(destinationPath));
            } catch (rollbackError) {
                if (rollbackError?.code !== 'ENOENT') {
                    logger.warn(
                        `[InlayFS] Failed to roll back unpublished payload for ${id}:`,
                        rollbackError?.message || rollbackError,
                    );
                }
            }
        }
        throw error;
    } finally {
        await fs.unlink(payloadTemporaryPath).catch(() => {});
        await fs.unlink(sidecarTemporaryPath).catch(() => {});
    }
    kvClearDeletion(`inlay/${id}`);
}

function writeInlayFileSync(id, ext, buffer, info = null) {
    const normalizedExt = assertSafeInlayTuple(id, ext);
    ensureInlayDirSync();
    assertCanonicalInlayWriteTargets(inlayDir, id, normalizedExt);
    const destinationPath = getInlayFilePath(id, normalizedExt);
    const sidecarPath = getInlaySidecarPath(id);
    const previousPath = resolveInlayFilePathSync(id);
    const payloadTemporaryPath = newInlayTempPath('payload');
    const sidecarTemporaryPath = newInlayTempPath('sidecar');
    const sidecarValue = inlaySidecarValue(id, {
        ...(info || {}),
        ext: normalizedExt,
    });
    let payloadPublished = false;
    let sidecarPublished = false;
    try {
        writeDurableInlayTempFileSync(payloadTemporaryPath, Buffer.from(buffer));
        writeDurableInlayTempFileSync(sidecarTemporaryPath, sidecarValue);
        renameSync(payloadTemporaryPath, destinationPath);
        payloadPublished = true;
        fsyncDirectoryPathSync(path.dirname(destinationPath));
        renameSync(sidecarTemporaryPath, sidecarPath);
        sidecarPublished = true;
        fsyncDirectoryPathSync(path.dirname(sidecarPath));
        try {
            removeObsoleteInlayFilesSync(id, destinationPath);
        } catch (error) {
            logger.warn(`[InlayFS] Failed to remove obsolete files for ${id}:`, error?.message || error);
        }
    } catch (error) {
        if (payloadPublished && !sidecarPublished && previousPath !== destinationPath) {
            try {
                unlinkSync(destinationPath);
                fsyncDirectoryPathSync(path.dirname(destinationPath));
            } catch (rollbackError) {
                if (rollbackError?.code !== 'ENOENT') {
                    logger.warn(
                        `[InlayFS] Failed to roll back unpublished payload for ${id}:`,
                        rollbackError?.message || rollbackError,
                    );
                }
            }
        }
        throw error;
    } finally {
        try { unlinkSync(payloadTemporaryPath); } catch {}
        try { unlinkSync(sidecarTemporaryPath); } catch {}
    }
    kvClearDeletion(`inlay/${id}`);
}

async function deleteInlayRawFile(id) {
    const touchedDirectories = new Set();
    for (const entry of await listAllInlayPayloadsForId(id)) {
        try {
            await fs.unlink(entry.filePath);
            touchedDirectories.add(path.dirname(entry.filePath));
        } catch (error) {
            if (error?.code !== 'ENOENT') throw error;
        }
    }
    for (const directory of touchedDirectories) await fsyncDirectoryPath(directory);
}

function deleteInlayRawFileSync(id) {
    const touchedDirectories = new Set();
    for (const entry of listAllInlayPayloadsForIdSync(id)) {
        try {
            unlinkSync(entry.filePath);
            touchedDirectories.add(path.dirname(entry.filePath));
        } catch (error) {
            if (error?.code !== 'ENOENT') throw error;
        }
    }
    for (const directory of touchedDirectories) fsyncDirectoryPathSync(directory);
}

async function deleteInlayFile(id) {
    await deleteInlayRawFile(id);
    await deleteInlaySidecars(id);
}

async function deleteInlaySidecars(id) {
    const sidecarPaths = [getInlaySidecarPath(id)];
    const legacy = await readLegacyInlaySidecarState(id);
    if (legacy.info) sidecarPaths.push(legacy.filePath);
    const touchedDirectories = new Set();
    for (const sidecarPath of new Set(sidecarPaths)) {
        try {
            await fs.unlink(sidecarPath);
            touchedDirectories.add(path.dirname(sidecarPath));
        } catch (error) {
            if (error?.code !== 'ENOENT') throw error;
        }
    }
    for (const directory of touchedDirectories) await fsyncDirectoryPath(directory);
}

function deleteInlayFileSync(id) {
    deleteInlayRawFileSync(id);
    deleteInlaySidecarsSync(id);
}

function deleteInlaySidecarsSync(id) {
    const sidecarPaths = [getInlaySidecarPath(id)];
    const legacy = readLegacyInlaySidecarStateSync(id);
    if (legacy.info) sidecarPaths.push(legacy.filePath);
    const touchedDirectories = new Set();
    for (const sidecarPath of new Set(sidecarPaths)) {
        try {
            unlinkSync(sidecarPath);
            touchedDirectories.add(path.dirname(sidecarPath));
        } catch (error) {
            if (error?.code !== 'ENOENT') throw error;
        }
    }
    for (const directory of touchedDirectories) fsyncDirectoryPathSync(directory);
}

async function listInlayFiles() {
    await ensureInlayDir();
    const canonical = await listCanonicalInlayPayloads();
    const canonicalSidecars = new Map(
        (await listCanonicalInlaySidecars()).map((entry) => [entry.id, entry]),
    );
    const rootEntries = await fs.readdir(inlayDir, { withFileTypes: true });
    const rootFileNames = new Set(
        rootEntries.filter((entry) => entry.isFile()).map((entry) => entry.name),
    );
    const canonicalById = new Map();
    for (const entry of canonical) {
        const group = canonicalById.get(entry.id) || [];
        group.push(entry);
        canonicalById.set(entry.id, group);
    }
    const resolvedCanonical = [];
    for (const [id, group] of canonicalById) {
        const sidecar = canonicalSidecars.get(id);
        if (sidecar) {
            resolvedCanonical.push(
                group.find((entry) => entry.ext === sidecar.info?.ext) || group[0],
            );
        } else if (rootFileNames.has(`${id}.meta.json`)) {
            const resolved = await resolveInlayPayload(id);
            if (resolved) resolvedCanonical.push(resolved);
        } else {
            resolvedCanonical.push(group[0]);
        }
    }

    const legacyIds = new Set();
    for (const entry of await listLegacyFallbackPayloads()) legacyIds.add(entry.id);
    for (const entry of rootEntries) {
        if (!entry.isFile() || !entry.name.endsWith('.meta.json')) continue;
        const id = entry.name.slice(0, -'.meta.json'.length);
        if (isSafeInlayId(id)) legacyIds.add(id);
    }
    for (const id of canonicalById.keys()) legacyIds.delete(id);
    const resolvedLegacy = await Promise.all(
        [...legacyIds].map((id) => resolveInlayPayload(id)),
    );
    return [...resolvedCanonical, ...resolvedLegacy]
        .filter(Boolean)
        .sort((left, right) => left.id.localeCompare(right.id));
}

async function readInlayLegacyInfo(id) {
    const value = kvGet(`inlay_info/${id}`);
    if (!value) return null;
    try {
        const parsed = JSON.parse(value.toString('utf-8'));
        return {
            ext: normalizeInlayExt(parsed?.ext),
            name: typeof parsed?.name === 'string' ? parsed.name : id,
            type: typeof parsed?.type === 'string' ? parsed.type : 'image',
            height: typeof parsed?.height === 'number' ? parsed.height : undefined,
            width: typeof parsed?.width === 'number' ? parsed.width : undefined,
        };
    } catch {
        return null;
    }
}

async function readInlayInfoPayload(id) {
    const sidecar = await readInlaySidecar(id);
    if (sidecar) return Buffer.from(JSON.stringify(sidecar));
    const legacy = await readInlayLegacyInfo(id);
    if (legacy) return Buffer.from(JSON.stringify(legacy));
    return kvGet(`inlay_info/${id}`);
}

async function readInlayAssetPayload(id) {
    const file = await readInlayFile(id);
    if (!file) return null;
    const sidecar = (await readInlaySidecar(id)) || (await readInlayLegacyInfo(id));
    const info = {
        ext: sidecar?.ext || file.ext,
        name: sidecar?.name || id,
        type: sidecar?.type || 'image',
        height: sidecar?.height,
        width: sidecar?.width,
    };
    const data = info.type === 'signature'
        ? file.buffer.toString('utf-8')
        : encodeDataUri(file.buffer, file.mime);
    return Buffer.from(JSON.stringify({
        ...info,
        data,
    }));
}

async function canonicalizeLegacyInlayFiles() {
    const entries = await listInlayFiles();
    for (const entry of entries) {
        try {
            if (entry.canonical) {
                const canonicalSidecar = await readSidecarFileState(
                    getInlaySidecarPath(entry.id),
                    entry.id,
                );
                if (canonicalSidecar.exists) continue;
            }
            const info = (await readInlaySidecar(entry.id)) || {
                ext: entry.ext,
                name: entry.id,
                type: 'image',
            };
            await writeInlayFileFromFile(entry.id, entry.ext, entry.filePath, {
                ...info,
                ext: entry.ext,
            });
        } catch (error) {
            logger.warn(
                `[InlayFS] Failed to canonicalize legacy inlay ${entry.id}:`,
                error?.message || error,
            );
        }
    }
}

async function migrateInlaysToFilesystem() {
    await reconcileInterruptedInlayPublications();
    await canonicalizeLegacyInlayFiles();
    const keys = kvList('inlay/');
    if (keys.length === 0) {
        if (!existsSync(inlayMigrationMarker)) {
            await fs.writeFile(inlayMigrationMarker, new Date().toISOString(), 'utf-8');
        }
        return;
    }

    for (const key of keys) {
        const id = key.slice('inlay/'.length);
        if (!isSafeInlayId(id)) {
            logger.warn(`[InlayFS] Cannot migrate unsafe legacy key ${JSON.stringify(key)}`);
            continue;
        }
        const fileAlreadyExists = await readInlayFile(id);
        if (fileAlreadyExists) {
            try {
                const sidecar = await readInlaySidecar(id);
                if (!sidecar || normalizeInlayExt(sidecar.ext) !== fileAlreadyExists.ext) {
                    const legacyInfo = await readInlayLegacyInfo(id);
                    await writeInlaySidecar(id, {
                        ...(legacyInfo || {}),
                        ext: fileAlreadyExists.ext,
                    });
                }
                kvDel(key);
                kvDel(`inlay_thumb/${id}`);
                kvDel(`inlay_info/${id}`);
                kvClearDeletion(key);
                continue;
            } catch (error) {
                logger.warn(`[InlayFS] Failed to finalize ${key}:`, error?.message || error);
                continue;
            }
        }
        const value = kvGet(key);
        if (!value) continue;
        try {
            const parsed = JSON.parse(value.toString('utf-8'));
            const type = typeof parsed?.type === 'string' ? parsed.type : 'image';
            const ext = normalizeInlayExt(parsed?.ext);
            let buffer;
            if (type === 'signature') {
                buffer = Buffer.from(typeof parsed?.data === 'string' ? parsed.data : '', 'utf-8');
            } else {
                buffer = decodeDataUri(parsed?.data).buffer;
            }
            const info = (await readInlayLegacyInfo(id)) || {
                ext,
                name: typeof parsed?.name === 'string' ? parsed.name : id,
                type,
                height: typeof parsed?.height === 'number' ? parsed.height : undefined,
                width: typeof parsed?.width === 'number' ? parsed.width : undefined,
            };
            await writeInlayFile(id, ext, buffer, info);
            kvDel(key);
            kvDel(`inlay_thumb/${id}`);
            kvDel(`inlay_info/${id}`);
            kvClearDeletion(key);
        } catch (error) {
            logger.warn(`[InlayFS] Failed to migrate ${key}:`, error?.message || error);
        }
    }

    if (kvList('inlay/').length === 0) {
        await fs.writeFile(inlayMigrationMarker, new Date().toISOString(), 'utf-8');
    } else if (existsSync(inlayMigrationMarker)) {
        await fs.unlink(inlayMigrationMarker).catch((error) => {
            if (error?.code !== 'ENOENT') throw error;
        });
    }
}

function assetNameForKey(key) {
    return typeof key === 'string' && key.startsWith('assets/')
        ? key.slice('assets/'.length)
        : null;
}

function readAssetValue(key, reader = { kvGet }) {
    const name = assetNameForKey(key);
    if (name !== null && isSafeAssetName(name)) {
        const fileDisposition = runtimeAssetFileDisposition(name);
        if (!fileDisposition.eligible) {
            const rowValue = reader.kvGet(key);
            if (rowValue !== null) return rowValue;
        }
        const fileValue = readAssetFile(name);
        if (fileValue !== null) return fileValue;
    }
    return reader.kvGet(key);
}

function verifyAssetHashForWrite(key, value) {
    const verification = verifyAssetHash(key, value);
    const name = assetNameForKey(key);
    const legacyHashMismatch = !verification.ok
        && name !== null
        && isLegacyHashAsset(name);
    return { ...verification, legacyHashMismatch };
}

function writeAssetValue(key, value, options = {}) {
    const {
        skipIfUnchanged = false,
        publishHooks = {},
        metadataHooks = {},
    } = options;
    const name = assetNameForKey(key);
    if (name === null) {
        kvSet(key, value);
        assetGcCandidateStore.remove(key);
        return true;
    }
    return withAssetFileMutationAdmission(
        name,
        `admitted asset write ${name}`,
        (fileDisposition, unlocked) => {
            if (fileDisposition?.eligible) {
                const verification = verifyAssetHash(key, value);
                const legacyHashMismatch = !verification.ok && isLegacyHashAsset(name);
                if (!verification.ok && !legacyHashMismatch) {
                    const error = new Error('asset content does not match its SHA-256 name');
                    error.code = 'ASSET_HASH_MISMATCH';
                    error.key = key;
                    error.expected = verification.claimed;
                    error.actual = verification.actual;
                    throw error;
                }
                if (legacyHashMismatch) markLegacyHashAsset(name);
                let wrote = true;
                if (skipIfUnchanged) {
                    wrote = unlocked.writeAssetFileIfChanged(name, value, publishHooks);
                } else {
                    unlocked.writeAssetFile(name, value, publishHooks);
                }
                if (verification.claimed !== null && verification.ok) {
                    if (metadataHooks.beforeLegacyHashClear) {
                        metadataHooks.beforeLegacyHashClear();
                    }
                    clearLegacyHashAsset(name);
                    if (metadataHooks.afterLegacyHashClear) {
                        metadataHooks.afterLegacyHashClear();
                    }
                }
                // Admission, legacy-marker mutation, payload publication, and
                // shadow-row cleanup share one lock ownership interval. An
                // import cannot install a portable-name collision between the
                // disposition check and the final publication.
                kvDel(key);
                kvClearDeletion(key);
                assetGcCandidateStore.remove(key);
                return wrote;
            }
            kvSet(key, value);
            assetGcCandidateStore.remove(key);
            return true;
        },
    );
}

function deleteAssetValue(key) {
    const name = assetNameForKey(key);
    if (name !== null && isSafeAssetName(name)) {
        deleteAssetFile(name);
    }
    kvDel(key);
    assetGcCandidateStore.remove(key);
}

function listAssetEntriesWithSizes(reader = { kvListWithSizes, kvGetUpdatedAt }) {
    const entries = new Map();
    for (const file of listAssetFiles()) {
        entries.set(`assets/${file.name}`, {
            key: `assets/${file.name}`,
            size: file.size,
            mtimeMs: file.mtimeMs,
            source: 'fs',
            legacyHash: isLegacyHashAsset(file.name),
        });
    }
    const rows = reader.kvListWithSizes('assets/');
    const names = rows.map((row) => assetNameForKey(row.key));
    const fileDispositions = runtimeAssetFileDispositions(names);
    for (let index = 0; index < rows.length; index++) {
        const row = rows[index];
        const fileDisposition = fileDispositions.get(names[index]);
        if (!entries.has(row.key) || !fileDisposition?.eligible) {
            entries.set(row.key, {
                key: row.key,
                size: row.size,
                mtimeMs: typeof reader.kvGetUpdatedAt === 'function'
                    ? reader.kvGetUpdatedAt(row.key)
                    : null,
                source: 'kv',
                legacyHash: false,
            });
        }
    }
    return [...entries.values()].sort((a, b) => a.key.localeCompare(b.key));
}

const assetImportStagingDir = path.join(savePath, 'assets_import_staging');
const assetImportBackupDir = path.join(savePath, 'assets_import_backup');
const DEMOTED_PORTABLE_ASSET_NAME = Symbol('demoted-portable-asset-name');

async function prepareAssetImportStage() {
    recoverPendingImportSwap('Asset import preparation');
    await fs.rm(assetImportStagingDir, { recursive: true, force: true });
    await fs.rm(assetImportBackupDir, { recursive: true, force: true });
    const store = createAssetStore({
        assetDir: assetImportStagingDir,
        // Detached staging is not observable by dedup. The live-directory
        // swap acquires and retains the stable save-level lock through the
        // import journal's finalize/rollback boundary.
        maintenanceLock: false,
    });
    store.ensureAssetDir();
    writeFileSync(store.migrationMarkerPath, new Date().toISOString(), 'utf-8');
    store.reconcileLegacyHashAssetIdentity({ discover: true });
    return { store, stagedPortableNames: new Map() };
}

function warnImportedAssetHashVerification(key, verification, source) {
    if (!verification.ok) {
        logger.warn(
            `[AssetFS] ${source} hash mismatch for ${key}: `
            + `expected=${verification.claimed} actual=${verification.actual}; importing verbatim`
        );
    }
}

function warnImportedAssetHashMismatch(key, value, source) {
    const verification = verifyAssetHash(key, value);
    warnImportedAssetHashVerification(key, verification, source);
    return verification;
}

async function writeImportedAssetFromFile(
    assetStage,
    key,
    source,
    signal,
    label,
    { maxBytes = BACKUP_IMPORT_MAX_BYTES } = {},
) {
    const sourceLabel = label || 'Legacy import';
    const name = assetNameForKey(key);
    if (name === null || !isSafeAssetName(name)) {
        kvSetFromFile(key, source.filePath);
        logger.warn(`[AssetFS] ${sourceLabel} retained unsafe asset key ${key} in SQLite`);
        return 'kv';
    }
    if (!isPortableAssetName(name)) {
        kvSetFromFile(key, source.filePath);
        logger.warn(`[AssetFS] ${sourceLabel} retained non-portable asset key ${key} in SQLite`);
        return 'kv';
    }

    const portableKey = portableAssetNameKey(name);
    const stagedName = assetStage.stagedPortableNames.get(portableKey);
    if (stagedName === DEMOTED_PORTABLE_ASSET_NAME) {
        kvSetFromFile(key, source.filePath);
        return 'kv';
    }
    if (stagedName !== undefined && stagedName !== name) {
        const stagedKey = `assets/${stagedName}`;
        kvSetFromFile(stagedKey, assetStage.store.assetPathFor(stagedName));
        assetStage.store.deleteAssetFile(stagedName);
        assetStage.stagedPortableNames.set(portableKey, DEMOTED_PORTABLE_ASSET_NAME);
        kvSetFromFile(key, source.filePath);
        logger.warn(
            `[AssetFS] ${sourceLabel} retained colliding asset keys ${stagedKey} and ${key} in SQLite`,
        );
        return 'kv';
    }

    await copyFileToSpool(source.filePath, assetStage.store.assetPathFor(name), {
        maxBytes,
        signal,
    });
    const verification = assetStage.store.verifyStoredAssetHash(name);
    warnImportedAssetHashVerification(key, verification, sourceLabel);
    if (!verification.ok) assetStage.store.markLegacyHashAsset(name);
    kvClearDeletion(key);
    if (stagedName === undefined) {
        assetStage.stagedPortableNames.set(portableKey, name);
    }
    return 'fs';
}

async function validateAndImportPluginValueFile(
    key,
    source,
    signal,
    { maxBytes = BACKUP_IMPORT_MAX_BYTES } = {},
) {
    if (isHashedPluginSaveStorageKey(key, PLUGIN_SAVE_PREFIX)) {
        assertArchiveSafePluginSaveStorageKey(key);
    } else {
        decodeValidatedPluginStorageKey(key, PLUGIN_SAVE_PREFIX);
    }
    const valueMaxBytes = Math.min(maxBytes, PLUGIN_VALUE_MAX_BYTES);
    let displayMetadata;
    try {
        if (source.size > valueMaxBytes) {
            throw new PluginStorageLimitError(
                `Plugin storage value exceeds the ${valueMaxBytes}-byte import limit.`,
                {
                    code: 'PLUGIN_VALUE_TOO_LARGE',
                    limit: valueMaxBytes,
                    actual: source.size,
                },
            );
        }
        const prefixHandle = await fs.open(source.filePath, 'r');
        let prefix;
        try {
            prefix = Buffer.alloc(Math.min(PLUGIN_STORAGE_LOSSLESS_MAGIC.length, source.size));
            const read = await prefixHandle.read(prefix, 0, prefix.length, 0);
            if (read.bytesRead !== prefix.length) {
                throw new PluginStorageValidationError(key);
            }
        } finally {
            await prefixHandle.close();
        }
        if (pluginStorageCodecForBuffer(prefix) !== PLUGIN_STORAGE_LOSSLESS_CODEC) {
            await validateJsonFileStreaming(source.filePath, {
                size: source.size,
                maxBytes: valueMaxBytes,
                signal,
            });
        }
        displayMetadata = await validateJsonSource({
            filePath: source.filePath,
            size: source.size,
        }, { signal });
    } catch (error) {
        if (error?.code === 'INVALID_PLUGIN_STORAGE_ROW') {
            throw new PluginStorageValidationError(key);
        }
        throw error;
    }
    throwIfImportAborted(signal);
    kvSetFromFile(key, source.filePath, {
        pluginStorageDisplaySize: pluginStorageViewerDisplaySizeFromMetadata(
            displayMetadata,
        ),
    });
}

async function validateAndImportPluginMetadataFile(
    key,
    source,
    signal,
    { maxBytes = BACKUP_IMPORT_MAX_BYTES } = {},
) {
    if (isHashedPluginSaveStorageKey(key, PLUGIN_SAVE_META_PREFIX)) {
        assertArchiveSafePluginSaveStorageKey(key);
    } else {
        decodeValidatedPluginStorageKey(key, PLUGIN_SAVE_META_PREFIX);
    }
    const ownerScanner = createPluginStorageOwnerScanner();
    await validateJsonFileStreaming(source.filePath, {
        size: source.size,
        maxBytes,
        signal,
        onPage: (page) => ownerScanner.push(page),
    });
    throwIfImportAborted(signal);
    kvSetFromFile(key, source.filePath, {
        pluginStorageOwner: ownerScanner.finish(),
    });
}

async function importOpaqueRowFromFile(key, source, signal) {
    throwIfImportAborted(signal);
    kvSetFromFile(key, source.filePath);
}

async function importColdStorageFromFile(
    storageKey,
    source,
    signal,
    label,
    { maxBytes, bufferedEntryMaxBytes },
) {
    const handle = await fs.open(source.filePath, 'r');
    let gzip = false;
    try {
        const header = Buffer.alloc(2);
        const { bytesRead } = await handle.read(header, 0, 2, 0);
        gzip = bytesRead === 2 && header[0] === 0x1f && header[1] === 0x8b;
    } finally {
        await handle.close();
    }

    // Historical third-party archives sometimes put already-compressed bytes
    // in a .json entry. Preserve that compatibility path; current PocketRisu
    // exports are plain JSON and take the fully streaming path below.
    if (gzip) {
        const data = await readFileToBufferBounded(source.filePath, {
            size: source.size,
            maxBytes: Math.min(bufferedEntryMaxBytes, maxBytes),
            label,
            code: 'IMPORT_BUFFERED_ENTRY_LIMIT',
            signal,
        });
        const storageValue = encodeColdStorageCanonicalBuffer(
            parseColdStorageJsonBuffer(data, label, { allowPlainJson: true }).coldData,
        );
        kvSet(storageKey, storageValue);
        return;
    }

    await validateJsonFileStreaming(source.filePath, {
        size: source.size,
        maxBytes,
        signal,
    });
    const compressedPath = `${source.filePath}.cold.gz`;
    try {
        await pipeline(
            createReadStream(source.filePath, { highWaterMark: IMPORT_IO_PAGE_BYTES }),
            zlib.createGzip({ chunkSize: IMPORT_IO_PAGE_BYTES }),
            createWriteStream(compressedPath, { flags: 'wx', mode: 0o600 }),
            signal ? { signal } : {},
        );
        kvSetFromFile(storageKey, compressedPath);
    } finally {
        await fs.unlink(compressedPath).catch(() => {});
    }
}

function migrateAssetsToFilesystem() {
    ensureAssetDir();
    let migratedRows = false;
    if (!existsSync(assetMigrationMarker)) {
        const keys = kvList('assets/');
        if (keys.length > 0) {
            console.log(`[AssetFS] Migrating ${keys.length} asset row(s) to ${assetDir}...`);
        }
        const result = migrateAssetRowsToFilesystem({
            keys,
            existingAssetNames: listAssetFiles().map((entry) => entry.name),
            getValue: (key) => {
                const value = kvGet(key);
                if (value !== null) {
                    warnImportedAssetHashMismatch(key, value, 'Startup migration');
                }
                return value;
            },
            deleteValue: (key) => {
                kvDel(key);
                kvClearDeletion(key);
            },
            store: {
                isSafeAssetName,
                portableAssetNameKey,
                isPortableAssetName,
                writeAssetFileIfChanged,
            },
            onSkipped: ({ key, reason }) => {
                const description = reason === 'collision'
                    ? 'its portable filename collides with another asset'
                    : 'its filename is not portable';
                logger.warn(`[AssetFS] Startup migration retained ${key} in SQLite because ${description}`);
            },
            onProgress: ({ index, total, migrated }) => {
                if (migrated % 100 === 0 || index === total - 1) {
                    console.log(`[AssetFS] Migrating... ${index + 1}/${total}`);
                }
            },
        });
        writeFileSync(assetMigrationMarker, new Date().toISOString(), 'utf-8');
        migratedRows = result.migrated > 0;
        if (keys.length > 0) {
            console.log(
                `[AssetFS] Migration complete. ${result.migrated} moved, `
                + `${result.skippedUnsafe} unsafe, `
                + `${result.skippedNonPortable} non-portable, and `
                + `${result.skippedCollision} colliding name(s) kept in SQLite.`
            );
        }
    }

    const discoverLegacyIdentity = migratedRows || !existsSync(legacyHashIdentityMarkerPath);
    const identity = reconcileLegacyHashAssetIdentity({ discover: discoverLegacyIdentity });
    if (identity.marked > 0 || identity.cleared > 0) {
        logger.info(
            `[AssetFS] Legacy hash identity reconciliation: ${identity.marked} marked, `
            + `${identity.cleared} stale marker(s) cleared.`,
        );
    }
}

// ── Session store for direct asset URL auth (F-0) ──────────────────────────
// <img src="/api/asset/..."> cannot send custom headers, so we use a session
// cookie issued after initial JWT auth. Single-user environment: Map is fine.
// Sessions are persisted to disk so they survive server restarts.
const SESSION_FILE = path.join(process.cwd(), 'save', '__sessions')
const sessions = new Map() // token → expiresAt (ms)

function loadSessions() {
    try {
        const raw = readFileSync(SESSION_FILE, 'utf-8')
        const now = Date.now()
        for (const [token, exp] of JSON.parse(raw)) {
            if (exp > now) sessions.set(token, exp)
        }
    } catch { /* file missing or corrupt – start fresh */ }
}

function saveSessions() {
    try { writeFileSync(SESSION_FILE, JSON.stringify([...sessions])) }
    catch { /* non-critical */ }
}

loadSessions()

function parseSessionCookie(req) {
    const cookieHeader = req.headers.cookie || ''
    for (const part of cookieHeader.split(';')) {
        const eq = part.indexOf('=')
        if (eq === -1) continue
        if (part.slice(0, eq).trim() === 'risu-session') return part.slice(eq + 1).trim()
    }
    return null
}

function checkSessionCookieAuth(req, res) {
    const token = parseSessionCookie(req)
    if (token && (sessions.get(token) ?? 0) > Date.now()) return true
    res.status(401).end()
    return false
}

function sessionAuthMiddleware(req, res, next) {
    if (checkSessionCookieAuth(req, res)) next()
}

// MIME detection by magic bytes (fallback when key has no extension)
function detectMime(buf) {
    if (!buf || buf.length < 12) return 'application/octet-stream'
    if (buf[0] === 0x89 && buf[1] === 0x50 && buf[2] === 0x4e && buf[3] === 0x47) return 'image/png'
    if (buf[0] === 0xff && buf[1] === 0xd8) return 'image/jpeg'
    if (buf[0] === 0x47 && buf[1] === 0x49 && buf[2] === 0x46) return 'image/gif'
    if (buf[0] === 0x52 && buf[1] === 0x49 && buf[2] === 0x46 && buf[3] === 0x46 &&
        buf[8] === 0x57 && buf[9] === 0x45 && buf[10] === 0x42 && buf[11] === 0x50) return 'image/webp'
    if (buf[0] === 0x1a && buf[1] === 0x45) return 'video/webm'
    if (buf.length >= 8 && buf[4] === 0x66 && buf[5] === 0x74 && buf[6] === 0x79 && buf[7] === 0x70) return 'video/mp4'
    return 'application/octet-stream'
}
const ASSET_EXT_MIME = {
    png: 'image/png', jpg: 'image/jpeg', jpeg: 'image/jpeg',
    gif: 'image/gif', webp: 'image/webp',
    mp4: 'video/mp4', webm: 'video/webm',
    mp3: 'audio/mpeg', ogg: 'audio/ogg', wav: 'audio/wav',
}

async function checkDiskSpace(requiredBytes, targetPath = path.join(process.cwd(), 'save')) {
    if (process.env.NODE_ENV === 'test'
        && process.env.POCKETRISU_TEST_IMPORT_AVAILABLE_BYTES !== undefined) {
        const available = Number(process.env.POCKETRISU_TEST_IMPORT_AVAILABLE_BYTES);
        if (Number.isSafeInteger(available) && available >= 0) {
            return { ok: available >= requiredBytes, available };
        }
    }
    if (process.env.POCKETRISU_PLUGIN_STORAGE_TEST_FAILPOINTS === '1'
        && process.env.POCKETRISU_PLUGIN_TRANSITION_TEST_AVAILABLE_BYTES !== undefined) {
        const available = Number(
            process.env.POCKETRISU_PLUGIN_TRANSITION_TEST_AVAILABLE_BYTES,
        );
        if (Number.isSafeInteger(available) && available >= 0) {
            return { ok: available >= requiredBytes, available };
        }
    }
    try {
        const stats = await fs.statfs(targetPath);
        const availableBytes = stats.bavail * stats.bsize;
        return { ok: availableBytes >= requiredBytes, available: availableBytes };
    } catch {
        // statfs unavailable on this platform — skip check
        return { ok: true, available: -1 };
    }
}

// ── Active writer session (single-writer lock) ────────────────────────────────
// Mirrors the BroadcastChannel-based tab lock on the server side so that the
// same protection extends across devices. Page loads register without stealing
// the lock; a recent user gesture allows a freshly booted session to take over.
const { createBoundedSessionState } = require('./runtime/boundedSessionState.cjs');
const PLUGIN_STORAGE_READ_SESSION_MAX_ENTRIES = 50;
const pluginStorageReadStateStatsPath = process.env.NODE_ENV === 'test'
    ? String(process.env.POCKETRISU_TEST_PLUGIN_READ_STATE_STATS_PATH ?? '').trim() || null
    : null;
let pluginStorageReadStateBySession;

function publishPluginStorageReadStateStats() {
    if (!pluginStorageReadStateStatsPath || !pluginStorageReadStateBySession) return;
    writeFileSync(
        path.resolve(process.cwd(), pluginStorageReadStateStatsPath),
        JSON.stringify(pluginStorageReadStateBySession.stats()),
        'utf-8',
    );
}

pluginStorageReadStateBySession = createBoundedSessionState({
    maxEntries: PLUGIN_STORAGE_READ_SESSION_MAX_ENTRIES,
    onEvict: publishPluginStorageReadStateStats,
});

function rememberSessionPluginStorageState(req, dbObj) {
    const clientSessionId = req.headers['x-session-id'];
    if (typeof clientSessionId !== 'string' || clientSessionId.length === 0) return;
    pluginStorageReadStateBySession.set(clientSessionId, {
        optimized: dbObj?.optimizePluginMemory === true,
        generation: pluginStorageGeneration(dbObj),
    });
    publishPluginStorageReadStateStats();
}

function sessionPluginStorageReadState(req) {
    const clientSessionId = req.headers['x-session-id'];
    return typeof clientSessionId === 'string'
        ? pluginStorageReadStateBySession.get(clientSessionId) ?? null
        : null;
}

function captureActiveSessionWriteRequest(req) {
    const clientSessionId = req.headers['x-session-id']
    const clientWriterEpoch = req.headers[WRITER_EPOCH_HEADER]
    return {
        sessionId: typeof clientSessionId === 'string' ? clientSessionId : '',
        userActive: req.headers['x-user-active'] === '1',
        writerEpoch: typeof clientWriterEpoch === 'string' ? clientWriterEpoch : '',
    }
}

function checkActiveSessionWrite(writeRequest, res) {
    const result = sessionLock.checkWrite(
        writeRequest.sessionId,
        writeRequest.userActive,
        writeRequest.writerEpoch,
    )
    if (result.tookOver) {
        console.log('[Session] Write lock taken over by a freshly-booted session')
    }
    if (result.ok) return true
    res.status(423).json({
        error: 'Session deactivated',
        code: 'SESSION_DEACTIVATED',
        retryable: false,
        commitOutcome: 'not-committed',
        commitOutcomeUnknown: false,
    })
    return false
}

function checkActiveSession(req, res) {
    return checkActiveSessionWrite(captureActiveSessionWriteRequest(req), res)
}

const loginRouteLimiter = rateLimit({
    windowMs: 30 * 1000,
    max: 10,
    standardHeaders: true,
    legacyHeaders: false,
    message: { error: 'Too many attempts. Please wait and try again later.' },
    validate: { xForwardedForHeader: false }
});

function isHex(str) {
    if (str === '__password') return true;
    if (typeof str !== 'string'
        || str.length === 0
        || str.length % 2 !== 0
        || !hexRegex.test(str)) return false;
    const bytes = Buffer.from(str, 'hex');
    const decoded = bytes.toString('utf-8');
    return Buffer.from(decoded, 'utf-8').equals(bytes);
}

function decodeAndCanonicalizeHexPath(filePath) {
    if (filePath === '__password') {
        return { canonicalPath: '__password', decodedKey: '__password' };
    }
    if (!isHex(filePath)) {
        const error = new Error('Invalid canonical UTF-8 hex path');
        error.code = 'INVALID_HEX_PATH';
        throw error;
    }
    const pathBytes = Buffer.from(filePath, 'hex');
    return {
        canonicalPath: pathBytes.toString('hex'),
        decodedKey: pathBytes.toString('utf-8'),
    };
}

async function hashJSON(json){
    const hash = nodeCrypto.createHash('sha256');
    hash.update(JSON.stringify(json));
    return hash.digest('hex');
}

// NodeOnly: server-issued JWT (see jwt_secret comment above)
function createServerJwt() {
    const now = Math.floor(Date.now() / 1000)
    const header = { alg: 'HS256', typ: 'JWT' }
    const payload = { iat: now, exp: now + 5 * 60 }
    const headerB64 = Buffer.from(JSON.stringify(header)).toString('base64url')
    const payloadB64 = Buffer.from(JSON.stringify(payload)).toString('base64url')
    const sig = nodeCrypto.createHmac('sha256', jwtSecret)
        .update(`${headerB64}.${payloadB64}`)
        .digest('base64url')
    return `${headerB64}.${payloadB64}.${sig}`
}

function encodeBackupEntry(name, data) {
    return Buffer.concat([encodeBackupEntryHeader(name, data.length), data]);
}

async function writeWithBackpressure(
    writable,
    chunk,
    isClosed = () => false,
    onBackpressure = null,
) {
    if (isClosed()) return false;
    if (writable.write(chunk)) return true;
    return new Promise((resolve, reject) => {
        let settled = false;
        let drained = false;
        let reported = !onBackpressure;
        function cleanup() {
            writable.removeListener('drain', onDrain);
            writable.removeListener('error', onError);
            writable.removeListener('close', onClose);
        }
        function settle(value, error = null) {
            if (settled) return;
            settled = true;
            cleanup();
            if (error) reject(error);
            else resolve(value);
        }
        function maybeResolve() {
            if (drained && reported) settle(true);
        }
        function onDrain() {
            drained = true;
            maybeResolve();
        }
        function onError(error) {
            settle(false, error);
        }
        function onClose() {
            if (isClosed()) settle(false);
            else settle(false, new Error('Backup destination closed before draining'));
        }
        writable.once('drain', onDrain);
        writable.once('error', onError);
        writable.once('close', onClose);
        if (isClosed()) {
            onClose();
            return;
        }
        if (onBackpressure) {
            Promise.resolve()
                .then(onBackpressure)
                .then(() => {
                    reported = true;
                    if (isClosed()) onClose();
                    else maybeResolve();
                }, onError);
        }
    });
}

async function streamFileToWritable(filePath, writable, isClosed = () => false) {
    const input = createReadStream(filePath, { highWaterMark: IMPORT_IO_PAGE_BYTES });
    try {
        for await (const chunk of input) {
            if (!await writeWithBackpressure(writable, chunk, isClosed)) return false;
        }
        return !isClosed();
    } finally {
        input.destroy();
    }
}

async function writePinnedBackupEntry(writable, entry, isClosed) {
    if (!await writeWithBackpressure(
        writable,
        encodeBackupEntryHeader(entry.backupName, entry.size),
        isClosed,
    )) return false;
    if (entry.kind !== 'file') {
        throw new Error(`Backup entry was not pinned to a private file: ${entry.backupName}`);
    }
    return streamFileToWritable(entry.sourcePath, writable, isClosed);
}

function isInvalidBackupPathSegment(name) {
    return (
        !name ||
        name.includes('\0') ||
        name.includes('\\') ||
        name.startsWith('/') ||
        name.includes('../') ||
        name.includes('/..') ||
        name === '.' ||
        name === '..'
    );
}

const INLAY_ARCHIVE_V2_PREFIX = 'inlay_v2/';

function toInlayBackupName(id, ext) {
    const normalizedExt = assertSafeInlayTuple(id, ext);
    if (!normalizedExt.includes('.')) return `inlay/${id}.${normalizedExt}`;
    return `${INLAY_ARCHIVE_V2_PREFIX}${encodeInlayPhysicalComponent(id)}`
        + `--${encodeInlayPhysicalComponent(normalizedExt)}`;
}

function parseInlayBackupName(name) {
    if (name.startsWith(INLAY_ARCHIVE_V2_PREFIX)) {
        const suffix = name.slice(INLAY_ARCHIVE_V2_PREFIX.length);
        if (!suffix || suffix.includes('/')) return null;
        const separator = suffix.indexOf('--');
        if (separator <= 0 || suffix.indexOf('--', separator + 2) !== -1) return null;
        const id = decodeInlayPhysicalComponent(suffix.slice(0, separator));
        const ext = decodeInlayPhysicalComponent(suffix.slice(separator + 2));
        if (!isSafeInlayTuple(id, ext) || normalizeInlayExt(ext) !== ext) return null;
        return { id, ext, encoded: true };
    }
    if (!name.startsWith('inlay/')) return null;
    const suffix = name.slice('inlay/'.length);
    if (!suffix || suffix.includes('/')) return null;
    const dotIdx = suffix.lastIndexOf('.');
    if (dotIdx <= 0) {
        return isSafeInlayId(suffix) ? { id: suffix, ext: null } : null;
    }
    const id = suffix.slice(0, dotIdx);
    const ext = suffix.slice(dotIdx + 1);
    return isSafeInlayTuple(id, ext) && normalizeInlayExt(ext) === ext
        ? { id, ext }
        : null;
}

function parseInlaySidecarBackupName(name) {
    if (!name.startsWith('inlay_sidecar/')) return null;
    const id = name.slice('inlay_sidecar/'.length);
    if (!isSafeInlayId(id)) return null;
    return { id };
}

// Upstream backups can use flat coldstorage_<uuid>.json entry names. Restrict
// that compatibility form to UUIDs so similarly named assets remain assets.
const COLD_STORAGE_FLAT_NAME_RE = /^coldstorage_([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})(?:\.json)?$/;

function normalizeColdStorageStorageKey(nameOrKey) {
    let key = nameOrKey;
    if (key.startsWith('coldstorage/')) {
        key = key.slice('coldstorage/'.length);
    } else {
        const flat = COLD_STORAGE_FLAT_NAME_RE.exec(key);
        if (flat) key = flat[1];
    }
    if (key.endsWith('.json')) {
        key = key.slice(0, -'.json'.length);
    }
    if (!key || key.includes('/') || isInvalidBackupPathSegment(key)) {
        throw new Error(`Invalid cold storage entry name: ${nameOrKey}`);
    }
    return `coldstorage/${key}`;
}

function toColdStorageBackupName(storageKey) {
    return `${normalizeColdStorageStorageKey(storageKey)}.json`;
}

function parseColdStorageJsonBuffer(buffer, sourceLabel, options = {}) {
    const { allowPlainJson = false } = options;
    try {
        const decompressed = zlib.gunzipSync(buffer);
        return {
            coldData: JSON.parse(decompressed.toString('utf-8')),
            format: 'gzip',
        };
    } catch (gzipError) {
        if (!allowPlainJson) {
            throw gzipError;
        }
        try {
            return {
                coldData: JSON.parse(buffer.toString('utf-8')),
                format: 'plain-json',
            };
        } catch (jsonError) {
            throw new Error(`[ColdStorage] failed to parse ${sourceLabel}: gzip=${gzipError.message}; json=${jsonError.message}`);
        }
    }
}

function encodeColdStorageCanonicalBuffer(coldData) {
    return Buffer.from(zlib.gzipSync(Buffer.from(JSON.stringify(coldData), 'utf-8')));
}

function readColdStorageJsonEntry(nameOrKey, options = {}) {
    const {
        migrateLegacy = false,
        allowPlainJsonFallback = false,
        reader = { kvGet },
    } = options;
    const canonicalKey = normalizeColdStorageStorageKey(nameOrKey);
    const legacyBackupKey = `${canonicalKey}.json`;

    let storageKey = canonicalKey;
    let value = reader.kvGet(canonicalKey);
    if (!value) {
        storageKey = legacyBackupKey;
        value = reader.kvGet(legacyBackupKey);
    }
    if (!value) {
        return null;
    }

    const parsed = parseColdStorageJsonBuffer(value, storageKey, {
        allowPlainJson: allowPlainJsonFallback || storageKey !== canonicalKey,
    });

    if (migrateLegacy && (storageKey !== canonicalKey || parsed.format !== 'gzip')) {
        kvSet(canonicalKey, encodeColdStorageCanonicalBuffer(parsed.coldData));
        if (storageKey !== canonicalKey) {
            kvDel(storageKey);
        }
    }

    return {
        coldData: parsed.coldData,
        storageKey,
        canonicalKey,
        format: parsed.format,
    };
}

function listColdStorageBackupEntries(options = {}) {
    const {
        reader = { kvGet, kvList },
        migrateLegacy = true,
    } = options;
    const canonicalKeys = Array.from(new Set(
        reader.kvList('coldstorage/').map((key) => normalizeColdStorageStorageKey(key))
    )).sort((a, b) => a.localeCompare(b));

    return canonicalKeys.map((storageKey) => {
        const entry = readColdStorageJsonEntry(storageKey, {
            migrateLegacy,
            allowPlainJsonFallback: true,
            reader,
        });
        if (!entry) {
            throw new Error(`[ColdStorage] missing cold storage entry while exporting: ${storageKey}`);
        }
        const plainJson = Buffer.from(JSON.stringify(entry.coldData), 'utf-8');
        return {
            kind: 'buffer',
            buffer: plainJson,
            backupName: toColdStorageBackupName(storageKey),
            sortKey: toColdStorageBackupName(storageKey),
            size: plainJson.length,
        };
    });
}

function hasExternalizablePluginStorage(dbObj) {
    if (!dbObj) return false;
    if (dbObj[PLUGIN_STORAGE_FOLDED_MARKER] === true) return true;
    if (dbObj.optimizePluginMemory !== true) return false;
    const inlineValues = dbObj.pluginCustomStorage;
    const hasValues = inlineValues !== null
        && typeof inlineValues === 'object'
        && Object.keys(inlineValues).length > 0;
    const hasMetaField = Object.prototype.hasOwnProperty.call(dbObj, 'pluginStorageMeta');
    return hasValues || hasMetaField;
}

function snapshotOptimizedPluginStorageFields(dbObj) {
    if (!dbObj || (
        dbObj.optimizePluginMemory !== true
        && dbObj[PLUGIN_STORAGE_FOLDED_MARKER] !== true
    )) return null;
    return {
        values: snapshotPluginStorageRecord(
            dbObj.pluginCustomStorage ?? {},
            'pluginCustomStorage',
            PLUGIN_SAVE_PREFIX
        ),
        meta: Object.prototype.hasOwnProperty.call(dbObj, 'pluginStorageMeta')
            ? snapshotPluginStorageRecord(
                dbObj.pluginStorageMeta,
                'pluginStorageMeta',
                PLUGIN_SAVE_META_PREFIX
            )
            : {},
        hasMeta: Object.prototype.hasOwnProperty.call(dbObj, 'pluginStorageMeta'),
    };
}

function pluginStorageValidationDiagnostic(error) {
    if (!isPluginStorageValidationError(error)) return null;
    const encodedKey = typeof error.encodedKey === 'string'
        && (error.encodedKey.startsWith(PLUGIN_SAVE_PREFIX)
            || error.encodedKey.startsWith(PLUGIN_SAVE_META_PREFIX))
        ? error.encodedKey
        : PLUGIN_SAVE_PREFIX;
    return {
        error: 'Invalid plugin storage JSON row',
        code: 'INVALID_PLUGIN_STORAGE_ROW',
        encodedKey,
    };
}

function logPluginStorageValidationFailure(context, error) {
    const diagnostic = pluginStorageValidationDiagnostic(error);
    if (!diagnostic) return null;
    logger.warn(`${context}: ${diagnostic.encodedKey}`);
    return diagnostic;
}

function preparePluginStorageExternalization(dbObj) {
    const hasMarkerField = Boolean(dbObj)
        && Object.prototype.hasOwnProperty.call(dbObj, PLUGIN_STORAGE_FOLDED_MARKER);
    const strictFields = snapshotOptimizedPluginStorageFields(dbObj);
    if (!hasExternalizablePluginStorage(dbObj)) {
        if (!hasMarkerField) {
            return {
                strippedDb: dbObj,
                rows: [],
                changed: false,
                externalized: false,
                clearExisting: false,
                values: 0,
                meta: 0,
                manifest: null,
            };
        }
        const strippedDb = { ...dbObj };
        delete strippedDb[PLUGIN_STORAGE_FOLDED_MARKER];
        return {
            strippedDb,
            rows: [],
            changed: true,
            externalized: false,
            clearExisting: false,
            values: 0,
            meta: 0,
            manifest: null,
        };
    }

    const valueEntries = Object.entries(strictFields.values);
    const metaEntries = Object.entries(strictFields.meta);
    const rows = [];
    for (const [rawKey, value] of valueEntries) {
        const storageKey = encodeValidatedPluginStorageKey(rawKey, PLUGIN_SAVE_PREFIX);
        rows.push({
            storageKey,
            value: serializePluginStorageRow(storageKey, value),
        });
    }
    for (const [rawKey, value] of metaEntries) {
        const storageKey = encodeValidatedPluginStorageKey(rawKey, PLUGIN_SAVE_META_PREFIX);
        rows.push({
            storageKey,
            value: serializePluginStorageRow(storageKey, value),
        });
    }
    const exactFoldedSet = dbObj[PLUGIN_STORAGE_FOLDED_MARKER] === true;
    const generation = exactFoldedSet
        && typeof dbObj[PLUGIN_STORAGE_GENERATION_FIELD] === 'string'
        && dbObj[PLUGIN_STORAGE_GENERATION_FIELD].length > 0
        ? dbObj[PLUGIN_STORAGE_GENERATION_FIELD]
        : nodeCrypto.randomUUID();
    const strippedDb = {
        ...dbObj,
        [PLUGIN_STORAGE_GENERATION_FIELD]: generation,
        pluginCustomStorage: {},
    };
    delete strippedDb.pluginStorageMeta;
    delete strippedDb[PLUGIN_STORAGE_FOLDED_MARKER];
    // A folded marker is already exact. Unmarked inline data is an import or
    // defensive monolith and starts a fresh generation rather than unioning
    // rows left by the previously selected database.
    const activeValueKeys = new Set();
    const activeMetaKeys = new Set();
    for (const row of rows) {
        if (row.storageKey.startsWith(PLUGIN_SAVE_META_PREFIX)) {
            activeMetaKeys.add(row.storageKey);
        } else {
            activeValueKeys.add(row.storageKey);
        }
    }
    return {
        strippedDb,
        rows,
        changed: true,
        externalized: true,
        clearExisting: dbObj[PLUGIN_STORAGE_FOLDED_MARKER] === true,
        values: valueEntries.length,
        meta: metaEntries.length,
        manifest: createPluginStorageManifest(
            generation,
            activeValueKeys,
            activeMetaKeys,
            mergePluginStorageKeyMappings(
                null,
                [
                    ...valueEntries.map(([rawKey]) => rawKey),
                    ...metaEntries.map(([rawKey]) => rawKey),
                ],
                activeValueKeys,
                activeMetaKeys,
            ),
        ),
    };
}

function writePluginStorageRows(rows) {
    for (const row of rows) {
        validatePluginStorageRow(row.storageKey, row.value);
        kvSet(row.storageKey, row.value);
    }
}

function writePluginStorageManifest(manifest) {
    if (!manifest) return null;
    const bytes = Buffer.from(JSON.stringify(manifest), 'utf-8');
    kvSet(PLUGIN_STORAGE_MANIFEST_KEY, bytes);
    return bytes;
}

function pluginStorageManifestEquals(left, right) {
    if (left === null || right === null) return left === right;
    if (left.version !== right.version || left.generation !== right.generation) return false;
    if (left.version === 2 || left.version === 3) {
        const sameOrder = (a, b) => a.length === b.length
            && a.every((key, index) => key === b[index]);
        return sameOrder(left.valueKeys, right.valueKeys)
            && sameOrder(left.metaKeys, right.metaKeys)
            && (left.version !== 3 || (
                sameOrder(
                    left.keyMappings.map(entry => JSON.stringify(entry)),
                    right.keyMappings.map(entry => JSON.stringify(entry)),
                )
            ));
    }
    const sameKeys = (a, b) => {
        if (a.length !== b.length) return false;
        const rightKeys = new Set(b);
        return rightKeys.size === b.length
            && new Set(a).size === a.length
            && a.every(key => rightKeys.has(key));
    };
    return sameKeys(left.valueKeys, right.valueKeys)
        && sameKeys(left.metaKeys, right.metaKeys);
}

function normalizePluginStorageManifestRequest(value, fieldName, { nullable = false } = {}) {
    if (value === null && nullable) return null;
    const manifest = parsePluginStorageManifest(value);
    if (!manifest) throw new TypeError(`${fieldName} is not a valid plugin storage manifest`);
    return manifest;
}

function readPluginStorageManifestStateUncached(readValue = kvGet) {
    const raw = readValue(PLUGIN_STORAGE_MANIFEST_KEY);
    if (!raw) return { manifest: null, present: false, valid: true, revision: null };
    try {
        const manifest = parsePluginStorageManifest(JSON.parse(raw.toString('utf-8')));
        return {
            manifest,
            present: true,
            valid: manifest !== null,
            revision: manifest ? `sha256:${sha256Hex(raw)}` : null,
        };
    } catch {
        return { manifest: null, present: true, valid: false, revision: null };
    }
}

function readPluginStorageManifestState(readValue) {
    if (readValue !== undefined) {
        return readPluginStorageManifestStateUncached(readValue);
    }
    return pluginStorageManifestCache.read().state;
}

function readStrictPluginStorageOwnershipManifest(readValue = kvGet) {
    const raw = readValue(PLUGIN_STORAGE_MANIFEST_KEY);
    if (!raw) return { manifest: null, valueKeys: [], metaKeys: [] };

    let parsed;
    try {
        const bytes = Buffer.isBuffer(raw)
            ? raw
            : ArrayBuffer.isView(raw)
                ? Buffer.from(raw.buffer, raw.byteOffset, raw.byteLength)
                : Buffer.from(raw);
        parsed = JSON.parse(bytes.toString('utf-8'));
    } catch {
        throw new TypeError('The live plugin storage manifest is malformed');
    }
    const manifest = parsePluginStorageManifest(parsed);
    if (!manifest) {
        throw new TypeError('The live plugin storage manifest is invalid');
    }
    // parsePluginStorageManifest canonicalizes duplicate entries for ordinary
    // reads. Destructive replacement needs a stronger ownership proof: a
    // duplicate declaration is ambiguous input, not permission to delete.
    if (manifest.valueKeys.length !== parsed.valueKeys.length
        || manifest.metaKeys.length !== parsed.metaKeys.length
        || (manifest.version === 3
            && manifest.keyMappings.length !== parsed.keyMappings.length)) {
        throw new TypeError('The live plugin storage manifest contains duplicate entries');
    }

    return {
        manifest,
        valueKeys: manifest.valueKeys,
        metaKeys: manifest.metaKeys,
    };
}

function validateStrictPluginStorageOwnershipRow(storageKey, readValue) {
    // Keep the byte body and its parsed JSON inside this narrow call. The
    // assertion validates row JSON without constructing another deep snapshot,
    // and neither representation is retained by the manifest proof.
    if (pluginStorageOwnershipReadFailpoint === 'any'
        || pluginStorageOwnershipReadFailpoint === storageKey) {
        throw new Error('Injected live plugin ownership body read failure');
    }
    const value = readValue(storageKey);
    if (!value) {
        throw new TypeError('The live plugin storage manifest references a missing row');
    }
    assertPluginStorageRow(storageKey, value);
    return value.byteLength ?? value.length ?? 0;
}

function readStrictPluginStorageOwnershipBoundary(readValue = kvGet) {
    const ownership = readStrictPluginStorageOwnershipManifest(readValue);
    for (const keys of [ownership.valueKeys, ownership.metaKeys]) {
        for (const storageKey of keys) {
            validateStrictPluginStorageOwnershipRow(storageKey, readValue);
        }
    }
    return ownership;
}

async function proveStrictPluginStorageOwnershipBoundary({
    readValue = kvGet,
    shouldAbort,
} = {}) {
    const stats = pluginStorageOwnershipStatsPath
        ? {
            activeRows: 0,
            completed: false,
            largestRowBytes: 0,
            maxActiveRows: 0,
            maxPostGcHeapGrowth: 0,
            rowsRead: 0,
        }
        : null;
    if (stats && typeof global.gc === 'function') global.gc();
    const baselineHeapUsed = stats ? process.memoryUsage().heapUsed : 0;
    throwIfStreamingRestoreAborted(shouldAbort);
    const ownership = readStrictPluginStorageOwnershipManifest(readValue);
    try {
        for (const keys of [ownership.valueKeys, ownership.metaKeys]) {
            for (const storageKey of keys) {
                throwIfStreamingRestoreAborted(shouldAbort);
                if (stats) {
                    stats.activeRows += 1;
                    stats.maxActiveRows = Math.max(stats.maxActiveRows, stats.activeRows);
                }
                let rowBytes = 0;
                try {
                    rowBytes = validateStrictPluginStorageOwnershipRow(storageKey, readValue);
                } finally {
                    if (stats) stats.activeRows -= 1;
                }
                if (stats) {
                    stats.rowsRead += 1;
                    stats.largestRowBytes = Math.max(stats.largestRowBytes, rowBytes);
                }
                // Give disconnect/AbortSignal state and GC a chance to settle
                // after every row. At this point the row-local Buffer, decoded
                // string, and parsed value are out of scope and no aggregate
                // body exists.
                await new Promise((resolve) => setImmediate(resolve));
                if (stats && typeof global.gc === 'function') {
                    global.gc();
                    stats.maxPostGcHeapGrowth = Math.max(
                        stats.maxPostGcHeapGrowth,
                        Math.max(0, process.memoryUsage().heapUsed - baselineHeapUsed),
                    );
                }
            }
        }
        throwIfStreamingRestoreAborted(shouldAbort);
        if (stats) stats.completed = true;
        return ownership;
    } finally {
        if (stats) {
            writeFileSync(pluginStorageOwnershipStatsPath, JSON.stringify(stats), 'utf-8');
        }
    }
}

function deleteOwnedPluginStorageRows(ownership) {
    for (const storageKey of ownership.valueKeys) kvDel(storageKey);
    for (const storageKey of ownership.metaKeys) kvDel(storageKey);
}

function pluginStorageGeneration(dbObj) {
    return typeof dbObj?.[PLUGIN_STORAGE_GENERATION_FIELD] === 'string'
        && dbObj[PLUGIN_STORAGE_GENERATION_FIELD].length > 0
        ? dbObj[PLUGIN_STORAGE_GENERATION_FIELD]
        : null;
}

function canonicalPluginStorageRowPrefix(storageKey) {
    const prefix = typeof storageKey === 'string' && storageKey.startsWith(PLUGIN_SAVE_META_PREFIX)
        ? PLUGIN_SAVE_META_PREFIX
        : typeof storageKey === 'string' && storageKey.startsWith(PLUGIN_SAVE_PREFIX)
            ? PLUGIN_SAVE_PREFIX
            : null;
    if (!prefix) return null;
    try {
        if (isHashedPluginSaveStorageKey(storageKey, prefix)) {
            assertArchiveSafePluginSaveStorageKey(storageKey);
        } else {
            decodePluginSaveStorageKey(storageKey, prefix);
        }
        return prefix;
    } catch {
        return null;
    }
}

async function readLivePluginStoragePublication() {
    await flushPendingDb();
    const prepared = await prepareLiveDatabaseRead('PluginStoragePublication', {
        includeFullBlob: false,
    });
    const dbObj = prepared?.strippedDatabase ?? null;
    const manifestEntry = pluginStorageManifestCache.read();
    return {
        dbObj,
        generation: pluginStorageGeneration(dbObj),
        manifestState: manifestEntry.state,
        manifestEntry,
    };
}

function pluginStorageBootRecoveryIssue(code, encodedKey) {
    return { code, encodedKey };
}

function serializeOptimizedPluginStorageRow(storageKey, prefix, value) {
    try {
        return serializePluginStorageRow(storageKey, value);
    } catch (error) {
        if (prefix !== PLUGIN_SAVE_PREFIX) throw error;
        return serializeLosslessPluginStorageRow(storageKey, value);
    }
}

function canonicalizeOptimizedPluginStorageRow(storageKey, prefix, bytes) {
    const codec = pluginStorageCodecForBuffer(bytes);
    if (prefix === PLUGIN_SAVE_META_PREFIX
        && codec === PLUGIN_STORAGE_LOSSLESS_CODEC) {
        throw new TypeError('Plugin storage metadata requires strict JSON');
    }
    const parsed = parsePluginStorageJsonBuffer(bytes, storageKey);
    return serializeOptimizedPluginStorageRow(storageKey, prefix, parsed);
}

function collectOptimizedBootInlineEntries(dbObj, field, prefix, issues) {
    const source = dbObj?.[field] ?? {};
    if (source === null || typeof source !== 'object' || Array.isArray(source)) {
        issues.push(pluginStorageBootRecoveryIssue('unsupported-json', prefix));
        return { entries: [], storageKeys: new Set(), valid: false };
    }
    const prototype = Reflect.getPrototypeOf(source);
    if (prototype !== Object.prototype && prototype !== null) {
        issues.push(pluginStorageBootRecoveryIssue('unsupported-json', prefix));
        return { entries: [], storageKeys: new Set(), valid: false };
    }

    const entries = [];
    const storageKeys = new Set();
    for (const rawKey of Reflect.ownKeys(source)) {
        if (typeof rawKey !== 'string') {
            issues.push(pluginStorageBootRecoveryIssue('unsupported-json', prefix));
            continue;
        }
        let storageKey;
        try {
            storageKey = encodePluginSaveStorageKey(rawKey, prefix);
        } catch {
            issues.push(pluginStorageBootRecoveryIssue('invalid-encoded-key', prefix));
            continue;
        }
        storageKeys.add(storageKey);
        const descriptor = Reflect.getOwnPropertyDescriptor(source, rawKey);
        if (!descriptor || !('value' in descriptor) || !descriptor.enumerable) {
            issues.push(pluginStorageBootRecoveryIssue('unsupported-json', storageKey));
            continue;
        }
        try {
            const canonical = serializeOptimizedPluginStorageRow(
                storageKey,
                prefix,
                descriptor.value,
            );
            entries.push({
                rawKey,
                storageKey,
                prefix,
                value: descriptor.value,
                canonicalHash: sha256Hex(canonical),
            });
        } catch {
            issues.push(pluginStorageBootRecoveryIssue('unsupported-json', storageKey));
        }
    }
    return { entries, storageKeys, valid: true };
}

function decodeOptimizedBootStorageKey(storageKey, prefix, manifest) {
    const rawKey = isHashedPluginSaveStorageKey(storageKey, prefix)
        ? manifest
            ? decodeManifestPluginSaveStorageKey(manifest, storageKey, prefix)
            : null
        : decodePluginSaveStorageKey(storageKey, prefix);
    if (rawKey === null || encodePluginSaveStorageKey(rawKey, prefix) !== storageKey) {
        throw new TypeError('Plugin storage key is not canonical');
    }
    return rawKey;
}

async function inspectOptimizedBootExternalRows({
    prefix,
    listed,
    inlineStorageKeys,
    generation,
    manifest,
    issues,
}) {
    // Only duplicate hashes survive an iteration. Clean optimized databases
    // normally have no inline keys, so the retained map stays empty even when
    // the publication contains many large rows.
    const duplicateHashes = new Map();
    const ownedKeys = generation && manifest?.generation === generation
        ? new Set(prefix === PLUGIN_SAVE_META_PREFIX ? manifest.metaKeys : manifest.valueKeys)
        : null;
    for (const storageKey of listed) {
        let duplicateHash = null;
        try {
            decodeOptimizedBootStorageKey(storageKey, prefix, manifest);
            if (generation && (!ownedKeys || !ownedKeys.has(storageKey))) {
                // Generation-bound browser reads deliberately make undeclared
                // physical rows look absent. Preserve that recovery diagnostic
                // without transferring or parsing the quarantined body.
                issues.push(pluginStorageBootRecoveryIssue('read-failed', storageKey));
            } else {
                let bytes;
                let readFailed = false;
                try {
                    bytes = await kvGetAsync(storageKey);
                } catch {
                    issues.push(pluginStorageBootRecoveryIssue('read-failed', storageKey));
                    readFailed = true;
                }
                if (!readFailed && !bytes) {
                    issues.push(pluginStorageBootRecoveryIssue('read-failed', storageKey));
                } else if (bytes) {
                    try {
                        const canonical = canonicalizeOptimizedPluginStorageRow(
                            storageKey,
                            prefix,
                            bytes,
                        );
                        duplicateHash = inlineStorageKeys.has(storageKey)
                            ? sha256Hex(canonical)
                            : null;
                    } catch (error) {
                        issues.push(pluginStorageBootRecoveryIssue(
                            error instanceof SyntaxError ? 'invalid-json' : 'unsupported-json',
                            storageKey,
                        ));
                    }
                }
            }
        } catch {
            issues.push(pluginStorageBootRecoveryIssue('invalid-encoded-key', storageKey));
        }
        if (duplicateHash) duplicateHashes.set(storageKey, duplicateHash);
        // The row Buffer, decoded string, parsed value and canonical bytes are
        // all out of scope here. Yield so V8 can reclaim them before the next
        // potentially large record is read.
        await new Promise(resolve => setImmediate(resolve));
    }
    return duplicateHashes;
}

function nextOptimizedBootRecoveryManifest(manifest, generation, entries) {
    if (!generation) return null;
    if (!manifest || manifest.generation !== generation) {
        throw new Error('The selected plugin storage generation has no matching manifest');
    }
    const valueKeys = new Set(manifest.valueKeys);
    const metaKeys = new Set(manifest.metaKeys);
    for (const entry of entries) {
        if (entry.storageKey.startsWith(PLUGIN_SAVE_META_PREFIX)) {
            metaKeys.add(entry.storageKey);
        } else {
            valueKeys.add(entry.storageKey);
        }
    }
    return createPluginStorageManifest(
        generation,
        valueKeys,
        metaKeys,
        mergePluginStorageKeyMappings(
            manifest,
            entries.map(entry => entry.rawKey),
            valueKeys,
            metaKeys,
        ),
    );
}

function publishOptimizedBootRecoveryRows(entries, generation, manifest) {
    const prepared = entries.map(entry => ({
        ...entry,
        bytes: serializeOptimizedPluginStorageRow(
            entry.storageKey,
            entry.prefix,
            entry.value,
        ),
    }));
    const nextManifest = nextOptimizedBootRecoveryManifest(manifest, generation, prepared);
    const recoverySnapshotToken = newPluginRecoverySnapshotToken();
    withPluginStorageQuotaPlan(
        prepared
            .filter(entry => entry.storageKey.startsWith(PLUGIN_SAVE_PREFIX))
            .map(entry => ({ key: entry.storageKey, size: entry.bytes.length })),
        () => {
            for (const entry of prepared) kvSet(entry.storageKey, entry.bytes);
            if (nextManifest) writePluginStorageManifest(nextManifest);
            markPluginRecoverySnapshotDirty(recoverySnapshotToken);
        },
    );
    return nextManifest ?? manifest;
}

async function persistOptimizedBootInlineCleanup(req, liveDb) {
    if (!ensureDatabaseSpoolDirSync()) {
        throw new Error('The database spool is unavailable');
    }
    const targetDb = {
        ...liveDb,
        pluginCustomStorage: {},
    };
    delete targetDb.pluginStorageMeta;
    const spoolPath = path.join(
        databaseSpoolDir,
        `${DATABASE_SPOOL_FILE_PREFIX}plugin-boot-${process.pid}-${nodeCrypto.randomUUID()}.tmp`,
    );
    let spool = null;
    try {
        spool = await streamRisuSaveToFile({
            dbObj: targetDb,
            filePath: spoolPath,
            readChatRow: async () => null,
            foldChatRows: false,
        });
        const resultEtag = await computeFileEtag(spool.filePath);
        const recoverySnapshotToken = newPluginRecoverySnapshotToken();
        sqliteDb.transaction(() => {
            kvSetFromFile('database/database.bin', spool.filePath);
            markPluginRecoverySnapshotDirty(recoverySnapshotToken);
        })();
        invalidateDbCache();
        dbEtag = resultEtag;
        rememberSessionPluginStorageState(req, targetDb);
        return { etag: resultEtag, databaseChanged: true };
    } finally {
        if (spool) await fs.unlink(spool.filePath).catch(() => {});
        else await fs.unlink(spoolPath).catch(() => {});
    }
}

/**
 * Reconcile only the optimized-mode boot case. External row bodies are parsed
 * and released one at a time on the server; none are returned to the browser.
 * Inline mode retains the legacy client recovery path because its final state
 * necessarily contains the complete plugin map in browser memory.
 */
async function reconcileOptimizedPluginStorageForBoot(req, expectedEtag) {
    let copiedRows = false;
    const result = await queueStorageMutation(async () => {
        await flushPendingDb();
        const rawDatabase = kvGet('database/database.bin');
        if (!rawDatabase) {
            const error = new Error('Database not found');
            error.pluginStorageBootStatus = 409;
            throw error;
        }
        // Derive both accepted boot tokens from the bytes selected inside this
        // queued operation. Cache-off boot receives the raw-row token, while
        // cache-enabled boot receives the legacy-encoded normalized-view token.
        // Accepting either equivalent representation preserves the fence while
        // still detecting an out-of-process change to the selected database.
        const rawEtag = computeBufferEtag(rawDatabase);
        const liveDb = await decodeAuthoritativeDatabase(rawDatabase);
        const canonicalEtag = expectedEtag === rawEtag
            ? null
            : prepareDatabaseReadPayload(
                normalizeDecodedDatabaseForRead(liveDb),
            ).etag;
        if (expectedEtag !== rawEtag && expectedEtag !== canonicalEtag) {
            // Raw boot reloads do not populate the process-local cache token;
            // leave it aligned with the current raw row until that reload or a
            // cached read establishes its own equivalent representation.
            dbEtag = rawEtag;
            const error = new Error('Database changed before plugin storage reconciliation');
            error.pluginStorageBootStatus = 409;
            error.currentEtag = rawEtag;
            throw error;
        }
        // Subsequent ordinary saves must use the same representation that the
        // active client proved, rather than switching token domains mid-boot.
        dbEtag = expectedEtag;

        if (liveDb?.optimizePluginMemory !== true) {
            return {
                direction: 'none',
                values: 0,
                meta: 0,
                issues: [],
                etag: expectedEtag,
                databaseChanged: false,
                storageChanged: false,
            };
        }

        const issues = [];
        const inlineValues = collectOptimizedBootInlineEntries(
            liveDb,
            'pluginCustomStorage',
            PLUGIN_SAVE_PREFIX,
            issues,
        );
        const inlineMeta = collectOptimizedBootInlineEntries(
            liveDb,
            'pluginStorageMeta',
            PLUGIN_SAVE_META_PREFIX,
            issues,
        );
        let listedValues;
        let listedMeta;
        try {
            listedValues = kvList(PLUGIN_SAVE_PREFIX);
        } catch {
            listedValues = null;
            issues.push(pluginStorageBootRecoveryIssue('list-failed', PLUGIN_SAVE_PREFIX));
        }
        try {
            listedMeta = kvList(PLUGIN_SAVE_META_PREFIX);
        } catch {
            listedMeta = null;
            issues.push(pluginStorageBootRecoveryIssue('list-failed', PLUGIN_SAVE_META_PREFIX));
        }

        const generation = pluginStorageGeneration(liveDb);
        let manifest = null;
        if (generation) {
            const rawManifest = kvGet(PLUGIN_STORAGE_MANIFEST_KEY);
            if (rawManifest) {
                try {
                    const parsed = JSON.parse(rawManifest.toString('utf-8'));
                    const normalized = parsePluginStorageManifest(parsed);
                    if (normalized?.generation === generation) manifest = normalized;
                } catch {
                    issues.push(pluginStorageBootRecoveryIssue(
                        'invalid-json',
                        PLUGIN_STORAGE_MANIFEST_KEY,
                    ));
                }
            }
        }

        const externalValues = await inspectOptimizedBootExternalRows({
            prefix: PLUGIN_SAVE_PREFIX,
            listed: listedValues ?? [],
            inlineStorageKeys: inlineValues.storageKeys,
            generation,
            manifest,
            issues,
        });
        const externalMeta = await inspectOptimizedBootExternalRows({
            prefix: PLUGIN_SAVE_META_PREFIX,
            listed: listedMeta ?? [],
            inlineStorageKeys: inlineMeta.storageKeys,
            generation,
            manifest,
            issues,
        });

        for (const [inline, external] of [
            [inlineValues, externalValues],
            [inlineMeta, externalMeta],
        ]) {
            for (const entry of inline.entries) {
                const duplicate = external.get(entry.storageKey);
                if (duplicate && duplicate !== entry.canonicalHash) {
                    issues.push(pluginStorageBootRecoveryIssue(
                        'conflicting-copies',
                        entry.storageKey,
                    ));
                }
            }
        }

        let valueCopies = 0;
        let metaCopies = 0;
        const listedValueSet = listedValues === null ? null : new Set(listedValues);
        const listedMetaSet = listedMeta === null ? null : new Set(listedMeta);
        const inlineMetaByRawKey = new Map(
            inlineMeta.entries.map(entry => [entry.rawKey, entry]),
        );
        const pairedMetaKeys = new Set();
        let currentManifest = manifest;

        if (listedValueSet) {
            for (const entry of inlineValues.entries) {
                if (listedValueSet.has(entry.storageKey)) continue;
                const metaEntry = inlineMetaByRawKey.get(entry.rawKey);
                if (metaEntry && listedMetaSet?.has(metaEntry.storageKey)) {
                    const external = externalMeta.get(metaEntry.storageKey);
                    if (!external || external !== metaEntry.canonicalHash) continue;
                }
                if (metaEntry) pairedMetaKeys.add(metaEntry.storageKey);
                try {
                    currentManifest = publishOptimizedBootRecoveryRows(
                        [entry, ...(metaEntry ? [metaEntry] : [])],
                        generation,
                        currentManifest,
                    );
                    copiedRows = true;
                    valueCopies += 1;
                    if (metaEntry && !listedMetaSet?.has(metaEntry.storageKey)) metaCopies += 1;
                } catch {
                    issues.push(pluginStorageBootRecoveryIssue('write-failed', entry.storageKey));
                }
            }
        }

        if (listedMetaSet) {
            for (const entry of inlineMeta.entries) {
                if (pairedMetaKeys.has(entry.storageKey) || listedMetaSet.has(entry.storageKey)) {
                    continue;
                }
                try {
                    currentManifest = publishOptimizedBootRecoveryRows(
                        [entry],
                        generation,
                        currentManifest,
                    );
                    copiedRows = true;
                    metaCopies += 1;
                } catch {
                    issues.push(pluginStorageBootRecoveryIssue('write-failed', entry.storageKey));
                }
            }
        }

        const inlineTotal = inlineValues.entries.length + inlineMeta.entries.length;
        let cleanup = {
            etag: expectedEtag,
            databaseChanged: false,
        };
        if (inlineTotal > 0 && issues.length === 0) {
            try {
                cleanup = await persistOptimizedBootInlineCleanup(req, liveDb);
            } catch {
                issues.push(pluginStorageBootRecoveryIssue(
                    'persist-failed',
                    'database/database.bin',
                ));
            }
        }

        return {
            direction: inlineTotal > 0 || issues.length > 0 ? 'externalize' : 'none',
            values: valueCopies,
            meta: metaCopies,
            issues,
            etag: cleanup.etag,
            databaseChanged: cleanup.databaseChanged,
            storageChanged: copiedRows,
        };
    }, 'plugin-boot-reconcile');
    if (copiedRows || result.databaseChanged) schedulePluginRecoverySnapshot();
    return result;
}


function pluginStorageNamespaceConflict(message) {
    const error = new Error(message);
    error.pluginStorageNamespaceConflict = true;
    return error;
}

function assertGenericPluginStorageMutationAllowed(storageKey, publication) {
    if (storageKey === 'database/database.bin') {
        const hasPublishedPluginStorage = publication.generation
            || publication.manifestState.present
            || (publication.dbObj?.optimizePluginMemory === true && (
                kvList(PLUGIN_SAVE_PREFIX).length > 0
                || kvList(PLUGIN_SAVE_META_PREFIX).length > 0
            ));
        if (hasPublishedPluginStorage) {
            throw pluginStorageNamespaceConflict(
                'database.bin cannot be removed while it selects a plugin storage publication',
            );
        }
        return;
    }
    if (storageKey === PLUGIN_STORAGE_MANIFEST_KEY) {
        throw pluginStorageNamespaceConflict(
            'The plugin storage manifest can only be changed by an atomic plugin storage transaction',
        );
    }
    const prefix = canonicalPluginStorageRowPrefix(storageKey);
    if (!prefix) return;

    const { dbObj, generation, manifestState } = publication;
    // As soon as a generation or manifest exists, the whole canonical row
    // namespace is reserved. Unlisted physical names are quarantined, not an
    // alternate generic-write channel into a future publication.
    if (generation || manifestState.present) {
        throw pluginStorageNamespaceConflict(
            'The generated plugin storage namespace can only be changed atomically',
        );
    }

    throw pluginStorageNamespaceConflict(
        dbObj?.optimizePluginMemory === true
            ? 'Legacy plugin storage must be adopted before rows can be changed atomically'
            : 'The generated plugin storage namespace is not writable in the selected mode',
    );
}

function assertGenericDatabasePluginPublicationAllowed(
    livePublication,
    incomingDb,
    pluginExternalization,
) {
    const liveDb = livePublication.dbObj;
    const liveGeneration = livePublication.generation;
    const incomingGeneration = pluginStorageGeneration(incomingDb);
    const manifestState = livePublication.manifestState;
    const touchesPublication = pluginExternalization.rows.length > 0
        || pluginExternalization.manifest !== null
        || pluginExternalization.clearExisting === true;
    const hasPhysicalRows = kvList(PLUGIN_SAVE_PREFIX).length > 0
        || kvList(PLUGIN_SAVE_META_PREFIX).length > 0;

    if (!liveDb) {
        if (manifestState.present || (hasPhysicalRows && (
            incomingDb?.optimizePluginMemory !== true
            || incomingGeneration !== null
            || touchesPublication
        ))) {
            throw pluginStorageNamespaceConflict(
                'Existing plugin storage rows must be adopted as an unchanged legacy publication',
            );
        }
        return;
    }

    if (liveGeneration || manifestState.present) {
        if (
            incomingDb?.optimizePluginMemory !== (liveDb.optimizePluginMemory === true)
            || incomingGeneration !== liveGeneration
            || touchesPublication
        ) {
            throw pluginStorageNamespaceConflict(
                'Plugin storage mode, generation, rows, and manifest must be changed atomically',
            );
        }
        return;
    }

    if (liveDb.optimizePluginMemory === true && hasPhysicalRows && (
        incomingDb?.optimizePluginMemory !== true
        || incomingGeneration !== null
        || touchesPublication
    )) {
        throw pluginStorageNamespaceConflict(
            'Legacy plugin storage must be adopted by an atomic plugin storage transition',
        );
    }
}

async function readGenerationBoundPluginStorageRow(req, storageKey) {
    const explicitGeneration = req.headers['x-plugin-storage-generation'];
    if (explicitGeneration !== undefined
        && (typeof explicitGeneration !== 'string' || explicitGeneration.length === 0)) {
        throw new TypeError('x-plugin-storage-generation must be a non-empty string');
    }
    const pinnedState = sessionPluginStorageReadState(req);
    if (typeof explicitGeneration === 'string' && pinnedState && (
        pinnedState.optimized !== true
        || pinnedState.generation !== explicitGeneration
    )) {
        throw pluginStorageNamespaceConflict(
            'The requested plugin storage generation does not match this session database',
        );
    }
    const expectedState = typeof explicitGeneration === 'string'
        ? { optimized: true, generation: explicitGeneration }
        : pinnedState;

    return queueStorageReadAfterImports(async () => {
        const publication = await readLivePluginStoragePublication();
        const { dbObj, generation, manifestState } = publication;
        const prefix = canonicalPluginStorageRowPrefix(storageKey);
        if (!prefix) throw new TypeError('Invalid plugin storage row key');

        const activeManifest = generation
            && dbObj?.optimizePluginMemory === true
            && manifestState.valid
            && manifestState.manifest?.generation === generation
            ? manifestState.manifest
            : null;
        const legacyPublication = !generation
            && dbObj?.optimizePluginMemory === true
            && !manifestState.present;

        if (!expectedState) {
            if (activeManifest || legacyPublication) {
                throw pluginStorageNamespaceConflict(
                    'Read database.bin before reading authoritative plugin storage rows',
                );
            }
            return kvGetAsync(storageKey);
        }
        if (
            expectedState.optimized !== (dbObj?.optimizePluginMemory === true)
            || expectedState.generation !== generation
        ) {
            throw pluginStorageNamespaceConflict(
                'Plugin storage generation changed before the row could be read',
            );
        }

        if (generation) {
            if (!activeManifest) {
                throw pluginStorageNamespaceConflict(
                    'The selected plugin storage generation has no matching manifest',
                );
            }
            const ownedKeys = prefix === PLUGIN_SAVE_META_PREFIX
                ? activeManifest.metaKeys
                : activeManifest.valueKeys;
            // Exact ownership is also enforced at the read boundary. A foreign
            // physical row must look absent even if a caller guesses its name.
            return ownedKeys.includes(storageKey) ? kvGetAsync(storageKey) : null;
        }
        if (!legacyPublication) {
            throw pluginStorageNamespaceConflict(
                'The legacy plugin storage publication changed before the row could be read',
            );
        }
        return kvGetAsync(storageKey);
    });
}

function assertPluginStorageSource(source, liveDb, manifestState) {
    if (!source || typeof source !== 'object' || Array.isArray(source)) {
        throw new TypeError('source must be an object');
    }
    if (typeof source.optimized !== 'boolean') {
        throw new TypeError('source.optimized must be a boolean');
    }
    const expectedGeneration = source.generation === null
        ? null
        : typeof source.generation === 'string' && source.generation.length > 0
            ? source.generation
            : undefined;
    if (expectedGeneration === undefined) {
        throw new TypeError('source.generation must be null or a non-empty string');
    }
    const expectedManifest = normalizePluginStorageManifestRequest(
        source.manifest,
        'source.manifest',
        { nullable: true },
    );
    const liveOptimized = liveDb?.optimizePluginMemory === true;
    const liveGeneration = pluginStorageGeneration(liveDb);
    if (
        liveOptimized !== source.optimized
        || liveGeneration !== expectedGeneration
        || !manifestState.valid
        || !pluginStorageManifestEquals(manifestState.manifest, expectedManifest)
    ) {
        const error = new Error('Plugin storage state changed while the operation was being prepared');
        error.pluginStorageConflict = true;
        throw error;
    }
}

function resolveOwnedPluginStorageKeys(dbObj, reader = { kvGet, kvList }) {
    if (dbObj?.optimizePluginMemory !== true) {
        return { valueKeys: [], metaKeys: [] };
    }

    const generation = pluginStorageGeneration(dbObj);
    if (generation) {
        const ownership = readStrictPluginStorageOwnershipManifest(reader.kvGet);
        if (!ownership.manifest) {
            throw new TypeError(
                'The selected plugin storage generation has no authoritative manifest',
            );
        }
        if (ownership.manifest.generation !== generation) {
            throw new TypeError(
                'The selected plugin storage generation does not match its manifest',
            );
        }
        const physicalValues = new Set(reader.kvList(PLUGIN_SAVE_PREFIX));
        const physicalMeta = new Set(reader.kvList(PLUGIN_SAVE_META_PREFIX));
        for (const storageKey of ownership.valueKeys) {
            if (!physicalValues.has(storageKey)) {
                throw new TypeError(
                    `The plugin storage manifest references a missing row: ${storageKey}`,
                );
            }
        }
        for (const storageKey of ownership.metaKeys) {
            if (!physicalMeta.has(storageKey)) {
                throw new TypeError(
                    `The plugin storage manifest references a missing row: ${storageKey}`,
                );
            }
        }
        return {
            valueKeys: ownership.valueKeys,
            metaKeys: ownership.metaKeys,
            manifest: ownership.manifest,
        };
    }

    const manifestState = readPluginStorageManifestState(reader.kvGet);
    if (manifestState.present) {
        throw new TypeError(
            'Legacy optimized plugin storage cannot select a generated manifest',
        );
    }
    return {
        valueKeys: reader.kvList(PLUGIN_SAVE_PREFIX),
        metaKeys: reader.kvList(PLUGIN_SAVE_META_PREFIX),
        manifest: null,
    };
}

function maybeFailPluginStorageTransaction(req, boundary) {
    if (process.env.POCKETRISU_PLUGIN_STORAGE_TEST_FAILPOINTS !== '1') return;
    if (req.headers['x-plugin-storage-failpoint'] === boundary) {
        throw new Error(`Injected plugin storage failure at ${boundary}`);
    }
}

/**
 * Re-externalize folded plugin storage from an optimized database object.
 * The source object is left untouched if any row write fails.
 */
function externalizePluginStorageIfNeeded(dbObj) {
    const prepared = preparePluginStorageExternalization(dbObj);
    if (!prepared.changed) {
        return { changed: false, values: 0, meta: 0 };
    }

    // A folded source proves its own exact target set, but it does not prove
    // that every physical row in the shared prefix belongs to the publication
    // being replaced. Delete only the rows named by a strict, complete live
    // ownership boundary; foreign/quarantined rows remain untouched.
    const priorOwnership = prepared.clearExisting
        ? readStrictPluginStorageOwnershipBoundary()
        : null;
    const writeRows = sqliteDb.transaction(() => {
        if (prepared.clearExisting) {
            deleteOwnedPluginStorageRows(priorOwnership);
        }
        writePluginStorageRows(prepared.rows);
        writePluginStorageManifest(prepared.manifest);
    });
    writeRows();

    if (prepared.externalized) {
        dbObj.pluginCustomStorage = {};
        delete dbObj.pluginStorageMeta;
        dbObj[PLUGIN_STORAGE_GENERATION_FIELD]
            = prepared.strippedDb[PLUGIN_STORAGE_GENERATION_FIELD];
    }
    delete dbObj[PLUGIN_STORAGE_FOLDED_MARKER];
    return {
        changed: true,
        values: prepared.values,
        meta: prepared.meta,
    };
}

function parsePluginSaveJson(storageKey, readValue = kvGet) {
    const value = readValue(storageKey);
    if (!value) {
        throw new PluginStorageValidationError(storageKey);
    }
    return validatePluginStorageRow(storageKey, value);
}

function readPluginStorageManifest(readValue) {
    return readPluginStorageManifestState(readValue).manifest;
}

function resolveOwnedPluginStorageRows(dbObj, reader) {
    const { valueKeys, metaKeys, manifest } = resolveOwnedPluginStorageKeys(dbObj, reader);

    return {
        valueRows: valueKeys.map((storageKey) => ({
            key: manifest
                ? decodeManifestPluginSaveStorageKey(manifest, storageKey, PLUGIN_SAVE_PREFIX)
                : decodeValidatedPluginStorageKey(storageKey, PLUGIN_SAVE_PREFIX),
            source: storageKey,
        })),
        metaRows: metaKeys.map((storageKey) => ({
            key: manifest
                ? decodeManifestPluginSaveStorageKey(manifest, storageKey, PLUGIN_SAVE_META_PREFIX)
                : decodeValidatedPluginStorageKey(storageKey, PLUGIN_SAVE_META_PREFIX),
            source: storageKey,
        })),
        readRow: (storageKey) => parsePluginSaveJson(storageKey, reader.kvGet),
    };
}

function collectDatabaseAssetReferences(
    dbObj,
    assetEntries,
    reader = { kvGet, kvList },
) {
    const knownAssetKeys = new Set(assetEntries.map((entry) => entry.key));
    const referencedKeys = collectReferencedAssetKeys(dbObj, knownAssetKeys);
    const pluginStorage = resolveOwnedPluginStorageRows(dbObj, reader);
    // Optimized values stay external precisely so large stores do not inflate
    // database.bin. Decode and release one authoritative manifest row at a time.
    for (const row of pluginStorage.valueRows) {
        collectReferencedAssetKeys(
            pluginStorage.readRow(row.source),
            knownAssetKeys,
            referencedKeys,
        );
    }
    return referencedKeys;
}

async function runServerAssetCleanup({ now = Date.now(), source = 'manual' } = {}) {
    return queueStorageMutation(async () => {
        await flushPendingDb();
        const raw = kvGet(DB_BLOB_KEY);
        if (!raw) {
            return {
                ok: true,
                skipped: true,
                reason: 'database-missing',
                source,
                graceMs: ASSET_GC_GRACE_MS,
                assets: 0,
                referenced: 0,
                marked: 0,
                retainedByGrace: 0,
                deleted: 0,
                candidatesCleared: 0,
            };
        }

        // Any decode, manifest, ownership, or plugin-row validation failure
        // escapes before the candidate table or an asset is changed.
        const dbObj = await loadStrippedDatabase(raw, 'AssetGC');
        const assetEntries = listAssetEntriesWithSizes();
        const referencedKeys = collectDatabaseAssetReferences(dbObj, assetEntries);
        const candidates = assetGcCandidateStore.list();
        const plan = planAssetGc({
            assets: assetEntries,
            referencedKeys,
            candidates,
            now,
            graceMs: ASSET_GC_GRACE_MS,
        });

        let candidatesCleared = 0;
        for (const key of plan.clear) {
            if (assetGcCandidateStore.remove(key)) candidatesCleared++;
        }
        for (const candidate of plan.mark) {
            assetGcCandidateStore.mark(
                candidate.key,
                candidate.firstUnreferencedAt,
                candidate.identity,
            );
        }
        let deleted = 0;
        for (const key of plan.remove) {
            deleteAssetValue(key);
            deleted++;
        }

        const result = {
            ok: true,
            skipped: false,
            source,
            graceMs: ASSET_GC_GRACE_MS,
            assets: assetEntries.length,
            referenced: referencedKeys.size,
            marked: plan.mark.length,
            retainedByGrace: plan.retainedByGrace,
            deleted,
            candidatesCleared,
        };
        logger.info(
            `[AssetGC] ${source}: ${result.referenced}/${result.assets} referenced, `
            + `${result.marked} newly marked, ${result.retainedByGrace} in grace, `
            + `${result.deleted} deleted`,
        );
        return result;
    }, 'asset-gc');
}

let assetGcTimer = null;
function scheduleServerAssetCleanup(delayMs = ASSET_GC_START_DELAY_MS) {
    if (!ASSET_GC_AUTO_ENABLED || assetGcTimer) return;
    assetGcTimer = setTimeout(async () => {
        assetGcTimer = null;
        try {
            await runServerAssetCleanup({ source: 'scheduled' });
        } catch (error) {
            logger.error('[AssetGC] Scheduled cleanup failed closed:', error);
        } finally {
            scheduleServerAssetCleanup(ASSET_GC_INTERVAL_MS);
        }
    }, delayMs);
    assetGcTimer.unref?.();
}

async function spoolBackupSnapshotRow(snapshot, key, {
    signal,
    shouldAbort,
    onChunk,
} = {}) {
    requireDatabaseSpoolDirSync();
    const size = snapshot.kvSize(key);
    if (!Number.isSafeInteger(size) || size < 0) return null;
    const rowPath = path.join(
        databaseSpoolDir,
        `${DATABASE_SPOOL_FILE_PREFIX}${process.pid}-${nodeCrypto.randomUUID()}.row`,
    );
    try {
        const result = await snapshot.kvWriteToFile(key, rowPath, {
            signal,
            shouldAbort,
            onChunk,
        });
        if (!result || result.size !== size) {
            throw new Error(`Snapshot row changed while spooling: ${key}`);
        }
        return {
            filePath: rowPath,
            size,
            cleanup: () => fs.unlink(rowPath).catch(() => {}),
        };
    } catch (error) {
        await fs.unlink(rowPath).catch(() => {});
        throw error;
    }
}

async function spoolLogicalChatSnapshotRow(snapshot, key, options = {}) {
    requireDatabaseSpoolDirSync();
    const metadata = typeof snapshot.chatRowMetadata === 'function'
        ? snapshot.chatRowMetadata(key)
        : null;
    if (!metadata || metadata.log_count === 0) {
        return spoolBackupSnapshotRow(snapshot, key, options);
    }
    if (options.signal?.aborted || options.shouldAbort?.()) {
        throw new DOMException('Snapshot chat-row spool was aborted', 'AbortError');
    }
    const bytes = chatRowStore.materializeChatRowBytesFromReader(snapshot, key);
    if (bytes === null) return null;
    if (bytes.length !== metadata.content_size) {
        throw new Error(`Snapshot logical chat-row size mismatch: ${key}`);
    }
    const rowPath = path.join(
        databaseSpoolDir,
        `${DATABASE_SPOOL_FILE_PREFIX}${process.pid}-${nodeCrypto.randomUUID()}.row`,
    );
    try {
        await fs.writeFile(rowPath, bytes, { flag: 'wx', mode: 0o600 });
        return {
            filePath: rowPath,
            size: bytes.length,
            cleanup: () => fs.unlink(rowPath).catch(() => {}),
        };
    } catch (error) {
        await fs.unlink(rowPath).catch(() => {});
        throw error;
    }
}

function listMcpToolCallBackupEntries(reader, kind = 'kv-source') {
    return reader.kvListWithSizes(MCP_TOOL_CALL_CACHE_PREFIX)
        .filter((entry) => parseMcpToolCallStorageKey(entry.key) !== null)
        .map((entry) => ({
            kind,
            key: entry.key,
            backupName: entry.key,
            sortKey: entry.key,
            size: entry.size,
            mcpToolCall: true,
        }));
}

function draftStorageKey(chaId, chatId) {
    return `${DRAFT_PREFIX}${chaId}/${chatId}`;
}

function referencedDraftStorageKeys(database) {
    const keys = new Set();
    for (const character of database?.characters ?? []) {
        if (typeof character?.chaId !== 'string' || character.chaId.length === 0
            || !Array.isArray(character.chats)) continue;
        for (const chat of character.chats) {
            if (typeof chat?.id !== 'string' || chat.id.length === 0) continue;
            keys.add(draftStorageKey(character.chaId, chat.id));
        }
    }
    return keys;
}

function referencedDraftStorageKeysFromChatRows(reader) {
    const keys = new Set();
    for (const entry of reader.kvListWithSizes('chats/')) {
        const parsed = parseChatRowKey(entry.key);
        if (!parsed?.chaId || !parsed.chatId) continue;
        keys.add(draftStorageKey(parsed.chaId, parsed.chatId));
    }
    return keys;
}

function listDraftBackupEntries(
    reader,
    { database = null, kind = 'kv-source' } = {},
) {
    const referenced = database
        ? referencedDraftStorageKeys(database)
        : referencedDraftStorageKeysFromChatRows(reader);
    return reader.kvListWithSizes(DRAFT_PREFIX)
        .filter((entry) => referenced.has(entry.key))
        .map((entry) => ({
            kind,
            key: entry.key,
            backupName: entry.key,
            sortKey: entry.key,
            size: entry.size,
        }));
}

function serializeMcpToolCallPayload(storageKey, callId, value) {
    if (!value || typeof value !== 'object' || Array.isArray(value)
        || !value.call || typeof value.call !== 'object' || Array.isArray(value.call)
        || value.call.id !== callId
        || typeof value.call.name !== 'string'
        || !Array.isArray(value.response)) {
        throw new TypeError(`Invalid remembered MCP tool-call payload: ${storageKey}`);
    }
    const serialized = JSON.stringify(value);
    if (serialized === undefined) {
        throw new TypeError(`Invalid remembered MCP tool-call payload: ${storageKey}`);
    }
    return Buffer.from(serialized, 'utf8');
}

function samePinnedSourceStat(actual, planned) {
    return actual.isFile()
        && actual.size === planned.size
        && actual.dev === planned.dev
        && actual.ino === planned.ino
        && actual.mtimeMs === planned.mtimeMs
        && actual.ctimeMs === planned.ctimeMs;
}

app.get('/', async (req, res, next) => {

    const clientIP = req.ip || 'Unknown IP';
    const timestamp = new Date().toISOString();
    console.log(`[Server] ${timestamp} | Connection from: ${clientIP}`);

    try {
        const mainIndex = await fs.readFile(path.join(process.cwd(), 'dist', 'index.html'))
        const root = htmlparser.parse(mainIndex)
        const head = root.querySelector('head')
        head.innerHTML = `<script>globalThis.__NODE__ = true; globalThis.__PATCH_SYNC__ = ${enablePatchSync}; globalThis.__ALLOW_INSECURE_CONTEXT__ = ${allowInsecureContext}; globalThis.__PLUGIN_STORAGE_DIAG__ = ${isRequestTracingEnabled()}</script>` + head.innerHTML

        res.send(root.toString())
    } catch (error) {
        console.log(error)
        next(error)
    }
})

async function checkAuth(req, res, returnOnlyStatus = false, {allowExpired = false} = {}){
    try {
        const authHeader = req.headers['risu-auth'];

        if(!authHeader){
            console.log('No auth header')
            if(returnOnlyStatus){
                return false;
            }
            res.status(400).send({
                error:'No auth header'
            });
            return false
        }


        //jwt token
        const [
            jsonHeaderB64,
            jsonPayloadB64,
            signatureB64,
        ] = authHeader.split('.');

        //alg, typ
        const jsonHeader = JSON.parse(Buffer.from(jsonHeaderB64, 'base64url').toString('utf-8'));

        //iat, exp
        const jsonPayload = JSON.parse(Buffer.from(jsonPayloadB64, 'base64url').toString('utf-8'));


        //check expiration
        if(!allowExpired){
            const now = Math.floor(Date.now() / 1000);
            if(jsonPayload.exp < now){
                console.log('Token expired')
                if(returnOnlyStatus){
                    return false;
                }
                res.status(400).send({
                    error:'Token Expired'
                });
                return false
            }
        }

        //check signature (HMAC-SHA256)
        if(jsonHeader.alg !== "HS256"){
            console.log('Unsupported algorithm')
            if(returnOnlyStatus){
                return false;
            }
            res.status(400).send({
                error:'Unsupported Algorithm'
            });
            return false
        }

        const expectedSig = nodeCrypto.createHmac('sha256', jwtSecret)
            .update(`${jsonHeaderB64}.${jsonPayloadB64}`)
            .digest()
        const actualSig = Buffer.from(signatureB64, 'base64url')

        if(expectedSig.length !== actualSig.length || !nodeCrypto.timingSafeEqual(expectedSig, actualSig)){
            console.log('Invalid signature')
            if(returnOnlyStatus){
                return false;
            }
            res.status(400).send({
                error:'Invalid Signature'
            });
            return false
        }
        return true
    } catch (error) {
        console.log(error)
        if(returnOnlyStatus){
            return false;
        }
        res.status(500).send({
            error:'Internal Server Error'
        });
        return false
    }
}

registerProxyRoutes(app, {
    checkAuth,
    get logger() { return logger; },
    isHubHostingMode,
});

// Durable model-preset relay. Provider bytes are streamed to the client and
// journaled so an interrupted tab can resume or recover the response.
const { createModelJobs } = require('./runtime/model-jobs.cjs');
const modelJobs = createModelJobs({ saveDir: savePath, logger });
modelJobs.registerRoutes(app, { auth: checkProxyAuth });

// app.get('/api/password', async(req, res)=> {
//     if(password === ''){
//         res.send({status: 'unset'})
//     }
//     else if(req.body.password && req.body.password.trim() === password.trim()){
//         res.send({status:'correct'})
//     }
//     else{
//         res.send({status:'incorrect'})
//     }
// })

app.get('/api/test_auth', async(req, res) => {

    if(!password){
        res.send({status: 'unset'})
    }
    else if(!await checkAuth(req, res, true)){
        // JWT missing/invalid – fall back to session cookie (survives page refresh)
        const sessionToken = parseSessionCookie(req)
        if (sessionToken && (sessions.get(sessionToken) ?? 0) > Date.now()) {
            res.send({status: 'success', token: createServerJwt()})
        } else {
            res.send({status: 'incorrect'})
        }
    }
    else{
        res.send({status: 'success', token: createServerJwt()})
    }
})

app.post('/api/login', loginRouteLimiter, async (req, res) => {
    if(password === ''){
        res.status(400).send({error: 'Password not set'})
        return;
    }
    if(req.body.password && req.body.password.trim() === password.trim()){
        res.send({status:'success', token: createServerJwt()})
    }
    else{
        res.status(400).send({error: 'Password incorrect'})
    }
})

// NodeOnly: token refresh endpoint (pairs with server-side JWT)
app.post('/api/token/refresh', async (req, res) => {
    if (!await checkAuth(req, res, false, {allowExpired: true})) return
    res.json({ token: createServerJwt() })
})

// Side-effect-free state check used when a tab returns to the foreground.
app.get('/api/session/lock-status', async (req, res) => {
    if (!await checkAuth(req, res)) return
    const id = req.headers['x-session-id']
    const clientWriterEpoch = req.headers[WRITER_EPOCH_HEADER]
    res.json({
        state: sessionLock.peek(
            typeof id === 'string' ? id : '',
            typeof clientWriterEpoch === 'string' ? clientWriterEpoch : '',
        ),
        writerEpoch: sessionLock.epoch(),
    })
})

// ── Session cookie issuance (F-0) ──────────────────────────────────────────
// Called once after JWT auth succeeds. Issues a long-lived cookie so that
// <img src="/api/asset/..."> requests can be authenticated without JS.
app.post('/api/session', async (req, res) => {
    if (!await checkAuth(req, res)) return
    const clientSessionId = req.headers['x-session-id']
    if (typeof clientSessionId === 'string') {
        sessionLock.register(clientSessionId)
        console.log('[Session] Session boot registered')
    }
    const token = nodeCrypto.randomBytes(32).toString('hex')
    const expiresAt = Date.now() + 7 * 24 * 60 * 60 * 1000
    sessions.set(token, expiresAt)
    // Prune stale sessions (bounded by single-user usage, safe to do inline)
    for (const [t, exp] of sessions) {
        if (exp < Date.now()) sessions.delete(t)
    }
    saveSessions()
    const maxAge = 7 * 24 * 60 * 60 // seconds
    res.setHeader('Set-Cookie', `risu-session=${token}; HttpOnly; SameSite=Strict; Max-Age=${maxAge}; Path=/`)
    res.json({
        ok: true,
        build: expectedClientBuild,
        writerEpoch: sessionLock.epoch(),
        capabilities: {
            pluginStorage: {
                maxValueBytes: PLUGIN_VALUE_MAX_BYTES,
            },
            pluginStorageBatch: {
                transport: 'framed-v1',
                maxOperations: PLUGIN_STORAGE_BATCH_MAX_OPERATIONS,
                maxMetadataBytes: PLUGIN_STORAGE_BATCH_STREAM_MAX_METADATA_BYTES,
                maxValueBytes: PLUGIN_VALUE_MAX_BYTES,
                maxPayloadBytes: PLUGIN_STORAGE_BATCH_STREAM_MAX_PAYLOAD_BYTES,
            },
            pluginStorageTransition: {
                transport: 'framed-v1',
                maxEntries: PLUGIN_STORAGE_TRANSITION_STREAM_MAX_ENTRIES,
                maxMetadataBytes: PLUGIN_STORAGE_TRANSITION_STREAM_MAX_METADATA_BYTES,
                maxValueBytes: PLUGIN_TRANSITION_MAX_ROW_BYTES,
                maxPayloadBytes: PLUGIN_STORAGE_TRANSITION_STREAM_MAX_PAYLOAD_BYTES,
            },
            database: {
                rawBootRead: true,
                atomicCreate: true,
                optimizedPluginStorageBootReconcile: true,
                rawBootByteLength: readRawBootByteLengthHint(),
            },
        },
    })
})

// ── Direct asset serving (F-1) ─────────────────────────────────────────────
// Serves filesystem-backed assets (with legacy KV fallback) as proper HTTP
// responses with long-term caching.
// Key is hex-encoded to safely pass through URL. Auth via session cookie.
//
// Storage formats differ by key prefix:
//   assets/*        → raw binary (Uint8Array)
//   inlay/*         → JSON { data: "data:<mime>;base64,...", ext, type, ... }
//   inlay_thumb/*   → JSON { data: "data:<mime>;base64,...", ext, type, ... }

/**
 * Extract raw binary and content-type from a KV value.
 * Handles both raw binary (assets/) and JSON+base64 wrapped (inlay/) formats.
 */
function resolveAssetPayload(key, rawValue) {
    // inlay/ and inlay_thumb/ keys store JSON with base64 data URI
    if (key.startsWith('inlay/') || key.startsWith('inlay_thumb/')) {
        try {
            const json = JSON.parse(rawValue.toString('utf-8'))
            const dataUri = json.data
            if (typeof dataUri === 'string' && dataUri.startsWith('data:')) {
                // Parse "data:<mime>;base64,<payload>"
                const commaIdx = dataUri.indexOf(',')
                const meta = dataUri.substring(5, commaIdx) // after "data:"
                const mime = meta.split(';')[0]
                const binary = Buffer.from(dataUri.substring(commaIdx + 1), 'base64')
                return { binary, contentType: mime || 'application/octet-stream' }
            }
            // Fallback: ext field
            const ext = (json.ext || '').toLowerCase()
            const mime = ASSET_EXT_MIME[ext] || 'application/octet-stream'
            return { binary: rawValue, contentType: mime }
        } catch {
            // JSON parse failed — treat as raw binary
        }
    }

    // assets/* and others: raw binary
    const ext = key.split('.').pop()?.toLowerCase()
    const contentType = ASSET_EXT_MIME[ext] || detectMime(rawValue)
    return { binary: rawValue, contentType }
}

const THUMB_MAX_SIDE = 320;
const THUMB_QUALITY = 75;
const THUMB_IMAGE_EXTS = new Set(['png', 'jpg', 'jpeg', 'gif', 'webp']);

async function generateThumbnail(buffer) {
    const vips = await getVips()
    const img = vips.Image.thumbnailBuffer(buffer, THUMB_MAX_SIDE, {
        height: THUMB_MAX_SIDE,
        size: 'down',
    })
    try {
        const out = img.writeToBuffer('.webp', { Q: THUMB_QUALITY })
        return Buffer.from(out);
    } finally {
        img.delete()
    }
}

app.get('/api/asset/:hexKey', sessionAuthMiddleware, async (req, res) => {
    try {
        let key;
        try {
            ({ decodedKey: key } = decodeAndCanonicalizeHexPath(req.params.hexKey));
        } catch (error) {
            if (error?.code === 'INVALID_HEX_PATH') {
                return res.status(400).set('Cache-Control', 'no-store').json({
                    error: 'Invalid canonical UTF-8 hex path',
                    code: error.code,
                });
            }
            throw error;
        }

        if (key.startsWith('inlay/')) {
            const id = key.slice('inlay/'.length)
            const file = await readInlayFile(id)
            if (file) {
                const etag = `"${Math.floor(file.mtimeMs)}"`
                if (req.headers['if-none-match'] === etag) {
                    return res.status(304).set('Cache-Control', 'public, max-age=31536000, immutable').end()
                }
                res.set({
                    'Content-Type': file.mime,
                    'Cache-Control': 'public, max-age=31536000, immutable',
                    'ETag': etag,
                })
                return res.send(file.buffer)
            }
            return res.status(404).set('Cache-Control', 'no-store').end()
        }

        if (key.startsWith('inlay_thumb/')) {
            const id = key.slice('inlay_thumb/'.length)
            const sidecar = await readInlaySidecar(id);
            if (!sidecar || sidecar.type !== 'image' || !THUMB_IMAGE_EXTS.has(sidecar.ext)) {
                return res.status(404).end()
            }
            const file = await readInlayFile(id)
            if (!file) return res.status(404).set('Cache-Control', 'no-store').end()
            const etag = `"thumb-${Math.floor(file.mtimeMs)}"`
            if (req.headers['if-none-match'] === etag) {
                return res.status(304).set('Cache-Control', 'public, max-age=31536000, immutable').end()
            }
            const thumb = await generateThumbnail(file.buffer)
            res.set({
                'Content-Type': 'image/webp',
                'Cache-Control': 'public, max-age=31536000, immutable',
                'ETag': etag,
            })
            return res.send(thumb)
        }

        if (key.startsWith('assets/')) {
            const name = assetNameForKey(key)
            if (isSafeAssetName(name)) {
                const fileDisposition = runtimeAssetFileDisposition(name)
                const kvLeads = !fileDisposition.eligible && kvGetUpdatedAt(key) !== null
                if (!kvLeads) {
                    const data = readAssetFile(name)
                    const mtimeMs = assetFileMtimeMs(name)
                    if (data !== null && mtimeMs !== null) {
                        const etag = `"${Math.floor(mtimeMs)}"`
                        if (req.headers['if-none-match'] === etag) {
                            return res.status(304).set('Cache-Control', 'public, max-age=31536000, immutable').end()
                        }
                        const { binary, contentType } = resolveAssetPayload(key, data)
                        res.set({
                            'Content-Type': contentType,
                            'Cache-Control': 'public, max-age=31536000, immutable',
                            'ETag': etag,
                        })
                        return res.send(binary)
                    }
                }
            }
        }

        // Fast-path 304: check updated_at BEFORE loading the blob.
        const updatedAt = kvGetUpdatedAt(key)
        if (updatedAt === null) return res.status(404).set('Cache-Control', 'no-store').end()

        const etag = `"${updatedAt}"`
        if (req.headers['if-none-match'] === etag) {
            return res.status(304).set('Cache-Control', 'public, max-age=31536000, immutable').end()
        }

        const data = kvGet(key)
        if (!data) return res.status(404).set('Cache-Control', 'no-store').end()

        const { binary, contentType } = resolveAssetPayload(key, data)
        res.set({
            'Content-Type': contentType,
            'Cache-Control': 'public, max-age=31536000, immutable',
            'ETag': etag,
        })
        res.send(binary)
    } catch (error) {
        logger.error('[Asset] Failed to serve asset:', error);
        res.status(500).end()
    }
})

app.post('/api/crypto', async (req, res) => {
    try {
        const hash = nodeCrypto.createHash('sha256')
        hash.update(Buffer.from(req.body.data, 'utf-8'))
        res.send(hash.digest('hex'))
    } catch (error) {
        res.status(500).send({ error: 'Crypto operation failed' });
    }
})

// Vertex / google-service-account access tokens. The browser cannot sign the
// RS256 JWT itself: crypto.subtle needs a Secure Context that HTTP remote
// access lacks, and node:crypto isn't in the client bundle. So the client
// forwards the SA JSON here and the server signs + exchanges it. Google's token
// response is forwarded verbatim so the client maps statuses unchanged.
// Never log the SA JSON / private key / assertion / OAuth body.
const GOOGLE_OAUTH_TOKEN_URI = 'https://oauth2.googleapis.com/token'
app.post('/api/model-preset/google-service-account/token', async (req, res) => {
    if (!await checkAuth(req, res)) return
    try {
        const serviceAccountJson = req.body && req.body.serviceAccountJson
        const scope = (req.body && typeof req.body.scope === 'string' && req.body.scope.length > 0)
            ? req.body.scope
            : 'https://www.googleapis.com/auth/cloud-platform'
        if (typeof serviceAccountJson !== 'string' || serviceAccountJson.length === 0) {
            res.status(400).send({ error: 'serviceAccountJson required' })
            return
        }
        let sa
        try {
            sa = JSON.parse(serviceAccountJson)
        } catch {
            res.status(400).send({ error: 'invalid service account JSON' })
            return
        }
        const clientEmail = sa && sa.client_email
        const privateKey = sa && sa.private_key
        const kid = sa && sa.private_key_id
        const tokenUri = (sa && typeof sa.token_uri === 'string' && sa.token_uri.length > 0)
            ? sa.token_uri
            : GOOGLE_OAUTH_TOKEN_URI
        if (typeof clientEmail !== 'string' || typeof privateKey !== 'string') {
            res.status(400).send({ error: 'service account missing client_email / private_key' })
            return
        }
        // SSRF / signed-JWT exfiltration guard: only Google's documented endpoint.
        if (tokenUri !== GOOGLE_OAUTH_TOKEN_URI) {
            res.status(400).send({ error: 'unsupported token_uri' })
            return
        }
        const nowSec = Math.floor(Date.now() / 1000)
        const header = { alg: 'RS256', typ: 'JWT' }
        if (typeof kid === 'string' && kid.length > 0) header.kid = kid
        const payload = { iss: clientEmail, scope, aud: tokenUri, iat: nowSec, exp: nowSec + 3600 }
        const signingInput =
            `${Buffer.from(JSON.stringify(header)).toString('base64url')}.` +
            `${Buffer.from(JSON.stringify(payload)).toString('base64url')}`
        let signature
        try {
            const signer = nodeCrypto.createSign('RSA-SHA256')
            signer.update(signingInput)
            signer.end()
            signature = signer.sign(privateKey).toString('base64url')
        } catch {
            res.status(400).send({ error: 'failed to sign with the provided private key' })
            return
        }
        const assertion = `${signingInput}.${signature}`

        let googleRes
        try {
            googleRes = await fetch(tokenUri, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    Accept: 'application/json',
                },
                body: new URLSearchParams({
                    grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                    assertion,
                }).toString(),
            })
        } catch {
            res.status(502).send({ error: 'OAuth token endpoint unreachable' })
            return
        }

        // Forward Google's status + body verbatim (client maps errors).
        const text = await googleRes.text().catch(() => '')
        const contentType = googleRes.headers.get('content-type')
        if (contentType) res.set('content-type', contentType)
        res.status(googleRes.status).send(text)
    } catch {
        res.status(500).send({ error: 'service account token exchange failed' })
    }
})


app.post('/api/set_password', async (req, res) => {
    if(password === ''){
        password = req.body.password
        writeFileSync(passwordPath, password, 'utf-8')
        res.send({status: 'success'})
    }
    else{
        res.status(400).send("already set")
    }
})

app.get('/api/read', async (req, res, next) => {
    if(!await checkAuth(req, res)){
        return;
    }
    const filePath = req.headers['file-path'];
    if (!filePath) {
        console.log('no path')
        res.status(400).send({ error:'File path required' });
        return;
    }
    if(!isHex(filePath)){
        res.status(400).send({ error:'Invaild Path' });
        return;
    }
    try {
        const { decodedKey: key } = decodeAndCanonicalizeHexPath(filePath);
        if (key === 'database/database.bin') {
            let prepared;
            try {
                prepared = await queueStorageReadAfterImports(async () => {
                    await flushPendingDb();
                    return prepareLiveDatabaseRead('Read');
                });
            } catch (error) {
                logger.error('[Read] Failed to load database.bin', error);
                return next(error);
            }
            if (!prepared) return res.send();
            rememberSessionPluginStorageState(req, prepared.strippedDatabase);
            if (req.headers['if-none-match'] === prepared.etag) {
                return res.status(304).end();
            }
            res.setHeader('x-db-etag', prepared.etag);
            if (DB_CACHE_TEST_DIAGNOSTICS) {
                res.setHeader('x-pocketrisu-test-db-cache', prepared.cacheStatus);
            }
            res.setHeader('Content-Type', 'application/octet-stream');
            res.send(prepared.fullBlob);
            return;
        }
        // Imports hold an open transaction on the server's only SQLite
        // connection while clearing and repopulating plugin rows. Waiting at
        // the last async boundary before kvGet ensures reads observe either the
        // pre-import commit or the post-import commit/rollback, never the
        // transaction's transient contents.
        await importBarrier.waitUntilIdle();
        let value = null;
        if (key.startsWith('inlay/')) {
            value = await readInlayAssetPayload(key.slice('inlay/'.length));
        } else if (key.startsWith('inlay_info/')) {
            value = await readInlayInfoPayload(key.slice('inlay_info/'.length));
        } else if (key.startsWith('assets/')) {
            value = readAssetValue(key);
        }
        if (value === null && !key.startsWith('assets/')) {
            if (canonicalPluginStorageRowPrefix(key)) {
                try {
                    value = await readGenerationBoundPluginStorageRow(req, key);
                } catch (error) {
                    if (error?.pluginStorageNamespaceConflict) {
                        return res.status(409).json({ error: error.message });
                    }
                    throw error;
                }
            } else {
                value = await kvGetAsync(key);
            }
        }
        if(value === null){
            res.send();
        } else {
            const cachedHashes = parseCachedHashesHeader(req.headers['x-cached-hashes']);
            if (cachedHashes.length > 0) {
                const contentHash = sha256Hex(value);
                res.setHeader('x-content-hash', contentHash);
                if (cachedHashes.includes(contentHash)) {
                    return res.status(204).end();
                }
            }
            res.setHeader('Content-Type', 'application/octet-stream');
            res.send(value);
        }
    } catch (error) {
        next(error);
    }
});

const cachedDbReadJsonParser = express.json({ limit: '1mb' });

app.post('/api/db/read-cached', (req, res, next) => {
    cachedDbReadJsonParser(req, res, (error) => {
        if (!error) return next();
        const status = error.type === 'entity.too.large' ? 413 : 400;
        return res.status(status).json({ error: error.message });
    });
}, async (req, res, next) => {
    if (!await checkAuth(req, res)) return;

    let inventory;
    try {
        inventory = parseDbCacheInventory(req.body);
    } catch (error) {
        return res.status(400).json({ error: error.message });
    }

    try {
        let selected;
        try {
            selected = await queueStorageReadAfterImports(async () => {
                await flushPendingDb();
                const prepared = await prepareLiveDatabaseRead('ReadCached', {
                    includeFullBlob: false,
                });
                if (!prepared) return null;
                return {
                    prepared,
                    cachedRead: dbSegmentMemo.build(
                        prepared.strippedDatabase,
                        inventory,
                        prepared.etag,
                        prepared.revision,
                    ),
                };
            });
        } catch (error) {
            logger.error('[ReadCached] Failed to load database.bin', error);
            return next(error);
        }
        if (!selected) return res.status(404).json({ error: 'Database not found' });
        const { prepared, cachedRead } = selected;
        rememberSessionPluginStorageState(req, prepared.strippedDatabase);
        if (DB_CACHE_TEST_DIAGNOSTICS) {
            res.setHeader(
                'x-pocketrisu-test-db-segments-encoded',
                String(cachedRead.stats.encodedSegments),
            );
            res.setHeader(
                'x-pocketrisu-test-db-segments-reused',
                String(cachedRead.stats.reusedSegments),
            );
        }
        if (cachedRead.kind === 'raw-boot') {
            res.setHeader('x-pocketrisu-db-cache-bypass', cachedRead.reason);
            return res.status(413).json({
                error: 'Database root exceeds the segmented cache value limit',
                code: 'DATABASE_CACHE_ROOT_TOO_LARGE',
            });
        }
        res.setHeader('x-db-etag', prepared.etag);
        if (DB_CACHE_TEST_DIAGNOSTICS) {
            res.setHeader('x-pocketrisu-test-db-cache', prepared.cacheStatus);
        }
        res.setHeader('Content-Type', 'application/octet-stream');
        res.send(encodeCachedDbReadEnvelope(cachedRead.envelope));
    } catch (error) {
        next(error);
    }
});

// Bootstrap needs one decode-free path to the authoritative monolith. The
// ordinary database read intentionally normalizes/externalizes its payload;
// when those bytes are corrupt that normalization fails before the browser can
// select an internal recovery snapshot. This endpoint performs no publication
// work and remains behind auth plus the import read barrier.
app.get('/api/db/read-raw-for-boot', async (req, res, next) => {
    if (!await checkAuth(req, res)) return;
    try {
        const raw = await queueStorageReadAfterImports(async () => {
            await flushPendingDb();
            return kvGetAsync('database/database.bin');
        });
        // A missing endpoint is also a 404. Use an explicit successful empty
        // response so newer clients never confuse version skew with a fresh
        // installation and overwrite an older server's database.
        if (raw === null) return res.status(204).end();
        res.setHeader('x-db-etag', computeBufferEtag(raw));
        res.setHeader('Content-Type', 'application/octet-stream');
        res.send(raw);
    } catch (error) {
        next(error);
    }
});

// Fresh initialization must never use the generic replacement endpoint. The
// queue linearizes this check with every other storage mutation, and the
// transaction makes creation a single create-only publication.
app.post('/api/db/create-if-absent', async (req, res, next) => {
    if (!await checkAuth(req, res)) return;
    if (!checkActiveSession(req, res)) return;
    let shouldCreateBackup = false;
    try {
        const result = await queueStorageMutation(async () => {
            await flushPendingDb();
            const existing = kvGet('database/database.bin');
            if (existing !== null) {
                return {
                    created: false,
                    currentEtag: computeBufferEtag(existing),
                };
            }

            const database = {};
            const encoded = Buffer.from(encodeRisuSaveLegacy(database));
            const created = sqliteDb.transaction(() => {
                // Keep the condition inside the transaction as defense in depth
                // if this route is ever reused outside queueStorageMutation().
                if (kvGet('database/database.bin') !== null) return false;
                kvSet('database/database.bin', encoded);
                return true;
            })();
            if (!created) {
                const committed = kvGet('database/database.bin');
                return {
                    created: false,
                    currentEtag: committed === null ? null : computeBufferEtag(committed),
                };
            }

            invalidateDbCache();
            replaceDbCacheValue(DB_HEX_KEY, database, {
                revision: kvGetDatabaseRevision(),
                estimatedBytes: encoded.length,
                dirty: false,
            });
            dbEtag = computeBufferEtag(encoded);
            seedDbCacheEtag(DB_HEX_KEY, dbEtag);
            rememberSessionPluginStorageState(req, database);
            shouldCreateBackup = true;
            return { created: true, etag: dbEtag };
        });

        if (!result.created) {
            return res.status(409).json({
                success: false,
                created: false,
                error: 'Database already exists',
                code: 'DATABASE_ALREADY_EXISTS',
                currentEtag: result.currentEtag,
                retryable: false,
                commitOutcome: 'not-committed',
                commitOutcomeUnknown: false,
            });
        }
        res.status(201).json({
            success: true,
            created: true,
            etag: result.etag,
            commitOutcome: 'committed',
            commitOutcomeUnknown: false,
        });
        if (shouldCreateBackup) scheduleBackupAndRotate();
    } catch (error) {
        if (isImportInProgressError(error)) return sendImportBusy(res);
        next(error);
    }
});

const pluginStorageRoutesCtx = {
    checkAuth,
    checkActiveSession,
    captureActiveSessionWriteRequest,
    checkActiveSessionWrite,
    queueStorageMutation,
    queueStorageReadAfterImports,
    isImportInProgressError,
    sendImportBusy,
    readLivePluginStoragePublication,
    sessionPluginStorageReadState,
    pluginStorageNamespaceConflict,
    writePluginStorageManifest,
    readPluginStorageManifestState,
    pluginStorageManifestEquals,
    pluginStorageGeneration,
    resolveOwnedPluginStorageKeys,
    newPluginRecoverySnapshotToken,
    markPluginRecoverySnapshotDirty,
    schedulePluginRecoverySnapshot,
    reconcileOptimizedPluginStorageForBoot,
    flushPendingDb,
    decodeAuthoritativeDatabase,
    collectOptimizedBootInlineEntries,
    decodeOptimizedBootStorageKey,
    canonicalizeOptimizedPluginStorageRow,
    canonicalPluginStorageRowPrefix,
    normalizePluginStorageManifestRequest,
    assertPluginStorageSource,
    maybeFailPluginStorageTransaction,
    pluginStorageManifestCache,
    writeWithBackpressure,
    throwIfSignalAborted,
    isHex,
    decodeAndCanonicalizeHexPath,
    logPluginStorageValidationFailure,
    risuSavePreparationRefusal,
    requireDatabaseSpoolDirSync,
    ensureDatabaseSpoolDirSync,
    getDatabaseSpoolDir,
    getPluginTransitionStageDir,
    pluginTransitionStageRowPath,
    readPluginTransitionStage,
    writePluginTransitionStage,
    fsyncPluginTransitionStageDirectory,
    pluginTransitionStageBelongsToRequest,
    removePluginTransitionStage,
    removePluginTransitionStageRows,
    findActivePluginTransition,
    refreshPluginTransitionStageState,
    pluginTransitionDesiredManifest,
    checkDiskSpace,
    computeFileEtag,
    getDbEtag,
    ensurePluginStorageTransitionDbEtag,
    publishPluginStorageTransitionDbState,
    getSessionLockEpoch,
    PLUGIN_STORAGE_BATCH_MAX_OPERATIONS,
    PLUGIN_STORAGE_BATCH_MAX_BODY_BYTES,
    PLUGIN_STORAGE_BATCH_STREAM_MAX_METADATA_BYTES,
    PLUGIN_STORAGE_BATCH_STREAM_MAX_PAYLOAD_BYTES,
    PLUGIN_STORAGE_TRANSITION_STREAM_MAX_ENTRIES,
    PLUGIN_STORAGE_TRANSITION_STREAM_MAX_METADATA_BYTES,
    PLUGIN_STORAGE_TRANSITION_STREAM_MAX_PAYLOAD_BYTES,
    PLUGIN_TRANSITION_MAX_ROW_BYTES,
    PLUGIN_STORAGE_UUID_PATTERN,
    PLUGIN_VALUE_SPOOL_FILE_PREFIX,
    PLUGIN_BATCH_VALUE_SPOOL_FILE_PREFIX,
    PLUGIN_RECOVERY_DOWNLOAD_SPOOL_FILE_PREFIX,
    PLUGIN_TRANSITION_STAGE_PREFIX,
    DATABASE_SPOOL_FILE_PREFIX,
    LEGACY_DATABASE_IMPORT_MAX_BYTES,
    expectedClientBuild,
};

registerPluginStorageClearRoute(app, pluginStorageRoutesCtx);

app.get('/api/inlays/references', async (req, res, next) => {
    if (!await checkAuth(req, res)) return;
    try {
        const result = await queueStorageMutation(() => scanAuthoritativeInlayReferences());
        res.json(result);
    } catch (error) {
        if (isImportInProgressError(error)) return sendImportBusy(res);
        next(error);
    }
});

app.post('/api/inlays/delete-unreferenced', async (req, res, next) => {
    if (!await checkAuth(req, res)) return;
    if (!checkActiveSession(req, res)) return;
    const request = validateInlayDeleteRequest(req.body);
    if (!request) {
        return res.status(400).json({
            success: false,
            error: `ids must contain 1-${MAX_INLAY_DELETE_BATCH} safe inlay IDs`,
            code: 'INVALID_INLAY_DELETE_REQUEST',
            commitOutcome: 'not-committed',
            commitOutcomeUnknown: false,
        });
    }
    try {
        const result = await queueStorageMutation(() => deleteUnreferencedInlays(
            request.ids,
            request.clientProtectedIds,
        ));
        res.json({
            success: true,
            ...result,
            commitOutcome: 'committed',
            commitOutcomeUnknown: false,
        });
    } catch (error) {
        if (isImportInProgressError(error)) return sendImportBusy(res);
        next(error);
    }
});

app.get('/api/remove', async (req, res, next) => {
    if(!await checkAuth(req, res)){
        return;
    }
    if (!checkActiveSession(req, res)) return;
    const filePath = req.headers['file-path'];
    if (!filePath) {
        res.status(400).send({ error:'File path required' });
        return;
    }
    if(!isHex(filePath)){
        res.status(400).send({ error:'Invaild Path' });
        return;
    }
    try {
        await queueStorageMutation(async () => {
            const { decodedKey: key } = decodeAndCanonicalizeHexPath(filePath);
            if (key === 'database/database.bin'
                || key === PLUGIN_STORAGE_MANIFEST_KEY
                || canonicalPluginStorageRowPrefix(key)) {
                try {
                    assertGenericPluginStorageMutationAllowed(
                        key,
                        await readLivePluginStoragePublication(),
                    );
                } catch (error) {
                    if (error?.pluginStorageNamespaceConflict) {
                        return res.status(409).json({ error: error.message });
                    }
                    throw error;
                }
            }
            if (key.startsWith('inlay/')) {
                const id = key.slice('inlay/'.length)
                if (!isSafeInlayId(id)) {
                    return res.status(400).json({ error: 'Invalid inlay ID' });
                }
                const result = await deleteUnreferencedInlays([id]);
                if (result.referencedIds.length > 0) {
                    return res.status(409).json({
                        success: false,
                        error: 'The inlay is still referenced by a stored chat message',
                        code: 'INLAY_REFERENCED',
                        referencedIds: result.referencedIds,
                        commitOutcome: 'not-committed',
                        commitOutcomeUnknown: false,
                    });
                }
                return res.send({ success: true });
            }
            if (key.startsWith('inlay_info/')) {
                const id = key.slice('inlay_info/'.length);
                const scan = await scanAuthoritativeInlayReferences();
                if ((scan.refCounts[id] ?? 0) > 0) {
                    return res.status(409).json({
                        success: false,
                        error: 'The inlay is still referenced by a stored chat message',
                        code: 'INLAY_REFERENCED',
                        commitOutcome: 'not-committed',
                        commitOutcomeUnknown: false,
                    });
                }
                await deleteInlaySidecars(id);
            }
            if (key.startsWith('inlay_meta/')) {
                const id = key.slice('inlay_meta/'.length);
                const scan = await scanAuthoritativeInlayReferences();
                if ((scan.refCounts[id] ?? 0) > 0) {
                    return res.status(409).json({
                        success: false,
                        error: 'The inlay is still referenced by a stored chat message',
                        code: 'INLAY_REFERENCED',
                        commitOutcome: 'not-committed',
                        commitOutcomeUnknown: false,
                    });
                }
            }
            if (key.startsWith('assets/')) {
                deleteAssetValue(key);
            } else {
                kvDel(key);
            }
            res.send({ success: true });
        });
    } catch (error) {
        if (isImportInProgressError(error)) return sendImportBusy(res);
        next(error);
    }
});

app.get('/api/list', async (req, res, next) => {
    if(!await checkAuth(req, res)){
        return;
    }
    try {
        const firstHeader = (value) => Array.isArray(value) ? value[0] : value;
        const keyPrefixHeader = firstHeader(req.headers['key-prefix']);
        const lastSyncHeader = firstHeader(req.headers['x-last-sync']);
        const epochHeader = firstHeader(req.headers['x-list-epoch']);
        const keyPrefix = typeof keyPrefixHeader === 'string' ? keyPrefixHeader : '';
        const parsedLastSync = Number(lastSyncHeader);
        const lastSync = Number.isSafeInteger(parsedLastSync) ? parsedLastSync : 0;
        await importBarrier.waitUntilIdle();
        const serverEpoch = kvGetListEpoch();
        const response = await buildListResponse({
            keyPrefix,
            lastSync,
            clientEpoch: typeof epochHeader === 'string' ? epochHeader : '',
            serverEpoch,
            now: Date.now(),
            listKv: kvList,
            listModifiedKv: kvListModifiedSince,
            listDeletedKv: kvGetDeletedSince,
            listAssetEntries: listAssetEntriesWithSizes,
            listInlayEntries: listInlayFiles,
            statFile: fs.stat,
        });
        res.send({ success: true, ...response });
    } catch (error) {
        next(error);
    }
});

registerPluginStorageStateRoutes(app, pluginStorageRoutesCtx);

registerStorageCapacityRoute(app, {
    checkAuth,
    getImportBarrier,
    isHubHostingMode,
    checkDiskSpace,
});

registerPluginStorageManagementRoutes(app, pluginStorageRoutesCtx);

registerPluginStorageBatchRoute(app, pluginStorageRoutesCtx);

registerStorageListSizesRoute(app, {
    checkAuth,
    getImportBarrier,
});

registerPluginStorageMutationRoute(app, pluginStorageRoutesCtx);

// ─── /api/logs — client-side error/warning/info log persistence ───────────────
registerLogRoutes(app, {
    checkAuth,
    checkActiveSession,
});



function pluginTransitionDesiredManifest(stage) {
    if (!stage.targetOptimized) return null;
    return createPluginStorageManifest(
        stage.targetGeneration,
        stage.rows
            .filter(row => row.storageKey.startsWith(PLUGIN_SAVE_PREFIX))
            .map(row => row.storageKey),
        stage.rows
            .filter(row => row.storageKey.startsWith(PLUGIN_SAVE_META_PREFIX))
            .map(row => row.storageKey),
        mergePluginStorageKeyMappings(
            null,
            stage.rows.map(row => row.rawKey),
            stage.rows
                .filter(row => row.storageKey.startsWith(PLUGIN_SAVE_PREFIX))
                .map(row => row.storageKey),
            stage.rows
                .filter(row => row.storageKey.startsWith(PLUGIN_SAVE_META_PREFIX))
                .map(row => row.storageKey),
        ),
    );
}

async function refreshPluginTransitionStageState(stage) {
    if (!stage || stage.state === 'committed' || stage.state === 'aborted') return stage;
    const rawDatabase = kvGet('database/database.bin');
    if (!rawDatabase) return stage;
    const dbObj = await decodeAuthoritativeDatabase(rawDatabase);
    const manifest = readPluginStorageManifestState().manifest;
    if (
        (dbObj?.optimizePluginMemory === true) === stage.targetOptimized
        && pluginStorageGeneration(dbObj) === stage.targetGeneration
        && pluginStorageManifestEquals(manifest, pluginTransitionDesiredManifest(stage))
    ) {
        stage.state = 'committed';
        stage.resultEtag = computeBufferEtag(rawDatabase);
        stage.updatedAt = Date.now();
        writePluginTransitionStage(stage);
        removePluginTransitionStageRows(stage);
    }
    return stage;
}

/**
 * A process can exit after the SQLite publication commits but before its
 * private receipt is rewritten. Keep fresh ready/uploading receipts until the
 * live database passes boot preflight, then resolve them against authoritative
 * state. Unpublished stages are removed before the server begins accepting
 * clients on a healthy boot; recovery-mode startup leaves private receipts
 * untouched until the database is repaired.
 */
async function reconcilePluginTransitionStagesAtStartup() {
    if (!ensureDatabaseSpoolDirSync()) return;
    // Keep the early module load read-only with respect to private transition
    // receipts. Startup calls this only after database preflight, so corrupt
    // recovery boots cannot discard a staged authoritative source.
    sweepStalePluginTransitionStages();
    for (const entry of readdirSync(pluginTransitionStageDir, { withFileTypes: true })) {
        if (!entry.isFile() || !entry.name.endsWith('.json')) continue;
        const match = entry.name.match(
            /^\.plugin-transition-stage-([0-9a-f-]{36})\.json$/,
        );
        if (!match || !PLUGIN_STORAGE_UUID_PATTERN.test(match[1])) continue;
        let stage = readPluginTransitionStage(match[1]);
        if (!stage || stage.state === 'aborted') {
            removePluginTransitionStage(stage ?? { transitionId: match[1], rows: [] });
            continue;
        }
        if (stage.state !== 'committed') {
            stage = await refreshPluginTransitionStageState(stage);
        }
        if (stage?.state !== 'committed') removePluginTransitionStage(stage);
    }
}


async function computeFileEtag(filePath) {
    const hash = nodeCrypto.createHash('md5');
    for await (const chunk of createReadStream(filePath)) hash.update(chunk);
    return hash.digest('hex');
}


registerPluginStorageTransitionRoutes(app, pluginStorageRoutesCtx);

// Provider request history and token-usage statistics use their own rotated DB;
// this deliberately coexists with logs.cjs system logging and redaction.
const requestLogs = createRequestLogs({ saveDir: savePath });
requestLogs.registerRoutes(app, { auth: checkAuth, activeSession: checkActiveSession });

async function writePrivateAdmittedStageFile(filePath, value) {
    const handle = await fs.open(filePath, 'wx', 0o600);
    try {
        await handle.writeFile(value);
        await handle.sync();
    } finally {
        await handle.close();
    }
    return { filePath, size: (await fs.stat(filePath)).size };
}

async function prepareChunkPlanWithFallback(filePath, label) {
    try {
        return await prepareFileChunkPlan(filePath, {
            forceFailure: process.env.NODE_ENV === 'test'
                && process.env.POCKETRISU_TEST_CHUNK_WORKER_FAIL === '1',
        });
    } catch (error) {
        logger.warn(
            `[ChunkPlan] ${label} worker preparation failed; using synchronous file publication:`,
            error?.message || error,
        );
        return null;
    }
}

async function hashFile(filePath, algorithm) {
    const digest = nodeCrypto.createHash(algorithm);
    for await (const chunk of createReadStream(filePath)) digest.update(chunk);
    return digest.digest('hex');
}

async function waitAtAdmittedWritePublishTestGate(kind) {
    if (process.env.NODE_ENV !== 'test') return;
    const gateDir = String(process.env.POCKETRISU_TEST_ADMITTED_WRITE_GATE_DIR ?? '').trim();
    if (!gateDir) return;
    const holdPath = path.join(gateDir, 'hold');
    if (!existsSync(holdPath)) return;
    await fs.mkdir(gateDir, { recursive: true });
    await fs.writeFile(path.join(gateDir, 'entered'), kind, 'utf-8');
    const releasePath = path.join(gateDir, 'release');
    while (existsSync(holdPath) && !existsSync(releasePath)) {
        await new Promise((resolve) => setTimeout(resolve, 10));
    }
}

async function decodeDatabaseSpoolForWrite(spool, stageDir, chatRows) {
    const source = { filePath: spool.filePath, size: spool.size };
    const inspection = await inspectRisuSaveSource(source);
    if (!inspection.supported) {
        // Preserve legacy/block/unknown compatibility only under the same
        // finite in-memory ceiling used by imports/restores. Headerless
        // compressed fallbacks still expand to a 64 KiB-paged private file
        // before the bounded compatibility decoder materializes them.
        return decodeBoundedLegacyRisuSave(source, {
            inspection,
            tempDir: requireDatabaseSpoolDirSync(),
            maxLegacyBytes: LEGACY_DATABASE_IMPORT_MAX_BYTES,
            resolveRemoteSize: async (name) => kvSize(`remotes/${name}.local.bin`),
            resolveRemote: async (name) => kvGet(`remotes/${name}.local.bin`),
        });
    }
    const streamedPluginValues = Object.create(null);
    const streamedPluginMeta = Object.create(null);
    const walked = await walkRisuSave(source, {
        inspection,
        tempDir: requireDatabaseSpoolDirSync(),
        externalizePluginStorage: true,
        onPluginStorageFolded: async () => {},
        onPluginStorageEntry: async ({ field, key, value }) => {
            Object.defineProperty(
                field === 'pluginStorageMeta' ? streamedPluginMeta : streamedPluginValues,
                key,
                { configurable: true, enumerable: true, value, writable: true },
            );
        },
        onMissingChatId: () => nodeCrypto.randomUUID(),
        retainCharacterChats: () => false,
        onChat: async ({ character, chat, externalizable }) => {
            if (!externalizable || !Array.isArray(chat?.message)) return chat;
            const payload = { ...chat };
            if (payload._stub === true) delete payload._stub;
            const index = chatRows.length;
            const filePath = path.join(stageDir, `chat-${index}.bin`);
            const bytes = Buffer.from(encodeRisuSaveLegacy(payload));
            await writePrivateAdmittedStageFile(filePath, bytes);
            chatRows.push({
                chaId: character.chaId,
                chatId: payload.id,
                filePath,
                coldStorage: isColdStorageChat(payload),
                messageCount: payload.message.length,
                logSupported: true,
            });
            return chatRowStore.chatToStub(payload);
        },
    });
    if (walked.pluginStats.changed) {
        walked.remainder.pluginCustomStorage = streamedPluginValues;
        if (walked.pluginStats.hasMetaField) {
            walked.remainder.pluginStorageMeta = streamedPluginMeta;
        }
    }
    if (walked.pluginStats.markerPresent) {
        walked.remainder[PLUGIN_STORAGE_FOLDED_MARKER] = walked.pluginStats.folded;
    }
    return walked.remainder;
}

async function prepareSpooledDatabaseWrite(spool) {
    const stageDir = await fs.mkdtemp(path.join(databaseSpoolDir, ADMITTED_WRITE_STAGE_PREFIX));
    const chatRows = [];
    try {
        const incomingDb = await decodeDatabaseSpoolForWrite(spool, stageDir, chatRows);
        const losses = findStubFlagLossChats(incomingDb);
        if (losses.length > 0) {
            const sample = losses.slice(0, 3)
                .map(loss => `${loss.chaId}/${loss.chatId ?? loss.chatIndex}`).join(', ');
            const error = new Error(
                `write aborted: ${losses.length} chat(s) lost _stub flag without upgrade — `
                + `would silently strip messages on disk. sample=[${sample}]`,
            );
            recordPersistFailure(error, '/api/write:stub-flag-loss');
            logger.error(`[Write] ${error.message}`);
            return {
                stageDir,
                refusal: { status: 500, body: { error: 'Write aborted: chat data integrity check failed' } },
            };
        }
        const duplicateChaIds = findDuplicateChaIds(incomingDb);
        if (duplicateChaIds.length > 0) {
            const sample = duplicateChaIds.slice(0, 3).join(', ');
            const error = new Error(
                `write aborted: ${duplicateChaIds.length} duplicate chaId value(s) — `
                + `would collapse distinct chat rows. sample=[${sample}]`,
            );
            recordPersistFailure(error, '/api/write:duplicate-cha-ids');
            logger.error(`[Write] ${error.message}`);
            return {
                stageDir,
                refusal: { status: 500, body: { error: 'Write aborted: chat data integrity check failed' } },
            };
        }
        const duplicateChatIds = findDuplicateChatIds(incomingDb);
        if (duplicateChatIds.length > 0) {
            const error = new Error(
                `write aborted: ${duplicateChatIds.length} duplicate chat id(s) — `
                + `would alias authoritative rows. sample=[${duplicateChatIdSample(duplicateChatIds)}]`,
            );
            recordPersistFailure(error, '/api/write:duplicate-chat-ids');
            logger.error(`[Write] ${error.message}`);
            return {
                stageDir,
                refusal: { status: 500, body: { error: 'Write aborted: chat data integrity check failed' } },
            };
        }

        const pluginExternalization = preparePluginStorageExternalization(incomingDb);
        const strippedDb = pluginExternalization.strippedDb;
        const normalizedDatabase = normalizeDecodedDatabaseForRead(strippedDb);
        const pluginRows = [];
        for (let index = 0; index < pluginExternalization.rows.length; index++) {
            const row = pluginExternalization.rows[index];
            const filePath = path.join(stageDir, `plugin-${index}.json`);
            await writePrivateAdmittedStageFile(filePath, row.value);
            const validated = validatePluginStorageRow(row.storageKey, row.value);
            pluginRows.push({
                storageKey: row.storageKey,
                filePath,
                displaySize: row.storageKey.startsWith(PLUGIN_SAVE_PREFIX)
                    ? pluginStorageViewerDisplaySize(validated)
                    : null,
            });
        }
        pluginExternalization.rows = pluginRows;

        const changed = chatRows.length > 0 || pluginExternalization.changed;
        let persistedPath = spool.filePath;
        let persistedSize = spool.size;
        if (changed) {
            persistedPath = path.join(stageDir, 'database.bin');
            const streamed = await streamRisuSaveToFile({
                dbObj: strippedDb,
                filePath: persistedPath,
                readChatRow: async () => null,
                foldChatRows: false,
            });
            persistedSize = streamed.size;
        }

        const planned = await Promise.all([
            prepareChunkPlanWithFallback(persistedPath, 'database write'),
            ...chatRows.map(row => prepareChunkPlanWithFallback(row.filePath, 'database chat row')),
            ...pluginRows.map(row => prepareChunkPlanWithFallback(row.filePath, 'database plugin row')),
        ]);
        const databasePlan = planned[0];
        for (let index = 0; index < chatRows.length; index++) {
            chatRows[index].chunkPlan = planned[index + 1];
        }
        for (let index = 0; index < pluginRows.length; index++) {
            pluginRows[index].chunkPlan = planned[index + 1 + chatRows.length];
        }
        const etag = databasePlan?.md5 ?? await hashFile(persistedPath, 'md5');
        return {
            stageDir,
            incomingDb,
            strippedDb,
            normalizedDatabase,
            chatRows,
            pluginExternalization,
            persistedPath,
            persistedSize,
            databasePlan,
            etag,
        };
    } catch (error) {
        if (isAdmittedSpoolPressureError(error)) {
            error.admittedWriteStageDir = stageDir;
            throw error;
        }
        const diagnostic = logPluginStorageValidationFailure(
            '[PluginStorage] Rejected invalid folded database row',
            error,
        );
        if (diagnostic) return { stageDir, refusal: { status: 400, body: diagnostic } };
        if (error instanceof PluginStorageLimitError) {
            error.admittedWriteStageDir = stageDir;
            throw error;
        }
        const preparationRefusal = risuSavePreparationRefusal(error);
        if (preparationRefusal) {
            return { stageDir, refusal: preparationRefusal };
        }
        logger.error('[Write] Failed to externalize database payloads:', error.message);
        if (error?.pluginStorageNamespaceConflict) {
            return { stageDir, refusal: { status: 409, body: { error: error.message } } };
        }
        return { stageDir, refusal: { status: 500, body: { error: 'Database write failed' } } };
    }
}

function writePreparedPluginStorageRows(rows) {
    for (const row of rows) {
        kvSetFromFile(row.storageKey, row.filePath, {
            chunkPlan: row.chunkPlan,
            ...(row.displaySize === null
                ? {}
                : { pluginStorageDisplaySize: row.displaySize }),
        });
    }
}

async function prepareSpooledChatWrite(spool) {
    let stageDir = null;
    try {
        let chatData;
        let logSupported = false;
        if (spool.bodyKind === 'raw') {
            const source = { filePath: spool.filePath, size: spool.size };
            try {
                const inspection = await inspectRisuSaveSource(source);
                logSupported = inspection.format === 'raw';
                chatData = inspection.supported
                    ? (await walkRisuSave(source, {
                        inspection,
                        tempDir: requireDatabaseSpoolDirSync(),
                    })).remainder
                    : await decodeBoundedLegacyRisuSave(source, {
                        inspection,
                        tempDir: requireDatabaseSpoolDirSync(),
                        maxLegacyBytes: LEGACY_DATABASE_IMPORT_MAX_BYTES,
                    });
            } catch (error) {
                if (error?.risuSavePreparationLimit === true) throw error;
                return { refusal: { status: 400, body: { error: 'Invalid binary chat data' } } };
            }
        } else {
            chatData = JSON.parse(await fs.readFile(spool.filePath, 'utf-8'));
        }
        if (!chatData) return { chatData };
        if (chatData._stub === true && !Array.isArray(chatData.message)) {
            return {
                refusal: { status: 400, body: { error: 'Bare chat stubs cannot be stored as chat content' } },
            };
        }
        let filePath = spool.filePath;
        if (spool.bodyKind !== 'raw' || chatData._stub === true) {
            if (chatData._stub === true) {
                chatData = { ...chatData };
                delete chatData._stub;
            }
            stageDir = await fs.mkdtemp(path.join(databaseSpoolDir, ADMITTED_WRITE_STAGE_PREFIX));
            filePath = path.join(stageDir, 'chat.bin');
            await writePrivateAdmittedStageFile(
                filePath,
                Buffer.from(encodeRisuSaveLegacy(chatData)),
            );
            logSupported = true;
        }
        const chunkPlan = await prepareChunkPlanWithFallback(filePath, 'chat write');
        return {
            chatData,
            filePath,
            stageDir,
            chunkPlan,
            contentHash: chunkPlan?.sha256 ?? null,
            coldStorage: isColdStorageChat(chatData),
            messageCount: Array.isArray(chatData.message) ? chatData.message.length : 0,
            logSupported,
        };
    } catch (error) {
        error.admittedWriteStageDir = stageDir;
        throw error;
    }
}

function verifyAssetHashFromDigest(key, digest) {
    const match = typeof key === 'string'
        ? key.match(/^assets\/([0-9a-f]{64})\.[A-Za-z0-9]{1,10}$/)
        : null;
    return match
        ? { claimed: match[1], actual: digest, ok: match[1] === digest }
        : { claimed: null, actual: null, ok: true };
}

function writeAssetValueFromSpool(key, spool, verification) {
    const name = assetNameForKey(key);
    if (name === null) {
        kvSetFromFile(key, spool.filePath, { chunkPlan: spool.chunkPlan });
        assetGcCandidateStore.remove(key);
        return true;
    }
    return withAssetFileMutationAdmission(
        name,
        `admitted spooled asset write ${name}`,
        (fileDisposition, unlocked) => {
            if (fileDisposition?.eligible) {
                const legacyHashMismatch = !verification.ok && isLegacyHashAsset(name);
                if (!verification.ok && !legacyHashMismatch) {
                    const error = new Error('asset content does not match its SHA-256 name');
                    error.code = 'ASSET_HASH_MISMATCH';
                    error.key = key;
                    error.expected = verification.claimed;
                    error.actual = verification.actual;
                    throw error;
                }
                if (legacyHashMismatch) markLegacyHashAsset(name);
                const wrote = unlocked.writeAssetFileFromFile(name, spool.filePath, {
                    skipIfUnchanged: verification.claimed !== null,
                });
                if (verification.ok) clearLegacyHashAsset(name);
                kvDel(key);
                kvClearDeletion(key);
                assetGcCandidateStore.remove(key);
                return wrote;
            }
            kvSetFromFile(key, spool.filePath, { chunkPlan: spool.chunkPlan });
            assetGcCandidateStore.remove(key);
            return true;
        },
    );
}

async function prepareSpooledGenericKvWrite(key, spool) {
    let stageDir = null;
    let parsedInlay = null;
    let inlayPayloadPath = null;
    let parsedInlayInfo = null;
    try {
        if (key.startsWith(PLUGIN_SAVE_PREFIX) || key.startsWith(PLUGIN_SAVE_META_PREFIX)) {
            validatePluginStorageRow(key, await fs.readFile(spool.filePath));
        }
        if (key.startsWith('inlay/')) {
            const id = key.slice('inlay/'.length);
            parsedInlay = JSON.parse(await fs.readFile(spool.filePath, 'utf-8'));
            assertSafeInlayTuple(id, parsedInlay?.ext);
            const type = typeof parsedInlay?.type === 'string' ? parsedInlay.type : 'image';
            const payload = type === 'signature'
                ? Buffer.from(typeof parsedInlay?.data === 'string' ? parsedInlay.data : '', 'utf-8')
                : decodeDataUri(parsedInlay?.data).buffer;
            stageDir = await fs.mkdtemp(path.join(databaseSpoolDir, ADMITTED_WRITE_STAGE_PREFIX));
            inlayPayloadPath = path.join(stageDir, 'inlay-payload');
            await writePrivateAdmittedStageFile(inlayPayloadPath, payload);
        } else if (key.startsWith('inlay_info/')) {
            const id = key.slice('inlay_info/'.length);
            parsedInlayInfo = JSON.parse(await fs.readFile(spool.filePath, 'utf-8'));
            assertSafeInlayTuple(id, parsedInlayInfo?.ext);
        }
        const chunkPlan = key.startsWith('inlay/') || key.startsWith('inlay_info/')
            ? null
            : await prepareChunkPlanWithFallback(spool.filePath, `KV write ${key}`);
        return {
            stageDir,
            chunkPlan,
            parsedInlay,
            inlayPayloadPath,
            parsedInlayInfo,
        };
    } catch (error) {
        error.admittedWriteStageDir = stageDir;
        throw error;
    }
}

async function handleSpooledKvWrite(req, res, next, {
    filePath,
    key,
    spool,
}) {
    let prepared = null;
    let shouldCreateBackup = false;
    try {
        if (key.startsWith(PLUGIN_SAVE_PREFIX) || key.startsWith(PLUGIN_SAVE_META_PREFIX)) {
            try {
                assertArchiveSafePluginSaveStorageKey(key);
            } catch (error) {
                return res.status(400).json({
                    error: error?.message || 'Invalid plugin storage key',
                    code: 'invalid_plugin_storage_key',
                });
            }
        }
        try {
            prepared = key === 'database/database.bin'
                ? await prepareSpooledDatabaseWrite(spool)
                : await prepareSpooledGenericKvWrite(key, spool);
        } catch (error) {
            prepared = {
                stageDir: error?.admittedWriteStageDir ?? null,
                preparationError: error,
            };
        }
        if (!prepared.refusal && !prepared.preparationError) {
            await waitAtAdmittedWritePublishTestGate(
                key === 'database/database.bin' ? 'database' : 'kv',
            );
        }

        await queueStorageMutation(async () => {
            const protectsPluginPublication = key === 'database/database.bin'
                || key === PLUGIN_STORAGE_MANIFEST_KEY
                || canonicalPluginStorageRowPrefix(key);
            const livePluginPublication = protectsPluginPublication
                ? await readLivePluginStoragePublication()
                : null;
            if (prepared.preparationError) throw prepared.preparationError;
            if (prepared.refusal) {
                return res.status(prepared.refusal.status).json(prepared.refusal.body);
            }
            if (key !== 'database/database.bin' && protectsPluginPublication) {
                try {
                    assertGenericPluginStorageMutationAllowed(key, livePluginPublication);
                } catch (error) {
                    if (error?.pluginStorageNamespaceConflict) {
                        return res.status(409).json({ error: error.message });
                    }
                    throw error;
                }
            }
            if (key === 'database/database.bin') {
                const ifMatch = req.headers['x-if-match'];
                if (ifMatch && dbEtag && ifMatch !== dbEtag) {
                    return res.status(409).send({
                        error: 'ETag mismatch - concurrent modification detected',
                        currentEtag: dbEtag,
                    });
                }

                const previousStrippedDb = getCurrentDatabaseCacheValue(filePath)
                    || getCurrentDatabaseCacheValue(DB_HEX_KEY)
                    || null;
                try {
                    assertGenericDatabasePluginPublicationAllowed(
                        livePluginPublication,
                        prepared.incomingDb,
                        prepared.pluginExternalization,
                    );
                } catch (error) {
                    if (error?.pluginStorageNamespaceConflict) {
                        return res.status(409).json({ error: error.message });
                    }
                    throw error;
                }
                const chatRowsToDelete = previousStrippedDb
                    ? chatRowStore.removedChatRowKeys(previousStrippedDb, prepared.strippedDb)
                    : [];
                await captureChatDeletionPreImages(chatRowsToDelete);
                let committedDatabaseRevision = null;
                sqliteDb.transaction(() => {
                    writePreparedPluginStorageRows(prepared.pluginExternalization.rows);
                    writePluginStorageManifest(prepared.pluginExternalization.manifest);
                    for (const row of prepared.chatRows) {
                        chatRowStore.writeChatRowFromFile(row.chaId, row.chatId, row.filePath, {
                            contentHash: row.chunkPlan?.sha256 ?? null,
                            chunkPlan: row.chunkPlan,
                            coldStorage: row.coldStorage,
                            messageCount: row.messageCount,
                            logSupported: row.logSupported,
                        });
                    }
                    kvSetFromFile(key, prepared.persistedPath, {
                        chunkPlan: prepared.databasePlan,
                    });
                    if (previousStrippedDb) {
                        chatRowStore.deleteRemovedChatRows(
                            previousStrippedDb,
                            prepared.strippedDb,
                        );
                    }
                    committedDatabaseRevision = kvGetDatabaseRevision();
                })();

                invalidateDbCache();
                replaceDbCacheValue(filePath, prepared.normalizedDatabase, {
                    revision: committedDatabaseRevision,
                    estimatedBytes: prepared.persistedSize,
                    dirty: false,
                });
                dbEtag = prepared.etag;
                seedDbCacheEtag(filePath, dbEtag);
                rememberSessionPluginStorageState(req, prepared.normalizedDatabase);
                shouldCreateBackup = true;
                return res.send({ success: true, etag: dbEtag, hash: undefined });
            }

            let writeResult = null;
            if (key.startsWith('inlay/')) {
                const id = key.slice('inlay/'.length);
                const parsed = prepared.parsedInlay;
                const type = typeof parsed?.type === 'string' ? parsed.type : 'image';
                const ext = normalizeInlayExt(parsed?.ext);
                await writeInlayFileFromFile(id, ext, prepared.inlayPayloadPath, {
                    ext,
                    name: typeof parsed?.name === 'string' ? parsed.name : id,
                    type,
                    height: typeof parsed?.height === 'number' ? parsed.height : undefined,
                    width: typeof parsed?.width === 'number' ? parsed.width : undefined,
                });
                kvDel(key);
                kvDel(`inlay_thumb/${id}`);
                kvDel(`inlay_info/${id}`);
                kvClearDeletion(key);
            } else if (key.startsWith('inlay_info/')) {
                const id = key.slice('inlay_info/'.length);
                await writeInlaySidecar(id, prepared.parsedInlayInfo);
                kvDel(key);
            } else if (key.startsWith('assets/')) {
                const digest = prepared.chunkPlan?.sha256
                    ?? await hashFile(spool.filePath, 'sha256');
                const assetVerification = verifyAssetHashFromDigest(key, digest);
                writeAssetValueFromSpool(key, {
                    ...spool,
                    chunkPlan: prepared.chunkPlan,
                }, assetVerification);
            } else {
                writeResult = kvSetFromFile(key, spool.filePath, {
                    chunkPlan: prepared.chunkPlan,
                });
            }

            if (dbCache.has(filePath) || saveTimers[filePath]) {
                invalidateDbCacheEntry(filePath);
            }
            return res.send({
                success: true,
                etag: undefined,
                hash: key.startsWith(PLUGIN_SAVE_PREFIX)
                    ? (prepared.chunkPlan?.sha256 ?? writeResult?.sha256)
                    : undefined,
            });
        }, 'api-write-publish');
        if (shouldCreateBackup) scheduleBackupAndRotate();
    } catch (error) {
        if (isImportInProgressError(error)) return sendImportBusy(res);
        const preparationRefusal = risuSavePreparationRefusal(error);
        if (error === prepared?.preparationError && preparationRefusal) {
            return res.status(preparationRefusal.status).json(preparationRefusal.body);
        }
        if (error === prepared?.preparationError
            && isAdmittedSpoolPressureError(error)) {
            return sendRetryableSpoolRefusal(
                res,
                req[BUFFERED_INGRESS_POLICY],
                bufferedIngressLimits.global,
                spool.size,
            );
        }
        const diagnostic = logPluginStorageValidationFailure(
            '[PluginStorage] Rejected invalid row write',
            error,
        );
        if (diagnostic) return res.status(400).json(diagnostic);
        next(error);
    } finally {
        if (prepared?.stageDir) {
            await fs.rm(prepared.stageDir, { recursive: true, force: true }).catch(() => {});
        }
        await disposeAdmittedIngressSpool(req);
    }
}

app.post('/api/write', async (req, res, next) => {
    if(!await checkAuth(req, res)){
        return;
    }
    if (!checkActiveSession(req, res)) return;
    const rawFilePath = req.headers['file-path'];
    const admittedSpool = req[ADMITTED_INGRESS_SPOOL] ?? null;
    const fileContent = admittedSpool ?? req.body;
    if (!rawFilePath || !fileContent) {
        res.status(400).send({ error:'File path required' });
        return;
    }
    if(!isHex(rawFilePath)){
        res.status(400).send({ error:'Invaild Path' });
        return;
    }
    const {
        canonicalPath: filePath,
        decodedKey: key,
    } = decodeAndCanonicalizeHexPath(rawFilePath);
    if (admittedSpool) {
        return handleSpooledKvWrite(req, res, next, {
            filePath,
            key,
            spool: admittedSpool,
        });
    }
    let shouldCreateBackup = false;
    try {
        await queueStorageMutation(async () => {
            const protectsPluginPublication = key === 'database/database.bin'
                || key === PLUGIN_STORAGE_MANIFEST_KEY
                || canonicalPluginStorageRowPrefix(key);
            const livePluginPublication = protectsPluginPublication
                ? await readLivePluginStoragePublication()
                : null;
            let persistedDatabaseContent = fileContent;
            let validatedStrippedDatabase = null;
            let committedDatabaseRevision = null;
            if (
                key.startsWith(PLUGIN_SAVE_PREFIX)
                || key.startsWith(PLUGIN_SAVE_META_PREFIX)
            ) {
                try {
                    // The generic KV API historically permits noncanonical
                    // short keys in these namespaces. Preserve that contract,
                    // but never admit a name the backup parser cannot frame.
                    assertArchiveSafePluginSaveStorageKey(key);
                } catch (error) {
                    res.status(400).json({
                        error: error?.message || 'Invalid plugin storage key',
                        code: 'invalid_plugin_storage_key',
                    });
                    return;
                }
            }
            const assetVerification = key.startsWith('assets/')
                ? verifyAssetHash(key, fileContent)
                : null;
            if (key.startsWith(PLUGIN_SAVE_PREFIX)
                || key.startsWith(PLUGIN_SAVE_META_PREFIX)) {
                try {
                    validatePluginStorageRow(key, fileContent);
                } catch (error) {
                    const diagnostic = logPluginStorageValidationFailure(
                        '[PluginStorage] Rejected invalid row write',
                        error
                    ) ?? {
                        error: 'Invalid plugin storage JSON row',
                        code: 'INVALID_PLUGIN_STORAGE_ROW',
                        encodedKey: key.startsWith(PLUGIN_SAVE_META_PREFIX)
                            ? PLUGIN_SAVE_META_PREFIX
                            : PLUGIN_SAVE_PREFIX,
                    };
                    res.status(400).json(diagnostic);
                    return;
                }
            }
            if (key !== 'database/database.bin' && protectsPluginPublication) {
                try {
                    assertGenericPluginStorageMutationAllowed(key, livePluginPublication);
                } catch (error) {
                    if (error?.pluginStorageNamespaceConflict) {
                        return res.status(409).json({ error: error.message });
                    }
                    throw error;
                }
            }

            // ETag conflict detection for database.bin
            if (key === 'database/database.bin') {
                const ifMatch = req.headers['x-if-match'];
                if (ifMatch && dbEtag && ifMatch !== dbEtag) {
                    res.status(409).send({
                        error: 'ETag mismatch - concurrent modification detected',
                        currentEtag: dbEtag
                    });
                    return;
                }
            }

            if (key.startsWith('inlay/')) {
                const id = key.slice('inlay/'.length)
                const parsed = JSON.parse(Buffer.from(fileContent).toString('utf-8'));
                const type = typeof parsed?.type === 'string' ? parsed.type : 'image';
                const ext = normalizeInlayExt(parsed?.ext);
                const buffer = type === 'signature'
                    ? Buffer.from(typeof parsed?.data === 'string' ? parsed.data : '', 'utf-8')
                    : decodeDataUri(parsed?.data).buffer;
                await writeInlayFile(id, ext, buffer, {
                    ext,
                    name: typeof parsed?.name === 'string' ? parsed.name : id,
                    type,
                    height: typeof parsed?.height === 'number' ? parsed.height : undefined,
                    width: typeof parsed?.width === 'number' ? parsed.width : undefined,
                });
                kvDel(key);
                kvDel(`inlay_thumb/${id}`);
                kvDel(`inlay_info/${id}`);
                kvClearDeletion(key);
            } else if (key.startsWith('inlay_info/')) {
                const id = key.slice('inlay_info/'.length)
                const parsed = JSON.parse(Buffer.from(fileContent).toString('utf-8'));
                await writeInlaySidecar(id, parsed);
                kvDel(key);
            } else if (key === 'database/database.bin') {
                try {
                    // Reuse the existing stripped cache when available. Do not
                    // decode the prior live row solely for targeted cleanup;
                    // optimize's grace-window sweep handles cache-cold writes.
                    const previousStrippedDb = getCurrentDatabaseCacheValue(filePath)
                        || getCurrentDatabaseCacheValue(DB_HEX_KEY)
                        || null;
                    const incomingInspection = await inspectRisuSaveSource(fileContent);
                    const incomingDb = incomingInspection.format === 'raw'
                        ? await decodeAuthoritativeDatabase(fileContent)
                        : await decodeBoundedLegacyRisuSave(fileContent, {
                            inspection: incomingInspection,
                            tempDir: requireDatabaseSpoolDirSync(),
                            maxLegacyBytes: LEGACY_DATABASE_IMPORT_MAX_BYTES,
                            resolveRemoteSize: async (name) => kvSize(`remotes/${name}.local.bin`),
                            resolveRemote: async (name) => kvGet(`remotes/${name}.local.bin`),
                        });

                    // Mirror the patch-persist guard:
                    // a malformed full-write payload could carry chats with
                    // neither `_stub` nor `message` (the v1.4.x metadata-only
                    // pattern). They would land in the stripped DB and silently
                    // strand the corresponding chat row.
                    // Normal clients are safe (RisuSaveEncoder runs chatToStub
                    // on every chat first), but external tools / future
                    // regressions could bypass that — keep the guard at the
                    // disk boundary for defense in depth.
                    const losses = findStubFlagLossChats(incomingDb);
                    if (losses.length > 0) {
                        const sample = losses.slice(0, 3).map(l => `${l.chaId}/${l.chatId ?? l.chatIndex}`).join(', ');
                        const err = new Error(
                            `write aborted: ${losses.length} chat(s) lost _stub flag without upgrade — `
                            + `would silently strip messages on disk. sample=[${sample}]`
                        );
                        recordPersistFailure(err, '/api/write:stub-flag-loss');
                        logger.error(`[Write] ${err.message}`);
                        res.status(500).json({ error: 'Write aborted: chat data integrity check failed' });
                        return;
                    }

                    const duplicateChaIds = findDuplicateChaIds(incomingDb);
                    if (duplicateChaIds.length > 0) {
                        const sample = duplicateChaIds.slice(0, 3).join(', ');
                        const err = new Error(
                            `write aborted: ${duplicateChaIds.length} duplicate chaId value(s) — `
                            + `would collapse distinct chat rows. sample=[${sample}]`
                        );
                        recordPersistFailure(err, '/api/write:duplicate-cha-ids');
                        logger.error(`[Write] ${err.message}`);
                        res.status(500).json({ error: 'Write aborted: chat data integrity check failed' });
                        return;
                    }

                    const duplicateChatIds = findDuplicateChatIds(incomingDb);
                    if (duplicateChatIds.length > 0) {
                        const err = new Error(
                            `write aborted: ${duplicateChatIds.length} duplicate chat id(s) — `
                            + `would alias authoritative rows. sample=[${duplicateChatIdSample(duplicateChatIds)}]`
                        );
                        recordPersistFailure(err, '/api/write:duplicate-chat-ids');
                        logger.error(`[Write] ${err.message}`);
                        res.status(500).json({ error: 'Write aborted: chat data integrity check failed' });
                        return;
                    }

                    const splitDatabase = chatRowStore.splitFullDb(incomingDb);
                    const chatRows = splitDatabase.chatEntries.map(entry => ({
                        ...entry,
                        value: Buffer.from(encodeRisuSaveLegacy(entry.chat)),
                        coldStorage: isColdStorageChat(entry.chat),
                        messageCount: Array.isArray(entry.chat?.message)
                            ? entry.chat.message.length
                            : 0,
                    }));
                    const pluginExternalization = preparePluginStorageExternalization(
                        splitDatabase.strippedDb
                    );
                    assertGenericDatabasePluginPublicationAllowed(
                        livePluginPublication,
                        splitDatabase.strippedDb,
                        pluginExternalization,
                    );
                    const strippedDb = pluginExternalization.strippedDb;
                    // Full writes already paid for authoritative decoding and
                    // validation. Retain the same normalized stubs-only graph
                    // for the exact committed revision instead of decoding the
                    // persisted bytes again here and once more on the next patch.
                    validatedStrippedDatabase = normalizeDecodedDatabaseForRead(strippedDb);
                    if (chatRows.length > 0 || pluginExternalization.changed) {
                        persistedDatabaseContent = Buffer.from(encodeRisuSaveLegacy(strippedDb));
                    }
                    const chatRowsToDelete = previousStrippedDb
                        ? chatRowStore.removedChatRowKeys(previousStrippedDb, strippedDb)
                        : [];
                    await captureChatDeletionPreImages(chatRowsToDelete);

                    // Must stay synchronous: every external row and the stub graph
                    // commit or roll back together with database.bin.
                    sqliteDb.transaction(() => {
                        writePluginStorageRows(pluginExternalization.rows);
                        writePluginStorageManifest(pluginExternalization.manifest);
                        for (const row of chatRows) {
                            chatRowStore.writeChatRowRaw(row.chaId, row.chatId, row.value, {
                                coldStorage: row.coldStorage,
                                messageCount: row.messageCount,
                                logSupported: true,
                            });
                        }
                        kvSet(key, persistedDatabaseContent);
                        if (previousStrippedDb) {
                            chatRowStore.deleteRemovedChatRows(previousStrippedDb, strippedDb);
                        }
                        committedDatabaseRevision = kvGetDatabaseRevision();
                    })();
                } catch (e) {
                    const diagnostic = logPluginStorageValidationFailure(
                        '[PluginStorage] Rejected invalid folded database row',
                        e
                    );
                    if (diagnostic) {
                        res.status(400).json(diagnostic);
                        return;
                    }
                    if (e instanceof PluginStorageLimitError) throw e;
                    const preparationRefusal = risuSavePreparationRefusal(e);
                    if (preparationRefusal) {
                        res.status(preparationRefusal.status).json(preparationRefusal.body);
                        return;
                    }
                    logger.error('[Write] Failed to externalize database payloads:', e.message);
                    if (e?.pluginStorageNamespaceConflict) {
                        res.status(409).json({ error: e.message });
                        return;
                    }
                    res.status(500).json({ error: 'Database write failed' });
                    return;
                }
            } else if (key.startsWith('assets/')) {
                writeAssetValue(key, fileContent, {
                    skipIfUnchanged: assetVerification.claimed !== null,
                    legacyHashMismatch: assetVerification.legacyHashMismatch,
                });
            } else {
                kvSet(key, fileContent);
            }

            // Update ETag and invalidate cache after database.bin write. The
            // snapshot is queued only after this user-visible mutation returns.
            if (key === 'database/database.bin') {
                invalidateDbCache();
                replaceDbCacheValue(filePath, validatedStrippedDatabase, {
                    revision: committedDatabaseRevision,
                    estimatedBytes: persistedDatabaseContent.length,
                    dirty: false,
                });
                // ETag based on stripped version (what client sees)
                dbEtag = computeBufferEtag(persistedDatabaseContent);
                seedDbCacheEtag(filePath, dbEtag);
                rememberSessionPluginStorageState(req, validatedStrippedDatabase);
                shouldCreateBackup = true;
            } else if (dbCache.has(filePath) || saveTimers[filePath]) {
                // A full write supersedes any cached/debounced patch state for
                // the same non-database key.
                invalidateDbCacheEntry(filePath);
            }

            res.send({
                success: true,
                etag: key === 'database/database.bin' ? dbEtag : undefined,
                hash: key.startsWith(PLUGIN_SAVE_PREFIX) ? sha256Hex(fileContent) : undefined,
            });
        }, 'api-write-publish');
        if (shouldCreateBackup) scheduleBackupAndRotate();
    } catch (error) {
        if (isImportInProgressError(error)) return sendImportBusy(res);
        next(error);
    }
});

app.post('/api/db/flush', sessionAuthMiddleware, async (req, res, next) => {
    if (!checkActiveSession(req, res)) return;
    try {
        await queueStorageMutation(async () => {
            await flushPendingDb();
            // A background automatic snapshot can hold a pinned WAL reader
            // below the current end after the write that triggered it has
            // already acknowledged. Assembly does not need this queue and
            // closes the pin before publication re-enters it, so bounded async
            // retries converge without deadlocking the queued flush.
            const checkpoint = await runTrackedWalCheckpointWithBusyRetry(
                'FULL',
                'explicit-flush',
            );
            if (!checkpoint.complete) {
                return res.status(503).send({
                    success: false,
                    durable: false,
                    outcome: 'unknown',
                    retryable: true,
                    error: 'SQLite durability checkpoint is busy; retry the flush',
                    checkpoint,
                    etag: dbEtag ?? undefined,
                });
            }
            res.send({
                success: true,
                durable: true,
                checkpoint,
                etag: dbEtag ?? undefined
            });
        });
    } catch (error) {
        if (isImportInProgressError(error)) return sendImportBusy(res);
        next(error);
    }
});

// ─── Patch sync endpoint ──────────────────────────────────────────────────────
const INLINE_PLUGIN_STORAGE_PATCH_ROOTS = [
    'pluginCustomStorage',
    'pluginStorageMeta',
];
const PLUGIN_STORAGE_CONTROL_PATCH_ROOTS = [
    'optimizePluginMemory',
    PLUGIN_STORAGE_GENERATION_FIELD,
    PLUGIN_STORAGE_FOLDED_MARKER,
];

function pluginStoragePatchPointerKind(pointer) {
    // Preserve the existing whole-document guard. The slash form is retained
    // for compatibility with the original predicate even though RFC 6901 uses
    // the empty string for the document root.
    if (pointer === '' || pointer === '/') return 'document';
    if (typeof pointer !== 'string') return null;
    if (INLINE_PLUGIN_STORAGE_PATCH_ROOTS.some((root) => (
        pointer === `/${root}` || pointer.startsWith(`/${root}/`)
    ))) return 'inline-record';
    if (PLUGIN_STORAGE_CONTROL_PATCH_ROOTS.some((root) => (
        pointer === `/${root}` || pointer.startsWith(`/${root}/`)
    ))) return 'control';
    return null;
}

function patchReferencesPluginStoragePointerKinds(patch, kinds) {
    if (!Array.isArray(patch)) return false;
    return patch.some((operation) => (
        operation && typeof operation === 'object' && (
            kinds.has(pluginStoragePatchPointerKind(operation.path))
            || kinds.has(pluginStoragePatchPointerKind(operation.from))
        )
    ));
}

function patchTouchesPluginStoragePublication(patch) {
    return patchReferencesPluginStoragePointerKinds(
        patch,
        new Set(['document', 'inline-record', 'control']),
    );
}

function patchTouchesPluginStoragePublicationControl(patch) {
    return patchReferencesPluginStoragePointerKinds(
        patch,
        new Set(['document', 'control']),
    );
}

function publicationAllowsInlinePluginStoragePatch(publication) {
    // A completed optimized -> inline transition retains a fresh generation as
    // its mode epoch, so generation presence alone does not imply external rows.
    return Boolean(publication.dbObj)
        && publication.dbObj.optimizePluginMemory !== true
        && publication.dbObj[PLUGIN_STORAGE_FOLDED_MARKER] !== true
        && publication.manifestState.present === false;
}

app.post('/api/patch', async (req, res, next) => {
    if (!enablePatchSync) {
        res.status(404).send({ error: 'Patch sync is not enabled' });
        return;
    }
    if(!await checkAuth(req, res)){
        return;
    }
    if (!checkActiveSession(req, res)) return;
    const rawFilePath = req.headers['file-path'];
    const patch = req.body.patch;
    const expectedHash = req.body.expectedHash;

    if (!rawFilePath || !patch || !expectedHash) {
        res.status(400).send({ error: 'File path, patch, and expected hash required' });
        return;
    }
    if (!isHex(rawFilePath)) {
        res.status(400).send({ error: 'Invaild Path' });
        return;
    }
    const {
        canonicalPath: filePath,
        decodedKey,
    } = decodeAndCanonicalizeHexPath(rawFilePath);

    try {
        await queueStorageMutation(async () => {
            // Manifest rows, optimized rows, mode controls, and whole-document
            // replacements must never reach dbCache or the eager externalizer.
            // Inline value/owner maps are different: database.bin is their sole
            // authority, so retain the original PocketRisu patch behavior only
            // after proving the live server publication is currently inline.
            let rejectPluginStoragePatch = decodedKey === PLUGIN_STORAGE_MANIFEST_KEY
                || Boolean(canonicalPluginStorageRowPrefix(decodedKey));
            if (
                !rejectPluginStoragePatch
                && decodedKey === 'database/database.bin'
                && patchTouchesPluginStoragePublication(patch)
            ) {
                if (patchTouchesPluginStoragePublicationControl(patch)) {
                    rejectPluginStoragePatch = true;
                } else {
                    const livePublication = await readLivePluginStoragePublication();
                    rejectPluginStoragePatch = !publicationAllowsInlinePluginStoragePatch(
                        livePublication,
                    );
                }
            }
            if (rejectPluginStoragePatch) {
                return res.status(409).json({
                    error: 'Patch rejected: plugin storage publication must be changed atomically',
                    code: 'PLUGIN_STORAGE_PUBLICATION_GUARD',
                });
            }

            // For database.bin, reuse is valid only while the authoritative
            // SQLite row revision still matches the decoded stubs-only graph.
            const cachedDb = await loadPatchCache(filePath, decodedKey);
            if (decodedKey === DB_BLOB_KEY
                && dbCache.metadata(filePath)?.revision !== kvGetDatabaseRevision()) {
                releaseDbCacheCanonicalEncoding(filePath, 'external-revision-conflict');
                throw new DatabaseCacheRevisionConflict();
            }

            // Reject patch ops that touch chat-internal fields. Lazy loading
            // strips chats to stubs in dbCache; the only legitimate chat ops
            // are stub metadata (id, name, _stub, lastDate, folderId, modules)
            // or whole-chat add/replace/remove. Field-level ops on chats —
            // particularly remove of message/hypaV3Data/scriptstate/etc —
            // strip the `_stub` flag and cause silent on-disk data loss when
            // persistence later sees the metadata-only chat. Reject as 409 so
            // the client falls through to a full write and rebases its
            // patcher baseline. See findStubFlagLossChats for the disk-side
            // partner guard.
            const chatInternalOps = decodedKey === 'database/database.bin'
                ? findChatInternalFieldOps(patch)
                : [];
            if (chatInternalOps.length > 0) {
                const sample = chatInternalOps.slice(0, 5).map(v => `${v.op} ${v.path}`).join(', ');
                logger.warn(
                    `[Patch] Rejected ${chatInternalOps.length} chat-internal field op(s) `
                    + `(would corrupt lazy-loaded chats): ${sample}`
                );
                let currentEtag;
                try {
                    currentEtag = getDbCacheEtag(filePath);
                    dbEtag = currentEtag;
                } catch {}
                res.status(409).send({
                    error: 'Patch rejected: chat-internal field ops not allowed for lazy-loaded chats',
                    code: 'CHAT_GUARD_REJECTED',
                    chatGuardRejected: true,
                    currentEtag,
                });
                return;
            }

            const serverHash = getDbCacheHash(filePath);

            if (expectedHash !== serverHash) {
                console.log(`[Patch] Hash mismatch for ${decodedKey}: expected=${expectedHash}, server=${serverHash}`);
                let currentEtag = undefined;
                if (decodedKey === 'database/database.bin') {
                    currentEtag = getDbCacheEtag(filePath);
                    dbEtag = currentEtag;
                }
                res.status(409).send({
                    error: 'Hash mismatch - data out of sync',
                    code: 'DATABASE_PATCH_CONFLICT',
                    currentEtag
                });
                return;
            }

            // Only patch-path ancestors are copied. Until the complete sequence
            // succeeds, every object reachable from dbCache remains untouched.
            const result = applyPatchAtomic(cachedDb, patch);
            const snapshot = result.newDocument;
            let preserveSegmentMemo = false;
            let structuralDatabasePatch = false;
            if (decodedKey === 'database/database.bin') {
                const duplicateChatIds = findDuplicateChatIds(snapshot);
                if (duplicateChatIds.length > 0) {
                    logger.warn(
                        `[Patch] Rejected ${duplicateChatIds.length} duplicate chat id(s): `
                        + duplicateChatIdSample(duplicateChatIds)
                    );
                    let currentEtag;
                    try {
                        currentEtag = getDbCacheEtag(filePath);
                        dbEtag = currentEtag;
                    } catch {}
                    res.status(409).send({
                        error: 'Patch rejected: duplicate chat ids would alias authoritative rows',
                        code: 'DUPLICATE_CHAT_IDS',
                        currentEtag,
                    });
                    return;
                }
                // Detection must remain pure and precede in-place normalizations:
                // untouched patch subtrees can still be shared with cachedDb.
                const payloadChatCount = chatRowStore.countPayloadChats(snapshot);
                if (payloadChatCount > 0) {
                    logger.warn(
                        `[Patch] Rejected ${payloadChatCount} whole-chat payload(s)`
                    );
                    let currentEtag;
                    try {
                        currentEtag = getDbCacheEtag(filePath);
                        dbEtag = currentEtag;
                    } catch {}
                    res.status(422).send({
                        error: 'Patch rejected: whole-chat payloads must be written through /api/chat-content',
                        code: 'CHAT_PAYLOAD_PATCH_UNSUPPORTED',
                        retryable: false,
                        commitOutcome: 'not-committed',
                        commitOutcomeUnknown: false,
                        currentEtag,
                    });
                    return;
                }
                // Keep dbCache and the ETag on the same optimized stub shape
                // that the debounced persist will write.
                const chatRowReferenceDiff = diffReferencedChatRowKeys(cachedDb, snapshot);
                structuralDatabasePatch = chatRowReferenceDiff.changed;
                const externalized = externalizePluginStorageIfNeeded(snapshot);
                preserveSegmentMemo = true;
                trackPendingChatRowDeletions(chatRowReferenceDiff);
                // A patch with no mutating op (empty, or test-only) returns the
                // cached object itself, so replaceDbCacheValue sees no identity
                // change and skips the generation bump. Plugin externalization
                // edits in place, so bump explicitly when it actually changed
                // something — otherwise the memoized hash/ETag would keep
                // describing the pre-normalization shape.
                if (snapshot === cachedDb && externalized.changed) {
                    dbCompositionalHashMemo = new WeakMap();
                    dbDerivedValueMemo.bump(filePath);
                }
            }
            replaceDbCacheValue(filePath, snapshot, {
                dirty: true,
                preserveHashMemo: snapshot !== cachedDb,
                preserveSegmentMemo,
            });

            // Update ETag after successful patch (based on stripped version).
            if (decodedKey === 'database/database.bin') {
                dbEtag = getDbCacheEtag(filePath, { retainCanonicalEncoding: true });
            }

            let durable = false;
            if (structuralDatabasePatch) {
                if (saveTimers[filePath]) clearTimeout(saveTimers[filePath]);
                delete saveTimers[filePath];
                try {
                    await persistDbCache(filePath, decodedKey);
                    dbPersistRetryPending = false;
                    clearPersistFailure();
                    scheduleBackupAndRotate();
                    durable = true;
                } catch (error) {
                    dbPersistRetryPending = Boolean(peekDbCacheValue(filePath));
                    logger.error(`[Patch] Error saving ${decodedKey}:`, error);
                    recordPersistFailure(error, `patch:${decodedKey}`);
                }
            } else {
                // Schedule stubs-only save to KV (debounced).
                if (saveTimers[filePath]) {
                    clearTimeout(saveTimers[filePath]);
                }
                const saveTimer = setTimeout(() => {
                    queueStorageMutation(async () => {
                        if (saveTimers[filePath] !== saveTimer) return;
                        try {
                            if (decodedKey === 'database/database.bin') {
                                await persistDbCache(filePath, decodedKey);
                                dbPersistRetryPending = false;
                            } else {
                                const data = Buffer.from(encodeRisuSaveLegacy(peekDbCacheValue(filePath)));
                                try {
                                    kvSet(decodedKey, data);
                                } catch (err) {
                                    if (err && typeof err === 'object') {
                                        try { err.attemptedSize = data.length; } catch {}
                                    }
                                    throw err;
                                }
                                markDbCacheClean(filePath, { estimatedBytes: data.length });
                            }
                            // Persist succeeded — clear before backup so a backup-only
                            // failure isn't attributed to data loss.
                            clearPersistFailure();
                            if (decodedKey === 'database/database.bin') scheduleBackupAndRotate();
                        } catch (error) {
                            if (decodedKey === 'database/database.bin') {
                                // persistDbCache may intentionally invalidate a
                                // malformed cache. Only retained cache state can be
                                // retried; otherwise the live database supersedes it.
                                dbPersistRetryPending = Boolean(peekDbCacheValue(filePath));
                            }
                            logger.error(`[Patch] Error saving ${decodedKey}:`, error);
                            recordPersistFailure(error, `patch:${decodedKey}`);
                        } finally {
                            if (saveTimers[filePath] === saveTimer) delete saveTimers[filePath];
                        }
                    }, 'patch-persist').catch((error) => {
                        if (saveTimers[filePath] === saveTimer) delete saveTimers[filePath];
                        if (isImportInProgressError(error)) {
                            // The import replaces this key wholesale and drops dbCache,
                            // so the superseded debounced save is not a persist failure.
                            logger.info(`[Patch] Skipped debounced save for ${decodedKey}: import in progress`);
                            return;
                        }
                        logger.error(`[Patch] Storage queue failed for ${decodedKey}:`, error);
                    });
                }, SAVE_INTERVAL);
                saveTimers[filePath] = saveTimer;
            }

            const responsePayload = {
                success: true,
                appliedOperations: result.length,
                etag: decodedKey === 'database/database.bin' ? dbEtag : undefined,
                durable,
            };
            const persistWarning = currentPersistWarning();
            if (persistWarning) {
                responsePayload.persistWarning = persistWarning;
            }
            res.send(responsePayload);
        });
    } catch (error) {
        if (isImportInProgressError(error)) return sendImportBusy(res);
        if (error instanceof DatabaseCacheRevisionConflict) {
            return res.status(409).json({
                error: error.message,
                code: error.code,
                retryable: false,
            });
        }
        const diagnostic = logPluginStorageValidationFailure(
            '[PluginStorage] Rejected invalid patched database row',
            error
        );
        if (diagnostic) {
            res.status(400).json(diagnostic);
            return;
        }
        logger.error(`[Patch] Error applying patch to ${filePath}:`, error.name);
        res.status(500).send({
            error: 'Patch application failed: ' + (error && error.message ? error.message : error)
        });
    }
});

// ─── Bulk asset endpoints (3-2-B) ─────────────────────────────────────────────
const BULK_BATCH = 50;

app.post('/api/assets/bulk-read', async (req, res, next) => {
    if(!await checkAuth(req, res)){ return; }
    try {
        const keys = req.body; // string[] — decoded key strings
        if(!Array.isArray(keys)){
            res.status(400).send({ error: 'Body must be a JSON array of keys' });
            return;
        }

        const acceptsBinary = (req.headers['accept'] || '').includes('application/octet-stream');

        if (acceptsBinary) {
            // Binary protocol: [count(4)] then per entry: [keyLen(4)][key][valLen(4)][value]
            // Eliminates ~33% base64 overhead
            const entries = [];
            let totalSize = 4; // count header
            for (let i = 0; i < keys.length; i += BULK_BATCH) {
                const batch = keys.slice(i, i + BULK_BATCH);
                for (const key of batch) {
                    let value = null;
                    if (typeof key === 'string' && key.startsWith('inlay_info/')) {
                        value = await readInlayInfoPayload(key.slice('inlay_info/'.length));
                    }
                    if (value === null) {
                        value = typeof key === 'string' && key.startsWith('assets/')
                            ? readAssetValue(key)
                            : kvGet(key);
                    }
                    if (value !== null) {
                        const keyBuf = Buffer.from(key, 'utf-8');
                        const valBuf = Buffer.from(value);
                        entries.push({ keyBuf, valBuf });
                        totalSize += 4 + keyBuf.length + 4 + valBuf.length;
                    }
                }
            }
            const out = Buffer.allocUnsafe(totalSize);
            let offset = 0;
            out.writeUInt32BE(entries.length, offset); offset += 4;
            for (const { keyBuf, valBuf } of entries) {
                out.writeUInt32BE(keyBuf.length, offset); offset += 4;
                keyBuf.copy(out, offset); offset += keyBuf.length;
                out.writeUInt32BE(valBuf.length, offset); offset += 4;
                valBuf.copy(out, offset); offset += valBuf.length;
            }
            res.set('Content-Type', 'application/octet-stream');
            res.send(out);
        } else {
            // Legacy JSON+base64 fallback
            const results = [];
            for (let i = 0; i < keys.length; i += BULK_BATCH) {
                const batch = keys.slice(i, i + BULK_BATCH);
                for (const key of batch) {
                    let value = null;
                    if (typeof key === 'string' && key.startsWith('inlay_info/')) {
                        value = await readInlayInfoPayload(key.slice('inlay_info/'.length));
                    }
                    if (value === null) {
                        value = typeof key === 'string' && key.startsWith('assets/')
                            ? readAssetValue(key)
                            : kvGet(key);
                    }
                    if (value !== null) {
                        results.push({ key, value: Buffer.from(value).toString('base64') });
                    }
                }
            }
            res.json(results);
        }
    } catch(error){ next(error); }
});

function bulkWriteNotCommitted(body, { retryable = false } = {}) {
    return {
        ...body,
        retryable,
        commitOutcome: 'not-committed',
        commitOutcomeUnknown: false,
    };
}

function bulkWriteEntryMatchesAuthoritativeState(entry) {
    const { key, buffer, verification } = entry;
    const authoritative = key.startsWith('assets/')
        ? readAssetValue(key)
        : kvGet(key);
    if (authoritative === null || !Buffer.from(authoritative).equals(buffer)) return false;

    // A hash-named asset's legacy exemption is write-admission metadata, not
    // auxiliary cleanup. Canonical bytes must clear it; a historical mismatch
    // must retain it. Do not call a byte-only readback a committed entry when
    // this invariant is inconsistent.
    if (verification && verification.claimed !== null) {
        const name = assetNameForKey(key);
        const markedLegacy = name !== null && isLegacyHashAsset(name);
        if (verification.ok && markedLegacy) return false;
        if (verification.legacyHashMismatch && !markedLegacy) return false;
    }
    return true;
}

app.post('/api/assets/bulk-write', async (req, res, next) => {
    if(!await checkAuth(req, res)){ return; }
    if (!checkActiveSession(req, res)) return;
    try {
        const entries = req.body; // {key: string, value: base64}[]
        if(!Array.isArray(entries)){
            res.status(400).send(bulkWriteNotCommitted({
                error: 'Body must be a JSON array of {key, value}',
                code: 'INVALID_BULK_WRITE',
            }));
            return;
        }
        if (entries.some(entry => !entry
            || typeof entry !== 'object'
            || Array.isArray(entry)
            || typeof entry.key !== 'string'
            || typeof entry.value !== 'string')) {
            res.status(400).json(bulkWriteNotCommitted({
                error: 'Every bulk write entry must contain string key and value fields',
                code: 'INVALID_BULK_WRITE_ENTRY',
            }));
            return;
        }
        const seenKeys = new Set();
        const duplicateKeys = [];
        for (const { key } of entries) {
            if (seenKeys.has(key) && !duplicateKeys.includes(key)) duplicateKeys.push(key);
            seenKeys.add(key);
        }
        if (duplicateKeys.length > 0) {
            res.status(400).json(bulkWriteNotCommitted({
                error: 'A bulk write cannot contain duplicate keys',
                code: 'DUPLICATE_BULK_WRITE_KEY',
                keys: duplicateKeys,
            }));
            return;
        }

        const decodedEntries = entries.map(({ key, value }) => {
            const buffer = Buffer.from(value, 'base64');
            const verification = typeof key === 'string' && key.startsWith('assets/')
                ? verifyAssetHashForWrite(key, buffer)
                : null;
            return { key, buffer, verification };
        });
        for (const { key, buffer } of decodedEntries) {
            if (!key.startsWith(PLUGIN_SAVE_PREFIX)
                && !key.startsWith(PLUGIN_SAVE_META_PREFIX)) continue;
            try {
                assertArchiveSafePluginSaveStorageKey(key);
            } catch (error) {
                res.status(400).json(bulkWriteNotCommitted({
                    error: error?.message || 'Invalid plugin storage key',
                    code: 'invalid_plugin_storage_key',
                }));
                return;
            }
            try {
                validatePluginStorageRow(key, buffer);
            } catch (error) {
                const diagnostic = logPluginStorageValidationFailure(
                    '[PluginStorage] Rejected invalid bulk row write',
                    error,
                ) ?? {
                    error: 'Invalid plugin storage JSON row',
                    code: 'INVALID_PLUGIN_STORAGE_ROW',
                    encodedKey: key.startsWith(PLUGIN_SAVE_META_PREFIX)
                        ? PLUGIN_SAVE_META_PREFIX
                        : PLUGIN_SAVE_PREFIX,
                };
                res.status(400).json(bulkWriteNotCommitted(diagnostic));
                return;
            }
        }
        const mismatches = decodedEntries
            .filter((entry) => entry.verification
                && !entry.verification.ok
                && !entry.verification.legacyHashMismatch)
            .map((entry) => ({
                key: entry.key,
                expected: entry.verification.claimed,
                actual: entry.verification.actual,
            }));
        if (mismatches.length > 0) {
            res.status(400).json(bulkWriteNotCommitted({
                error: 'asset content does not match its SHA-256 name',
                code: 'ASSET_HASH_MISMATCH',
                keys: mismatches.map((entry) => entry.key),
                mismatches,
            }));
            return;
        }

        const results = [];

        // Hold one queue slot for the whole request so an import cannot split
        // its ordered outcomes. Entries deliberately commit independently:
        // each write is idempotent and reports its own durable outcome.
        await queueStorageMutation(async () => {
            await waitAtBulkWriteValidationTestGate();
            const queuedMismatches = [];
            for (const entry of decodedEntries) {
                if (!entry.key.startsWith('assets/')) continue;
                entry.verification = verifyAssetHashForWrite(entry.key, entry.buffer);
                if (!entry.verification.ok && !entry.verification.legacyHashMismatch) {
                    queuedMismatches.push({
                        key: entry.key,
                        expected: entry.verification.claimed,
                        actual: entry.verification.actual,
                    });
                }
            }
            if (queuedMismatches.length > 0) {
                return res.status(409).json(bulkWriteNotCommitted({
                    error: 'Asset hash identity changed while the bulk write was queued',
                    code: 'ASSET_HASH_STATE_CONFLICT',
                    keys: queuedMismatches.map(entry => entry.key),
                    mismatches: queuedMismatches,
                }));
            }
            const protectedEntries = decodedEntries.filter(({ key }) => (
                key === 'database/database.bin'
                || key === PLUGIN_STORAGE_MANIFEST_KEY
                || canonicalPluginStorageRowPrefix(key)
            ));
            if (protectedEntries.length > 0) {
                const publication = await readLivePluginStoragePublication();
                try {
                    for (const { key } of protectedEntries) {
                        if (key === 'database/database.bin') {
                            throw pluginStorageNamespaceConflict(
                                'database.bin cannot be changed through the bulk asset endpoint',
                            );
                        }
                        assertGenericPluginStorageMutationAllowed(key, publication);
                    }
                } catch (error) {
                    if (error?.pluginStorageNamespaceConflict) {
                        return res.status(409).json(bulkWriteNotCommitted({
                            error: error.message,
                            code: 'PLUGIN_STORAGE_NAMESPACE_CONFLICT',
                        }));
                    }
                    throw error;
                }
            }
            for (let index = 0; index < decodedEntries.length; index++) {
                const { key, buffer, verification } = decodedEntries[index];
                try {
                    let changed = true;
                    const writeEntry = sqliteDb.transaction(() => {
                        if (typeof key === 'string' && key.startsWith('assets/')) {
                            changed = writeAssetValue(key, buffer, {
                                skipIfUnchanged: verification.claimed !== null,
                                legacyHashMismatch: verification.legacyHashMismatch,
                                publishHooks: {
                                    beforePublish: () => hitBulkWriteFailpoint(
                                        'before-asset-publish',
                                        index,
                                    ),
                                    afterPublish: () => hitBulkWriteFailpoint(
                                        'after-asset-publish',
                                        index,
                                    ),
                                },
                                metadataHooks: {
                                    beforeLegacyHashClear: () => hitBulkWriteFailpoint(
                                        'before-legacy-hash-clear',
                                        index,
                                    ),
                                    afterLegacyHashClear: () => hitBulkWriteFailpoint(
                                        'after-legacy-hash-clear',
                                        index,
                                    ),
                                },
                            });
                        } else {
                            kvSet(key, buffer);
                        }
                        hitBulkWriteFailpoint('before-sqlite-commit', index);
                    });
                    writeEntry();
                    hitBulkWriteFailpoint('after-sqlite-commit', index);
                    results.push({
                        index,
                        key,
                        status: 'committed',
                        changed,
                        retryable: false,
                    });
                } catch (error) {
                    let status = 'unknown';
                    try {
                        hitBulkWriteFailpoint('reconciliation-read', index);
                        status = bulkWriteEntryMatchesAuthoritativeState(decodedEntries[index])
                            ? 'committed'
                            : 'not-committed';
                    } catch (readError) {
                        logger.error(
                            `[BulkWrite] Could not reconcile entry ${index} (${String(key)}):`,
                            readError,
                        );
                    }
                    logger.warn(
                        `[BulkWrite] Entry ${index} (${String(key)}) failed at a commit boundary; `
                        + `reconciled as ${status}:`,
                        error?.message || error,
                    );
                    results.push({
                        index,
                        key,
                        status,
                        reconciled: status !== 'unknown',
                        retryable: status === 'not-committed',
                        code: status === 'unknown'
                            ? 'BULK_ENTRY_OUTCOME_UNKNOWN'
                            : status === 'not-committed'
                                ? 'BULK_ENTRY_WRITE_FAILED'
                                : undefined,
                    });
                }
            }
        });
        if (res.headersSent) return;
        res.json({ results });
    } catch(error){
        if (isImportInProgressError(error)) return sendImportBusy(res);
        next(error);
    }
});

const backupRoutesCtx = {
    BACKUP_DISK_HEADROOM,
    BACKUP_ENTRY_STAGE_PREFIX,
    BACKUP_FILENAME_REGEX,
    BACKUP_IMPORT_MAX_BYTES,
    BACKUP_IMPORT_MAX_ENTRIES,
    BACKUP_IMPORT_SPOOL_FILE_PREFIX,
    BACKUP_PATH_CONFIG_KEY,
    BACKUP_PATH_MARKER,
    CHAT_BACKUP_VERSION_ID_REGEX,
    CHAT_EXTERNALIZATION_MARKER_KEY,
    COLD_STORAGE_FLAT_NAME_RE,
    DATABASE_SPOOL_FILE_PREFIX,
    DB_BLOB_KEY,
    DEFAULT_BACKUPS_DIR,
    FULL_EXPORT_PIN_PREFIX,
    HUB_HOSTING_MODE,
    IMPORT_BUFFERED_ENTRY_MAX_BYTES,
    IMPORT_JOURNAL_MARKER_KEY,
    IMPORT_JOURNAL_PATH,
    INLAY_ARCHIVE_V2_PREFIX,
    INLAY_CANONICAL_PAYLOAD_DIR_NAME,
    INLAY_CANONICAL_ROOT_NAME,
    LARGE_RESTORE_MAX_BYTES,
    LARGE_RESTORE_MAX_ENTRIES,
    LEGACY_DATABASE_IMPORT_MAX_BYTES,
    PARTIAL_EXPORT_JOB_PREFIX,
    PLUGIN_STORAGE_UUID_PATTERN,
    REMOTE_MIGRATION_MARKER_KEY,
    SAVE_FOLDER_IMPORT_MAX_ENTRIES,
    SERVER_BACKUP_TEMP_PREFIX,
    applySqliteDurabilityMode,
    assertCanonicalInlayNamespace,
    assertCanonicalInlayWriteTargets,
    assertSafeInlayTuple,
    assetImportBackupDir,
    assetImportStagingDir,
    assetNameForKey,
    canonicalInlayPaths,
    chatRowKey,
    chatRowStore,
    checkActiveSession,
    checkAuth,
    checkDiskSpace,
    createBackupAndRotate,
    decodeDataUri,
    flushPendingDb,
    getBackupsDir,
    getDatabaseSpoolDir,
    getImportInProgress,
    hexRegex,
    importBarrier,
    importColdStorageFromFile,
    importOpaqueRowFromFile,
    ingestDatabase,
    ingestDatabaseStreaming,
    inlayDir,
    inlayMigrationMarker,
    invalidateAllDbCaches,
    isImportInProgressError,
    isInvalidBackupPathSegment,
    isManagedBackupPath,
    isSafeInlayId,
    listAssetEntriesWithSizes,
    listChatBackupChats,
    listChatBackups,
    listDraftBackupEntries,
    listInlayFiles,
    listMcpToolCallBackupEntries,
    listRegularFilesRecursiveSync,
    loadStrippedDatabase,
    logPluginStorageValidationFailure,
    markRemoteMigrationDone,
    normalizeColdStorageStorageKey,
    normalizeInlayExt,
    parseCanonicalInlayPayloadPath,
    parseInlayBackupName,
    parseInlaySidecarBackupName,
    parseInternalSnapshotKey,
    partialExportSpoolDir,
    pluginStorageGeneration,
    pluginStorageValidationDiagnostic,
    prepareAssetImportStage,
    publishUpdaterPathMarkerSet,
    queueStorageMutation,
    queueStorageOperation,
    queueStorageReadAfterImports,
    readBackupRisuSaveTopLevelFields,
    readChatBackup,
    readInlaySidecar,
    readPluginStorageManifestState,
    recoverPendingImportSwap,
    referencedDraftStorageKeys,
    rememberSessionPluginStorageState,
    requireDatabaseSpoolDirSync,
    resolveInlaySidecarPath,
    resolveOwnedPluginStorageKeys,
    resolveOwnedPluginStorageRows,
    runTrackedWalCheckpoint,
    samePinnedSourceStat,
    savePath,
    sendImportBusy,
    setBackupsDirResolved,
    setDbEtag,
    setImportInProgress,
    spoolBackupSnapshotRow,
    spoolLogicalChatSnapshotRow,
    streamFileToWritable,
    sweepServerBackupTemps,
    throwIfSignalAborted,
    toColdStorageBackupName,
    toInlayBackupName,
    validateAndImportPluginMetadataFile,
    validateAndImportPluginValueFile,
    warnAndPreserveMissingChatRow,
    writeImportedAssetFromFile,
    writePinnedBackupEntry,
    writeWithBackpressure,
};

registerBackupExportRoutes(app, backupRoutesCtx);

registerBackupImportRoutes(app, backupRoutesCtx);
registerServerBackupRoutes(app, backupRoutesCtx);
// ── Chat content endpoints (runtime lazy load) ─────────────────────────────

// Cold storage compatibility: restore data stored in coldstorage/ KV entries
const COLD_STORAGE_HEADER = '\uEF01COLDSTORAGE\uEF01';

function restoreColdStorageCharacter(character) {
    if (!character?.coldstorage) return true;
    const key = character.coldstorage;
    const entry = readColdStorageJsonEntry(key, {
        migrateLegacy: true,
    });
    if (!entry) {
        logger.error(`[ColdStorage] character data not found for key: ${key}`);
        return false;
    }
    try {
        const coldData = entry.coldData;
        if (coldData?.character) {
            Object.assign(character, coldData.character);
            delete character.coldstorage;
            delete character.coldStoragedChats;
        } else {
            logger.error(`[ColdStorage] unexpected character cold data format for key: ${key}`);
            return false;
        }
        return true;
    } catch (err) {
        logger.error(`[ColdStorage] character restore failed for key ${key}:`, err.message);
        return false;
    }
}

function promoteFailedColdStorageStub(char) {
    const coldKey = char.coldstorage;
    // Fill in missing fields with safe defaults matching createBlankChar() in src/ts/characters.ts.
    // SYNC: if createBlankChar() defaults change, update this object to match.
    const defaults = {
        firstMessage: '', desc: '', notes: '', chatFolders: [],
        emotionImages: [], bias: [], viewScreen: 'none', globalLore: [],
        sdData: [
            ['always', 'solo, 1girl'], ['negative', ''],
            ["|character's appearance", ''], ['current situation', ''],
            ["$character's pose", ''], ["$character's emotion", ''],
            ['current location', ''],
        ],
        utilityBot: false, customscript: [], exampleMessage: '',
        creatorNotes: '', systemPrompt: '', postHistoryInstructions: '',
        alternateGreetings: [], tags: [], creator: '', characterVersion: '',
        personality: '', scenario: '',
        firstMsgIndex: -1,
        replaceGlobalNote: '', additionalText: '',
        triggerscript: [
            { comment: '', type: 'manual', conditions: [], effect: [{ type: 'v2Header', code: '', indent: 0 }] },
            { comment: 'New Event', type: 'manual', conditions: [], effect: [] },
        ],
    };
    for (const [key, value] of Object.entries(defaults)) {
        if (char[key] === undefined || char[key] === null) {
            char[key] = value;
        }
    }
    // Force firstMsgIndex to -1 even if stub had 0 — prevents alternateGreetings[0] access on empty array
    char.firstMsgIndex = -1;
    // Ensure chats array is valid
    if (!Array.isArray(char.chats) || char.chats.length === 0) {
        char.chats = [{ message: [], note: '', name: 'Chat 1', localLore: [] }];
    }
    // Leave recovery breadcrumb and remove cold storage markers
    char.desc = `[Cold storage restore failed. Original key: ${coldKey}]\n\n${char.desc || ''}`.trim();
    delete char.coldstorage;
    delete char.coldStoragedChats;
}

function restoreColdStorageCharactersInDb(dbObj) {
    const result = { restored: 0, failed: 0, failedNames: [] };
    if (!Array.isArray(dbObj?.characters)) return result;
    for (let i = 0; i < dbObj.characters.length; i++) {
        const char = dbObj.characters[i];
        if (!char?.coldstorage) continue;
        if (restoreColdStorageCharacter(char)) {
            result.restored++;
        } else {
            result.failed++;
            result.failedNames.push(char.name || `(index ${i})`);
            promoteFailedColdStorageStub(char);
        }
    }
    return result;
}

function isColdStorageChat(chat) {
    return chat?.message?.[0]?.data?.startsWith(COLD_STORAGE_HEADER);
}

function restoreColdStorageChat(chat) {
    if (!isColdStorageChat(chat)) return true;
    const key = chat.message[0].data.slice(COLD_STORAGE_HEADER.length);
    const entry = readColdStorageJsonEntry(key, {
        migrateLegacy: true,
    });
    if (!entry) {
        logger.error(`[ColdStorage] data not found for key: ${key}`);
        return false;
    }
    try {
        const coldData = entry.coldData;
        if (Array.isArray(coldData)) {
            chat.message = coldData;
        } else if (coldData?.message) {
            chat.message = coldData.message;
            if (coldData.hypaV3Data) chat.hypaV3Data = coldData.hypaV3Data;
            if (coldData.scriptstate) chat.scriptstate = coldData.scriptstate;
            if (coldData.localLore) chat.localLore = coldData.localLore;
        }
        chat.lastDate = Date.now();
        return true;
    } catch (err) {
        logger.error(`[ColdStorage] restore failed for key ${key}:`, err.message);
        return false;
    }
}

// GET /api/chat-content/:chaId/:chatIndex — retrieve full chat from server
app.get('/api/chat-content/:chaId/:chatIndex', async (req, res, next) => {
    if (!await checkAuth(req, res)) { return; }
    try {
        await importBarrier.waitUntilIdle();
        const result = await (async () => {
            const chaId = req.params.chaId;
            const chatIndex = parseInt(req.params.chatIndex, 10);
            const expectedChatId = req.headers['x-chat-id'];
            let chatId = expectedChatId;
            let row = chatId
                ? await chatRowStore.readChatRowRawWithMetadataAsync(chaId, chatId)
                : null;

            // Header-less legacy callers resolve index→id through the stripped DB.
            // A failed id lookup also keeps the historical shifted-index 409 check.
            let fallbackCacheStatus = null;
            if (!row) {
                let strippedDb = getCurrentDatabaseCacheValue(
                    DB_HEX_KEY,
                    { allowDirty: true },
                );
                if (strippedDb) {
                    fallbackCacheStatus = 'hit';
                } else {
                    const prepared = await queueStorageReadAfterImports(async () => {
                        await flushPendingDb();
                        return prepareLiveDatabaseRead('ChatContentFallback', {
                            includeFullBlob: false,
                        });
                    });
                    if (!prepared) return { status: 404, error: 'Database not found' };
                    strippedDb = prepared.strippedDatabase;
                    fallbackCacheStatus = prepared.cacheStatus;
                }
                const char = strippedDb.characters?.find(c => c?.chaId === chaId);
                const stub = char?.chats?.[chatIndex];
                if (!stub) return { status: 404, error: 'Chat not found' };
                if (expectedChatId && stub.id !== expectedChatId) {
                    return { status: 409, error: 'Chat ID mismatch — index may have shifted' };
                }
                chatId = stub.id;
                if (!chatId) return { status: 404, error: 'Chat not found' };
                row = await chatRowStore.readChatRowRawWithMetadataAsync(chaId, chatId);
            }
            if (!row) return { status: 404, error: 'Chat not found' };

            let encoded = row.bytes;
            let contentHash = row.contentHash;
            // A matching warm derivative is the fast path: raw bytes already
            // selected for the response require no decode or second store read.
            // Missing/mismatched metadata falls back to the row body once and
            // repairs the derivative only if the row's mutation token is still
            // current after the asynchronous decode.
            if (row.coldStorage !== false) {
                const chat = await decodeRisuSave(row.bytes);
                const needsRehydration = isColdStorageChat(chat);
                if (!importBarrier.isHeld()) {
                    chatRowStore.repairChatRowMetadata(
                        row,
                        needsRehydration,
                        Array.isArray(chat?.message) ? chat.message.length : 0,
                    );
                }
                if (needsRehydration) {
                    if (!restoreColdStorageChat(chat)) {
                        return { status: 500, error: 'Cold storage restore failed' };
                    }
                    encoded = Buffer.from(encodeRisuSaveLegacy(chat));
                    // Cache-fill only, so it can be skipped rather than gated: an
                    // import that claimed the barrier will replace this dataset.
                    const storedHash = importBarrier.isHeld()
                        ? null
                        : chatRowStore.writeChatRowIfUnchanged(chaId, chatId, row, chat);
                    contentHash = storedHash ?? sha256Hex(encoded);
                }
            }
            return { status: 200, encoded, contentHash, fallbackCacheStatus };
        })();
        if (result.error) return res.status(result.status).json({ error: result.error });
        const { encoded, contentHash, fallbackCacheStatus } = result;
        if (DB_CACHE_TEST_DIAGNOSTICS && fallbackCacheStatus) {
            res.setHeader('x-pocketrisu-test-db-cache', fallbackCacheStatus);
        }
        res.setHeader('x-content-hash', contentHash);
        const cachedHashes = parseCachedHashesHeader(req.headers['x-cached-hashes']);
        if (cachedHashes.includes(contentHash)) {
            return res.status(204).end();
        }
        res.setHeader('Content-Type', 'application/octet-stream');
        res.send(encoded);
    } catch (error) {
        next(error);
    }
});

// POST /api/chat-content/:chaId/:chatIndex — save chat content to server
const pendingChatDeltaCompactions = new Set();

function sendChatDeltaRefusal(res, result, status = 409) {
    const code = result?.code ?? 'CHAT_DELTA_CONFLICT';
    const messages = {
        CHAT_DELTA_BASE_MISSING: 'The chat row has no base for this delta.',
        CHAT_DELTA_BASE_UNAVAILABLE: 'The chat row does not support exact delta replay.',
        CHAT_DELTA_BASE_MISMATCH: 'The chat row changed since the acknowledged base.',
        CHAT_DELTA_LOG_CONFLICT: 'The chat operation log is not appendable.',
    };
    return res.status(status).json({
        success: false,
        error: messages[code] ?? result?.message ?? 'The chat delta was refused.',
        code,
        retryable: false,
        commitOutcome: 'not-committed',
        commitOutcomeUnknown: false,
        ...(result?.currentHash ? { currentHash: result.currentHash } : {}),
    });
}

const CHAT_ROW_BASE_HASH_HEADER = 'x-chat-base-hash';

function checkChatRowBasePrecondition(req, res, chaId, chatId) {
    const expectedBaseHash = req.headers[CHAT_ROW_BASE_HASH_HEADER];
    if (expectedBaseHash === undefined) return true;
    if (typeof expectedBaseHash !== 'string'
        || !/^[0-9a-f]{64}$/.test(expectedBaseHash)) {
        res.status(400).json({
            success: false,
            error: 'The full chat row base hash is invalid.',
            code: 'CHAT_ROW_BASE_INVALID',
            retryable: false,
            commitOutcome: 'not-committed',
            commitOutcomeUnknown: false,
        });
        return false;
    }

    const currentRow = chatRowStore.readChatRowRawWithMetadata(chaId, chatId);
    if (currentRow?.contentHash === expectedBaseHash) return true;
    res.status(409).json({
        success: false,
        error: 'The chat row changed since the acknowledged base.',
        code: 'CHAT_ROW_BASE_MISMATCH',
        retryable: false,
        commitOutcome: 'not-committed',
        commitOutcomeUnknown: false,
        ...(currentRow?.contentHash ? { currentHash: currentRow.contentHash } : {}),
    });
    return false;
}

function scheduleChatDeltaCompaction(chaId, chatId) {
    const key = chatRowKey(chaId, chatId);
    if (pendingChatDeltaCompactions.has(key)) return;
    pendingChatDeltaCompactions.add(key);
    setImmediate(() => {
        queueStorageMutation(
            () => chatRowStore.compactChatRow(chaId, chatId),
            'chat-log-compact',
        )
            .catch(error => {
                logger.error(`[ChatDelta] Compaction failed for ${key}:`, error);
            })
            .finally(() => pendingChatDeltaCompactions.delete(key));
    });
}

async function captureChatContentPreImage(req, res, chaId, chatId) {
    const reason = req.headers['x-chat-backup-reason'];
    if (!isDestructiveBackupReason(reason)) {
        await chatBackupStore.captureChatPreImage({ chaId, chatId, reason });
        return true;
    }

    try {
        await chatBackupStore.captureChatPreImage({
            chaId,
            chatId,
            reason,
            force: true,
            required: true,
        });
        return true;
    } catch (error) {
        res.status(500).json({
            success: false,
            commitOutcome: 'not-committed',
            commitOutcomeUnknown: false,
            code: 'CHAT_PREIMAGE_CAPTURE_FAILED',
            error: error?.message || String(error),
            retryable: true,
        });
        return false;
    }
}

async function handleChatDeltaWrite(req, res, next) {
    let shouldCreateBackup = false;
    let shouldCompact = false;
    try {
        await queueStorageMutation(async () => {
            const chaId = req.params.chaId;
            const expectedChatId = req.headers['x-chat-id'];
            if (!expectedChatId || typeof expectedChatId !== 'string') {
                return res.status(400).json({
                    error: 'Chat delta and x-chat-id required',
                    code: 'CHAT_DELTA_INVALID',
                    retryable: false,
                    commitOutcome: 'not-committed',
                    commitOutcomeUnknown: false,
                });
            }
            let inspection;
            try {
                inspection = chatRowStore.inspectChatDelta(
                    chaId,
                    expectedChatId,
                    req.body,
                    { maxResultBytes: bufferedIngressLimits.chat },
                );
            } catch (error) {
                if (error instanceof ChatDeltaValidationError) {
                    return sendChatDeltaRefusal(res, error, error.status ?? 400);
                }
                throw error;
            }
            if (!inspection.applied) return sendChatDeltaRefusal(res, inspection);

            // As on a full-row write, capture the exact prior logical bytes
            // after queue admission and immediately before the atomic append.
            if (!await captureChatContentPreImage(req, res, chaId, expectedChatId)) return;
            let result;
            try {
                result = chatRowStore.appendChatDelta(
                    chaId,
                    expectedChatId,
                    req.body,
                    { maxResultBytes: bufferedIngressLimits.chat },
                );
            } catch (error) {
                if (error instanceof ChatDeltaValidationError) {
                    return sendChatDeltaRefusal(res, error, error.status ?? 400);
                }
                throw error;
            }
            if (!result.applied) return sendChatDeltaRefusal(res, result);
            shouldCreateBackup = true;
            shouldCompact = result.shouldCompact;
            res.json({
                success: true,
                hash: result.hash,
                size: result.size,
                log: { count: result.logCount, bytes: result.logBytes },
            });
        }, 'chat-preimage+write');
        if (shouldCreateBackup) scheduleBackupAndRotate();
        if (shouldCompact) {
            scheduleChatDeltaCompaction(req.params.chaId, req.headers['x-chat-id']);
        }
    } catch (error) {
        if (isImportInProgressError(error)) return sendImportBusy(res);
        next(error);
    }
}

async function handleSpooledChatWrite(req, res, next, spool) {
    let prepared;
    let shouldCreateBackup = false;
    try {
        try {
            prepared = await prepareSpooledChatWrite(spool);
        } catch (error) {
            prepared = {
                stageDir: error?.admittedWriteStageDir ?? null,
                preparationError: error,
            };
        }
        // Express historically parsed JSON before the authoritative session
        // transition. Raw bodies were opaque and reached that transition
        // before binary decode. Preserve that distinction on the spool path.
        if (spool.bodyKind === 'json') {
            if (prepared.preparationError) throw prepared.preparationError;
            if (!checkActiveSession(req, res)) return;
        }
        if (!prepared.refusal && !prepared.preparationError) {
            await waitAtAdmittedWritePublishTestGate('chat');
        }
        await queueStorageMutation(async () => {
            if (prepared.preparationError) throw prepared.preparationError;
            if (prepared.refusal) {
                return res.status(prepared.refusal.status).json(prepared.refusal.body);
            }
            const chaId = req.params.chaId;
            const expectedChatId = req.headers['x-chat-id'];
            if (!prepared.chatData || !expectedChatId) {
                return res.status(400).json({ error: 'Chat data and x-chat-id required' });
            }
            if (!checkChatRowBasePrecondition(req, res, chaId, expectedChatId)) return;
            // Keep the exact old ordering: the prior authoritative row is
            // captured after queue admission and immediately before publish.
            if (!await captureChatContentPreImage(req, res, chaId, expectedChatId)) return;
            const hash = chatRowStore.writeChatRowFromFile(
                chaId,
                expectedChatId,
                prepared.filePath,
                {
                    coldStorage: prepared.coldStorage,
                    messageCount: prepared.messageCount,
                    logSupported: prepared.logSupported,
                    contentHash: prepared.contentHash,
                    chunkPlan: prepared.chunkPlan,
                },
            );
            shouldCreateBackup = true;
            res.json({ success: true, hash });
        }, 'chat-preimage+write');
        if (shouldCreateBackup) scheduleBackupAndRotate();
    } catch (error) {
        if (isImportInProgressError(error)) return sendImportBusy(res);
        const preparationRefusal = risuSavePreparationRefusal(error);
        if (error === prepared?.preparationError && preparationRefusal) {
            return res.status(preparationRefusal.status).json(preparationRefusal.body);
        }
        if (error === prepared?.preparationError
            && isAdmittedSpoolPressureError(error)) {
            return sendRetryableSpoolRefusal(
                res,
                req[BUFFERED_INGRESS_POLICY],
                bufferedIngressLimits.global,
                spool.size,
            );
        }
        next(error);
    } finally {
        if (prepared?.stageDir) {
            await fs.rm(prepared.stageDir, { recursive: true, force: true }).catch(() => {});
        }
        await disposeAdmittedIngressSpool(req);
    }
}

app.post('/api/chat-content/:chaId/:chatIndex', async (req, res, next) => {
    if (!await checkAuth(req, res)) { return; }
    const contentType = String(req.headers['content-type'] ?? '')
        .split(';', 1)[0]
        .trim()
        .toLowerCase();
    if (contentType === CHAT_DELTA_CONTENT_TYPE) {
        if (!checkActiveSession(req, res)) return;
        return handleChatDeltaWrite(req, res, next);
    }
    const admittedSpool = req[ADMITTED_INGRESS_SPOOL] ?? null;
    if (admittedSpool) {
        if (admittedSpool.bodyKind !== 'json' && !checkActiveSession(req, res)) return;
        return handleSpooledChatWrite(req, res, next, admittedSpool);
    }
    if (!checkActiveSession(req, res)) return;
    let shouldCreateBackup = false;
    try {
        await queueStorageMutation(async () => {
            const chaId = req.params.chaId;
            const expectedChatId = req.headers['x-chat-id'];
            let chatData;
            const isRawBinary = Buffer.isBuffer(req.body);
            if (isRawBinary) {
                // Binary msgpack body (application/octet-stream)
                try {
                    const inspection = await inspectRisuSaveSource(req.body);
                    chatData = inspection.format === 'raw'
                        ? await decodeRisuSave(req.body)
                        : await decodeBoundedLegacyRisuSave(req.body, {
                            inspection,
                            tempDir: requireDatabaseSpoolDirSync(),
                            maxLegacyBytes: LEGACY_DATABASE_IMPORT_MAX_BYTES,
                        });
                } catch (e) {
                    const refusal = risuSavePreparationRefusal(e);
                    if (refusal) return res.status(refusal.status).json(refusal.body);
                    return res.status(400).json({ error: 'Invalid binary chat data' });
                }
            } else {
                // JSON body (legacy)
                chatData = req.body;
            }

            if (!chatData || !expectedChatId) {
                return res.status(400).json({ error: 'Chat data and x-chat-id required' });
            }
            if (chatData._stub === true && !Array.isArray(chatData.message)) {
                return res.status(400).json({ error: 'Bare chat stubs cannot be stored as chat content' });
            }
            let healedHybrid = false;
            if (chatData._stub === true && Array.isArray(chatData.message)) {
                chatData = { ...chatData };
                delete chatData._stub;
                healedHybrid = true;
            }

            if (!checkChatRowBasePrecondition(req, res, chaId, expectedChatId)) return;

            // This must remain immediately before the row write: every version
            // is the exact state the incoming save was about to replace.
            if (!await captureChatContentPreImage(req, res, chaId, expectedChatId)) return;
            let hash;
            if (isRawBinary && !healedHybrid) {
                hash = chatRowStore.writeChatRowRawOwned(chaId, expectedChatId, req.body, {
                    coldStorage: isColdStorageChat(chatData),
                    messageCount: Array.isArray(chatData.message) ? chatData.message.length : 0,
                    logSupported: isCanonicalRawChatRow(req.body),
                });
            } else {
                hash = chatRowStore.writeChatRow(chaId, expectedChatId, chatData);
            }
            // The authoritative row is already durable. Keep full recovery
            // snapshot assembly outside the response-critical mutation so a
            // large store cannot turn this acknowledgement into a timeout.
            shouldCreateBackup = true;
            res.json({ success: true, hash });
        }, 'chat-preimage+write');
        if (shouldCreateBackup) scheduleBackupAndRotate();
    } catch (error) {
        if (isImportInProgressError(error)) return sendImportBusy(res);
        next(error);
    }
});

// ── Save-folder migration endpoints ──────────────────────────────────────────
registerSaveFolderMigrationRoutes(app, backupRoutesCtx);

// ── Storage dashboard endpoints ──────────────────────────────────────────────

const DB_BACKUP_PREFIX = 'database/dbbackup-';
const INTERNAL_SNAPSHOT_KEY_PATTERN = /^database\/dbbackup-(0|[1-9]\d*)\.bin$/;

registerAssetCleanupRoute(app, {
    checkAuth,
    checkActiveSession,
    runServerAssetCleanup,
    isImportInProgressError,
    sendImportBusy,
});

function parseInternalSnapshotKey(key) {
    if (typeof key !== 'string') return null;
    const match = INTERNAL_SNAPSHOT_KEY_PATTERN.exec(key);
    if (!match) return null;
    const snapshotTimestamp = Number(match[1]);
    const timestamp = snapshotTimestamp * 100;
    if (!Number.isSafeInteger(snapshotTimestamp) || snapshotTimestamp < 0
        || !Number.isSafeInteger(timestamp) || timestamp < 0) return null;
    return { key, timestamp };
}

if (STORAGE_QUEUE_DIAG_ENABLED) {
    app.get('/api/debug/queue-diag', async (req, res) => {
        if (!await checkAuth(req, res)) return;
        res.json(storageQueueDiagSnapshot());
    });
}

registerMaintenanceRoutes(app, {
    checkAuth,
    checkActiveSession,
    isHubHostingMode,
    getDbEtag,
    getBackupsDir,
    getChatBackupsDir,
    DB_BACKUP_PREFIX,
    BACKUP_FILENAME_REGEX,
    DB_BLOB_KEY,
    DB_HEX_KEY,
    DB_CACHE_TEST_DIAGNOSTICS,
    SNAPSHOT_LIMIT_COUNT_KEY,
    SNAPSHOT_LIMIT_BYTES_KEY,
    SNAPSHOT_LIMIT_DEFAULT_COUNT,
    SNAPSHOT_LIMIT_DEFAULT_BYTES,
    SNAPSHOT_LIMIT_MIN_COUNT,
    SNAPSHOT_LIMIT_MAX_COUNT,
    SNAPSHOT_LIMIT_MIN_BYTES,
    SNAPSHOT_LIMIT_MAX_BYTES,
    getSnapshotLimits,
    trimSnapshotsToLimits,
    parseInternalSnapshotKey,
    collectDatabaseAssetReferences,
    listAssetEntriesWithSizes,
    listRegularFilesRecursive,
    inlayDir,
    inlayMigrationMarker,
    isInlayTemporaryFileName,
    normalizeColdStorageStorageKey,
    readColdStorageJsonEntry,
    listColdStorageBackupEntries,
    listDraftBackupEntries,
    getCurrentDatabaseCacheValue,
    queueStorageReadAfterImports,
    flushPendingDb,
    prepareLiveDatabaseRead,
    loadStrippedDatabase,
    queueStorageMutation,
    chatRowStore,
    captureOrphanChatPreImage,
    runTrackedWalCheckpoint,
    sqliteDurabilityState,
    isSqliteDurabilityManaged,
    normalizeSqliteDurabilityMode,
    persistSqliteDurabilityMode,
    rescheduleSqliteDurabilityCheckpoint,
    isImportInProgressError,
    sendImportBusy,
});

// Restore a snapshot server-side. Supported large snapshots ingest directly
// into chat rows + the stripped live blob; legacy formats retain copy-then-
// ingest. Client-side reload is racy because the patch-sync save loop is
// debounced and the reload can fire before the snapshot data lands on disk.
registerSnapshotRestoreRoute(app, backupRoutesCtx);

registerBackupConfigurationRoutes(app, backupRoutesCtx);

// ── Inlay bulk compression endpoint ──────────────────────────────────────────
const COMPRESS_IMAGE_EXTS = new Set(['png', 'jpg', 'jpeg', 'gif', 'bmp']);

async function waitAtInlayCompressionBeforeCommitTestGate() {
    if (process.env.NODE_ENV !== 'test') return;
    const configured = String(
        process.env.POCKETRISU_TEST_INLAY_COMPRESS_BEFORE_COMMIT_GATE_DIR ?? '',
    ).trim();
    if (!configured) return;
    const gateDir = path.resolve(configured);
    const holdPath = path.join(gateDir, 'hold');
    if (!existsSync(holdPath)) return;
    await fs.mkdir(gateDir, { recursive: true });
    await fs.writeFile(path.join(gateDir, 'entered'), 'converted', 'utf-8');
    const releasePath = path.join(gateDir, 'release');
    while (existsSync(holdPath) && !existsSync(releasePath)) {
        await new Promise((resolve) => setTimeout(resolve, 10));
    }
}

app.post('/api/inlays/compress', sessionAuthMiddleware, async (req, res) => {
    if (!checkActiveSession(req, res)) return;
    // Rewrites inlay files an in-flight import is about to replace wholesale.
    if (importBarrier.isHeld()) {
        res.setHeader('Retry-After', '5');
        return res.status(503).json({
            error: 'An import is in progress; retry compression after it completes',
            code: 'IMPORT_IN_PROGRESS',
            retryable: true,
        });
    }
    const quality = typeof req.body?.quality === 'number' ? req.body.quality : 85;

    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
    });

    const send = (data) => {
        res.write(`data: ${JSON.stringify(data)}\n\n`);
    };

    try {
        const files = await listInlayFiles();
        const imageFiles = [];

        for (const entry of files) {
            if (!COMPRESS_IMAGE_EXTS.has(entry.ext)) continue;
            const sidecar = await readInlaySidecar(entry.id);
            if (sidecar && sidecar.type !== 'image') continue;
            imageFiles.push(entry);
        }

        const total = imageFiles.length;
        let compressed = 0;
        let skipped = 0;
        let totalSaved = 0;

        const vips = await getVips()

        for (let i = 0; i < imageFiles.length; i++) {
            const entry = imageFiles[i];
            try {
                const source = await fs.open(entry.filePath, 'r');
                let sourceStat;
                let original;
                try {
                    sourceStat = await source.stat();
                    original = await source.readFile();
                    const afterRead = await source.stat();
                    if (!samePinnedSourceStat(afterRead, sourceStat)) {
                        throw new Error('Inlay changed while preparing compression');
                    }
                } finally {
                    await source.close().catch(() => {});
                }
                const img = vips.Image.newFromBuffer(original)
                let webpBuf
                try {
                    const out = img.writeToBuffer('.webp', { Q: quality })
                    webpBuf = Buffer.from(out);
                } finally {
                    img.delete()
                }

                if (webpBuf.length < original.length) {
                    await waitAtInlayCompressionBeforeCommitTestGate();
                    const published = await queueStorageMutation(async () => {
                        const currentPath = await resolveInlayFilePath(entry.id);
                        if (currentPath !== entry.filePath) return false;
                        const currentStat = await fs.stat(currentPath).catch(() => null);
                        if (!currentStat || !samePinnedSourceStat(currentStat, sourceStat)) {
                            return false;
                        }
                        const sidecar = await readInlaySidecar(entry.id);
                        const info = sidecar || {};
                        try {
                            await writeInlayFile(
                                entry.id,
                                'webp',
                                webpBuf,
                                { ...info, ext: 'webp' },
                            );
                        } catch (cause) {
                            const publicationError = new Error(
                                `Failed to publish compressed inlay ${entry.id}: ${cause?.message || cause}`,
                                { cause },
                            );
                            publicationError.code = 'INLAY_PUBLICATION_FAILED';
                            publicationError.inlayId = entry.id;
                            throw publicationError;
                        }
                        kvDel(`inlay_thumb/${entry.id}`);
                        return true;
                    });
                    if (published) {
                        const saved = original.length - webpBuf.length;
                        totalSaved += saved;
                        compressed++;
                    } else {
                        skipped++;
                    }
                } else {
                    skipped++;
                }
            } catch (entryError) {
                // An import that claimed the barrier mid-run must stop the sweep,
                // not be counted as a per-image skip: the remaining files are
                // about to be replaced anyway.
                if (isImportInProgressError(entryError)) {
                    send({
                        type: 'error',
                        message: 'An import started; compression stopped. Retry after it completes.',
                    });
                    res.end();
                    return;
                }
                if (entryError?.code === 'INLAY_PUBLICATION_FAILED') {
                    send({
                        type: 'error',
                        code: entryError.code,
                        id: entryError.inlayId,
                        message: entryError.message,
                    });
                    res.end();
                    return;
                }
                skipped++;
            }

            send({ type: 'progress', current: i + 1, total, compressed, skipped, totalSaved });
        }

        send({ type: 'done', total, compressed, skipped, totalSaved });
    } catch (err) {
        send({ type: 'error', message: err?.message || 'Unknown error' });
    }

    res.end();
});

registerSelfUpdateRoutes(app, {
    checkAuth,
    get logger() { return logger; },
    getSavePath: () => savePath,
    getInstanceId: () => instanceId,
    getRecoveryPathPlatform: recoveryPathPlatform,
    queueStorageMutation,
    flushPendingDb,
    createBackupAndRotate,
    runTrackedWalCheckpointWithBusyRetry,
});

// ─── Express error middleware — must be registered after all routes ─────────
app.use(expressErrorMiddleware);
app.use((err, req, res, next) => {
    if (res.headersSent) return next(err);
    const diagnostic = logPluginStorageValidationFailure(
        '[PluginStorage] Rejected invalid ingested row',
        err
    );
    if (diagnostic) {
        res.status(400).json(diagnostic);
        return;
    }
    if (err instanceof PluginStorageLimitError) {
        return res.status(413).json({
            error: err.message,
            code: err.code,
            limit: err.limit,
            actual: err.actual,
            retryable: false,
            commitOutcome: 'not-committed',
            commitOutcomeUnknown: false,
        });
    }
    if (err?.code === 'ASSET_HASH_MISMATCH') {
        return res.status(400).json({
            error: err.message,
            code: err.code,
            key: err.key,
            expected: err.expected,
            actual: err.actual,
        });
    }
    if (err?.code === 'INVALID_INLAY_TUPLE') {
        return res.status(400).json({
            error: err.message,
            code: err.code,
        });
    }
    if (isAssetMaintenanceLockedError(err)) {
        res.setHeader('Retry-After', '5');
        return res.status(503).json({
            error: 'Asset maintenance is in progress; retry this write after it completes',
            code: err.code,
            retryAfter: 5,
            retryable: true,
            commitOutcome: 'not-committed',
            commitOutcomeUnknown: false,
        });
    }
    res.status(500).json({ error: err?.message || 'internal server error' });
});

// ─────────────────────────────────────────────────────────────────────────────

async function getHttpsOptions() {

    const keyPath = path.join(sslPath, 'server.key');
    const certPath = path.join(sslPath, 'server.crt');

    try {
 
        await fs.access(keyPath);
        await fs.access(certPath);

        const [key, cert] = await Promise.all([
            fs.readFile(keyPath),
            fs.readFile(certPath)
        ]);
       
        return { key, cert };

    } catch (error) {
        if (error.code === 'ENOENT') {
            logger.info('[Server] No SSL certificate found, starting with HTTP');
        } else {
            logger.error('[Server] SSL setup errors:', error.message);
            console.log('[Server] Start the server with HTTP instead of HTTPS...');
        }
        return null;
    }
}

async function startServer() {
    try {
        recoverPendingImportSwap('Startup');
        let bootDatabaseValidated = true;
        try {
            // Migration helpers are intentionally mutation-heavy: they publish
            // completion markers, safety backups, external rows, and rewritten
            // live bytes. Validate first so a corrupt monolith enters recovery
            // mode without changing even the list epoch.
            const bootDatabase = kvGet('database/database.bin');
            if (bootDatabase) await preflightBootDatabase(bootDatabase);
        } catch (error) {
            bootDatabaseValidated = false;
            // A damaged live monolith must not make every recovery API
            // unreachable. Preserve it and every physical plugin row byte-for-
            // byte; authenticated bootstrap can now inspect internal snapshots
            // and publish a selected one through /api/db/snapshots/restore.
            invalidateDbCache();
            dbEtag = null;
            const diagnostic = pluginStorageValidationDiagnostic(error);
            logger.error(
                '[BootRecovery] Live database could not be normalized; '
                + 'starting in snapshot-recovery mode without changing storage'
                + (diagnostic
                    ? ` (${diagnostic.code}: ${diagnostic.encodedKey})`
                    : ` (${error?.name || 'decode error'})`),
            );
        }
        if (bootDatabaseValidated) {
            kvBumpListEpoch();
            logger.info('[ListDelta] Bumped list epoch at startup');
            migrateAssetsToFilesystem();
            await migrateInlaysToFilesystem();
            await migrateChatsToRowsIfNeeded();
            await migrateCharacterDefaultsIfNeeded();
            // The chat marker can already exist on databases restored by older
            // Node-only versions, so independently inspect the steady-state stub
            // for folded optimized plugin storage before accepting clients.
            const bootDatabase = kvGet('database/database.bin');
            if (bootDatabase) await loadStrippedDatabase(bootDatabase, 'Migration');
            await migrateRemoteBlocksIfNeeded();
            // Private transition stages are ordinary migration state. Reconcile
            // them only after the authoritative live database has passed the
            // same read-only preflight, so a corrupt boot remains byte-exact.
            await reconcilePluginTransitionStagesAtStartup();
        }
        // A prior process may have exited while the snapshot cooldown was
        // deferring an already-committed plugin publication.
        schedulePluginRecoverySnapshot();
        const port = process.env.PORT || 6001;
        // HOST limits the bind address (e.g. 127.0.0.1 behind a reverse
        // proxy). Unset keeps the historical all-interfaces behavior.
        const host = process.env.HOST || undefined;
        const httpsOptions = await getHttpsOptions();
        let server;

        if (httpsOptions) {
            // HTTPS
            server = https.createServer(httpsOptions, app);
            setupProxyStreamWebSocket(server);
            server.listen(port, host, () => {
                console.log("[Server] HTTPS server is running.");
                console.log(`[Server] https://${host || 'localhost'}:${port}/`);
            });
        } else {
            // HTTP
            server = http.createServer(app);
            setupProxyStreamWebSocket(server);
            server.listen(port, host, () => {
                console.log("[Server] HTTP server is running.");
                console.log(`[Server] http://${host || 'localhost'}:${port}/`);
            });
        }
    } catch (error) {
        if (!logPluginStorageValidationFailure(
            '[PluginStorage] Rejected invalid row during startup',
            error
        )) {
            logger.error('[Server] Failed to start server :', error);
        }
        process.exit(1);
    }
}

// Graceful shutdown: flush pending patches and checkpoint WAL before exit
for (const sig of ['SIGTERM', 'SIGINT']) {
    process.on(sig, async () => {
        console.log(`[Server] Received ${sig}, flushing pending data...`);
        try {
            const persisted = await queueStorageMutation(
                () => flushPendingDb({ scheduleSnapshot: false }),
            );
            if (persisted) await createBackupAndRotate();
        } catch (e) { logger.error('[Server] Flush error:', e); }
        try {
            await runTrackedWalCheckpointWithBusyRetry('TRUNCATE', 'graceful-shutdown');
        } catch { /* non-fatal */ }
        if (sig === 'SIGTERM' && STORAGE_QUEUE_DIAG_ENABLED) {
            logStorageQueueDiagSummary();
        }
        try { modelJobs.close(); } catch (e) { logger.error('[ModelJobs] Close error:', e); }
        try { requestLogs.close(); } catch (e) { logger.error('[RequestLogs] Close error:', e); }
        process.exit(0);
    });
}

(async () => {
    try { kvCleanupOldDeletions(); }
    catch (error) { logger.warn('[ListDelta] Initial deletion cleanup failed:', error?.message || error); }

    // Proxy stream job garbage collection
    startProxyStreamJobGc();

    // Partial export job garbage collection
    startPartialExportJobGc();

    const normalizedChatHistory = await chatBackupStore.normalizeChatBackups();
    logger.info(
        `[ChatBackups] Startup history normalization complete: `
        + `${normalizedChatHistory.rootsVisited} root(s), `
        + `${normalizedChatHistory.framesCreated} frame(s), `
        + `${normalizedChatHistory.conflictsPreserved} conflict(s) preserved, `
        + `${normalizedChatHistory.framesInvalid} invalid frame(s) retained`,
    );

    await startServer();
    startSqliteDurabilityCheckpointScheduler();
    scheduleServerAssetCleanup();

    chatBackupStore.reconcileChatBackups()
        .then((result) => {
            logger.info(
                `[ChatBackups] Startup reconcile complete: `
                + `${result.framesCreated} frame(s), `
                + `${result.legacyBundlesMigrated} legacy bundle migration(s), `
                + `${result.budgetItemsRemoved} budget eviction(s)`
            );
        })
        .catch(error => logger.error('[ChatBackups] Startup reconcile failed:', error));

    setInterval(() => {
        try { kvCleanupOldDeletions(); }
        catch { /* non-fatal */ }
    }, 60 * 60 * 1000); // every hour

})();
