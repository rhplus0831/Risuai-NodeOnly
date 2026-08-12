'use strict';

const path = require('path');
const fsSync = require('fs');
const {
    existsSync,
    mkdirSync,
    writeFileSync,
    readdirSync,
    unlinkSync,
    createReadStream,
    createWriteStream,
} = fsSync;
const fs = require('fs/promises');
const nodeCrypto = require('crypto');
const zlib = require('zlib');
const { finished } = require('stream/promises');
const {
    kvGet,
    kvWriteToFile,
    kvSet,
    kvDel,
    kvList,
    kvDelPrefix,
    kvListWithSizes,
    kvSize,
    kvClearDeletion,
    kvRecordDeletion,
    kvBumpListEpoch,
    clearEntities,
    createKvSnapshot,
    isLegacyHexMigrationComplete,
    markLegacyHexMigrationComplete,
    publishLegacyHexMigrationMarker,
    db: sqliteDb,
} = require('../db/db.cjs');
const {
    assetDir,
    isSafeAssetName,
    assetPathFor,
    swapAssetDirectoryFromStaging,
    swapDirectoryFromStaging,
} = require('../assets/assetStore.cjs');
const {
    writeImportJournal,
    clearImportJournal,
    fsyncDirectoryTree,
    recoverImportSwap,
} = require('./importJournal.cjs');
const {
    IMPORT_IO_PAGE_BYTES,
    SAVE_FOLDER_IMPORT_STAGE_PREFIX,
    ImportIngressError,
    finiteByteLimit,
    importSizeError,
    importFormatError,
    importErrorPayload,
    assertImportSize,
    throwIfAborted: throwIfImportAborted,
    createImportAbortTracker,
    spoolAsyncIterable,
    copyFileToSpool,
    readFileToBufferBounded,
    validateJsonFileStreaming,
    inspectZipFile,
    extractZipEntries,
} = require('./importSpool.cjs');
const {
    assertBackupEntryNameWithinLimit,
    encodeBackupEntryHeader,
    backupEntrySize,
    preflightBackupEntries,
} = require('./backupEntryFormat.cjs');
const { createBackupImportIndex } = require('./backupImportIndex.cjs');
const {
    MCP_TOOL_CALL_CACHE_PREFIX,
    mcpToolCallStorageKey,
    parseMcpToolCallStorageKey,
    scanMcpToolCallIdsFromFile,
} = require('./mcpToolCallRecovery.cjs');
const { streamRisuSaveToFile } = require('./streamRisuSave.cjs');
const {
    convertBlockRisuSaveToMessagePack,
    streamBackupRisuSaveToFile,
} = require('./streamBackupRisuSave.cjs');
const {
    RisuSavePreparationError,
    configuredMaxDecodedBytes,
    decodeBoundedLegacyRisuSave,
    inspectRisuSaveSource,
} = require('./streamRisuLoad.cjs');
const {
    decodeRisuSave,
    decodeAuthoritativeRisuSave,
    encodeRisuSaveLegacy,
    magicHeader,
    magicRisuSaveHeader,
} = require('../utils.cjs');
const { computeBufferEtag } = require('../db/dbCachedRead.cjs');
const { CHARACTER_DEFAULTS_MARKER_KEY } = require('../chat/characterDefaults.cjs');
const {
    BACKUP_ENTRY_NAME_MAX_BYTES,
    PLUGIN_SAVE_PREFIX,
    PLUGIN_SAVE_META_PREFIX,
    assertArchiveSafePluginSaveStorageKey,
    PLUGIN_STORAGE_GENERATION_FIELD,
    PLUGIN_STORAGE_MANIFEST_KEY,
    isHashedPluginSaveStorageKey,
} = require('../plugin-storage/pluginSaveKeys.cjs');
const { decodeValidatedPluginStorageKey } = require('../plugin-storage/pluginStorageJson.cjs');
const {
    PluginStorageLimitError,
} = require('../plugin-storage/pluginStorageLimits.cjs');
const {
    isSelfUpdateInProgress,
    withLocalRecoveryPathStateLock,
    waitAtRecoveryPathStateTestGate,
} = require('../runtime/selfUpdate.cjs');
const {
    acquireRecoveryPathStateLockSync,
    readRecoveryPathMarkerTargetsSync,
} = require('../recoveryPathMarkers.cjs');
const { logger } = require('../runtime/logs.cjs');
const { spawn } = require('child_process');

const DRAFT_PREFIX = 'drafts/';
const configuredPartialExportJobTtlMs = Number(
    process.env.NODE_ENV === 'test'
        ? process.env.POCKETRISU_TEST_PARTIAL_EXPORT_TTL_MS
        : NaN,
);
const PARTIAL_EXPORT_JOB_TTL_MS = Number.isSafeInteger(configuredPartialExportJobTtlMs)
    && configuredPartialExportJobTtlMs >= 100
    ? configuredPartialExportJobTtlMs
    : 15 * 60 * 1000;
const configuredPartialExportGcIntervalMs = Number(
    process.env.NODE_ENV === 'test'
        ? process.env.POCKETRISU_TEST_PARTIAL_EXPORT_GC_INTERVAL_MS
        : NaN,
);
const PARTIAL_EXPORT_GC_INTERVAL_MS = Number.isSafeInteger(configuredPartialExportGcIntervalMs)
    && configuredPartialExportGcIntervalMs >= 10
    ? configuredPartialExportGcIntervalMs
    : 60 * 1000;
// A client can time out its create POST and send DELETE before the POST reaches
// admission. Remember that exact owner/id cancellation for a bounded window so
// the delayed create cannot resurrect a job after cleanup appeared to succeed.
const PARTIAL_EXPORT_CANCELLATION_TTL_MS = 15 * 60 * 1000;
const PARTIAL_EXPORT_MAX_CANCELLATION_TOMBSTONES = 256;
// This is a single-user server and each job can hold a WAL snapshot plus two
// archive-sized spools. Serial admission makes the statfs preflight an actual
// reservation instead of letting concurrent jobs all spend the same bytes.
const PARTIAL_EXPORT_MAX_ACTIVE_JOBS = 1;
const PLUGIN_VALUE_SPOOL_FILE_PREFIX = '.plugin-value-';

// Test-only recovery transaction/acknowledgement boundaries. Both are after
// snapshot validation; before-commit must roll back every publication row,
// while response simulates an acknowledgement lost after COMMIT.
const snapshotRestoreFailpoint = process.env.NODE_ENV === 'test'
    ? String(process.env.POCKETRISU_TEST_SNAPSHOT_RESTORE_FAILPOINT ?? '').trim()
    : '';
// Test-only REMOTE resolver boundaries. These live at the restore-route adapter
// so production KV primitives never gain failure behavior.
const snapshotRestoreRemoteFailpoint = process.env.NODE_ENV === 'test'
    ? String(process.env.POCKETRISU_TEST_SNAPSHOT_REMOTE_FAILPOINT ?? '').trim()
    : '';
const SNAPSHOT_RESTORE_DECODE_TEST_GATE_DIR = process.env.NODE_ENV === 'test'
    ? String(process.env.POCKETRISU_SNAPSHOT_RESTORE_DECODE_TEST_GATE_DIR ?? '').trim() || null
    : null;
const BACKUP_IMPORT_TEST_GATE_DIR = process.env.NODE_ENV === 'test'
    ? String(process.env.POCKETRISU_BACKUP_IMPORT_TEST_GATE_DIR ?? '').trim() || null
    : null;
const backupImportFailpoint = process.env.NODE_ENV === 'test'
    ? String(process.env.POCKETRISU_TEST_BACKUP_IMPORT_FAILPOINT ?? '').trim()
    : '';
const saveFolderImportFailpoint = process.env.NODE_ENV === 'test'
    ? String(process.env.POCKETRISU_TEST_SAVE_FOLDER_IMPORT_FAILPOINT ?? '').trim()
    : '';
function hasSaveFolderImportFailpoint(name) {
    return saveFolderImportFailpoint
        .split(',')
        .map(value => value.trim())
        .filter(Boolean)
        .includes(name);
}
let snapshotRestoreDecodeGateEntered = false;
async function waitAtSnapshotRestoreDecodeTestGate(signal) {
    if (!SNAPSHOT_RESTORE_DECODE_TEST_GATE_DIR || snapshotRestoreDecodeGateEntered) return;
    const holdPath = path.join(SNAPSHOT_RESTORE_DECODE_TEST_GATE_DIR, 'hold');
    if (!existsSync(holdPath)) return;
    snapshotRestoreDecodeGateEntered = true;
    await fs.mkdir(SNAPSHOT_RESTORE_DECODE_TEST_GATE_DIR, { recursive: true });
    await fs.writeFile(
        path.join(SNAPSHOT_RESTORE_DECODE_TEST_GATE_DIR, 'entered'),
        'during-decompression',
        'utf-8',
    );
    const releasePath = path.join(SNAPSHOT_RESTORE_DECODE_TEST_GATE_DIR, 'release');
    while (!signal?.aborted && existsSync(holdPath) && !existsSync(releasePath)) {
        await new Promise((resolve) => setTimeout(resolve, 10));
    }
}
async function waitAtBackupImportTestGate(signal = null) {
    if (!BACKUP_IMPORT_TEST_GATE_DIR) return;
    const holdPath = path.join(BACKUP_IMPORT_TEST_GATE_DIR, 'hold');
    if (!existsSync(holdPath)) return;
    await fs.mkdir(BACKUP_IMPORT_TEST_GATE_DIR, { recursive: true });
    await fs.writeFile(
        path.join(BACKUP_IMPORT_TEST_GATE_DIR, 'entered'),
        'after-database-ingestion',
        'utf-8',
    );
    const releasePath = path.join(BACKUP_IMPORT_TEST_GATE_DIR, 'release');
    while (!signal?.aborted && existsSync(holdPath) && !existsSync(releasePath)) {
        await new Promise((resolve) => setTimeout(resolve, 10));
    }
    throwIfImportAborted(signal);
}

const partialExportJobs = new Map();
const partialExportCancellationTombstones = new Map();

function partialExportOwner(req) {
    const sessionId = req.headers['x-session-id'];
    return typeof sessionId === 'string' ? sessionId : '';
}

function partialExportCancellationKey(owner, jobId) {
    return JSON.stringify([owner, jobId]);
}

function prunePartialExportCancellationTombstones(now = Date.now()) {
    for (const [key, expiresAt] of partialExportCancellationTombstones) {
        if (expiresAt <= now) partialExportCancellationTombstones.delete(key);
    }
}

function recordPartialExportCancellation(owner, jobId) {
    const now = Date.now();
    prunePartialExportCancellationTombstones(now);
    const key = partialExportCancellationKey(owner, jobId);
    partialExportCancellationTombstones.delete(key);
    while (partialExportCancellationTombstones.size
        >= PARTIAL_EXPORT_MAX_CANCELLATION_TOMBSTONES) {
        partialExportCancellationTombstones.delete(
            partialExportCancellationTombstones.keys().next().value,
        );
    }
    partialExportCancellationTombstones.set(
        key,
        now + PARTIAL_EXPORT_CANCELLATION_TTL_MS,
    );
}

function wasPartialExportCancelled(owner, jobId) {
    prunePartialExportCancellationTombstones();
    return partialExportCancellationTombstones.has(
        partialExportCancellationKey(owner, jobId),
    );
}

function partialExportJobForRequest(req, res) {
    const job = partialExportJobs.get(req.params.jobId);
    if (!job || job.owner !== partialExportOwner(req)) {
        res.status(404).json({ error: 'Partial export job not found' });
        return null;
    }
    return job;
}

function throwIfPartialExportCancelled(job) {
    if (job.abortController.signal.aborted) {
        const error = new Error('Partial export was cancelled');
        error.name = 'AbortError';
        throw error;
    }
}

async function cleanupPartialExportArtifacts(job) {
    try { job.snapshot?.close(); } catch {}
    job.snapshot = null;
    if (job.databaseSpool?.filePath) {
        await fs.unlink(job.databaseSpool.filePath).catch(() => {});
    }
    job.databaseSpool = null;
    await fs.rm(job.spoolDir, { recursive: true, force: true }).catch(() => {});
}

async function cleanupPartialExportJob(job) {
    if (job.cleaned) return;
    job.cleaned = true;
    partialExportJobs.delete(job.id);
    job.abortController.abort();
    await cleanupPartialExportArtifacts(job);
}


function startPartialExportJobGc() {
    setInterval(() => {
        const now = Date.now();
        for (const job of partialExportJobs.values()) {
            if (now < job.expiresAt) continue;
            job.state = 'cancelled';
            job.abortController.abort();
            partialExportJobs.delete(job.id);
            if (!job.preparation || job.progress.phase === 'ready' || job.progress.phase === 'failed') {
                cleanupPartialExportJob(job).catch(error => {
                    logger.warn('[Partial Backup Export] TTL cleanup failed:', error);
                });
            }
        }
        prunePartialExportCancellationTombstones(now);
    }, PARTIAL_EXPORT_GC_INTERVAL_MS);
}

const backupRouteFamilies = new WeakMap();

function createBackupRouteFamily(ctx) {
    const {
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
    } = ctx;

    function withRecoveryPathStateLock(operation) {
        return withLocalRecoveryPathStateLock(async () => {
            const interprocessLock = acquireRecoveryPathStateLockSync(savePath, {
                purpose: 'server backup-path transition',
            });
            try {
                return await operation();
            } finally {
                interprocessLock.release();
            }
        });
    }

    // Heartbeat interval for NDJSON import progress stream. 5 s by default —
    // shorter than every common reverse-proxy response timeout (nginx 60 s, Cloudflare
    // 100 s). Operators behind more aggressive proxies can tighten this. Clamped to
    // 100 ms so a misconfiguration can't spam the socket.
    const BACKUP_NDJSON_HEARTBEAT_MS = Math.max(
        100,
        Number(process.env.BACKUP_NDJSON_HEARTBEAT_MS ?? '5000') || 5000,
    );
    const REPLACEMENT_OPERATION_ID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
    const REPLACEMENT_OPERATION_RETENTION_MS = Math.max(
        60_000,
        Number(process.env.POCKETRISU_REPLACEMENT_OPERATION_RETENTION_MS ?? 24 * 60 * 60 * 1000)
            || 24 * 60 * 60 * 1000,
    );

    const insertReplacementOperation = sqliteDb.prepare(`
        INSERT INTO replacement_operations (
            operation_id, kind, state, result_json, error_json, created_at, updated_at
        ) VALUES (?, ?, 'running', NULL, NULL, ?, ?)
    `);
    const updateReplacementOperation = sqliteDb.prepare(`
        UPDATE replacement_operations
        SET state = ?, result_json = ?, error_json = ?, updated_at = ?
        WHERE operation_id = ?
    `);
    const readReplacementOperation = sqliteDb.prepare(`
        SELECT operation_id, kind, state, result_json, error_json, created_at, updated_at
        FROM replacement_operations
        WHERE operation_id = ?
    `);
    const deleteExpiredReplacementOperations = sqliteDb.prepare(`
        DELETE FROM replacement_operations
        WHERE state != 'running' AND updated_at < ?
    `);

    // A running record is committed before destructive work begins. Every actual
    // publication writes `committed` inside its data transaction, so a process
    // restart can safely classify any leftover running record as not committed.
    sqliteDb.prepare(`
        UPDATE replacement_operations
        SET state = 'not-committed',
            error_json = ?,
            updated_at = ?
        WHERE state = 'running'
    `).run(JSON.stringify({
        message: 'The server restarted before the replacement committed.',
        code: 'REPLACEMENT_INTERRUPTED',
        retryable: true,
    }), Date.now());
    deleteExpiredReplacementOperations.run(Date.now() - REPLACEMENT_OPERATION_RETENTION_MS);

    function replacementOperationId(req) {
        const value = req.headers['x-risu-replacement-id'];
        return typeof value === 'string' && REPLACEMENT_OPERATION_ID_REGEX.test(value)
            ? value
            : null;
    }

    function registerReplacementOperation(req, kind) {
        const operationId = replacementOperationId(req);
        if (!operationId) {
            const error = new Error('A canonical replacement operation ID is required');
            error.code = 'INVALID_REPLACEMENT_OPERATION_ID';
            error.statusCode = 400;
            throw error;
        }
        const now = Date.now();
        deleteExpiredReplacementOperations.run(now - REPLACEMENT_OPERATION_RETENTION_MS);
        try {
            insertReplacementOperation.run(operationId, kind, now, now);
        } catch (error) {
            if (error?.code === 'SQLITE_CONSTRAINT_PRIMARYKEY'
                || error?.code === 'SQLITE_CONSTRAINT_UNIQUE') {
                const conflict = new Error('Replacement operation ID already exists');
                conflict.code = 'REPLACEMENT_OPERATION_EXISTS';
                conflict.statusCode = 409;
                throw conflict;
            }
            throw error;
        }
        return operationId;
    }

    function setReplacementOperationOutcome(operationId, state, { result = null, error = null } = {}) {
        if (!operationId) return;
        updateReplacementOperation.run(
            state,
            result === null ? null : JSON.stringify(result),
            error === null ? null : JSON.stringify(error),
            Date.now(),
            operationId,
        );
    }

    function replacementErrorRecord(error, fallbackCode) {
        const annotated = authoritativeImportErrorPayload(error, fallbackCode);
        return annotated ?? {
            message: String(error?.message ?? 'Replacement failed'),
            code: String(error?.code ?? fallbackCode),
            retryable: false,
        };
    }

    function finalizeReplacementOperationError(operationId, error, fallbackCode) {
        if (!operationId) return;
        const existing = readReplacementOperation.get(operationId);
        if (!existing || existing.state === 'committed') return;
        const annotated = authoritativeImportErrorPayload(error, fallbackCode);
        const state = annotated?.commitOutcome === 'unknown'
            ? 'unknown'
            : annotated?.commitOutcome === 'committed'
                ? 'committed'
                : 'not-committed';
        setReplacementOperationOutcome(operationId, state, {
            error: replacementErrorRecord(error, fallbackCode),
        });
    }

    function parseReplacementOperationRow(row) {
        if (!row) return null;
        let result = null;
        let error = null;
        try { result = row.result_json === null ? null : JSON.parse(row.result_json); } catch {}
        try { error = row.error_json === null ? null : JSON.parse(row.error_json); } catch {}
        return {
            operationId: row.operation_id,
            kind: row.kind,
            state: row.state,
            result,
            error,
            createdAt: row.created_at,
            updatedAt: row.updated_at,
        };
    }

    function beginReplacementNdjson(res) {
        res.setHeader('content-type', 'application/x-ndjson');
        res.setHeader('cache-control', 'no-cache, no-transform');
        res.setHeader('x-accel-buffering', 'no');
        res.flushHeaders();
        res.write('{"type":"heartbeat"}\n');
        return setInterval(() => {
            if (!res.writableEnded && !res.destroyed) res.write('{"type":"heartbeat"}\n');
        }, BACKUP_NDJSON_HEARTBEAT_MS);
    }

    function sendReplacementProgress(res, phase) {
        if (!res.writableEnded && !res.destroyed) {
            res.write(`${JSON.stringify({ type: 'progress', phase })}\n`);
        }
    }

    function sendReplacementDone(res, operationId, result) {
        if (res.writableEnded || res.destroyed) return;
        res.write(`${JSON.stringify({
            type: 'done',
            operationId,
            commitOutcome: 'committed',
            commitOutcomeUnknown: false,
            ...result,
        })}\n`);
        res.end();
    }

    function importDiskSpaceError(required, available) {
        const error = new ImportIngressError('Insufficient disk space for import staging', {
            code: 'IMPORT_DISK_SPACE',
            statusCode: 507,
            limit: available,
            actual: required,
        });
        error.available = available;
        error.required = required;
        return error;
    }

    function authoritativeImportErrorPayload(error, fallbackCode) {
        if (!error || typeof error !== 'object') return null;
        const claimedOutcome = error.commitOutcome;
        const claimedUnknown = error.commitOutcomeUnknown;
        const commitOutcome = claimedUnknown === true || claimedOutcome === 'unknown'
            ? 'unknown'
            : claimedOutcome === 'not-committed' || claimedOutcome === 'committed'
                ? claimedOutcome
                : null;
        if (commitOutcome === null
            || typeof claimedUnknown !== 'boolean'
            || claimedUnknown !== (commitOutcome === 'unknown')) return null;
        return {
            error: String(error.message ?? 'Import failed'),
            code: typeof error.code === 'string' && error.code.length > 0
                ? error.code
                : fallbackCode,
            retryable: error.retryable === true,
            commitOutcome,
            commitOutcomeUnknown: commitOutcome === 'unknown',
        };
    }

    function sendImportIngressError(res, error, {
        ndjson = false,
        fallbackCode = 'BACKUP_IMPORT_FAILED',
        includeAnnotatedOutcome = false,
    } = {}) {
        const payload = importErrorPayload(error)
            ?? (includeAnnotatedOutcome
                ? authoritativeImportErrorPayload(error, fallbackCode)
                : null)
            ?? (
            error?.risuSavePreparationLimit === true
            || error?.risuSavePreparationInvalid === true
            || error instanceof PluginStorageLimitError
                ? {
                    error: error.message,
                    code: error.code,
                    ...(error.limit === undefined ? {} : { limit: error.limit }),
                    ...(error.actual === undefined ? {} : { actual: error.actual }),
                    retryable: false,
                    commitOutcome: error.commitOutcome ?? 'not-committed',
                    commitOutcomeUnknown: error.commitOutcomeUnknown ?? false,
                }
                : null
        );
        if (!payload) return false;
        // A disconnected importer has no response channel left. Treat its
        // structured cancellation as handled rather than handing it to Express,
        // which can only produce a secondary socket-write failure.
        if (res.destroyed) return true;
        if (error.available !== undefined) payload.available = error.available;
        if (error.required !== undefined) payload.required = error.required;
        if (ndjson && res.headersSent) {
            if (!res.writableEnded && !res.destroyed) {
                res.write(`${JSON.stringify(importNdjsonErrorEvent(error, payload))}\n`);
                res.end();
            }
            return true;
        }
        if (!res.headersSent) res.status(error.statusCode ?? error.status ?? 400).json(payload);
        return true;
    }

    function importNdjsonErrorEvent(error, payload = null, fallbackCode = 'BACKUP_IMPORT_FAILED') {
        const claimedOutcome = payload?.commitOutcome ?? error?.commitOutcome;
        const claimedUnknown = payload?.commitOutcomeUnknown ?? error?.commitOutcomeUnknown;
        const commitOutcome = claimedUnknown === true || claimedOutcome === 'unknown'
            ? 'unknown'
            : claimedOutcome === 'not-committed' || claimedOutcome === 'committed'
                ? claimedOutcome
                : 'unknown';
        return {
            type: 'error',
            message: String(payload?.message ?? payload?.error ?? error?.message ?? 'Backup import failed'),
            code: String(payload?.code ?? error?.code ?? fallbackCode),
            retryable: payload?.retryable === true || error?.retryable === true,
            commitOutcome,
            commitOutcomeUnknown: commitOutcome === 'unknown',
            status: Number(error?.statusCode ?? error?.status ?? payload?.status ?? 500),
        };
    }

    function importContentLength(req, label) {
        const raw = req.headers['content-length'];
        if (raw === undefined) return null;
        const value = Number(raw);
        if (!Number.isSafeInteger(value) || value < 0) {
            throw importFormatError(`${label} has an invalid Content-Length`, 'INVALID_IMPORT_SIZE');
        }
        return value;
    }

    function requestConfirmsLargeRestore(req) {
        return req.headers['x-risu-large-restore'] === '1';
    }

    function backupImportLimits({ allowLargeRestore = false } = {}) {
        return allowLargeRestore
            ? {
                maxBytes: LARGE_RESTORE_MAX_BYTES,
                maxEntries: LARGE_RESTORE_MAX_ENTRIES,
                // Remaining compatibility-only buffered rows are admitted under
                // the explicit recovery ceiling.  Current exported assets, raw
                // inlays, cold storage, plugin values, and remote rows all use
                // file-backed paths below and do not allocate this amount.
                bufferedEntryMaxBytes: LARGE_RESTORE_MAX_BYTES,
            }
            : {
                maxBytes: BACKUP_IMPORT_MAX_BYTES,
                maxEntries: BACKUP_IMPORT_MAX_ENTRIES,
                bufferedEntryMaxBytes: IMPORT_BUFFERED_ENTRY_MAX_BYTES,
            };
    }

    async function assertImportDiskSpace(sourceBytes, targetPath = getDatabaseSpoolDir()) {
        if (targetPath === getDatabaseSpoolDir()) requireDatabaseSpoolDirSync();
        const required = sourceBytes * BACKUP_DISK_HEADROOM;
        if (!Number.isSafeInteger(required)) {
            throw importFormatError('Import disk requirement is not a safe byte count', 'INVALID_IMPORT_SIZE');
        }
        const disk = await checkDiskSpace(required, targetPath);
        if (!disk.ok) throw importDiskSpaceError(required, disk.available);
        return disk;
    }

    function attachImportSettlementError(primaryError, settlementError) {
        if (!primaryError || typeof primaryError !== 'object') return;
        if (!Array.isArray(primaryError.cleanupErrors)) primaryError.cleanupErrors = [];
        primaryError.cleanupErrors.push(settlementError);
    }

    function settleJournaledImportAssetSwap({
        journal,
        assetSwap,
        transactionCommitted,
        databaseSettled,
        source,
        primaryError = null,
        beforeRecovery = null,
    }) {
        if (!journal || !assetSwap || assetSwap.isMaintenanceReleased()) {
            return { settled: true, journal };
        }
        if (!databaseSettled) {
            const settlementError = new Error(
                `${source} cannot settle the asset swap because database recovery is incomplete`,
            );
            settlementError.code = 'IMPORT_DATABASE_RECOVERY_INCOMPLETE';
            if (primaryError) {
                attachImportSettlementError(primaryError, settlementError);
                return { settled: false, journal };
            }
            throw settlementError;
        }

        try {
            beforeRecovery?.();
            let durableJournal = journal;
            if (transactionCommitted && durableJournal.phase !== 'committed') {
                durableJournal = { ...durableJournal, phase: 'committed' };
                writeImportJournal(IMPORT_JOURNAL_PATH, durableJournal);
            }
            const markerValue = kvGet(IMPORT_JOURNAL_MARKER_KEY);
            const markerPresent = transactionCommitted || (
                markerValue !== null
                && Buffer.from(markerValue).toString('utf-8') === durableJournal.id
            );
            recoverImportSwap({
                journal: durableJournal,
                markerPresent,
                fs: fsSync,
            });
            if (markerValue !== null) kvDel(IMPORT_JOURNAL_MARKER_KEY);
            clearImportJournal(IMPORT_JOURNAL_PATH);
            const released = assetSwap.releaseAfterRecovery(primaryError);
            return { settled: released, journal: durableJournal };
        } catch (settlementError) {
            if (primaryError) {
                attachImportSettlementError(primaryError, settlementError);
                return { settled: false, journal };
            }
            throw settlementError;
        }
    }

    /**
     * Spool an assembled legacy database to disk. Chat and optional plugin rows
     * are decoded and encoded one at a time; strippedDb is never mutated.
     */
    async function spoolSelfContainedBackupDatabase(
        strippedDb,
        {
            foldPluginStorage = false,
            foldMcpToolCalls = false,
            markPluginStorageFolded = false,
            shouldAbort = () => false,
            reader = { kvGet, kvList, kvListWithSizes },
            onMissingChatRow,
        } = {}
    ) {
        requireDatabaseSpoolDirSync();
        const finalPath = path.join(
            getDatabaseSpoolDir(),
            `${DATABASE_SPOOL_FILE_PREFIX}${process.pid}-${nodeCrypto.randomUUID()}`
        );
        const filePath = finalPath + '.tmp';
        const pluginStorage = foldPluginStorage
            ? resolveOwnedPluginStorageRows(strippedDb, reader)
            : null;
        const mcpToolCalls = foldMcpToolCalls
            ? mcpToolCallSnapshotStorage(reader)
            : null;

        try {
            return await streamRisuSaveToFile({
                dbObj: strippedDb,
                filePath,
                readChatRow: async (chaId, chatId) => {
                    const value = chatRowStore.materializeChatRowBytesFromReader(
                        reader,
                        chatRowKey(chaId, chatId),
                    );
                    return value === null ? null : decodeRisuSave(value);
                },
                pluginStorage,
                mcpToolCalls,
                markPluginStorageFolded,
                shouldAbort,
                onMissingChatRow,
            });
        } catch (error) {
            await fs.unlink(filePath).catch(() => {});
            throw error;
        }
    }


    function listLogicalChatRowsWithSizes(reader) {
        return reader.kvListWithSizes('chats/').map((entry) => {
            const metadata = typeof reader.chatRowMetadata === 'function'
                ? reader.chatRowMetadata(entry.key)
                : null;
            const logicalSize = metadata?.log_count > 0
                ? metadata.content_size
                : entry.size;
            if (!Number.isSafeInteger(logicalSize) || logicalSize < 0) {
                throw new Error(`Chat row has an invalid logical size: ${entry.key}`);
            }
            return { ...entry, size: logicalSize };
        });
    }

    function canStreamImportedDatabase(inspection) {
        return inspection.supported || inspection.format === 'risusave';
    }

    /**
     * Convert block-oriented RISUSAVE databases to the canonical streaming input
     * on disk, then feed them through the same chat/plugin externalization path as
     * ordinary MessagePack imports. REMOTE payloads are spooled from the rows that
     * the enclosing replacement transaction has already staged, so neither the
     * database nor a large remote character has to be assembled in memory.
     */
    async function ingestImportedDatabaseStreaming(
        databaseSource,
        inspection,
        { signal = null } = {},
    ) {
        if (inspection.format !== 'risusave') {
            return ingestDatabaseStreaming(databaseSource, {
                inspection,
                shouldAbort: () => signal?.aborted === true,
                signal,
            });
        }

        const convertedPath = path.join(
            getDatabaseSpoolDir(),
            `${DATABASE_SPOOL_FILE_PREFIX}block-import-${process.pid}-${nodeCrypto.randomUUID()}.tmp`,
        );
        const liveReader = { kvSize, kvWriteToFile };
        let converted = null;
        try {
            try {
                converted = await convertBlockRisuSaveToMessagePack(
                    databaseSource,
                    convertedPath,
                    {
                        readRemoteRowSize: (name) => kvSize(`remotes/${name}.local.bin`),
                        readRemoteRowSource: (name) => spoolBackupSnapshotRow(
                            liveReader,
                            `remotes/${name}.local.bin`,
                            {
                                signal,
                                shouldAbort: () => signal?.aborted === true,
                            },
                        ),
                        maxDecodedBytes: configuredMaxDecodedBytes(),
                        shouldAbort: () => signal?.aborted === true,
                        signal,
                        throwIfAborted: () => throwIfImportAborted(signal),
                    },
                );
            } catch (error) {
                if (signal?.aborted
                    || error?.risuSavePreparationInvalid === true
                    || error?.risuSavePreparationLimit === true
                    || error?.name === 'AbortError'
                    || error?.syscall
                    || error?.code === 'KV_CHUNK_CORRUPT') {
                    throw error;
                }
                throw new RisuSavePreparationError(
                    String(error?.message ?? 'Invalid RisuSave block database'),
                    { cause: error },
                );
            }
            return await ingestDatabaseStreaming(converted, {
                shouldAbort: () => signal?.aborted === true,
                signal,
                maxDecodedBytes: configuredMaxDecodedBytes(),
            });
        } finally {
            await converted?.cleanup?.();
            // The converter removes incomplete outputs itself. This catches a
            // process-local failure before it has returned its cleanup handle.
            await fs.unlink(convertedPath).catch(() => {});
        }
    }

    /**
     * Chat rows are always assembled into database.risudat. Migration targets
     * (upstream and main rollback) also fold external plugin rows into that
     * database; Node-only exports keep them as independent archive entries so
     * large plugin stores are never monolithized.
     */
    async function buildSelfContainedBackupDatabase({
        foldPluginStorage = true,
        shouldAbort = () => false,
        onMissingChatRow,
        snapshot: externalSnapshot = null,
        databaseSource = null,
        databaseState = null,
        signal = null,
        onDatabaseLoaded,
        omitAccount = false,
    } = {}) {
        let snapshot = externalSnapshot;
        let ownsSnapshot = false;
        try {
            if (!snapshot) {
                snapshot = await queueStorageOperation(async () => {
                    await flushPendingDb();
                    return createKvSnapshot();
                }, 'snapshot-capture');
                ownsSnapshot = true;
            }
            if (databaseSource) {
                const finalPath = path.join(
                    getDatabaseSpoolDir(),
                    `${DATABASE_SPOOL_FILE_PREFIX}${process.pid}-${nodeCrypto.randomUUID()}`,
                );
                const filePath = finalPath + '.tmp';
                const spoolSnapshotRow = (key) => spoolBackupSnapshotRow(snapshot, key, {
                    signal,
                    shouldAbort,
                });
                const spoolSnapshotChatRow = (key) => spoolLogicalChatSnapshotRow(
                    snapshot,
                    key,
                    { signal, shouldAbort },
                );
                const pluginStorage = foldPluginStorage
                    ? resolveOwnedPluginStorageRows(databaseState ?? {}, snapshot)
                    : null;
                try {
                    return await streamBackupRisuSaveToFile({
                        databaseSource,
                        filePath,
                        readChatRowSource: (chaId, chatId) => spoolSnapshotChatRow(
                            chatRowKey(chaId, chatId),
                        ),
                        readRemoteRowSource: (name) => spoolSnapshotRow(
                            `remotes/${name}.local.bin`,
                        ),
                        readRemoteRowSize: (name) => snapshot.kvSize(
                            `remotes/${name}.local.bin`,
                        ),
                        pluginStorage: pluginStorage
                            ? {
                                valueRows: pluginStorage.valueRows,
                                metaRows: pluginStorage.metaRows,
                                readRowSource: spoolSnapshotRow,
                            }
                            : null,
                        shouldAbort,
                        signal,
                        tempDir: requireDatabaseSpoolDirSync(),
                        onMissingChatRow,
                    });
                } catch (error) {
                    await fs.unlink(filePath).catch(() => {});
                    throw error;
                }
            }
            let strippedDb;
            const raw = snapshot.kvGet('database/database.bin');
            if (!raw) return null;
            strippedDb = await loadStrippedDatabase(raw, 'Backup');
            const backupDatabase = omitAccount
                ? { ...strippedDb, account: undefined }
                : strippedDb;
            onDatabaseLoaded?.(backupDatabase);
            return await spoolSelfContainedBackupDatabase(backupDatabase, {
                foldPluginStorage,
                shouldAbort,
                reader: snapshot,
                onMissingChatRow,
            });
        } finally {
            if (ownsSnapshot) snapshot?.close();
        }
    }

    async function requireTargetCompatibleBackupDatabase(databaseSpool, target) {
        if (target !== 'main' && target !== 'upstream') {
            throw new Error(`Unsupported backup compatibility target: ${target}`);
        }
        if (!databaseSpool?.filePath || databaseSpool.size < magicHeader.length) {
            const error = new Error(target === 'main'
                ? 'The main-compatible database export is incomplete'
                : 'The upstream-compatible database export is incomplete');
            error.code = target === 'main'
                ? 'BACKUP_MAIN_DATABASE_INCOMPLETE'
                : 'BACKUP_UPSTREAM_DATABASE_INCOMPLETE';
            error.statusCode = 500;
            throw error;
        }

        const handle = await fs.open(databaseSpool.filePath, 'r');
        try {
            const header = Buffer.alloc(magicHeader.length);
            const { bytesRead } = await handle.read(header, 0, header.length, 0);
            if (bytesRead === header.length && header.equals(Buffer.from(magicHeader))) return;
        } finally {
            await handle.close();
        }

        // PocketRisu's escape envelope uses version byte 10, which the main rollback
        // branch predates. Upstream recognizes only legacy bytes 7/8/9 and RISUSAVE\0
        // block headers; byte 10 falls through to raw msgpack and decodes as garbage.
        // Re-encoding as 7/8/9 is not faithful: upstream's msgpackr renames __proto__
        // to __proto_ on decode, and ill-formed Unicode keys cannot round-trip via UTF-8.
        const error = new Error(target === 'main'
            ? 'Cannot export for main because plugin storage contains keys that its save format cannot represent. '
                + 'Rename or remove __proto__ and ill-formed Unicode plugin keys, then retry.'
            : 'Cannot export for upstream RisuAI because plugin storage contains keys that its save format cannot represent. '
                + 'Rename or remove __proto__ and ill-formed Unicode plugin keys, then retry.');
        error.code = target === 'main'
            ? 'BACKUP_MAIN_UNSUPPORTED_PLUGIN_KEYS'
            : 'BACKUP_UPSTREAM_UNSUPPORTED_PLUGIN_KEYS';
        error.statusCode = 409;
        throw error;
    }

    async function listPluginBackupEntries(
        reader = { kvGet, kvList, kvListWithSizes },
        databaseState = null,
    ) {
        let dbObj = databaseState;
        if (!dbObj) {
            const rawDatabase = reader.kvGet('database/database.bin');
            if (!rawDatabase) return [];
            dbObj = await decodeAuthoritativeRisuSave(rawDatabase, {
                resolveRemote: async (name) => reader.kvGet(`remotes/${name}.local.bin`) || null,
            });
        }
        const owned = resolveOwnedPluginStorageKeys(dbObj, reader);
        const sizes = new Map([
            ...reader.kvListWithSizes(PLUGIN_SAVE_PREFIX),
            ...reader.kvListWithSizes(PLUGIN_SAVE_META_PREFIX),
        ].map(entry => [entry.key, entry.size]));
        const rows = [...owned.valueKeys, ...owned.metaKeys].map((key) => {
            // Export and import use the same validator. This catches legacy or
            // manually inserted rows before an archive is published.
            resolveBackupStorageKey(key);
            const size = sizes.get(key);
            if (!Number.isSafeInteger(size) || size < 0) {
                throw new Error(`Owned plugin storage row is unavailable: ${key}`);
            }
            return {
                kind: 'kv-source',
                key,
                backupName: key,
                sortKey: key,
                size,
            };
        });
        const manifestState = readPluginStorageManifestState(reader.kvGet);
        const generation = pluginStorageGeneration(dbObj);
        const manifestSize = reader.kvListWithSizes(PLUGIN_STORAGE_MANIFEST_KEY)
            .find(entry => entry.key === PLUGIN_STORAGE_MANIFEST_KEY)?.size;
        if (dbObj.optimizePluginMemory === true && generation) {
            if (!manifestState.valid
                || manifestState.manifest?.generation !== generation
                || !Number.isSafeInteger(manifestSize)
                || manifestSize < 0) {
                throw new TypeError(
                    'The selected plugin storage manifest is not physically available',
                );
            }
            rows.push({
                kind: 'kv-source',
                key: PLUGIN_STORAGE_MANIFEST_KEY,
                backupName: PLUGIN_STORAGE_MANIFEST_KEY,
                sortKey: PLUGIN_STORAGE_MANIFEST_KEY,
                size: manifestSize,
            });
        }
        return rows;
    }


    async function restoreImportedDraftEntries(entries, database, { signal = null } = {}) {
        const referenced = referencedDraftStorageKeys(database);
        let restored = 0;
        for (const entry of entries) {
            throwIfImportAborted(signal);
            if (!referenced.has(entry.key)) continue;
            await importOpaqueRowFromFile(entry.key, entry, signal);
            restored++;
        }
        return restored;
    }

    function mcpToolCallSnapshotStorage(reader) {
        const rows = listMcpToolCallBackupEntries(reader).map((entry) => ({
            key: parseMcpToolCallStorageKey(entry.key).suffix,
            source: entry.key,
        }));
        return {
            rows,
            readRow: (storageKey) => {
                const value = reader.kvGet(storageKey);
                if (!value) throw new Error(`Remembered MCP tool-call row is unavailable: ${storageKey}`);
                try {
                    return JSON.parse(value.toString('utf8'));
                } catch (cause) {
                    throw new Error(`Remembered MCP tool-call row is invalid: ${storageKey}`, { cause });
                }
            },
        };
    }

    function missingMcpToolCallBackupRowError(callId) {
        const error = new Error(`Backup cannot resolve remembered MCP tool call ${callId}`);
        error.code = 'BACKUP_MISSING_MCP_TOOL_CALL_ROW';
        error.statusCode = 500;
        return error;
    }

    async function selectReferencedMcpToolCallEntries(entries, databaseSpool, shouldAbort) {
        const candidates = new Map(
            entries.filter((entry) => entry.mcpToolCall === true)
                .map((entry) => [entry.key, entry]),
        );
        if (candidates.size === 0) {
            const referenced = await scanMcpToolCallIdsFromFile(databaseSpool.filePath, { shouldAbort });
            if (referenced.size > 0) {
                throw missingMcpToolCallBackupRowError(referenced.values().next().value);
            }
            return entries;
        }
        const referenced = await scanMcpToolCallIdsFromFile(databaseSpool.filePath, { shouldAbort });
        const selectedKeys = new Set();
        for (const callId of referenced) {
            const storageKey = mcpToolCallStorageKey(callId);
            if (!storageKey || !candidates.has(storageKey)) {
                throw missingMcpToolCallBackupRowError(callId);
            }
            selectedKeys.add(storageKey);
        }
        return entries.filter((entry) => entry.mcpToolCall !== true || selectedKeys.has(entry.key));
    }

    // Full downloads and server-side saves can each retain a SQLite WAL snapshot,
    // a database assembly spool, and a private copy of every filesystem asset.
    // Keep admission bounded so concurrent requests cannot all reserve the same
    // free space reported by statfs.
    const FULL_EXPORT_MAX_ACTIVE_PINS = 2;
    const activeFullExportPins = new Set();
    const fullExportReservedBytesByVolume = new Map();

    function createBackupExportAbortTracker(req, res) {
        const controller = new AbortController();
        const socket = req.socket;
        const abort = () => {
            if (controller.signal.aborted || res.writableFinished) return;
            const error = new Error('Backup export client disconnected');
            error.name = 'AbortError';
            controller.abort(error);
        };
        req.once('aborted', abort);
        socket?.once('close', abort);
        socket?.once('error', abort);
        res.once('error', abort);
        res.once('close', abort);
        const disconnectPoll = setInterval(() => {
            if (req.aborted || socket?.destroyed || res.destroyed) abort();
        }, 25);
        disconnectPoll.unref?.();
        if (req.aborted || socket?.destroyed || res.destroyed) abort();
        return {
            signal: controller.signal,
            cleanup() {
                clearInterval(disconnectPoll);
                req.removeListener('aborted', abort);
                socket?.removeListener('close', abort);
                socket?.removeListener('error', abort);
                res.removeListener('error', abort);
                res.removeListener('close', abort);
            },
        };
    }

    function backupExportCapacityError() {
        const error = new Error('Too many full backup exports are active');
        error.code = 'BACKUP_EXPORT_CAPACITY';
        error.statusCode = 503;
        return error;
    }

    function backupExportErrorPayload(error) {
        return {
            error: error.message,
            code: error.code,
            ...(error.required === undefined ? {} : { required: error.required }),
            ...(error.available === undefined ? {} : { available: error.available }),
            ...(error.reserved === undefined ? {} : { reserved: error.reserved }),
            ...(error.roles === undefined ? {} : { roles: error.roles }),
        };
    }

    function throwIfBackupExportAborted(signal) {
        throwIfSignalAborted(signal);
    }

    async function hashBackupExportFile(entry, signal) {
        const source = await fs.open(entry.sourcePath, 'r');
        try {
            const before = await source.stat();
            if (!samePinnedSourceStat(before, entry.sourceStat)) {
                throw new Error(`Backup source changed before hashing: ${entry.backupName}`);
            }
            const digest = nodeCrypto.createHash('sha256');
            const page = Buffer.allocUnsafe(IMPORT_IO_PAGE_BYTES);
            let offset = 0;
            while (offset < before.size) {
                throwIfBackupExportAborted(signal);
                const length = Math.min(page.length, before.size - offset);
                const { bytesRead } = await source.read(page, 0, length, offset);
                if (bytesRead <= 0) break;
                digest.update(page.subarray(0, bytesRead));
                offset += bytesRead;
            }
            const after = await source.stat();
            if (offset !== before.size || !samePinnedSourceStat(after, entry.sourceStat)) {
                throw new Error(`Backup source changed while hashing: ${entry.backupName}`);
            }
            return digest.digest('hex');
        } finally {
            await source.close().catch(() => {});
        }
    }

    async function copyBackupExportFile(entry, destination, signal) {
        const source = await fs.open(entry.sourcePath, 'r');
        let output = null;
        try {
            const before = await source.stat();
            if (!samePinnedSourceStat(before, entry.sourceStat)) {
                throw new Error(`Backup source changed before pinning: ${entry.backupName}`);
            }
            output = await fs.open(destination, 'wx', 0o600);
            const page = Buffer.allocUnsafe(IMPORT_IO_PAGE_BYTES);
            const digest = nodeCrypto.createHash('sha256');
            const pageDelayMs = process.env.NODE_ENV === 'test'
                ? Math.max(0, Number(process.env.POCKETRISU_TEST_FULL_EXPORT_FILE_PAGE_DELAY_MS) || 0)
                : 0;
            let offset = 0;
            while (offset < before.size) {
                throwIfBackupExportAborted(signal);
                const length = Math.min(page.length, before.size - offset);
                const { bytesRead } = await source.read(page, 0, length, offset);
                if (bytesRead === 0) break;
                const chunk = page.subarray(0, bytesRead);
                let written = 0;
                while (written < bytesRead) {
                    throwIfBackupExportAborted(signal);
                    const result = await output.write(
                        chunk,
                        written,
                        bytesRead - written,
                        offset + written,
                    );
                    written += result.bytesWritten;
                }
                digest.update(chunk);
                offset += bytesRead;
                if (pageDelayMs > 0) {
                    await new Promise((resolve) => setTimeout(resolve, pageDelayMs));
                }
            }
            const after = await source.stat();
            const pathAfter = await fs.stat(entry.sourcePath).catch(() => null);
            if (offset !== before.size || !samePinnedSourceStat(after, entry.sourceStat)
                || !pathAfter || !samePinnedSourceStat(pathAfter, entry.sourceStat)) {
                throw new Error(`Backup source changed while pinning: ${entry.backupName}`);
            }
            const pinnedHash = digest.digest('hex');
            const stableHash = await hashBackupExportFile(entry, signal);
            if (pinnedHash !== stableHash) {
                throw new Error(`Backup source content changed while pinning: ${entry.backupName}`);
            }
            await output.sync();
            await output.close();
            output = null;
            return {
                kind: 'file',
                sourcePath: destination,
                backupName: entry.backupName,
                sortKey: entry.sortKey,
                size: offset,
            };
        } finally {
            await output?.close().catch(() => {});
            await source.close().catch(() => {});
        }
    }

    function unsafeLegacyInlayBackupError(key) {
        const error = new Error(
            `Backup cannot safely archive legacy inlay key ${JSON.stringify(key)}; `
            + 'migrate or remove the invalid inlay before retrying',
        );
        error.code = 'BACKUP_UNSAFE_LEGACY_INLAY';
        error.statusCode = 409;
        return error;
    }

    async function planFullBackupFilesystemEntries(snapshot, target) {
        const entries = [];
        for (const asset of listAssetEntriesWithSizes(snapshot)) {
            const backupName = path.basename(asset.key);
            if (asset.source === 'fs') {
                const sourcePath = assetPathFor(assetNameForKey(asset.key));
                const sourceStat = await fs.stat(sourcePath);
                entries.push({
                    kind: 'source-file',
                    sourcePath,
                    sourceStat,
                    backupName,
                    sortKey: asset.key,
                    size: sourceStat.size,
                });
            } else {
                // Preserve the source chosen at the cut. A filesystem file created
                // after this point must never shadow the pinned SQLite row.
                entries.push({
                    kind: 'kv-source',
                    key: asset.key,
                    backupName,
                    sortKey: asset.key,
                    size: asset.size,
                });
            }
        }
        // Original upstream cannot import PocketRisu's slash-named inlay entries.
        // The PocketRisu main rollback target can, so retain them there.
        if (target === 'upstream') return entries;

        const physicalInlays = await listInlayFiles();
        const filesystemPayloadIds = new Set();
        const filesystemSidecarIds = new Set();
        for (const payload of physicalInlays.sort((left, right) => left.id.localeCompare(right.id))) {
            const { id, filePath: sourcePath, ext } = payload;
            const sourceStat = await fs.stat(sourcePath);
            entries.push({
                kind: 'source-file',
                sourcePath,
                sourceStat,
                backupName: toInlayBackupName(id, ext),
                sortKey: `inlay/${id}`,
                size: sourceStat.size,
            });
            filesystemPayloadIds.add(id);

            const sidecar = await readInlaySidecar(id);
            if (!sidecar || normalizeInlayExt(sidecar.ext) !== ext) continue;
            const sidecarPath = await resolveInlaySidecarPath(id);
            if (!sidecarPath) continue;
            try {
                const sidecarStat = await fs.stat(sidecarPath);
                entries.push({
                    kind: 'source-file',
                    sourcePath: sidecarPath,
                    sourceStat: sidecarStat,
                    backupName: `inlay_sidecar/${id}`,
                    sortKey: `inlay_sidecar/${id}`,
                    size: sidecarStat.size,
                });
                filesystemSidecarIds.add(id);
            } catch (error) {
                if (error?.code !== 'ENOENT') throw error;
            }
        }

        const restorableInlayIds = new Set(filesystemPayloadIds);
        for (const row of snapshot.kvListWithSizes('inlay/')) {
            const id = row.key.slice('inlay/'.length);
            if (!isSafeInlayId(id)) throw unsafeLegacyInlayBackupError(row.key);
            restorableInlayIds.add(id);
            if (filesystemPayloadIds.has(id)) continue;
            entries.push({
                kind: 'kv-source',
                key: row.key,
                backupName: row.key,
                sortKey: row.key,
                size: row.size,
            });
        }
        for (const row of snapshot.kvListWithSizes('inlay_info/')) {
            const id = row.key.slice('inlay_info/'.length);
            if (!restorableInlayIds.has(id) || filesystemSidecarIds.has(id)) continue;
            if (!isSafeInlayId(id)) throw unsafeLegacyInlayBackupError(row.key);
            entries.push({
                kind: 'kv-source',
                key: row.key,
                backupName: row.key,
                sortKey: row.key,
                size: row.size,
            });
        }
        return entries;
    }

    function planFullBackupColdStorageEntries(snapshot) {
        const sizes = new Map(
            snapshot.kvListWithSizes('coldstorage/').map((entry) => [entry.key, entry.size]),
        );
        const canonicalKeys = Array.from(new Set(
            [...sizes.keys()].map((key) => normalizeColdStorageStorageKey(key)),
        )).sort((a, b) => a.localeCompare(b));
        return canonicalKeys.map((canonicalKey) => {
            const legacyKey = `${canonicalKey}.json`;
            const key = sizes.has(canonicalKey) ? canonicalKey : legacyKey;
            const sourceSize = sizes.get(key);
            if (!Number.isSafeInteger(sourceSize) || sourceSize < 0) {
                throw new Error(`Cold storage row is unavailable: ${canonicalKey}`);
            }
            const header = sourceSize >= 2 ? snapshot.kvReadRange(key, 0, 2) : Buffer.alloc(0);
            const compressed = header?.[0] === 0x1f && header?.[1] === 0x8b;
            let size = sourceSize;
            if (compressed) {
                if (sourceSize < 18) {
                    throw new Error(`Cold storage gzip row is truncated: ${canonicalKey}`);
                }
                const footer = snapshot.kvReadRange(key, sourceSize - 4, 4);
                size = footer.readUInt32LE(0);
                if (size > BACKUP_IMPORT_MAX_BYTES) {
                    const error = new Error(`Cold storage row exceeds the backup export limit: ${canonicalKey}`);
                    error.code = 'BACKUP_EXPORT_COLD_LIMIT';
                    error.statusCode = 413;
                    throw error;
                }
            }
            return {
                kind: 'cold-source',
                key,
                compressed,
                sourceSize,
                backupName: toColdStorageBackupName(canonicalKey),
                sortKey: toColdStorageBackupName(canonicalKey),
                size,
                // A gzip transform briefly owns both its exact source spool and
                // exact expanded output. Plain rows need only their final pin.
                peakPinBytes: compressed ? sourceSize + size : size,
            };
        });
    }

    function fullExportTestDiskValue(role, suffix) {
        if (process.env.NODE_ENV !== 'test') return null;
        const value = process.env[`POCKETRISU_TEST_FULL_EXPORT_${role}_${suffix}`];
        return typeof value === 'string' && value.length > 0 ? value : null;
    }

    async function resolveFullExportVolume(targetPath, role) {
        const testVolume = fullExportTestDiskValue(role, 'VOLUME');
        const testAvailableRaw = fullExportTestDiskValue(role, 'AVAILABLE_BYTES');
        const testAvailable = testAvailableRaw === null ? NaN : Number(testAvailableRaw);
        let key;
        if (testVolume) {
            key = `test:${testVolume}`;
        } else {
            const stat = await fs.stat(targetPath);
            key = `dev:${stat.dev}`;
        }
        let available;
        if (Number.isSafeInteger(testAvailable) && testAvailable >= 0) {
            available = testAvailable;
        } else {
            try {
                const statfs = await fs.statfs(targetPath);
                available = statfs.bavail * statfs.bsize;
            } catch {
                available = -1;
            }
        }
        return { key, available, targetPath, role };
    }

    async function reserveFullExportDisk(token, requirements) {
        const resolved = await Promise.all(requirements.map(async (requirement) => ({
            ...await resolveFullExportVolume(requirement.targetPath, requirement.role),
            bytes: requirement.bytes,
        })));
        const byVolume = new Map();
        for (const entry of resolved) {
            if (!Number.isSafeInteger(entry.bytes) || entry.bytes < 0) {
                throw new Error('Full backup disk reservation is not a safe byte count');
            }
            const current = byVolume.get(entry.key) ?? {
                key: entry.key,
                bytes: 0,
                available: entry.available,
                roles: [],
                roleBytes: new Map(),
            };
            current.bytes += entry.bytes;
            if (!Number.isSafeInteger(current.bytes)) {
                throw new Error('Full backup disk reservation exceeds the safe integer range');
            }
            if (current.available < 0) current.available = entry.available;
            else if (entry.available >= 0) current.available = Math.min(current.available, entry.available);
            current.roles.push(entry.role);
            current.roleBytes.set(
                entry.role,
                (current.roleBytes.get(entry.role) ?? 0) + entry.bytes,
            );
            byVolume.set(entry.key, current);
        }

        // JavaScript runs this check-and-charge synchronously after all stat calls,
        // so two cuts cannot both spend the same free-space observation.
        for (const entry of byVolume.values()) {
            const existing = fullExportReservedBytesByVolume.get(entry.key) ?? null;
            const reserved = existing?.reserved ?? 0;
            let capacity = existing?.capacity ?? entry.available;
            // statfs already reflects private bytes written by older reservations.
            // Add their whole reservation back before tightening the original
            // capacity, otherwise those bytes are charged once as used space and a
            // second time as reserved space. External consumption can still lower
            // the active ledger conservatively.
            if (entry.available >= 0) {
                const observedCapacity = entry.available + reserved;
                capacity = capacity < 0 ? observedCapacity : Math.min(capacity, observedCapacity);
            }
            entry.capacity = capacity;
            if (capacity >= 0 && capacity - reserved < entry.bytes) {
                const error = new Error(
                    `Insufficient disk space for full backup ${entry.roles.join('+')} reservation`,
                );
                error.code = 'BACKUP_EXPORT_DISK_SPACE';
                error.statusCode = 507;
                error.required = entry.bytes;
                error.available = entry.available;
                error.reserved = reserved;
                error.volume = entry.key;
                error.roles = entry.roles;
                throw error;
            }
        }
        for (const entry of byVolume.values()) {
            const existing = fullExportReservedBytesByVolume.get(entry.key);
            fullExportReservedBytesByVolume.set(entry.key, {
                capacity: entry.capacity,
                reserved: (existing?.reserved ?? 0) + entry.bytes,
            });
        }
        return { token, volumes: [...byVolume.values()] };
    }

    function markFullExportReservationConsumed(reservation, role, bytes) {
        if (!reservation || !Number.isSafeInteger(bytes) || bytes < 0) {
            throw new Error('Invalid committed full backup reservation size');
        }
        const entry = reservation.volumes.find((volume) => volume.roleBytes?.has(role));
        const reservedForRole = entry?.roleBytes?.get(role) ?? -1;
        if (!entry || bytes > reservedForRole) {
            throw new Error(`Committed ${role} bytes exceed the full backup reservation`);
        }
        const ledger = fullExportReservedBytesByVolume.get(entry.key);
        if (ledger?.capacity >= 0) {
            ledger.capacity = Math.max(0, ledger.capacity - bytes);
        }
        entry.committedBytes = (entry.committedBytes ?? 0) + bytes;
    }

    function releaseFullExportDiskReservation(reservation) {
        if (!reservation) return;
        for (const entry of reservation.volumes) {
            const existing = fullExportReservedBytesByVolume.get(entry.key);
            const remaining = Math.max(
                0,
                (existing?.reserved ?? 0) - entry.bytes,
            );
            if (remaining === 0) fullExportReservedBytesByVolume.delete(entry.key);
            else fullExportReservedBytesByVolume.set(entry.key, {
                capacity: existing.capacity,
                reserved: remaining,
            });
        }
        reservation.volumes = [];
    }

    async function waitAtFullExportAfterPinTestGate(signal) {
        if (process.env.NODE_ENV !== 'test') return;
        const configured = String(
            process.env.POCKETRISU_TEST_FULL_EXPORT_AFTER_PIN_GATE_DIR ?? '',
        ).trim();
        if (!configured) return;
        const gateDir = path.resolve(configured);
        const holdPath = path.join(gateDir, 'hold');
        if (!existsSync(holdPath)) return;
        await fs.mkdir(gateDir, { recursive: true });
        await fs.writeFile(path.join(gateDir, 'entered'), 'pinned', 'utf-8');
        const releasePath = path.join(gateDir, 'release');
        while (existsSync(holdPath) && !existsSync(releasePath)) {
            throwIfBackupExportAborted(signal);
            await new Promise((resolve) => setTimeout(resolve, 10));
        }
        throwIfBackupExportAborted(signal);
    }

    async function waitAtFullExportDuringPinTestGate(signal) {
        if (process.env.NODE_ENV !== 'test') return;
        const configured = String(
            process.env.POCKETRISU_TEST_FULL_EXPORT_DURING_PIN_GATE_DIR ?? '',
        ).trim();
        if (!configured) return;
        const gateDir = path.resolve(configured);
        const holdPath = path.join(gateDir, 'hold');
        if (!existsSync(holdPath)) return;
        await fs.mkdir(gateDir, { recursive: true });
        await fs.writeFile(path.join(gateDir, 'entered'), 'pinning', 'utf-8');
        const releasePath = path.join(gateDir, 'release');
        while (existsSync(holdPath) && !existsSync(releasePath)) {
            throwIfBackupExportAborted(signal);
            await new Promise((resolve) => setTimeout(resolve, 10));
        }
        throwIfBackupExportAborted(signal);
    }

    async function waitAtServerBackupBeforePublishTestGate(signal) {
        if (process.env.NODE_ENV !== 'test') return;
        const configured = String(
            process.env.POCKETRISU_TEST_SERVER_BACKUP_BEFORE_PUBLISH_GATE_DIR ?? '',
        ).trim();
        if (!configured) return;
        const gateDir = path.resolve(configured);
        const holdPath = path.join(gateDir, 'hold');
        if (!existsSync(holdPath)) return;
        await fs.mkdir(gateDir, { recursive: true });
        await fs.writeFile(path.join(gateDir, 'entered'), 'ready-to-publish', 'utf-8');
        const releasePath = path.join(gateDir, 'release');
        while (existsSync(holdPath) && !existsSync(releasePath)) {
            throwIfBackupExportAborted(signal);
            await new Promise((resolve) => setTimeout(resolve, 10));
        }
        throwIfBackupExportAborted(signal);
    }

    async function expandPinnedColdStorage(sourcePath, destination, expectedSize, signal) {
        const input = createReadStream(sourcePath, { highWaterMark: IMPORT_IO_PAGE_BYTES });
        const gunzip = zlib.createGunzip({ chunkSize: IMPORT_IO_PAGE_BYTES });
        const inputFinished = finished(input);
        inputFinished.catch(() => {});
        const output = await fs.open(destination, 'wx', 0o600);
        const forwardInputError = (error) => gunzip.destroy(error);
        input.once('error', forwardInputError);
        const abort = () => {
            const reason = signal?.reason instanceof Error ? signal.reason : new Error('Backup export cancelled');
            input.destroy(reason);
            gunzip.destroy(reason);
        };
        signal?.addEventListener('abort', abort, { once: true });
        let size = 0;
        try {
            throwIfBackupExportAborted(signal);
            input.pipe(gunzip);
            for await (const chunk of gunzip) {
                for (let offset = 0; offset < chunk.length; offset += IMPORT_IO_PAGE_BYTES) {
                    throwIfBackupExportAborted(signal);
                    const page = chunk.subarray(
                        offset,
                        Math.min(chunk.length, offset + IMPORT_IO_PAGE_BYTES),
                    );
                    if (size + page.length > expectedSize) {
                        throw new Error('Cold storage gzip expanded beyond its declared size');
                    }
                    let written = 0;
                    while (written < page.length) {
                        const result = await output.write(
                            page,
                            written,
                            page.length - written,
                            size + written,
                        );
                        if (result.bytesWritten <= 0) {
                            throw new Error('Cold storage pin write made no progress');
                        }
                        written += result.bytesWritten;
                    }
                    size += page.length;
                }
            }
            if (size !== expectedSize) {
                throw new Error('Cold storage gzip length does not match its declared size');
            }
            await output.sync();
            await output.close();
            return { size };
        } catch (error) {
            input.destroy();
            gunzip.destroy();
            try { await output.close(); } catch {}
            await fs.unlink(destination).catch(() => {});
            throw error;
        } finally {
            signal?.removeEventListener('abort', abort);
            await inputFinished.catch(() => {});
        }
    }

    async function validatePinnedColdStorage(destination, entry, signal) {
        try {
            if (entry.size <= 8 * 1024 * 1024) {
                await validateJsonFileStreaming(destination, {
                    size: entry.size,
                    maxBytes: BACKUP_IMPORT_MAX_BYTES,
                    signal,
                });
            } else {
                await new Promise((resolve, reject) => {
                    throwIfBackupExportAborted(signal);
                    const worker = spawn(process.execPath, [
                        path.join(__dirname, 'jsonValidateWorker.cjs'),
                        destination,
                        String(entry.size),
                        String(BACKUP_IMPORT_MAX_BYTES),
                    ], {
                        stdio: 'ignore',
                        windowsHide: true,
                    });
                    let settled = false;
                    const finishWorker = (operation, value) => {
                        if (settled) return;
                        settled = true;
                        signal?.removeEventListener('abort', abortWorker);
                        operation(value);
                    };
                    const abortWorker = () => {
                        worker.kill('SIGTERM');
                        finishWorker(
                            reject,
                            signal?.reason instanceof Error
                                ? signal.reason
                                : new Error('Backup export cancelled'),
                        );
                    };
                    signal?.addEventListener('abort', abortWorker, { once: true });
                    worker.once('error', (error) => finishWorker(reject, error));
                    worker.once('exit', (code, signalName) => {
                        if (code === 0) finishWorker(resolve);
                        else {
                            const error = new Error(
                                signalName
                                    ? `Cold storage JSON validator exited on ${signalName}`
                                    : 'Invalid plugin storage JSON row',
                            );
                            error.code = code === 2
                                ? 'INVALID_PLUGIN_STORAGE_ROW'
                                : 'COLD_STORAGE_VALIDATOR_FAILED';
                            finishWorker(reject, error);
                        }
                    });
                    if (signal?.aborted) abortWorker();
                });
            }
        } catch (error) {
            if (error?.code !== 'INVALID_PLUGIN_STORAGE_ROW') throw error;
            const invalid = new Error(`Invalid cold storage JSON row: ${entry.backupName}`);
            invalid.code = 'INVALID_COLD_STORAGE_ROW';
            invalid.statusCode = 400;
            throw invalid;
        }
    }

    function fullExportSnapshotSpoolOptions(signal) {
        const delayMs = process.env.NODE_ENV === 'test'
            ? Math.max(0, Number(process.env.POCKETRISU_TEST_FULL_EXPORT_PAGE_DELAY_MS) || 0)
            : 0;
        return {
            signal,
            onChunk: delayMs > 0
                ? () => new Promise((resolve) => setTimeout(resolve, delayMs))
                : undefined,
        };
    }

    async function pinFullBackupSnapshotEntry(snapshot, entry, destination, signal) {
        if (entry.kind === 'kv-source') {
            const spool = await snapshot.kvWriteToFile(
                entry.key,
                destination,
                fullExportSnapshotSpoolOptions(signal),
            );
            if (!spool || spool.size !== entry.size) {
                throw new Error(`Snapshot row changed while pinning: ${entry.backupName}`);
            }
            return { ...entry, kind: 'file', sourcePath: destination };
        }
        if (entry.kind !== 'cold-source') {
            throw new Error(`Unsupported full backup pin source: ${entry.kind}`);
        }
        if (!entry.compressed) {
            const spool = await snapshot.kvWriteToFile(
                entry.key,
                destination,
                fullExportSnapshotSpoolOptions(signal),
            );
            if (!spool || spool.size !== entry.size) {
                throw new Error(`Cold storage row changed while pinning: ${entry.backupName}`);
            }
            await validatePinnedColdStorage(destination, entry, signal);
            return { ...entry, kind: 'file', sourcePath: destination };
        }
        const rawPath = `${destination}.gz`;
        try {
            const raw = await snapshot.kvWriteToFile(
                entry.key,
                rawPath,
                fullExportSnapshotSpoolOptions(signal),
            );
            if (!raw || raw.size !== entry.sourceSize) {
                throw new Error(`Cold storage gzip changed while pinning: ${entry.backupName}`);
            }
            await expandPinnedColdStorage(rawPath, destination, entry.size, signal);
            await validatePinnedColdStorage(destination, entry, signal);
            return { ...entry, kind: 'file', sourcePath: destination };
        } finally {
            await fs.unlink(rawPath).catch(() => {});
        }
    }

    function estimateFullBackupDatabaseAssemblyBytes(reader, key, physicalSize) {
        if (physicalSize < magicRisuSaveHeader.length) return physicalSize;
        const prefix = reader.kvReadRange(key, 0, magicRisuSaveHeader.length);
        if (!prefix?.equals(Buffer.from(magicRisuSaveHeader))) return physicalSize;
        let offset = magicRisuSaveHeader.length;
        let decodedBytes = 0;
        let blocks = 0;
        while (offset < physicalSize) {
            if (++blocks > 1_000_000 || offset + 7 > physicalSize) {
                throw new Error('Invalid or excessive RisuSave block inventory');
            }
            const header = reader.kvReadRange(key, offset, 3);
            const compression = header[1];
            const nameLength = header[2];
            if (compression !== 0 && compression !== 1) {
                throw new Error('Invalid RisuSave block compression flag');
            }
            offset += 3;
            if (offset + nameLength + 4 > physicalSize) {
                throw new Error('Truncated RisuSave block name');
            }
            offset += nameLength;
            const length = reader.kvReadRange(key, offset, 4).readUInt32LE(0);
            offset += 4;
            if (offset + length > physicalSize) throw new Error('Truncated RisuSave block body');
            let decodedSize = length;
            if (compression === 1) {
                if (length < 18) throw new Error('Truncated gzip RisuSave block');
                decodedSize = reader.kvReadRange(key, offset + length - 4, 4).readUInt32LE(0);
            }
            decodedBytes += decodedSize;
            if (!Number.isSafeInteger(decodedBytes)
                || decodedBytes > 4 * 1024 * 1024 * 1024) {
                throw new Error('RisuSave blocks exceed the bounded decode limit');
            }
            offset += length;
        }
        return decodedBytes;
    }

    function fullBackupDatabaseUnavailableError(cause = null) {
        const error = new Error('The authoritative live database is unavailable for backup', {
            ...(cause ? { cause } : {}),
        });
        error.code = 'BACKUP_DATABASE_UNAVAILABLE';
        error.statusCode = 500;
        return error;
    }

    async function validateFullBackupDatabase(snapshot, key, size, signal) {
        requireDatabaseSpoolDirSync();
        if (!Number.isSafeInteger(size) || size <= 0) {
            throw fullBackupDatabaseUnavailableError();
        }
        const validationPath = path.join(
            getDatabaseSpoolDir(),
            `${DATABASE_SPOOL_FILE_PREFIX}full-export-validation-${process.pid}-${nodeCrypto.randomUUID()}.tmp`,
        );
        try {
            const validationSpool = await snapshot.kvWriteToFile(
                key,
                validationPath,
                fullExportSnapshotSpoolOptions(signal),
            );
            if (!validationSpool || validationSpool.size !== size) {
                throw new Error('Snapshot database changed while validating');
            }
            const databaseSource = { filePath: validationPath, size };
            return await readBackupRisuSaveTopLevelFields(
                databaseSource,
                ['optimizePluginMemory', PLUGIN_STORAGE_GENERATION_FIELD],
                {
                    tempDir: requireDatabaseSpoolDirSync(),
                    signal,
                    shouldAbort: () => signal?.aborted,
                    readRemoteRowSize: (name) => snapshot.kvSize(
                        `remotes/${name}.local.bin`,
                    ),
                    readRemoteRowSource: (name) => spoolBackupSnapshotRow(
                        snapshot,
                        `remotes/${name}.local.bin`,
                        {
                            signal,
                            shouldAbort: () => signal?.aborted,
                        },
                    ),
                },
            );
        } catch (cause) {
            if (signal?.aborted || cause?.name === 'AbortError'
                || cause?.code === 'RISU_STREAM_ABORTED') {
                throw cause;
            }
            if (cause?.code === 'BACKUP_DATABASE_UNAVAILABLE') throw cause;
            throw fullBackupDatabaseUnavailableError(cause);
        } finally {
            await fs.unlink(validationPath).catch(() => {});
        }
    }

    async function pinFullBackupState({ target, signal, archiveTargetPath = null }) {
        throwIfBackupExportAborted(signal);
        if (activeFullExportPins.size >= FULL_EXPORT_MAX_ACTIVE_PINS) {
            throw backupExportCapacityError();
        }
        const token = nodeCrypto.randomUUID();
        activeFullExportPins.add(token);
        let snapshot = null;
        let pinDir = null;
        let reservation = null;
        try {
            return await queueStorageReadAfterImports(async () => {
                throwIfBackupExportAborted(signal);
                await flushPendingDb();
                throwIfBackupExportAborted(signal);
                snapshot = createKvSnapshot();

                const databaseKey = 'database/database.bin';
                let databaseSize;
                let databaseState;
                let databaseAssemblyBytes;
                try {
                    databaseSize = snapshot.kvSize(databaseKey);
                    databaseState = await validateFullBackupDatabase(
                        snapshot,
                        databaseKey,
                        databaseSize,
                        signal,
                    );
                    databaseAssemblyBytes = estimateFullBackupDatabaseAssemblyBytes(
                        snapshot,
                        databaseKey,
                        databaseSize,
                    );
                } catch (cause) {
                    if (signal?.aborted || cause?.name === 'AbortError'
                        || cause?.code === 'RISU_STREAM_ABORTED'
                        || cause?.code === 'BACKUP_DATABASE_UNAVAILABLE') {
                        throw cause;
                    }
                    throw fullBackupDatabaseUnavailableError(cause);
                }
                const filesystemEntries = await planFullBackupFilesystemEntries(snapshot, target);
                const includeInlays = target !== 'upstream';
                const includeServeOnlyRows = target === 'nodeonly';
                const foldPluginStorage = target !== 'nodeonly';
                const baseSnapshotEntries = [
                    ...planFullBackupColdStorageEntries(snapshot),
                    ...(includeInlays ? snapshot.kvListWithSizes('inlay_meta/').map((entry) => ({
                        kind: 'kv-source',
                        key: entry.key,
                        backupName: entry.key,
                        sortKey: entry.key,
                        size: entry.size,
                    })) : []),
                    ...(includeServeOnlyRows ? listMcpToolCallBackupEntries(snapshot) : []),
                    ...(includeServeOnlyRows ? listDraftBackupEntries(snapshot) : []),
                ];
                // Admission is deliberately conservative: reserve every physical
                // plugin candidate before decoding the tiny ownership metadata
                // from the pinned database file. Only owned rows are published.
                const pluginCandidates = foldPluginStorage ? [] : [
                    ...snapshot.kvListWithSizes(PLUGIN_SAVE_PREFIX),
                    ...snapshot.kvListWithSizes(PLUGIN_SAVE_META_PREFIX),
                    ...snapshot.kvListWithSizes(PLUGIN_STORAGE_MANIFEST_KEY),
                ].map((entry) => ({
                    kind: 'kv-source',
                    key: entry.key,
                    backupName: entry.key,
                    sortKey: entry.key,
                    size: entry.size,
                }));
                const reservationEntries = [
                    ...filesystemEntries,
                    ...baseSnapshotEntries,
                    ...pluginCandidates,
                ];
                // Plugin candidates deliberately include quarantined physical rows
                // that may not belong to the selected publication. Reserve their
                // bytes conservatively, but enforce archive limits only after the
                // authoritative manifest selects actual backup entries.
                preflightBackupEntries([
                    { backupName: 'database.risudat', size: databaseSize },
                    ...filesystemEntries,
                    ...baseSnapshotEntries,
                ]);

                const pinPayloadBytes = reservationEntries.reduce(
                    (sum, entry) => sum + (
                        entry.kind === 'cold-source' ? entry.peakPinBytes : entry.size
                    ),
                    databaseSize,
                );
                const remoteCandidates = snapshot.kvListWithSizes('remotes/');
                if (remoteCandidates.length > 1_000_000) {
                    throw new Error('RisuSave REMOTE inventory exceeds the bounded limit');
                }
                const assemblyRows = [
                    ...listLogicalChatRowsWithSizes(snapshot),
                    ...(foldPluginStorage ? snapshot.kvListWithSizes(PLUGIN_SAVE_PREFIX) : []),
                    ...(foldPluginStorage ? snapshot.kvListWithSizes(PLUGIN_SAVE_META_PREFIX) : []),
                    // REMOTE rows are not archive entries. They are private source
                    // spools used while rebuilding database.risudat, so reserve
                    // every physical candidate conservatively before resolving
                    // the bounded pointer graph.
                    ...remoteCandidates,
                ];
                const assemblyBytes = assemblyRows.reduce(
                    (sum, entry) => sum + entry.size + Buffer.byteLength(entry.key, 'utf-8'),
                    databaseAssemblyBytes,
                );
                const pinRequired = pinPayloadBytes * BACKUP_DISK_HEADROOM + 16 * 1024 * 1024;
                // The source-to-source assembler may simultaneously retain one
                // complete intermediate database (or one row pin) and the growing
                // final database spool. Reserve both before either is created.
                const assemblyRequired = assemblyBytes * 2 * BACKUP_DISK_HEADROOM
                    + 16 * 1024 * 1024;
                const archivePayloadUpperBound = archiveTargetPath === null
                    ? 0
                    : reservationEntries.reduce(
                        (sum, entry) => sum
                            + 8
                            + Buffer.byteLength(entry.backupName, 'utf-8')
                            + entry.size,
                        8 + Buffer.byteLength('database.risudat', 'utf-8') + assemblyRequired,
                    );
                const archiveRequired = archiveTargetPath === null
                    ? 0
                    : archivePayloadUpperBound * BACKUP_DISK_HEADROOM + 16 * 1024 * 1024;
                if (!Number.isSafeInteger(pinRequired)
                    || !Number.isSafeInteger(assemblyRequired)
                    || !Number.isSafeInteger(archiveRequired)) {
                    throw new Error('Full backup export size exceeds the safe integer range');
                }
                const requirements = [
                    { targetPath: partialExportSpoolDir, role: 'PIN', bytes: pinRequired },
                    { targetPath: getDatabaseSpoolDir(), role: 'DATABASE', bytes: assemblyRequired },
                ];
                if (archiveTargetPath !== null) {
                    requirements.push({
                        targetPath: archiveTargetPath,
                        role: 'ARCHIVE',
                        bytes: archiveRequired,
                    });
                }
                reservation = await reserveFullExportDisk(token, requirements);

                pinDir = path.join(
                    partialExportSpoolDir,
                    `${FULL_EXPORT_PIN_PREFIX}${process.pid}-${token}`,
                );
                await fs.mkdir(pinDir, { recursive: false, mode: 0o700 });
                await waitAtFullExportDuringPinTestGate(signal);
                const databaseSourcePath = path.join(pinDir, 'database.pin');
                const databasePin = await snapshot.kvWriteToFile(
                    databaseKey,
                    databaseSourcePath,
                    fullExportSnapshotSpoolOptions(signal),
                );
                if (!databasePin || databasePin.size !== databaseSize) {
                    throw fullBackupDatabaseUnavailableError(
                        new Error('Snapshot database changed while pinning'),
                    );
                }
                const databaseSource = { filePath: databaseSourcePath, size: databaseSize };
                const snapshotEntries = [
                    ...baseSnapshotEntries,
                    ...(includeServeOnlyRows
                        ? await listPluginBackupEntries(snapshot, databaseState)
                        : []),
                ];
                const plannedEntries = [...filesystemEntries, ...snapshotEntries]
                    .sort((a, b) => a.sortKey.localeCompare(b.sortKey));
                preflightBackupEntries(plannedEntries);
                const pinnedEntries = [];
                let index = 0;
                for (const entry of plannedEntries) {
                    throwIfBackupExportAborted(signal);
                    if (entry.kind !== 'source-file') {
                        const destination = path.join(pinDir, `${String(index).padStart(8, '0')}.pin`);
                        pinnedEntries.push(await pinFullBackupSnapshotEntry(
                            snapshot,
                            entry,
                            destination,
                            signal,
                        ));
                        index++;
                        continue;
                    }
                    const destination = path.join(pinDir, `${String(index).padStart(8, '0')}.pin`);
                    pinnedEntries.push(await copyBackupExportFile(entry, destination, signal));
                    index++;
                }
                preflightBackupEntries([
                    ...pinnedEntries,
                    { backupName: 'database.risudat', size: databaseSource.size },
                ]);
                return {
                    token,
                    snapshot,
                    databaseSource,
                    databaseState,
                    pinDir,
                    entries: pinnedEntries,
                    reservation,
                    archiveReservedBytes: archiveRequired,
                };
            }, signal);
        } catch (error) {
            try { snapshot?.close(); } catch {}
            if (pinDir) await fs.rm(pinDir, { recursive: true, force: true }).catch(() => {});
            releaseFullExportDiskReservation(reservation);
            activeFullExportPins.delete(token);
            throw error;
        }
    }

    async function cleanupFullBackupState(state) {
        if (!state) return;
        try { state.snapshot?.close(); } catch {}
        await fs.rm(state.pinDir, { recursive: true, force: true }).catch(() => {});
        releaseFullExportDiskReservation(state.reservation);
        activeFullExportPins.delete(state.token);
    }


    function partialBackupAssetKeys(database) {
        const keys = new Set();
        const addPng = (key) => {
            if (typeof key === 'string' && key.endsWith('.png')) keys.add(key);
        };
        for (const character of database?.characters ?? []) addPng(character?.image);
        addPng(database?.userIcon);
        for (const persona of database?.personas ?? []) addPng(persona?.icon);
        addPng(database?.customBackground);
        for (const item of database?.characterOrder ?? []) {
            if (item && typeof item === 'object') {
                addPng(item.img);
                addPng(item.imgFile);
            }
        }
        for (const preset of database?.botPresets ?? []) addPng(preset?.image);
        return keys;
    }

    function listPartialBackupAssetEntries(database, reader) {
        const requested = partialBackupAssetKeys(database);
        const available = new Map(
            listAssetEntriesWithSizes(reader).map(entry => [entry.key, entry]),
        );
        const entries = [];
        let missing = 0;
        for (const key of requested) {
            const entry = available.get(key);
            if (!entry) {
                missing++;
                continue;
            }
            entries.push({
                kind: 'asset',
                key: entry.key,
                backupName: path.basename(entry.key),
                sortKey: entry.key,
                size: entry.size,
                source: entry.source,
                legacyHash: entry.legacyHash,
            });
        }
        entries.sort((a, b) => a.sortKey.localeCompare(b.sortKey));
        return { entries, missing };
    }


    async function copyPartialExportAsset(job, entry, destination) {
        const sourceName = assetNameForKey(entry.key);
        if (!sourceName || !isSafeAssetName(sourceName)) {
            throw new Error(`Invalid partial export asset: ${entry.key}`);
        }
        const source = await fs.open(assetPathFor(sourceName), 'r');
        let output;
        try {
            const before = await source.stat();
            if (!before.isFile() || before.size !== entry.size) {
                throw new Error(`Partial export asset changed before it could be pinned: ${entry.key}`);
            }
            output = await fs.open(destination, 'wx', 0o600);
            const hash = nodeCrypto.createHash('sha256');
            const buffer = Buffer.allocUnsafe(256 * 1024);
            let offset = 0;
            while (true) {
                throwIfPartialExportCancelled(job);
                const { bytesRead } = await source.read(buffer, 0, buffer.length, offset);
                if (bytesRead === 0) break;
                const chunk = buffer.subarray(0, bytesRead);
                hash.update(chunk);
                let written = 0;
                while (written < bytesRead) {
                    const result = await output.write(chunk, written, bytesRead - written, offset + written);
                    written += result.bytesWritten;
                }
                offset += bytesRead;
                job.progress.bytes += bytesRead;
            }
            const after = await source.stat();
            if (
                offset !== before.size
                || after.size !== before.size
                || after.dev !== before.dev
                || after.ino !== before.ino
                || after.mtimeMs !== before.mtimeMs
            ) {
                throw new Error(`Partial export asset changed while it was being pinned: ${entry.key}`);
            }
            await output.sync();
            await output.close();
            output = null;
            const digest = hash.digest('hex');
            const verification = sourceName.match(/^([0-9a-f]{64})\.[A-Za-z0-9]{1,10}$/);
            if (verification && verification[1] !== digest && !entry.legacyHash) {
                throw new Error(`Partial export asset hash mismatch: ${entry.key}`);
            }
            return {
                kind: 'file',
                sourcePath: destination,
                backupName: entry.backupName,
                sortKey: entry.sortKey,
                size: offset,
                sha256: digest,
            };
        } finally {
            await output?.close().catch(() => {});
            await source.close().catch(() => {});
        }
    }

    async function pinPartialExportState(job) {
        return queueStorageReadAfterImports(async () => {
            throwIfPartialExportCancelled(job);
            await flushPendingDb();
            const snapshot = createKvSnapshot();
            job.snapshot = snapshot;
            try {
                const raw = snapshot.kvGet('database/database.bin');
                if (!raw) throw new Error('No database is available to export');
                const strippedDb = await loadStrippedDatabase(raw, 'Partial Backup');
                const database = { ...strippedDb, account: undefined };
                const selected = listPartialBackupAssetEntries(database, snapshot);
                const mcpToolCalls = listMcpToolCallBackupEntries(snapshot, 'kv');
                const drafts = listDraftBackupEntries(snapshot, { database, kind: 'kv' });
                const selectedEntries = [...selected.entries, ...mcpToolCalls, ...drafts]
                    .sort((left, right) => left.sortKey.localeCompare(right.sortKey));
                preflightBackupEntries([
                    { backupName: 'database.risudat', size: raw.length },
                    ...selectedEntries,
                ]);
                const assemblyBytes = [
                    ...listLogicalChatRowsWithSizes(snapshot),
                    ...snapshot.kvListWithSizes(PLUGIN_SAVE_PREFIX),
                    ...snapshot.kvListWithSizes(PLUGIN_SAVE_META_PREFIX),
                ].reduce(
                    (sum, entry) => sum + entry.size + Buffer.byteLength(entry.key, 'utf-8'),
                    raw.length,
                );
                const selectedEntryBytes = selectedEntries.reduce(
                    (sum, entry) => sum + entry.size,
                    0,
                );
                const requiredBytes = (
                    (assemblyBytes + selectedEntryBytes) * BACKUP_DISK_HEADROOM
                    + 16 * 1024 * 1024
                );
                if (!Number.isSafeInteger(requiredBytes)) {
                    throw new Error('Partial export size exceeds the safe integer range');
                }
                const disk = await checkDiskSpace(requiredBytes);
                if (!disk.ok) {
                    const error = new Error(
                        `Insufficient disk space for partial export (requires ${requiredBytes} bytes)`,
                    );
                    error.code = 'ENOSPC';
                    throw error;
                }
                const databaseSpoolDisk = await checkDiskSpace(
                    assemblyBytes + 8 * 1024 * 1024,
                    getDatabaseSpoolDir(),
                );
                if (!databaseSpoolDisk.ok) {
                    const error = new Error(
                        'Insufficient disk space on the configured database spool volume',
                    );
                    error.code = 'ENOSPC';
                    throw error;
                }
                job.missingAssets = selected.missing;
                job.progress.phase = 'pinning-assets';
                job.progress.total = selectedEntries.length + 2;

                const pinnedEntries = [];
                let pinIndex = 0;
                for (const entry of selectedEntries) {
                    throwIfPartialExportCancelled(job);
                    if (entry.source === 'fs') {
                        const destination = path.join(job.pinDir, `${String(pinIndex).padStart(8, '0')}.asset`);
                        pinnedEntries.push(await copyPartialExportAsset(job, entry, destination));
                        pinIndex++;
                    } else {
                        // Preserve the source selected at the snapshot boundary.
                        // A later filesystem file must never shadow this KV row.
                        pinnedEntries.push({ ...entry, kind: 'kv' });
                    }
                    job.progress.current++;
                }
                preflightBackupEntries(pinnedEntries);
                return { snapshot, database, entries: pinnedEntries };
            } catch (error) {
                snapshot.close();
                job.snapshot = null;
                throw error;
            }
        }, job.abortController.signal);
    }

    async function writePartialExportArchive(job, database, entries) {
        job.progress.phase = 'folding-database';
        job.databaseSpool = await spoolSelfContainedBackupDatabase(database, {
            foldPluginStorage: true,
            shouldAbort: () => job.abortController.signal.aborted,
            reader: job.snapshot,
            onMissingChatRow: (chaId, chatId) => {
                warnAndPreserveMissingChatRow('Partial Backup Export', chaId, chatId);
            },
        });
        throwIfPartialExportCancelled(job);
        const selectedEntries = await selectReferencedMcpToolCallEntries(
            entries,
            job.databaseSpool,
            () => job.abortController.signal.aborted,
        );
        preflightBackupEntries([
            ...selectedEntries,
            { backupName: 'database.risudat', size: job.databaseSpool.size },
        ]);
        job.progress.total = selectedEntries.length + 2;
        job.progress.current = selectedEntries.length;

        const output = createWriteStream(job.archiveTempPath, { flags: 'wx', mode: 0o600 });
        try {
            for (const entry of selectedEntries) {
                throwIfPartialExportCancelled(job);
                if (!await writeWithBackpressure(
                    output,
                    encodeBackupEntryHeader(entry.backupName, entry.size),
                    () => job.abortController.signal.aborted,
                )) throwIfPartialExportCancelled(job);
                if (entry.kind === 'file') {
                    if (!await streamFileToWritable(
                        entry.sourcePath,
                        output,
                        () => job.abortController.signal.aborted,
                    )) throwIfPartialExportCancelled(job);
                } else {
                    const value = job.snapshot.kvGet(entry.key);
                    if (value === null || value.length !== entry.size) {
                        throw new Error(`Pinned partial export row is unavailable: ${entry.key}`);
                    }
                    if (!await writeWithBackpressure(
                        output,
                        value,
                        () => job.abortController.signal.aborted,
                    )) throwIfPartialExportCancelled(job);
                }
            }

            job.progress.current++;
            if (!await writeWithBackpressure(
                output,
                encodeBackupEntryHeader('database.risudat', job.databaseSpool.size),
                () => job.abortController.signal.aborted,
            )) throwIfPartialExportCancelled(job);
            if (!await streamFileToWritable(
                job.databaseSpool.filePath,
                output,
                () => job.abortController.signal.aborted,
            )) throwIfPartialExportCancelled(job);
            job.progress.current++;
            output.end();
            await finished(output);
            await fs.rename(job.archiveTempPath, job.archivePath);
        } catch (error) {
            output.destroy();
            await finished(output).catch(() => {});
            throw error;
        }
    }

    async function preparePartialExportJob(job) {
        try {
            job.progress.phase = 'snapshot';
            const pinned = await pinPartialExportState(job);
            job.progress.phase = 'assembling';

            const testDelay = process.env.NODE_ENV === 'test'
                ? Number(process.env.POCKETRISU_TEST_PARTIAL_EXPORT_DELAY_MS ?? 0)
                : 0;
            if (Number.isFinite(testDelay) && testDelay > 0) {
                throwIfPartialExportCancelled(job);
                await new Promise((resolve, reject) => {
                    const timer = setTimeout(resolve, testDelay);
                    job.abortController.signal.addEventListener('abort', () => {
                        clearTimeout(timer);
                        reject(Object.assign(new Error('Partial export was cancelled'), { name: 'AbortError' }));
                    }, { once: true });
                });
            }

            await writePartialExportArchive(job, pinned.database, pinned.entries);
            throwIfPartialExportCancelled(job);
            const stat = await fs.stat(job.archivePath);
            job.size = stat.size;
            job.state = 'ready';
            job.progress.phase = 'ready';
            job.expiresAt = Date.now() + PARTIAL_EXPORT_JOB_TTL_MS;
            try { job.snapshot?.close(); } catch {}
            job.snapshot = null;
            if (job.databaseSpool?.filePath) {
                await fs.unlink(job.databaseSpool.filePath).catch(() => {});
            }
            job.databaseSpool = null;
            await fs.rm(job.pinDir, { recursive: true, force: true }).catch(() => {});
        } catch (error) {
            if (!job.abortController.signal.aborted) {
                logger.error('[Partial Backup Export] Preparation failed:', error);
                job.state = 'failed';
                job.error = error?.message || String(error);
                job.progress.phase = 'failed';
                job.expiresAt = Date.now() + PARTIAL_EXPORT_JOB_TTL_MS;
                await cleanupPartialExportArtifacts(job);
            } else {
                job.state = 'cancelled';
                await cleanupPartialExportJob(job);
            }
        }
    }

    function resolveBackupStorageKey(name) {
        assertBackupEntryNameWithinLimit(name);

        if (name === 'database.risudat') {
            return 'database/database.bin';
        }

        if (
            name.startsWith('inlay_thumb/') ||
            name.startsWith('inlay_meta/')
        ) {
            if (isInvalidBackupPathSegment(name)) {
                throw new Error(`Invalid backup entry name: ${name}`);
            }
            return name;
        }

        if (name.startsWith('inlay/') || name.startsWith(INLAY_ARCHIVE_V2_PREFIX)) {
            const parsed = parseInlayBackupName(name);
            if (!parsed || !isSafeInlayId(parsed.id)) {
                throw importFormatError(
                    `Invalid inlay backup entry name or tuple exceeds the portable limit: ${name}`,
                    'INVALID_INLAY_BACKUP_ENTRY',
                );
            }
            return name;
        }

        if (name.startsWith('inlay_sidecar/')) {
            const parsed = parseInlaySidecarBackupName(name);
            if (!parsed) {
                throw importFormatError(
                    `Invalid inlay sidecar backup entry name or ID exceeds the portable limit: ${name}`,
                    'INVALID_INLAY_BACKUP_ENTRY',
                );
            }
            return name;
        }

        if (name === PLUGIN_STORAGE_MANIFEST_KEY) {
            return PLUGIN_STORAGE_MANIFEST_KEY;
        }

        if (name.startsWith(MCP_TOOL_CALL_CACHE_PREFIX)) {
            if (!parseMcpToolCallStorageKey(name)) {
                throw new Error(`Invalid remembered MCP tool-call entry name: ${name}`);
            }
            return name;
        }

        if (name.startsWith(DRAFT_PREFIX)) {
            if (name.length === DRAFT_PREFIX.length || name.includes('\0')) {
                throw new Error(`Invalid composer draft entry name: ${name}`);
            }
            return name;
        }

        if (
            name.startsWith(PLUGIN_SAVE_PREFIX) ||
            name.startsWith(PLUGIN_SAVE_META_PREFIX)
        ) {
            const prefix = name.startsWith(PLUGIN_SAVE_PREFIX)
                ? PLUGIN_SAVE_PREFIX
                : PLUGIN_SAVE_META_PREFIX;
            if (isHashedPluginSaveStorageKey(name, prefix)) {
                assertArchiveSafePluginSaveStorageKey(name);
            } else {
                decodeValidatedPluginStorageKey(name, prefix);
            }
            return name;
        }

        // Upstream backups transport cold storage as coldstorage/<uuid>.json.
        // Normalize back to the runtime KV key: coldstorage/<uuid>.
        if (name.startsWith('coldstorage/') || COLD_STORAGE_FLAT_NAME_RE.test(name)) {
            return normalizeColdStorageStorageKey(name);
        }

        if (isInvalidBackupPathSegment(name) || name !== path.basename(name)) {
            throw new Error(`Invalid asset backup entry name: ${name}`);
        }

        return `assets/${name}`;
    }

    function validateInlayBackupEntryNameBeforeStaging(name) {
        if (name.startsWith('inlay/')
            || name.startsWith(INLAY_ARCHIVE_V2_PREFIX)
            || name.startsWith('inlay_sidecar/')) {
            resolveBackupStorageKey(name);
            return;
        }
        if (name.startsWith('inlay_info/')) {
            const id = name.slice('inlay_info/'.length);
            if (!isSafeInlayId(id)) {
                throw importFormatError(
                    `Invalid legacy inlay info entry name or ID exceeds the portable limit: ${name}`,
                    'INVALID_INLAY_BACKUP_ENTRY',
                );
            }
        }
    }

    // ─── Shared backup import logic ─────────────────────────────────────────────
    // Accepts any async iterable of Buffer chunks (HTTP request body, file stream, etc.)
    async function importBackupFromSource(dataSource, {
        maxBytes = BACKUP_IMPORT_MAX_BYTES,
        maxEntries = BACKUP_IMPORT_MAX_ENTRIES,
        bufferedEntryMaxBytes = IMPORT_BUFFERED_ENTRY_MAX_BYTES,
        totalBytes = 0,
        onProgress = null,
        signal = null,
    } = {}) {
        maxBytes = finiteByteLimit(maxBytes, BACKUP_IMPORT_MAX_BYTES);
        maxEntries = finiteByteLimit(maxEntries, BACKUP_IMPORT_MAX_ENTRIES);
        bufferedEntryMaxBytes = finiteByteLimit(
            bufferedEntryMaxBytes,
            IMPORT_BUFFERED_ENTRY_MAX_BYTES,
            { max: maxBytes },
        );
        if (totalBytes > 0) assertImportSize(totalBytes, maxBytes, 'Backup archive');
        throwIfImportAborted(signal);
        recoverPendingImportSwap('Backup import preparation');
        let hasDatabase = false;
        let databaseSpool = null;
        let activeEntryWriteStream = null;
        let activeEntryWriteFinished = null;
        let databaseIngestion = null;
        let backupEntryStageDir = null;
        let entryIndex = null;
        let backupEntryIndex = 0;
        const deferredDraftEntries = [];
        let assetsRestored = 0;
        let bytesReceived = 0;
        const existingInlayKeys = (await listInlayFiles()).map((entry) => `inlay/${entry.id}`);
        const existingAssetKeys = listAssetEntriesWithSizes()
            .filter((entry) => entry.source === 'fs')
            .map((entry) => entry.key);

        const stagingDir = path.join(savePath, 'inlays_import_staging');
        const backupInlayDir = path.join(savePath, 'inlays_import_backup');
        recoverPendingImportSwap('Inlay import preparation');
        await fs.rm(stagingDir, { recursive: true, force: true });
        await fs.rm(backupInlayDir, { recursive: true, force: true });
        await fs.mkdir(stagingDir, { recursive: true });
        assertCanonicalInlayNamespace(stagingDir);
        let assetStage;
        try {
            assetStage = await prepareAssetImportStage();
        } catch (error) {
            await fs.rm(stagingDir, { recursive: true, force: true }).catch(() => {});
            await fs.rm(backupInlayDir, { recursive: true, force: true }).catch(() => {});
            throw error;
        }

        function stagingInlayFilePath(id, ext) {
            return canonicalInlayPaths(stagingDir, id, ext).payloadPath;
        }
        function stagingSidecarPath(id) {
            return canonicalInlayPaths(stagingDir, id).sidecarPath;
        }
        function removeObsoleteStagingPayloadsSync(id, destinationPath) {
            const payloadDir = path.join(
                stagingDir,
                INLAY_CANONICAL_ROOT_NAME,
                INLAY_CANONICAL_PAYLOAD_DIR_NAME,
            );
            for (const entryPath of listRegularFilesRecursiveSync(payloadDir)) {
                const parsed = parseCanonicalInlayPayloadPath(entryPath, payloadDir);
                if (parsed?.id === id && entryPath !== destinationPath) unlinkSync(entryPath);
            }
        }
        function writeStagingInlayFileSync(id, ext, buffer, info) {
            const normalizedExt = assertSafeInlayTuple(id, ext);
            assertCanonicalInlayWriteTargets(stagingDir, id, normalizedExt);
            const destinationPath = stagingInlayFilePath(id, normalizedExt);
            writeFileSync(destinationPath, Buffer.from(buffer));
            removeObsoleteStagingPayloadsSync(id, destinationPath);
            const sidecar = {
                ext: normalizedExt,
                name: typeof info?.name === 'string' ? info.name : id,
                type: typeof info?.type === 'string' ? info.type : 'image',
                height: typeof info?.height === 'number' ? info.height : undefined,
                width: typeof info?.width === 'number' ? info.width : undefined,
            };
            writeFileSync(stagingSidecarPath(id), JSON.stringify(sidecar));
        }
        async function writeStagingInlayFileFromSource(id, ext, source, info) {
            const normalizedExt = assertSafeInlayTuple(id, ext);
            assertCanonicalInlayWriteTargets(stagingDir, id, normalizedExt);
            const destinationPath = stagingInlayFilePath(id, normalizedExt);
            await copyFileToSpool(
                source.filePath,
                destinationPath,
                { maxBytes, signal },
            );
            removeObsoleteStagingPayloadsSync(id, destinationPath);
            writeStagingSidecarSync(id, { ...(info || {}), ext: normalizedExt });
        }
        function writeStagingSidecarSync(id, info) {
            const normalizedExt = assertSafeInlayTuple(id, info?.ext);
            assertCanonicalInlayWriteTargets(stagingDir, id);
            const sidecar = {
                ext: normalizedExt,
                name: typeof info?.name === 'string' ? info.name : id,
                type: typeof info?.type === 'string' ? info.type : 'image',
                height: typeof info?.height === 'number' ? info.height : undefined,
                width: typeof info?.width === 'number' ? info.width : undefined,
            };
            writeFileSync(stagingSidecarPath(id), JSON.stringify(sidecar));
        }

        async function importStagedEntry(name, source) {
            const inlayRaw = parseInlayBackupName(name);
            const inlaySidecar = parseInlaySidecarBackupName(name);
            const readBuffered = () => readFileToBufferBounded(source.filePath, {
                size: source.size,
                maxBytes: Math.min(bufferedEntryMaxBytes, maxBytes),
                label: `Backup entry ${name}`,
                code: 'IMPORT_BUFFERED_ENTRY_LIMIT',
                signal,
            });

            if (inlayRaw) {
                const before = entryIndex.getInlay(inlayRaw.id);
                entryIndex.markInlayImported(inlayRaw.id);
                if (inlayRaw.ext) {
                    await writeStagingInlayFileFromSource(
                        inlayRaw.id,
                        inlayRaw.ext,
                        source,
                        before?.legacy || { ext: inlayRaw.ext, name: inlayRaw.id, type: 'image' },
                    );
                } else {
                    const data = await readBuffered();
                    if (data.length > 0 && data[0] === 0x7b) {
                        const parsed = JSON.parse(data.toString('utf-8'));
                        const type = typeof parsed?.type === 'string' ? parsed.type : 'image';
                        const ext = normalizeInlayExt(parsed?.ext);
                        const buffer = type === 'signature'
                            ? Buffer.from(typeof parsed?.data === 'string' ? parsed.data : '', 'utf-8')
                            : decodeDataUri(parsed?.data).buffer;
                        writeStagingInlayFileSync(inlayRaw.id, ext, buffer, before?.legacy || {
                            ext,
                            name: typeof parsed?.name === 'string' ? parsed.name : inlayRaw.id,
                            type,
                            height: typeof parsed?.height === 'number' ? parsed.height : undefined,
                            width: typeof parsed?.width === 'number' ? parsed.width : undefined,
                        });
                    } else {
                        writeStagingInlayFileSync(inlayRaw.id, 'bin', data, before?.legacy || {
                            ext: 'bin',
                            name: inlayRaw.id,
                            type: 'image',
                        });
                    }
                }
                const after = entryIndex.getInlay(inlayRaw.id);
                if (after?.explicit) {
                    writeStagingSidecarSync(inlayRaw.id, after.explicit);
                } else if (!after?.sidecar && after?.legacy) {
                    writeStagingSidecarSync(inlayRaw.id, after.legacy);
                }
                kvClearDeletion(`inlay/${inlayRaw.id}`);
                assetsRestored += 1;
            } else if (inlaySidecar) {
                const data = await readBuffered();
                const parsed = JSON.parse(data.toString('utf-8'));
                entryIndex.markInlaySidecar(inlaySidecar.id, parsed);
                writeStagingSidecarSync(inlaySidecar.id, parsed);
            } else if (name.startsWith('inlay_info/')) {
                const data = await readBuffered();
                const id = name.slice('inlay_info/'.length);
                if (!isSafeInlayId(id)) {
                    throw new Error(`Invalid legacy inlay info entry name: ${name}`);
                }
                const parsed = JSON.parse(data.toString('utf-8'));
                const info = {
                    ext: normalizeInlayExt(parsed?.ext),
                    name: typeof parsed?.name === 'string' ? parsed.name : id,
                    type: typeof parsed?.type === 'string' ? parsed.type : 'image',
                    height: typeof parsed?.height === 'number' ? parsed.height : undefined,
                    width: typeof parsed?.width === 'number' ? parsed.width : undefined,
                };
                entryIndex.setLegacyInlayInfo(id, info);
                const state = entryIndex.getInlay(id);
                if (state?.imported && !state.sidecar) {
                    writeStagingSidecarSync(id, info);
                }
            } else if (name.startsWith('inlay_thumb/')) {
                // Skip deprecated thumbnail entries from legacy backups.
            } else {
                const storageKey = resolveBackupStorageKey(name);
                if (storageKey.startsWith('assets/')) {
                    await writeImportedAssetFromFile(
                        assetStage,
                        storageKey,
                        source,
                        signal,
                        'Backup import',
                        { maxBytes },
                    );
                } else if (storageKey.startsWith(PLUGIN_SAVE_PREFIX)) {
                    await validateAndImportPluginValueFile(
                        storageKey,
                        source,
                        signal,
                        { maxBytes },
                    );
                } else if (storageKey.startsWith('coldstorage/')) {
                    await importColdStorageFromFile(
                        storageKey,
                        source,
                        signal,
                        `Backup entry ${name}`,
                        { maxBytes, bufferedEntryMaxBytes },
                    );
                } else if (storageKey.startsWith(PLUGIN_SAVE_META_PREFIX)) {
                    await validateAndImportPluginMetadataFile(
                        storageKey,
                        source,
                        signal,
                        { maxBytes },
                    );
                } else if (storageKey === PLUGIN_STORAGE_MANIFEST_KEY) {
                    await importOpaqueRowFromFile(storageKey, source, signal);
                } else if (storageKey.startsWith(DRAFT_PREFIX)) {
                    deferredDraftEntries.push({ key: storageKey, ...source });
                    return { retainSource: true };
                } else {
                    await importOpaqueRowFromFile(
                        storageKey,
                        source,
                        signal,
                    );
                }
                assetsRestored += 1;
            }
        }

        throwIfImportAborted(signal);
        await flushPendingDb({ scheduleSnapshot: false });
        throwIfImportAborted(signal);
        await createBackupAndRotate({ storageAlreadyExclusive: true });
        throwIfImportAborted(signal);
        backupEntryStageDir = path.join(
            getDatabaseSpoolDir(),
            `${BACKUP_ENTRY_STAGE_PREFIX}${process.pid}-${nodeCrypto.randomUUID()}`,
        );
        await fs.mkdir(backupEntryStageDir, { recursive: false, mode: 0o700 });
        entryIndex = createBackupImportIndex(path.join(backupEntryStageDir, 'index.sqlite'));

        sqliteDb.pragma('synchronous = OFF');

        let assetSwap = null;
        let inlaySwap = null;
        let journal = null;
        let transactionCommitted = false;
        let databaseSettled = false;
        let importPrimaryError = null;
        try {
            sqliteDb.exec('BEGIN');
            // Prefix deletes can only journal kv rows. Record filesystem-backed
            // logical keys before replacing their directories; imported keys clear
            // their records as they are staged below.
            for (const key of existingAssetKeys) kvRecordDeletion(key);
            for (const key of existingInlayKeys) kvRecordDeletion(key);
            kvDelPrefix('assets/');
            kvDelPrefix('inlay/');
            kvDelPrefix('inlay_thumb/');
            kvDelPrefix('inlay_meta/');
            kvDelPrefix('inlay_info/');
            kvDelPrefix('coldstorage/');
            kvDelPrefix(MCP_TOOL_CALL_CACHE_PREFIX);
            // Chat rows are per-database payloads and are never carried as backup
            // entries; imported database.risudat recreates them before commit.
            for (const key of chatRowStore.listAllChatRowKeys()) kvDel(key);
            // Plugin rows belong to the imported database. New Node-only backups
            // repopulate them as entries below; legacy/upstream backups keep their
            // values folded in database.risudat. Either way, stale rows must go.
            kvDelPrefix(PLUGIN_SAVE_PREFIX);
            kvDelPrefix(PLUGIN_SAVE_META_PREFIX);
            kvDel(PLUGIN_STORAGE_MANIFEST_KEY);
            // Drafts are graph-owned backup rows. Clear the prior dataset now, then
            // restore only entries whose character/chat IDs survive normalization.
            kvDelPrefix(DRAFT_PREFIX);
            // Same reasoning as clearExistingData (save-folder import path): wipe stale
            // remote payloads from the prior user before this backup's contents land.
            // .bin backups never carry REMOTE blocks today, so the migration won't
            // resolveRemote on them — but keeping the two import paths consistent
            // avoids a contamination regression if that ever changes (upstream sync,
            // plugin-generated buffers, etc.).
            kvDelPrefix('remotes/');
            // Allow remote-block migration to re-evaluate against the new database.bin.
            // (.bin backups themselves never carry REMOTE blocks — legacy msgpack
            // format only — but a fresh import is a clear "data changed" signal.)
            kvDel(REMOTE_MIGRATION_MARKER_KEY);
            kvDel(CHAT_EXTERNALIZATION_MARKER_KEY);
            kvDel(CHARACTER_DEFAULTS_MARKER_KEY);
            clearEntities();

            let pending = Buffer.alloc(0);
            let currentEntry = null;
            for await (const sourceChunk of dataSource) {
                throwIfImportAborted(signal);
                const sourceBuffer = Buffer.isBuffer(sourceChunk)
                    ? sourceChunk
                    : Buffer.from(sourceChunk);
                for (let pageOffset = 0; pageOffset < sourceBuffer.length; pageOffset += IMPORT_IO_PAGE_BYTES) {
                    throwIfImportAborted(signal);
                    const chunk = sourceBuffer.subarray(
                        pageOffset,
                        Math.min(sourceBuffer.length, pageOffset + IMPORT_IO_PAGE_BYTES),
                    );
                    const nextBytesReceived = bytesReceived + chunk.length;
                    assertImportSize(nextBytesReceived, maxBytes, 'Backup archive');
                    bytesReceived = nextBytesReceived;
                    if (onProgress) onProgress(bytesReceived, totalBytes);

                    let buffer = pending.length > 0
                        ? Buffer.concat([pending, chunk])
                        : chunk;
                    pending = Buffer.alloc(0);

                    while (buffer.length > 0) {
                        if (!currentEntry) {
                            if (buffer.length < 4) {
                                pending = Buffer.from(buffer);
                                break;
                            }
                            const nameLength = buffer.readUInt32LE(0);
                            if (nameLength > BACKUP_ENTRY_NAME_MAX_BYTES) {
                                throw importFormatError(
                                    `Backup entry name exceeds ${BACKUP_ENTRY_NAME_MAX_BYTES} bytes`,
                                    'INVALID_BACKUP_ENTRY_NAME',
                                );
                            }
                            const headerLength = 4 + nameLength + 4;
                            if (buffer.length < headerLength) {
                                pending = Buffer.from(buffer);
                                break;
                            }

                            const name = buffer.subarray(4, 4 + nameLength).toString('utf-8');
                            const dataLength = buffer.readUInt32LE(4 + nameLength);
                            assertImportSize(dataLength, maxBytes, `Backup entry ${name}`);
                            buffer = buffer.subarray(headerLength);
                            validateInlayBackupEntryNameBeforeStaging(name);

                            if (!entryIndex.addEntry(name)) {
                                throw importFormatError(`Duplicate backup entry: ${name}`, 'DUPLICATE_BACKUP_ENTRY');
                            }
                            if (entryIndex.count > maxEntries) {
                                throw importSizeError(
                                    'Backup entry count',
                                    maxEntries,
                                    entryIndex.count,
                                    'IMPORT_ENTRY_COUNT_LIMIT',
                                );
                            }
                            if (name === 'encryption.risudat') {
                                throw importFormatError(
                                    'Encrypted risuai.xyz account backups cannot be imported. Re-export the backup without account encryption and try again.',
                                    'ENCRYPTED_BACKUP_UNSUPPORTED',
                                );
                            }
                            let filePath;
                            if (name === 'database.risudat') {
                                filePath = path.join(
                                    getDatabaseSpoolDir(),
                                    `${DATABASE_SPOOL_FILE_PREFIX}backup-import-${process.pid}-${nodeCrypto.randomUUID()}.tmp`,
                                );
                                databaseSpool = { filePath, size: dataLength };
                            } else {
                                filePath = path.join(
                                    backupEntryStageDir,
                                    `${String(backupEntryIndex++).padStart(8, '0')}.row`,
                                );
                            }
                            const writeStream = createWriteStream(filePath, {
                                flags: 'wx',
                                mode: 0o600,
                                highWaterMark: IMPORT_IO_PAGE_BYTES,
                            });
                            const writeFinished = finished(writeStream);
                            writeFinished.catch(() => {});
                            activeEntryWriteStream = writeStream;
                            activeEntryWriteFinished = writeFinished;
                            currentEntry = {
                                name,
                                remaining: dataLength,
                                filePath,
                                size: dataLength,
                                writeStream,
                                writeFinished,
                            };
                        }

                        const take = Math.min(currentEntry.remaining, buffer.length);
                        if (take > 0) {
                            const piece = buffer.subarray(0, take);
                            if (!await writeWithBackpressure(currentEntry.writeStream, piece)) {
                                throw new Error('Entry spool closed during backup import');
                            }
                            currentEntry.remaining -= take;
                            buffer = buffer.subarray(take);
                        }

                        if (currentEntry.remaining === 0) {
                            currentEntry.writeStream.end();
                            await currentEntry.writeFinished;
                            activeEntryWriteStream = null;
                            activeEntryWriteFinished = null;
                            if (currentEntry.name === 'database.risudat') {
                                hasDatabase = true;
                            } else {
                                const importResult = await importStagedEntry(currentEntry.name, {
                                    filePath: currentEntry.filePath,
                                    size: currentEntry.size,
                                });
                                if (!importResult?.retainSource) {
                                    await fs.unlink(currentEntry.filePath).catch(() => {});
                                }
                            }
                            currentEntry = null;
                        }
                    }
                }
            }

            if (pending.length > 0 || currentEntry) {
                throw new Error('Backup stream ended with incomplete entry');
            }
            if (!hasDatabase) {
                throw new Error('Backup does not contain database.risudat');
            }

            const databaseSource = {
                filePath: databaseSpool.filePath,
                size: databaseSpool.size,
            };
            throwIfImportAborted(signal);
            const databaseInspection = await inspectRisuSaveSource(databaseSource);
            if (canStreamImportedDatabase(databaseInspection)) {
                databaseIngestion = await ingestImportedDatabaseStreaming(
                    databaseSource,
                    databaseInspection,
                    { signal },
                );
                // Canonical streaming saves have no REMOTE blocks; block saves
                // have already resolved them into the converted database.
                markRemoteMigrationDone();
            } else {
                assertImportSize(
                    databaseSpool.size,
                    LEGACY_DATABASE_IMPORT_MAX_BYTES,
                    'Legacy database',
                    'LEGACY_DATABASE_IMPORT_LIMIT',
                );
                const decoded = await decodeBoundedLegacyRisuSave(databaseSource, {
                    inspection: databaseInspection,
                    tempDir: requireDatabaseSpoolDirSync(),
                    maxLegacyBytes: LEGACY_DATABASE_IMPORT_MAX_BYTES,
                    shouldAbort: () => signal?.aborted === true,
                    signal,
                    resolveRemoteSize: async (name) => kvSize(`remotes/${name}.local.bin`),
                    resolveRemote: async (name) => kvGet(`remotes/${name}.local.bin`),
                });
                databaseIngestion = await ingestDatabase(decoded, {
                    skipLiveRemoteMigration: true,
                });
                markRemoteMigrationDone();
            }
            await restoreImportedDraftEntries(
                deferredDraftEntries,
                databaseIngestion?.strippedDb,
                { signal },
            );
            // Deterministically hold the hardest reader race in compatibility
            // tests: the candidate database is visible inside SQLite's uncommitted
            // replacement transaction, but the import has not published it.
            await waitAtBackupImportTestGate(signal);
            throwIfImportAborted(signal);
            if (backupImportFailpoint === 'after-database-ingestion') {
                throw new ImportIngressError('Backup import was rolled back before publication', {
                    code: 'BACKUP_IMPORT_NOT_COMMITTED',
                    statusCode: 500,
                    retryable: true,
                });
            }
            for (const { id, info } of entryIndex.legacyInlaysMissingSidecars()) {
                writeStagingSidecarSync(id, info);
            }
            writeFileSync(
                path.join(stagingDir, path.basename(inlayMigrationMarker)),
                new Date().toISOString(),
                'utf-8'
            );

            fsyncDirectoryTree(assetImportStagingDir);
            fsyncDirectoryTree(stagingDir);
            journal = {
                id: nodeCrypto.randomUUID(),
                phase: 'swapped',
                dirs: [
                    {
                        liveDir: assetDir,
                        backupDir: assetImportBackupDir,
                        stagingDir: assetImportStagingDir,
                        liveExisted: fsSync.existsSync(assetDir),
                    },
                    {
                        liveDir: inlayDir,
                        backupDir: backupInlayDir,
                        stagingDir,
                        liveExisted: fsSync.existsSync(inlayDir),
                    },
                ],
            };
            kvSet(IMPORT_JOURNAL_MARKER_KEY, Buffer.from(journal.id, 'utf-8'));
            writeImportJournal(IMPORT_JOURNAL_PATH, journal);
            assetSwap = swapAssetDirectoryFromStaging(
                assetImportStagingDir,
                assetImportBackupDir
            );
            inlaySwap = swapDirectoryFromStaging({
                liveDir: inlayDir,
                stagingDir,
                backupDir: backupInlayDir,
            });
            // Publish the new epoch only once the replacement directories and all
            // logical writes are ready. A list served mid-import is then invalidated
            // when this transaction commits.
            kvBumpListEpoch();
            throwIfImportAborted(signal);
            sqliteDb.exec('COMMIT');
            transactionCommitted = true;
            databaseSettled = true;

            applySqliteDurabilityMode();
            runTrackedWalCheckpoint('TRUNCATE', 'backup-import-commit');
            const settlement = settleJournaledImportAssetSwap({
                journal,
                assetSwap,
                transactionCommitted,
                databaseSettled,
                source: 'Backup import finalization',
            });
            journal = settlement.journal;
        } catch (error) {
            importPrimaryError = error;
            if (!transactionCommitted) {
                let rollbackSucceeded = true;
                try {
                    sqliteDb.exec('ROLLBACK');
                    databaseSettled = true;
                } catch (rollbackError) {
                    rollbackSucceeded = false;
                    attachImportSettlementError(error, rollbackError);
                    logger.error('[Backup Import] Failed to roll back SQLite transaction:', rollbackError);
                }
                try {
                    kvBumpListEpoch();
                } catch (epochError) {
                    logger.error('[Backup Import] Failed to bump list epoch after rollback:', epochError);
                }
                if (journal && assetSwap) {
                    const settlement = settleJournaledImportAssetSwap({
                        journal,
                        assetSwap,
                        transactionCommitted,
                        databaseSettled,
                        source: 'Backup import rollback',
                        primaryError: error,
                    });
                    journal = settlement.journal;
                    rollbackSucceeded = rollbackSucceeded && settlement.settled;
                } else {
                    try {
                        await fs.rm(stagingDir, { recursive: true, force: true });
                    } catch (cleanupError) {
                        rollbackSucceeded = false;
                        attachImportSettlementError(error, cleanupError);
                        logger.error('[Backup Import] Failed to remove inlay staging directory:', cleanupError);
                    }
                    try {
                        await fs.rm(assetImportStagingDir, { recursive: true, force: true });
                    } catch (cleanupError) {
                        rollbackSucceeded = false;
                        attachImportSettlementError(error, cleanupError);
                        logger.error('[Backup Import] Failed to remove asset staging directory:', cleanupError);
                    }
                }
                if (error instanceof ImportIngressError && !rollbackSucceeded) {
                    error.commitOutcome = 'unknown';
                    error.commitOutcomeUnknown = true;
                    error.statusCode = 500;
                }
                if (rollbackSucceeded && error && typeof error === 'object') {
                    error.commitOutcome = 'not-committed';
                    error.commitOutcomeUnknown = false;
                }
            } else if (error && typeof error === 'object') {
                error.commitOutcome = 'committed';
                error.commitOutcomeUnknown = false;
            }
            throw error;
        } finally {
            if (assetSwap && !assetSwap.isMaintenanceReleased()) {
                const fallbackError = importPrimaryError
                    ?? new Error('Backup import left its asset swap unsettled');
                const settlement = settleJournaledImportAssetSwap({
                    journal,
                    assetSwap,
                    transactionCommitted,
                    databaseSettled,
                    source: 'Backup import final settlement',
                    primaryError: fallbackError,
                });
                journal = settlement.journal;
            }
            applySqliteDurabilityMode();
            if (activeEntryWriteStream) {
                activeEntryWriteStream.destroy();
                await activeEntryWriteFinished?.catch(() => {});
            }
            if (databaseSpool) {
                await fs.unlink(databaseSpool.filePath).catch(() => {});
            }
            try { entryIndex?.destroy(); } catch (error) {
                logger.warn('[Backup Import] Failed to remove entry index:', error);
            }
            if (backupEntryStageDir) {
                await fs.rm(backupEntryStageDir, { recursive: true, force: true }).catch(() => {});
            }
        }

        invalidateAllDbCaches();

        const coldStorageFailed = databaseIngestion?.stats.failed || 0;

        try {
            runTrackedWalCheckpoint('TRUNCATE', 'backup-import-cleanup');
        } catch (checkpointError) {
            logger.warn('[Backup Import] WAL checkpoint after import failed:', checkpointError);
        }

        console.log(`[Backup Import] Complete: ${assetsRestored} assets restored, ${(bytesReceived / 1024 / 1024).toFixed(1)}MB processed`);
        if (coldStorageFailed > 0) {
            logger.error(`[Backup Import] ${coldStorageFailed} cold storage character(s) could not be restored`);
        }
        return { assetsRestored, bytesReceived, coldStorageFailed };
    }

    function decodeCanonicalHexStorageKey(filename) {
        if (typeof filename !== 'string'
            || filename.length === 0
            || filename.length % 2 !== 0
            || !hexRegex.test(filename)) return null;
        const bytes = Buffer.from(filename, 'hex');
        const key = bytes.toString('utf8');
        if (key.length === 0
            || Buffer.from(key, 'utf8').toString('hex') !== filename.toLowerCase()) return null;
        return key;
    }

    function scanHexFilesInDir(dirPath) {
        let files;
        try {
            files = readdirSync(dirPath);
        } catch {
            return { hexFiles: [], count: 0, totalSize: 0, hasDatabase: false };
        }
        const hexFiles = files.filter(f => decodeCanonicalHexStorageKey(f) !== null);
        let totalSize = 0;
        let hasDatabase = false;
        for (const f of hexFiles) {
            try {
                const stat = fsSync.lstatSync(path.join(dirPath, f));
                if (!stat.isFile() || stat.isSymbolicLink()) continue;
                const nextSize = totalSize + stat.size;
                if (!Number.isSafeInteger(nextSize)) continue;
                totalSize = nextSize;
                if (decodeCanonicalHexStorageKey(f) === DB_BLOB_KEY) hasDatabase = true;
            } catch { /* scan reports only accessible regular files */ }
        }
        return { hexFiles, count: hexFiles.length, totalSize, hasDatabase };
    }

    function clearExistingData() {
        kvDelPrefix('assets/');
        kvDelPrefix('inlay/');
        kvDelPrefix('inlay_thumb/');
        kvDelPrefix('inlay_meta/');
        kvDelPrefix('inlay_info/');
        kvDelPrefix(PLUGIN_SAVE_PREFIX);
        kvDelPrefix(PLUGIN_SAVE_META_PREFIX);
        kvDel(PLUGIN_STORAGE_MANIFEST_KEY);
        kvDelPrefix(MCP_TOOL_CALL_CACHE_PREFIX);
        for (const key of chatRowStore.listAllChatRowKeys()) kvDel(key);
        // Draft rows belong to the imported chat graph. Matching rows from a legacy
        // save folder are restored only after the database has normalized its IDs.
        kvDelPrefix(DRAFT_PREFIX);
        // Drop the previous user's remote payloads. The new save folder usually
        // brings its own remotes/<id>.local.bin files (INSERT OR REPLACE), but if
        // the imported character ids reuse names from the prior user without
        // shipping a matching payload, the migration's resolveRemote would silently
        // stitch in stale cross-user data. Wiping here ensures only payloads
        // that arrived in this import survive.
        kvDelPrefix('remotes/');
        // Cold-storage rows belong to the previous user's chat graph too.
        kvDelPrefix('coldstorage/');
        // Clear remote-block migration marker — newly imported database.bin may
        // contain REMOTE blocks (it usually does, since save-folder imports
        // preserve upstream's split-character format) and we want the migration
        // to re-evaluate against the new contents during post-import ingest.
        kvDel(REMOTE_MIGRATION_MARKER_KEY);
        kvDel(CHAT_EXTERNALIZATION_MARKER_KEY);
        kvDel(CHARACTER_DEFAULTS_MARKER_KEY);
        clearEntities();
    }

    async function importLegacySaveEntries(
        sources,
        missingDatabaseMessage,
        { signal = null, operationId = null } = {},
    ) {
        throwIfImportAborted(signal);
        recoverPendingImportSwap('Save-folder import preparation');
        if (sources.length === 0) return { imported: 0 };
        const databaseEntry = sources.find((entry) => entry.key === DB_BLOB_KEY);
        if (!databaseEntry) {
            throw new Error(missingDatabaseMessage);
        }
        const databaseSource = {
            filePath: databaseEntry.filePath,
            size: databaseEntry.size,
        };
        const databaseInspection = await inspectRisuSaveSource(databaseSource);
        const streamDatabase = canStreamImportedDatabase(databaseInspection);
        if (!streamDatabase) {
            assertImportSize(
                databaseEntry.size,
                LEGACY_DATABASE_IMPORT_MAX_BYTES,
                'Legacy database',
                'LEGACY_DATABASE_IMPORT_LIMIT',
            );
        }
        throwIfImportAborted(signal);
        await flushPendingDb({ scheduleSnapshot: false });
        throwIfImportAborted(signal);
        await createBackupAndRotate({ storageAlreadyExclusive: true });
        throwIfImportAborted(signal);
        invalidateAllDbCaches();
        const existingAssetKeys = listAssetEntriesWithSizes()
            .filter((entry) => entry.source === 'fs')
            .map((entry) => entry.key);
        const assetStage = await prepareAssetImportStage();
        let assetSwap = null;
        let databaseIngestion = null;
        const deferredDraftEntries = [];
        let journal = null;
        let transactionCommitted = false;
        let databaseSettled = false;
        let importPrimaryError = null;

        try {
            sqliteDb.exec('BEGIN');
            for (const key of existingAssetKeys) kvRecordDeletion(key);
            clearExistingData();
            for (const source of sources) {
                throwIfImportAborted(signal);
                const { key } = source;
                if (key === DB_BLOB_KEY && streamDatabase) continue;
                if (key === DB_BLOB_KEY) continue;
                if (key.startsWith(DRAFT_PREFIX)) {
                    deferredDraftEntries.push(source);
                    continue;
                }
                if (key.startsWith('assets/')) {
                    await writeImportedAssetFromFile(
                        assetStage,
                        key,
                        source,
                        signal,
                    );
                    continue;
                }
                if (key.startsWith(PLUGIN_SAVE_PREFIX)) {
                    await validateAndImportPluginValueFile(
                        key,
                        source,
                        signal,
                    );
                    continue;
                }
                if (key.startsWith(PLUGIN_SAVE_META_PREFIX)) {
                    await validateAndImportPluginMetadataFile(
                        key,
                        source,
                        signal,
                    );
                } else if (key === PLUGIN_STORAGE_MANIFEST_KEY) {
                    await importOpaqueRowFromFile(key, source, signal);
                } else {
                    await importOpaqueRowFromFile(
                        key,
                        source,
                        signal,
                    );
                }
            }

            if (streamDatabase) {
                databaseIngestion = await ingestImportedDatabaseStreaming(
                    databaseSource,
                    databaseInspection,
                    { signal },
                );
                markRemoteMigrationDone();
            } else {
                const decoded = await decodeBoundedLegacyRisuSave(databaseSource, {
                    inspection: databaseInspection,
                    tempDir: requireDatabaseSpoolDirSync(),
                    maxLegacyBytes: LEGACY_DATABASE_IMPORT_MAX_BYTES,
                    shouldAbort: () => signal?.aborted === true,
                    signal,
                    resolveRemoteSize: async (name) => kvSize(`remotes/${name}.local.bin`),
                    resolveRemote: async (name) => kvGet(`remotes/${name}.local.bin`),
                });
                databaseIngestion = await ingestDatabase(decoded, {
                    skipLiveRemoteMigration: true,
                });
                markRemoteMigrationDone();
            }

            await restoreImportedDraftEntries(
                deferredDraftEntries,
                databaseIngestion?.strippedDb,
                { signal },
            );

            throwIfImportAborted(signal);
            fsyncDirectoryTree(assetImportStagingDir);
            journal = {
                id: nodeCrypto.randomUUID(),
                phase: 'swapped',
                dirs: [{
                    liveDir: assetDir,
                    backupDir: assetImportBackupDir,
                    stagingDir: assetImportStagingDir,
                    liveExisted: fsSync.existsSync(assetDir),
                }],
            };
            kvSet(IMPORT_JOURNAL_MARKER_KEY, Buffer.from(journal.id, 'utf-8'));
            writeImportJournal(IMPORT_JOURNAL_PATH, journal);
            assetSwap = swapAssetDirectoryFromStaging(
                assetImportStagingDir,
                assetImportBackupDir
            );
            kvBumpListEpoch();
            throwIfImportAborted(signal);
            if (hasSaveFolderImportFailpoint('after-asset-swap')) {
                throw new ImportIngressError('Save-folder import was rolled back before publication', {
                    code: 'SAVE_FOLDER_IMPORT_NOT_COMMITTED',
                    statusCode: 500,
                    retryable: true,
                });
            }
            // Bind the legacy-file migration completion signal to the same SQLite
            // commit as every imported row. The filesystem marker published after
            // commit is retained only for rollback and UI compatibility.
            markLegacyHexMigrationComplete(sources.length);
            setReplacementOperationOutcome(operationId, 'committed', {
                result: { ok: true, imported: sources.length },
            });
            sqliteDb.exec('COMMIT');
            transactionCommitted = true;
            databaseSettled = true;

            runTrackedWalCheckpoint('TRUNCATE', 'save-folder-import');
            if (hasSaveFolderImportFailpoint('post-commit-cleanup')) {
                const failure = new Error('Injected save-folder cleanup failure after commit');
                failure.code = 'SAVE_FOLDER_IMPORT_POST_COMMIT_CLEANUP_FAILED';
                throw failure;
            }
            const settlement = settleJournaledImportAssetSwap({
                journal,
                assetSwap,
                transactionCommitted,
                databaseSettled,
                source: 'Save-folder import finalization',
            });
            journal = settlement.journal;
            if (hasSaveFolderImportFailpoint('migration-marker')) {
                const failure = new Error('Injected save-folder migration-marker failure after commit');
                failure.code = 'SAVE_FOLDER_IMPORT_MIGRATION_MARKER_FAILED';
                throw failure;
            }
            publishLegacyHexMigrationMarker();
        } catch (error) {
            importPrimaryError = error;
            if (!transactionCommitted) {
                let rollbackSucceeded = true;
                try {
                    sqliteDb.exec('ROLLBACK');
                    databaseSettled = true;
                } catch (rollbackError) {
                    rollbackSucceeded = false;
                    attachImportSettlementError(error, rollbackError);
                    logger.error('[Save-folder Import] Failed to roll back SQLite transaction:', rollbackError);
                }
                try {
                    kvBumpListEpoch();
                } catch (epochError) {
                    logger.error('[Save-folder Import] Failed to bump list epoch after rollback:', epochError);
                }
                if (journal && assetSwap) {
                    const settlement = settleJournaledImportAssetSwap({
                        journal,
                        assetSwap,
                        transactionCommitted,
                        databaseSettled,
                        source: 'Save-folder import rollback',
                        primaryError: error,
                        beforeRecovery() {
                            if (hasSaveFolderImportFailpoint('rollback-cleanup')) {
                                throw new Error('Injected save-folder asset rollback failure');
                            }
                        },
                    });
                    journal = settlement.journal;
                    rollbackSucceeded = rollbackSucceeded && settlement.settled;
                } else {
                    try {
                        if (hasSaveFolderImportFailpoint('rollback-cleanup')) {
                            throw new Error('Injected save-folder staging cleanup failure');
                        }
                        await fs.rm(assetImportStagingDir, { recursive: true, force: true });
                    } catch (cleanupError) {
                        rollbackSucceeded = false;
                        attachImportSettlementError(error, cleanupError);
                        logger.error('[Save-folder Import] Failed to remove asset staging directory:', cleanupError);
                    }
                }
                if (!rollbackSucceeded && error && typeof error === 'object') {
                    error.commitOutcome = 'unknown';
                    error.commitOutcomeUnknown = true;
                    error.statusCode = 500;
                    error.retryable = false;
                }
                if (rollbackSucceeded && error && typeof error === 'object') {
                    error.commitOutcome = 'not-committed';
                    error.commitOutcomeUnknown = false;
                }
            } else if (error && typeof error === 'object') {
                error.commitOutcome = 'committed';
                error.commitOutcomeUnknown = false;
                error.statusCode = 500;
                error.retryable = false;
            }
            throw error;
        } finally {
            if (assetSwap && !assetSwap.isMaintenanceReleased()) {
                const fallbackError = importPrimaryError
                    ?? new Error('Save-folder import left its asset swap unsettled');
                const settlement = settleJournaledImportAssetSwap({
                    journal,
                    assetSwap,
                    transactionCommitted,
                    databaseSettled,
                    source: 'Save-folder import final settlement',
                    primaryError: fallbackError,
                    beforeRecovery() {
                        if (!transactionCommitted
                            && hasSaveFolderImportFailpoint('rollback-cleanup')) {
                            throw new Error('Injected save-folder asset rollback failure');
                        }
                    },
                });
                journal = settlement.journal;
            }
        }

        return { imported: sources.length };
    }

    function createSaveFolderImportStage() {
        requireDatabaseSpoolDirSync();
        const stageDir = path.join(
            getDatabaseSpoolDir(),
            `${SAVE_FOLDER_IMPORT_STAGE_PREFIX}${process.pid}-${nodeCrypto.randomUUID()}`,
        );
        mkdirSync(stageDir, { recursive: false, mode: 0o700 });
        return stageDir;
    }

    async function stageHexFilesFromDir(dirPath, stageDir, { signal = null } = {}) {
        const keys = new Set();
        const sources = [];
        let stagedBytes = 0;
        const directory = await fs.opendir(dirPath);
        try {
            for await (const entry of directory) {
                throwIfImportAborted(signal);
                const key = decodeCanonicalHexStorageKey(entry.name);
                if (key === null) continue;
                const nextCount = sources.length + 1;
                if (nextCount > SAVE_FOLDER_IMPORT_MAX_ENTRIES) {
                    throw importSizeError(
                        'Save-folder entry count',
                        SAVE_FOLDER_IMPORT_MAX_ENTRIES,
                        nextCount,
                        'IMPORT_ENTRY_COUNT_LIMIT',
                    );
                }
                if (!entry.isFile()) {
                    throw importFormatError(`Save-folder entry is not a regular file: ${entry.name}`, 'INVALID_SAVE_FOLDER_ENTRY');
                }
                if (keys.has(key)) {
                    throw importFormatError(`Duplicate save-folder entry: ${key}`, 'DUPLICATE_SAVE_FOLDER_ENTRY');
                }
                keys.add(key);
                const sourcePath = path.join(dirPath, entry.name);
                const stat = await fs.lstat(sourcePath);
                if (!stat.isFile() || stat.isSymbolicLink()) {
                    throw importFormatError(`Save-folder entry is not a regular file: ${entry.name}`, 'INVALID_SAVE_FOLDER_ENTRY');
                }
                const nextBytes = stagedBytes + stat.size;
                assertImportSize(nextBytes, BACKUP_IMPORT_MAX_BYTES, 'Save folder');
                const destination = path.join(stageDir, `${String(sources.length).padStart(8, '0')}.row`);
                const staged = await copyFileToSpool(sourcePath, destination, {
                    expectedStat: stat,
                    maxBytes: BACKUP_IMPORT_MAX_BYTES,
                    signal,
                });
                const actualNextBytes = stagedBytes + staged.size;
                assertImportSize(actualNextBytes, BACKUP_IMPORT_MAX_BYTES, 'Save folder');
                stagedBytes = actualNextBytes;
                sources.push({ key, filePath: staged.filePath, size: staged.size });
            }
        } finally {
            await directory.close().catch((error) => {
                if (error?.code !== 'ERR_DIR_CLOSED') throw error;
            });
        }
        if (sources.length === 0) return [];
        if (!keys.has(DB_BLOB_KEY)) {
            throw importFormatError(
                'Save folder does not contain database/database.bin',
                'SAVE_FOLDER_DATABASE_MISSING',
            );
        }
        await assertImportDiskSpace(stagedBytes);
        return sources;
    }

    async function importHexFilesFromDir(dirPath, options = {}) {
        const stageDir = createSaveFolderImportStage();
        try {
            const sources = await stageHexFilesFromDir(dirPath, stageDir, options);
            if (sources.length === 0) return { imported: 0 };
            return await importLegacySaveEntries(
                sources,
                'Save folder does not contain database/database.bin',
                options,
            );
        } finally {
            await fs.rm(stageDir, { recursive: true, force: true }).catch(() => {});
        }
    }

    function sendSaveFolderImportFailure(res, error, diagnostic, { ndjson = false } = {}) {
        const annotated = authoritativeImportErrorPayload(
            error,
            'SAVE_FOLDER_IMPORT_FAILED',
        );
        // A committed or unknown transaction outcome always outranks legacy
        // validation response shapes. Those shapes are safe only after a known
        // rollback; otherwise they would falsely invite the client to replay a
        // replacement whose publication could not be recovered conclusively.
        const preserveLegacyValidation = annotated === null
            || annotated.commitOutcome === 'not-committed';
        if (!ndjson && preserveLegacyValidation && diagnostic && !res.headersSent) {
            res.status(400).json(diagnostic);
            return true;
        }
        if (!ndjson
            && preserveLegacyValidation
            && error?.risuSavePreparationInvalid === true
            && !res.headersSent) {
            res.status(400).json({ error: error.message });
            return true;
        }
        return sendImportIngressError(res, error, {
            ndjson,
            fallbackCode: 'SAVE_FOLDER_IMPORT_FAILED',
            includeAnnotatedOutcome: true,
        });
    }


    function registerBackupExportRoutes(app) {
        app.post('/api/backup/export/jobs', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            try {
                if (req.body?.scope !== 'partial') {
                    res.status(400).json({ error: 'Only partial export jobs are supported' });
                    return;
                }
                const owner = partialExportOwner(req);
                const requestedId = req.body?.jobId;
                if (typeof requestedId !== 'string' || !PLUGIN_STORAGE_UUID_PATTERN.test(requestedId)) {
                    res.status(400).json({ error: 'Partial export jobId must be a canonical UUID' });
                    return;
                }
                const testCreateDelay = process.env.NODE_ENV === 'test'
                    ? Number(process.env.POCKETRISU_TEST_PARTIAL_EXPORT_CREATE_DELAY_MS ?? 0)
                    : 0;
                if (Number.isFinite(testCreateDelay) && testCreateDelay > 0) {
                    await new Promise(resolve => setTimeout(resolve, testCreateDelay));
                }
                if (wasPartialExportCancelled(owner, requestedId)) {
                    res.status(409).json({
                        error: 'Partial export job was cancelled before creation',
                        state: 'cancelled',
                    });
                    return;
                }
                const existingById = partialExportJobs.get(requestedId);
                if (existingById) {
                    if (existingById.owner !== owner) {
                        res.status(409).json({ error: 'Partial export jobId is already in use' });
                        return;
                    }
                    res.status(202).json({ jobId: existingById.id, state: existingById.state });
                    return;
                }
                const existingForOwner = [...partialExportJobs.values()].find(job => job.owner === owner);
                if (existingForOwner) {
                    res.status(409).json({
                        error: 'A partial export job is already active for this session',
                        jobId: existingForOwner.id,
                        state: existingForOwner.state,
                    });
                    return;
                }
                if (partialExportJobs.size >= PARTIAL_EXPORT_MAX_ACTIVE_JOBS) {
                    res.status(429).json({
                        error: 'Too many partial export jobs are active',
                        retryable: true,
                    });
                    return;
                }
                const id = requestedId;
                const spoolDir = path.join(partialExportSpoolDir, `${PARTIAL_EXPORT_JOB_PREFIX}${id}`);
                const pinDir = path.join(spoolDir, 'assets');
                const job = {
                    id,
                    owner,
                    state: 'creating',
                    createdAt: Date.now(),
                    expiresAt: Date.now() + PARTIAL_EXPORT_JOB_TTL_MS,
                    abortController: new AbortController(),
                    spoolDir,
                    pinDir,
                    archiveTempPath: path.join(spoolDir, 'partial-backup.bin.tmp'),
                    archivePath: path.join(spoolDir, 'partial-backup.bin'),
                    filename: `risu-backup-${Date.now()}-partial.bin`,
                    snapshot: null,
                    databaseSpool: null,
                    missingAssets: 0,
                    size: 0,
                    error: null,
                    cleaned: false,
                    progress: { phase: 'queued', current: 0, total: 0, bytes: 0 },
                };
                // Reserve identity, owner admission, and the sole disk budget before
                // the first await. Duplicate creates, concurrent creates, and DELETE
                // now observe this job even while its directory is being created.
                partialExportJobs.set(id, job);
                try {
                    await fs.mkdir(pinDir, { recursive: true, mode: 0o700 });
                } catch (error) {
                    await cleanupPartialExportJob(job);
                    throw error;
                }
                if (job.cleaned || job.abortController.signal.aborted) {
                    await cleanupPartialExportArtifacts(job);
                    if (!res.headersSent) {
                        res.status(409).json({ error: 'Partial export job was cancelled during creation' });
                    }
                    return;
                }
                job.state = 'preparing';
                res.status(202).json({ jobId: id, state: job.state });
                job.preparation = Promise.resolve().then(() => preparePartialExportJob(job));
            } catch (error) {
                next(error);
            }
        });

        app.get('/api/backup/export/jobs/:jobId', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            try {
                const job = partialExportJobForRequest(req, res);
                if (!job) return;
                res.setHeader('cache-control', 'no-store');
                res.json({
                    jobId: job.id,
                    state: job.state,
                    phase: job.progress.phase,
                    current: job.progress.current,
                    total: job.progress.total,
                    bytes: job.progress.bytes,
                    size: job.state === 'ready' ? job.size : undefined,
                    missingAssets: job.missingAssets,
                    error: job.state === 'failed' ? job.error : undefined,
                });
            } catch (error) {
                next(error);
            }
        });

        app.delete('/api/backup/export/jobs/:jobId', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            try {
                const id = req.params.jobId;
                if (!PLUGIN_STORAGE_UUID_PATTERN.test(id)) {
                    res.status(404).json({ error: 'Partial export job not found' });
                    return;
                }
                const owner = partialExportOwner(req);
                const job = partialExportJobs.get(id);
                if (job && job.owner !== owner) {
                    res.status(404).json({ error: 'Partial export job not found' });
                    return;
                }
                recordPartialExportCancellation(owner, id);
                if (!job) {
                    res.status(202).json({ ok: true, state: 'cancelled' });
                    return;
                }
                job.state = 'cancelled';
                job.abortController.abort();
                partialExportJobs.delete(job.id);
                if (!job.preparation || job.progress.phase === 'ready' || job.progress.phase === 'failed') {
                    await cleanupPartialExportJob(job);
                }
                res.status(202).json({ ok: true, state: 'cancelled' });
            } catch (error) {
                next(error);
            }
        });

        app.get('/api/backup/export/jobs/:jobId/download', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            let job;
            let closed = false;
            let consuming = false;
            let onJobAbort = null;
            try {
                job = partialExportJobForRequest(req, res);
                if (!job) return;
                if (job.state !== 'ready') {
                    res.status(409).json({ error: 'Partial export is not ready', state: job.state });
                    return;
                }
                consuming = true;
                job.state = 'streaming';
                job.progress.phase = 'streaming';
                res.once('close', () => { closed = true; });
                onJobAbort = () => {
                    closed = true;
                    if (!res.destroyed) res.destroy();
                };
                job.abortController.signal.addEventListener('abort', onJobAbort, { once: true });
                res.setHeader('cache-control', 'no-store');
                res.setHeader('content-type', 'application/octet-stream');
                res.setHeader('content-disposition', `attachment; filename="${job.filename}"`);
                res.setHeader('content-length', job.size);
                res.setHeader('x-risu-backup-assets', Math.max(0, job.progress.total - 2));
                res.setHeader('x-risu-backup-missing-assets', job.missingAssets);
                if (process.env.NODE_ENV === 'test'
                    && process.env.POCKETRISU_TEST_PARTIAL_EXPORT_STALL_DOWNLOAD === '1') {
                    // Deterministically hold a response after headers and a real
                    // archive chunk have entered the streaming path. TTL must wake
                    // this wait by aborting the job and destroying the response.
                    const archive = await fs.open(job.archivePath, 'r');
                    try {
                        const firstChunk = Buffer.allocUnsafe(Math.min(job.size, 64 * 1024));
                        const { bytesRead } = await archive.read(firstChunk, 0, firstChunk.length, 0);
                        if (bytesRead > 0) {
                            await writeWithBackpressure(
                                res,
                                firstChunk.subarray(0, bytesRead),
                                () => closed || job.abortController.signal.aborted,
                            );
                        }
                    } finally {
                        await archive.close();
                    }
                    if (!job.abortController.signal.aborted) {
                        await new Promise(resolve => {
                            job.abortController.signal.addEventListener('abort', resolve, { once: true });
                        });
                    }
                    return;
                }
                if (!await streamFileToWritable(
                    job.archivePath,
                    res,
                    () => closed || job.abortController.signal.aborted,
                )) return;
                if (!closed) res.end();
            } catch (error) {
                if (!closed && !res.headersSent) next(error);
                else if (!closed) res.destroy(error);
            } finally {
                if (job && onJobAbort) {
                    job.abortController.signal.removeEventListener('abort', onJobAbort);
                }
                if (job && consuming) await cleanupPartialExportJob(job);
            }
        });

        app.get('/api/backup/export', async (req, res, next) => {
            if(!await checkAuth(req, res)){ return; }
            if (req.query.scope === 'partial') {
                res.status(409).json({
                    error: 'Partial exports use the cancellable export-job protocol',
                    code: 'PARTIAL_EXPORT_JOB_REQUIRED',
                    create: '/api/backup/export/jobs',
                });
                return;
            }
            const abortTracker = createBackupExportAbortTracker(req, res);
            let backupDbSpool = null;
            let pinnedState = null;
            const shouldAbort = () => abortTracker.signal.aborted || res.destroyed;
            try {
                const requestedTarget = req.query.target;
                if (requestedTarget !== undefined
                    && requestedTarget !== 'nodeonly'
                    && requestedTarget !== 'upstream'
                    && requestedTarget !== 'main') {
                    res.status(400).json({
                        error: 'Unsupported backup export target',
                        code: 'BACKUP_EXPORT_TARGET_INVALID',
                    });
                    return;
                }
                // upstream excludes slash-named PocketRisu namespaces its importer
                // rejects. main is a separate downgrade contract: it folds optimized
                // plugin rows and omits serve-only drafts/MCP rows, while retaining the
                // inlay namespaces that the PocketRisu main importer understands.
                const target = requestedTarget ?? 'nodeonly';
                const foldPluginStorage = target !== 'nodeonly';
                pinnedState = await pinFullBackupState({
                    target,
                    signal: abortTracker.signal,
                });
                await waitAtFullExportAfterPinTestGate(abortTracker.signal);
                throwIfBackupExportAborted(abortTracker.signal);
                backupDbSpool = await buildSelfContainedBackupDatabase({
                    foldPluginStorage,
                    shouldAbort,
                    snapshot: pinnedState.snapshot,
                    databaseSource: pinnedState.databaseSource,
                    databaseState: pinnedState.databaseState,
                    signal: abortTracker.signal,
                });
                if (target === 'main' || target === 'upstream') {
                    await requireTargetCompatibleBackupDatabase(backupDbSpool, target);
                }
                throwIfBackupExportAborted(abortTracker.signal);
                const namespacedEntries = target === 'nodeonly'
                    ? await selectReferencedMcpToolCallEntries(
                        pinnedState.entries,
                        backupDbSpool,
                        shouldAbort,
                    )
                    : pinnedState.entries;
                const dbSize = backupDbSpool?.size ?? 0;
                preflightBackupEntries([
                    ...namespacedEntries,
                    ...(dbSize ? [{ backupName: 'database.risudat', size: dbSize }] : []),
                ]);
                const totalBytes = namespacedEntries.reduce((sum, entry) => {
                    return sum + backupEntrySize(entry.backupName, entry.size);
                }, 0) + (dbSize ? backupEntrySize('database.risudat', dbSize) : 0);

                const filenameSuffix = target === 'nodeonly' ? '' : `-${target}`;
                res.setHeader('content-type', 'application/octet-stream');
                res.setHeader('content-disposition', `attachment; filename="risu-backup-${Date.now()}${filenameSuffix}.bin"`);
                res.setHeader('content-length', totalBytes);
                res.setHeader('x-risu-backup-assets', namespacedEntries.length);
                res.setHeader('x-risu-backup-target', target);
                if (target === 'main') {
                    res.setHeader('x-risu-backup-omitted', 'drafts,remembered-mcp-tool-calls');
                }

                for (const entry of namespacedEntries) {
                    throwIfBackupExportAborted(abortTracker.signal);
                    if (!await writePinnedBackupEntry(
                        res,
                        entry,
                        shouldAbort,
                    )) break;
                }

                if (!shouldAbort() && dbSize && backupDbSpool) {
                    const header = encodeBackupEntryHeader('database.risudat', dbSize);
                    if (await writeWithBackpressure(res, header, shouldAbort)) {
                        await streamFileToWritable(backupDbSpool.filePath, res, shouldAbort);
                    }
                }
                if (!shouldAbort()) res.end();
            } catch (error) {
                if (abortTracker.signal.aborted || res.destroyed) {
                    return;
                } else if (error?.code === 'BACKUP_MISSING_CHAT_ROW') {
                    logger.error('[Backup Export] Failed:', error);
                    res.status(500).json({ error: error.message, code: error.code });
                } else if (!res.headersSent && error?.statusCode) {
                    res.status(error.statusCode).json(backupExportErrorPayload(error));
                } else {
                    next(error);
                }
            } finally {
                abortTracker.cleanup();
                if (backupDbSpool) {
                    await fs.unlink(backupDbSpool.filePath).catch(() => {});
                }
                await cleanupFullBackupState(pinnedState);
            }
        });

    }

    function registerBackupImportRoutes(app) {
        // Pre-flight check: auth + size + disk space before client starts uploading
        app.post('/api/backup/import/prepare', async (req, res, next) => {
            if (!await checkAuth(req, res)) { return; }
            if (!checkActiveSession(req, res)) return;
            try {
                if (getImportInProgress()) {
                    res.status(409).json({ error: 'Another import is already in progress' });
                    return;
                }

                const size = Number(req.body?.size ?? 0);
                if (!Number.isSafeInteger(size) || size < 0) {
                    throw importFormatError('Backup has an invalid byte length', 'INVALID_IMPORT_SIZE');
                }
                const limits = backupImportLimits({
                    allowLargeRestore: req.body?.allowLargeRestore === true,
                });
                assertImportSize(size, limits.maxBytes, 'Backup archive');

                if (size > 0) {
                    await assertImportDiskSpace(size);
                }

                res.json({ ok: true });
            } catch (error) {
                if (!sendImportIngressError(res, error)) next(error);
            }
        });

        app.post('/api/backup/import', async (req, res, next) => {
            if(!await checkAuth(req, res)){ return; }
            if (!checkActiveSession(req, res)) return;
            const abortTracker = createImportAbortTracker(req, res);
            let ownsImportSlot = false;
            let releaseImportBarrier = null;
            let prevRequestTimeout;
            let wantsNdjson = false;
            let heartbeatTimer = null;
            let uploadSpool = null;
            let uploadStream = null;
            const limits = backupImportLimits({
                allowLargeRestore: requestConfirmsLargeRestore(req),
            });

            try {
                if (getImportInProgress()) {
                    res.status(409).json({ error: 'Another import is already in progress' });
                    return;
                }
                setImportInProgress(true);
                ownsImportSlot = true;
                releaseImportBarrier = await importBarrier.acquire(abortTracker.signal);
                throwIfImportAborted(abortTracker.signal);

                // Disable timeouts for large backup uploads
                prevRequestTimeout = req.socket.server?.requestTimeout;
                req.socket.setTimeout(0);
                req.socket.setKeepAlive(true);
                if (req.socket.server) req.socket.server.requestTimeout = 0;

                // NDJSON streaming keeps the response socket alive during long
                // post-upload work (WAL checkpoint, cold-storage migration). Without it
                // a reverse proxy in front of the server can hit its response timeout
                // and bounce the request back to the client as 502 Bad Gateway.
                wantsNdjson = String(req.headers['accept'] ?? '').includes('application/x-ndjson');
                const contentType = String(req.headers['content-type'] ?? '');
                if (contentType && !contentType.includes('application/x-risu-backup') && !contentType.includes('application/octet-stream')) {
                    res.status(415).json({ error: 'Unsupported backup content-type' });
                    return;
                }

                const contentLength = importContentLength(req, 'Backup archive');
                if (contentLength !== null) {
                    assertImportSize(contentLength, limits.maxBytes, 'Backup archive');
                    await assertImportDiskSpace(contentLength);
                }

                const uploadPath = path.join(
                    getDatabaseSpoolDir(),
                    `${BACKUP_IMPORT_SPOOL_FILE_PREFIX}${process.pid}-${nodeCrypto.randomUUID()}.tmp`,
                );
                uploadSpool = await spoolAsyncIterable(req, uploadPath, {
                    maxBytes: limits.maxBytes,
                    expectedBytes: contentLength,
                    signal: abortTracker.signal,
                });
                await assertImportDiskSpace(uploadSpool.size);
                throwIfImportAborted(abortTracker.signal);
                uploadStream = createReadStream(uploadSpool.filePath, {
                    highWaterMark: IMPORT_IO_PAGE_BYTES,
                });

                if (wantsNdjson) {
                    res.setHeader('content-type', 'application/x-ndjson');
                    res.setHeader('cache-control', 'no-cache, no-transform');
                    // Disable nginx response buffering so progress events flush immediately.
                    res.setHeader('x-accel-buffering', 'no');
                    res.flushHeaders();
                    // The upload was deliberately validated before response headers so
                    // cap violations can remain literal HTTP 413 responses. Emit one
                    // immediate keepalive when the post-validation NDJSON phase starts.
                    res.write('{"type":"heartbeat"}\n');

                    // Periodic keepalive — covers the post-stream phase (commit,
                    // inlay dir swap, cold storage migration) where onProgress is silent.
                    heartbeatTimer = setInterval(() => {
                        if (!res.writableEnded) res.write('{"type":"heartbeat"}\n');
                    }, BACKUP_NDJSON_HEARTBEAT_MS);

                    let lastProgressWrite = 0;
                    const totalBytes = uploadSpool.size;
                    const result = await importBackupFromSource(uploadStream, {
                        ...limits,
                        totalBytes,
                        signal: abortTracker.signal,
                        onProgress: (received, total) => {
                            const now = Date.now();
                            if (now - lastProgressWrite < 200) return;
                            lastProgressWrite = now;
                            res.write(JSON.stringify({ type: 'progress', bytes: received, totalBytes: total }) + '\n');
                        },
                    });
                    uploadStream.destroy();
                    uploadStream = null;
                    await fs.unlink(uploadSpool.filePath).catch(() => {});
                    uploadSpool = null;
                    res.write(JSON.stringify({
                        type: 'done',
                        ok: true,
                        assetsRestored: result.assetsRestored,
                        coldStorageFailed: result.coldStorageFailed,
                    }) + '\n');
                    res.end();
                } else {
                    const result = await importBackupFromSource(uploadStream, {
                        ...limits,
                        totalBytes: uploadSpool.size,
                        signal: abortTracker.signal,
                    });
                    uploadStream.destroy();
                    uploadStream = null;
                    await fs.unlink(uploadSpool.filePath).catch(() => {});
                    uploadSpool = null;
                    res.json({
                        ok: true,
                        assetsRestored: result.assetsRestored,
                        coldStorageFailed: result.coldStorageFailed,
                    });
                }
            } catch (error) {
                const diagnostic = logPluginStorageValidationFailure(
                    '[PluginStorage] Rejected invalid backup import row',
                    error
                );
                if (sendImportIngressError(res, error, { ndjson: wantsNdjson })) {
                    // Structured ingress failures always report their publication outcome.
                } else if (wantsNdjson && res.headersSent) {
                    try {
                        res.write(JSON.stringify(importNdjsonErrorEvent(error, diagnostic)) + '\n');
                        res.end();
                    } catch (_) {}
                } else if (diagnostic) {
                    res.status(400).json(diagnostic);
                } else {
                    next(error);
                }
            } finally {
                if (heartbeatTimer) clearInterval(heartbeatTimer);
                abortTracker.cleanup();
                uploadStream?.destroy();
                if (uploadSpool) await fs.unlink(uploadSpool.filePath).catch(() => {});
                releaseImportBarrier?.();
                if (ownsImportSlot) setImportInProgress(false);
                if (req.socket.server && prevRequestTimeout !== undefined) {
                    req.socket.server.requestTimeout = prevRequestTimeout;
                }
            }
        });

    }

    function registerServerBackupRoutes(app) {
        // ── Server-side backup endpoints ────────────────────────────────────────────

        // Save current data as a .bin backup file on the server
        app.post('/api/backup/server/save', async (req, res, next) => {
            if (!await checkAuth(req, res)) { return; }
            if (!checkActiveSession(req, res)) return;
            if (HUB_HOSTING_MODE) return res.status(403).json({ error: 'Server backups are disabled on this instance' });
            const abortTracker = createBackupExportAbortTracker(req, res);
            const destinationDir = path.resolve(getBackupsDir());
            let backupDbSpool = null;
            let pinnedState = null;
            const shouldAbort = () => abortTracker.signal.aborted || res.destroyed;
            try {
                pinnedState = await pinFullBackupState({
                    target: 'nodeonly',
                    signal: abortTracker.signal,
                    archiveTargetPath: destinationDir,
                });
                await waitAtFullExportAfterPinTestGate(abortTracker.signal);
                throwIfBackupExportAborted(abortTracker.signal);
                backupDbSpool = await buildSelfContainedBackupDatabase({
                    foldPluginStorage: false,
                    shouldAbort,
                    snapshot: pinnedState.snapshot,
                    databaseSource: pinnedState.databaseSource,
                    databaseState: pinnedState.databaseState,
                    signal: abortTracker.signal,
                });
                throwIfBackupExportAborted(abortTracker.signal);

                const namespacedEntries = await selectReferencedMcpToolCallEntries(
                    pinnedState.entries,
                    backupDbSpool,
                    shouldAbort,
                );
                preflightBackupEntries([
                    ...namespacedEntries,
                    ...(backupDbSpool
                        ? [{ backupName: 'database.risudat', size: backupDbSpool.size }]
                        : []),
                ]);
                const totalEntries = namespacedEntries.length + 1; // +1 for database
                const totalBytes = namespacedEntries.reduce(
                    (sum, entry) => sum + backupEntrySize(entry.backupName, entry.size),
                    0,
                ) + (backupDbSpool
                    ? backupEntrySize('database.risudat', backupDbSpool.size)
                    : 0);
                if (totalBytes > pinnedState.archiveReservedBytes) {
                    throw new Error('Server backup archive exceeds its admitted disk reservation');
                }

                // Stream progress as NDJSON
                res.setHeader('content-type', 'application/x-ndjson');
                res.flushHeaders();

                const tmpPath = path.join(
                    destinationDir,
                    `${SERVER_BACKUP_TEMP_PREFIX}${process.pid}-${pinnedState.token}.tmp`,
                );
                const writeStream = createWriteStream(tmpPath, {
                    flags: 'wx',
                    mode: 0o600,
                    flush: true,
                });
                const writeStreamFinished = finished(writeStream);
                writeStreamFinished.catch(() => {});
                const abortLocalWrite = () => {
                    const reason = abortTracker.signal.reason instanceof Error
                        ? abortTracker.signal.reason
                        : new Error('Server backup save cancelled');
                    writeStream.destroy(reason);
                };
                abortTracker.signal.addEventListener('abort', abortLocalWrite, { once: true });

                let finalPath = null;
                let filename = null;
                let responseComplete = false;

                try {
                    let written = 0;
                    let bytesWritten = 0;
                    for (const entry of namespacedEntries) {
                        throwIfBackupExportAborted(abortTracker.signal);
                        if (!await writePinnedBackupEntry(
                            writeStream,
                            entry,
                            shouldAbort,
                        )) break;
                        bytesWritten += backupEntrySize(entry.backupName, entry.size);
                        written++;
                        if (written % 50 === 0 || written === namespacedEntries.length) {
                            if (!await writeWithBackpressure(
                                res,
                                JSON.stringify({ type: 'progress', current: written, total: totalEntries, bytes: bytesWritten, totalBytes }) + '\n',
                                shouldAbort,
                            )) throw new Error('Client disconnected during backup save');
                        }
                    }
                    throwIfBackupExportAborted(abortTracker.signal);
                    if (backupDbSpool) {
                        const header = encodeBackupEntryHeader('database.risudat', backupDbSpool.size);
                        if (!await writeWithBackpressure(writeStream, header, shouldAbort)) {
                            throw new Error('Client disconnected during backup save');
                        }
                        if (!await streamFileToWritable(backupDbSpool.filePath, writeStream, shouldAbort)) {
                            throw new Error('Client disconnected during backup save');
                        }
                        bytesWritten += header.length + backupDbSpool.size;
                    }
                    if (!await writeWithBackpressure(
                        res,
                        JSON.stringify({ type: 'progress', current: totalEntries, total: totalEntries, bytes: bytesWritten, totalBytes }) + '\n',
                        shouldAbort,
                    )) throw new Error('Client disconnected during backup save');
                    writeStream.end();
                    await writeStreamFinished;
                    await waitAtServerBackupBeforePublishTestGate(abortTracker.signal);
                    throwIfBackupExportAborted(abortTracker.signal);
                    const tempStat = await fs.stat(tmpPath);
                    if (tempStat.size !== totalBytes) {
                        throw new Error('Server backup temp file length does not match its plan');
                    }

                    // Hard-link publication is atomic and never replaces an existing
                    // backup. Numeric suffix probing preserves the public filename
                    // contract while making same-millisecond saves collision-safe.
                    const timestamp = Date.now();
                    for (let attempt = 0; attempt < 10_000; attempt++) {
                        throwIfBackupExportAborted(abortTracker.signal);
                        filename = `risu-backup-${timestamp + attempt}.bin`;
                        finalPath = path.join(destinationDir, filename);
                        try {
                            await fs.link(tmpPath, finalPath);
                            break;
                        } catch (error) {
                            if (error?.code !== 'EEXIST') throw error;
                            finalPath = null;
                            filename = null;
                        }
                    }
                    if (!finalPath || !filename) {
                        throw new Error('Could not allocate a unique server backup filename');
                    }
                    throwIfBackupExportAborted(abortTracker.signal);
                    await fs.unlink(tmpPath);

                    const stat = await fs.stat(finalPath);
                    console.log(`[Server Backup] Saved: ${filename} (${(stat.size / 1024 / 1024).toFixed(1)} MB)`);
                    if (!await writeWithBackpressure(
                        res,
                        JSON.stringify({ type: 'done', ok: true, filename, size: stat.size }) + '\n',
                        shouldAbort,
                    )) throw new Error('Client disconnected before backup publication acknowledgement');
                    res.end();
                    await finished(res);
                    responseComplete = true;
                    markFullExportReservationConsumed(
                        pinnedState.reservation,
                        'ARCHIVE',
                        stat.size,
                    );
                } catch (innerError) {
                    writeStream.destroy();
                    await writeStreamFinished.catch(() => {});
                    await fs.unlink(tmpPath).catch(() => {});
                    if (!responseComplete && finalPath) {
                        await fs.unlink(finalPath).catch(() => {});
                    }
                    throw innerError;
                } finally {
                    abortTracker.signal.removeEventListener('abort', abortLocalWrite);
                }
            } catch (error) {
                if (abortTracker.signal.aborted || res.destroyed) {
                    return;
                } else if (!res.headersSent && error?.code === 'BACKUP_MISSING_CHAT_ROW') {
                    res.status(500).json({ error: error.message, code: error.code });
                } else if (!res.headersSent && error?.statusCode) {
                    res.status(error.statusCode).json(backupExportErrorPayload(error));
                } else if (!res.headersSent) {
                    next(error);
                } else {
                    res.write(JSON.stringify({ type: 'error', message: error.message }) + '\n');
                    res.end();
                }
            } finally {
                abortTracker.cleanup();
                if (backupDbSpool) {
                    await fs.unlink(backupDbSpool.filePath).catch(() => {});
                }
                await cleanupFullBackupState(pinnedState);
            }
        });

        // List backup files on the server
        app.get('/api/backup/server/list', async (req, res, next) => {
            if (!await checkAuth(req, res)) { return; }
            if (HUB_HOSTING_MODE) return res.status(403).json({ error: 'Server backups are disabled on this instance' });
            try {
                let entries;
                try {
                    entries = await fs.readdir(getBackupsDir(), { withFileTypes: true });
                } catch {
                    res.json({ backups: [] });
                    return;
                }
                const backups = [];
                for (const entry of entries) {
                    if (!entry.isFile() || !BACKUP_FILENAME_REGEX.test(entry.name)) continue;
                    const stat = await fs.stat(path.join(getBackupsDir(), entry.name));
                    const tsMatch = entry.name.match(/^risu-backup-(\d+)\.bin$/);
                    backups.push({
                        filename: entry.name,
                        size: stat.size,
                        createdAt: tsMatch ? Number(tsMatch[1]) : stat.mtimeMs,
                    });
                }
                backups.sort((a, b) => b.createdAt - a.createdAt);
                res.json({ backups });
            } catch (error) {
                next(error);
            }
        });

        // Restore from a server backup file
        app.post('/api/backup/server/restore', async (req, res, next) => {
            if (!await checkAuth(req, res)) { return; }
            if (!checkActiveSession(req, res)) return;
            if (HUB_HOSTING_MODE) return res.status(403).json({ error: 'Server backups are disabled on this instance' });

            const abortTracker = createImportAbortTracker(req, res);
            let ownsImportSlot = false;
            let releaseImportBarrier = null;
            let heartbeatTimer = null;
            let restoreStream = null;

            try {
                if (getImportInProgress()) {
                    res.status(409).json({ error: 'Another import is already in progress' });
                    return;
                }
                setImportInProgress(true);
                ownsImportSlot = true;
                releaseImportBarrier = await importBarrier.acquire(abortTracker.signal);
                throwIfImportAborted(abortTracker.signal);

                const filename = req.body?.filename;
                if (!filename || !BACKUP_FILENAME_REGEX.test(filename)) {
                    res.status(400).json({ error: 'Invalid backup filename' });
                    return;
                }
                const filePath = path.join(getBackupsDir(), filename);
                let fileStat;
                try {
                    fileStat = await fs.stat(filePath);
                } catch {
                    res.status(404).json({ error: 'Backup file not found' });
                    return;
                }

                const limits = backupImportLimits({ allowLargeRestore: true });
                assertImportSize(fileStat.size, limits.maxBytes, 'Server backup');

                await assertImportDiskSpace(fileStat.size);

                res.setHeader('content-type', 'application/x-ndjson');
                res.setHeader('cache-control', 'no-cache, no-transform');
                res.setHeader('x-accel-buffering', 'no');
                res.flushHeaders();
                res.write('{"type":"heartbeat"}\n');
                heartbeatTimer = setInterval(() => {
                    if (!res.writableEnded && !res.destroyed) res.write('{"type":"heartbeat"}\n');
                }, BACKUP_NDJSON_HEARTBEAT_MS);

                let lastProgressWrite = 0;
                const { createReadStream } = require('fs');
                restoreStream = createReadStream(filePath, { highWaterMark: IMPORT_IO_PAGE_BYTES });
                const result = await importBackupFromSource(restoreStream, {
                    ...limits,
                    totalBytes: fileStat.size,
                    signal: abortTracker.signal,
                    onProgress: (received, total) => {
                        const now = Date.now();
                        if (now - lastProgressWrite < 200) return;
                        lastProgressWrite = now;
                        res.write(JSON.stringify({ type: 'progress', bytes: received, totalBytes: total }) + '\n');
                    },
                });
                res.write(JSON.stringify({
                    type: 'done',
                    ok: true,
                    assetsRestored: result.assetsRestored,
                    coldStorageFailed: result.coldStorageFailed,
                }) + '\n');
                res.end();
            } catch (error) {
                const diagnostic = logPluginStorageValidationFailure(
                    '[PluginStorage] Rejected invalid server-backup row',
                    error
                );
                if (sendImportIngressError(res, error, { ndjson: true })) {
                    // Structured import failures include a stable publication outcome.
                } else if (!res.headersSent) {
                    if (diagnostic) res.status(400).json(diagnostic);
                    else next(error);
                } else {
                    res.write(JSON.stringify(importNdjsonErrorEvent(error, diagnostic)) + '\n');
                    res.end();
                }
            } finally {
                if (heartbeatTimer) clearInterval(heartbeatTimer);
                abortTracker.cleanup();
                restoreStream?.destroy();
                releaseImportBarrier?.();
                if (ownsImportSlot) setImportInProgress(false);
            }
        });

        // Delete a server backup file
        app.delete('/api/backup/server/:filename', async (req, res, next) => {
            if (!await checkAuth(req, res)) { return; }
            if (!checkActiveSession(req, res)) return;
            if (HUB_HOSTING_MODE) return res.status(403).json({ error: 'Server backups are disabled on this instance' });
            try {
                const filename = req.params.filename;
                if (!BACKUP_FILENAME_REGEX.test(filename)) {
                    res.status(400).json({ error: 'Invalid backup filename' });
                    return;
                }
                const filePath = path.join(getBackupsDir(), filename);
                try {
                    await fs.unlink(filePath);
                } catch (err) {
                    if (err.code === 'ENOENT') {
                        res.status(404).json({ error: 'Backup file not found' });
                        return;
                    }
                    throw err;
                }
                res.json({ ok: true });
            } catch (error) {
                next(error);
            }
        });

        // Download a server backup file
        app.get('/api/backup/server/download/:filename', async (req, res, next) => {
            if (!await checkAuth(req, res)) { return; }
            if (HUB_HOSTING_MODE) return res.status(403).json({ error: 'Server backups are disabled on this instance' });
            try {
                const filename = req.params.filename;
                if (!BACKUP_FILENAME_REGEX.test(filename)) {
                    res.status(400).json({ error: 'Invalid backup filename' });
                    return;
                }
                const filePath = path.join(getBackupsDir(), filename);
                let stat;
                try {
                    stat = await fs.stat(filePath);
                } catch {
                    res.status(404).json({ error: 'Backup file not found' });
                    return;
                }
                res.setHeader('content-type', 'application/octet-stream');
                res.setHeader('content-disposition', `attachment; filename="${filename}"`);
                res.setHeader('content-length', stat.size);
                const { createReadStream } = require('fs');
                createReadStream(filePath).pipe(res);
            } catch (error) {
                next(error);
            }
        });

        // ── Chat backup endpoints ──────────────────────────────────────────────────

        app.get('/api/chat-backups', async (req, res, next) => {
            if (!await checkAuth(req, res)) { return; }
            try {
                const chats = await queueStorageOperation(
                    () => listChatBackupChats()
                );
                res.json({ chats });
            } catch (error) {
                next(error);
            }
        });

        app.get('/api/chat-backups/:chaId/:chatId', async (req, res, next) => {
            if (!await checkAuth(req, res)) { return; }
            try {
                const versions = await queueStorageOperation(
                    () => listChatBackups(req.params.chaId, req.params.chatId)
                );
                res.json({ versions });
            } catch (error) {
                next(error);
            }
        });

        app.get('/api/chat-backups/:chaId/:chatId/:versionId', async (req, res, next) => {
            if (!await checkAuth(req, res)) { return; }
            const { chaId, chatId, versionId } = req.params;
            if (!CHAT_BACKUP_VERSION_ID_REGEX.test(versionId)) {
                res.status(400).json({ error: 'Invalid chat backup version ID' });
                return;
            }
            try {
                const raw = await queueStorageOperation(
                    () => readChatBackup(chaId, chatId, versionId)
                );
                if (!raw) {
                    res.status(404).json({ error: 'Chat backup version not found' });
                    return;
                }
                res.setHeader('Content-Type', 'application/octet-stream');
                res.send(raw);
            } catch (error) {
                next(error);
            }
        });

    }

    function registerSaveFolderMigrationRoutes(app) {
        app.get('/api/replacement-operations/:operationId', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            try {
                const operationId = req.params.operationId;
                if (!REPLACEMENT_OPERATION_ID_REGEX.test(operationId)) {
                    return res.status(400).json({ error: 'Invalid replacement operation ID' });
                }
                const operation = parseReplacementOperationRow(
                    readReplacementOperation.get(operationId),
                );
                if (!operation) {
                    return res.status(404).json({ error: 'Replacement operation not found' });
                }
                res.setHeader('cache-control', 'no-store');
                res.json(operation);
            } catch (error) {
                next(error);
            }
        });

        app.post('/api/migrate/save-folder/scan', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            if (!checkActiveSession(req, res)) return;
            try {
                const folderPath = req.body?.path || savePath;
                const resolved = path.resolve(folderPath);
                try {
                    const stat = require('fs').statSync(resolved);
                    if (!stat.isDirectory()) {
                        res.status(400).json({ error: 'Path is not a directory' });
                        return;
                    }
                } catch {
                    res.status(400).json({ error: 'Cannot access directory' });
                    return;
                }
                const { count, totalSize, hasDatabase } = scanHexFilesInDir(resolved);
                res.json({ count, totalSize, hasDatabase });
            } catch (error) {
                next(error);
            }
        });

        app.post('/api/migrate/save-folder/execute', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            if (!checkActiveSession(req, res)) return;
            const abortTracker = createImportAbortTracker(req, res);
            let ownsImportSlot = false;
            let releaseImportBarrier = null;
            let operationId = null;
            let heartbeatTimer = null;
            const wantsNdjson = String(req.headers.accept ?? '').includes('application/x-ndjson');
            try {
                const folderPath = req.body?.path || savePath;
                const resolved = path.resolve(folderPath);
                try {
                    const stat = require('fs').statSync(resolved);
                    if (!stat.isDirectory()) {
                        res.status(400).json({ error: 'Path is not a directory' });
                        return;
                    }
                } catch {
                    res.status(400).json({ error: 'Cannot access directory' });
                    return;
                }
                if (getImportInProgress()) {
                    res.status(409).json({ error: 'Another import is already in progress' });
                    return;
                }
                setImportInProgress(true);
                ownsImportSlot = true;
                if (wantsNdjson) {
                    operationId = registerReplacementOperation(req, 'save-folder-directory');
                    heartbeatTimer = beginReplacementNdjson(res);
                    sendReplacementProgress(res, 'queued');
                }
                releaseImportBarrier = await importBarrier.acquire(abortTracker.signal);
                throwIfImportAborted(abortTracker.signal);
                if (wantsNdjson) sendReplacementProgress(res, 'staging');

                const result = await importHexFilesFromDir(resolved, {
                    signal: abortTracker.signal,
                    operationId,
                });
                if (operationId
                    && readReplacementOperation.get(operationId)?.state === 'running') {
                    setReplacementOperationOutcome(operationId, 'committed', {
                        result: { ok: true, imported: result.imported },
                    });
                }
                if (wantsNdjson) {
                    sendReplacementDone(res, operationId, { ok: true, imported: result.imported });
                } else {
                    res.json({ ok: true, imported: result.imported });
                }
            } catch (error) {
                finalizeReplacementOperationError(
                    operationId,
                    error,
                    'SAVE_FOLDER_IMPORT_FAILED',
                );
                const diagnostic = logPluginStorageValidationFailure(
                    '[PluginStorage] Rejected invalid save-folder row',
                    error
                );
                if (!sendSaveFolderImportFailure(res, error, diagnostic, { ndjson: wantsNdjson })) {
                    if (wantsNdjson && res.headersSent) {
                        if (!res.writableEnded && !res.destroyed) {
                            res.write(`${JSON.stringify(importNdjsonErrorEvent(
                                error,
                                diagnostic,
                                'SAVE_FOLDER_IMPORT_FAILED',
                            ))}\n`);
                            res.end();
                        }
                        return;
                    }
                    res.status(400).json(diagnostic ?? { error: error.message || 'Import failed' });
                }
            } finally {
                if (heartbeatTimer) clearInterval(heartbeatTimer);
                abortTracker.cleanup();
                releaseImportBarrier?.();
                if (ownsImportSlot) setImportInProgress(false);
            }
        });

        app.post('/api/migrate/save-folder/upload', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            if (!checkActiveSession(req, res)) return;
            const abortTracker = createImportAbortTracker(req, res);
            let ownsImportSlot = false;
            let releaseImportBarrier = null;
            let prevRequestTimeout;
            let stageDir = null;
            let operationId = null;
            let heartbeatTimer = null;
            const wantsNdjson = String(req.headers.accept ?? '').includes('application/x-ndjson');

            try {
                const contentType = String(req.headers['content-type'] ?? '');
                if (contentType
                    && !contentType.includes('application/zip')
                    && !contentType.includes('application/octet-stream')) {
                    res.status(415).json({ error: 'Unsupported save-folder archive content-type' });
                    return;
                }
                const contentLength = importContentLength(req, 'Save-folder ZIP');
                if (contentLength !== null) {
                    assertImportSize(contentLength, BACKUP_IMPORT_MAX_BYTES, 'Save-folder ZIP');
                    await assertImportDiskSpace(contentLength);
                }
                if (getImportInProgress()) {
                    res.status(409).json({ error: 'Another import is already in progress' });
                    return;
                }
                setImportInProgress(true);
                ownsImportSlot = true;
                if (wantsNdjson) {
                    operationId = registerReplacementOperation(req, 'save-folder-zip');
                    heartbeatTimer = beginReplacementNdjson(res);
                    sendReplacementProgress(res, 'queued');
                }
                releaseImportBarrier = await importBarrier.acquire(abortTracker.signal);
                throwIfImportAborted(abortTracker.signal);

                req.socket.setTimeout(0);
                req.socket.setKeepAlive(true);
                prevRequestTimeout = req.socket.server?.requestTimeout;
                if (req.socket.server) req.socket.server.requestTimeout = 0;

                stageDir = createSaveFolderImportStage();
                if (wantsNdjson) sendReplacementProgress(res, 'uploading');
                const zipPath = path.join(stageDir, 'upload.zip');
                const zipSpool = await spoolAsyncIterable(req, zipPath, {
                    maxBytes: BACKUP_IMPORT_MAX_BYTES,
                    expectedBytes: contentLength,
                    signal: abortTracker.signal,
                });
                await assertImportDiskSpace(zipSpool.size);
                if (wantsNdjson) sendReplacementProgress(res, 'inspecting');
                const inventory = await inspectZipFile(zipSpool.filePath, {
                    acceptEntry: (entryPath) => {
                        const basename = path.posix.basename(entryPath.replaceAll('\\', '/'));
                        const key = decodeCanonicalHexStorageKey(basename);
                        return key === null ? null : { key };
                    },
                    maxEntries: SAVE_FOLDER_IMPORT_MAX_ENTRIES,
                    maxExpandedBytes: BACKUP_IMPORT_MAX_BYTES,
                    signal: abortTracker.signal,
                });
                if (inventory.entries.length === 0) {
                    throw importFormatError('No compatible hex files found in ZIP', 'SAVE_FOLDER_ENTRIES_MISSING');
                }
                if (!inventory.entries.some((entry) => entry.key === DB_BLOB_KEY)) {
                    throw importFormatError(
                        'Data does not contain database/database.bin',
                        'SAVE_FOLDER_DATABASE_MISSING',
                    );
                }
                await assertImportDiskSpace(inventory.expandedBytes);
                if (wantsNdjson) sendReplacementProgress(res, 'extracting');
                const sources = await extractZipEntries(
                    inventory,
                    path.join(stageDir, 'rows'),
                    { signal: abortTracker.signal },
                );
                throwIfImportAborted(abortTracker.signal);
                const result = await importLegacySaveEntries(
                    sources,
                    'Data does not contain database/database.bin',
                    { signal: abortTracker.signal, operationId },
                );
                if (operationId
                    && readReplacementOperation.get(operationId)?.state === 'running') {
                    setReplacementOperationOutcome(operationId, 'committed', {
                        result: { ok: true, imported: result.imported },
                    });
                }
                await fs.rm(stageDir, { recursive: true, force: true });
                stageDir = null;
                if (wantsNdjson) {
                    sendReplacementDone(res, operationId, { ok: true, imported: result.imported });
                } else {
                    res.json({ ok: true, imported: result.imported });
                }
            } catch (error) {
                finalizeReplacementOperationError(
                    operationId,
                    error,
                    'SAVE_FOLDER_IMPORT_FAILED',
                );
                const diagnostic = logPluginStorageValidationFailure(
                    '[PluginStorage] Rejected invalid uploaded save-folder row',
                    error
                );
                if (!sendSaveFolderImportFailure(res, error, diagnostic, { ndjson: wantsNdjson })) {
                    if (wantsNdjson && res.headersSent) {
                        if (!res.writableEnded && !res.destroyed) {
                            res.write(`${JSON.stringify(importNdjsonErrorEvent(
                                error,
                                diagnostic,
                                'SAVE_FOLDER_IMPORT_FAILED',
                            ))}\n`);
                            res.end();
                        }
                        return;
                    }
                    if (!res.headersSent) {
                        res.status(400).json(diagnostic ?? { error: error.message || 'Import failed' });
                    }
                }
            } finally {
                if (heartbeatTimer) clearInterval(heartbeatTimer);
                abortTracker.cleanup();
                if (stageDir) await fs.rm(stageDir, { recursive: true, force: true }).catch(() => {});
                releaseImportBarrier?.();
                if (ownsImportSlot) setImportInProgress(false);
                if (req.socket.server && prevRequestTimeout !== undefined) {
                    req.socket.server.requestTimeout = prevRequestTimeout;
                }
            }
        });

        app.post('/api/migrate/save-folder/cleanup/scan', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            if (!checkActiveSession(req, res)) return;
            try {
                if (!isLegacyHexMigrationComplete()) {
                    res.status(400).json({ error: 'Migration has not been completed yet' });
                    return;
                }
                const { count, totalSize } = scanHexFilesInDir(savePath);
                res.json({ count, totalSize });
            } catch (error) {
                next(error);
            }
        });

        app.post('/api/migrate/save-folder/cleanup/execute', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            if (!checkActiveSession(req, res)) return;
            try {
                if (!isLegacyHexMigrationComplete()) {
                    res.status(400).json({ error: 'Migration has not been completed yet' });
                    return;
                }
                const { hexFiles } = scanHexFilesInDir(savePath);
                let removed = 0;
                let freedBytes = 0;
                for (const f of hexFiles) {
                    try {
                        const filePath = path.join(savePath, f);
                        const stat = require('fs').statSync(filePath);
                        unlinkSync(filePath);
                        freedBytes += stat.size;
                        removed++;
                    } catch { /* skip unremovable files */ }
                }
                res.json({ ok: true, removed, freedBytes });
            } catch (error) {
                next(error);
            }
        });

    }

    function registerSnapshotRestoreRoute(app) {
        app.post('/api/db/snapshots/restore', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            if (!checkActiveSession(req, res)) return;
            let restoreCommitted = false;
            let restoreSpool = null;
            let restorePublicationStarted = false;
            let closed = false;
            let releaseImportBarrier = null;
            let operationId = null;
            let heartbeatTimer = null;
            const wantsNdjson = String(req.headers.accept ?? '').includes('application/x-ndjson');
            const restoreAbortController = new AbortController();
            const restoreSocket = req.socket;
            const abortRestoreOnDisconnect = () => {
                // `close` also follows a normal completed response.  Only an unfinished
                // response represents the peer disappearing while spooling/publishing.
                if (res.writableEnded) return;
                closed = true;
                if (!restoreCommitted && !restoreAbortController.signal.aborted) {
                    restoreAbortController.abort(new Error('Snapshot restore client disconnected'));
                }
            };
            const throwIfRestoreAborted = () => {
                if (!restoreAbortController.signal.aborted) return;
                const error = new Error('Snapshot restore client disconnected');
                error.code = 'KV_STREAM_ABORTED';
                throw error;
            };
            req.once('aborted', abortRestoreOnDisconnect);
            restoreSocket?.once('close', abortRestoreOnDisconnect);
            res.once('close', abortRestoreOnDisconnect);
            // Authentication and session validation both yield before this lifecycle
            // tracker is installed. Seed cancellation from the current stream/socket
            // state so a disconnect observed during either await cannot be lost before
            // the restore enters (or waits for) the import barrier.
            if (req.aborted
                || (req.destroyed && !req.complete)
                || res.destroyed
                || restoreSocket?.destroyed) {
                abortRestoreOnDisconnect();
            }
            try {
                const key = typeof req.body?.key === 'string' ? req.body.key : '';
                if (!parseInternalSnapshotKey(key)) {
                    return res.status(400).json({
                        error: 'Invalid snapshot key',
                        retryable: false,
                        commitOutcome: 'not-committed',
                        commitOutcomeUnknown: false,
                    });
                }
                if (wantsNdjson) {
                    operationId = registerReplacementOperation(req, 'internal-snapshot');
                    heartbeatTimer = beginReplacementNdjson(res);
                    sendReplacementProgress(res, 'queued');
                }
                // Acquire before entering the storage queue: acquire() drains that same
                // queue, so holding a slot while waiting for it would deadlock.
                // The disconnect signal must participate in this wait. Otherwise an
                // abandoned restore remains queued behind a long-running import until
                // that holder releases, retaining its request lifecycle unnecessarily.
                releaseImportBarrier = await importBarrier.acquire(restoreAbortController.signal);
                throwIfRestoreAborted();
                if (wantsNdjson) sendReplacementProgress(res, 'spooling');
                let snapshotFound = true;
                let committedPublication = null;
                {
                    await queueStorageOperation(async () => {
                        throwIfRestoreAborted();
                        // Drain any pending debounced persist first — same pattern as
                        // /api/db/optimize. Without this, an in-flight save could land
                        // after kvCopyValue and overwrite the restored snapshot.
                        await flushPendingDb();
                        throwIfRestoreAborted();
                        // Read only after the import barrier is held. The importer uses
                        // this same SQLite connection, so an earlier cursor could observe
                        // its uncommitted snapshot rows and publish transient state. Spool
                        // one persisted chunk at a time instead of assembling the value.
                        const restorePath = path.join(
                            getDatabaseSpoolDir(),
                            `${DATABASE_SPOOL_FILE_PREFIX}snapshot-restore-${process.pid}-${nodeCrypto.randomUUID()}.tmp`,
                        );
                        restoreSpool = await kvWriteToFile(key, restorePath, {
                            signal: restoreAbortController.signal,
                        });
                        if (!restoreSpool) {
                            snapshotFound = false;
                            return;
                        }
                        const source = {
                            filePath: restoreSpool.filePath,
                            size: restoreSpool.size,
                        };
                        const inspection = await inspectRisuSaveSource(source);
                        throwIfRestoreAborted();
                        if (wantsNdjson) sendReplacementProgress(res, 'publishing');
                        let restoreTransactionOpen = false;
                        try {
                            // Keep the live monolith, external plugin rows, ownership
                            // sidecars, chat rows, and migration markers in one rollback
                            // boundary. Both ingest paths join an existing transaction.
                            // The spool and inspection both yield.  Re-check the real
                            // socket-derived AbortSignal immediately before opening the
                            // publication transaction, and again before COMMIT.
                            throwIfRestoreAborted();
                            sqliteDb.exec('BEGIN');
                            restorePublicationStarted = true;
                            restoreTransactionOpen = true;
                            let ingestion;
                            if (inspection.supported) {
                                // Every supported snapshot ingests from the bounded file
                                // cursor. This avoids kvGet()/Buffer.concat() even below
                                // the general import streaming threshold.
                                kvDel(REMOTE_MIGRATION_MARKER_KEY);
                                invalidateAllDbCaches();
                                ingestion = await ingestDatabaseStreaming(source, {
                                    inspection,
                                    shouldAbort: () => restoreAbortController.signal.aborted,
                                    signal: restoreAbortController.signal,
                                    onDecodedChunk: () => waitAtSnapshotRestoreDecodeTestGate(
                                        restoreAbortController.signal,
                                    ),
                                });
                                markRemoteMigrationDone();
                            } else {
                                // Compatibility formats that cannot be cursor-walked
                                // safely are decoded only below an explicit finite cap.
                                // Compressed legacy inputs still expand through the same
                                // disk-backed output meter and AbortSignal as canonical
                                // gzip/zlib, so no fallback performs an unbounded read or
                                // synchronous expansion bomb.
                                kvDel(REMOTE_MIGRATION_MARKER_KEY);
                                invalidateAllDbCaches();
                                const decoded = await decodeBoundedLegacyRisuSave(source, {
                                    inspection,
                                    tempDir: requireDatabaseSpoolDirSync(),
                                    shouldAbort: () => restoreAbortController.signal.aborted,
                                    signal: restoreAbortController.signal,
                                    onDecodedChunk: () => waitAtSnapshotRestoreDecodeTestGate(
                                        restoreAbortController.signal,
                                    ),
                                    // Check the logical value length from chunk metadata
                                    // before kvGet is allowed to concatenate its chunks.
                                    resolveRemoteSize: async (name) => {
                                        if (snapshotRestoreRemoteFailpoint === 'size') {
                                            throw new Error('Injected REMOTE size read failure');
                                        }
                                        return kvSize(`remotes/${name}.local.bin`);
                                    },
                                    resolveRemote: async (name) => {
                                        if (snapshotRestoreRemoteFailpoint === 'body') {
                                            throw new Error('Injected REMOTE body read failure');
                                        }
                                        return kvGet(`remotes/${name}.local.bin`);
                                    },
                                });
                                // `decoded` is the requested snapshot. Running the live
                                // REMOTE migration here would replace it with the current
                                // database.bin after the marker was cleared.
                                ingestion = await ingestDatabase(decoded, {
                                    skipLiveRemoteMigration: true,
                                });
                                markRemoteMigrationDone();
                            }
                            if (ingestion) {
                                const strippedBytes = Buffer.from(encodeRisuSaveLegacy(ingestion.strippedDb));
                                committedPublication = {
                                    strippedBytes,
                                    strippedDb: ingestion.strippedDb,
                                };
                            }
                            // A restore can replace a broad logical database state. Force every
                            // browser list cache to take one full snapshot after it completes.
                            kvBumpListEpoch();
                            if (snapshotRestoreFailpoint === 'before-commit') {
                                throw new Error('Injected snapshot restore failure before commit');
                            }
                            throwIfRestoreAborted();
                            setReplacementOperationOutcome(operationId, 'committed', {
                                result: { ok: true, key },
                            });
                            sqliteDb.exec('COMMIT');
                            restoreTransactionOpen = false;
                            restoreCommitted = true;
                        } catch (error) {
                            if (restoreTransactionOpen) {
                                try { sqliteDb.exec('ROLLBACK'); } catch (rollbackError) {
                                    logger.error('[Snapshot Restore] Failed to roll back SQLite transaction:', rollbackError);
                                }
                            }
                            // The cache and ETag may have been derived from tentative rows.
                            invalidateAllDbCaches();
                            setDbEtag(null);
                            throw error;
                        }
                    });
                }
                if (!snapshotFound) {
                    if (closed) return;
                    if (wantsNdjson) {
                        const error = new Error('Snapshot not found');
                        error.code = 'SNAPSHOT_NOT_FOUND';
                        error.statusCode = 404;
                        error.retryable = false;
                        error.commitOutcome = 'not-committed';
                        error.commitOutcomeUnknown = false;
                        throw error;
                    }
                    return res.status(404).json({
                        error: 'Snapshot not found',
                        retryable: false,
                        commitOutcome: 'not-committed',
                        commitOutcomeUnknown: false,
                    });
                }
                if (committedPublication) {
                    setDbEtag(computeBufferEtag(committedPublication.strippedBytes));
                    try {
                        rememberSessionPluginStorageState(req, committedPublication.strippedDb);
                    } catch (error) {
                        // Session pinning is disposable process state. The SQLite
                        // publication has committed; a pin failure must not turn its
                        // acknowledgement into a false rollback report.
                        logger.error('[Snapshot Restore] Failed to refresh session read state:', error);
                    }
                }
                if (closed) return;
                if (snapshotRestoreFailpoint === 'response') {
                    res.destroy();
                    return;
                }
                if (wantsNdjson) {
                    sendReplacementDone(res, operationId, { ok: true, key });
                } else {
                    res.json({
                        ok: true,
                        key,
                        commitOutcome: 'committed',
                        commitOutcomeUnknown: false,
                    });
                }
            } catch (err) {
                const diagnostic = pluginStorageValidationDiagnostic(err);
                if (err && typeof err === 'object') {
                    if (restoreCommitted) {
                        err.commitOutcome = 'committed';
                        err.commitOutcomeUnknown = false;
                        err.retryable = false;
                    } else if (err.commitOutcome !== 'unknown') {
                        err.commitOutcome = 'not-committed';
                        err.commitOutcomeUnknown = false;
                    }
                }
                finalizeReplacementOperationError(
                    operationId,
                    err,
                    'SNAPSHOT_RESTORE_NOT_COMMITTED',
                );
                if (wantsNdjson && res.headersSent && !closed) {
                    if (!res.writableEnded && !res.destroyed) {
                        res.write(`${JSON.stringify(importNdjsonErrorEvent(
                            err,
                            diagnostic ? {
                                ...diagnostic,
                                status: 400,
                                retryable: false,
                                commitOutcome: 'not-committed',
                                commitOutcomeUnknown: false,
                            } : null,
                            restoreCommitted
                                ? 'SNAPSHOT_RESTORE_COMMIT_FAILED'
                                : 'SNAPSHOT_RESTORE_NOT_COMMITTED',
                        ))}\n`);
                        res.end();
                    }
                    return;
                }
                if (restoreCommitted) {
                    logger.error('[Snapshot Restore] Commit succeeded but acknowledgement failed:', err);
                    if (closed) return;
                    if (!res.headersSent) {
                        res.status(500).json({
                            error: 'Snapshot restore committed, but acknowledgement failed',
                            code: 'SNAPSHOT_RESTORE_COMMIT_UNKNOWN',
                            retryable: false,
                            commitOutcome: 'unknown',
                            commitOutcomeUnknown: true,
                        });
                    } else {
                        res.destroy();
                    }
                    return;
                }
                if (closed) {
                    logger.warn(restorePublicationStarted
                        ? '[Snapshot Restore] Client disconnected before commit; transaction was rolled back'
                        : '[Snapshot Restore] Client disconnected before publication; partial spool was discarded');
                    return;
                }
                if (diagnostic) return res.status(400).json({
                    ...diagnostic,
                    retryable: false,
                    commitOutcome: 'not-committed',
                    commitOutcomeUnknown: false,
                });
                if (isImportInProgressError(err)) return sendImportBusy(res);
                if (err?.risuSavePreparationInvalid === true) {
                    return res.status(400).json({
                        error: err.message,
                        code: err.code,
                        retryable: false,
                        commitOutcome: 'not-committed',
                        commitOutcomeUnknown: false,
                    });
                }
                if (err?.risuSavePreparationLimit === true) {
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
                logger.error('[Snapshot Restore] Transaction was not committed:', err);
                res.status(500).json({
                    error: 'Snapshot restore was not committed',
                    code: 'SNAPSHOT_RESTORE_NOT_COMMITTED',
                    retryAfter: 0,
                    retryable: true,
                    commitOutcome: 'not-committed',
                    commitOutcomeUnknown: false,
                });
            } finally {
                if (heartbeatTimer) clearInterval(heartbeatTimer);
                releaseImportBarrier?.();
                req.off('aborted', abortRestoreOnDisconnect);
                restoreSocket?.off('close', abortRestoreOnDisconnect);
                res.off('close', abortRestoreOnDisconnect);
                if (restoreSpool?.filePath) {
                    await fs.unlink(restoreSpool.filePath).catch(() => {});
                }
            }
        });

    }

    function registerBackupConfigurationRoutes(app) {
        // ── Boot-time backup reminder ───────────────────────────────────────────────

        const BOOT_REMINDER_KEY = 'config/boot-backup-reminder';

        function readBootReminder() {
            try {
                const raw = kvGet(BOOT_REMINDER_KEY);
                if (!raw) return false;
                return Buffer.from(raw).toString('utf-8').trim() === '1';
            } catch { return false; }
        }

        app.get('/api/backup/boot-reminder', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            if (HUB_HOSTING_MODE) return res.json({ enabled: false });
            try {
                res.json({ enabled: readBootReminder() });
            } catch (err) { next(err); }
        });

        app.put('/api/backup/boot-reminder', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            if (!checkActiveSession(req, res)) return;
            if (HUB_HOSTING_MODE) return res.status(403).json({ error: 'Server backups are disabled on this instance' });
            try {
                const enabled = !!req.body?.enabled;
                await queueStorageMutation(() => {
                    kvSet(BOOT_REMINDER_KEY, Buffer.from(enabled ? '1' : '0', 'utf-8'));
                });
                res.json({ enabled });
            } catch (err) {
                if (isImportInProgressError(err)) return sendImportBusy(res);
                next(err);
            }
        });

        // ── Backup directory configuration ──────────────────────────────────────────

        app.get('/api/backup/server/path', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            if (HUB_HOSTING_MODE) return res.status(403).json({ error: 'Server backups are disabled on this instance' });
            try {
                res.json({
                    path: getBackupsDir(),
                    default: DEFAULT_BACKUPS_DIR,
                    isDefault: getBackupsDir() === DEFAULT_BACKUPS_DIR,
                });
            } catch (err) { next(err); }
        });

        app.put('/api/backup/server/path', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            if (!checkActiveSession(req, res)) return;
            if (HUB_HOSTING_MODE) return res.status(403).json({ error: 'Server backups are disabled on this instance' });
            try {
                const transition = await withRecoveryPathStateLock(async () => {
                    if (isSelfUpdateInProgress()) {
                        return {
                            status: 409,
                            body: { error: 'Backup path cannot change while a self-update is in progress' },
                        };
                    }
                    const nextPath = typeof req.body?.path === 'string' ? req.body.path.trim() : '';
                    if (!nextPath) return { status: 400, body: { error: 'Path required' } };
                    const resolved = path.resolve(nextPath);
                    if (isManagedBackupPath(resolved)) {
                        return {
                            status: 400,
                            body: {
                                error: 'Backup path cannot be inside PocketRisu app files. Choose a separate folder such as data/backups.',
                            },
                        };
                    }
                    // Admission, writability probing, marker/KV publication, and live
                    // state movement share the same lock as self-update preservation.
                    try {
                        if (!existsSync(resolved)) mkdirSync(resolved, { recursive: true });
                        const probe = path.join(resolved, `.risu-write-probe-${Date.now()}`);
                        require('fs').writeFileSync(probe, '');
                        require('fs').unlinkSync(probe);
                    } catch (error) {
                        return {
                            status: 400,
                            body: { error: 'Path is not writable: ' + (error?.message || String(error)) },
                        };
                    }
                    const previous = getBackupsDir();
                    const previousMarkerTargets = readRecoveryPathMarkerTargetsSync(BACKUP_PATH_MARKER);
                    const transitionTargets = [
                        ...previousMarkerTargets,
                        path.resolve(previous),
                        resolved,
                    ];
                    // The durable transition record is a conservative union: until KV
                    // and live state both move, every updater preserves both old and new
                    // roots. A crash at either test boundary therefore cannot strand the
                    // root still used by the running or most recently durable config.
                    publishUpdaterPathMarkerSet(BACKUP_PATH_MARKER, transitionTargets);
                    await waitAtRecoveryPathStateTestGate('after-transition-marker');
                    try {
                        await queueStorageMutation(() => {
                            kvSet(BACKUP_PATH_CONFIG_KEY, Buffer.from(resolved, 'utf-8'));
                        });
                    } catch (configurationError) {
                        try {
                            publishUpdaterPathMarkerSet(BACKUP_PATH_MARKER, previousMarkerTargets);
                        } catch (markerRollbackError) {
                            throw new AggregateError(
                                [configurationError, markerRollbackError],
                                'Backup-path configuration failed; preservation metadata remains conservative or fail-closed',
                            );
                        }
                        throw configurationError;
                    }
                    await waitAtRecoveryPathStateTestGate('after-kv-before-live');
                    setBackupsDirResolved(resolved);
                    sweepServerBackupTemps(getBackupsDir());
                    return {
                        status: 200,
                        body: {
                            path: getBackupsDir(),
                            previous,
                            default: DEFAULT_BACKUPS_DIR,
                            isDefault: getBackupsDir() === DEFAULT_BACKUPS_DIR,
                        },
                    };
                });
                res.status(transition.status).json(transition.body);
            } catch (err) {
                if (err?.code === 'RECOVERY_PATH_STATE_LOCKED') {
                    return res.status(409).json({ error: err.message });
                }
                if (isImportInProgressError(err)) return sendImportBusy(res);
                next(err);
            }
        });

    }

    return {
        registerBackupExportRoutes,
        registerBackupImportRoutes,
        registerServerBackupRoutes,
        registerSaveFolderMigrationRoutes,
        registerSnapshotRestoreRoute,
        registerBackupConfigurationRoutes,
    };
}

function routes(app, ctx) {
    let family = backupRouteFamilies.get(app);
    if (!family) {
        family = createBackupRouteFamily(ctx);
        backupRouteFamilies.set(app, family);
    }
    return family;
}

function registerBackupExportRoutes(app, ctx) {
    routes(app, ctx).registerBackupExportRoutes(app);
}

function registerBackupImportRoutes(app, ctx) {
    routes(app, ctx).registerBackupImportRoutes(app);
}

function registerServerBackupRoutes(app, ctx) {
    routes(app, ctx).registerServerBackupRoutes(app);
}

function registerSaveFolderMigrationRoutes(app, ctx) {
    routes(app, ctx).registerSaveFolderMigrationRoutes(app);
}

function registerSnapshotRestoreRoute(app, ctx) {
    routes(app, ctx).registerSnapshotRestoreRoute(app);
}

function registerBackupConfigurationRoutes(app, ctx) {
    routes(app, ctx).registerBackupConfigurationRoutes(app);
}

module.exports = {
    registerBackupExportRoutes,
    registerBackupImportRoutes,
    registerServerBackupRoutes,
    registerSaveFolderMigrationRoutes,
    registerSnapshotRestoreRoute,
    registerBackupConfigurationRoutes,
    startPartialExportJobGc,
};
