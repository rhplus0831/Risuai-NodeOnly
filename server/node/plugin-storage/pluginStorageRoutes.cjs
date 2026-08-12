'use strict';

const path = require('path');
const fsSync = require('fs');
const {
    existsSync,
    readFileSync,
    writeFileSync,
    renameSync,
    unlinkSync,
    openSync,
    closeSync,
    fsyncSync,
    createReadStream,
    createWriteStream,
} = fsSync;
const fs = require('fs/promises');
const nodeCrypto = require('crypto');
const { hardenPrivateFile } = require('../runtime/platformFilesystem.cjs');
const { Transform } = require('stream');
const { pipeline } = require('stream/promises');
const { addExtension, Unpackr } = require('msgpackr');
const {
    kvGet,
    kvGetAsync,
    kvWriteToFile,
    kvSet,
    kvSetFromFile,
    kvDel,
    kvDelPrefix,
    kvList,
    kvListSelectedWithSizes,
    kvSize,
    kvGetDatabaseRevision,
    kvGetPluginStoragePublicationRevision,
    rebuildPluginStorageViewerFacets,
    reconcilePluginStorageUsage,
    getPluginStorageMutationVersion,
    withPluginStorageQuotaPlan,
    createKvSnapshot,
    db: sqliteDb,
} = require('../db/db.cjs');
const { openStageRowDownload } = require('../db/stageRowDownload.cjs');
const { streamRisuSaveToFile } = require('../backup/streamRisuSave.cjs');
const { validateJsonSource } = require('../backup/streamJsonToMsgpack.cjs');
const {
    decodeBoundedLegacyRisuSave,
    inspectRisuSaveSource,
} = require('../backup/streamRisuLoad.cjs');
const {
    decodeRisuSave,
    decodeAuthoritativeRisuSave,
    sha256Hex,
} = require('../utils.cjs');
const { computeBufferEtag } = require('../db/dbCachedRead.cjs');
const {
    BACKUP_ENTRY_NAME_MAX_BYTES,
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
    pluginStorageManifestMappingMap,
} = require('./pluginSaveKeys.cjs');
const {
    PluginStorageValidationError,
    PLUGIN_STORAGE_JSON_CODEC,
    PLUGIN_STORAGE_LOSSLESS_CODEC,
    assertPluginStorageRow,
    convertCompatiblePluginStorageJson,
    decodeValidatedPluginStorageKey,
    encodeValidatedPluginStorageKey,
    isPluginStorageValidationError,
    parsePluginStorageJsonBuffer,
    pluginStorageCodecForBuffer,
    serializeLosslessPluginStorageRow,
    serializePluginStorageRow,
    validatePluginStorageRow,
} = require('./pluginStorageJson.cjs');
const {
    PLUGIN_VALUE_MAX_BYTES,
    PLUGIN_STORAGE_MAX_BYTES,
    PluginStorageLimitError,
} = require('./pluginStorageLimits.cjs');
const {
    pluginStorageViewerDisplaySize,
    pluginStorageViewerDisplaySizeFromMetadata,
    pluginStorageViewerValueText,
} = require('./pluginStorageViewerFacets.cjs');
const {
    BUFFERED_INGRESS_POLICY,
    sendClientUpgradeRequired,
} = require('../chat/bufferedIngress.cjs');
const { logger } = require('../runtime/logs.cjs');

class UnsupportedPluginStorageTransitionValue {
    constructor() {
        this.kind = 'function';
    }
}

addExtension({
    type: 63,
    unpack: bytes => {
        if (bytes.length !== 1 || bytes[0] !== 1) {
            throw new TypeError('Invalid plugin transition function marker');
        }
        return new UnsupportedPluginStorageTransitionValue();
    },
});

const richPluginTransitionUnpackr = new Unpackr({
    structuredClone: true,
    useRecords: true,
});

const pluginStorageRouteFamilies = new WeakMap();

function createPluginStorageRouteFamily(ctx) {
    const {
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
    } = ctx;

    // Test-only boundaries for the optimized plugin clear. Kept at route level so
    // production deletion primitives cannot accidentally acquire a failpoint.
    const pluginStorageClearFailpoint = process.env.NODE_ENV === 'test'
        ? String(process.env.POCKETRISU_TEST_PLUGIN_CLEAR_FAILPOINT ?? '')
        : '';
    
    // Test-only boundaries for the AA1 transaction contract. These are scoped to
    // the narrow plugin mutation endpoint so ordinary KV fault-injection remains
    // unchanged: owner-write | owner-remove | pre-commit | verification-read |
    // acknowledgement-loss.
    const pluginStorageMutationFailpoint = process.env.NODE_ENV === 'test'
        ? String(process.env.POCKETRISU_TEST_PLUGIN_MUTATION_FAILPOINT ?? '').trim()
        : '';
    function hitPluginStorageMutationFailpoint(boundary) {
        if (pluginStorageMutationFailpoint === boundary) {
            throw new Error(`Injected plugin storage mutation failure at ${boundary}`);
        }
    }
    const PLUGIN_STORAGE_BATCH_STREAM_MAGIC = Buffer.from('PRISUB01', 'ascii');
    const PLUGIN_STORAGE_BATCH_STREAM_PREFIX_BYTES = 12;
    const PLUGIN_STORAGE_TRANSITION_STREAM_MAGIC = Buffer.from('PRISUT01', 'ascii');
    const PLUGIN_STORAGE_TRANSITION_STREAM_PREFIX_BYTES = 12;
    const PLUGIN_STORAGE_REVISION_PATTERN = /^sha256:[0-9a-f]{64}$/;
    const pluginStorageBatchFailpoint = process.env.NODE_ENV === 'test'
        ? String(process.env.POCKETRISU_TEST_PLUGIN_BATCH_FAILPOINT ?? '').trim()
        : '';
    const pluginStorageBatchAcknowledgementDelayMs = process.env.NODE_ENV === 'test'
        ? Math.max(0, Number.parseInt(
            process.env.POCKETRISU_TEST_PLUGIN_BATCH_ACK_DELAY_MS ?? '0',
            10,
        ) || 0)
        : 0;
    
    // Test-only authoritative read failure used by the IP1 integration contract.
    // It is intentionally scoped to the versioned state endpoint.
    const pluginStorageStateFailpoint = process.env.NODE_ENV === 'test'
        ? String(process.env.POCKETRISU_TEST_PLUGIN_STATE_FAILPOINT ?? '').trim()
        : '';
    const pluginStorageViewerTestGateDir = process.env.NODE_ENV === 'test'
        ? String(process.env.POCKETRISU_TEST_PLUGIN_VIEWER_GATE_DIR ?? '').trim()
        : '';
    
    function hitPluginStorageBatchFailpoint(boundary) {
        if (pluginStorageBatchFailpoint === boundary) {
            throw new Error(`Injected plugin storage batch failure at ${boundary}`);
        }
    }
    
    async function waitAtPluginStorageViewerTestGate(isClosed) {
        if (!pluginStorageViewerTestGateDir) return;
        const holdPath = path.join(pluginStorageViewerTestGateDir, 'hold');
        if (!existsSync(holdPath)) return;
        await fs.mkdir(pluginStorageViewerTestGateDir, { recursive: true });
        await fs.writeFile(
            path.join(pluginStorageViewerTestGateDir, 'entered'),
            'snapshot-pinned',
            'utf-8',
        );
        const releasePath = path.join(pluginStorageViewerTestGateDir, 'release');
        while (!isClosed() && existsSync(holdPath) && !existsSync(releasePath)) {
            await new Promise((resolve) => setTimeout(resolve, 10));
        }
    }
    
    async function reportPluginStorageViewerTestProgress(metrics, fileName = 'progress.json') {
        if (!pluginStorageViewerTestGateDir) return;
        await fs.mkdir(pluginStorageViewerTestGateDir, { recursive: true });
        await fs.writeFile(
            path.join(pluginStorageViewerTestGateDir, fileName),
            JSON.stringify(metrics),
            'utf-8',
        );
    }
    
    function parsePluginStorageOwnerRecord(bytes) {
        if (!bytes) return null;
        try {
            const text = bytes.toString('utf-8');
            if (!Buffer.from(text, 'utf-8').equals(bytes)) return null;
            const value = JSON.parse(text);
            return value && typeof value === 'object' && !Array.isArray(value) ? value : null;
        } catch {
            return null;
        }
    }
    
    function isCanonicalPluginStorageOwnerRecord(owner, bytes) {
        if (!owner || !bytes) return false;
        const keys = Object.keys(owner);
        if (keys.length !== 4
            || keys[0] !== 'plugin'
            || keys[1] !== 'updatedAt'
            || keys[2] !== 'revision'
            || keys[3] !== 'generation'
            || typeof owner.plugin !== 'string'
            || owner.plugin.length === 0
            || !owner.plugin.isWellFormed()
            || !Number.isSafeInteger(owner.updatedAt)
            || owner.updatedAt < 0
            || typeof owner.revision !== 'string'
            || !PLUGIN_STORAGE_UUID_PATTERN.test(owner.revision)
            || typeof owner.generation !== 'string'
            || !PLUGIN_STORAGE_UUID_PATTERN.test(owner.generation)) return false;
        return Buffer.from(JSON.stringify(owner), 'utf-8').equals(bytes);
    }
    
    /**
     * Opaque CAS revision for one logical value+owner pair. Including the stored
     * owner incarnation makes a same-value remove/recreate distinguishable while
     * still giving historical rows (without revision metadata) a stable token.
     */
    function pluginStorageRevisionDigest(ownerBytes) {
        const owner = parsePluginStorageOwnerRecord(ownerBytes);
        const incarnation = isCanonicalPluginStorageOwnerRecord(owner, ownerBytes)
            ? owner.revision
            : `legacy:${ownerBytes ? sha256Hex(ownerBytes) : 'unowned'}`;
        const digest = nodeCrypto.createHash('sha256');
        digest.update('pocketrisu-plugin-storage-v1\0', 'utf-8');
        digest.update(incarnation, 'utf-8');
        digest.update('\0', 'utf-8');
        return digest;
    }
    
    function pluginStorageRevision(valueBytes, ownerBytes) {
        if (!valueBytes) return null;
        const digest = pluginStorageRevisionDigest(ownerBytes);
        digest.update(valueBytes);
        return `sha256:${digest.digest('hex')}`;
    }
    
    async function pluginStorageRevisionFromFile(filePath, ownerBytes) {
        const digest = pluginStorageRevisionDigest(ownerBytes);
        for await (const chunk of createReadStream(filePath)) digest.update(chunk);
        return `sha256:${digest.digest('hex')}`;
    }
    
    const MAX_PLUGIN_STORAGE_ARRAY_INDEX = 0xffff_ffff;
    
    function pluginStorageArrayIndex(key) {
        const index = Number(key);
        return Number.isInteger(index)
            && index >= 0
            && index < MAX_PLUGIN_STORAGE_ARRAY_INDEX
            && String(index) === key
            ? index
            : null;
    }
    
    function comparePluginStorageRecordKeys(left, right) {
        const leftIndex = pluginStorageArrayIndex(left);
        const rightIndex = pluginStorageArrayIndex(right);
        if (leftIndex !== null || rightIndex !== null) {
            if (leftIndex === null) return 1;
            if (rightIndex === null) return -1;
            return leftIndex - rightIndex;
        }
        return left < right ? -1 : left > right ? 1 : 0;
    }
    
    async function readPluginStorageState(valueKey, ownerKey) {
        const [valueBytes, ownerBytes] = await Promise.all([
            kvGetAsync(valueKey),
            kvGetAsync(ownerKey),
        ]);
        const owner = parsePluginStorageOwnerRecord(ownerBytes);
        return {
            valueBytes,
            ownerBytes,
            revision: pluginStorageRevision(valueBytes, ownerBytes),
            generation: valueBytes !== null
                && isCanonicalPluginStorageOwnerRecord(owner, ownerBytes)
                ? owner.generation
                : null,
        };
    }

    function registerPluginStorageClearRoute(app) {
        /**
         * Clear optimized plugin save values and their owner sidecars as one logical
         * mutation. The namespace is intentionally fixed server-side: this is the
         * narrow clear primitive, not a caller-controlled batch or prefix API.
         */
        app.post('/api/plugin-storage/clear', async (req, res) => {
            if (!await checkAuth(req, res)) return;
            if (!checkActiveSession(req, res)) return;
        
            try {
                await queueStorageMutation(async () => {
                    if (pluginStorageClearFailpoint === 'pre-transaction') {
                        throw new Error('Injected plugin storage clear failure before transaction');
                    }
                    const publication = await readLivePluginStoragePublication();
                    const { dbObj, generation, manifestState } = publication;
                    const pinnedState = sessionPluginStorageReadState(req);
                    const activeManifest = generation
                        && dbObj?.optimizePluginMemory === true
                        && manifestState.valid
                        && manifestState.manifest?.generation === generation
                        ? manifestState.manifest
                        : null;
                    const legacyPublication = !generation
                        && dbObj?.optimizePluginMemory === true
                        && !manifestState.present;
                    if (
                        (!activeManifest && !legacyPublication)
                        || (pinnedState && (
                            pinnedState.optimized !== true
                            || pinnedState.generation !== generation
                        ))
                    ) {
                        throw pluginStorageNamespaceConflict(
                            'Plugin storage generation changed before clear committed',
                        );
                    }
                    const nextManifest = activeManifest
                        ? createPluginStorageManifest(generation, [], [])
                        : null;
                    const recoverySnapshotToken = newPluginRecoverySnapshotToken();
                    sqliteDb.transaction(() => {
                        kvDelPrefix(PLUGIN_SAVE_PREFIX);
                        if (pluginStorageClearFailpoint === 'transaction') {
                            throw new Error('Injected plugin storage clear transaction failure');
                        }
                        kvDelPrefix(PLUGIN_SAVE_META_PREFIX);
                        writePluginStorageManifest(nextManifest);
                        markPluginRecoverySnapshotDirty(recoverySnapshotToken);
                    })();
                });
            } catch (error) {
                if (isImportInProgressError(error)) return sendImportBusy(res);
                if (error?.pluginStorageNamespaceConflict) {
                    return res.status(409).json({
                        error: error.message,
                        code: 'PLUGIN_STORAGE_GENERATION_CONFLICT',
                        commitOutcome: 'not-committed',
                        commitOutcomeUnknown: false,
                    });
                }
                logger.error('[PluginStorage] Atomic clear rolled back:', error);
                return res.status(500).json({
                    error: 'Plugin storage clear was not committed',
                    code: 'PLUGIN_STORAGE_CLEAR_NOT_COMMITTED',
                    retryAfter: 0,
                    retryable: true,
                    commitOutcome: 'not-committed',
                    commitOutcomeUnknown: false,
                });
            }
        
            // This is a known commit even if the response is lost below.
            schedulePluginRecoverySnapshot();
        
            // A response lost after this point cannot prove whether the transaction
            // committed. The client labels that outcome unknown and may safely retry
            // because clearing this fixed namespace is idempotent.
            if (pluginStorageClearFailpoint === 'response') {
                res.destroy();
                return;
            }
            res.json({
                success: true,
                commitOutcome: 'committed',
                commitOutcomeUnknown: false,
            });
        });
    }

    function registerPluginStorageStateRoutes(app) {
        const PLUGIN_STORAGE_JSON_CONTENT_TYPE = 'application/json';
        const PLUGIN_STORAGE_LOSSLESS_CONTENT_TYPE = 'application/octet-stream';
        
        async function readAuthoritativePluginStorageState(req, valueKey, requestedGeneration) {
            const ownerKey = `${PLUGIN_SAVE_META_PREFIX}${valueKey.slice(PLUGIN_SAVE_PREFIX.length)}`;
            return await queueStorageReadAfterImports(async () => {
                const publication = await readLivePluginStoragePublication();
                const { dbObj, generation, manifestState } = publication;
                const pinnedState = sessionPluginStorageReadState(req);
                const expectedState = requestedGeneration !== undefined
                    ? { optimized: true, generation: requestedGeneration }
                    : pinnedState;
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
                            'Read database.bin before reading authoritative plugin storage state',
                        );
                    }
                    return {
                        state: await readPluginStorageState(valueKey, ownerKey),
                        publicationGeneration: null,
                        publicationRevision: null,
                    };
                }
                if (
                    expectedState.optimized !== (dbObj?.optimizePluginMemory === true)
                    || expectedState.generation !== generation
                    || (requestedGeneration !== undefined && pinnedState && (
                        pinnedState.optimized !== true
                        || pinnedState.generation !== requestedGeneration
                    ))
                ) {
                    throw pluginStorageNamespaceConflict(
                        'Plugin storage generation changed before the state could be read',
                    );
                }
                if (generation) {
                    if (!activeManifest) {
                        throw pluginStorageNamespaceConflict(
                            'The selected plugin storage generation has no matching manifest',
                        );
                    }
                    if (isHashedPluginSaveStorageKey(valueKey, PLUGIN_SAVE_PREFIX)
                        && (activeManifest.valueKeys.includes(valueKey)
                            || activeManifest.metaKeys.includes(ownerKey))) {
                        decodeManifestPluginSaveStorageKey(
                            activeManifest,
                            valueKey,
                            PLUGIN_SAVE_PREFIX,
                        );
                    }
                    const [valueBytes, ownerBytes] = await Promise.all([
                        activeManifest.valueKeys.includes(valueKey)
                            ? kvGetAsync(valueKey)
                            : null,
                        activeManifest.metaKeys.includes(ownerKey)
                            ? kvGetAsync(ownerKey)
                            : null,
                    ]);
                    const owner = parsePluginStorageOwnerRecord(ownerBytes);
                    return {
                        state: {
                            valueBytes,
                            ownerBytes,
                            revision: pluginStorageRevision(valueBytes, ownerBytes),
                            generation: valueBytes !== null
                                && isCanonicalPluginStorageOwnerRecord(owner, ownerBytes)
                                ? owner.generation
                                : null,
                        },
                        publicationGeneration: generation,
                        publicationRevision: manifestState.revision,
                    };
                }
                if (!legacyPublication) {
                    throw pluginStorageNamespaceConflict(
                        'The legacy plugin storage publication changed before the state could be read',
                    );
                }
                return {
                    state: await readPluginStorageState(valueKey, ownerKey),
                    publicationGeneration: null,
                    publicationRevision: null,
                };
            });
        }
        
        function handlePluginStorageStateRead({ binary }) {
            return async (req, res, next) => {
                if (!await checkAuth(req, res)) return;
                const firstHeader = (value) => Array.isArray(value) ? value[0] : value;
                const filePath = firstHeader(req.headers['file-path']);
                const requestedGeneration = firstHeader(req.headers['x-plugin-storage-generation']);
                if (typeof filePath !== 'string' || !isHex(filePath)) {
                    return res.status(400).json({
                        success: false,
                        error: 'A valid value row path is required.',
                        code: 'INVALID_PLUGIN_STORAGE_STATE_READ',
                    });
                }
                if (requestedGeneration !== undefined
                    && (typeof requestedGeneration !== 'string' || requestedGeneration.length === 0)) {
                    return res.status(400).json({
                        success: false,
                        error: 'Plugin storage generation must be a non-empty string.',
                        code: 'INVALID_PLUGIN_STORAGE_STATE_READ',
                    });
                }
        
                if (pluginStorageStateFailpoint === 'read') {
                    res.setHeader('Retry-After', '0');
                    return res.status(503).json({
                        success: false,
                        error: 'Injected plugin storage state read failure.',
                        code: 'TEMPORARY_STORAGE_FAILURE',
                        retryAfter: 0,
                        retryable: true,
                    });
                }
        
                try {
                    const { decodedKey: valueKey } = decodeAndCanonicalizeHexPath(filePath);
                    if (isHashedPluginSaveStorageKey(valueKey, PLUGIN_SAVE_PREFIX)) {
                        assertArchiveSafePluginSaveStorageKey(valueKey);
                    } else {
                        decodePluginSaveStorageKey(valueKey, PLUGIN_SAVE_PREFIX);
                    }
                    const publication = await readAuthoritativePluginStorageState(
                        req,
                        valueKey,
                        requestedGeneration,
                    );
                    const { state } = publication;
                    if (publication.publicationGeneration !== null) {
                        res.setHeader(
                            'x-plugin-storage-publication-generation',
                            publication.publicationGeneration,
                        );
                    }
                    if (publication.publicationRevision !== null) {
                        res.setHeader(
                            'x-plugin-storage-publication-revision',
                            publication.publicationRevision,
                        );
                    }
                    if (binary) {
                        res.setHeader(
                            'x-plugin-storage-missing',
                            state.valueBytes === null ? '1' : '0',
                        );
                        if (state.valueBytes === null) return res.status(204).end();
        
                        const codec = pluginStorageCodecForBuffer(state.valueBytes);
                        res.setHeader(
                            'Content-Type',
                            codec === PLUGIN_STORAGE_LOSSLESS_CODEC
                                ? PLUGIN_STORAGE_LOSSLESS_CONTENT_TYPE
                                : PLUGIN_STORAGE_JSON_CONTENT_TYPE,
                        );
                        res.setHeader('Content-Length', String(state.valueBytes.byteLength));
                        res.setHeader('x-plugin-storage-codec', codec);
                        res.setHeader(
                            'x-plugin-storage-byte-length',
                            String(state.valueBytes.byteLength),
                        );
                        res.setHeader(
                            'x-plugin-storage-content-digest',
                            `sha256:${sha256Hex(state.valueBytes)}`,
                        );
                        res.setHeader('x-plugin-storage-row-revision', state.revision);
                        if (state.generation !== null) {
                            res.setHeader('x-plugin-storage-row-generation', state.generation);
                        }
                        return res.status(200).end(state.valueBytes);
                    }
                    return res.json({
                        success: true,
                        missing: state.valueBytes === null,
                        value: state.valueBytes?.toString('base64'),
                        revision: state.revision,
                        generation: state.generation,
                    });
                } catch (error) {
                    if (error?.pluginStorageNamespaceConflict) {
                        return res.status(409).json({
                            success: false,
                            error: error.message,
                            code: 'PLUGIN_STORAGE_GENERATION_CONFLICT',
                        });
                    }
                    if (error instanceof RangeError || error?.message?.includes('plugin storage key')) {
                        return res.status(400).json({
                            success: false,
                            error: error instanceof Error ? error.message : String(error),
                            code: 'INVALID_PLUGIN_STORAGE_STATE_READ',
                        });
                    }
                    next(error);
                }
            };
        }
        
        /** Retained byte-compatible JSON/base64 read for stale-client recovery. */
        app.get('/api/plugin-storage/state', handlePluginStorageStateRead({ binary: false }));
        /** Current client read: exact stored row bytes with extensible codec/identity headers. */
        app.get('/api/plugin-storage/state/raw', handlePluginStorageStateRead({ binary: true }));
    }

    function registerPluginStorageManagementRoutes(app) {
        const pluginStorageRecoveryManagementSecret = nodeCrypto.randomBytes(32);
        
        function pluginStorageRecoveryManagementKind(encodedKey) {
            if (encodedKey === PLUGIN_STORAGE_MANIFEST_KEY) return 'manifest';
            if (encodedKey.startsWith(PLUGIN_SAVE_META_PREFIX)) return 'metadata';
            if (encodedKey.startsWith(PLUGIN_SAVE_PREFIX)) return 'value';
            return 'storage';
        }
        
        function pluginStorageRecoveryManagementToken(context, issue) {
            return nodeCrypto.createHmac('sha256', pluginStorageRecoveryManagementSecret)
                .update(JSON.stringify([
                    issue.code,
                    issue.encodedKey,
                    getSessionLockEpoch(),
                    context.databaseRevision,
                    context.generation ?? '',
                    context.manifestRevision ?? '',
                    issue.externalHash ?? '',
                    issue.inlineEntry?.canonicalHash ?? '',
                    issue.externalAvailable === true,
                    issue.inlineEntry !== null,
                    issue.owned === true,
                    issue.canUseInline === true,
                    issue.canDelete === true,
                ]))
                .digest('base64url');
        }
        
        function publicPluginStorageRecoveryManagementIssue(context, issue) {
            return {
                code: issue.code,
                encodedKey: issue.encodedKey,
                kind: pluginStorageRecoveryManagementKind(issue.encodedKey),
                inlineAvailable: issue.inlineEntry !== null,
                externalAvailable: issue.externalAvailable === true,
                externalSize: Number.isSafeInteger(issue.externalSize) ? issue.externalSize : null,
                actions: {
                    download: issue.externalAvailable === true,
                    useInline: issue.canUseInline === true,
                    delete: issue.canDelete === true,
                },
                token: pluginStorageRecoveryManagementToken(context, issue),
            };
        }
        
        function internalPluginStorageRecoveryManagementIssue(code, encodedKey, overrides = {}) {
            return {
                code,
                encodedKey,
                rawKey: null,
                prefix: null,
                inlineEntry: null,
                externalAvailable: false,
                externalSize: null,
                externalHash: null,
                owned: false,
                canUseInline: false,
                canDelete: false,
                ...overrides,
            };
        }
        
        /**
         * Rebuild an encoded-key-only recovery management view from the live optimized
         * publication. Values remain server-side; the returned action token binds the
         * database row, manifest, inline candidate, and exact external bytes.
         */
        async function inspectOptimizedPluginStorageRecoveryManagement() {
            await flushPendingDb();
            const rawDatabase = kvGet('database/database.bin');
            if (!rawDatabase) throw new Error('Database not found');
            const liveDb = await decodeAuthoritativeDatabase(rawDatabase);
            const databaseRevision = sha256Hex(rawDatabase);
            if (liveDb?.optimizePluginMemory !== true) {
                return {
                    mode: 'inline',
                    checkedAt: Date.now(),
                    context: {
                        databaseRevision,
                        generation: null,
                        manifestRevision: null,
                    },
                    issues: [],
                    liveDb,
                    manifestEntry: null,
                };
            }
        
            const collectionIssues = [];
            const inlineValues = collectOptimizedBootInlineEntries(
                liveDb,
                'pluginCustomStorage',
                PLUGIN_SAVE_PREFIX,
                collectionIssues,
            );
            const inlineMeta = collectOptimizedBootInlineEntries(
                liveDb,
                'pluginStorageMeta',
                PLUGIN_SAVE_META_PREFIX,
                collectionIssues,
            );
            const inlineValueByStorageKey = new Map(
                inlineValues.entries.map(entry => [entry.storageKey, entry]),
            );
            const inlineMetaByStorageKey = new Map(
                inlineMeta.entries.map(entry => [entry.storageKey, entry]),
            );
            const generation = pluginStorageGeneration(liveDb);
            const manifestBytes = kvGet(PLUGIN_STORAGE_MANIFEST_KEY);
            const manifestEntry = pluginStorageManifestCache.read();
            const manifestState = manifestEntry.state;
            const manifest = generation
                && manifestState.valid === true
                && manifestState.manifest?.generation === generation
                ? manifestState.manifest
                : null;
            const context = {
                databaseRevision,
                generation,
                manifestRevision: Buffer.isBuffer(manifestBytes) ? sha256Hex(manifestBytes) : null,
            };
            const canMutatePublication = generation === null || manifest !== null;
            const issues = collectionIssues.map(issue => internalPluginStorageRecoveryManagementIssue(
                issue.code,
                issue.encodedKey,
            ));
        
            if (manifestState.present && manifestState.valid !== true) {
                issues.push(internalPluginStorageRecoveryManagementIssue(
                    'invalid-json',
                    PLUGIN_STORAGE_MANIFEST_KEY,
                    {
                        externalAvailable: Buffer.isBuffer(manifestBytes),
                        externalSize: Buffer.isBuffer(manifestBytes) ? manifestBytes.length : null,
                        externalHash: Buffer.isBuffer(manifestBytes) ? sha256Hex(manifestBytes) : null,
                    },
                ));
            }
        
            const scanPrefix = async (prefix, listed, inlineByStorageKey) => {
                const ownedKeys = generation && manifest
                    ? new Set(prefix === PLUGIN_SAVE_META_PREFIX ? manifest.metaKeys : manifest.valueKeys)
                    : null;
                for (const encodedKey of listed) {
                    let rawKey = null;
                    try {
                        rawKey = decodeOptimizedBootStorageKey(encodedKey, prefix, manifest);
                    } catch {
                        const inlineEntry = inlineByStorageKey.get(encodedKey) ?? null;
                        rawKey = inlineEntry?.rawKey ?? null;
                        const externalBytes = kvGet(encodedKey);
                        issues.push(internalPluginStorageRecoveryManagementIssue(
                            'invalid-encoded-key',
                            encodedKey,
                            {
                                rawKey,
                                prefix,
                                inlineEntry,
                                externalAvailable: Buffer.isBuffer(externalBytes),
                                externalSize: Buffer.isBuffer(externalBytes) ? externalBytes.length : null,
                                externalHash: Buffer.isBuffer(externalBytes) ? sha256Hex(externalBytes) : null,
                                owned: ownedKeys?.has(encodedKey) === true,
                                canUseInline: inlineEntry !== null && canMutatePublication,
                                canDelete: Buffer.isBuffer(externalBytes)
                                    && inlineEntry === null
                                    && canMutatePublication,
                            },
                        ));
                        continue;
                    }
        
                    const inlineEntry = inlineByStorageKey.get(encodedKey) ?? null;
                    let externalBytes = null;
                    try {
                        externalBytes = await kvGetAsync(encodedKey);
                    } catch {
                        issues.push(internalPluginStorageRecoveryManagementIssue(
                            'read-failed',
                            encodedKey,
                            { rawKey, prefix, inlineEntry },
                        ));
                        continue;
                    }
                    if (!Buffer.isBuffer(externalBytes)) {
                        issues.push(internalPluginStorageRecoveryManagementIssue(
                            'read-failed',
                            encodedKey,
                            { rawKey, prefix, inlineEntry },
                        ));
                        continue;
                    }
        
                    const externalHash = sha256Hex(externalBytes);
                    const owned = generation === null || ownedKeys?.has(encodedKey) === true;
                    const common = {
                        rawKey,
                        prefix,
                        inlineEntry,
                        externalAvailable: true,
                        externalSize: externalBytes.length,
                        externalHash,
                        owned,
                    };
                    if (generation && !owned) {
                        issues.push(internalPluginStorageRecoveryManagementIssue(
                            'read-failed',
                            encodedKey,
                            {
                                ...common,
                                canUseInline: inlineEntry !== null && canMutatePublication,
                                canDelete: inlineEntry === null && canMutatePublication,
                            },
                        ));
                        continue;
                    }
        
                    let canonicalHash = null;
                    try {
                        canonicalHash = sha256Hex(canonicalizeOptimizedPluginStorageRow(
                            encodedKey,
                            prefix,
                            externalBytes,
                        ));
                    } catch (error) {
                        issues.push(internalPluginStorageRecoveryManagementIssue(
                            error instanceof SyntaxError ? 'invalid-json' : 'unsupported-json',
                            encodedKey,
                            {
                                ...common,
                                canUseInline: inlineEntry !== null && canMutatePublication,
                                canDelete: inlineEntry === null && canMutatePublication,
                            },
                        ));
                        continue;
                    }
                    if (inlineEntry && canonicalHash !== inlineEntry.canonicalHash) {
                        issues.push(internalPluginStorageRecoveryManagementIssue(
                            'conflicting-copies',
                            encodedKey,
                            {
                                ...common,
                                canUseInline: canMutatePublication,
                            },
                        ));
                    }
                }
            };
        
            let listedValues = [];
            let listedMeta = [];
            try {
                listedValues = kvList(PLUGIN_SAVE_PREFIX);
            } catch {
                issues.push(internalPluginStorageRecoveryManagementIssue('list-failed', PLUGIN_SAVE_PREFIX));
            }
            try {
                listedMeta = kvList(PLUGIN_SAVE_META_PREFIX);
            } catch {
                issues.push(internalPluginStorageRecoveryManagementIssue('list-failed', PLUGIN_SAVE_META_PREFIX));
            }
            await scanPrefix(PLUGIN_SAVE_PREFIX, listedValues, inlineValueByStorageKey);
            await scanPrefix(PLUGIN_SAVE_META_PREFIX, listedMeta, inlineMetaByStorageKey);
        
            // Deleting a value also removes its ownership sidecar. Do not offer that
            // action while a recoverable inline owner copy would be discarded with it.
            for (const issue of issues) {
                if (!issue.canDelete || issue.prefix !== PLUGIN_SAVE_PREFIX || issue.rawKey === null) continue;
                const inlineMetaKey = encodePluginSaveStorageKey(issue.rawKey, PLUGIN_SAVE_META_PREFIX);
                if (inlineMetaByStorageKey.has(inlineMetaKey)) issue.canDelete = false;
            }
        
            return {
                mode: 'optimized',
                checkedAt: Date.now(),
                context,
                issues,
                liveDb,
                manifestEntry,
            };
        }
        
        function publicOptimizedPluginStorageRecoveryManagementInspection(inspection) {
            return {
                success: true,
                mode: inspection.mode,
                checkedAt: inspection.checkedAt,
                issues: inspection.issues.map(issue => (
                    publicPluginStorageRecoveryManagementIssue(inspection.context, issue)
                )),
            };
        }
        
        function findOptimizedPluginStorageRecoveryManagementIssue(inspection, encodedKey, token) {
            return inspection.issues.find(issue => (
                issue.encodedKey === encodedKey
                && pluginStorageRecoveryManagementToken(inspection.context, issue) === token
            )) ?? null;
        }
        
        function preparePluginStorageRecoveryManifestUpdate(inspection, changes) {
            if (inspection.context.generation === null) return null;
            return pluginStorageManifestCache.prepareUpdate(inspection.manifestEntry, changes);
        }
        
        function pluginStorageRecoveryProofChanged() {
            const error = new Error('Plugin storage recovery proof changed');
            error.pluginStorageRecoveryStale = true;
            return error;
        }
        
        function assertPluginStorageRecoveryProofCurrent(inspection, issue) {
            try {
                const databaseBytes = kvGet('database/database.bin');
                if (!Buffer.isBuffer(databaseBytes)
                    || sha256Hex(databaseBytes) !== inspection.context.databaseRevision) {
                    throw pluginStorageRecoveryProofChanged();
                }
                const manifestBytes = kvGet(PLUGIN_STORAGE_MANIFEST_KEY);
                const manifestRevision = Buffer.isBuffer(manifestBytes)
                    ? sha256Hex(manifestBytes)
                    : null;
                if (manifestRevision !== inspection.context.manifestRevision) {
                    throw pluginStorageRecoveryProofChanged();
                }
                const externalBytes = kvGet(issue.encodedKey);
                if (!Buffer.isBuffer(externalBytes)
                    || externalBytes.length !== issue.externalSize
                    || sha256Hex(externalBytes) !== issue.externalHash) {
                    throw pluginStorageRecoveryProofChanged();
                }
            } catch (error) {
                if (error?.pluginStorageRecoveryStale) throw error;
                throw pluginStorageRecoveryProofChanged();
            }
        }
        
        function resolveOptimizedPluginStorageRecoveryIssue(inspection, issue, action) {
            let manifestUpdate = null;
            let committedManifestBytes = null;
            let committedPublicationRevision = null;
            const recoverySnapshotToken = newPluginRecoverySnapshotToken();
            sqliteDb.transaction(() => {
                assertPluginStorageRecoveryProofCurrent(inspection, issue);
                // Suspicious rows may have been restored or edited outside the ordinary
                // mutation API. Repair derived quota accounting inside the same write
                // transaction that revalidates and resolves the selected row.
                reconcilePluginStorageUsage();
        
                if (action === 'use-inline') {
                    if (!issue.canUseInline || !issue.inlineEntry || issue.rawKey === null || !issue.prefix) {
                        throw new TypeError('The inline recovery action is unavailable.');
                    }
                    const rowBytes = serializePluginStorageRow(issue.encodedKey, issue.inlineEntry.value);
                    if (inspection.context.generation && !issue.owned) {
                        manifestUpdate = preparePluginStorageRecoveryManifestUpdate(inspection, {
                            valueAdds: issue.prefix === PLUGIN_SAVE_PREFIX ? [issue.encodedKey] : [],
                            metaAdds: issue.prefix === PLUGIN_SAVE_META_PREFIX ? [issue.encodedKey] : [],
                            rawKeys: [issue.rawKey],
                        });
                    }
                    withPluginStorageQuotaPlan(
                        issue.prefix === PLUGIN_SAVE_PREFIX
                            ? [{ key: issue.encodedKey, size: rowBytes.length }]
                            : [],
                        () => {
                            kvSet(issue.encodedKey, rowBytes);
                            if (manifestUpdate) {
                                committedManifestBytes = writePluginStorageManifest(manifestUpdate.manifest);
                            }
                            markPluginRecoverySnapshotDirty(recoverySnapshotToken);
                            committedPublicationRevision = kvGetPluginStoragePublicationRevision();
                        },
                    );
                } else if (action === 'delete') {
                    if (!issue.canDelete || !issue.externalAvailable) {
                        throw new TypeError('The delete recovery action is unavailable.');
                    }
                    const valueDeletes = [];
                    const metaDeletes = [];
                    const deleteKeys = [issue.encodedKey];
                    if (issue.prefix === PLUGIN_SAVE_PREFIX) {
                        valueDeletes.push(issue.encodedKey);
                        if (issue.rawKey !== null) {
                            const ownerKey = encodePluginSaveStorageKey(issue.rawKey, PLUGIN_SAVE_META_PREFIX);
                            metaDeletes.push(ownerKey);
                            if (kvSize(ownerKey) !== null) deleteKeys.push(ownerKey);
                        }
                    } else if (issue.prefix === PLUGIN_SAVE_META_PREFIX) {
                        metaDeletes.push(issue.encodedKey);
                    }
                    if (inspection.context.generation) {
                        manifestUpdate = preparePluginStorageRecoveryManifestUpdate(inspection, {
                            valueDeletes,
                            metaDeletes,
                        });
                    }
                    withPluginStorageQuotaPlan(
                        deleteKeys
                            .filter(key => key.startsWith(PLUGIN_SAVE_PREFIX))
                            .map(key => ({ key, size: null })),
                        () => {
                            for (const key of deleteKeys) kvDel(key);
                            if (manifestUpdate) {
                                committedManifestBytes = writePluginStorageManifest(manifestUpdate.manifest);
                            }
                            markPluginRecoverySnapshotDirty(recoverySnapshotToken);
                            committedPublicationRevision = kvGetPluginStoragePublicationRevision();
                        },
                    );
                } else {
                    throw new TypeError('Unknown plugin storage recovery action.');
                }
            })();
        
            if (manifestUpdate) {
                pluginStorageManifestCache.publishPrepared(manifestUpdate, {
                    revision: committedPublicationRevision,
                    manifestRevision: `sha256:${sha256Hex(committedManifestBytes)}`,
                });
            }
            schedulePluginRecoverySnapshot();
        }
        /**
         * Read the active publication manifest and its physically present row set in
         * one generation-bound snapshot. Clients use this instead of issuing two list
         * requests plus a separate manifest read for every batch or enumeration.
         */
        // Quota usage counts physical JSON bytes (including quarantined rows), while
        // the viewer reports UTF-8 bytes of each decoded logical value. Cache that
        // exact aggregate until a low-level plugin-value mutation invalidates it.
        let pluginStorageViewerTotalSizeCache = null;
        const PLUGIN_STORAGE_VIEWER_SNAPSHOT_CAP = process.env.NODE_ENV === 'test'
            ? Math.max(1, Math.min(8, Number.parseInt(
                process.env.POCKETRISU_TEST_PLUGIN_VIEWER_SNAPSHOT_CAP ?? '2',
                10,
            ) || 2))
            : 2;
        let pluginStorageViewerActiveSnapshots = 0;
        let pluginStorageViewerMaxActiveSnapshots = 0;
        const pluginStorageViewerSnapshotWaiters = [];
        const pluginStorageViewerBackfills = new Map();
        const pluginStorageViewerTestCounters = {
            backfillPasses: 0,
            pageValueReuses: 0,
        };
        
        async function reportPluginStorageViewerSnapshotState() {
            if (!pluginStorageViewerTestGateDir) return;
            await reportPluginStorageViewerTestProgress({
                active: pluginStorageViewerActiveSnapshots,
                queued: pluginStorageViewerSnapshotWaiters.length,
                maxActive: pluginStorageViewerMaxActiveSnapshots,
                cap: PLUGIN_STORAGE_VIEWER_SNAPSHOT_CAP,
                ...pluginStorageViewerTestCounters,
            }, 'snapshot-state.json');
        }
        
        function releasePluginStorageViewerSnapshotSlot() {
            pluginStorageViewerActiveSnapshots = Math.max(0, pluginStorageViewerActiveSnapshots - 1);
            while (pluginStorageViewerSnapshotWaiters.length > 0) {
                const waiter = pluginStorageViewerSnapshotWaiters.shift();
                if (waiter.signal?.aborted) continue;
                waiter.signal?.removeEventListener('abort', waiter.onAbort);
                pluginStorageViewerActiveSnapshots += 1;
                pluginStorageViewerMaxActiveSnapshots = Math.max(
                    pluginStorageViewerMaxActiveSnapshots,
                    pluginStorageViewerActiveSnapshots,
                );
                waiter.resolve(releasePluginStorageViewerSnapshotSlot);
                break;
            }
            reportPluginStorageViewerSnapshotState().catch(() => {});
        }
        
        function acquirePluginStorageViewerSnapshotSlot(signal) {
            throwIfSignalAborted(signal);
            if (pluginStorageViewerActiveSnapshots < PLUGIN_STORAGE_VIEWER_SNAPSHOT_CAP) {
                pluginStorageViewerActiveSnapshots += 1;
                pluginStorageViewerMaxActiveSnapshots = Math.max(
                    pluginStorageViewerMaxActiveSnapshots,
                    pluginStorageViewerActiveSnapshots,
                );
                reportPluginStorageViewerSnapshotState().catch(() => {});
                return Promise.resolve(releasePluginStorageViewerSnapshotSlot);
            }
            return new Promise((resolve, reject) => {
                const waiter = {
                    signal,
                    resolve,
                    reject,
                    onAbort: null,
                };
                waiter.onAbort = () => {
                    const index = pluginStorageViewerSnapshotWaiters.indexOf(waiter);
                    if (index >= 0) pluginStorageViewerSnapshotWaiters.splice(index, 1);
                    reject(signal.reason instanceof Error
                        ? signal.reason
                        : new DOMException('Plugin storage viewer cancelled', 'AbortError'));
                    reportPluginStorageViewerSnapshotState().catch(() => {});
                };
                signal?.addEventListener('abort', waiter.onAbort, { once: true });
                pluginStorageViewerSnapshotWaiters.push(waiter);
                reportPluginStorageViewerSnapshotState().catch(() => {});
            });
        }
        
        async function waitAtPluginStorageViewerSnapshotTestGate(isClosed) {
            if (!pluginStorageViewerTestGateDir) return;
            const holdPath = path.join(pluginStorageViewerTestGateDir, 'snapshot-hold');
            if (!existsSync(holdPath)) return;
            await fs.mkdir(pluginStorageViewerTestGateDir, { recursive: true });
            await fs.writeFile(
                path.join(pluginStorageViewerTestGateDir, 'snapshot-entered'),
                'snapshot-pinned',
                'utf-8',
            );
            await reportPluginStorageViewerSnapshotState();
            const releasePath = path.join(pluginStorageViewerTestGateDir, 'snapshot-release');
            while (!isClosed() && existsSync(holdPath) && !existsSync(releasePath)) {
                await new Promise((resolve) => setTimeout(resolve, 10));
            }
        }
        
        function closePluginStorageViewerContext(context) {
            if (!context || context.closed) return;
            context.closed = true;
            context.snapshot?.close();
            context.releaseSnapshotSlot?.();
        }
        
        function pluginStorageViewerParseOne(metrics, operation) {
            metrics.activeRowParses += 1;
            metrics.maxRowParses = Math.max(metrics.maxRowParses, metrics.activeRowParses);
            try {
                return operation();
            } finally {
                metrics.activeRowParses -= 1;
            }
        }
        
        function pluginStorageViewerOwner(record) {
            return record
                && typeof record.plugin === 'string'
                && record.plugin.length > 0
                && record.plugin.isWellFormed()
                ? record.plugin
                : null;
        }
        
        function selectPluginStorageViewerRows(context, options, ownerByStorageKey = null) {
            const normalizedQuery = options.keyQuery.toLowerCase();
            const keyMatchedValues = context.authoritativeValues
                .filter(({ key }) => !normalizedQuery || key.toLowerCase().includes(normalizedQuery))
                .sort((left, right) => comparePluginStorageRecordKeys(left.key, right.key));
            const candidateOwnerStorageKeys = keyMatchedValues
                .map(({ key }) => encodePluginSaveStorageKey(key, PLUGIN_SAVE_META_PREFIX))
                .filter((storageKey) => (
                    context.manifestMeta.has(storageKey) && context.physicalMeta.has(storageKey)
                ));
            let ownerFacets;
            let matchingOwnerStorageKeys = null;
            if (ownerByStorageKey) {
                const counts = new Map();
                for (const storageKey of candidateOwnerStorageKeys) {
                    const owner = ownerByStorageKey.get(storageKey);
                    if (owner !== undefined) counts.set(owner, (counts.get(owner) ?? 0) + 1);
                }
                ownerFacets = [...counts]
                    .map(([owner, count]) => ({ owner, count }))
                    .sort((left, right) => left.owner < right.owner ? -1 : left.owner > right.owner ? 1 : 0);
                if (options.ownerQueryValue !== undefined || options.unknownOwner) {
                    matchingOwnerStorageKeys = new Set(candidateOwnerStorageKeys.filter((storageKey) => {
                        const owner = ownerByStorageKey.get(storageKey);
                        return options.ownerQueryValue !== undefined
                            ? owner === options.ownerQuery
                            : owner !== undefined;
                    }));
                }
            } else {
                ownerFacets = context.snapshot.viewerOwnerFacets(candidateOwnerStorageKeys)
                    .sort((left, right) => left.owner < right.owner ? -1 : left.owner > right.owner ? 1 : 0);
                if (options.ownerQueryValue !== undefined || options.unknownOwner) {
                    matchingOwnerStorageKeys = new Set(context.snapshot.viewerOwnerKeys(
                        candidateOwnerStorageKeys,
                        options.ownerQueryValue !== undefined ? options.ownerQuery : null,
                    ));
                }
            }
            const knownOwnerCount = ownerFacets.reduce((sum, facet) => sum + facet.count, 0);
            const unknownOwnerCount = keyMatchedValues.length - knownOwnerCount;
            const ownedValues = keyMatchedValues.filter(({ key }) => {
                const ownerStorageKey = encodePluginSaveStorageKey(key, PLUGIN_SAVE_META_PREFIX);
                if (options.unknownOwner) return !matchingOwnerStorageKeys.has(ownerStorageKey);
                if (options.ownerQueryValue !== undefined) {
                    return matchingOwnerStorageKeys.has(ownerStorageKey);
                }
                return true;
            });
            const total = ownedValues.length;
            const pageCount = Math.max(1, Math.ceil(total / options.pageSize));
            const boundedPage = Math.min(options.page, pageCount - 1);
            const pageRows = ownedValues.slice(
                boundedPage * options.pageSize,
                Math.min(total, (boundedPage + 1) * options.pageSize),
            );
            return {
                ownerFacets,
                unknownOwnerCount,
                ownerFacetTotal: keyMatchedValues.length,
                total,
                pageCount,
                boundedPage,
                pageRows,
            };
        }
        
        function pluginStorageViewerEntry(context, descriptor, valueBytes, value, ownerBytes) {
            const ownerRecord = ownerBytes === null
                ? null
                : pluginStorageViewerParseOne(
                    context.metrics,
                    () => parsePluginStorageOwnerRecord(ownerBytes),
                );
            const owner = pluginStorageViewerOwner(ownerRecord);
            const text = pluginStorageViewerValueText(value);
            const valueType = value === null
                ? 'object'
                : value === undefined || text === ''
                    ? 'empty'
                    : Array.isArray(value)
                        ? 'array'
                        : typeof value;
            const codec = pluginStorageCodecForBuffer(valueBytes);
            const editor = codec === PLUGIN_STORAGE_JSON_CODEC
                ? {
                    codec,
                    kind: typeof value === 'string' ? 'string' : 'json',
                    // JSON-v1 is already the canonical faithful representation. The
                    // display text deliberately remains unchanged for facets/search.
                    text: typeof value === 'string'
                        ? JSON.stringify(value)
                        : value === null ? 'null' : text,
                }
                : { codec, kind: 'readonly', text: null };
            const revision = pluginStorageRevision(valueBytes, ownerBytes);
            const size = Buffer.byteLength(text, 'utf-8');
            const contentHash = `sha256:${sha256Hex(Buffer.from(JSON.stringify([
                descriptor.key,
                owner,
                text,
                size,
                valueType,
                editor.codec,
                editor.kind,
                editor.text,
                revision,
            ]), 'utf-8'))}`;
            return {
                event: 'entry',
                key: descriptor.key,
                owner,
                text,
                size,
                valueType,
                editor,
                revision,
                contentHash,
            };
        }
        
        async function assemblePluginStorageViewerEntries(
            context,
            pageRows,
            isClosed,
            reusedValues = new Map(),
        ) {
            const entries = [];
            for (const descriptor of pageRows) {
                await new Promise((resolve) => setImmediate(resolve));
                throwIfSignalAborted(context.signal);
                if (isClosed()) throw context.signal.reason;
                let retained = reusedValues.get(descriptor.storageKey);
                if (!retained) {
                    const valueBytes = context.snapshot.kvGet(descriptor.storageKey);
                    context.metrics.valueReads += 1;
                    if (valueBytes === null) {
                        throw pluginStorageNamespaceConflict(
                            'A plugin storage viewer row disappeared from its pinned snapshot',
                        );
                    }
                    retained = {
                        valueBytes,
                        value: pluginStorageViewerParseOne(
                            context.metrics,
                            () => validatePluginStorageRow(descriptor.storageKey, valueBytes),
                        ),
                    };
                }
                const ownerStorageKey = encodePluginSaveStorageKey(
                    descriptor.key,
                    PLUGIN_SAVE_META_PREFIX,
                );
                let ownerBytes = null;
                if (context.manifestMeta.has(ownerStorageKey)
                    && context.physicalMeta.has(ownerStorageKey)) {
                    ownerBytes = context.snapshot.kvGet(ownerStorageKey);
                    context.metrics.ownerReads += 1;
                }
                entries.push(pluginStorageViewerEntry(
                    context,
                    descriptor,
                    retained.valueBytes,
                    retained.value,
                    ownerBytes,
                ));
                await reportPluginStorageViewerTestProgress(context.metrics);
            }
            return entries;
        }
        
        function finalizePluginStorageViewerAssembly(context, options, selection, totalBytes, entries) {
            const pageTokenEntries = entries.map((entry) => [entry.key, entry.contentHash]);
            const pageTokenMaterial = JSON.stringify([
                'pocketrisu-plugin-storage-viewer-page-v3',
                context.generation,
                context.manifestState.revision,
                context.databaseRevision,
                selection.boundedPage,
                options.pageSize,
                options.keyQuery,
                options.ownerQueryValue === undefined ? null : options.ownerQuery,
                options.unknownOwner,
                selection.ownerFacets.map((facet) => [facet.owner, facet.count]),
                selection.unknownOwnerCount,
                pageTokenEntries,
            ]);
            return {
                meta: {
                    event: 'meta',
                    version: 2,
                    generation: context.generation,
                    manifestRevision: context.manifestState.revision,
                    databaseRevision: context.databaseRevision,
                    page: selection.boundedPage,
                    pageSize: options.pageSize,
                    pageCount: selection.pageCount,
                    total: selection.total,
                    totalBytes,
                    ownerFacets: selection.ownerFacets,
                    unknownOwnerCount: selection.unknownOwnerCount,
                    ownerFacetTotal: selection.ownerFacetTotal,
                },
                entries,
                done: {
                    event: 'done',
                    pageToken: `sha256:${sha256Hex(Buffer.from(pageTokenMaterial, 'utf-8'))}`,
                    metrics: {
                        manifestParses: context.metrics.manifestParses,
                        valueReads: context.metrics.valueReads,
                        sizeValueReads: context.metrics.sizeValueReads,
                        ownerReads: context.metrics.ownerReads,
                        maxRowParses: context.metrics.maxRowParses,
                    },
                },
            };
        }
        
        async function pinPluginStorageViewerContext(req, requestedGeneration, signal, isClosed, metrics) {
            const releaseSnapshotSlot = await acquirePluginStorageViewerSnapshotSlot(signal);
            let snapshot = null;
            try {
                const pinned = await queueStorageReadAfterImports(async () => {
                    await flushPendingDb();
                    throwIfSignalAborted(signal);
                    return {
                        snapshot: createKvSnapshot(),
                        mutationVersion: getPluginStorageMutationVersion(),
                    };
                }, signal);
                snapshot = pinned.snapshot;
                await waitAtPluginStorageViewerSnapshotTestGate(isClosed);
                throwIfSignalAborted(signal);
                const rawDatabase = snapshot.kvGet('database/database.bin');
                const dbObj = rawDatabase ? await decodeAuthoritativeRisuSave(rawDatabase, {
                    resolveRemote: async (name) => snapshot.kvGet(`remotes/${name}.local.bin`) || null,
                }) : null;
                const generation = pluginStorageGeneration(dbObj);
                const manifestState = readPluginStorageManifestState(snapshot.kvGet);
                metrics.manifestParses = 1;
                const pinnedState = sessionPluginStorageReadState(req);
                const activeManifest = generation
                    && dbObj?.optimizePluginMemory === true
                    && manifestState.valid
                    && manifestState.manifest?.generation === generation
                    ? manifestState.manifest
                    : null;
                if (generation !== requestedGeneration
                    || !activeManifest
                    || (pinnedState && (
                        pinnedState.optimized !== true
                        || pinnedState.generation !== requestedGeneration
                    ))) {
                    throw pluginStorageNamespaceConflict(
                        'Plugin storage generation changed before the viewer page could be read',
                    );
                }
                const physicalValues = new Set(snapshot.kvList(PLUGIN_SAVE_PREFIX));
                const physicalMeta = new Set(snapshot.kvList(PLUGIN_SAVE_META_PREFIX));
                const authoritativeValues = activeManifest.valueKeys
                    .filter((storageKey) => physicalValues.has(storageKey))
                    .map((storageKey) => ({
                        storageKey,
                        key: decodeManifestPluginSaveStorageKey(
                            activeManifest,
                            storageKey,
                            PLUGIN_SAVE_PREFIX,
                        ),
                    }));
                return {
                    snapshot,
                    releaseSnapshotSlot,
                    closed: false,
                    signal,
                    metrics,
                    mutationVersion: pinned.mutationVersion,
                    rawDatabase,
                    databaseRevision: computeBufferEtag(rawDatabase),
                    generation,
                    manifestState,
                    activeManifest,
                    authoritativeValues,
                    physicalMeta,
                    manifestMeta: new Set(activeManifest.metaKeys),
                    facetState: snapshot.viewerFacetState(),
                };
            } catch (error) {
                snapshot?.close();
                releaseSnapshotSlot();
                throw error;
            }
        }
        
        function pluginStorageViewerTotalCacheMatches(context) {
            const cached = pluginStorageViewerTotalSizeCache;
            return cached
                && cached.generation === context.generation
                && cached.manifestRevision === context.manifestState.revision
                && cached.mutationVersion === context.mutationVersion
                && cached.sourceRevision === context.facetState.sourceRevision;
        }
        
        async function assemblePluginStorageViewerFromFacets(context, options, isClosed) {
            if (!context.facetState.current) return null;
            const storageKeys = context.authoritativeValues.map((row) => row.storageKey);
            const summary = context.snapshot.viewerValueFacetSummary(storageKeys);
            if (summary.count !== storageKeys.length
                || !Number.isSafeInteger(summary.totalBytes)
                || summary.totalBytes < 0) return null;
            const totalBytes = pluginStorageViewerTotalCacheMatches(context)
                ? pluginStorageViewerTotalSizeCache.totalBytes
                : summary.totalBytes;
            if (!pluginStorageViewerTotalCacheMatches(context)) {
                pluginStorageViewerTotalSizeCache = {
                    generation: context.generation,
                    manifestRevision: context.manifestState.revision,
                    mutationVersion: context.mutationVersion,
                    sourceRevision: context.facetState.sourceRevision,
                    totalBytes,
                };
            }
            const selection = selectPluginStorageViewerRows(context, options);
            const entries = await assemblePluginStorageViewerEntries(
                context,
                selection.pageRows,
                isClosed,
            );
            const pageFacets = new Map(context.snapshot.viewerValueFacets(
                selection.pageRows.map((row) => row.storageKey),
            ).map((facet) => [facet.storageKey, facet.displaySize]));
            if (entries.some((entry, index) => (
                pageFacets.get(selection.pageRows[index].storageKey) !== entry.size
            ))) return null;
            return finalizePluginStorageViewerAssembly(
                context,
                options,
                selection,
                totalBytes,
                entries,
            );
        }
        
        async function backfillPluginStorageViewerFacets(context, options, isClosed) {
            pluginStorageViewerTestCounters.backfillPasses += 1;
            await reportPluginStorageViewerSnapshotState();
            const ownerByStorageKey = new Map();
            const rebuiltOwners = [];
            for (const storageKey of context.activeManifest.metaKeys) {
                if (!context.physicalMeta.has(storageKey)) continue;
                await new Promise((resolve) => setImmediate(resolve));
                throwIfSignalAborted(context.signal);
                if (isClosed()) throw context.signal.reason;
                const bytes = context.snapshot.kvGet(storageKey);
                const owner = pluginStorageViewerOwner(parsePluginStorageOwnerRecord(bytes));
                if (owner !== null) {
                    ownerByStorageKey.set(storageKey, owner);
                    rebuiltOwners.push({ storageKey, owner });
                }
            }
            const selection = selectPluginStorageViewerRows(context, options, ownerByStorageKey);
            const pageKeys = new Set(selection.pageRows.map((row) => row.storageKey));
            const reusedValues = new Map();
            const rebuiltValues = [];
            let totalBytes = 0;
            for (const descriptor of context.authoritativeValues) {
                await new Promise((resolve) => setImmediate(resolve));
                throwIfSignalAborted(context.signal);
                if (isClosed()) throw context.signal.reason;
                const valueBytes = context.snapshot.kvGet(descriptor.storageKey);
                context.metrics.sizeValueReads += 1;
                if (valueBytes === null) {
                    throw pluginStorageNamespaceConflict(
                        'A plugin storage viewer size row disappeared from its pinned snapshot',
                    );
                }
                const value = pluginStorageViewerParseOne(
                    context.metrics,
                    () => validatePluginStorageRow(descriptor.storageKey, valueBytes),
                );
                const displaySize = pluginStorageViewerDisplaySize(value);
                totalBytes += displaySize;
                if (!Number.isSafeInteger(totalBytes)) {
                    throw new RangeError('Plugin storage viewer total size exceeds the safe integer range');
                }
                rebuiltValues.push({ storageKey: descriptor.storageKey, displaySize });
                if (pageKeys.has(descriptor.storageKey)) {
                    reusedValues.set(descriptor.storageKey, { valueBytes, value });
                    context.metrics.valueReads += 1;
                    pluginStorageViewerTestCounters.pageValueReuses += 1;
                }
            }
            let publication = null;
            try {
                publication = rebuildPluginStorageViewerFacets(
                    context.facetState.sourceRevision,
                    rebuiltValues,
                    rebuiltOwners,
                );
            } catch (error) {
                logger.warn('[PluginStorage] Viewer facet rebuild was not published:', error);
            }
            if (publication?.published
                && getPluginStorageMutationVersion() === context.mutationVersion) {
                pluginStorageViewerTotalSizeCache = {
                    generation: context.generation,
                    manifestRevision: context.manifestState.revision,
                    mutationVersion: context.mutationVersion,
                    sourceRevision: publication.state.sourceRevision,
                    totalBytes,
                };
            }
            const entries = await assemblePluginStorageViewerEntries(
                context,
                selection.pageRows,
                isClosed,
                reusedValues,
            );
            await reportPluginStorageViewerSnapshotState();
            return finalizePluginStorageViewerAssembly(
                context,
                options,
                selection,
                totalBytes,
                entries,
            );
        }
        
        async function handlePluginStorageViewerPage(req, res, next) {
            if (!await checkAuth(req, res)) return;
            const firstHeader = (value) => Array.isArray(value) ? value[0] : value;
            const requestedGeneration = firstHeader(req.headers['x-plugin-storage-generation']);
            const pageText = Array.isArray(req.query.page) ? req.query.page[0] : req.query.page;
            const pageSizeText = Array.isArray(req.query.pageSize)
                ? req.query.pageSize[0]
                : req.query.pageSize;
            const keyQueryValue = Array.isArray(req.query.key) ? req.query.key[0] : req.query.key;
            const ownerQueryValue = Array.isArray(req.query.owner) ? req.query.owner[0] : req.query.owner;
            const unknownOwnerValue = Array.isArray(req.query.unknownOwner)
                ? req.query.unknownOwner[0]
                : req.query.unknownOwner;
            const page = pageText === undefined || pageText === '' ? 0 : Number(pageText);
            const pageSize = pageSizeText === undefined || pageSizeText === '' ? 50 : Number(pageSizeText);
            const keyQuery = keyQueryValue === undefined ? '' : String(keyQueryValue).trim();
            const ownerQuery = ownerQueryValue === undefined ? '' : String(ownerQueryValue);
            const unknownOwner = unknownOwnerValue === '1';
            if (typeof requestedGeneration !== 'string' || requestedGeneration.length === 0
                || !Number.isSafeInteger(page) || page < 0
                || !Number.isSafeInteger(pageSize) || pageSize < 1 || pageSize > 50
                || keyQuery.length > 1024 || !keyQuery.isWellFormed()
                || ownerQuery.length > 1024 || !ownerQuery.isWellFormed()
                || (ownerQueryValue !== undefined && ownerQuery.length === 0)
                || (unknownOwnerValue !== undefined && unknownOwnerValue !== '1')
                || (unknownOwner && ownerQueryValue !== undefined)) {
                return res.status(400).json({
                    success: false,
                    error: 'Plugin storage viewer requires a generation, a non-negative page, and a page size from 1 to 50.',
                    code: 'INVALID_PLUGIN_STORAGE_VIEWER_PAGE',
                });
            }
        
            const options = {
                page,
                pageSize,
                keyQuery,
                ownerQuery,
                ownerQueryValue,
                unknownOwner,
            };
            const metrics = {
                manifestParses: 0,
                valueReads: 0,
                sizeValueReads: 0,
                ownerReads: 0,
                maxRowParses: 0,
                activeRowParses: 0,
            };
            let context = null;
            let completed = false;
            let closed = false;
            const requestAbort = new AbortController();
            const isClosed = () => closed || req.aborted || res.destroyed;
            const onClose = () => {
                if (!completed) {
                    closed = true;
                    requestAbort.abort(new DOMException('Plugin storage viewer closed', 'AbortError'));
                }
            };
            req.once('aborted', onClose);
            res.once('close', onClose);
            try {
                let assembly = null;
                for (let attempt = 0; attempt < 4 && !assembly; attempt++) {
                    context = await pinPluginStorageViewerContext(
                        req,
                        requestedGeneration,
                        requestAbort.signal,
                        isClosed,
                        metrics,
                    );
                    assembly = await assemblePluginStorageViewerFromFacets(
                        context,
                        options,
                        isClosed,
                    );
                    if (assembly) {
                        closePluginStorageViewerContext(context);
                        context = null;
                        break;
                    }
        
                    const backfillKey = JSON.stringify([
                        context.generation,
                        context.manifestState.revision,
                        context.databaseRevision,
                        context.facetState.sourceRevision,
                        context.mutationVersion,
                    ]);
                    const existing = pluginStorageViewerBackfills.get(backfillKey);
                    if (existing) {
                        closePluginStorageViewerContext(context);
                        context = null;
                        try {
                            await existing;
                        } catch (error) {
                            if (isClosed()) return;
                            if (attempt === 3) throw error;
                        }
                        continue;
                    }
        
                    const backfillContext = context;
                    context = null;
                    const backfill = backfillPluginStorageViewerFacets(
                        backfillContext,
                        options,
                        isClosed,
                    ).finally(() => {
                        closePluginStorageViewerContext(backfillContext);
                        if (pluginStorageViewerBackfills.get(backfillKey) === backfill) {
                            pluginStorageViewerBackfills.delete(backfillKey);
                        }
                    });
                    pluginStorageViewerBackfills.set(backfillKey, backfill);
                    assembly = await backfill;
                }
                if (!assembly) {
                    throw new Error('Plugin storage viewer facets could not be verified');
                }
                if (isClosed()) return;
        
                res.status(200);
                res.setHeader('content-type', 'application/x-ndjson; charset=utf-8');
                res.setHeader('cache-control', 'no-store');
                res.setHeader('x-accel-buffering', 'no');
                res.flushHeaders();
                if (!await writeWithBackpressure(
                    res,
                    `${JSON.stringify(assembly.meta)}\n`,
                    isClosed,
                )) return;
        
                await waitAtPluginStorageViewerTestGate(isClosed);
                for (const entry of assembly.entries) {
                    if (isClosed()) return;
                    if (!await writeWithBackpressure(
                        res,
                        `${JSON.stringify(entry)}\n`,
                        isClosed,
                        () => reportPluginStorageViewerTestProgress(
                            metrics,
                            'backpressure.json',
                        ),
                    )) return;
                }
                if (isClosed()) return;
                if (!await writeWithBackpressure(
                    res,
                    `${JSON.stringify(assembly.done)}\n`,
                    isClosed,
                )) return;
                completed = true;
                res.end();
            } catch (error) {
                if (isClosed()) return;
                if (error?.pluginStorageNamespaceConflict && !res.headersSent) {
                    return res.status(409).json({
                        success: false,
                        error: error.message,
                        code: 'PLUGIN_STORAGE_GENERATION_CONFLICT',
                    });
                }
                if (res.headersSent) {
                    try {
                        res.write(`${JSON.stringify({
                            event: 'error',
                            message: error instanceof Error ? error.message : String(error),
                        })}\n`);
                        res.end();
                    } catch {}
                    return;
                }
                next(error);
            } finally {
                req.removeListener('aborted', onClose);
                res.removeListener('close', onClose);
                closePluginStorageViewerContext(context);
                if (pluginStorageViewerTestGateDir) {
                    try {
                        // Temp-then-rename: this write races the test's read (the
                        // response has already ended), so the file must never be
                        // observable half-written.
                        const gateResultPath = path.join(pluginStorageViewerTestGateDir, 'result.json');
                        await fs.writeFile(
                            `${gateResultPath}.tmp`,
                            JSON.stringify({
                                manifestParses: metrics.manifestParses,
                                valueReads: metrics.valueReads,
                                sizeValueReads: metrics.sizeValueReads,
                                ownerReads: metrics.ownerReads,
                                maxRowParses: metrics.maxRowParses,
                                ...pluginStorageViewerTestCounters,
                                aborted: !completed,
                            }),
                            'utf-8',
                        );
                        await fs.rename(`${gateResultPath}.tmp`, gateResultPath);
                    } catch {}
                }
            }
        }
        
        app.get('/api/plugin-storage/viewer-page', handlePluginStorageViewerPage);
        
        app.get('/api/plugin-storage/manifest', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            const firstHeader = (value) => Array.isArray(value) ? value[0] : value;
            const requestedGeneration = firstHeader(req.headers['x-plugin-storage-generation']);
            const requestedMode = firstHeader(req.headers['x-plugin-storage-manifest-mode']) ?? 'snapshot';
            if (requestedGeneration !== undefined
                && (typeof requestedGeneration !== 'string' || requestedGeneration.length === 0)) {
                return res.status(400).json({
                    success: false,
                    error: 'Plugin storage generation must be a non-empty string.',
                    code: 'INVALID_PLUGIN_STORAGE_MANIFEST_READ',
                });
            }
            if (requestedMode !== 'snapshot' && requestedMode !== 'state') {
                return res.status(400).json({
                    success: false,
                    error: 'Plugin storage manifest mode must be snapshot or state.',
                    code: 'INVALID_PLUGIN_STORAGE_MANIFEST_READ',
                });
            }
            if (requestedMode === 'state') {
                res.set('Cache-Control', 'no-store');
            }
        
            try {
                const snapshot = await queueStorageReadAfterImports(async () => {
                    const publication = await readLivePluginStoragePublication();
                    const { dbObj, generation, manifestState } = publication;
                    const pinnedState = sessionPluginStorageReadState(req);
                    const expectedState = requestedGeneration !== undefined
                        ? { optimized: true, generation: requestedGeneration }
                        : pinnedState;
                    const activeManifest = generation
                        && dbObj?.optimizePluginMemory === true
                        && manifestState.valid
                        && manifestState.manifest?.generation === generation
                        ? manifestState.manifest
                        : null;
                    if (!expectedState
                        || expectedState.optimized !== true
                        || expectedState.generation !== generation
                        || !activeManifest
                        || (requestedGeneration !== undefined && pinnedState && (
                            pinnedState.optimized !== true
                            || pinnedState.generation !== requestedGeneration
                        ))) {
                        throw pluginStorageNamespaceConflict(
                            'Plugin storage generation changed before the manifest could be read',
                        );
                    }
        
                    const manifestRevision = manifestState.revision;
                    if (requestedMode === 'state') {
                        return { generation, manifestRevision };
                    }
                    const physicalValues = new Set(kvList(PLUGIN_SAVE_PREFIX));
                    const physicalMeta = new Set(kvList(PLUGIN_SAVE_META_PREFIX));
                    return {
                        generation,
                        manifestRevision,
                        manifest: activeManifest,
                        valueKeys: activeManifest.valueKeys.filter(key => physicalValues.has(key)),
                        metaKeys: activeManifest.metaKeys.filter(key => physicalMeta.has(key)),
                    };
                });
                return res.json({ success: true, ...snapshot });
            } catch (error) {
                if (error?.pluginStorageNamespaceConflict) {
                    return res.status(409).json({
                        success: false,
                        error: error.message,
                        code: 'PLUGIN_STORAGE_GENERATION_CONFLICT',
                    });
                }
                next(error);
            }
        });
        
        app.post('/api/plugin-storage/reconcile-boot', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            if (!checkActiveSession(req, res)) return;
            const expectedEtag = req.headers['x-if-match'];
            if (typeof expectedEtag !== 'string' || !/^[0-9a-f]{32}$/.test(expectedEtag)) {
                return res.status(400).json({
                    success: false,
                    commitOutcome: 'not-committed',
                    commitOutcomeUnknown: false,
                    code: 'INVALID_DATABASE_ETAG',
                    error: 'Optimized plugin storage boot reconciliation requires a database ETag.',
                    retryable: false,
                });
            }
            try {
                const result = await reconcileOptimizedPluginStorageForBoot(req, expectedEtag);
                return res.json({
                    success: true,
                    commitOutcome: 'committed',
                    commitOutcomeUnknown: false,
                    ...result,
                });
            } catch (error) {
                if (error?.pluginStorageBootStatus === 409) {
                    return res.status(409).json({
                        success: false,
                        commitOutcome: 'not-committed',
                        commitOutcomeUnknown: false,
                        code: 'PLUGIN_STORAGE_BOOT_CONFLICT',
                        error: error.message,
                        currentEtag: error.currentEtag ?? null,
                        retryable: true,
                    });
                }
                if (isImportInProgressError(error)) {
                    res.setHeader('Retry-After', '5');
                    return res.status(503).json({
                        success: false,
                        commitOutcome: 'not-committed',
                        commitOutcomeUnknown: false,
                        code: 'IMPORT_IN_PROGRESS',
                        error: 'An import is in progress; retry reconciliation after it completes.',
                        retryable: true,
                    });
                }
                next(error);
            }
        });
        
        app.get('/api/plugin-storage/recovery', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            try {
                const inspection = await queueStorageReadAfterImports(
                    () => inspectOptimizedPluginStorageRecoveryManagement(),
                );
                res.setHeader('Cache-Control', 'no-store');
                return res.json(publicOptimizedPluginStorageRecoveryManagementInspection(inspection));
            } catch (error) {
                next(error);
            }
        });
        
        app.get('/api/plugin-storage/recovery/download', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            const encodedKey = req.query.encodedKey;
            const token = req.query.token;
            if (typeof encodedKey !== 'string' || encodedKey.length === 0
                || typeof token !== 'string' || !/^[A-Za-z0-9_-]{43}$/.test(token)) {
                return res.status(400).json({
                    success: false,
                    code: 'INVALID_PLUGIN_STORAGE_RECOVERY_REQUEST',
                    error: 'A recovery issue key and token are required.',
                    retryable: false,
                });
            }
            if (!ensureDatabaseSpoolDirSync()) {
                return res.status(503).json({
                    success: false,
                    code: 'PLUGIN_STORAGE_RECOVERY_DOWNLOAD_UNAVAILABLE',
                    error: 'The recovery download spool is unavailable.',
                    retryable: true,
                });
            }
        
            const spoolPath = path.join(
                getDatabaseSpoolDir(),
                `${PLUGIN_RECOVERY_DOWNLOAD_SPOOL_FILE_PREFIX}${process.pid}-${nodeCrypto.randomUUID()}.bin`,
            );
            const requestAbort = new AbortController();
            const abortDownload = () => {
                if (!res.writableFinished && !requestAbort.signal.aborted) {
                    requestAbort.abort(new DOMException(
                        'Plugin storage recovery download closed',
                        'AbortError',
                    ));
                }
            };
            req.once('aborted', abortDownload);
            res.once('close', abortDownload);
            let prepared = null;
            try {
                prepared = await queueStorageReadAfterImports(async () => {
                    const inspection = await inspectOptimizedPluginStorageRecoveryManagement();
                    const issue = findOptimizedPluginStorageRecoveryManagementIssue(
                        inspection,
                        encodedKey,
                        token,
                    );
                    if (!issue || !issue.externalAvailable) return null;
                    const digest = nodeCrypto.createHash('sha256');
                    const row = await kvWriteToFile(encodedKey, spoolPath, {
                        signal: requestAbort.signal,
                        onBytes: bytes => digest.update(bytes),
                    });
                    if (!row || row.size !== issue.externalSize
                        || digest.digest('hex') !== issue.externalHash) {
                        await fs.unlink(spoolPath).catch(() => {});
                        return null;
                    }
                    await hardenPrivateFile(spoolPath, 0o600, { fs });
                    return {
                        size: row.size,
                        sha256: issue.externalHash,
                        filename: `plugin-storage-recovery-${sha256Hex(Buffer.from(encodedKey, 'utf-8')).slice(0, 12)}.bin`,
                    };
                }, requestAbort.signal);
                if (!prepared) {
                    return res.status(409).json({
                        success: false,
                        code: 'PLUGIN_STORAGE_RECOVERY_STALE',
                        error: 'The affected row changed; refresh recovery details before downloading it.',
                        retryable: true,
                    });
                }
                res.status(200);
                res.setHeader('Content-Type', 'application/octet-stream');
                res.setHeader('Content-Length', String(prepared.size));
                res.setHeader('Content-Disposition', `attachment; filename="${prepared.filename}"`);
                res.setHeader('Cache-Control', 'no-store');
                res.setHeader('X-Content-Type-Options', 'nosniff');
                res.setHeader('X-Content-SHA256', prepared.sha256);
                await pipeline(createReadStream(spoolPath), res);
            } catch (error) {
                if (!res.headersSent) next(error);
                else if (!res.destroyed) res.destroy(error);
            } finally {
                req.removeListener('aborted', abortDownload);
                res.removeListener('close', abortDownload);
                await fs.unlink(spoolPath).catch(() => {});
            }
        });
        
        app.post('/api/plugin-storage/recovery/resolve', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            const writeRequest = captureActiveSessionWriteRequest(req);
            if (!checkActiveSessionWrite(writeRequest, res)) return;
            const body = req.body;
            if (!body || typeof body !== 'object' || Array.isArray(body)
                || Object.keys(body).length !== 3
                || Object.keys(body).some(key => !['encodedKey', 'token', 'action'].includes(key))
                || typeof body.encodedKey !== 'string' || body.encodedKey.length === 0
                || typeof body.token !== 'string' || !/^[A-Za-z0-9_-]{43}$/.test(body.token)
                || (body.action !== 'use-inline' && body.action !== 'delete')) {
                return res.status(400).json({
                    success: false,
                    commitOutcome: 'not-committed',
                    commitOutcomeUnknown: false,
                    code: 'INVALID_PLUGIN_STORAGE_RECOVERY_REQUEST',
                    error: 'The recovery resolution request is invalid.',
                    retryable: false,
                });
            }
        
            try {
                const result = await queueStorageMutation(async () => {
                    const inspection = await inspectOptimizedPluginStorageRecoveryManagement();
                    const issue = findOptimizedPluginStorageRecoveryManagementIssue(
                        inspection,
                        body.encodedKey,
                        body.token,
                    );
                    if (!issue) return { stale: true };
                    if ((body.action === 'use-inline' && !issue.canUseInline)
                        || (body.action === 'delete' && !issue.canDelete)) {
                        return { unavailable: true };
                    }
                    // Inspection yields; recheck the admitted writer after its last await.
                    if (!checkActiveSessionWrite(writeRequest, res)) {
                        return { sessionDeactivated: true };
                    }
                    try {
                        resolveOptimizedPluginStorageRecoveryIssue(inspection, issue, body.action);
                    } catch (error) {
                        if (error?.pluginStorageRecoveryStale) return { stale: true };
                        throw error;
                    }
                    return { committed: true };
                });
                if (result.sessionDeactivated) return;
                if (result.stale) {
                    return res.status(409).json({
                        success: false,
                        commitOutcome: 'not-committed',
                        commitOutcomeUnknown: false,
                        code: 'PLUGIN_STORAGE_RECOVERY_STALE',
                        error: 'The affected row changed; refresh recovery details before continuing.',
                        retryable: true,
                    });
                }
                if (result.unavailable) {
                    return res.status(409).json({
                        success: false,
                        commitOutcome: 'not-committed',
                        commitOutcomeUnknown: false,
                        code: 'PLUGIN_STORAGE_RECOVERY_ACTION_UNAVAILABLE',
                        error: 'That recovery action is no longer available.',
                        retryable: true,
                    });
                }
                return res.json({
                    success: true,
                    commitOutcome: 'committed',
                    commitOutcomeUnknown: false,
                    action: body.action,
                    encodedKey: body.encodedKey,
                });
            } catch (error) {
                if (isImportInProgressError(error)) return sendImportBusy(res);
                logger.warn('[PluginStorageRecovery] Recovery action rolled back:', error);
                return res.status(500).json({
                    success: false,
                    commitOutcome: 'not-committed',
                    commitOutcomeUnknown: false,
                    code: 'PLUGIN_STORAGE_RECOVERY_ROLLED_BACK',
                    error: 'The recovery action rolled back without changing plugin storage.',
                    retryable: false,
                });
            }
        });
    }

    function parsePluginStorageBatchEnvelope(body, { streamed = false } = {}) {
        if (!body || typeof body !== 'object' || Array.isArray(body)
            || Object.keys(body).length !== 4
            || Object.keys(body).some(key => ![
                'version', 'generation', 'expectedManifest',
                'expectedManifestRevision', 'operations',
            ].includes(key))
            || (streamed ? body.version !== 3 : (body.version !== 1 && body.version !== 2))
            || typeof body.generation !== 'string'
            || body.generation.length === 0
            || !Array.isArray(body.operations)
            || body.operations.length < 1
            || body.operations.length > PLUGIN_STORAGE_BATCH_MAX_OPERATIONS) {
            throw new Error(`Plugin storage batch requires 1-${PLUGIN_STORAGE_BATCH_MAX_OPERATIONS} operations.`);
        }
    
        let expectedManifest;
        let expectedManifestRevision;
        if (body.version === 1 || (body.version === 3 && body.expectedManifest !== undefined)) {
            if (!body.expectedManifest || body.expectedManifestRevision !== undefined
                || typeof body.expectedManifest !== 'object'
                || Array.isArray(body.expectedManifest)
                || Object.keys(body.expectedManifest).length
                    !== (body.expectedManifest.version === 3 ? 5 : 4)
                || Object.keys(body.expectedManifest).some(key => ![
                    'version', 'generation', 'valueKeys', 'metaKeys', 'keyMappings',
                ].includes(key))) {
                throw new Error('Plugin storage batch requires an exact expectedManifest.');
            }
            expectedManifest = normalizePluginStorageManifestRequest(
                body.expectedManifest,
                'expectedManifest',
            );
            if (expectedManifest.generation !== body.generation
                || expectedManifest.valueKeys.length !== body.expectedManifest.valueKeys.length
                || expectedManifest.metaKeys.length !== body.expectedManifest.metaKeys.length
                || (expectedManifest.version === 3
                    && expectedManifest.keyMappings.length
                        !== body.expectedManifest.keyMappings.length)) {
                throw new Error('Plugin storage batch expectedManifest is not canonical.');
            }
        } else {
            if (body.expectedManifest !== undefined
                || typeof body.expectedManifestRevision !== 'string'
                || !PLUGIN_STORAGE_REVISION_PATTERN.test(body.expectedManifestRevision)) {
                throw new Error('Plugin storage batch requires an exact manifest revision.');
            }
            expectedManifestRevision = body.expectedManifestRevision;
        }
    
        const seen = new Set();
        const operations = body.operations.map((input, index) => {
            if (!input || typeof input !== 'object' || Array.isArray(input)) {
                throw new Error(`Plugin storage batch operation ${index} must be an object.`);
            }
            const allowed = streamed
                ? new Set([
                    'operation', 'key', 'valueLength', 'valueHash',
                    'owner', 'expectedRevision',
                ])
                : new Set(['operation', 'key', 'value', 'owner', 'expectedRevision']);
            if (Object.keys(input).some(key => !allowed.has(key))) {
                throw new Error(`Plugin storage batch operation ${index} has unsupported fields.`);
            }
            if (input.operation !== 'set' && input.operation !== 'remove') {
                throw new Error(`Plugin storage batch operation ${index} must be set or remove.`);
            }
            if (typeof input.key !== 'string') {
                throw new Error(`Plugin storage batch operation ${index} requires a string key.`);
            }
            const valueKey = encodePluginSaveStorageKey(input.key, PLUGIN_SAVE_PREFIX);
            const ownerKey = encodePluginSaveStorageKey(input.key, PLUGIN_SAVE_META_PREFIX);
            if (seen.has(valueKey)) throw new Error(`Duplicate plugin storage key at operation ${index}.`);
            seen.add(valueKey);
    
            const hasExpectedRevision = Object.prototype.hasOwnProperty.call(input, 'expectedRevision');
            if (hasExpectedRevision
                && input.expectedRevision !== null
                && (typeof input.expectedRevision !== 'string'
                    || !PLUGIN_STORAGE_REVISION_PATTERN.test(input.expectedRevision))) {
                throw new Error(`Plugin storage operation ${index} has an invalid expectedRevision.`);
            }
    
            if (input.operation === 'remove') {
                if (Object.keys(input).some(key => ![
                    'operation', 'key', 'expectedRevision',
                ].includes(key))) {
                    throw new Error(`Remove operation ${index} cannot include value or owner data.`);
                }
                return {
                    operation: 'remove',
                    rawKey: input.key,
                    valueKey,
                    ownerKey,
                    hasExpectedRevision,
                    expectedRevision: input.expectedRevision,
                };
            }
    
            let valueBytes = null;
            let valueSize;
            let valueHash;
            let valueDisplaySize = null;
            if (streamed) {
                if (!Number.isSafeInteger(input.valueLength) || input.valueLength < 1) {
                    throw new Error(`Set operation ${index} requires a positive valueLength.`);
                }
                if (input.valueLength > PLUGIN_VALUE_MAX_BYTES) {
                    throw new PluginStorageLimitError(
                        `Plugin value is ${input.valueLength} bytes; the per-value limit is ${PLUGIN_VALUE_MAX_BYTES} bytes. Split the value into smaller records.`,
                        {
                            code: 'PLUGIN_VALUE_TOO_LARGE',
                            limit: PLUGIN_VALUE_MAX_BYTES,
                            actual: input.valueLength,
                        },
                    );
                }
                if (typeof input.valueHash !== 'string' || !/^[0-9a-f]{64}$/.test(input.valueHash)) {
                    throw new Error(`Set operation ${index} requires a SHA-256 valueHash.`);
                }
                valueSize = input.valueLength;
                valueHash = input.valueHash;
            } else {
                if (typeof input.value !== 'string' || !/^[A-Za-z0-9+/]*={0,2}$/.test(input.value)) {
                    throw new Error(`Set operation ${index} requires canonical base64 value bytes.`);
                }
                valueBytes = Buffer.from(input.value, 'base64');
                if (valueBytes.length === 0 || valueBytes.toString('base64') !== input.value) {
                    throw new Error(`Set operation ${index} requires canonical non-empty value bytes.`);
                }
                const valueText = valueBytes.toString('utf-8');
                if (!Buffer.from(valueText, 'utf-8').equals(valueBytes)) {
                    throw new Error(`Set operation ${index} value must be valid UTF-8 JSON.`);
                }
                valueDisplaySize = pluginStorageViewerDisplaySize(
                    validatePluginStorageRow(valueKey, valueBytes),
                );
                valueSize = valueBytes.length;
                valueHash = sha256Hex(valueBytes);
            }
            if (typeof input.owner !== 'string' || !input.owner.isWellFormed()) {
                throw new Error(`Set operation ${index} requires a well-formed owner string.`);
            }
            return {
                operation: 'set',
                rawKey: input.key,
                valueKey,
                ownerKey,
                valueBytes,
                valueFilePath: null,
                valueSize,
                valueHash,
                valueDisplaySize,
                owner: input.owner,
                hasExpectedRevision,
                expectedRevision: input.expectedRevision,
            };
        });
    
        return {
            requestedGeneration: body.generation,
            expectedManifest,
            expectedManifestRevision,
            operations,
        };
    }
    
    function createPluginStorageBatchRequestReader(req) {
        const iterator = req[Symbol.asyncIterator]();
        let current = Buffer.alloc(0);
        let offset = 0;
        let ended = false;
    
        async function nextSlice(maxBytes) {
            while (offset >= current.length) {
                if (ended) return null;
                const result = await iterator.next();
                if (result.done) {
                    ended = true;
                    return null;
                }
                current = Buffer.isBuffer(result.value)
                    ? result.value
                    : Buffer.from(result.value);
                offset = 0;
                if (current.length === 0) continue;
            }
            const length = Math.min(maxBytes, current.length - offset);
            const slice = current.subarray(offset, offset + length);
            offset += length;
            return slice;
        }
    
        return {
            async readBuffer(length) {
                const result = Buffer.allocUnsafe(length);
                let written = 0;
                while (written < length) {
                    const slice = await nextSlice(length - written);
                    if (!slice) throw new Error('Streamed plugin storage batch was truncated.');
                    slice.copy(result, written);
                    written += slice.length;
                }
                return result;
            },
            async writeFile(length, filePath, digest) {
                const handle = await fs.open(filePath, 'wx', 0o600);
                let written = 0;
                try {
                    while (written < length) {
                        const slice = await nextSlice(length - written);
                        if (!slice) throw new Error('Streamed plugin storage batch value was truncated.');
                        let sliceOffset = 0;
                        while (sliceOffset < slice.length) {
                            const result = await handle.write(
                                slice,
                                sliceOffset,
                                slice.length - sliceOffset,
                                written + sliceOffset,
                            );
                            if (result.bytesWritten <= 0) {
                                throw new Error('Streamed plugin storage batch value could not be staged.');
                            }
                            sliceOffset += result.bytesWritten;
                        }
                        digest.update(slice);
                        written += slice.length;
                    }
                } finally {
                    await handle.close();
                }
            },
            async assertEnd() {
                if (await nextSlice(1)) {
                    throw new Error('Streamed plugin storage batch contains trailing bytes.');
                }
            },
        };
    }
    
    async function receiveStreamedPluginStorageBatch(req, res) {
        if (!ensureDatabaseSpoolDirSync()) {
            const error = new Error('The server upload spool is unavailable; check the save volume permissions.');
            error.code = 'PLUGIN_STORAGE_SPOOL_UNAVAILABLE';
            error.status = 503;
            error.retryable = true;
            throw error;
        }
        const declaredText = Array.isArray(req.headers['x-plugin-storage-batch-length'])
            ? req.headers['x-plugin-storage-batch-length'][0]
            : req.headers['x-plugin-storage-batch-length'];
        const declaredLength = typeof declaredText === 'string' ? Number(declaredText) : NaN;
        const contentLength = Number(req.headers['content-length']);
        const maximumLength = PLUGIN_STORAGE_BATCH_STREAM_PREFIX_BYTES
            + PLUGIN_STORAGE_BATCH_STREAM_MAX_METADATA_BYTES
            + PLUGIN_STORAGE_BATCH_STREAM_MAX_PAYLOAD_BYTES;
        if (!Number.isSafeInteger(declaredLength) || declaredLength <= 0
            || declaredLength > maximumLength
            || (Number.isSafeInteger(contentLength) && contentLength !== declaredLength)) {
            const error = new Error('Streamed plugin storage batch requires an exact bounded length.');
            error.code = 'PLUGIN_STORAGE_BATCH_TOO_LARGE';
            error.status = declaredLength > maximumLength ? 413 : 400;
            error.limit = maximumLength;
            error.actual = declaredLength;
            throw error;
        }
    
        const stagedPaths = [];
        try {
            const reader = createPluginStorageBatchRequestReader(req);
            const prefix = await reader.readBuffer(PLUGIN_STORAGE_BATCH_STREAM_PREFIX_BYTES);
            if (!prefix.subarray(0, PLUGIN_STORAGE_BATCH_STREAM_MAGIC.length)
                .equals(PLUGIN_STORAGE_BATCH_STREAM_MAGIC)) {
                throw new Error('Streamed plugin storage batch has an invalid magic header.');
            }
            const metadataLength = prefix.readUInt32BE(PLUGIN_STORAGE_BATCH_STREAM_MAGIC.length);
            if (metadataLength < 1 || metadataLength > PLUGIN_STORAGE_BATCH_STREAM_MAX_METADATA_BYTES) {
                const error = new Error('Streamed plugin storage batch metadata exceeds its limit.');
                error.code = 'PLUGIN_STORAGE_BATCH_METADATA_TOO_LARGE';
                error.status = 413;
                error.limit = PLUGIN_STORAGE_BATCH_STREAM_MAX_METADATA_BYTES;
                error.actual = metadataLength;
                throw error;
            }
            const metadataBytes = await reader.readBuffer(metadataLength);
            const metadataText = metadataBytes.toString('utf-8');
            if (!Buffer.from(metadataText, 'utf-8').equals(metadataBytes)) {
                throw new Error('Streamed plugin storage batch metadata must be UTF-8 JSON.');
            }
            const body = JSON.parse(metadataText);
            if (!Buffer.from(JSON.stringify(body), 'utf-8').equals(metadataBytes)) {
                throw new Error('Streamed plugin storage batch metadata must use canonical JSON framing.');
            }
            const parsed = parsePluginStorageBatchEnvelope(body, { streamed: true });
            const payloadBytes = parsed.operations.reduce(
                (total, operation) => total + (operation.operation === 'set' ? operation.valueSize : 0),
                0,
            );
            if (!Number.isSafeInteger(payloadBytes)
                || payloadBytes > PLUGIN_STORAGE_BATCH_STREAM_MAX_PAYLOAD_BYTES) {
                const error = new Error('Streamed plugin storage batch values exceed the payload limit.');
                error.code = 'PLUGIN_STORAGE_BATCH_TOO_LARGE';
                error.status = 413;
                error.limit = PLUGIN_STORAGE_BATCH_STREAM_MAX_PAYLOAD_BYTES;
                error.actual = payloadBytes;
                throw error;
            }
            const expectedLength = PLUGIN_STORAGE_BATCH_STREAM_PREFIX_BYTES
                + metadataLength
                + payloadBytes;
            if (expectedLength !== declaredLength) {
                throw new Error('Streamed plugin storage batch length does not match its metadata.');
            }
    
            for (let index = 0; index < parsed.operations.length; index++) {
                const operation = parsed.operations[index];
                if (operation.operation !== 'set') continue;
                const valueFilePath = path.join(
                    getDatabaseSpoolDir(),
                    `${PLUGIN_BATCH_VALUE_SPOOL_FILE_PREFIX}${nodeCrypto.randomUUID()}.${index}.upload`,
                );
                stagedPaths.push(valueFilePath);
                const digest = nodeCrypto.createHash('sha256');
                await reader.writeFile(operation.valueSize, valueFilePath, digest);
                if (digest.digest('hex') !== operation.valueHash) {
                    throw new Error(`Streamed plugin storage batch value ${index} failed its hash check.`);
                }
                try {
                    const displayMetadata = await validateJsonSource({
                        filePath: valueFilePath,
                        size: operation.valueSize,
                    }, {
                        shouldAbort: () => req.aborted || res.destroyed,
                    });
                    operation.valueDisplaySize = pluginStorageViewerDisplaySizeFromMetadata(
                        displayMetadata,
                    );
                } catch {
                    throw new PluginStorageValidationError(operation.valueKey);
                }
                operation.valueFilePath = valueFilePath;
            }
            await reader.assertEnd();
            return {
                ...parsed,
                requestHash: sha256Hex(metadataBytes),
                stagedPaths,
            };
        } catch (error) {
            for (const filePath of stagedPaths) {
                try { unlinkSync(filePath); } catch {}
            }
            throw error;
        }
    }

    function registerPluginStorageBatchRoute(app) {
        /**
         * Atomically mutate a bounded set of optimized plugin values. Every CAS is
         * checked before the first write, and every value plus owner sidecar is
         * applied inside the same SQLite writer transaction.
         */
        app.post('/api/plugin-storage/batch', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            if (!checkActiveSession(req, res)) return;
        
            const reject = (status, error, code = 'INVALID_PLUGIN_STORAGE_BATCH') => res.status(status).json({
                success: false,
                outcome: 'not-committed',
                operation: 'batch',
                error,
                code,
                retryable: false,
            });
        
            let operations;
            let requestHash;
            let requestedGeneration;
            let expectedManifest;
            let expectedManifestRevision;
            let stagedPaths = [];
            try {
                const streamHeader = Array.isArray(req.headers['x-plugin-storage-batch-stream'])
                    ? req.headers['x-plugin-storage-batch-stream'][0]
                    : req.headers['x-plugin-storage-batch-stream'];
                if (streamHeader !== undefined && streamHeader !== '1') {
                    throw new Error('x-plugin-storage-batch-stream must be 1 when present.');
                }
                let parsed;
                if (streamHeader === '1') {
                    parsed = await receiveStreamedPluginStorageBatch(req, res);
                    stagedPaths = parsed.stagedPaths;
                } else {
                    if (!Buffer.isBuffer(req.body)) throw new Error('A JSON batch body is required.');
                    if (req.body.length > PLUGIN_STORAGE_BATCH_MAX_BODY_BYTES) {
                        return reject(413, 'Plugin storage batch body exceeds the 16 MiB limit.');
                    }
                    requestHash = sha256Hex(req.body);
                    const text = req.body.toString('utf-8');
                    if (!Buffer.from(text, 'utf-8').equals(req.body)) {
                        throw new Error('Plugin storage batch must be valid UTF-8 JSON.');
                    }
                    parsed = parsePluginStorageBatchEnvelope(JSON.parse(text));
                }
                operations = parsed.operations;
                requestHash = parsed.requestHash ?? requestHash;
                requestedGeneration = parsed.requestedGeneration;
                expectedManifest = parsed.expectedManifest;
                expectedManifestRevision = parsed.expectedManifestRevision;
            } catch (error) {
                if (error instanceof PluginStorageLimitError) {
                    return sendPluginStorageMutationLimitError(res, 'batch', error);
                }
                if (error?.status === 503 && error?.code === 'PLUGIN_STORAGE_SPOOL_UNAVAILABLE') {
                    return res.status(503).json({
                        success: false,
                        outcome: 'not-committed',
                        operation: 'batch',
                        error: error.message,
                        code: error.code,
                        retryable: true,
                    });
                }
                if (error?.status === 413) {
                    return res.status(413).json({
                        success: false,
                        outcome: 'not-committed',
                        operation: 'batch',
                        error: error instanceof Error ? error.message : String(error),
                        code: error.code ?? 'INVALID_PLUGIN_STORAGE_BATCH',
                        limit: error.limit,
                        actual: error.actual,
                        retryable: false,
                    });
                }
                return reject(error?.status ?? 400, error instanceof Error ? error.message : String(error));
            }
        
            let cleanedStage = false;
            const cleanupStage = () => {
                if (cleanedStage) return;
                cleanedStage = true;
                for (const filePath of stagedPaths) {
                    try { unlinkSync(filePath); } catch {}
                }
            };
            res.once('finish', cleanupStage);
            res.once('close', cleanupStage);
        
            class PluginStorageRevisionConflict extends Error {
                constructor(conflicts) {
                    super('One or more plugin storage revisions no longer match.');
                    this.conflicts = conflicts;
                }
            }
        
            try {
                await queueStorageMutation(async () => {
                    let generation;
                    let committedRevisions;
                    let nextManifest;
                    let manifestUpdate;
                    let committedManifestBytes;
                    let committedPublicationRevision;
                    try {
                        const publication = await readLivePluginStoragePublication();
                        const {
                            dbObj,
                            generation: liveGeneration,
                            manifestState,
                            manifestEntry,
                        } = publication;
                        const pinnedState = sessionPluginStorageReadState(req);
                        const activeManifest = liveGeneration
                            && dbObj?.optimizePluginMemory === true
                            && manifestState.valid
                            && manifestState.manifest?.generation === liveGeneration
                            ? manifestState.manifest
                            : null;
                        if (!activeManifest
                            || requestedGeneration !== liveGeneration
                            || (expectedManifest
                                ? !pluginStorageManifestEquals(activeManifest, expectedManifest)
                                : manifestState.revision !== expectedManifestRevision)
                            || (pinnedState && (
                                pinnedState.optimized !== true
                                || pinnedState.generation !== requestedGeneration
                            ))) {
                            return res.status(409).json({
                                success: false,
                                outcome: 'not-committed',
                                operation: 'batch',
                                error: 'Plugin storage generation or manifest changed before the batch committed.',
                                code: 'PLUGIN_STORAGE_GENERATION_CONFLICT',
                                retryable: true,
                                ...(activeManifest && manifestState.revision
                                    ? {
                                        currentGeneration: liveGeneration,
                                        currentManifestRevision: manifestState.revision,
                                    }
                                    : {}),
                            });
                        }
        
                        const valueAdds = [];
                        const valueDeletes = [];
                        const metaAdds = [];
                        const metaDeletes = [];
                        for (const operation of operations) {
                            if (operation.operation === 'set') {
                                valueAdds.push(operation.valueKey);
                                if (operation.owner) metaAdds.push(operation.ownerKey);
                                else metaDeletes.push(operation.ownerKey);
                            } else {
                                valueDeletes.push(operation.valueKey);
                                metaDeletes.push(operation.ownerKey);
                            }
                        }
                        manifestUpdate = pluginStorageManifestCache.prepareUpdate(manifestEntry, {
                            valueAdds,
                            valueDeletes,
                            metaAdds,
                            metaDeletes,
                            rawKeys: operations.map(operation => operation.rawKey),
                        });
                        nextManifest = manifestUpdate.manifest;
                        const readActiveState = (operation) => {
                            const valueBytes = manifestEntry.valueKeys.has(operation.valueKey)
                                ? kvGet(operation.valueKey)
                                : null;
                            const ownerBytes = manifestEntry.metaKeys.has(operation.ownerKey)
                                ? kvGet(operation.ownerKey)
                                : null;
                            const owner = parsePluginStorageOwnerRecord(ownerBytes);
                            return {
                                valueBytes,
                                ownerBytes,
                                revision: pluginStorageRevision(valueBytes, ownerBytes),
                                generation: valueBytes !== null
                                    && isCanonicalPluginStorageOwnerRecord(owner, ownerBytes)
                                    ? owner.generation
                                    : null,
                            };
                        };
                        const recoverySnapshotToken = newPluginRecoverySnapshotToken();
                        generation = nodeCrypto.randomUUID();
                        const updatedAt = Date.now();
                        for (const operation of operations) {
                            if (operation.operation !== 'set') continue;
                            operation.committedOwnerBytes = operation.owner
                                ? Buffer.from(JSON.stringify({
                                    plugin: operation.owner,
                                    updatedAt,
                                    revision: nodeCrypto.randomUUID(),
                                    generation,
                                }), 'utf-8')
                                : null;
                            operation.committedRevision = operation.valueFilePath
                                ? await pluginStorageRevisionFromFile(
                                    operation.valueFilePath,
                                    operation.committedOwnerBytes,
                                )
                                : pluginStorageRevision(
                                    operation.valueBytes,
                                    operation.committedOwnerBytes,
                                );
                        }
                        hitPluginStorageBatchFailpoint('before-transaction');
                        withPluginStorageQuotaPlan(operations.map(operation => ({
                            key: operation.valueKey,
                            size: operation.operation === 'set' ? operation.valueSize : null,
                        })), () => {
                            const conflicts = [];
                            for (const operation of operations) {
                                if (!operation.hasExpectedRevision) continue;
                                const current = readActiveState(operation);
                                if (current.revision !== operation.expectedRevision) {
                                    conflicts.push({
                                        key: operation.rawKey,
                                        currentRevision: current.revision,
                                        currentGeneration: current.generation,
                                    });
                                }
                            }
                            if (conflicts.length > 0) throw new PluginStorageRevisionConflict(conflicts);
        
                            for (let index = 0; index < operations.length; index++) {
                                const operation = operations[index];
                                if (operation.operation === 'set') {
                                    if (operation.valueFilePath) {
                                        kvSetFromFile(operation.valueKey, operation.valueFilePath, {
                                            pluginStorageDisplaySize: operation.valueDisplaySize,
                                        });
                                    } else {
                                        kvSet(operation.valueKey, operation.valueBytes, {
                                            pluginStorageDisplaySize: operation.valueDisplaySize,
                                        });
                                    }
                                    hitPluginStorageBatchFailpoint(`after-value:${index}`);
                                    if (operation.owner) {
                                        kvSet(operation.ownerKey, operation.committedOwnerBytes);
                                    } else {
                                        kvDel(operation.ownerKey);
                                    }
                                    hitPluginStorageBatchFailpoint(`after-owner:${index}`);
                                } else {
                                    kvDel(operation.valueKey);
                                    hitPluginStorageBatchFailpoint(`after-value:${index}`);
                                    kvDel(operation.ownerKey);
                                    hitPluginStorageBatchFailpoint(`after-owner:${index}`);
                                }
                                hitPluginStorageBatchFailpoint(`after-operation:${index}`);
                            }
                            hitPluginStorageBatchFailpoint('pre-commit');
                            committedManifestBytes = writePluginStorageManifest(nextManifest);
                            hitPluginStorageBatchFailpoint('after-manifest');
                            markPluginRecoverySnapshotDirty(recoverySnapshotToken);
                            committedPublicationRevision =
                                kvGetPluginStoragePublicationRevision();
                            committedRevisions = operations.map(operation => ({
                                key: operation.rawKey,
                                revision: operation.operation === 'set'
                                    ? operation.committedRevision
                                    : null,
                                valueHash: operation.operation === 'set'
                                    ? operation.valueHash
                                    : null,
                            }));
                        });
                    } catch (error) {
                        if (error instanceof PluginStorageRevisionConflict) {
                            return res.status(409).json({
                                success: false,
                                outcome: 'not-committed',
                                operation: 'batch',
                                error: error.message,
                                code: 'PLUGIN_STORAGE_REVISION_CONFLICT',
                                retryable: false,
                                conflicts: error.conflicts,
                            });
                        }
                        if (error instanceof PluginStorageLimitError) {
                            return sendPluginStorageMutationLimitError(res, 'batch', error);
                        }
                        logger.warn('[PluginStorageBatch] Transaction rolled back:', error);
                        return res.status(500).json({
                            success: false,
                            outcome: 'not-committed',
                            operation: 'batch',
                            error: 'Plugin storage batch transaction rolled back.',
                            code: 'PLUGIN_STORAGE_BATCH_ROLLED_BACK',
                            retryable: false,
                        });
                    }
        
                    const committedManifestRevision = `sha256:${sha256Hex(committedManifestBytes)}`;
                    pluginStorageManifestCache.publishPrepared(manifestUpdate, {
                        revision: committedPublicationRevision,
                        manifestRevision: committedManifestRevision,
                    });
        
                    // Establish the deferred BR1 recovery obligation from the known
                    // commit boundary before acknowledgement loss.
                    schedulePluginRecoverySnapshot();
        
                    if (pluginStorageBatchFailpoint === 'acknowledgement-loss') {
                        res.socket?.destroy();
                        return;
                    }
        
                    let verification = 'verified';
                    try {
                        // Retain the acknowledgement-downgrade failpoint without
                        // re-reading any committed row or manifest bytes.
                        hitPluginStorageBatchFailpoint('verification-read');
                    } catch (error) {
                        verification = 'unavailable';
                        logger.warn('[PluginStorageBatch] Post-commit verification unavailable:', error);
                    }
        
                    const acknowledgement = {
                        success: true,
                        outcome: 'committed',
                        operation: 'batch',
                        verification,
                        requestHash,
                        generation,
                        revisions: committedRevisions,
                        manifestRevision: committedManifestRevision,
                    };
                    if (pluginStorageBatchFailpoint === 'acknowledgement-delay') {
                        const acknowledgementTimer = setTimeout(() => {
                            if (!res.headersSent && !res.destroyed) res.json(acknowledgement);
                        }, pluginStorageBatchAcknowledgementDelayMs);
                        acknowledgementTimer.unref?.();
                        return;
                    }
                    return res.json(acknowledgement);
                });
            } catch (error) {
                if (isImportInProgressError(error)) {
                    res.setHeader('Retry-After', '5');
                    return res.status(503).json({
                        success: false,
                        outcome: 'not-committed',
                        operation: 'batch',
                        error: 'An import is in progress; retry this batch after it completes',
                        code: 'IMPORT_IN_PROGRESS',
                        retryable: true,
                    });
                }
                next(error);
            }
        });
    }

    /**
     * One logical V3 save mutation. The value row and its ownership sidecar share
     * one synchronous SQLite writer transaction. Empty owner means deliberately
     * unowned (delete stale metadata). Owned removes delete the matching sidecar;
     * value-only removes preserve it byte-exact for inline/optimized parity.
     */
    function sendPluginStorageMutationLimitError(res, operation, error) {
        return res.status(error.status || 413).json({
            success: false,
            outcome: 'not-committed',
            operation,
            error: error.message,
            code: error.code,
            limit: error.limit,
            actual: error.actual,
            retryable: false,
        });
    }

    function registerPluginStorageMutationRoute(app) {
        app.post('/api/plugin-storage/mutate', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            if (!checkActiveSession(req, res)) return;
        
            const firstHeader = (value) => Array.isArray(value) ? value[0] : value;
            const filePath = firstHeader(req.headers['file-path']);
            const operation = firstHeader(req.headers['x-plugin-storage-operation']);
            const requestedGeneration = firstHeader(req.headers['x-plugin-storage-generation']);
            const ownerHeader = firstHeader(req.headers['x-plugin-storage-owner']) ?? '';
            const ownerPolicyHeader = firstHeader(req.headers['x-plugin-storage-owner-policy']) ?? '';
            const ownerRecordHeader = firstHeader(req.headers['x-plugin-storage-owner-record']);
            const streamHeader = firstHeader(req.headers['x-plugin-storage-stream']);
            const reject = (error, code = 'INVALID_PLUGIN_STORAGE_MUTATION') => res.status(400).json({
                success: false,
                outcome: 'not-committed',
                operation: operation === 'remove' ? 'remove' : 'set',
                error,
                code,
                retryable: false,
            });
        
            // BR2 batch/CAS publications share this endpoint with AA1's exact
            // value+owner acknowledgement protocol. Dispatch by the canonical AA1
            // operation header so only one route owns the namespace and writer queue.
            if (operation === undefined) {
                return handlePluginStorageManifestMutation(req, res, next);
            }
        
            if (operation !== 'set' && operation !== 'remove') {
                return reject('Plugin storage operation must be set or remove.');
            }
            if (streamHeader !== undefined && streamHeader !== '1') {
                return reject('x-plugin-storage-stream must be 1 when present.');
            }
            const streamingSet = operation === 'set' && streamHeader === '1';
            if (operation === 'remove' && streamHeader !== undefined) {
                return reject('Remove mutations cannot stream a value body.');
            }
            if (requestedGeneration !== undefined
                && (typeof requestedGeneration !== 'string' || requestedGeneration.length === 0)) {
                return reject('Plugin storage generation must be a non-empty string.');
            }
            if (typeof filePath !== 'string' || !isHex(filePath)) {
                return reject('A valid value row path is required.');
            }
        
            let valueKey;
            let ownerKey;
            let owner = '';
            let ownerPolicy = 'replace';
            let ownerRecordBytes = null;
            let valueBytes = null;
            let valueFilePath = null;
            let valueHash = null;
            let valueSize = 0;
            let valueDisplaySize = null;
            try {
                ({ decodedKey: valueKey } = decodeAndCanonicalizeHexPath(filePath));
                const hashedValueKey = isHashedPluginSaveStorageKey(valueKey, PLUGIN_SAVE_PREFIX);
                const rawKey = hashedValueKey
                    ? null
                    : decodePluginSaveStorageKey(valueKey, PLUGIN_SAVE_PREFIX);
                const unrestrictedOwnerKey = `${PLUGIN_SAVE_META_PREFIX}${valueKey.slice(PLUGIN_SAVE_PREFIX.length)}`;
        
                if (operation === 'set') {
                    if (!['', 'preserve', 'record'].includes(ownerPolicyHeader)) {
                        throw new Error('Invalid plugin owner mutation policy.');
                    }
                    ownerPolicy = ownerPolicyHeader || 'replace';
                    if (typeof ownerHeader !== 'string' || !/^[A-Za-z0-9_-]*$/.test(ownerHeader)) {
                        throw new Error('Plugin owner must use canonical base64url encoding.');
                    }
                    owner = Buffer.from(ownerHeader, 'base64url').toString('utf-8');
                    if (Buffer.from(owner, 'utf-8').toString('base64url') !== ownerHeader) {
                        throw new Error('Plugin owner must use canonical UTF-8 base64url encoding.');
                    }
                    if (ownerPolicy === 'record') {
                        if (ownerHeader !== '' || typeof ownerRecordHeader !== 'string'
                            || !/^[A-Za-z0-9_-]+$/.test(ownerRecordHeader)) {
                            throw new Error('An exact owner record is required.');
                        }
                        ownerKey = unrestrictedOwnerKey;
                        ownerRecordBytes = Buffer.from(ownerRecordHeader, 'base64url');
                        if (ownerRecordBytes.toString('base64url') !== ownerRecordHeader) {
                            throw new Error('Plugin owner record must use canonical base64url encoding.');
                        }
                        validatePluginStorageRow(ownerKey, ownerRecordBytes);
                    } else if (ownerPolicy === 'preserve') {
                        if (ownerHeader !== '' || ownerRecordHeader !== undefined) {
                            throw new Error('Preserved ownership cannot include replacement data.');
                        }
                        ownerKey = unrestrictedOwnerKey;
                    } else {
                        if (ownerRecordHeader !== undefined) {
                            throw new Error('Unexpected plugin owner record.');
                        }
                        ownerKey = unrestrictedOwnerKey;
                    }
                    if (!streamingSet) {
                        if (!Buffer.isBuffer(req.body) || req.body.length === 0) {
                            throw new Error('A set mutation requires JSON value bytes.');
                        }
                        // express.raw already owns an exact Buffer for this request.
                        // Retain it through validation, hashing, and the transaction
                        // instead of allocating another full-value defensive copy.
                        valueBytes = req.body;
                        valueSize = valueBytes.length;
                        valueHash = sha256Hex(valueBytes);
                        // Match every other optimized plugin row ingress boundary.
                        valueDisplaySize = pluginStorageViewerDisplaySize(
                            validatePluginStorageRow(valueKey, valueBytes),
                        );
                    }
                } else {
                    if (!['', 'preserve'].includes(ownerPolicyHeader)
                        || ownerRecordHeader !== undefined) {
                        throw new Error('Remove mutations accept only the preserve owner policy.');
                    }
                    ownerPolicy = ownerPolicyHeader || 'replace';
                    // BR4 permits a few value-only keys whose corresponding metadata
                    // name is too long for an archive. Derive the unrestricted name so
                    // owned removal can clean it and value-only removal can preserve it.
                    ownerKey = unrestrictedOwnerKey;
                }
            } catch (error) {
                return reject(error instanceof Error ? error.message : String(error));
            }
        
            if (streamingSet) {
                const rawLength = firstHeader(req.headers['content-length']);
                const expectedLength = typeof rawLength === 'string' ? Number(rawLength) : NaN;
                if (!Number.isSafeInteger(expectedLength) || expectedLength <= 0) {
                    return reject('Streaming plugin mutations require an exact positive Content-Length.');
                }
                if (expectedLength > PLUGIN_VALUE_MAX_BYTES) {
                    return sendPluginStorageMutationLimitError(res, operation, new PluginStorageLimitError(
                        `Plugin value is ${expectedLength} bytes; the per-value limit is ${PLUGIN_VALUE_MAX_BYTES} bytes. Split the value into smaller records.`,
                        { code: 'PLUGIN_VALUE_TOO_LARGE', limit: PLUGIN_VALUE_MAX_BYTES, actual: expectedLength },
                    ));
                }
                if (!ensureDatabaseSpoolDirSync()) {
                    return res.status(503).json({
                        success: false,
                        outcome: 'not-committed',
                        operation,
                        error: 'The server upload spool is unavailable; check the save volume permissions.',
                        code: 'PLUGIN_STORAGE_SPOOL_UNAVAILABLE',
                        retryable: true,
                    });
                }
                valueFilePath = path.join(
                    getDatabaseSpoolDir(),
                    `${PLUGIN_VALUE_SPOOL_FILE_PREFIX}${nodeCrypto.randomUUID()}.upload`,
                );
                let received = 0;
                const digest = nodeCrypto.createHash('sha256');
                const meter = new Transform({
                    transform(chunk, _encoding, callback) {
                        received += chunk.length;
                        if (received > PLUGIN_VALUE_MAX_BYTES) {
                            return callback(new PluginStorageLimitError(
                                `Plugin value exceeded the ${PLUGIN_VALUE_MAX_BYTES}-byte per-value limit while uploading. Split the value into smaller records.`,
                                { code: 'PLUGIN_VALUE_TOO_LARGE', limit: PLUGIN_VALUE_MAX_BYTES, actual: received },
                            ));
                        }
                        digest.update(chunk);
                        callback(null, chunk);
                    },
                });
                try {
                    await pipeline(req, meter, createWriteStream(valueFilePath, { flags: 'wx' }));
                    if (received !== expectedLength) {
                        try { unlinkSync(valueFilePath); } catch {}
                        valueFilePath = null;
                        return reject(
                            `Plugin value length mismatch: expected ${expectedLength} bytes but received ${received}.`,
                            'PLUGIN_VALUE_LENGTH_MISMATCH',
                        );
                    }
                    valueSize = received;
                    valueHash = digest.digest('hex');
                    // Strict validation stays outside the authoritative mutation queue.
                    // The subsequent SQLite commit reads chunks directly from the spool.
                    try {
                        const displayMetadata = await validateJsonSource({
                            filePath: valueFilePath,
                            size: valueSize,
                        }, {
                            shouldAbort: () => req.aborted || res.destroyed,
                        });
                        valueDisplaySize = pluginStorageViewerDisplaySizeFromMetadata(
                            displayMetadata,
                        );
                    } catch {
                        // Preserve the single-row mutation diagnostic instead of
                        // exposing parser-specific streaming errors to the client.
                        throw new PluginStorageValidationError(valueKey);
                    }
                } catch (error) {
                    try { if (valueFilePath) unlinkSync(valueFilePath); } catch {}
                    valueFilePath = null;
                    if (error instanceof PluginStorageLimitError) {
                        return sendPluginStorageMutationLimitError(res, operation, error);
                    }
                    const diagnostic = logPluginStorageValidationFailure(
                        '[PluginStorage] Rejected invalid streamed row',
                        error,
                    );
                    if (diagnostic) return res.status(400).json({
                        success: false,
                        outcome: 'not-committed',
                        operation,
                        ...diagnostic,
                        retryable: false,
                    });
                    return next(error);
                }
            }
        
            try {
                await queueStorageMutation(async () => {
                    const publication = await readLivePluginStoragePublication();
                    const liveGeneration = publication.generation;
                    const activeManifest = liveGeneration
                        && publication.dbObj?.optimizePluginMemory === true
                        && publication.manifestState.valid
                        && publication.manifestState.manifest?.generation === liveGeneration
                        ? publication.manifestState.manifest
                        : null;
                    const pinnedState = sessionPluginStorageReadState(req);
                    if (
                        (requestedGeneration !== undefined && (
                            requestedGeneration !== liveGeneration
                            || !activeManifest
                            || (pinnedState && (
                                pinnedState.optimized !== true
                                || pinnedState.generation !== requestedGeneration
                            ))
                        ))
                        || (liveGeneration && requestedGeneration === undefined)
                    ) {
                        return res.status(409).json({
                            success: false,
                            outcome: 'not-committed',
                            operation,
                            error: 'Plugin storage generation changed before the mutation committed.',
                            code: 'PLUGIN_STORAGE_GENERATION_CONFLICT',
                            retryable: true,
                            ...(activeManifest && publication.manifestState.revision
                                ? {
                                    currentGeneration: liveGeneration,
                                    currentManifestRevision: publication.manifestState.revision,
                                }
                                : {}),
                        });
                    }
                    if (isHashedPluginSaveStorageKey(valueKey, PLUGIN_SAVE_PREFIX)) {
                        if (!activeManifest) {
                            return res.status(409).json({
                                success: false,
                                outcome: 'not-committed',
                                operation,
                                error: 'A hashed plugin storage key requires an active mapped manifest.',
                                code: 'PLUGIN_STORAGE_GENERATION_CONFLICT',
                                retryable: true,
                                ...(activeManifest && publication.manifestState.revision
                                    ? {
                                        currentGeneration: liveGeneration,
                                        currentManifestRevision: publication.manifestState.revision,
                                    }
                                    : {}),
                            });
                        }
                        decodeManifestPluginSaveStorageKey(
                            activeManifest,
                            valueKey,
                            PLUGIN_SAVE_PREFIX,
                        );
                    }
                    let manifestUpdate = null;
                    if (activeManifest) {
                        const valueAdds = operation === 'set' ? [valueKey] : [];
                        const valueDeletes = operation === 'remove' ? [valueKey] : [];
                        const metaAdds = [];
                        const metaDeletes = [];
                        if ((operation === 'remove' && ownerPolicy !== 'preserve')
                            || (ownerPolicy === 'replace' && !owner)) {
                            metaDeletes.push(ownerKey);
                        } else if (operation === 'set' && ownerPolicy !== 'preserve') {
                            metaAdds.push(ownerKey);
                        }
                        manifestUpdate = pluginStorageManifestCache.prepareUpdate(
                            publication.manifestEntry,
                            { valueAdds, valueDeletes, metaAdds, metaDeletes },
                        );
                    }
                    const nextManifest = manifestUpdate?.manifest ?? null;
                    const previousManifestRevision = manifestUpdate
                        ? publication.manifestState.revision
                        : null;
                    const recoverySnapshotToken = newPluginRecoverySnapshotToken();
                    let committedManifestBytes = null;
                    let committedPublicationRevision = null;
                    try {
                        sqliteDb.transaction(() => {
                            if (operation === 'set') {
                                if (valueFilePath) {
                                    kvSetFromFile(valueKey, valueFilePath, {
                                        pluginStorageDisplaySize: valueDisplaySize,
                                    });
                                }
                                else {
                                    kvSet(valueKey, valueBytes, {
                                        pluginStorageDisplaySize: valueDisplaySize,
                                    });
                                }
                                hitPluginStorageMutationFailpoint('owner-write');
                                if (ownerPolicy === 'record') {
                                    kvSet(ownerKey, ownerRecordBytes);
                                } else if (ownerPolicy === 'preserve') {
                                    // Boot recovery has no inline owner for this key;
                                    // retain any historical external sidecar byte-exact.
                                } else if (owner) {
                                    kvSet(ownerKey, Buffer.from(JSON.stringify({
                                        plugin: owner,
                                        updatedAt: Date.now(),
                                        revision: nodeCrypto.randomUUID(),
                                        generation: nodeCrypto.randomUUID(),
                                    }), 'utf-8'));
                                } else {
                                    kvDel(ownerKey);
                                }
                            } else {
                                kvDel(valueKey);
                                hitPluginStorageMutationFailpoint('owner-remove');
                                if (ownerPolicy !== 'preserve') kvDel(ownerKey);
                            }
                            maybeFailPluginStorageTransaction(req, 'after-row');
                            committedManifestBytes = writePluginStorageManifest(nextManifest);
                            maybeFailPluginStorageTransaction(req, 'after-manifest');
                            hitPluginStorageMutationFailpoint('pre-commit');
                            markPluginRecoverySnapshotDirty(recoverySnapshotToken);
                            committedPublicationRevision =
                                kvGetPluginStoragePublicationRevision();
                        })();
                    } catch (error) {
                        logger.warn('[PluginStorageMutation] Transaction rolled back:', error);
                        if (error instanceof PluginStorageLimitError) {
                            return sendPluginStorageMutationLimitError(res, operation, error);
                        }
                        return res.status(500).json({
                            success: false,
                            outcome: 'not-committed',
                            operation,
                            error: 'Plugin storage transaction rolled back.',
                            code: 'PLUGIN_STORAGE_TRANSACTION_ROLLED_BACK',
                            retryable: false,
                        });
                    }
        
                    const committedManifestRevision = manifestUpdate
                        ? `sha256:${sha256Hex(committedManifestBytes)}`
                        : null;
                    if (manifestUpdate) {
                        pluginStorageManifestCache.publishPrepared(manifestUpdate, {
                            revision: committedPublicationRevision,
                            manifestRevision: committedManifestRevision,
                        });
                    }
        
                    // kvSetFromFile has consumed the private upload by this known-commit
                    // boundary. Remove it before acknowledging success so callers never
                    // observe a committed response while its normal-path spool remains.
                    try { if (valueFilePath) unlinkSync(valueFilePath); } catch {}
                    valueFilePath = null;
        
                    // Schedule from the known-commit boundary before a deliberately
                    // lost acknowledgement can return control.
                    schedulePluginRecoverySnapshot();
        
                    if (pluginStorageMutationFailpoint === 'acknowledgement-loss') {
                        // The transaction is durably committed, but the client receives
                        // no schema-valid response and must report outcome unknown.
                        res.socket?.destroy();
                        return;
                    }
        
                    let verification = 'verified';
                    try {
                        // Retain the acknowledgement-downgrade failpoint without
                        // re-reading any committed row or manifest bytes.
                        hitPluginStorageMutationFailpoint('verification-read');
                    } catch (error) {
                        // The writer transaction has already returned successfully.
                        // Never reject a known committed primary mutation because a
                        // test-only acknowledgement diagnostic failed.
                        verification = 'unavailable';
                        logger.warn('[PluginStorageMutation] Post-commit verification unavailable:', error);
                    }
        
                    return res.json({
                        success: true,
                        outcome: 'committed',
                        operation,
                        verification,
                        hash: operation === 'set' ? valueHash : undefined,
                        ...(committedManifestRevision
                            ? {
                                manifestRevision: committedManifestRevision,
                                previousManifestRevision,
                            }
                            : {}),
                    });
                });
            } catch (error) {
                if (isImportInProgressError(error)) {
                    res.setHeader('Retry-After', '5');
                    return res.status(503).json({
                        success: false,
                        outcome: 'not-committed',
                        operation,
                        error: 'An import is in progress; retry this write after it completes',
                        code: 'IMPORT_IN_PROGRESS',
                        retryable: true,
                    });
                }
                next(error);
            } finally {
                try { if (valueFilePath) unlinkSync(valueFilePath); } catch {}
            }
        });
        
        async function handlePluginStorageManifestMutation(req, res, next) {
            try {
                const inspection = await inspectRisuSaveSource(req.body);
                const plan = inspection.format === 'raw'
                    ? await decodeRisuSave(req.body)
                    : await decodeBoundedLegacyRisuSave(req.body, {
                        inspection,
                        tempDir: requireDatabaseSpoolDirSync(),
                        maxLegacyBytes: LEGACY_DATABASE_IMPORT_MAX_BYTES,
                    });
                await queueStorageMutation(async () => {
                    await flushPendingDb();
                    const rawDatabase = kvGet('database/database.bin');
                    if (!rawDatabase) return res.status(409).json({ error: 'Database not found' });
                    const liveDb = await decodeAuthoritativeDatabase(rawDatabase);
                    const generation = typeof plan?.generation === 'string' && plan.generation.length > 0
                        ? plan.generation
                        : null;
                    const expectedManifest = normalizePluginStorageManifestRequest(
                        plan?.expectedManifest,
                        'expectedManifest',
                    );
                    const nextManifest = normalizePluginStorageManifestRequest(
                        plan?.nextManifest,
                        'nextManifest',
                    );
                    const manifestState = readPluginStorageManifestState();
                    if (
                        plan?.version !== 1
                        || !generation
                        || liveDb?.optimizePluginMemory !== true
                        || pluginStorageGeneration(liveDb) !== generation
                        || !manifestState.valid
                        || !pluginStorageManifestEquals(manifestState.manifest, expectedManifest)
                        || expectedManifest.generation !== generation
                        || nextManifest.generation !== generation
                    ) {
                        return res.status(409).json({
                            error: 'Plugin storage state changed while the mutation was being prepared',
                        });
                    }
        
                    const writes = Array.isArray(plan.writes) ? plan.writes : null;
                    const deletes = Array.isArray(plan.deletes) ? plan.deletes : null;
                    if (!writes || !deletes) {
                        return res.status(400).json({ error: 'writes and deletes must be arrays' });
                    }
                    const nextValueKeys = new Set(expectedManifest.valueKeys);
                    const nextMetaKeys = new Set(expectedManifest.metaKeys);
                    const seen = new Set();
                    const classify = (storageKey) => {
                        if (typeof storageKey !== 'string') throw new TypeError('storage key must be a string');
                        const prefix = storageKey.startsWith(PLUGIN_SAVE_META_PREFIX)
                            ? PLUGIN_SAVE_META_PREFIX
                            : storageKey.startsWith(PLUGIN_SAVE_PREFIX)
                                ? PLUGIN_SAVE_PREFIX
                                : null;
                        if (!prefix) throw new TypeError(`Invalid plugin storage key: ${storageKey}`);
                        const mappingManifest = nextManifest.valueKeys.includes(storageKey)
                            || nextManifest.metaKeys.includes(storageKey)
                            ? nextManifest
                            : expectedManifest;
                        decodeManifestPluginSaveStorageKey(mappingManifest, storageKey, prefix);
                        if (seen.has(storageKey)) throw new TypeError(`Duplicate plugin storage mutation: ${storageKey}`);
                        seen.add(storageKey);
                        return prefix === PLUGIN_SAVE_META_PREFIX ? nextMetaKeys : nextValueKeys;
                    };
                    const preparedWrites = writes.map((write) => {
                        const keys = classify(write?.storageKey);
                        if (!(write?.valueBytes instanceof Uint8Array)) {
                            throw new TypeError('Plugin storage writes require valueBytes');
                        }
                        const value = Buffer.from(write.valueBytes);
                        validatePluginStorageRow(write.storageKey, value);
                        keys.add(write.storageKey);
                        return { storageKey: write.storageKey, value };
                    });
                    const preparedDeletes = deletes.map((storageKey) => {
                        const keys = classify(storageKey);
                        keys.delete(storageKey);
                        return storageKey;
                    });
                    const derivedManifest = createPluginStorageManifest(
                        generation,
                        nextValueKeys,
                        nextMetaKeys,
                        nextManifest.version === 3 ? nextManifest.keyMappings : [],
                    );
                    if (!pluginStorageManifestEquals(derivedManifest, nextManifest)) {
                        return res.status(400).json({
                            error: 'nextManifest does not exactly match the requested row mutations',
                        });
                    }
        
                    const recoverySnapshotToken = newPluginRecoverySnapshotToken();
                    withPluginStorageQuotaPlan([
                        ...preparedWrites
                            .filter(write => write.storageKey.startsWith(PLUGIN_SAVE_PREFIX))
                            .map(write => ({ key: write.storageKey, size: write.value.length })),
                        ...preparedDeletes
                            .filter(storageKey => storageKey.startsWith(PLUGIN_SAVE_PREFIX))
                            .map(key => ({ key, size: null })),
                    ], () => {
                        for (const write of preparedWrites) {
                            kvSet(write.storageKey, write.value);
                            maybeFailPluginStorageTransaction(req, 'after-row');
                        }
                        for (const storageKey of preparedDeletes) {
                            kvDel(storageKey);
                            maybeFailPluginStorageTransaction(req, 'after-row');
                        }
                        writePluginStorageManifest(nextManifest);
                        maybeFailPluginStorageTransaction(req, 'after-manifest');
                        markPluginRecoverySnapshotDirty(recoverySnapshotToken);
                    });
                    schedulePluginRecoverySnapshot();
                    res.json({ success: true });
                });
            } catch (error) {
                if (isImportInProgressError(error)) {
                    res.setHeader('Retry-After', '5');
                    return res.status(503).json({
                        success: false,
                        commitOutcome: 'not-committed',
                        commitOutcomeUnknown: false,
                        error: 'An import is in progress; retry this mutation after it completes',
                        code: 'IMPORT_IN_PROGRESS',
                        retryable: true,
                    });
                }
                const refusal = risuSavePreparationRefusal(error);
                if (refusal) return res.status(refusal.status).json(refusal.body);
                next(error);
            }
        }
    }

    function registerPluginStorageTransitionRoutes(app) {
        function pluginTransitionStageResponse(stage) {
            return {
                success: true,
                transitionId: stage.transitionId,
                state: stage.state,
                direction: stage.targetOptimized ? 'externalize' : 'internalize',
                targetGeneration: stage.targetGeneration,
                rows: stage.rows.map(row => ({
                    storageKey: row.storageKey,
                    rawKey: row.rawKey,
                    size: row.size,
                    sha256: row.sha256 ?? null,
                    uploaded: row.uploaded === true,
                })),
                uploaded: stage.rows.filter(row => row.uploaded === true).length,
                total: stage.rows.length,
                totalBytes: stage.rows.reduce((sum, row) => sum + row.size, 0),
                etag: stage.resultEtag ?? undefined,
            };
        }
        function normalizedPluginTransitionRows(rows) {
            if (!Array.isArray(rows) || rows.length > 100_000) {
                throw new TypeError('Plugin transition rows must be a bounded array');
            }
            const seen = new Set();
            let total = 0;
            return rows.map((row, index) => {
                if (!row || typeof row !== 'object' || Array.isArray(row)) {
                    throw new TypeError('Invalid plugin transition row descriptor');
                }
                const rowKeys = Object.keys(row).sort();
                const hasRawKey = Object.hasOwn(row, 'rawKey');
                if ((hasRawKey
                    && (rowKeys.length !== 3
                        || rowKeys[0] !== 'rawKey'
                        || rowKeys[1] !== 'size'
                        || rowKeys[2] !== 'storageKey'))
                    || (!hasRawKey
                        && (rowKeys.length !== 2
                            || rowKeys[0] !== 'size'
                            || rowKeys[1] !== 'storageKey'))) {
                    throw new TypeError('Transition row descriptors require rawKey, storageKey, and size');
                }
                const storageKey = row.storageKey;
                const prefix = canonicalPluginStorageRowPrefix(storageKey);
                let rawKey = row.rawKey;
                if (prefix && !hasRawKey) {
                    try {
                        rawKey = decodePluginSaveStorageKey(storageKey, prefix);
                    } catch {
                        rawKey = null;
                    }
                }
                if (!prefix || typeof rawKey !== 'string'
                    || encodePluginSaveStorageKey(rawKey, prefix) !== storageKey
                    || seen.has(storageKey)) {
                    throw new TypeError('Plugin transition row keys must be unique and canonical');
                }
                seen.add(storageKey);
                const size = row.size;
                if (!Number.isSafeInteger(size) || size <= 0 || size > PLUGIN_TRANSITION_MAX_ROW_BYTES) {
                    throw new PluginStorageLimitError(
                        `Plugin transition row exceeds the ${PLUGIN_TRANSITION_MAX_ROW_BYTES}-byte transition limit.`,
                        {
                            code: 'PLUGIN_STORAGE_SIZE_LIMIT',
                            limit: PLUGIN_TRANSITION_MAX_ROW_BYTES,
                            actual: size,
                        },
                    );
                }
                total += size;
                if (!Number.isSafeInteger(total)) {
                    throw new TypeError('Plugin transition size exceeds the safe integer range');
                }
                return {
                    index,
                    storageKey,
                    rawKey,
                    size,
                    sha256: null,
                    stagedSha256: null,
                    uploaded: false,
                };
            });
        }
        
        function authoritativeInlinePluginTransitionRows(liveDb) {
            const rows = [];
            const seen = new Set();
            const appendRecord = (record, prefix) => {
                if (record === undefined) return;
                if (record === null || typeof record !== 'object' || Array.isArray(record)) {
                    throw new PluginStorageValidationError(prefix);
                }
                const prototype = Reflect.getPrototypeOf(record);
                if (prototype !== Object.prototype && prototype !== null) {
                    throw new PluginStorageValidationError(prefix);
                }
                for (const rawKey of Reflect.ownKeys(record)) {
                    if (typeof rawKey !== 'string') throw new PluginStorageValidationError(prefix);
                    const storageKey = encodeValidatedPluginStorageKey(rawKey, prefix);
                    if (seen.has(storageKey)) throw new PluginStorageValidationError(storageKey);
                    const descriptor = Reflect.getOwnPropertyDescriptor(record, rawKey);
                    if (!descriptor || !('value' in descriptor) || !descriptor.enumerable) {
                        throw new PluginStorageValidationError(storageKey);
                    }
                    const value = serializePluginStorageRow(storageKey, descriptor.value);
                    if (value.length > PLUGIN_TRANSITION_MAX_ROW_BYTES) {
                        throw new PluginStorageLimitError(
                            `Plugin transition row exceeds the ${PLUGIN_TRANSITION_MAX_ROW_BYTES}-byte transition limit.`,
                            {
                                code: 'PLUGIN_STORAGE_SIZE_LIMIT',
                                limit: PLUGIN_TRANSITION_MAX_ROW_BYTES,
                                actual: value.length,
                            },
                        );
                    }
                    seen.add(storageKey);
                    rows.push({
                        storageKey,
                        rawKey,
                        size: value.length,
                        sha256: sha256Hex(value),
                        stagedSha256: null,
                        displaySize: prefix === PLUGIN_SAVE_PREFIX
                            ? pluginStorageViewerDisplaySize(descriptor.value)
                            : null,
                        uploaded: false,
                    });
                    if (rows.length > 100_000) {
                        throw new PluginStorageLimitError(
                            'Plugin storage exceeds the 100000-entry transition limit.',
                            { code: 'PLUGIN_STORAGE_SIZE_LIMIT', limit: 100_000, actual: rows.length },
                        );
                    }
                }
            };
            appendRecord(liveDb?.pluginCustomStorage, PLUGIN_SAVE_PREFIX);
            appendRecord(liveDb?.pluginStorageMeta, PLUGIN_SAVE_META_PREFIX);
            return rows.map((row, index) => ({ ...row, index }));
        }
        
        function assertDeclaredTransitionRowsMatch(declaredRows, authoritativeRows) {
            const mismatch = () => {
                const error = new Error('Transition rows must exactly match authoritative inline storage');
                error.pluginTransitionPlanMismatch = true;
                return error;
            };
            if (!Array.isArray(declaredRows) || declaredRows.length !== authoritativeRows.length) {
                throw mismatch();
            }
            let declared;
            try {
                declared = normalizedPluginTransitionRows(declaredRows);
            } catch {
                throw mismatch();
            }
            const declaredByKey = new Map(declared.map(row => [row.storageKey, row]));
            for (const row of authoritativeRows) {
                const candidate = declaredByKey.get(row.storageKey);
                if (!candidate || candidate.size !== row.size || candidate.rawKey !== row.rawKey) {
                    throw mismatch();
                }
            }
        }
        
        function assertInlineTransitionSourceHashes(liveDb, sourceRowHashes) {
            const currentRows = authoritativeInlinePluginTransitionRows(liveDb);
            const expected = new Map(
                sourceRowHashes
                    .filter(row => row.backend === 'inline')
                    .map(row => [row.storageKey, row]),
            );
            if (currentRows.length !== expected.size) {
                throw new Error('Inline plugin storage changed during transition');
            }
            for (const row of currentRows) {
                const source = expected.get(row.storageKey);
                if (!source || source.size !== row.size || source.sha256 !== row.sha256) {
                    throw new Error('Inline plugin storage changed during transition');
                }
            }
        }
        async function computeFileSha256(filePath) {
            const hash = nodeCrypto.createHash('sha256');
            for await (const chunk of createReadStream(filePath)) hash.update(chunk);
            return hash.digest('hex');
        }
        
        async function spoolPluginTransitionKvRow(storageKey, destinationPath, options = {}) {
            const expectedSize = kvSize(storageKey);
            if (!Number.isSafeInteger(expectedSize) || expectedSize < 0) {
                throw new PluginStorageValidationError(storageKey);
            }
            if (expectedSize > PLUGIN_TRANSITION_MAX_ROW_BYTES) {
                throw new PluginStorageLimitError(
                    'Plugin storage contains a row outside the configured value limit.',
                    {
                        code: 'PLUGIN_STORAGE_SIZE_LIMIT',
                        limit: PLUGIN_TRANSITION_MAX_ROW_BYTES,
                        actual: expectedSize,
                    },
                );
            }
            const digest = nodeCrypto.createHash('sha256');
            const result = await kvWriteToFile(storageKey, destinationPath, {
                shouldAbort: options.shouldAbort,
                onBytes: (bytes) => digest.update(bytes),
            });
            if (!result || result.size !== expectedSize) {
                throw new PluginStorageValidationError(storageKey);
            }
            await hardenPrivateFile(destinationPath, 0o600, { fs });
            let displaySize = null;
            if (options.validateJson !== false) {
                try {
                    const displayMetadata = await validateJsonSource({
                        filePath: destinationPath,
                        size: result.size,
                    }, {
                        shouldAbort: options.shouldAbort,
                    });
                    if (storageKey.startsWith(PLUGIN_SAVE_PREFIX)) {
                        displaySize = pluginStorageViewerDisplaySizeFromMetadata(displayMetadata);
                    }
                } catch {
                    throw new PluginStorageValidationError(storageKey);
                }
            }
            const sha256 = digest.digest('hex');
            return { size: result.size, sha256, displaySize };
        }
        
        async function writeDurablePluginTransitionStageRow(storageKey, filePath, shouldAbort) {
            const temporaryPath = `${filePath}.${nodeCrypto.randomUUID()}.tmp`;
            try {
                const result = await spoolPluginTransitionKvRow(storageKey, temporaryPath, {
                    shouldAbort,
                    validateJson: true,
                });
                const fileDescriptor = openSync(temporaryPath, 'r');
                try {
                    fsyncSync(fileDescriptor);
                } finally {
                    closeSync(fileDescriptor);
                }
                renameSync(temporaryPath, filePath);
                fsyncPluginTransitionStageDirectory();
                return result;
            } catch (error) {
                try { unlinkSync(temporaryPath); } catch {}
                fsyncPluginTransitionStageDirectory();
                throw error;
            }
        }
        
        async function pluginTransitionKvRowMatches(storageKey, expected, shouldAbort) {
            const temporaryPath = path.join(
                getPluginTransitionStageDir(),
                `${PLUGIN_TRANSITION_STAGE_PREFIX}${nodeCrypto.randomUUID()}.verify.tmp`,
            );
            try {
                const actual = await spoolPluginTransitionKvRow(storageKey, temporaryPath, {
                    shouldAbort,
                    validateJson: false,
                });
                return actual.size === expected.size && actual.sha256 === expected.sha256;
            } finally {
                try { unlinkSync(temporaryPath); } catch {}
            }
        }
        
        async function assertInternalTransitionBounds(liveDb, sourceKeys) {
            const ownedValueRaw = new Set(sourceKeys.valueKeys.map(
                key => sourceKeys.manifest
                    ? decodeManifestPluginSaveStorageKey(sourceKeys.manifest, key, PLUGIN_SAVE_PREFIX)
                    : decodeValidatedPluginStorageKey(key, PLUGIN_SAVE_PREFIX),
            ));
            const ownedMetaRaw = new Set(sourceKeys.metaKeys.map(
                key => sourceKeys.manifest
                    ? decodeManifestPluginSaveStorageKey(sourceKeys.manifest, key, PLUGIN_SAVE_META_PREFIX)
                    : decodeValidatedPluginStorageKey(key, PLUGIN_SAVE_META_PREFIX),
            ));
            let entries = sourceKeys.valueKeys.length + sourceKeys.metaKeys.length;
            let bytes = 0;
            const addSize = size => {
                if (!Number.isSafeInteger(size) || size < 0 || size > PLUGIN_TRANSITION_MAX_ROW_BYTES) {
                    throw new PluginStorageLimitError(
                        'Plugin storage contains a row outside the transition limit.',
                        {
                            code: 'PLUGIN_STORAGE_SIZE_LIMIT',
                            limit: PLUGIN_TRANSITION_MAX_ROW_BYTES,
                            actual: size,
                        },
                    );
                }
                bytes += size;
                if (!Number.isSafeInteger(bytes)) {
                    throw new PluginStorageLimitError(
                        'Plugin storage size exceeds the safe transition range.',
                        {
                            code: 'PLUGIN_STORAGE_SIZE_LIMIT',
                            limit: Number.MAX_SAFE_INTEGER,
                            actual: bytes,
                        },
                    );
                }
            };
            const selectedSizes = new Map(
                kvListSelectedWithSizes([...sourceKeys.valueKeys, ...sourceKeys.metaKeys])
                    .map((entry) => [entry.key, entry.size]),
            );
            for (const key of sourceKeys.valueKeys) addSize(selectedSizes.get(key));
            for (const key of sourceKeys.metaKeys) addSize(selectedSizes.get(key));
            const accountInline = (record, prefix, externalKeys) => {
                for (const [rawKey, value] of Object.entries(record ?? {})) {
                    const storageKey = encodeValidatedPluginStorageKey(rawKey, prefix);
                    if (externalKeys.has(rawKey)) continue;
                    const rowBytes = serializePluginStorageRow(storageKey, value);
                    entries++;
                    addSize(rowBytes.length);
                }
            };
            accountInline(liveDb.pluginCustomStorage, PLUGIN_SAVE_PREFIX, ownedValueRaw);
            accountInline(liveDb.pluginStorageMeta, PLUGIN_SAVE_META_PREFIX, ownedMetaRaw);
            if (entries > 100_000) {
                throw new PluginStorageLimitError(
                    'Plugin storage exceeds the 100000-entry internalization limit.',
                    { code: 'PLUGIN_STORAGE_SIZE_LIMIT', limit: 100_000, actual: entries },
                );
            }
            const currentDbBytes = kvSize('database/database.bin') ?? 0;
            const required = bytes * 3 + currentDbBytes * 2;
            if (!Number.isSafeInteger(required)) {
                throw new PluginStorageLimitError(
                    'Plugin storage transition disk requirement is too large.',
                    { code: 'PLUGIN_STORAGE_DISK_LIMIT', limit: Number.MAX_SAFE_INTEGER, actual: required },
                );
            }
            const disk = await checkDiskSpace(required);
            if (!disk.ok) {
                throw new PluginStorageLimitError(
                    `Plugin storage transition requires ${required} free bytes.`,
                    { code: 'PLUGIN_STORAGE_DISK_LIMIT', limit: disk.available, actual: required },
                );
            }
            return { entries, bytes };
        }
        app.post('/api/plugin-storage/transition/stage/begin', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            if (!checkActiveSession(req, res)) return;
            try {
                const plan = req.body;
                const planKeys = plan && typeof plan === 'object' && !Array.isArray(plan)
                    ? Object.keys(plan)
                    : [];
                const allowedPlanKeys = new Set([
                    'version',
                    'transitionId',
                    'source',
                    'targetOptimized',
                    'targetGeneration',
                    'rows',
                    'expectedEtag',
                ]);
                const requiredPlanKeys = [
                    'version',
                    'transitionId',
                    'source',
                    'targetOptimized',
                    'targetGeneration',
                    'rows',
                ];
                const sourceKeys = plan?.source && typeof plan.source === 'object'
                    && !Array.isArray(plan.source)
                    ? Object.keys(plan.source).sort()
                    : [];
                const sourceShapeValid = sourceKeys.length === 3
                    && sourceKeys[0] === 'generation'
                    && sourceKeys[1] === 'manifest'
                    && sourceKeys[2] === 'optimized';
                const manifest = plan?.source?.manifest;
                const manifestShapeValid = manifest === null
                    || parsePluginStorageManifest(manifest) !== null;
                if (plan?.version !== 2
                    || !PLUGIN_STORAGE_UUID_PATTERN.test(plan.transitionId)
                    || !PLUGIN_STORAGE_UUID_PATTERN.test(plan.targetGeneration)
                    || typeof plan.targetOptimized !== 'boolean'
                    || planKeys.some(key => !allowedPlanKeys.has(key))
                    || requiredPlanKeys.some(key => !planKeys.includes(key))
                    || !sourceShapeValid
                    || typeof plan.source.optimized !== 'boolean'
                    || !(plan.source.generation === null || typeof plan.source.generation === 'string')
                    || !manifestShapeValid
                    || !Array.isArray(plan.rows)
                    || (plan.expectedEtag !== undefined && typeof plan.expectedEtag !== 'string')) {
                    return res.status(400).json({ error: 'Invalid staged plugin transition plan' });
                }
                await queueStorageMutation(async () => {
                    await flushPendingDb();
                    const rawDatabase = kvGet('database/database.bin');
                    if (!rawDatabase) return res.status(409).json({ error: 'Database not found' });
                    const currentEtag = ensurePluginStorageTransitionDbEtag(rawDatabase);
                    const requestHash = sha256Hex(Buffer.from(JSON.stringify(plan), 'utf-8'));
                    let existing = readPluginTransitionStage(plan.transitionId);
                    if (existing) {
                        if (!pluginTransitionStageBelongsToRequest(existing, req)) {
                            return res.status(404).json({ error: 'Transition not found' });
                        }
                        existing = await refreshPluginTransitionStageState(existing);
                        if (existing.requestHash !== requestHash) {
                            return res.status(409).json({ error: 'Transition id is already bound to another plan' });
                        }
                        return res.json(pluginTransitionStageResponse(existing));
                    }
                    const activeStage = await findActivePluginTransition(req, plan.transitionId);
                    if (activeStage) {
                        return res.status(409).json({
                            error: 'Another plugin storage transition is already active',
                        });
                    }
                    const liveDb = await decodeAuthoritativeDatabase(rawDatabase);
                    const manifestState = readPluginStorageManifestState();
                    try {
                        assertPluginStorageSource(plan.source, liveDb, manifestState);
                    } catch (error) {
                        if (error?.pluginStorageConflict) {
                            return res.status(409).json({ error: error.message, currentEtag });
                        }
                        throw error;
                    }
                    if (plan.expectedEtag && plan.expectedEtag !== currentEtag) {
                        return res.status(409).json({ error: 'ETag mismatch', currentEtag });
                    }
                    if (
                        plan.targetOptimized === (liveDb?.optimizePluginMemory === true)
                        || plan.targetGeneration === pluginStorageGeneration(liveDb)
                    ) {
                        return res.status(400).json({ error: 'Transition target must use a fresh mode generation' });
                    }
                    const sourceKeys = resolveOwnedPluginStorageKeys(liveDb);
                    let rows;
                    if (plan.targetOptimized) {
                        rows = authoritativeInlinePluginTransitionRows(liveDb);
                        try {
                            assertDeclaredTransitionRowsMatch(plan.rows, rows);
                        } catch (error) {
                            if (error?.pluginTransitionPlanMismatch) {
                                return res.status(409).json({
                                    error: error.message,
                                    code: 'PLUGIN_STORAGE_CHANGED',
                                });
                            }
                            throw error;
                        }
                        const stagedBytes = rows.reduce((sum, row) => sum + row.size, 0);
                        const required = stagedBytes * 3 + (kvSize('database/database.bin') ?? 0) * 2;
                        if (!Number.isSafeInteger(required)) {
                            throw new PluginStorageLimitError(
                                'Plugin transition disk requirement is too large.',
                                {
                                    code: 'PLUGIN_STORAGE_DISK_LIMIT',
                                    limit: Number.MAX_SAFE_INTEGER,
                                    actual: required,
                                },
                            );
                        }
                        const disk = await checkDiskSpace(required);
                        if (!disk.ok) {
                            throw new PluginStorageLimitError(
                                `Plugin transition requires ${required} free bytes.`,
                                {
                                    code: 'PLUGIN_STORAGE_DISK_LIMIT',
                                    limit: disk.available,
                                    actual: required,
                                },
                            );
                        }
                    } else {
                        if (Array.isArray(plan.rows) && plan.rows.length > 0) {
                            return res.status(400).json({ error: 'Internalization rows are server-derived' });
                        }
                        await assertInternalTransitionBounds(liveDb, sourceKeys);
                        rows = [];
                        try {
                            for (const storageKey of [...sourceKeys.valueKeys, ...sourceKeys.metaKeys]) {
                                if (req.aborted || res.destroyed) {
                                    throw new Error('Plugin transition begin disconnected');
                                }
                                const index = rows.length;
                                const prefix = storageKey.startsWith(PLUGIN_SAVE_META_PREFIX)
                                    ? PLUGIN_SAVE_META_PREFIX
                                    : PLUGIN_SAVE_PREFIX;
                                const rawKey = sourceKeys.manifest
                                    ? decodeManifestPluginSaveStorageKey(
                                        sourceKeys.manifest,
                                        storageKey,
                                        prefix,
                                    )
                                    : decodeValidatedPluginStorageKey(storageKey, prefix);
                                const filePath = pluginTransitionStageRowPath(plan.transitionId, index);
                                const staged = await writeDurablePluginTransitionStageRow(
                                    storageKey,
                                    filePath,
                                    () => req.aborted || res.destroyed,
                                );
                                rows.push({
                                    index,
                                    storageKey,
                                    rawKey,
                                    size: staged.size,
                                    sha256: staged.sha256,
                                    stagedSha256: staged.sha256,
                                    displaySize: staged.displaySize,
                                    uploaded: true,
                                });
                            }
                        } catch (error) {
                            for (const row of rows) {
                                try {
                                    unlinkSync(pluginTransitionStageRowPath(plan.transitionId, row.index));
                                } catch {}
                            }
                            fsyncPluginTransitionStageDirectory();
                            throw error;
                        }
                    }
                    const stage = {
                        version: 1,
                        transitionId: plan.transitionId,
                        sessionId: typeof req.headers['x-session-id'] === 'string'
                            ? req.headers['x-session-id']
                            : null,
                        requestHash,
                        source: plan.source,
                        sourceEtag: currentEtag,
                        sourceRowHashes: rows.map(row => ({
                            storageKey: row.storageKey,
                            size: row.size,
                            sha256: row.sha256,
                            backend: plan.targetOptimized ? 'inline' : 'kv',
                        })),
                        targetOptimized: plan.targetOptimized,
                        targetGeneration: plan.targetGeneration,
                        rows,
                        state: rows.every(row => row.uploaded) ? 'ready' : 'uploading',
                        createdAt: Date.now(),
                        updatedAt: Date.now(),
                    };
                    try {
                        writePluginTransitionStage(stage);
                    } catch (error) {
                        removePluginTransitionStage(stage);
                        throw error;
                    }
                    res.json(pluginTransitionStageResponse(stage));
                });
            } catch (error) {
                next(error);
            }
        });
        
        app.post('/api/plugin-storage/transition/stage/upload', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            if (!checkActiveSession(req, res)) return;
            let temporaryPath = null;
            try {
                const transitionId = req.headers['x-plugin-storage-transition'];
                const storageKey = req.headers['x-plugin-storage-key'];
                if (typeof transitionId !== 'string' || typeof storageKey !== 'string') {
                    return res.status(400).json({ error: 'Transition id and storage key are required' });
                }
                const stage = readPluginTransitionStage(transitionId);
                if (!pluginTransitionStageBelongsToRequest(stage, req) || stage.state === 'aborted') {
                    return res.status(404).json({ error: 'Transition not found' });
                }
                if (stage.state === 'committed') return res.json(pluginTransitionStageResponse(stage));
                if (!stage.targetOptimized) return res.status(409).json({ error: 'Internalization does not accept uploads' });
                const row = stage.rows.find(entry => entry.storageKey === storageKey);
                if (!row) return res.status(400).json({ error: 'Unexpected transition row' });
                const declaredLength = Number(req.headers['content-length']);
                if (!Number.isSafeInteger(declaredLength) || declaredLength !== row.size) {
                    return res.status(400).json({ error: 'Transition row length mismatch' });
                }
                temporaryPath = `${pluginTransitionStageRowPath(transitionId, row.index)}.${nodeCrypto.randomUUID()}.tmp`;
                let received = 0;
                const digest = nodeCrypto.createHash('sha256');
                const meter = new Transform({
                    transform(chunk, _encoding, callback) {
                        received += chunk.length;
                        if (received > row.size || received > PLUGIN_TRANSITION_MAX_ROW_BYTES) {
                            return callback(new Error('Transition row exceeded its declared size'));
                        }
                        digest.update(chunk);
                        callback(null, chunk);
                    },
                });
                await pipeline(req, meter, createWriteStream(temporaryPath, { flags: 'wx', mode: 0o600 }));
                if (received !== row.size) throw new Error('Transition row length mismatch');
                const fileDescriptor = openSync(temporaryPath, 'r');
                try {
                    fsyncSync(fileDescriptor);
                } finally {
                    closeSync(fileDescriptor);
                }
                const hash = digest.digest('hex');
                let displaySize = null;
                try {
                    const displayMetadata = await validateJsonSource({
                        filePath: temporaryPath,
                        size: received,
                    }, {
                        shouldAbort: () => req.aborted || res.destroyed,
                    });
                    if (storageKey.startsWith(PLUGIN_SAVE_PREFIX)) {
                        displaySize = pluginStorageViewerDisplaySizeFromMetadata(displayMetadata);
                    }
                } catch {
                    throw new PluginStorageValidationError(storageKey);
                }
                await queueStorageMutation(async () => {
                    const current = readPluginTransitionStage(transitionId);
                    if (!pluginTransitionStageBelongsToRequest(current, req) || current.state === 'aborted') {
                        return res.status(404).json({ error: 'Transition not found' });
                    }
                    const currentRow = current.rows.find(entry => entry.storageKey === storageKey);
                    if (hash !== currentRow.sha256) {
                        return res.status(409).json({
                            error: 'Transition row does not match the authoritative source',
                            code: 'PLUGIN_STORAGE_CHANGED',
                        });
                    }
                    if (currentRow.uploaded) {
                        if (currentRow.sha256 !== hash || currentRow.size !== received) {
                            return res.status(409).json({ error: 'Conflicting transition row retry' });
                        }
                        try { unlinkSync(temporaryPath); } catch {}
                        temporaryPath = null;
                        return res.json(pluginTransitionStageResponse(current));
                    }
                    renameSync(temporaryPath, pluginTransitionStageRowPath(transitionId, currentRow.index));
                    fsyncPluginTransitionStageDirectory();
                    temporaryPath = null;
                    currentRow.uploaded = true;
                    currentRow.stagedSha256 = hash;
                    currentRow.displaySize = displaySize;
                    current.state = current.rows.every(entry => entry.uploaded) ? 'ready' : 'uploading';
                    current.updatedAt = Date.now();
                    writePluginTransitionStage(current);
                    res.json(pluginTransitionStageResponse(current));
                });
            } catch (error) {
                next(error);
            } finally {
                try { if (temporaryPath) unlinkSync(temporaryPath); } catch {}
            }
        });
        
        app.get('/api/plugin-storage/transition/stage/row', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            if (!checkActiveSession(req, res)) return;
            let download = null;
            try {
                const transitionId = req.headers['x-plugin-storage-transition'];
                const storageKey = req.headers['x-plugin-storage-key'];
                if (typeof transitionId !== 'string' || typeof storageKey !== 'string') {
                    return res.status(400).json({ error: 'Transition id and storage key are required' });
                }
                const stage = readPluginTransitionStage(transitionId);
                const row = stage?.rows?.find(entry => entry.storageKey === storageKey);
                if (!pluginTransitionStageBelongsToRequest(stage, req)
                    || !row?.uploaded
                    || stage.state === 'aborted') return res.status(404).end();
                if (!/^[0-9a-f]{64}$/.test(row.sha256)
                    || row.stagedSha256 !== row.sha256) {
                    return res.status(409).json({ error: 'Staged transition row failed verification' });
                }
                const filePath = pluginTransitionStageRowPath(transitionId, row.index);
                download = await openStageRowDownload(filePath, row.size);
                if (!download) {
                    return res.status(409).json({ error: 'Staged transition row failed verification' });
                }
                res.setHeader('content-type', 'application/octet-stream');
                res.setHeader('content-length', row.size);
                await pipeline(download.stream, res);
            } catch (error) {
                next(error);
            } finally {
                await download?.close().catch(() => {});
            }
        });
        
        app.get('/api/plugin-storage/transition/stage/status', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            if (!checkActiveSession(req, res)) return;
            try {
                const transitionId = req.headers['x-plugin-storage-transition'];
                if (typeof transitionId !== 'string') return res.status(400).json({ error: 'Transition id required' });
                await queueStorageReadAfterImports(async () => {
                    const stage = await refreshPluginTransitionStageState(
                        readPluginTransitionStage(transitionId),
                    );
                    if (!pluginTransitionStageBelongsToRequest(stage, req)) {
                        return res.status(404).json({ error: 'Transition not found' });
                    }
                    res.json(pluginTransitionStageResponse(stage));
                });
            } catch (error) {
                next(error);
            }
        });
        
        app.post('/api/plugin-storage/transition/stage/abort', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            if (!checkActiveSession(req, res)) return;
            try {
                const transitionId = req.headers['x-plugin-storage-transition'];
                if (typeof transitionId !== 'string') return res.status(400).json({ error: 'Transition id required' });
                await queueStorageMutation(async () => {
                    const stage = await refreshPluginTransitionStageState(
                        readPluginTransitionStage(transitionId),
                    );
                    if (!stage) return res.json({ success: true, state: 'aborted', transitionId });
                    if (!pluginTransitionStageBelongsToRequest(stage, req)) {
                        return res.status(404).json({ error: 'Transition not found' });
                    }
                    if (stage.state === 'committed') return res.json(pluginTransitionStageResponse(stage));
                    stage.state = 'aborted';
                    stage.updatedAt = Date.now();
                    writePluginTransitionStage(stage);
                    removePluginTransitionStageRows(stage);
                    res.json(pluginTransitionStageResponse(stage));
                });
            } catch (error) {
                next(error);
            }
        });
        
        app.post('/api/plugin-storage/transition/stage/finalize', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            if (!checkActiveSession(req, res)) return;
            let databaseSpool = null;
            try {
                const transitionId = req.headers['x-plugin-storage-transition'];
                if (typeof transitionId !== 'string') return res.status(400).json({ error: 'Transition id required' });
                await queueStorageMutation(async () => {
                    let stage = await refreshPluginTransitionStageState(
                        readPluginTransitionStage(transitionId),
                    );
                    if (!pluginTransitionStageBelongsToRequest(stage, req)) {
                        return res.status(404).json({ error: 'Transition not found' });
                    }
                    if (stage.state === 'committed') return res.json(pluginTransitionStageResponse(stage));
                    if (stage.state !== 'ready' || stage.rows.some(row => !row.uploaded)) {
                        return res.status(409).json({ error: 'Transition rows are incomplete' });
                    }
                    await flushPendingDb();
                    const rawDatabase = kvGet('database/database.bin');
                    if (!rawDatabase) return res.status(409).json({ error: 'Database not found' });
                    const liveDb = await decodeAuthoritativeDatabase(rawDatabase);
                    const manifestState = readPluginStorageManifestState();
                    try {
                        assertPluginStorageSource(stage.source, liveDb, manifestState);
                    } catch (error) {
                        if (error?.pluginStorageConflict) {
                            return res.status(409).json({ error: error.message, currentEtag: getDbEtag() });
                        }
                        throw error;
                    }
                    const currentEtag = getDbEtag() ?? computeBufferEtag(rawDatabase);
                    if (currentEtag !== stage.sourceEtag) {
                        return res.status(409).json({ error: 'Database changed during transition', currentEtag });
                    }
                    if (stage.targetOptimized) {
                        try {
                            assertInlineTransitionSourceHashes(liveDb, stage.sourceRowHashes);
                        } catch {
                            return res.status(409).json({ error: 'Inline plugin storage changed during transition' });
                        }
                    } else {
                        for (const sourceRow of stage.sourceRowHashes) {
                            let matches = false;
                            try {
                                matches = await pluginTransitionKvRowMatches(
                                    sourceRow.storageKey,
                                    sourceRow,
                                    () => req.aborted || res.destroyed,
                                );
                            } catch {}
                            if (!matches) {
                                return res.status(409).json({ error: 'Plugin row changed during transition' });
                            }
                        }
                    }
                    const sourceKeys = resolveOwnedPluginStorageKeys(liveDb);
                    if (!stage.targetOptimized) await assertInternalTransitionBounds(liveDb, sourceKeys);
                    const targetDb = {
                        ...liveDb,
                        optimizePluginMemory: stage.targetOptimized,
                        [PLUGIN_STORAGE_GENERATION_FIELD]: stage.targetGeneration,
                    };
                    delete targetDb[PLUGIN_STORAGE_FOLDED_MARKER];
                    let pluginStorage = null;
                    if (stage.targetOptimized) {
                        targetDb.pluginCustomStorage = {};
                        delete targetDb.pluginStorageMeta;
                    } else {
                        pluginStorage = {
                            valueRows: stage.rows
                                .filter(row => row.storageKey.startsWith(PLUGIN_SAVE_PREFIX))
                                .map(row => ({
                                    key: row.rawKey,
                                    source: row.index,
                                })),
                            metaRows: stage.rows
                                .filter(row => row.storageKey.startsWith(PLUGIN_SAVE_META_PREFIX))
                                .map(row => ({
                                    key: row.rawKey,
                                    source: row.index,
                                })),
                            rowSource: index => {
                                const row = stage.rows[index];
                                return {
                                    filePath: pluginTransitionStageRowPath(transitionId, index),
                                    size: row.size,
                                };
                            },
                        };
                    }
                    const spoolPath = path.join(
                        getDatabaseSpoolDir(),
                        `${DATABASE_SPOOL_FILE_PREFIX}${process.pid}-${nodeCrypto.randomUUID()}.transition`,
                    );
                    databaseSpool = await streamRisuSaveToFile({
                        dbObj: targetDb,
                        filePath: spoolPath,
                        readChatRow: async () => null,
                        foldChatRows: false,
                        pluginStorage,
                    });
                    const resultEtag = await computeFileEtag(databaseSpool.filePath);
                    const targetManifest = pluginTransitionDesiredManifest(stage);
                    const targetKeys = new Set([
                        ...(targetManifest?.valueKeys ?? []),
                        ...(targetManifest?.metaKeys ?? []),
                    ]);
                    const quotaChanges = new Map(
                        sourceKeys.valueKeys.map(key => [key, { key, size: null }]),
                    );
                    if (stage.targetOptimized) {
                        for (const row of stage.rows) {
                            if (row.storageKey.startsWith(PLUGIN_SAVE_PREFIX)) {
                                quotaChanges.set(row.storageKey, { key: row.storageKey, size: row.size });
                            }
                        }
                    }
                    const recoverySnapshotToken = newPluginRecoverySnapshotToken();
                    for (const row of stage.rows) {
                        const filePath = pluginTransitionStageRowPath(transitionId, row.index);
                        const stat = await fs.stat(filePath);
                        if (!stat.isFile()
                            || stat.size !== row.size
                            || await computeFileSha256(filePath) !== row.sha256) {
                            return res.status(409).json({ error: 'Staged transition row changed' });
                        }
                    }
                    withPluginStorageQuotaPlan([...quotaChanges.values()], () => {
                        if (stage.targetOptimized) {
                            for (const row of stage.rows) {
                                kvSetFromFile(
                                    row.storageKey,
                                    pluginTransitionStageRowPath(transitionId, row.index),
                                    row.storageKey.startsWith(PLUGIN_SAVE_PREFIX)
                                        ? { pluginStorageDisplaySize: row.displaySize }
                                        : {},
                                );
                                maybeFailPluginStorageTransaction(req, 'after-row');
                            }
                        }
                        for (const storageKey of [...sourceKeys.valueKeys, ...sourceKeys.metaKeys]) {
                            if (targetKeys.has(storageKey)) continue;
                            kvDel(storageKey);
                            maybeFailPluginStorageTransaction(req, 'after-row');
                        }
                        if (targetManifest) writePluginStorageManifest(targetManifest);
                        else kvDel(PLUGIN_STORAGE_MANIFEST_KEY);
                        maybeFailPluginStorageTransaction(req, 'after-manifest');
                        kvSetFromFile('database/database.bin', databaseSpool.filePath);
                        maybeFailPluginStorageTransaction(req, 'after-database');
                        markPluginRecoverySnapshotDirty(recoverySnapshotToken);
                    });
                    publishPluginStorageTransitionDbState(req, targetDb, resultEtag);
                    schedulePluginRecoverySnapshot();
                    // Exercise the hardest acknowledgement window: publication is
                    // durable but the private receipt still says ready. Status (or
                    // startup reconciliation after a crash) must infer the commit
                    // from the authoritative mode/generation/manifest tuple.
                    if (req.headers['x-plugin-storage-failpoint'] === 'acknowledgement-loss') {
                        res.socket?.destroy();
                        return;
                    }
                    stage.state = 'committed';
                    stage.resultEtag = resultEtag;
                    stage.updatedAt = Date.now();
                    writePluginTransitionStage(stage);
                    removePluginTransitionStageRows(stage);
                    res.json(pluginTransitionStageResponse(stage));
                });
            } catch (error) {
                if (isImportInProgressError(error)) return sendImportBusy(res);
                next(error);
            } finally {
                if (databaseSpool) await fs.unlink(databaseSpool.filePath).catch(() => {});
            }
        });
        
        class PluginStorageTransitionRequestError extends Error {
            constructor(status, message, code = 'PLUGIN_STORAGE_CHANGED') {
                super(message);
                this.name = 'PluginStorageTransitionRequestError';
                this.status = status;
                this.code = code;
            }
        }
        
        /**
         * Commit a ready private stage without owning the HTTP response. The bulk
         * transition route and the staged finalize route share the same publication
         * invariants; this helper lets the bulk route complete in its one request.
         */
        async function commitReadyPluginStorageTransition(stage, req) {
            let databaseSpool = null;
            try {
                await flushPendingDb();
                const rawDatabase = kvGet('database/database.bin');
                if (!rawDatabase) {
                    throw new PluginStorageTransitionRequestError(409, 'Database not found');
                }
                const liveDb = await decodeAuthoritativeDatabase(rawDatabase);
                const manifestState = readPluginStorageManifestState();
                try {
                    assertPluginStorageSource(stage.source, liveDb, manifestState);
                } catch (error) {
                    if (error?.pluginStorageConflict) {
                        throw new PluginStorageTransitionRequestError(409, error.message);
                    }
                    throw error;
                }
                const currentEtag = ensurePluginStorageTransitionDbEtag(rawDatabase);
                if (currentEtag !== stage.sourceEtag) {
                    throw new PluginStorageTransitionRequestError(
                        409,
                        'Database changed during transition',
                    );
                }
                if (stage.targetOptimized) {
                    if (stage.sourceKind !== 'client-inline-snapshot') {
                        try {
                            assertInlineTransitionSourceHashes(liveDb, stage.sourceRowHashes);
                        } catch {
                            throw new PluginStorageTransitionRequestError(
                                409,
                                'Inline plugin storage changed during transition',
                            );
                        }
                    }
                } else {
                    for (const sourceRow of stage.sourceRowHashes) {
                        let matches = false;
                        try {
                            matches = await pluginTransitionKvRowMatches(
                                sourceRow.storageKey,
                                sourceRow,
                                () => req.aborted,
                            );
                        } catch {}
                        if (!matches) {
                            throw new PluginStorageTransitionRequestError(
                                409,
                                'Plugin row changed during transition',
                            );
                        }
                    }
                }
        
                const sourceKeys = resolveOwnedPluginStorageKeys(liveDb);
                if (!stage.targetOptimized) await assertInternalTransitionBounds(liveDb, sourceKeys);
                const targetDb = {
                    ...liveDb,
                    optimizePluginMemory: stage.targetOptimized,
                    [PLUGIN_STORAGE_GENERATION_FIELD]: stage.targetGeneration,
                };
                delete targetDb[PLUGIN_STORAGE_FOLDED_MARKER];
                let pluginStorage = null;
                if (stage.targetOptimized) {
                    targetDb.pluginCustomStorage = {};
                    delete targetDb.pluginStorageMeta;
                } else {
                    pluginStorage = {
                        valueRows: stage.rows
                            .filter(row => row.storageKey.startsWith(PLUGIN_SAVE_PREFIX))
                            .map(row => ({ key: row.rawKey, source: row.index })),
                        metaRows: stage.rows
                            .filter(row => row.storageKey.startsWith(PLUGIN_SAVE_META_PREFIX))
                            .map(row => ({ key: row.rawKey, source: row.index })),
                        rowSource: index => {
                            const row = stage.rows[index];
                            return {
                                filePath: pluginTransitionStageRowPath(stage.transitionId, index),
                                size: row.size,
                            };
                        },
                    };
                }
        
                const spoolPath = path.join(
                    getDatabaseSpoolDir(),
                    `${DATABASE_SPOOL_FILE_PREFIX}${process.pid}-${nodeCrypto.randomUUID()}.transition`,
                );
                databaseSpool = await streamRisuSaveToFile({
                    dbObj: targetDb,
                    filePath: spoolPath,
                    readChatRow: async () => null,
                    foldChatRows: false,
                    pluginStorage,
                });
                const resultEtag = await computeFileEtag(databaseSpool.filePath);
                const targetManifest = pluginTransitionDesiredManifest(stage);
                const targetKeys = new Set([
                    ...(targetManifest?.valueKeys ?? []),
                    ...(targetManifest?.metaKeys ?? []),
                ]);
                const quotaChanges = new Map(
                    sourceKeys.valueKeys.map(key => [key, { key, size: null }]),
                );
                if (stage.targetOptimized) {
                    for (const row of stage.rows) {
                        if (row.storageKey.startsWith(PLUGIN_SAVE_PREFIX)) {
                            quotaChanges.set(row.storageKey, {
                                key: row.storageKey,
                                size: row.size,
                            });
                        }
                    }
                }
                const recoverySnapshotToken = newPluginRecoverySnapshotToken();
                for (const row of stage.rows) {
                    const filePath = pluginTransitionStageRowPath(stage.transitionId, row.index);
                    const stat = await fs.stat(filePath);
                    if (!stat.isFile()
                        || stat.size !== row.size
                        || await computeFileSha256(filePath) !== row.sha256) {
                        throw new PluginStorageTransitionRequestError(
                            409,
                            'Staged transition row changed',
                        );
                    }
                }
                withPluginStorageQuotaPlan([...quotaChanges.values()], () => {
                    if (stage.targetOptimized) {
                        for (const row of stage.rows) {
                            kvSetFromFile(
                                row.storageKey,
                                pluginTransitionStageRowPath(stage.transitionId, row.index),
                                row.storageKey.startsWith(PLUGIN_SAVE_PREFIX)
                                    ? { pluginStorageDisplaySize: row.displaySize }
                                    : {},
                            );
                            maybeFailPluginStorageTransaction(req, 'after-row');
                        }
                    }
                    for (const storageKey of [...sourceKeys.valueKeys, ...sourceKeys.metaKeys]) {
                        if (targetKeys.has(storageKey)) continue;
                        kvDel(storageKey);
                        maybeFailPluginStorageTransaction(req, 'after-row');
                    }
                    if (targetManifest) writePluginStorageManifest(targetManifest);
                    else kvDel(PLUGIN_STORAGE_MANIFEST_KEY);
                    maybeFailPluginStorageTransaction(req, 'after-manifest');
                    kvSetFromFile('database/database.bin', databaseSpool.filePath);
                    maybeFailPluginStorageTransaction(req, 'after-database');
                    markPluginRecoverySnapshotDirty(recoverySnapshotToken);
                });
                publishPluginStorageTransitionDbState(req, targetDb, resultEtag);
                schedulePluginRecoverySnapshot();
                stage.state = 'committed';
                stage.resultEtag = resultEtag;
                stage.updatedAt = Date.now();
                writePluginTransitionStage(stage);
                removePluginTransitionStageRows(stage);
                return {
                    ...pluginTransitionStageResponse(stage),
                    values: stage.rows.filter(row => row.storageKey.startsWith(PLUGIN_SAVE_PREFIX)).length,
                    meta: stage.rows.filter(row => row.storageKey.startsWith(PLUGIN_SAVE_META_PREFIX)).length,
                };
            } finally {
                if (databaseSpool) await fs.unlink(databaseSpool.filePath).catch(() => {});
            }
        }
        
        function writeDurablePluginTransitionRowBuffer(transitionId, index, bytes) {
            const filePath = pluginTransitionStageRowPath(transitionId, index);
            const temporaryPath = `${filePath}.${nodeCrypto.randomUUID()}.tmp`;
            try {
                writeFileSync(temporaryPath, bytes, { flag: 'wx', mode: 0o600 });
                const fileDescriptor = openSync(temporaryPath, 'r');
                try {
                    fsyncSync(fileDescriptor);
                } finally {
                    closeSync(fileDescriptor);
                }
                renameSync(temporaryPath, filePath);
                fsyncPluginTransitionStageDirectory();
            } catch (error) {
                try { unlinkSync(temporaryPath); } catch {}
                throw error;
            }
        }
        
        async function receiveBulkPluginStorageTransition(req) {
            if (!ensureDatabaseSpoolDirSync()) {
                const error = new PluginStorageTransitionRequestError(
                    503,
                    'The server transition spool is unavailable; check the save volume permissions.',
                    'PLUGIN_STORAGE_SPOOL_UNAVAILABLE',
                );
                error.retryable = true;
                throw error;
            }
            const declaredText = Array.isArray(req.headers['x-plugin-storage-transition-length'])
                ? req.headers['x-plugin-storage-transition-length'][0]
                : req.headers['x-plugin-storage-transition-length'];
            const declaredLength = typeof declaredText === 'string' ? Number(declaredText) : NaN;
            const contentLength = Number(req.headers['content-length']);
            const maximumLength = PLUGIN_STORAGE_TRANSITION_STREAM_PREFIX_BYTES
                + PLUGIN_STORAGE_TRANSITION_STREAM_MAX_METADATA_BYTES
                + PLUGIN_STORAGE_TRANSITION_STREAM_MAX_PAYLOAD_BYTES;
            if (!Number.isSafeInteger(declaredLength) || declaredLength <= 0
                || declaredLength > maximumLength
                || (Number.isSafeInteger(contentLength) && contentLength !== declaredLength)) {
                throw new PluginStorageTransitionRequestError(
                    declaredLength > maximumLength ? 413 : 400,
                    'Bulk plugin transition requires an exact bounded length.',
                    'PLUGIN_STORAGE_SIZE_LIMIT',
                );
            }
        
            const reader = createPluginStorageBatchRequestReader(req);
            const prefix = await reader.readBuffer(PLUGIN_STORAGE_TRANSITION_STREAM_PREFIX_BYTES);
            if (!prefix.subarray(0, PLUGIN_STORAGE_TRANSITION_STREAM_MAGIC.length)
                .equals(PLUGIN_STORAGE_TRANSITION_STREAM_MAGIC)) {
                throw new PluginStorageTransitionRequestError(400, 'Invalid bulk transition magic header');
            }
            const metadataLength = prefix.readUInt32BE(PLUGIN_STORAGE_TRANSITION_STREAM_MAGIC.length);
            if (metadataLength < 1
                || metadataLength > PLUGIN_STORAGE_TRANSITION_STREAM_MAX_METADATA_BYTES) {
                throw new PluginStorageTransitionRequestError(
                    413,
                    'Bulk transition metadata exceeds its limit.',
                    'PLUGIN_STORAGE_SIZE_LIMIT',
                );
            }
            const metadataBytes = await reader.readBuffer(metadataLength);
            const metadataText = metadataBytes.toString('utf-8');
            if (!Buffer.from(metadataText, 'utf-8').equals(metadataBytes)) {
                throw new PluginStorageTransitionRequestError(400, 'Bulk transition metadata must be UTF-8 JSON');
            }
            let metadata;
            try {
                metadata = JSON.parse(metadataText);
            } catch {
                throw new PluginStorageTransitionRequestError(
                    400,
                    'Bulk transition metadata must be valid JSON',
                );
            }
            if (!Buffer.from(JSON.stringify(metadata), 'utf-8').equals(metadataBytes)) {
                throw new PluginStorageTransitionRequestError(400, 'Bulk transition metadata must be canonical JSON');
            }
            const allowedKeys = new Set([
                'version',
                'transitionId',
                'source',
                'targetOptimized',
                'targetGeneration',
                'expectedEtag',
                'autoConvert',
                'rows',
            ]);
            if (!metadata || typeof metadata !== 'object' || Array.isArray(metadata)
                || Object.keys(metadata).some(key => !allowedKeys.has(key))
                || metadata.version !== 1
                || !PLUGIN_STORAGE_UUID_PATTERN.test(metadata.transitionId)
                || !PLUGIN_STORAGE_UUID_PATTERN.test(metadata.targetGeneration)
                || typeof metadata.targetOptimized !== 'boolean'
                || typeof metadata.autoConvert !== 'boolean'
                || (metadata.expectedEtag !== undefined
                    && (typeof metadata.expectedEtag !== 'string'
                        || !/^[0-9a-f]{32}$/.test(metadata.expectedEtag)))
                || !Array.isArray(metadata.rows)
                || metadata.rows.length > PLUGIN_STORAGE_TRANSITION_STREAM_MAX_ENTRIES
                || (!metadata.targetOptimized && metadata.rows.length !== 0)) {
                throw new PluginStorageTransitionRequestError(400, 'Invalid bulk plugin transition metadata');
            }
            const parsedManifest = metadata.source?.manifest === null
                ? null
                : parsePluginStorageManifest(metadata.source?.manifest);
            if (!metadata.source || typeof metadata.source !== 'object'
                || Array.isArray(metadata.source)
                || typeof metadata.source.optimized !== 'boolean'
                || !(metadata.source.generation === null
                    || typeof metadata.source.generation === 'string')
                || (metadata.source.manifest !== null && parsedManifest === null)
                || metadata.targetOptimized === metadata.source.optimized) {
                throw new PluginStorageTransitionRequestError(400, 'Invalid bulk transition source');
            }
            if (readPluginTransitionStage(metadata.transitionId)) {
                throw new PluginStorageTransitionRequestError(
                    409,
                    'Transition id is already active',
                );
            }
        
            let payloadBytes = 0;
            const descriptors = metadata.rows.map((row, index) => {
                if (!row || typeof row !== 'object' || Array.isArray(row)
                    || Object.keys(row).length !== 4
                    || typeof row.rawKey !== 'string'
                    || typeof row.storageKey !== 'string'
                    || !Number.isSafeInteger(row.valueLength)
                    || row.valueLength < 1
                    || row.valueLength > PLUGIN_TRANSITION_MAX_ROW_BYTES
                    || typeof row.valueHash !== 'string'
                    || !/^[0-9a-f]{64}$/.test(row.valueHash)) {
                    throw new PluginStorageTransitionRequestError(400, `Invalid bulk transition row ${index}`);
                }
                const prefixForRow = row.storageKey.startsWith(PLUGIN_SAVE_META_PREFIX)
                    ? PLUGIN_SAVE_META_PREFIX
                    : row.storageKey.startsWith(PLUGIN_SAVE_PREFIX)
                        ? PLUGIN_SAVE_PREFIX
                        : null;
                if (!prefixForRow
                    || encodePluginSaveStorageKey(row.rawKey, prefixForRow) !== row.storageKey) {
                    throw new PluginStorageTransitionRequestError(400, `Invalid bulk transition key ${index}`);
                }
                payloadBytes += row.valueLength;
                if (!Number.isSafeInteger(payloadBytes)
                    || payloadBytes > PLUGIN_STORAGE_TRANSITION_STREAM_MAX_PAYLOAD_BYTES) {
                    throw new PluginStorageTransitionRequestError(
                        413,
                        'Bulk transition payload exceeds its limit.',
                        'PLUGIN_STORAGE_TOTAL_TOO_LARGE',
                    );
                }
                return { ...row, index, prefix: prefixForRow };
            });
            if (new Set(descriptors.map(row => row.storageKey)).size !== descriptors.length) {
                throw new PluginStorageTransitionRequestError(400, 'Bulk transition keys must be unique');
            }
            const expectedLength = PLUGIN_STORAGE_TRANSITION_STREAM_PREFIX_BYTES
                + metadataLength
                + payloadBytes;
            if (expectedLength !== declaredLength) {
                throw new PluginStorageTransitionRequestError(400, 'Bulk transition length does not match metadata');
            }
        
            const rows = [];
            try {
                for (const descriptor of descriptors) {
                    const encoded = await reader.readBuffer(descriptor.valueLength);
                    if (sha256Hex(encoded) !== descriptor.valueHash) {
                        throw new PluginStorageTransitionRequestError(400, 'Bulk transition row failed its hash check');
                    }
                    let richValue;
                    try {
                        richValue = richPluginTransitionUnpackr.decode(encoded);
                    } catch {
                        throw new PluginStorageTransitionRequestError(
                            400,
                            'Bulk transition row is not valid structured-clone MessagePack.',
                            'PLUGIN_STORAGE_VALUE_UNSUPPORTED',
                        );
                    }
                    let jsonBytes;
                    let jsonValue = richValue;
                    try {
                        jsonBytes = serializePluginStorageRow(descriptor.storageKey, jsonValue);
                    } catch (strictError) {
                        if (!metadata.autoConvert || descriptor.prefix === PLUGIN_SAVE_META_PREFIX) {
                            throw strictError;
                        }
                        try {
                            jsonValue = convertCompatiblePluginStorageJson(richValue);
                            try {
                                jsonBytes = serializePluginStorageRow(descriptor.storageKey, jsonValue);
                            } catch {
                                jsonBytes = serializeLosslessPluginStorageRow(
                                    descriptor.storageKey,
                                    jsonValue,
                                );
                            }
                        } catch {
                            throw strictError;
                        }
                    }
                    if (jsonBytes.length > PLUGIN_TRANSITION_MAX_ROW_BYTES) {
                        throw new PluginStorageLimitError(
                            `Plugin transition row exceeds the ${PLUGIN_TRANSITION_MAX_ROW_BYTES}-byte transition limit.`,
                            {
                                code: 'PLUGIN_VALUE_TOO_LARGE',
                                limit: PLUGIN_TRANSITION_MAX_ROW_BYTES,
                                actual: jsonBytes.length,
                            },
                        );
                    }
                    writeDurablePluginTransitionRowBuffer(
                        metadata.transitionId,
                        descriptor.index,
                        jsonBytes,
                    );
                    const stagedSha256 = sha256Hex(jsonBytes);
                    rows.push({
                        index: descriptor.index,
                        storageKey: descriptor.storageKey,
                        rawKey: descriptor.rawKey,
                        size: jsonBytes.length,
                        sha256: stagedSha256,
                        stagedSha256,
                        displaySize: descriptor.prefix === PLUGIN_SAVE_PREFIX
                            ? pluginStorageViewerDisplaySize(jsonValue)
                            : null,
                        uploaded: true,
                    });
                }
                await reader.assertEnd();
                return {
                    metadata: {
                        ...metadata,
                        source: {
                            ...metadata.source,
                            manifest: parsedManifest,
                        },
                    },
                    metadataBytes,
                    rows,
                };
            } catch (error) {
                removePluginTransitionStage({ transitionId: metadata.transitionId, rows });
                if (isPluginStorageValidationError(error)) {
                    throw new PluginStorageTransitionRequestError(
                        400,
                        'Some existing plugin data cannot be moved into optimized storage because it is not JSON-compatible. Turn optimization off and update or reset the affected plugin, or ask its developer to store only null, booleans, finite numbers, strings, dense arrays, and plain objects.',
                        'PLUGIN_STORAGE_VALUE_UNSUPPORTED',
                    );
                }
                throw error;
            }
        }
        
        app.post('/api/plugin-storage/transition/bulk', async (req, res, next) => {
            if (!await checkAuth(req, res)) return;
            if (!checkActiveSession(req, res)) return;
            let received;
            let stage = null;
            try {
                received = await receiveBulkPluginStorageTransition(req);
                const plan = received.metadata;
                await queueStorageMutation(async () => {
                    await flushPendingDb();
                    const rawDatabase = kvGet('database/database.bin');
                    if (!rawDatabase) {
                        throw new PluginStorageTransitionRequestError(409, 'Database not found');
                    }
                    const currentEtag = ensurePluginStorageTransitionDbEtag(rawDatabase);
                    const liveDb = await decodeAuthoritativeDatabase(rawDatabase);
                    const manifestState = readPluginStorageManifestState();
                    try {
                        assertPluginStorageSource(plan.source, liveDb, manifestState);
                    } catch (error) {
                        if (error?.pluginStorageConflict) {
                            throw new PluginStorageTransitionRequestError(409, error.message);
                        }
                        throw error;
                    }
                    if (plan.expectedEtag && plan.expectedEtag !== currentEtag) {
                        throw new PluginStorageTransitionRequestError(409, 'ETag mismatch');
                    }
                    if (plan.targetGeneration === pluginStorageGeneration(liveDb)) {
                        throw new PluginStorageTransitionRequestError(
                            400,
                            'Transition target must use a fresh generation',
                        );
                    }
                    const existing = await refreshPluginTransitionStageState(
                        readPluginTransitionStage(plan.transitionId),
                    );
                    if (existing) {
                        if (!pluginTransitionStageBelongsToRequest(existing, req)) {
                            throw new PluginStorageTransitionRequestError(404, 'Transition not found');
                        }
                        if (existing.state === 'committed') {
                            removePluginTransitionStageRows(existing);
                            return res.json({
                                ...pluginTransitionStageResponse(existing),
                                values: existing.rows.filter(row => row.storageKey.startsWith(PLUGIN_SAVE_PREFIX)).length,
                                meta: existing.rows.filter(row => row.storageKey.startsWith(PLUGIN_SAVE_META_PREFIX)).length,
                            });
                        }
                        throw new PluginStorageTransitionRequestError(409, 'Transition id is already active');
                    }
                    const activeStage = await findActivePluginTransition(req, plan.transitionId);
                    if (activeStage) {
                        throw new PluginStorageTransitionRequestError(
                            409,
                            'Another plugin storage transition is already active',
                        );
                    }
        
                    let rows = received.rows;
                    let sourceRowHashes;
                    if (plan.targetOptimized) {
                        const totalConvertedBytes = rows.reduce((sum, row) => sum + row.size, 0);
                        const required = totalConvertedBytes * 3 + (kvSize('database/database.bin') ?? 0) * 2;
                        if (!Number.isSafeInteger(required)) {
                            throw new PluginStorageLimitError(
                                'Plugin transition disk requirement is too large.',
                                {
                                    code: 'PLUGIN_STORAGE_DISK_LIMIT',
                                    limit: Number.MAX_SAFE_INTEGER,
                                    actual: required,
                                },
                            );
                        }
                        const disk = await checkDiskSpace(required);
                        if (!disk.ok) {
                            throw new PluginStorageLimitError(
                                `Plugin transition requires ${required} free bytes.`,
                                {
                                    code: 'PLUGIN_STORAGE_DISK_LIMIT',
                                    limit: disk.available,
                                    actual: required,
                                },
                            );
                        }
                        sourceRowHashes = rows.map(row => ({
                            storageKey: row.storageKey,
                            size: row.size,
                            sha256: row.sha256,
                            backend: 'bulk',
                        }));
                    } else {
                        const sourceKeys = resolveOwnedPluginStorageKeys(liveDb);
                        await assertInternalTransitionBounds(liveDb, sourceKeys);
                        rows = [];
                        for (const storageKey of [...sourceKeys.valueKeys, ...sourceKeys.metaKeys]) {
                            const index = rows.length;
                            const prefixForRow = storageKey.startsWith(PLUGIN_SAVE_META_PREFIX)
                                ? PLUGIN_SAVE_META_PREFIX
                                : PLUGIN_SAVE_PREFIX;
                            const rawKey = sourceKeys.manifest
                                ? decodeManifestPluginSaveStorageKey(
                                    sourceKeys.manifest,
                                    storageKey,
                                    prefixForRow,
                                )
                                : decodeValidatedPluginStorageKey(storageKey, prefixForRow);
                            const staged = await writeDurablePluginTransitionStageRow(
                                storageKey,
                                pluginTransitionStageRowPath(plan.transitionId, index),
                                () => req.aborted,
                            );
                            rows.push({
                                index,
                                storageKey,
                                rawKey,
                                size: staged.size,
                                sha256: staged.sha256,
                                stagedSha256: staged.sha256,
                                displaySize: staged.displaySize,
                                uploaded: true,
                            });
                        }
                        sourceRowHashes = rows.map(row => ({
                            storageKey: row.storageKey,
                            size: row.size,
                            sha256: row.sha256,
                            backend: 'kv',
                        }));
                    }
                    stage = {
                        version: 1,
                        transitionId: plan.transitionId,
                        sessionId: typeof req.headers['x-session-id'] === 'string'
                            ? req.headers['x-session-id']
                            : null,
                        requestHash: sha256Hex(received.metadataBytes),
                        source: plan.source,
                        sourceEtag: currentEtag,
                        sourceKind: plan.targetOptimized ? 'client-inline-snapshot' : 'server-optimized',
                        sourceRowHashes,
                        targetOptimized: plan.targetOptimized,
                        targetGeneration: plan.targetGeneration,
                        rows,
                        state: 'ready',
                        createdAt: Date.now(),
                        updatedAt: Date.now(),
                    };
                    writePluginTransitionStage(stage);
                    const result = await commitReadyPluginStorageTransition(stage, req);
                    if (req.headers['x-plugin-storage-failpoint'] === 'acknowledgement-loss') {
                        res.socket?.destroy();
                        return;
                    }
                    res.json(result);
                });
            } catch (error) {
                if (stage?.state !== 'committed') {
                    removePluginTransitionStage(stage ?? (
                        received?.metadata?.transitionId
                            ? { transitionId: received.metadata.transitionId, rows: received.rows }
                            : null
                    ));
                } else removePluginTransitionStageRows(stage);
                if (isImportInProgressError(error)) return sendImportBusy(res);
                if (error instanceof PluginStorageTransitionRequestError) {
                    return res.status(error.status).json({
                        success: false,
                        outcome: 'not-committed',
                        operation: 'transition',
                        error: error.message,
                        code: error.code,
                        retryable: error.retryable === true,
                    });
                }
                if (error instanceof PluginStorageLimitError) {
                    return res.status(error.status || 413).json({
                        success: false,
                        outcome: 'not-committed',
                        operation: 'transition',
                        error: error.message,
                        code: error.code,
                        limit: error.limit,
                        actual: error.actual,
                        retryable: false,
                    });
                }
                next(error);
            }
        });
        
        app.post('/api/plugin-storage/transition', async (req, res) => {
            if (!await checkAuth(req, res)) return;
            return sendClientUpgradeRequired(
                res,
                req[BUFFERED_INGRESS_POLICY] ?? { responseKind: 'generic' },
                expectedClientBuild,
                'This plugin storage transition protocol is retired. Reload to continue.',
            );
        });
    }

    // PLUGIN_STORAGE_ROUTE_FAMILY_BODY

    return {
        registerPluginStorageClearRoute,
        registerPluginStorageStateRoutes,
        registerPluginStorageManagementRoutes,
        registerPluginStorageBatchRoute,
        registerPluginStorageMutationRoute,
        registerPluginStorageTransitionRoutes,
    };
}

function routes(app, ctx) {
    let family = pluginStorageRouteFamilies.get(app);
    if (!family) {
        family = createPluginStorageRouteFamily(ctx);
        pluginStorageRouteFamilies.set(app, family);
    }
    return family;
}

function registerPluginStorageClearRoute(app, ctx) {
    routes(app, ctx).registerPluginStorageClearRoute(app);
}

function registerPluginStorageStateRoutes(app, ctx) {
    routes(app, ctx).registerPluginStorageStateRoutes(app);
}

function registerPluginStorageManagementRoutes(app, ctx) {
    routes(app, ctx).registerPluginStorageManagementRoutes(app);
}

function registerPluginStorageBatchRoute(app, ctx) {
    routes(app, ctx).registerPluginStorageBatchRoute(app);
}

function registerPluginStorageMutationRoute(app, ctx) {
    routes(app, ctx).registerPluginStorageMutationRoute(app);
}

function registerPluginStorageTransitionRoutes(app, ctx) {
    routes(app, ctx).registerPluginStorageTransitionRoutes(app);
}

module.exports = {
    registerPluginStorageClearRoute,
    registerPluginStorageStateRoutes,
    registerPluginStorageManagementRoutes,
    registerPluginStorageBatchRoute,
    registerPluginStorageMutationRoute,
    registerPluginStorageTransitionRoutes,
};
