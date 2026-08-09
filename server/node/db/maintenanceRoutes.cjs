'use strict';

const path = require('path');
const fs = require('fs/promises');
const {
    kvGet,
    kvSet,
    kvDel,
    kvList,
    kvListWithSizes,
    kvListSelectedWithSizes,
    kvSize,
    kvGetUpdatedAt,
    gcChunks,
    reclaimableChunkBytes,
    isDbBlobChunked,
    snapshotFootprints,
    db: sqliteDb,
} = require('./db.cjs');
const { sumAssetFsBytes } = require('../assets/assetStore.cjs');
const {
    PLUGIN_SAVE_PREFIX,
    PLUGIN_SAVE_META_PREFIX,
    PLUGIN_STORAGE_MANIFEST_KEY,
} = require('../plugin-storage/pluginSaveKeys.cjs');
const { logger } = require('../runtime/logs.cjs');

const ASSET_PREFIXES = ['assets/', 'remotes/', 'inlay/', 'inlay_thumb/', 'inlay_meta/', 'inlay_info/', 'coldstorage/'];
const CHAT_ORPHAN_GRACE_MS = 60 * 60 * 1000;

function registerAssetCleanupRoute(app, ctx) {
    const {
        checkAuth,
        checkActiveSession,
        runServerAssetCleanup,
        isImportInProgressError,
        sendImportBusy,
    } = ctx;

    app.post('/api/assets/cleanup', async (req, res, next) => {
        if (!await checkAuth(req, res)) return;
        if (!checkActiveSession(req, res)) return;
        try {
            res.json(await runServerAssetCleanup({ source: 'endpoint' }));
        } catch (error) {
            if (isImportInProgressError(error)) return sendImportBusy(res);
            next(error);
        }
    });
}

function registerMaintenanceRoutes(app, ctx) {
    const {
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
    } = ctx;

    function internalSnapshotMetadata(key) {
        const parsed = parseInternalSnapshotKey(key);
        if (!parsed) return null;
        let size = null;
        try {
            size = kvSize(key);
        } catch (error) {
            // Discovery must retain an exact corrupt candidate so boot recovery can
            // submit it to the definitive restore boundary, then try an older key.
            if (error?.code !== 'KV_CHUNK_CORRUPT') throw error;
        }
        if (size !== null && (!Number.isSafeInteger(size) || size < 0)) return null;
        return { ...parsed, size };
    }

    function listInternalSnapshotMetadata() {
        const keys = kvList(DB_BACKUP_PREFIX);
        try {
            const sizes = new Map(
                kvListWithSizes(DB_BACKUP_PREFIX).map((entry) => [entry.key, entry.size]),
            );
            return keys.map((key) => {
                const parsed = parseInternalSnapshotKey(key);
                if (!parsed) return null;
                const size = sizes.get(key);
                return Number.isSafeInteger(size) && size >= 0 ? { ...parsed, size } : null;
            });
        } catch (error) {
            if (error?.code !== 'KV_CHUNK_CORRUPT') throw error;
            // Discovery must still retain each corrupt candidate with size:null so
            // boot recovery can try it and continue to an older snapshot. The rare
            // damaged-state path deliberately falls back to the per-key verifier.
            return keys.map(internalSnapshotMetadata);
        }
    }

    function statsBasename(s) {
        if (!s) return '';
        return String(s).replace(/\\/g, '/').split('/').pop();
    }

    // Storage statistics and the destructive collector share one reachability
    // implementation, including inline and active-generation optimized plugin data.
    function buildReachableAssetBasenameSet(dbObj) {
        const assetEntries = listAssetEntriesWithSizes();
        return new Set([...collectDatabaseAssetReferences(dbObj, assetEntries)]
            .map((key) => statsBasename(key)));
    }

    function statSafe(p) {
        try { return require('fs').statSync(p); } catch { return null; }
    }

    async function diskFreeStat(dirPath) {
        try {
            const sf = await fs.statfs(dirPath);
            return { free: sf.bsize * sf.bavail, total: sf.bsize * sf.blocks };
        } catch { return { free: null, total: null }; }
    }

    // Sum the on-disk inlay payload (image files + sidecar JSONs in save/inlays).
    // Returns 0 if the directory is missing. Used by both the backup-size
    // estimator and the dashboard inlay total — kv inlay/* prefixes don't
    // reflect filesystem bytes after the inlay→fs migration.
    async function sumInlayFsBytes() {
        let total = 0;
        try {
            const files = await listRegularFilesRecursive(inlayDir);
            const sizes = await Promise.all(files.map(async (filePath) => {
                const name = path.basename(filePath);
                if (filePath === inlayMigrationMarker || isInlayTemporaryFileName(name)) return 0;
                try { return (await fs.stat(filePath)).size; } catch { return 0; }
            }));
            total = sizes.reduce((sum, size) => sum + size, 0);
        } catch { /* dir missing */ }
        return total;
    }

    async function sumDirectoryFsBytes(directory) {
        let entries = [];
        try {
            entries = await fs.readdir(directory, { withFileTypes: true });
        } catch {
            return 0;
        }
        const sizes = await Promise.all(entries.map(async (entry) => {
            const entryPath = path.join(directory, entry.name);
            if (entry.isDirectory()) {
                return sumDirectoryFsBytes(entryPath);
            } else if (entry.isFile()) {
                try { return (await fs.stat(entryPath)).size; } catch {}
            }
            return 0;
        }));
        return sizes.reduce((total, size) => total + size, 0);
    }

    const coldStorageBackupSizeMemo = new Map();

    function estimateColdStorageBackupSize() {
        const storedRows = kvListWithSizes('coldstorage/');
        const storedSizeByKey = new Map(storedRows.map((row) => [row.key, row.size]));
        const canonicalKeys = Array.from(new Set(
            storedRows.map((row) => normalizeColdStorageStorageKey(row.key)),
        )).sort((a, b) => a.localeCompare(b));
        const liveCanonicalKeys = new Set(canonicalKeys);
        for (const key of coldStorageBackupSizeMemo.keys()) {
            if (!liveCanonicalKeys.has(key)) coldStorageBackupSizeMemo.delete(key);
        }

        let size = 0;
        let recomputed = 0;
        for (const canonicalKey of canonicalKeys) {
            const legacyKey = `${canonicalKey}.json`;
            const storageKey = storedSizeByKey.has(canonicalKey) ? canonicalKey : legacyKey;
            const storedSize = storedSizeByKey.get(storageKey);
            const updatedAt = kvGetUpdatedAt(storageKey);
            const signalIsUsable = Number.isSafeInteger(updatedAt)
                && updatedAt >= 0
                && Number.isSafeInteger(storedSize)
                && storedSize >= 0;
            const memo = signalIsUsable ? coldStorageBackupSizeMemo.get(canonicalKey) : null;
            if (memo
                && memo.storageKey === storageKey
                && memo.updatedAt === updatedAt
                && memo.storedSize === storedSize) {
                size += memo.outputSize;
                continue;
            }

            const entry = readColdStorageJsonEntry(canonicalKey, {
                migrateLegacy: true,
                allowPlainJsonFallback: true,
            });
            if (!entry) {
                throw new Error(`[ColdStorage] missing cold storage entry while exporting: ${canonicalKey}`);
            }
            // Backup output is the re-stringified JSON, not the stored gzip payload.
            const outputSize = Buffer.from(JSON.stringify(entry.coldData), 'utf-8').length;
            size += outputSize;
            recomputed++;

            let finalStorageKey = storageKey;
            let finalStoredSize = storedSize;
            let finalUpdatedAt = updatedAt;
            if (entry.storageKey !== entry.canonicalKey || entry.format !== 'gzip') {
                finalStorageKey = entry.canonicalKey;
                finalStoredSize = kvSize(finalStorageKey);
                finalUpdatedAt = kvGetUpdatedAt(finalStorageKey);
            }
            if (Number.isSafeInteger(finalUpdatedAt)
                && finalUpdatedAt >= 0
                && Number.isSafeInteger(finalStoredSize)
                && finalStoredSize >= 0) {
                coldStorageBackupSizeMemo.set(canonicalKey, {
                    storageKey: finalStorageKey,
                    updatedAt: finalUpdatedAt,
                    storedSize: finalStoredSize,
                    outputSize,
                });
            } else {
                coldStorageBackupSizeMemo.delete(canonicalKey);
            }
        }
        return { size, recomputed };
    }

    // Estimated server-backup size — mirrors the enumeration in
    // /api/backup/server/save without writing anything. Inlay files live on the
    // filesystem (post-migration), so we have to fs.stat them rather than read
    // kvSize. Cost: ~5-50 ms typical, ~200 ms for users with thousands of inlays.
    async function estimateServerBackupSize(reader = null) {
        let total = 0;
        total += reader
            ? (reader.kvListWithSizes(DB_BLOB_KEY).find((it) => it.key === DB_BLOB_KEY)?.size ?? 0)
            : (kvSize(DB_BLOB_KEY) || 0);
        // Server backups carry plugin values as individual archive entries. Count
        // their raw payload sizes without reading or decoding them.
        const sizeReader = reader || { kvListWithSizes };
        for (const it of sizeReader.kvListWithSizes(PLUGIN_SAVE_PREFIX)) total += it.size;
        for (const it of sizeReader.kvListWithSizes(PLUGIN_SAVE_META_PREFIX)) total += it.size;
        for (const it of listDraftBackupEntries(sizeReader)) total += it.size;
        for (const it of listAssetEntriesWithSizes(sizeReader)) total += it.size;
        for (const it of sizeReader.kvListWithSizes('inlay_meta/')) total += it.size;
        let coldRowsRecomputed;
        if (reader) {
            const coldEntries = listColdStorageBackupEntries({ reader, migrateLegacy: false });
            for (const entry of coldEntries) total += entry.size;
            coldRowsRecomputed = coldEntries.length;
        } else {
            const coldEstimate = estimateColdStorageBackupSize();
            total += coldEstimate.size;
            coldRowsRecomputed = coldEstimate.recomputed;
        }
        total += await sumInlayFsBytes();
        return { size: total, coldRowsRecomputed };
    }

    // Current snapshot count + two totals:
    //   bytes        — marginal disk cost (snapshotFootprint), the SAME measure the
    //                  byte limit/trim uses, so the limit gauge matches what trimming
    //                  sees. kvListWithSizes would report a chunked snapshot's marker.
    //   logicalBytes — sum of each snapshot's full logical size (kvSize), i.e. what
    //                  the snapshots would cost WITHOUT dedup. Drives the "saved by
    //                  deduplication" figure; never used for trimming.
    function snapshotUsage() {
        const footprints = snapshotFootprints(DB_BACKUP_PREFIX);
        return {
            count: footprints.length,
            bytes: footprints.reduce((sum, entry) => sum + entry.size, 0),
            logicalBytes: footprints.reduce((sum, entry) => sum + entry.logicalSize, 0),
        };
    }

    app.get('/api/db/stats', async (req, res, next) => {
        if (!await checkAuth(req, res)) return;
        try {
            const HUB_HOSTING_MODE = isHubHostingMode();
            const saveDir = path.join(process.cwd(), 'save');
            const dbFilePath = path.join(saveDir, 'risuai.db');
            const walPath = dbFilePath + '-wal';
            const shmPath = dbFilePath + '-shm';

            const files = {
                db: statSafe(dbFilePath)?.size ?? 0,
                wal: statSafe(walPath)?.size ?? 0,
                shm: statSafe(shmPath)?.size ?? 0,
            };

            const disk = HUB_HOSTING_MODE
                ? { free: null, total: null }
                : await diskFreeStat(saveDir);
            // Backup destination disk — same as save/ in the default config but
            // can diverge when the user points backupsDir at a different mount.
            // Surfaced separately so backup-side warnings target the right disk.
            // `sameAsSaveDir` is true when both paths land on the same filesystem
            // (compared by Stat.dev). Dashboard uses this to decide whether to
            // count file backups against the save/ disk in the storage chart.
            let backupDisk;
            if (!HUB_HOSTING_MODE) {
                const bDisk = await diskFreeStat(getBackupsDir());
                let sameAsSaveDir = false;
                try {
                    const saveStat = require('fs').statSync(saveDir);
                    const bStat = require('fs').statSync(getBackupsDir());
                    sameAsSaveDir = saveStat.dev === bStat.dev;
                } catch { /* non-fatal */ }
                backupDisk = { ...bDisk, path: getBackupsDir(), sameAsSaveDir };
            }

            const pageSize = sqliteDb.pragma('page_size', { simple: true });
            const pageCount = sqliteDb.pragma('page_count', { simple: true });
            const freelistCount = sqliteDb.pragma('freelist_count', { simple: true });
            const journalMode = sqliteDb.pragma('journal_mode', { simple: true });
            const synchronous = sqliteDb.pragma('synchronous', { simple: true });
            const autoVacuum = sqliteDb.pragma('auto_vacuum', { simple: true });
            const reclaimable = freelistCount * pageSize;

            const dbBlobSize = kvSize(DB_BLOB_KEY) || 0;

            // Physical storage of the chunked DB blob (and all snapshots, which share
            // chunks). This is where the blob bytes actually live post-chunking — kv
            // holds only a tiny marker, so the chart must count this table separately.
            const chunkStat = sqliteDb.prepare('SELECT COUNT(*) AS c, COALESCE(SUM(LENGTH(data)), 0) AS b FROM chunks').get();
            // Bytes the next gc() would reclaim (true orphans + chunks pinned only by
            // stale/raw-overwritten manifests) — drives the Optimize button.
            const orphanChunkBytes = reclaimableChunkBytes();
            const liveChunked = isDbBlobChunked();

            // Prefix breakdown — split database/ into the live blob vs rotated backups.
            const prefixes = {};
            prefixes[DB_BLOB_KEY] = { totalSize: dbBlobSize, count: dbBlobSize > 0 ? 1 : 0 };
            const backupEntries = kvListWithSizes(DB_BACKUP_PREFIX);
            const backupKeys = backupEntries.map((entry) => entry.key);
            let backupTotal = 0;
            let backupOldest = null, backupNewest = null;
            for (const entry of backupEntries) {
                const k = entry.key;
                backupTotal += entry.size;
                const tsRaw = parseInt(k.slice(DB_BACKUP_PREFIX.length, -4), 10);
                if (Number.isFinite(tsRaw)) {
                    const ts = tsRaw * 100;
                    if (!backupOldest || ts < backupOldest) backupOldest = ts;
                    if (!backupNewest || ts > backupNewest) backupNewest = ts;
                }
            }
            prefixes[DB_BACKUP_PREFIX] = { totalSize: backupTotal, count: backupKeys.length };
            const chatKeys = chatRowStore.listAllChatRowKeys();
            let chatTotal = 0, chatKvRowSize = 0;
            const chatSizes = kvListWithSizes('chats/');
            const chatSizeByKey = new Map(chatSizes.map((entry) => [entry.key, entry.size]));
            for (const key of chatKeys) {
                const metadata = chatRowStore.metadataForKey(key);
                chatTotal += metadata?.logCount > 0
                    ? metadata.contentSize
                    : (chatSizeByKey.get(key) || 0);
            }
            for (const entry of chatSizes) chatKvRowSize += entry.size;
            const chatChunkBytes = sqliteDb.prepare(
                `SELECT COALESCE(SUM(LENGTH(data)), 0) AS b
                 FROM chunks
                 WHERE hash IN (
                     SELECT hash FROM manifest_chunks WHERE manifest_key LIKE 'chats/%'
                 )`
            ).get().b;
            prefixes['chats/'] = {
                totalSize: chatTotal,
                count: chatKeys.length,
                physicalSize: chatKvRowSize + chatChunkBytes,
                kvRowSize: chatKvRowSize,
                chunkBytes: chatChunkBytes,
            };

            // Optimized plugin storage is one physical category made up of value
            // rows, owner sidecars, and the publication manifest. Keep its logical
            // payload total separate from its SQLite footprint: chunked rows leave
            // only a small marker in kv and store their bodies in chunks.
            const pluginStorageEntries = [
                ...kvListWithSizes(PLUGIN_SAVE_PREFIX),
                ...kvListWithSizes(PLUGIN_SAVE_META_PREFIX),
                ...kvListSelectedWithSizes([PLUGIN_STORAGE_MANIFEST_KEY]),
            ];
            const pluginStorageTotalSize = pluginStorageEntries
                .reduce((total, entry) => total + entry.size, 0);
            const pluginStorageKv = sqliteDb.prepare(
                `SELECT COUNT(*) AS c, COALESCE(SUM(LENGTH(value)), 0) AS b
                 FROM kv
                 WHERE key LIKE 'pluginsave/%'
                    OR key LIKE 'pluginsave-meta/%'
                    OR key = @manifestKey`
            ).get({ manifestKey: PLUGIN_STORAGE_MANIFEST_KEY });
            // Attribute a shared chunk to chats first so dashboard categories stay
            // disjoint. Chunks shared with database snapshots but not chats belong
            // to plugin storage; every remaining chunk stays in the database slice.
            const pluginStorageChunkBytes = sqliteDb.prepare(
                `SELECT COALESCE(SUM(LENGTH(data)), 0) AS b
                 FROM chunks
                 WHERE hash IN (
                     SELECT hash FROM manifest_chunks
                     WHERE manifest_key LIKE 'pluginsave/%'
                        OR manifest_key LIKE 'pluginsave-meta/%'
                        OR manifest_key = @manifestKey
                 )
                   AND hash NOT IN (
                     SELECT hash FROM manifest_chunks WHERE manifest_key LIKE 'chats/%'
                 )`
            ).get({ manifestKey: PLUGIN_STORAGE_MANIFEST_KEY }).b;
            const pluginStorage = {
                count: pluginStorageKv.c,
                totalSize: pluginStorageTotalSize,
                kvRowSize: pluginStorageKv.b,
                chunkBytes: pluginStorageChunkBytes,
                physicalSize: pluginStorageKv.b + pluginStorageChunkBytes,
            };
            for (const p of ASSET_PREFIXES) {
                const items = p === 'assets/'
                    ? listAssetEntriesWithSizes()
                    : kvListWithSizes(p);
                let total = 0;
                for (const it of items) total += it.size;
                prefixes[p] = { totalSize: total, count: items.length };
            }

            const kvRows = sqliteDb.prepare('SELECT COUNT(*) AS c FROM kv').get().c;
            const kvTotalBytes = sqliteDb.prepare('SELECT COALESCE(SUM(LENGTH(value)), 0) AS s FROM kv').get().s;

            let fileBackups = { count: 0, totalSize: 0, oldest: null, newest: null };
            if (!HUB_HOSTING_MODE) {
                try {
                    const entries = await fs.readdir(getBackupsDir(), { withFileTypes: true });
                    for (const e of entries) {
                        if (!e.isFile() || !BACKUP_FILENAME_REGEX.test(e.name)) continue;
                        const st = await fs.stat(path.join(getBackupsDir(), e.name));
                        fileBackups.count++;
                        fileBackups.totalSize += st.size;
                        const ts = st.mtimeMs;
                        if (!fileBackups.oldest || ts < fileBackups.oldest) fileBackups.oldest = ts;
                        if (!fileBackups.newest || ts > fileBackups.newest) fileBackups.newest = ts;
                    }
                } catch { /* backups dir may not exist */ }
            }

            // Quick estimates from in-memory cache only — never decode the BLOB just for stats.
            let trashed = { count: 0, expiredCount: 0, available: false };
            let orphan = { count: 0, totalSize: 0, available: false };
            const stripped = getCurrentDatabaseCacheValue(DB_HEX_KEY, { allowDirty: true });
            if (stripped?.characters) {
                const now = Date.now();
                const GRACE = 1000 * 60 * 60 * 24 * 3;
                for (const c of stripped.characters) {
                    if (c?.trashTime) {
                        trashed.count++;
                        if (c.trashTime + GRACE < now) trashed.expiredCount++;
                    }
                }
                trashed.available = true;
            }
            if (stripped) {
                const uncleanable = buildReachableAssetBasenameSet(stripped);
                for (const it of listAssetEntriesWithSizes()) {
                    if (!uncleanable.has(statsBasename(it.key))) {
                        orphan.count++;
                        orphan.totalSize += it.size;
                    }
                }
                orphan.available = true;
            }

            let estimatedBackupSize;
            let coldRowsRecomputed = 0;
            if (!HUB_HOSTING_MODE) {
                const estimate = await estimateServerBackupSize();
                estimatedBackupSize = estimate.size;
                coldRowsRecomputed = estimate.coldRowsRecomputed;
            }
            // Inlay payload now lives on the filesystem (post-migration) rather
            // than in kv `inlay/*` prefixes. Surface explicitly so the dashboard
            // chart can include it in the inlay slice instead of underreporting.
            const inlayFsBytes = await sumInlayFsBytes();
            const assetFsBytes = sumAssetFsBytes();
            const chatBackupFsBytes = await sumDirectoryFsBytes(getChatBackupsDir());
            let chatBackupSameAsSaveDir = true;
            try {
                const saveStat = require('fs').statSync(saveDir);
                const chatBackupStat = require('fs').statSync(getChatBackupsDir());
                chatBackupSameAsSaveDir = saveStat.dev === chatBackupStat.dev;
            } catch {
                const relative = path.relative(saveDir, getChatBackupsDir());
                chatBackupSameAsSaveDir = !relative.startsWith('..') && !path.isAbsolute(relative);
            }

            if (DB_CACHE_TEST_DIAGNOSTICS) {
                res.setHeader(
                    'x-pocketrisu-test-cold-rows-recomputed',
                    String(coldRowsRecomputed),
                );
            }
            res.json({
                hubHosting: HUB_HOSTING_MODE,
                files,
                disk,
                ...(backupDisk ? { backupDisk } : {}),
                sqlite: { pageSize, pageCount, freelistCount, reclaimable, journalMode, synchronous, autoVacuum },
                chunks: { count: chunkStat.c, bytes: chunkStat.b, orphanBytes: orphanChunkBytes, liveChunked },
                prefixes,
                pluginStorage,
                kvRows,
                kvTotalBytes,
                ...(typeof estimatedBackupSize === 'number' ? { estimatedBackupSize } : {}),
                assetFsBytes,
                inlayFsBytes,
                chatBackupFsBytes,
                chatBackupSameAsSaveDir,
                backups: {
                    kv: { count: backupKeys.length, totalSize: backupTotal, oldest: backupOldest, newest: backupNewest },
                    file: fileBackups,
                },
                trashed,
                orphan,
                etag: getDbEtag(),
            });
        } catch (err) { next(err); }
    });

    app.get('/api/db/stats/characters', async (req, res, next) => {
        if (!await checkAuth(req, res)) return;
        try {
            const prepared = await queueStorageReadAfterImports(async () => {
                await flushPendingDb();
                return prepareLiveDatabaseRead('StatsCharacters', {
                    includeFullBlob: false,
                });
            });
            if (!prepared) {
                res.json({ characters: [], orphan: { count: 0, totalSize: 0 }, chatBytesNote: 'estimate' });
                return;
            }
            const dbObj = prepared.strippedDatabase;

            const assetSize = new Map();
            for (const it of listAssetEntriesWithSizes()) {
                assetSize.set(statsBasename(it.key), it.size);
            }
            // remotes/<chaId>.local.bin (+ optional .meta sidecar) → bucket by chaId.
            const remoteSize = new Map();
            for (const it of kvListWithSizes('remotes/')) {
                const bn = statsBasename(it.key).replace(/\.meta$/, '');
                const chaId = bn.replace(/\.local\.bin$/, '');
                if (chaId) remoteSize.set(chaId, (remoteSize.get(chaId) || 0) + it.size);
            }

            const claimed = new Set();
            const characters = [];
            const list = Array.isArray(dbObj.characters) ? dbObj.characters : [];
            for (const cha of list) {
                if (!cha) continue;
                const refs = [];
                const collect = (v) => { if (v) refs.push(statsBasename(v)); };
                collect(cha.image);
                if (Array.isArray(cha.emotionImages)) for (const em of cha.emotionImages) collect(em?.[1]);
                if (Array.isArray(cha.additionalAssets)) for (const em of cha.additionalAssets) collect(em?.[1]);
                if (cha.vits?.files) for (const k of Object.keys(cha.vits.files)) collect(cha.vits.files[k]);
                if (Array.isArray(cha.ccAssets)) for (const a of cha.ccAssets) collect(a?.uri);

                // Same asset shared across characters is attributed to the first one we see — avoids double-counting.
                let imgBytes = 0;
                for (const bn of refs) {
                    if (!bn || claimed.has(bn)) continue;
                    const sz = assetSize.get(bn);
                    if (sz != null) {
                        imgBytes += sz;
                        claimed.add(bn);
                    }
                }
                const remoteBytes = remoteSize.get(cha.chaId) || 0;

                const chatBytes = chatRowStore.chatBytesForChar(cha.chaId);

                // Card body = the character row minus chats (which we count separately).
                // Asset URIs themselves are tiny strings — leaving them in card body is fine.
                let cardBytes = 0;
                try {
                    const { chats: _drop, ...body } = cha;
                    cardBytes = JSON.stringify(body).length;
                } catch { /* skip un-serializable */ }

                characters.push({
                    chaId: cha.chaId || '',
                    name: cha.name || '',
                    image: cha.image || '',
                    trashed: !!cha.trashTime,
                    cardBytes,
                    imgBytes: imgBytes + remoteBytes,
                    chatBytes,
                    totalBytes: cardBytes + imgBytes + remoteBytes + chatBytes,
                });
            }

            const uncleanable = buildReachableAssetBasenameSet(dbObj);
            let orphanCount = 0, orphanTotal = 0;
            for (const it of listAssetEntriesWithSizes()) {
                if (!uncleanable.has(statsBasename(it.key))) {
                    orphanCount++;
                    orphanTotal += it.size;
                }
            }

            characters.sort((a, b) => b.totalBytes - a.totalBytes);
            if (DB_CACHE_TEST_DIAGNOSTICS) {
                res.setHeader('x-pocketrisu-test-db-cache', prepared.cacheStatus);
            }
            res.json({
                characters,
                orphan: { count: orphanCount, totalSize: orphanTotal },
                chatBytesNote: 'JSON.stringify estimate; on-disk msgpack ~0.6×',
                etag: prepared.etag,
            });
        } catch (err) { next(err); }
    });

    // Per-module breakdown — modules live inside database.bin (no separate kv keys
    // for module bodies), so size = JSON.stringify of the module + sum of its
    // referenced assets. Assets attribution is independent from /characters; an
    // asset shared between a character and a module would be counted in both.
    app.get('/api/db/stats/modules', async (req, res, next) => {
        if (!await checkAuth(req, res)) return;
        try {
            const prepared = await queueStorageReadAfterImports(async () => {
                await flushPendingDb();
                return prepareLiveDatabaseRead('StatsModules', {
                    includeFullBlob: false,
                });
            });
            if (!prepared) {
                res.json({ modules: [] });
                return;
            }
            const dbObj = prepared.strippedDatabase;
            const list = Array.isArray(dbObj.modules) ? dbObj.modules : [];

            const assetSize = new Map();
            for (const it of listAssetEntriesWithSizes()) {
                assetSize.set(statsBasename(it.key), it.size);
            }

            const modules = [];
            for (const m of list) {
                if (!m) continue;

                let bodyBytes = 0;
                try {
                    const { assets: _drop, ...body } = m;
                    bodyBytes = JSON.stringify(body).length;
                } catch { /* skip un-serializable */ }

                let assetBytes = 0;
                const seen = new Set();
                if (Array.isArray(m.assets)) {
                    for (const a of m.assets) {
                        const bn = statsBasename(a?.[1]);
                        if (!bn || seen.has(bn)) continue;
                        seen.add(bn);
                        const sz = assetSize.get(bn);
                        if (sz != null) assetBytes += sz;
                    }
                }

                modules.push({
                    id: m.id || m.namespace || m.name || '',
                    name: m.name || m.namespace || '',
                    bodyBytes,
                    assetBytes,
                    totalBytes: bodyBytes + assetBytes,
                });
            }

            modules.sort((a, b) => b.totalBytes - a.totalBytes);
            if (DB_CACHE_TEST_DIAGNOSTICS) {
                res.setHeader('x-pocketrisu-test-db-cache', prepared.cacheStatus);
            }
            res.json({ modules, etag: prepared.etag });
        } catch (err) { next(err); }
    });

    app.post('/api/db/optimize', async (req, res, next) => {
        if (!await checkAuth(req, res)) return;
        if (!checkActiveSession(req, res)) return;
        try {
            const saveDir = path.join(process.cwd(), 'save');
            const dbFilePath = path.join(saveDir, 'risuai.db');
            const preDbSize = statSafe(dbFilePath)?.size ?? 0;

            const { free } = await diskFreeStat(saveDir);
            if (preDbSize > 0 && free != null && free < preDbSize * 1.2) {
                return res.status(400).json({
                    error: 'Insufficient disk space for VACUUM',
                    required: Math.ceil(preDbSize * 1.2),
                    free,
                });
            }

            // VACUUM cannot run inside another request's transaction, and the orphan
            // sweep would delete rows an in-flight import is still publishing.
            const result = await queueStorageMutation(async () => {
                await flushPendingDb();
                const t0 = Date.now();
                const rawDb = kvGet(DB_BLOB_KEY);
                const strippedDb = rawDb
                    ? getCurrentDatabaseCacheValue(DB_HEX_KEY)
                        || await loadStrippedDatabase(rawDb, 'Optimize')
                    : { characters: [] };
                const chatSweep = await chatRowStore.sweepOrphanChatRows(strippedDb, {
                    graceMs: CHAT_ORPHAN_GRACE_MS,
                    capturePreImage: captureOrphanChatPreImage,
                    onPreImageCaptureFailure: (identity, error) => {
                        logger.warn(
                            `[Optimize] Skipping orphan chat row ${identity.chaId}/${identity.chatId}; `
                            + `required pre-image capture failed: ${error?.message || error}`
                        );
                    },
                });
                logger.info(
                    `[Optimize] Chat row sweep deleted ${chatSweep.deleted} orphan row(s); `
                    + `skipped ${chatSweep.skippedRecent} recent row(s) and `
                    + `${chatSweep.skippedPreImage} row(s) without a captured pre-image`
                );
                // Reclaim chunks orphaned by edits/snapshot rotation before VACUUM, so
                // their pages get compacted in the same pass. Serialized with saves by
                // the surrounding queueStorageOperation.
                let gcDeleted = 0;
                try { gcDeleted = gcChunks(); } catch (e) { logger.warn('[Optimize] chunk gc failed:', e?.message || e); }
                try { runTrackedWalCheckpoint('TRUNCATE', 'optimize-before-vacuum'); } catch (e) { logger.warn('[Optimize] checkpoint failed:', e?.message || e); }
                sqliteDb.exec('VACUUM');
                // VACUUM streams the whole DB through the WAL; without this checkpoint the
                // -wal file stays inflated until the next 5-min background TRUNCATE.
                try { runTrackedWalCheckpoint('TRUNCATE', 'optimize-after-vacuum'); } catch (e) { logger.warn('[Optimize] post-VACUUM checkpoint failed:', e?.message || e); }
                const elapsed = Date.now() - t0;
                const postDbSize = statSafe(dbFilePath)?.size ?? 0;
                return {
                    ok: true,
                    elapsedMs: elapsed,
                    preDbSize,
                    postDbSize,
                    reclaimed: Math.max(0, preDbSize - postDbSize),
                    chunksReclaimed: gcDeleted,
                    orphanChatRowsDeleted: chatSweep.deleted,
                    orphanChatRowsSkippedRecent: chatSweep.skippedRecent,
                    orphanChatRowsSkippedPreImage: chatSweep.skippedPreImage,
                };
            });
            res.json(result);
        } catch (err) {
            if (isImportInProgressError(err)) return sendImportBusy(res);
            next(err);
        }
    });

    app.get('/api/db/durability', async (req, res, next) => {
        if (!await checkAuth(req, res)) return;
        try {
            res.json(sqliteDurabilityState());
        } catch (err) { next(err); }
    });

    app.put('/api/db/durability', async (req, res, next) => {
        if (!await checkAuth(req, res)) return;
        if (!checkActiveSession(req, res)) return;
        if (isSqliteDurabilityManaged()) {
            return res.status(403).json({
                error: 'SQLite durability is managed by the server administrator',
                ...sqliteDurabilityState(),
            });
        }
        const nextMode = normalizeSqliteDurabilityMode(req.body?.mode);
        if (!nextMode) {
            return res.status(400).json({
                error: 'mode must be one of: durable, balanced, performance',
            });
        }
        try {
            await queueStorageMutation(() => {
                persistSqliteDurabilityMode(nextMode);
            });
            rescheduleSqliteDurabilityCheckpoint();
            res.json(sqliteDurabilityState());
        } catch (err) {
            if (isImportInProgressError(err)) return sendImportBusy(res);
            next(err);
        }
    });

    app.post('/api/db/wal-checkpoint', async (req, res, next) => {
        if (!await checkAuth(req, res)) return;
        if (!checkActiveSession(req, res)) return;
        try {
            const saveDir = path.join(process.cwd(), 'save');
            const walFilePath = path.join(saveDir, 'risuai.db-wal');
            const preWalSize = statSafe(walFilePath)?.size ?? 0;

            // A checkpoint cannot truncate past an import's open transaction.
            const result = await queueStorageMutation(async () => {
                await flushPendingDb();
                const t0 = Date.now();
                const checkpoint = runTrackedWalCheckpoint('TRUNCATE', 'manual-cleanup');
                const elapsed = Date.now() - t0;
                const postWalSize = statSafe(walFilePath)?.size ?? 0;
                return {
                    ok: checkpoint.complete,
                    checkpoint,
                    elapsedMs: elapsed,
                    preWalSize,
                    postWalSize,
                    reclaimed: Math.max(0, preWalSize - postWalSize),
                };
            });
            if (!result.ok) return res.status(503).json(result);
            res.json(result);
        } catch (err) {
            if (isImportInProgressError(err)) return sendImportBusy(res);
            next(err);
        }
    });

    // ── Snapshot list (database/dbbackup-* keys) ─────────────────────────────────

    app.get('/api/db/snapshots/limits', async (req, res, next) => {
        if (!await checkAuth(req, res)) return;
        try {
            const { maxCount, maxBytes } = getSnapshotLimits();
            const usage = snapshotUsage();
            res.json({
                maxCount,
                maxBytes,
                currentCount: usage.count,
                currentBytes: usage.bytes,
                logicalBytes: usage.logicalBytes,
                bounds: {
                    minCount: SNAPSHOT_LIMIT_MIN_COUNT,
                    maxCount: SNAPSHOT_LIMIT_MAX_COUNT,
                    minBytes: SNAPSHOT_LIMIT_MIN_BYTES,
                    maxBytes: SNAPSHOT_LIMIT_MAX_BYTES,
                },
                defaults: {
                    count: SNAPSHOT_LIMIT_DEFAULT_COUNT,
                    bytes: SNAPSHOT_LIMIT_DEFAULT_BYTES,
                },
            });
        } catch (err) { next(err); }
    });

    app.put('/api/db/snapshots/limits', async (req, res, next) => {
        if (!await checkAuth(req, res)) return;
        if (!checkActiveSession(req, res)) return;
        try {
            const rawCount = Number(req.body?.maxCount);
            if (!Number.isFinite(rawCount) || rawCount < SNAPSHOT_LIMIT_MIN_COUNT || rawCount > SNAPSHOT_LIMIT_MAX_COUNT) {
                return res.status(400).json({ error: `maxCount out of range (${SNAPSHOT_LIMIT_MIN_COUNT}-${SNAPSHOT_LIMIT_MAX_COUNT})` });
            }
            const maxCount = Math.floor(rawCount);
            // Hub instances pin the byte cap server-side — only the snapshot count
            // is tenant-tunable, so a crafted request can't grow host disk usage.
            const HUB_HOSTING_MODE = isHubHostingMode();
            let maxBytes;
            if (HUB_HOSTING_MODE) {
                maxBytes = getSnapshotLimits().maxBytes;
            } else {
                const rawBytes = Number(req.body?.maxBytes);
                if (!Number.isFinite(rawBytes) || rawBytes < SNAPSHOT_LIMIT_MIN_BYTES || rawBytes > SNAPSHOT_LIMIT_MAX_BYTES) {
                    return res.status(400).json({ error: `maxBytes out of range` });
                }
                maxBytes = Math.floor(rawBytes);
            }
            const { trim, usage } = await queueStorageMutation(() => {
                if (!HUB_HOSTING_MODE) {
                    kvSet(SNAPSHOT_LIMIT_BYTES_KEY, Buffer.from(String(maxBytes), 'utf-8'));
                }
                kvSet(SNAPSHOT_LIMIT_COUNT_KEY, Buffer.from(String(maxCount), 'utf-8'));
                return { trim: trimSnapshotsToLimits(), usage: snapshotUsage() };
            });
            res.json({
                maxCount, maxBytes,
                currentCount: usage.count,
                currentBytes: usage.bytes,
                logicalBytes: usage.logicalBytes,
                removed: trim.removed,
            });
        } catch (err) {
            if (isImportInProgressError(err)) return sendImportBusy(res);
            next(err);
        }
    });

    app.get('/api/db/snapshots', async (req, res, next) => {
        if (!await checkAuth(req, res)) return;
        try {
            const out = await queueStorageReadAfterImports(() => listInternalSnapshotMetadata()
                .filter(Boolean)
                .sort((a, b) => b.timestamp - a.timestamp || b.key.localeCompare(a.key)));
            res.json({ snapshots: out });
        } catch (err) { next(err); }
    });

    app.delete('/api/db/snapshots', async (req, res, next) => {
        if (!await checkAuth(req, res)) return;
        if (!checkActiveSession(req, res)) return;
        try {
            const key = typeof req.query?.key === 'string' ? req.query.key : '';
            // Require the complete canonical name — a prefix sibling must never be
            // deletable through this endpoint.
            if (!parseInternalSnapshotKey(key)) {
                return res.status(400).json({ error: 'Invalid snapshot key' });
            }
            await queueStorageMutation(() => kvDel(key));
            res.json({ ok: true });
        } catch (err) {
            if (isImportInProgressError(err)) return sendImportBusy(res);
            next(err);
        }
    });
}

module.exports = {
    registerAssetCleanupRoute,
    registerMaintenanceRoutes,
};
