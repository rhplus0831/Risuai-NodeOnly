'use strict';

const {
    kvListWithSizes,
} = require('../db/db.cjs');
const {
    PLUGIN_SAVE_PREFIX,
    PLUGIN_SAVE_META_PREFIX,
} = require('../plugin-storage/pluginSaveKeys.cjs');
const {
    addLogBatch,
    queryLogs,
    countLogs,
    clearLogs,
} = require('./logs.cjs');

function registerStorageCapacityRoute(app, ctx) {
    const {
        checkAuth,
        getImportBarrier,
        isHubHostingMode,
        checkDiskSpace,
    } = ctx;

    // Lightweight capacity preflight for operations that temporarily need both
    // the external plugin rows and a newly-expanded database blob on the save
    // volume. Unlike /api/db/stats this does not scan repository contents.
    app.get('/api/storage/capacity', async (req, res, next) => {
        if (!await checkAuth(req, res)) return;
        try {
            await getImportBarrier().waitUntilIdle();
            if (isHubHostingMode()) return res.send({ success: true, freeBytes: null });
            const capacity = await checkDiskSpace(0);
            res.send({
                success: true,
                freeBytes: Number.isSafeInteger(capacity.available) && capacity.available >= 0
                    ? capacity.available
                    : null,
            });
        } catch (error) {
            next(error);
        }
    });
}

function registerStorageListSizesRoute(app, ctx) {
    const {
        checkAuth,
        getImportBarrier,
    } = ctx;

    const PLUGIN_STORAGE_SIZE_PREFIXES = new Set([
        PLUGIN_SAVE_PREFIX,
        PLUGIN_SAVE_META_PREFIX,
    ]);

    // Logical sizes let clients reject an unsafe transition before a large or
    // malformed row is downloaded. Restrict this inventory to plugin storage so
    // the endpoint cannot become an arbitrary repository-inspection primitive.
    app.get('/api/storage/list-sizes', async (req, res, next) => {
        if (!await checkAuth(req, res)) return;
        try {
            const rawPrefix = Array.isArray(req.headers['key-prefix'])
                ? req.headers['key-prefix'][0]
                : req.headers['key-prefix'];
            if (!PLUGIN_STORAGE_SIZE_PREFIXES.has(rawPrefix)) {
                return res.status(400).send({ error: 'Unsupported storage size prefix' });
            }
            await getImportBarrier().waitUntilIdle();
            // The chunk-aware inventory reports authoritative logical sizes without
            // loading or reassembling plugin value bodies.
            const content = kvListWithSizes(rawPrefix);
            if (content.some((entry) => (
                typeof entry.key !== 'string'
                || !entry.key.startsWith(rawPrefix)
                || !Number.isSafeInteger(entry.size)
                || entry.size < 0
            ))) {
                throw new Error('Invalid logical plugin storage size');
            }
            res.send({ success: true, content });
        } catch (error) {
            next(error);
        }
    });
}

function registerLogRoutes(app, ctx) {
    const {
        checkAuth,
        checkActiveSession,
    } = ctx;

    const LOGS_POST_MAX_ENTRIES = 1000;
    app.post('/api/logs', async (req, res, next) => {
        if (!await checkAuth(req, res)) return;
        try {
            const body = req.body;
            const entries = Array.isArray(body) ? body : [body];
            if (entries.length === 0) {
                return res.send({ success: true, written: 0 });
            }
            if (entries.length > LOGS_POST_MAX_ENTRIES) {
                return res.status(413).send({ error: `too many entries (max ${LOGS_POST_MAX_ENTRIES})` });
            }
            const prepared = entries
                .filter(e => e && typeof e === 'object' && typeof e.message === 'string')
                .map(e => ({
                    timestamp: typeof e.timestamp === 'number' ? e.timestamp : Date.now(),
                    level: e.level,
                    origin: 'client',
                    message: e.message,
                    description: e.description,
                    source: e.source,
                    count: e.count,
                    platform: e.platform,
                    clientId: e.clientId,
                    userAgent: e.userAgent,
                }));
            const written = addLogBatch(prepared);
            res.send({ success: true, written });
        } catch (error) {
            next(error);
        }
    });

    app.get('/api/logs', async (req, res, next) => {
        if (!await checkAuth(req, res)) return;
        try {
            const parseCsv = (v) => typeof v === 'string' && v.length ? v.split(',').filter(Boolean) : undefined;
            const filterArgs = {
                level: typeof req.query.level === 'string' ? req.query.level : undefined,
                origin: typeof req.query.origin === 'string' ? req.query.origin : undefined,
                since: req.query.since ? Number(req.query.since) : undefined,
                excludeLevels: parseCsv(req.query.exclude_levels),
                excludeOrigins: parseCsv(req.query.exclude_origins),
                excludeBackground: req.query.exclude_background === '1',
            };
            const rows = queryLogs({
                ...filterArgs,
                beforeId: req.query.before_id ? Number(req.query.before_id) : undefined,
                limit: req.query.limit ? Number(req.query.limit) : undefined,
            });
            // total reflects rows matching the same filter — pagination math depends on it.
            res.send({ success: true, content: rows, total: countLogs(filterArgs) });
        } catch (error) {
            next(error);
        }
    });

    app.delete('/api/logs', async (req, res, next) => {
        if (!await checkAuth(req, res)) return;
        if (!checkActiveSession(req, res)) return;
        try {
            clearLogs();
            res.send({ success: true });
        } catch (error) {
            next(error);
        }
    });
}

module.exports = {
    registerStorageCapacityRoute,
    registerStorageListSizesRoute,
    registerLogRoutes,
};
