'use strict';

const path = require('path');
const {
    existsSync,
    readFileSync,
    writeFileSync,
} = require('fs');
const fs = require('fs/promises');
const { spawn } = require('child_process');
const os = require('os');
const { Readable, Transform } = require('stream');
const { pipeline } = require('stream/promises');
const {
    RECOVERY_PATH_STATE_HANDOFF_NAME,
    acquireRecoveryPathStateLockSync,
    addRecoveryPathMarkerKeepEntriesSync,
    publishRecoveryPathStateLockHandoffSync,
    recoveryPathKeepSetHas,
} = require('../recoveryPathMarkers.cjs');
const { extractArchiveSync } = require('./archiveExtraction.cjs');

let selfUpdateInProgress = false;
let recoveryPathStateLockTail = Promise.resolve();

function withLocalRecoveryPathStateLock(operation) {
    const predecessor = recoveryPathStateLockTail;
    let release;
    recoveryPathStateLockTail = new Promise(resolve => { release = resolve; });
    return predecessor.then(operation).finally(() => release());
}

function isSelfUpdateInProgress() {
    return selfUpdateInProgress;
}

function quoteWindowsBatchArgument(value) {
    return `"${String(value).replaceAll('%', '%%')}"`;
}

async function waitAtRecoveryPathStateTestGate(stage) {
    if (process.env.NODE_ENV !== 'test') return;
    const configured = String(
        process.env.POCKETRISU_TEST_RECOVERY_PATH_STATE_GATE_DIR ?? '',
    ).trim();
    if (!configured) return;
    const gateDir = path.resolve(configured);
    let selectedStage;
    try { selectedStage = (await fs.readFile(path.join(gateDir, 'stage'), 'utf8')).trim(); }
    catch { return; }
    if (selectedStage !== stage || !existsSync(path.join(gateDir, 'hold'))) return;
    await fs.mkdir(gateDir, { recursive: true });
    await fs.writeFile(path.join(gateDir, 'entered'), stage, 'utf8');
    const releasePath = path.join(gateDir, 'release');
    while (existsSync(path.join(gateDir, 'hold')) && !existsSync(releasePath)) {
        await new Promise(resolve => setTimeout(resolve, 10));
    }
}

let getRegisteredSavePath;
let getRegisteredRecoveryPathPlatform;

function addUpdaterRecoveryKeeps(keep, onKeep) {
    return addRecoveryPathMarkerKeepEntriesSync({
        root: process.cwd(),
        markerDirectory: getRegisteredSavePath(),
        keep,
        onKeep,
        platform: getRegisteredRecoveryPathPlatform(),
    });
}

// ── Update check ─────────────────────────────────────────────────────────────
const UPDATE_CHECK_DISABLED = process.env.RISU_UPDATE_CHECK === 'false';
const UPDATE_CHECK_URL = process.env.RISU_UPDATE_URL || 'https://risu-update-worker.nodridan.workers.dev/check';
const PUBLIC_STATS_URL = (process.env.RISU_UPDATE_URL || 'https://risu-update-worker.nodridan.workers.dev/check').replace(/\/check$/, '/api/public-stats');

// Re-read on each call so non-portable updates (docker/git pull) without a
// process restart don't keep reporting the old version to the update worker.
function getCurrentVersion() {
    try {
        const pkg = JSON.parse(readFileSync(path.join(process.cwd(), 'package.json'), 'utf-8'));
        return pkg.version || '0.0.0';
    } catch { return '0.0.0'; }
}

// ── Deployment type & self-update helpers ─────────────────────────────────────
const GITHUB_REPO = 'PocketRisu/PocketRisu';

const deploymentType = (() => {
    // Only portable builds have the .portable marker (created by CI release workflow).
    // Self-update is gated on this — all other types are inferred for analytics only.
    // Wrapped in try/catch so unexpected filesystem errors can't crash server boot.
    try {
        if (existsSync(path.join(process.cwd(), '.portable'))) return 'portable';
        if (existsSync(path.join(process.cwd(), '.git'))) return 'git';
        if (existsSync('/.dockerenv')) return 'docker';
        try {
            const cgroup = readFileSync('/proc/1/cgroup', 'utf-8');
            if (cgroup.includes('docker') || cgroup.includes('containerd')) return 'docker';
        } catch {}
        if (process.platform === 'android') return 'termux';
    } catch {}
    return 'unknown';
})();

function getSelfUpdateAssetInfo(version) {
    const platformMap = { win32: 'win', linux: 'linux', darwin: 'macos' };
    const platformName = platformMap[process.platform];
    if (!platformName) return null;
    const arch = process.arch; // x64, arm64
    const ext = process.platform === 'win32' ? 'zip' : 'tar.gz';
    const filename = `PocketRisu-v${version}-${platformName}-${arch}.${ext}`;
    const testUrl = process.env.NODE_ENV === 'test'
        ? String(process.env.POCKETRISU_TEST_SELF_UPDATE_ASSET_URL ?? '').trim()
        : '';
    const url = testUrl
        || `https://github.com/${GITHUB_REPO}/releases/download/v${version}/${filename}`;
    return { platformName, arch, ext, filename, url };
}

function registerSelfUpdateRoutes(app, ctx) {
    const {
        checkAuth,
        logger,
        getSavePath,
        getInstanceId,
        getRecoveryPathPlatform,
        queueStorageMutation,
        flushPendingDb,
        createBackupAndRotate,
        runTrackedWalCheckpointWithBusyRetry,
    } = ctx;
    getRegisteredSavePath = getSavePath;
    getRegisteredRecoveryPathPlatform = getRecoveryPathPlatform;

    async function fetchLatestRelease(lang) {
        if (UPDATE_CHECK_DISABLED) return null;
        try {
            const currentVersion = getCurrentVersion();
            const params = new URLSearchParams({
                v: currentVersion,
                d: deploymentType,
                os: `${process.platform}-${process.arch}`,
                id: getInstanceId(),
            });
            if (lang) params.set('l', String(lang).slice(0, 16));
            const url = `${UPDATE_CHECK_URL}?${params}`;
            const res = await fetch(url);
            if (!res.ok) return null;
            const data = await res.json();
            if (data.hasUpdate) {
                console.log(`[Update] New version available: v${data.latestVersion} (current: v${currentVersion}, ${data.severity})`);
            }
            return data;
        } catch (e) {
            logger.error('[Update] Failed to check for updates:', e.message);
            return null;
        }
    }

    // ── Public stats proxy ───────────────────────────────────────────────────────
    app.get('/api/public-stats', async (req, res) => {
        try {
            const r = await fetch(PUBLIC_STATS_URL);
            if (!r.ok) { res.status(r.status).json({ error: 'upstream error' }); return; }
            const data = await r.json();
            res.json(data);
        } catch {
            res.status(502).json({ error: 'fetch failed' });
        }
    });

    // ── Update check endpoint ────────────────────────────────────────────────────
    app.get('/api/update-check', async (req, res) => {
        const currentVersion = getCurrentVersion();
        if (UPDATE_CHECK_DISABLED) {
            res.json({ currentVersion, hasUpdate: false, severity: 'none', disabled: true, deploymentType, canSelfUpdate: false });
            return;
        }
        const result = await fetchLatestRelease(req.query.lang);
        const response = result || { currentVersion, hasUpdate: false, severity: 'none' };
        response.deploymentType = deploymentType;
        response.canSelfUpdate = deploymentType === 'portable'
            && !!response.hasUpdate
            && !response.manualOnly
            && !!getSelfUpdateAssetInfo(response.latestVersion);
        res.json(response);
    });

    // ── Self-update endpoint (portable only) ─────────────────────────────────────
    app.post('/api/self-update', async (req, res) => {
        if (!await checkAuth(req, res)) return;

        if (deploymentType !== 'portable') {
            res.status(400).json({ error: 'Self-update is only available for portable deployments' });
            return;
        }
        let recoveryKeepSnapshot;
        let recoveryPathInterprocessLock = null;
        let recoveryPathInterprocessLockHandedOff = false;
        const windowsPostUpdateFinalizer = process.platform === 'win32'
            || (process.env.NODE_ENV === 'test'
                && process.env.POCKETRISU_TEST_SELF_UPDATE_WINDOWS_FINALIZER === 'true');
        const releaseSelfUpdateAdmission = () => {
            selfUpdateInProgress = false;
            const activeLock = recoveryPathInterprocessLock;
            recoveryPathInterprocessLock = null;
            if (recoveryPathInterprocessLockHandedOff) return;
            if (activeLock) {
                try { activeLock.release(); }
                catch (error) {
                    // A failed release leaves the exact lock fail-closed for the
                    // next operation; do not hide the update's primary outcome.
                    logger.error('[RecoveryPath] Could not release self-update state lock:', error);
                }
            }
        };
        try {
            const admission = await withLocalRecoveryPathStateLock(async () => {
                if (selfUpdateInProgress) return { updateConflict: true };
                recoveryPathInterprocessLock = acquireRecoveryPathStateLockSync(getSavePath(), {
                    purpose: 'server self-update',
                });
                try {
                    const snapshot = addUpdaterRecoveryKeeps(new Set());
                    selfUpdateInProgress = true;
                    await waitAtRecoveryPathStateTestGate('self-update-admitted');
                    return { updateConflict: false, snapshot };
                } catch (error) {
                    releaseSelfUpdateAdmission();
                    throw error;
                }
            });
            if (admission.updateConflict) {
                res.status(409).json({ error: 'Update already in progress' });
                return;
            }
            recoveryKeepSnapshot = admission.snapshot;
        } catch (error) {
            res.status(409).json({ error: error?.message || 'Recovery metadata is unavailable' });
            return;
        }

        // Track client disconnect — used to abort download, but NOT to release the lock.
        // The lock stays held until the update fully completes or fails, preventing
        // a second request from touching the same install directory concurrently.
        let clientDisconnected = false;
        res.on('close', () => {
            clientDisconnected = true;
            console.log('[Update] Client disconnected (update continues if past download stage).');
        });

        // NDJSON streaming response
        res.writeHead(200, {
            'Content-Type': 'application/x-ndjson',
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
        });
        const send = (step, progress, message) => {
            try { res.write(JSON.stringify({ step, progress, message }) + '\n'); } catch {}
        };

        let tmpDir = null;
        try {
            // 1. Check update
            send('checking', 0, 'Checking for updates...');
            const updateInfo = await fetchLatestRelease();
            if (!updateInfo?.hasUpdate) {
                send('done', 100, 'Already up to date.');
                res.end();
                releaseSelfUpdateAdmission();
                return;
            }

            const targetVersion = updateInfo.latestVersion;
            const assetInfo = getSelfUpdateAssetInfo(targetVersion);
            if (!assetInfo) {
                throw new Error(`No release asset for ${process.platform}-${process.arch}`);
            }

            // 2. Download
            tmpDir = path.join(os.tmpdir(), `risu-update-${Date.now()}`);
            await fs.mkdir(tmpDir, { recursive: true });
            const archivePath = path.join(tmpDir, assetInfo.filename);

            send('downloading', 0, 'Starting download...');
            const dlRes = await fetch(assetInfo.url, { redirect: 'follow' });
            if (!dlRes.ok) throw new Error(`Download failed: ${dlRes.status} ${dlRes.statusText}`);

            const totalSize = parseInt(dlRes.headers.get('content-length'), 10) || 0;
            const fileStream = require('fs').createWriteStream(archivePath);
            let downloaded = 0;
            let lastPct = -1;

            const progress = new Transform({
                transform(chunk, _enc, cb) {
                    if (clientDisconnected) { cb(new Error('Client disconnected')); return; }
                    downloaded += chunk.length;
                    if (totalSize > 0) {
                        const pct = Math.round((downloaded / totalSize) * 100);
                        if (pct >= lastPct + 5) {
                            lastPct = pct;
                            const dlMB = (downloaded / 1048576).toFixed(0);
                            const totalMB = (totalSize / 1048576).toFixed(0);
                            send('downloading', pct, `Downloading... ${pct}% (${dlMB}/${totalMB} MB)`);
                        }
                    }
                    cb(null, chunk);
                },
            });
            await pipeline(Readable.fromWeb(dlRes.body), progress, fileStream);
            send('downloading', 100, 'Download complete.');

            // 3. Extract
            send('extracting', null, 'Extracting...');
            const extractDir = path.join(tmpDir, 'extracted');
            await fs.mkdir(extractDir, { recursive: true });

            extractArchiveSync(archivePath, extractDir, {
                format: assetInfo.ext === 'zip' ? 'zip' : 'tar.gz',
            });

            // Resolve possibly nested root directory (same as updater.cjs resolveExtractedRoot)
            const entries = await fs.readdir(extractDir);
            let sourceDir = extractDir;
            if (entries.length === 1) {
                const candidate = path.join(extractDir, entries[0]);
                if ((await fs.stat(candidate)).isDirectory()) sourceDir = candidate;
            }

            // 4. Validate extracted package (mirrors updater.cjs validateExtractedRoot)
            const REQUIRED_ENTRIES = ['dist', 'server', 'package.json'];
            const REQUIRED_DIST_FILES = ['index.html'];
            for (const entry of REQUIRED_ENTRIES) {
                try { await fs.access(path.join(sourceDir, entry)); }
                catch { throw new Error(`Downloaded package is missing required entry: ${entry}`); }
            }
            for (const file of REQUIRED_DIST_FILES) {
                try { await fs.access(path.join(sourceDir, 'dist', file)); }
                catch { throw new Error(`Downloaded package is missing dist/${file}`); }
            }
            if (windowsPostUpdateFinalizer) {
                try { await fs.access(path.join(sourceDir, 'bin')); }
                catch { throw new Error('Downloaded Windows package is missing bin/'); }
            }

            // 5. Replace files (follows updater.cjs Phase 1-4 pattern)
            send('replacing', null, 'Replacing files...');
            const appDir = process.cwd();
            const isWin = windowsPostUpdateFinalizer;
            const updateTmp = path.join(appDir, '.update-tmp');

            // Restore from a previous interrupted update if leftover exists
            const prevBackup = path.join(updateTmp, 'backup');
            try {
                await fs.access(prevBackup);
                console.log('[Update] Restoring files from previous interrupted update...');
                await restoreBackup(prevBackup, appDir);
            } catch { /* no leftover */ }
            await fs.rm(updateTmp, { recursive: true, force: true }).catch(() => {});
            await fs.mkdir(updateTmp, { recursive: true });

            // Carry over SSL certificates into new package before swap
            const sslSrc = path.join(appDir, 'server', 'node', 'ssl', 'certificate');
            try {
                await fs.access(sslSrc);
                const sslDst = path.join(sourceDir, 'server', 'node', 'ssl', 'certificate');
                await fs.mkdir(path.dirname(sslDst), { recursive: true });
                await fs.cp(sslSrc, sslDst, { recursive: true });
            } catch { /* no user certs */ }

            // Keep set — matches updater.cjs + user data/config that must survive updates
            const keep = new Set(['save', 'backups', '.installed-version', '.update-tmp', 'scripts', '.env', '.npmrc', '.portable']);
            if (isWin) keep.add('bin');
            for (const entry of recoveryKeepSnapshot) keep.add(entry);
            addUpdaterRecoveryKeeps(keep, (entry, label) => {
                logger.info(`[Update] Preserving ${label.toLowerCase()}: ${entry}/`);
            });

            // Phase 1: move old files to backup — rollback immediately on any failure
            const backupDir = path.join(updateTmp, 'backup');
            await fs.mkdir(backupDir, { recursive: true });

            const oldEntries = await fs.readdir(appDir);
            for (const e of oldEntries) {
                if (recoveryPathKeepSetHas(keep, e, getRecoveryPathPlatform())) continue;
                try {
                    await fs.rename(path.join(appDir, e), path.join(backupDir, e));
                } catch (backupErr) {
                    logger.error(`[Update] Failed to back up ${e}: ${backupErr.message}`);
                    console.log('[Update] Restoring files already moved to backup...');
                    await restoreBackup(backupDir, appDir);
                    throw new Error(isWin
                        ? 'Update failed: some files are in use. Close RisuAI first, then try again.'
                        : 'Update failed: some files are in use. Stop the server first, then try again.');
                }
            }

            // Phase 2: move new files from extracted to app root
            const skipMove = new Set(['save', 'scripts']);
            if (isWin) skipMove.add('bin');
            const moved = [];
            try {
                const newEntries = await fs.readdir(sourceDir);
                for (const e of newEntries) {
                    if (skipMove.has(e)) continue;
                    const dest = path.join(appDir, e);
                    await fs.rm(dest, { recursive: true, force: true }).catch(() => {});
                    await moveAcrossVolumes(path.join(sourceDir, e), dest);
                    moved.push(e);
                }
                // Post-move validation
                for (const entry of REQUIRED_ENTRIES) {
                    if (!moved.includes(entry) && !existsSync(path.join(appDir, entry))) {
                        throw new Error(`Required entry was not installed: ${entry}`);
                    }
                }
                for (const file of REQUIRED_DIST_FILES) {
                    if (!existsSync(path.join(appDir, 'dist', file))) {
                        throw new Error(`Required file was not installed: dist/${file}`);
                    }
                }
            } catch (moveErr) {
                logger.error(`[Update] Move failed: ${moveErr.message}`);
                console.log('[Update] Restoring from backup...');
                await restoreBackup(backupDir, appDir);
                throw new Error('Update failed, previous version restored. Please try again.');
            }

            // Phase 3: update scripts/ from new release
            const newScripts = path.join(sourceDir, 'scripts');
            try {
                await fs.access(newScripts);
                await fs.mkdir(path.join(appDir, 'scripts'), { recursive: true });
                for (const f of await fs.readdir(newScripts)) {
                    await fs.copyFile(path.join(newScripts, f), path.join(appDir, 'scripts', f));
                }
            } catch { /* no scripts in release */ }

            // Phase 4 (Windows): stage bin/ for restart script to apply after exit
            if (isWin) {
                const newBin = path.join(sourceDir, 'bin');
                const stagedBin = path.join(updateTmp, 'new-bin');
                await fs.rm(stagedBin, { recursive: true, force: true }).catch(() => {});
                await fs.cp(newBin, stagedBin, { recursive: true });
                // Version marker — finalized after bin/ is applied
                await fs.writeFile(path.join(updateTmp, 'latest-version'), `v${targetVersion}`);
            } else {
                await fs.writeFile(path.join(appDir, '.installed-version'), `v${targetVersion}`);
            }

            // Cleanup temp download (not .update-tmp — that stays on Windows for bin/ post-step)
            fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
            tmpDir = null;
            if (!isWin) {
                fs.rm(updateTmp, { recursive: true, force: true }).catch(() => {});
            }

            send('restarting', 100, 'Update complete. Restarting...');
            res.end();

            if (process.env.NODE_ENV === 'test'
                && process.env.POCKETRISU_TEST_SELF_UPDATE_SKIP_RESTART === 'true'
                && !windowsPostUpdateFinalizer) {
                releaseSelfUpdateAdmission();
                return;
            }

            // 6. Flush DB and restart
            setTimeout(async () => {
                try {
                console.log(`[Update] Self-update to v${targetVersion} complete. Restarting...`);
                try {
                    const persisted = await queueStorageMutation(
                        () => flushPendingDb({ scheduleSnapshot: false }),
                    );
                    if (persisted) await createBackupAndRotate();
                } catch {}
                try {
                    await runTrackedWalCheckpointWithBusyRetry('TRUNCATE', 'self-update');
                } catch {}

                const port = process.env.PORT || 6001;

                if (isWin) {
                    // Windows: use a .bat script to apply bin/, finalize version, and restart.
                    // A bat script can replace bin/node.exe after the Node process exits,
                    // avoiding file-lock issues that a Node child process would hit. The
                    // token handoff keeps recovery-path exclusion continuously owned until
                    // xcopy/version finalization has completed.
                    const batScript = path.join(os.tmpdir(), `risu-restart-${Date.now()}.bat`);
                    const utmp = path.join(appDir, '.update-tmp');
                    const binDir = path.join(appDir, 'bin');
                    const binBackup = path.join(utmp, 'old-bin');
                    const handoffPath = path.join(utmp, RECOVERY_PATH_STATE_HANDOFF_NAME);
                    const finalizerScript = path.join(
                        appDir,
                        'scripts',
                        'recoveryPathLockFinalizer.cjs',
                    );
                    publishRecoveryPathStateLockHandoffSync(
                        handoffPath,
                        getSavePath(),
                        recoveryPathInterprocessLock.token,
                    );

                    if (process.env.NODE_ENV === 'test'
                        && process.env.POCKETRISU_TEST_SELF_UPDATE_WINDOWS_FINALIZER === 'true') {
                        // Exercise the same post-parent ordering on non-Windows CI:
                        // bin copy and version finalize precede token-verified release.
                        await fs.cp(path.join(utmp, 'new-bin'), binDir, {
                            recursive: true,
                            force: true,
                        });
                        await fs.copyFile(
                            path.join(utmp, 'latest-version'),
                            path.join(appDir, '.installed-version'),
                        );
                        const finalizer = spawn(process.execPath, [finalizerScript, handoffPath], {
                            cwd: appDir,
                            env: { ...process.env },
                            stdio: ['ignore', 'ignore', 'pipe'],
                        });
                        recoveryPathInterprocessLockHandedOff = true;
                        let finalizerError = '';
                        finalizer.stderr?.on('data', chunk => { finalizerError += chunk.toString(); });
                        const finalizerExit = await new Promise((resolve, reject) => {
                            finalizer.once('error', reject);
                            finalizer.once('exit', resolve);
                        });
                        if (finalizerExit !== 0) {
                            throw new Error(
                                `Windows recovery-lock finalizer failed (${finalizerExit}): ${finalizerError}`,
                            );
                        }
                        await fs.rm(utmp, { recursive: true, force: true });
                        releaseSelfUpdateAdmission();
                        return;
                    }

                    const batLines = [
                        '@echo off',
                        'timeout /t 3 /nobreak >nul',
                        // Apply staged bin/: backup current → copy new → on failure restore backup
                        `if exist ${quoteWindowsBatchArgument(`${path.join(utmp, 'new-bin')}\\`)} (`,
                        `  if exist ${quoteWindowsBatchArgument(`${binDir}\\`)} (`,
                        `    xcopy /E /I /Y ${quoteWindowsBatchArgument(`${binDir}\\*`)} ${quoteWindowsBatchArgument(`${binBackup}\\`)} >nul`,
                        `  )`,
                        `  xcopy /E /I /Y ${quoteWindowsBatchArgument(`${path.join(utmp, 'new-bin')}\\*`)} ${quoteWindowsBatchArgument(`${binDir}\\`)} >nul`,
                        `  if errorlevel 1 (`,
                        `    echo [Update] bin/ copy failed, restoring backup...`,
                        `    if exist ${quoteWindowsBatchArgument(`${binBackup}\\`)} (`,
                        `      xcopy /E /I /Y ${quoteWindowsBatchArgument(`${binBackup}\\*`)} ${quoteWindowsBatchArgument(`${binDir}\\`)} >nul`,
                        `    )`,
                        `    echo [Update] bin/ restored. Staged files kept for retry.`,
                        `    goto finalize`,
                        `  )`,
                        `)`,
                        // Finalize version marker only after successful bin/ copy
                        `if exist ${quoteWindowsBatchArgument(path.join(utmp, 'latest-version'))} (`,
                        `  copy /Y ${quoteWindowsBatchArgument(path.join(utmp, 'latest-version'))} ${quoteWindowsBatchArgument(path.join(appDir, '.installed-version'))} >nul`,
                        `)`,
                        ':finalize',
                        `${quoteWindowsBatchArgument(path.join(binDir, 'node.exe'))} ${quoteWindowsBatchArgument(finalizerScript)} ${quoteWindowsBatchArgument(handoffPath)}`,
                        `if errorlevel 1 exit /b 1`,
                        // Cleanup .update-tmp (includes old-bin backup)
                        `rmdir /s /q ${quoteWindowsBatchArgument(utmp)} 2>nul`,
                        ':start',
                        // Start server with correct working directory
                        `cd /d ${quoteWindowsBatchArgument(appDir)}`,
                        `start "" ${quoteWindowsBatchArgument(path.join(appDir, 'bin', 'node.exe'))} ${quoteWindowsBatchArgument(path.join(appDir, 'server', 'node', 'server.cjs'))}`,
                        'exit /b 0',
                    ];
                    writeFileSync(batScript, batLines.join('\r\n'));
                    spawn('cmd.exe', ['/d', '/s', '/c', batScript], {
                        detached: true,
                        stdio: 'ignore',
                    }).unref();
                    recoveryPathInterprocessLockHandedOff = true;
                } else {
                    // Unix: Node restart helper with port-check to avoid clashing with process managers
                    const restartScript = path.join(os.tmpdir(), `risu-restart-${Date.now()}.cjs`);
                    writeFileSync(restartScript, [
                        `const net = require('net');`,
                        `const { spawn } = require('child_process');`,
                        `setTimeout(() => {`,
                        `  const s = net.createServer();`,
                        `  s.once('error', () => process.exit(0));`,
                        `  s.once('listening', () => {`,
                        `    s.close();`,
                        `    spawn(${JSON.stringify(process.execPath)}, ['server/node/server.cjs'], {`,
                        `      cwd: ${JSON.stringify(appDir)},`,
                        `      detached: true,`,
                        `      stdio: 'inherit',`,
                        `      env: Object.assign({}, process.env),`,
                        `    }).unref();`,
                        `    setTimeout(() => process.exit(0), 500);`,
                        `  });`,
                        `  s.listen(${Number(port)});`,
                        `}, 3000);`,
                    ].join('\n'));
                    spawn(process.execPath, [restartScript], { detached: true, stdio: 'ignore' }).unref();
                }
                releaseSelfUpdateAdmission();
                process.exit(0);
                } catch (restartErr) {
                    logger.error('[Update] Restart failed:', restartErr);
                    releaseSelfUpdateAdmission();
                }
            }, 500);

        } catch (e) {
            logger.error('[Update] Self-update failed:', e);
            send('error', null, `Update failed: ${e.message}`);
            res.end();
            releaseSelfUpdateAdmission();
            if (tmpDir) fs.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
        }
    });

    // Helper: rename, falling back to copy+remove when src and dest are on
    // different volumes (Windows EXDEV — e.g. app on D:, os.tmpdir() on C:)
    async function moveAcrossVolumes(src, dest) {
        try {
            await fs.rename(src, dest);
        } catch (err) {
            if (err && err.code === 'EXDEV') {
                await fs.cp(src, dest, { recursive: true, force: true });
                await fs.rm(src, { recursive: true, force: true });
                return;
            }
            throw err;
        }
    }

    // Helper: restore files from backup directory into app root (mirrors updater.cjs restoreBackupIntoRoot)
    async function restoreBackup(backupDir, rootDir) {
        try { await fs.access(backupDir); } catch { return; }
        for (const entry of await fs.readdir(backupDir)) {
            const src = path.join(backupDir, entry);
            const dest = path.join(rootDir, entry);
            try {
                await fs.rm(dest, { recursive: true, force: true }).catch(() => {});
                await moveAcrossVolumes(src, dest);
            } catch { /* best effort */ }
        }
    }
}

module.exports = {
    registerSelfUpdateRoutes,
    isSelfUpdateInProgress,
    withLocalRecoveryPathStateLock,
    waitAtRecoveryPathStateTestGate,
    addUpdaterRecoveryKeeps,
    quoteWindowsBatchArgument,
};
