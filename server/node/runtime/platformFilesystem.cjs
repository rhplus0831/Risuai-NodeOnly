'use strict';

const fs = require('fs');

const WINDOWS_DIRECTORY_FSYNC_UNSUPPORTED_CODES = new Set([
    'EACCES',
    'EISDIR',
    'EPERM',
]);
const DIRECTORY_FSYNC_UNSUPPORTED_CODES = new Set([
    'EINVAL',
    'ENOTSUP',
]);
const WINDOWS_RENAME_RETRY_CODES = new Set([
    'EACCES',
    'EBUSY',
    'EPERM',
]);

function sameFileIdentity(left, right) {
    if (!left || !right) return false;
    return left.dev === right.dev && left.ino === right.ino;
}

function supportsPosixModeHardening(platform = process.platform) {
    return platform !== 'win32';
}

function directoryFsyncErrorIsUnsupported(error, platform = process.platform) {
    if (DIRECTORY_FSYNC_UNSUPPORTED_CODES.has(error?.code)) return true;
    return platform === 'win32'
        && WINDOWS_DIRECTORY_FSYNC_UNSUPPORTED_CODES.has(error?.code);
}

function fsyncDirectorySync(directoryPath, options = {}) {
    const fsOps = options.fs ?? fs;
    const platform = options.platform ?? process.platform;
    const additionalUnsupportedCodes = options.additionalUnsupportedCodes ?? null;
    const isUnsupported = (error) => directoryFsyncErrorIsUnsupported(error, platform)
        || additionalUnsupportedCodes?.has?.(error?.code) === true;
    let descriptor;
    let pendingError = null;
    try {
        descriptor = fsOps.openSync(directoryPath, 'r');
        fsOps.fsyncSync(descriptor);
    } catch (error) {
        if (!isUnsupported(error)) pendingError = error;
    } finally {
        if (descriptor !== undefined) {
            try {
                fsOps.closeSync(descriptor);
            } catch (error) {
                if (!isUnsupported(error) && !pendingError) {
                    pendingError = error;
                }
            }
        }
    }
    if (pendingError) throw pendingError;
}

function hardenPrivateDescriptorSync(descriptor, mode, options = {}) {
    const fsOps = options.fs ?? fs;
    const platform = options.platform ?? process.platform;
    if (!supportsPosixModeHardening(platform)) return false;
    const stat = options.stat ?? fsOps.fstatSync(descriptor);
    if ((stat.mode & 0o777) === mode) return false;
    fsOps.fchmodSync(descriptor, mode);
    if (options.sync === true) fsOps.fsyncSync(descriptor);
    return true;
}

async function hardenPrivateFile(filePath, mode = 0o600, options = {}) {
    const platform = options.platform ?? process.platform;
    if (!supportsPosixModeHardening(platform)) return false;
    const fsOps = options.fs ?? require('fs/promises');
    await fsOps.chmod(filePath, mode);
    return true;
}

function lstatOrNullSync(filePath, fsOps) {
    try {
        return fsOps.lstatSync(filePath);
    } catch (error) {
        if (error?.code === 'ENOENT') return null;
        throw error;
    }
}

async function lstatOrNull(filePath, fsOps) {
    try {
        return await fsOps.lstat(filePath);
    } catch (error) {
        if (error?.code === 'ENOENT') return null;
        throw error;
    }
}

function waitSync(milliseconds) {
    if (milliseconds <= 0) return;
    const signal = new Int32Array(new SharedArrayBuffer(4));
    Atomics.wait(signal, 0, 0, milliseconds);
}

function windowsRenameWasCommittedSync(sourcePath, destinationPath, expectedSource, fsOps) {
    const sourceAfter = lstatOrNullSync(sourcePath, fsOps);
    if (sourceAfter) return false;
    const destinationAfter = lstatOrNullSync(destinationPath, fsOps);
    return sameFileIdentity(destinationAfter, expectedSource);
}

async function windowsRenameWasCommitted(sourcePath, destinationPath, expectedSource, fsOps) {
    const sourceAfter = await lstatOrNull(sourcePath, fsOps);
    if (sourceAfter) return false;
    const destinationAfter = await lstatOrNull(destinationPath, fsOps);
    return sameFileIdentity(destinationAfter, expectedSource);
}

function renamePublishedFileSync(sourcePath, destinationPath, options = {}) {
    const fsOps = options.fs ?? fs;
    const platform = options.platform ?? process.platform;
    if (platform !== 'win32') {
        fsOps.renameSync(sourcePath, destinationPath);
        return;
    }

    const attempts = Math.max(1, options.attempts ?? 5);
    const delayMs = Math.max(0, options.delayMs ?? 20);
    const expectedSource = fsOps.lstatSync(sourcePath);
    for (let attempt = 1; attempt <= attempts; attempt += 1) {
        try {
            fsOps.renameSync(sourcePath, destinationPath);
            return;
        } catch (error) {
            if (!WINDOWS_RENAME_RETRY_CODES.has(error?.code)) throw error;
            if (windowsRenameWasCommittedSync(
                sourcePath,
                destinationPath,
                expectedSource,
                fsOps,
            )) return;
            if (!lstatOrNullSync(sourcePath, fsOps) || attempt === attempts) throw error;
            waitSync(delayMs * attempt);
        }
    }
}

async function renamePublishedFile(sourcePath, destinationPath, options = {}) {
    const fsOps = options.fs ?? require('fs/promises');
    const platform = options.platform ?? process.platform;
    if (platform !== 'win32') {
        await fsOps.rename(sourcePath, destinationPath);
        return;
    }

    const attempts = Math.max(1, options.attempts ?? 5);
    const delayMs = Math.max(0, options.delayMs ?? 20);
    const expectedSource = await fsOps.lstat(sourcePath);
    for (let attempt = 1; attempt <= attempts; attempt += 1) {
        try {
            await fsOps.rename(sourcePath, destinationPath);
            return;
        } catch (error) {
            if (!WINDOWS_RENAME_RETRY_CODES.has(error?.code)) throw error;
            if (await windowsRenameWasCommitted(
                sourcePath,
                destinationPath,
                expectedSource,
                fsOps,
            )) return;
            if (!await lstatOrNull(sourcePath, fsOps) || attempt === attempts) throw error;
            await new Promise(resolve => setTimeout(resolve, delayMs * attempt));
        }
    }
}

module.exports = {
    directoryFsyncErrorIsUnsupported,
    fsyncDirectorySync,
    hardenPrivateDescriptorSync,
    hardenPrivateFile,
    renamePublishedFile,
    renamePublishedFileSync,
    sameFileIdentity,
    supportsPosixModeHardening,
};
