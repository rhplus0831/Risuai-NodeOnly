'use strict';

const WINDOWS_RESERVED_BASENAME_RE = /^(?:con|prn|aux|nul|conin\$|conout\$|com(?:[1-9¹²³])|lpt(?:[1-9¹²³]))$/i;
const WINDOWS_INVALID_FILENAME_RE = /[<>:"/\\|?*\u0000-\u001f]/;
const PORTABLE_PATH_ESCAPE_PREFIX = '%2Eportable-v1-';

function portableFilenameKey(name) {
    return String(name).toLowerCase().replace(/[. ]+$/, '');
}

function isPortableWindowsFileName(name) {
    if (typeof name !== 'string'
        || name.length === 0
        || name === '.'
        || name === '..'
        || Buffer.byteLength(name, 'utf8') > 255
        || WINDOWS_INVALID_FILENAME_RE.test(name)
        || /[. ]$/.test(name)) return false;
    const basename = name.split('.', 1)[0];
    return !WINDOWS_RESERVED_BASENAME_RE.test(basename);
}

function encodeLegacyPathComponent(value) {
    const encoded = encodeURIComponent(String(value));
    if (encoded === '.') return '%2E';
    if (encoded === '..') return '%2E%2E';
    return encoded;
}

function encodePortablePathComponent(value) {
    const logical = String(value);
    const legacy = encodeLegacyPathComponent(logical);
    // Windows path identity is case-insensitive. Retain legacy paths only
    // when their physical spelling is already lowercase and portable; encode
    // every other logical value with a lowercase-only alphabet so distinct
    // case-sensitive IDs cannot alias one directory.
    if (isPortableWindowsFileName(legacy) && !/[A-Z]/.test(legacy)) return legacy;
    return `${PORTABLE_PATH_ESCAPE_PREFIX}${Buffer.from(logical, 'utf8').toString('hex')}`;
}

function decodePortablePathComponent(value) {
    if (typeof value !== 'string') return null;
    if (!value.startsWith(PORTABLE_PATH_ESCAPE_PREFIX)) {
        try {
            return decodeURIComponent(value);
        } catch {
            return null;
        }
    }
    const encoded = value.slice(PORTABLE_PATH_ESCAPE_PREFIX.length);
    try {
        if (!/^(?:[0-9a-f]{2})*$/.test(encoded)) return null;
        const bytes = Buffer.from(encoded, 'hex');
        const decoded = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
        return encodePortablePathComponent(decoded) === value ? decoded : null;
    } catch {
        return null;
    }
}

module.exports = {
    PORTABLE_PATH_ESCAPE_PREFIX,
    WINDOWS_RESERVED_BASENAME_RE,
    decodePortablePathComponent,
    encodeLegacyPathComponent,
    encodePortablePathComponent,
    isPortableWindowsFileName,
    portableFilenameKey,
};
