'use strict';

const { execFileSync: defaultExecFileSync } = require('child_process');

function quotePowerShellLiteral(value) {
    return `'${String(value).replaceAll("'", "''")}'`;
}

function encodedPowerShellCommand(script) {
    return Buffer.from(script, 'utf16le').toString('base64');
}

function extractArchiveSync(archivePath, destinationPath, options = {}) {
    const platform = options.platform ?? process.platform;
    const format = options.format ?? (String(archivePath).endsWith('.zip') ? 'zip' : 'tar.gz');
    const execFileSync = options.execFileSync ?? defaultExecFileSync;
    const commandOptions = {
        stdio: options.stdio ?? 'pipe',
        timeout: options.timeout ?? 300_000,
        windowsHide: true,
    };

    if (platform === 'win32' && format === 'zip') {
        try {
            execFileSync('tar.exe', ['-xf', archivePath, '-C', destinationPath], commandOptions);
            return 'tar.exe';
        } catch (tarError) {
            const script = [
                "$ErrorActionPreference = 'Stop'",
                `Expand-Archive -Force -LiteralPath ${quotePowerShellLiteral(archivePath)} `
                    + `-DestinationPath ${quotePowerShellLiteral(destinationPath)}`,
            ].join('; ');
            try {
                execFileSync('powershell.exe', [
                    '-NoLogo',
                    '-NoProfile',
                    '-NonInteractive',
                    '-EncodedCommand',
                    encodedPowerShellCommand(script),
                ], commandOptions);
                return 'powershell.exe';
            } catch (powershellError) {
                const error = new Error(
                    'Could not extract the update archive: neither tar.exe nor PowerShell Expand-Archive succeeded',
                    { cause: powershellError },
                );
                error.tarError = tarError;
                throw error;
            }
        }
    }

    const args = format === 'zip'
        ? ['-xf', archivePath, '-C', destinationPath]
        : ['-xzf', archivePath, '-C', destinationPath];
    execFileSync('tar', args, commandOptions);
    return 'tar';
}

module.exports = {
    encodedPowerShellCommand,
    extractArchiveSync,
    quotePowerShellLiteral,
};
