'use strict';

function gracefulShutdownSignals(platform = process.platform) {
    const signals = ['SIGTERM', 'SIGINT', 'SIGHUP'];
    if (platform === 'win32') signals.push('SIGBREAK');
    return signals;
}

function createIdempotentSignalHandler(shutdown, options = {}) {
    if (typeof shutdown !== 'function') {
        throw new TypeError('shutdown must be a function');
    }
    const onError = options.onError ?? (() => {});
    let shutdownPromise = null;
    return function handleSignal(signal) {
        if (!shutdownPromise) {
            shutdownPromise = Promise.resolve()
                .then(() => shutdown(signal))
                .catch((error) => {
                    onError(error, signal);
                });
        }
        return shutdownPromise;
    };
}

function installGracefulShutdownHandlers(processLike, shutdown, options = {}) {
    const handler = createIdempotentSignalHandler(shutdown, options);
    const signals = options.signals ?? gracefulShutdownSignals(options.platform);
    for (const signal of signals) {
        processLike.on(signal, () => handler(signal));
    }
    return handler;
}

module.exports = {
    createIdempotentSignalHandler,
    gracefulShutdownSignals,
    installGracefulShutdownHandlers,
};
