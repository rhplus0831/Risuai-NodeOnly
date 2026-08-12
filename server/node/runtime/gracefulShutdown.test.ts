import { describe, expect, it, vi } from 'vitest'
import gracefulShutdownPkg from './gracefulShutdown.cjs'

const {
  createIdempotentSignalHandler,
  gracefulShutdownSignals,
  installGracefulShutdownHandlers,
} = gracefulShutdownPkg as {
  createIdempotentSignalHandler: (
    shutdown: (signal: string) => Promise<void> | void,
    options?: { onError?: (error: unknown, signal: string) => void },
  ) => (signal: string) => Promise<void>
  gracefulShutdownSignals: (platform?: string) => string[]
  installGracefulShutdownHandlers: (
    processLike: { on: (signal: string, listener: () => void) => void },
    shutdown: (signal: string) => Promise<void> | void,
    options?: { platform?: string, signals?: string[] },
  ) => (signal: string) => Promise<void>
}

describe('graceful shutdown portability', () => {
  it('includes Windows console-break handling without registering it on POSIX', () => {
    expect(gracefulShutdownSignals('win32')).toContain('SIGBREAK')
    expect(gracefulShutdownSignals('linux')).not.toContain('SIGBREAK')
    expect(gracefulShutdownSignals('win32')).toContain('SIGHUP')
  })

  it('runs one shutdown sequence when signals overlap', async () => {
    let release!: () => void
    const pending = new Promise<void>(resolve => { release = resolve })
    const shutdown = vi.fn(() => pending)
    const handler = createIdempotentSignalHandler(shutdown)

    const first = handler('SIGINT')
    const second = handler('SIGTERM')
    release()
    await Promise.all([first, second])

    expect(shutdown).toHaveBeenCalledTimes(1)
    expect(shutdown).toHaveBeenCalledWith('SIGINT')
  })

  it('installs the platform signal set through the supplied process adapter', () => {
    const on = vi.fn()
    installGracefulShutdownHandlers({ on }, () => {}, { platform: 'win32' })
    expect(on.mock.calls.map(([signal]) => signal)).toEqual([
      'SIGTERM',
      'SIGINT',
      'SIGHUP',
      'SIGBREAK',
    ])
  })
})
