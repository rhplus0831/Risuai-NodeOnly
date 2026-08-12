import { describe, expect, it, vi } from 'vitest'
import archiveExtractionPkg from './archiveExtraction.cjs'

const {
  extractArchiveSync,
} = archiveExtractionPkg as {
  extractArchiveSync: (
    archivePath: string,
    destinationPath: string,
    options?: {
      platform?: string
      format?: 'zip' | 'tar.gz'
      execFileSync?: (...args: any[]) => unknown
      timeout?: number
      stdio?: string
    },
  ) => string
}

describe('archive extraction commands', () => {
  it('passes archive paths as literal process arguments to tar', () => {
    const execFileSync = vi.fn()
    expect(extractArchiveSync("C:\\Pocket & Risu\\update's.zip", 'C:\\out (new)', {
      platform: 'win32',
      format: 'zip',
      execFileSync,
    })).toBe('tar.exe')
    expect(execFileSync).toHaveBeenCalledWith(
      'tar.exe',
      ['-xf', "C:\\Pocket & Risu\\update's.zip", '-C', 'C:\\out (new)'],
      expect.objectContaining({ windowsHide: true }),
    )
  })

  it('uses an encoded PowerShell fallback with safely quoted literal paths', () => {
    const execFileSync = vi.fn()
      .mockImplementationOnce(() => { throw new Error('tar unavailable') })
      .mockImplementationOnce(() => undefined)
    extractArchiveSync("C:\\Pocket & Risu\\update's.zip", 'C:\\out (new)', {
      platform: 'win32',
      format: 'zip',
      execFileSync,
    })

    const [command, args] = execFileSync.mock.calls[1]
    expect(command).toBe('powershell.exe')
    const encoded = args[args.indexOf('-EncodedCommand') + 1]
    const script = Buffer.from(encoded, 'base64').toString('utf16le')
    expect(script).toContain("-LiteralPath 'C:\\Pocket & Risu\\update''s.zip'")
    expect(script).toContain("-DestinationPath 'C:\\out (new)'")
  })

  it('uses argument arrays for POSIX tar archives', () => {
    const execFileSync = vi.fn()
    extractArchiveSync('/tmp/update shell.tar.gz', '/tmp/out dir', {
      platform: 'linux',
      format: 'tar.gz',
      execFileSync,
    })
    expect(execFileSync).toHaveBeenCalledWith(
      'tar',
      ['-xzf', '/tmp/update shell.tar.gz', '-C', '/tmp/out dir'],
      expect.any(Object),
    )
  })
})
