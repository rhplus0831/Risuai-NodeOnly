import { afterEach, describe, expect, test } from 'vitest'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import spoolOwnershipPkg from './spoolOwnership.cjs'

const {
  ensureOwnedSpoolDirSync,
  openRuntimeOwnedSpoolDirSync,
  releaseRuntimeOwnedSpoolDirSync,
  validateRuntimeOwnedSpoolDirSync,
  withQuarantinedOwnedSpoolDirSync,
} = spoolOwnershipPkg as {
  ensureOwnedSpoolDirSync: (
    root: string,
    owned: string,
    options?: { platform?: string },
  ) => string
  openRuntimeOwnedSpoolDirSync: (
    root: string,
    owned: string,
    options?: { platform?: string },
  ) => { descriptor: number | null, pinnedPath: string, cleanupPath: string | null, stat: fs.Stats }
  releaseRuntimeOwnedSpoolDirSync: (handle: any) => boolean
  validateRuntimeOwnedSpoolDirSync: (handle: any) => boolean
  withQuarantinedOwnedSpoolDirSync: (
    root: string,
    owned: string,
    sweep: (directory: string) => void,
    options?: { platform?: string },
  ) => { quarantined: boolean, swept: boolean, unsupported?: boolean }
}

const tempDirs: string[] = []

afterEach(() => {
  for (const directory of tempDirs.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true })
  }
})

describe('Windows runtime spool fallback', () => {
  test('uses a fresh per-boot child and never sweeps prior contents', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pocketrisu-windows-spool-'))
    tempDirs.push(root)
    const owned = path.join(root, '.instance-test')
    ensureOwnedSpoolDirSync(root, owned, { platform: 'win32' })

    const first = openRuntimeOwnedSpoolDirSync(root, owned, { platform: 'win32' })
    fs.writeFileSync(path.join(first.pinnedPath, '.database-risudat-stale'), 'retain')
    const sweep = withQuarantinedOwnedSpoolDirSync(
      root,
      owned,
      () => { throw new Error('Windows must not sweep a reusable pathname') },
      { platform: 'win32' },
    )
    const second = openRuntimeOwnedSpoolDirSync(root, owned, { platform: 'win32' })

    expect(sweep).toEqual({ quarantined: false, swept: false, unsupported: true })
    expect(second.pinnedPath).not.toBe(first.pinnedPath)
    expect(path.dirname(first.pinnedPath)).toBe(owned)
    expect(path.dirname(second.pinnedPath)).toBe(owned)
    expect(fs.readFileSync(path.join(first.pinnedPath, '.database-risudat-stale'), 'utf8'))
      .toBe('retain')
    expect(fs.readdirSync(second.pinnedPath)).toEqual([])
    expect(validateRuntimeOwnedSpoolDirSync(second)).toBe(true)
    expect(releaseRuntimeOwnedSpoolDirSync(second)).toBe(true)
    expect(fs.existsSync(second.pinnedPath)).toBe(false)
    expect(releaseRuntimeOwnedSpoolDirSync(first)).toBe(false)
    expect(fs.existsSync(first.pinnedPath)).toBe(true)
  })

  test('rejects a runtime pathname that was replaced after creation', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pocketrisu-windows-spool-'))
    tempDirs.push(root)
    const owned = path.join(root, '.instance-test')
    const opened = openRuntimeOwnedSpoolDirSync(root, owned, { platform: 'win32' })
    const parked = `${opened.pinnedPath}.parked`
    fs.renameSync(opened.pinnedPath, parked)
    fs.mkdirSync(opened.pinnedPath)

    expect(validateRuntimeOwnedSpoolDirSync(opened)).toBe(false)
    expect(releaseRuntimeOwnedSpoolDirSync(opened)).toBe(false)
    expect(fs.existsSync(opened.pinnedPath)).toBe(true)
    expect(fs.existsSync(parked)).toBe(true)
  })
})
