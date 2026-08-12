import { afterEach, describe, expect, test, vi } from 'vitest'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import platformFilesystemPkg from './platformFilesystem.cjs'

const {
  directoryFsyncErrorIsUnsupported,
  fsyncDirectorySync,
  hardenPrivateDescriptorSync,
  renamePublishedFile,
  renamePublishedFileSync,
  sameFileIdentity,
} = platformFilesystemPkg as {
  directoryFsyncErrorIsUnsupported: (error: NodeJS.ErrnoException, platform?: string) => boolean
  fsyncDirectorySync: (directoryPath: string, options?: { fs?: typeof fs, platform?: string }) => void
  hardenPrivateDescriptorSync: (
    descriptor: number,
    mode: number,
    options?: { fs?: typeof fs, platform?: string, stat?: fs.Stats, sync?: boolean },
  ) => boolean
  renamePublishedFile: (
    sourcePath: string,
    destinationPath: string,
    options?: { fs?: typeof import('node:fs/promises'), platform?: string, attempts?: number, delayMs?: number },
  ) => Promise<void>
  renamePublishedFileSync: (
    sourcePath: string,
    destinationPath: string,
    options?: { fs?: typeof fs, platform?: string, attempts?: number, delayMs?: number },
  ) => void
  sameFileIdentity: (left: fs.Stats, right: fs.Stats) => boolean
}

const tempDirs: string[] = []

afterEach(() => {
  vi.restoreAllMocks()
  for (const directory of tempDirs.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true })
  }
})

describe('platform filesystem capabilities', () => {
  test.each(['EACCES', 'EISDIR', 'EPERM'])(
    'classifies Windows directory fsync error %s without hiding it on POSIX',
    code => {
      const error = Object.assign(new Error(code), { code })
      expect(directoryFsyncErrorIsUnsupported(error, 'win32')).toBe(true)
      expect(directoryFsyncErrorIsUnsupported(error, 'linux')).toBe(false)
    },
  )

  test('directory fsync ignores only known unsupported platform errors', () => {
    const fsOps = Object.create(fs) as typeof fs
    fsOps.openSync = vi.fn(() => { throw Object.assign(new Error('unsupported'), { code: 'EISDIR' }) }) as any
    expect(() => fsyncDirectorySync('unused', { fs: fsOps, platform: 'win32' })).not.toThrow()
    expect(() => fsyncDirectorySync('unused', { fs: fsOps, platform: 'linux' }))
      .toThrow('unsupported')
  })

  test('POSIX mode hardening is an explicit no-op on Windows', () => {
    const fsOps = Object.create(fs) as typeof fs
    fsOps.fchmodSync = vi.fn(() => { throw new Error('must not run') }) as typeof fs.fchmodSync
    const applied = hardenPrivateDescriptorSync(123, 0o600, {
      fs: fsOps,
      platform: 'win32',
    })
    expect(applied).toBe(false)
    expect(fsOps.fchmodSync).not.toHaveBeenCalled()
  })

  test('file identity compares the opened filesystem object', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pocketrisu-platform-fs-'))
    tempDirs.push(root)
    const first = path.join(root, 'first')
    const second = path.join(root, 'second')
    fs.writeFileSync(first, 'first')
    fs.writeFileSync(second, 'second')
    expect(sameFileIdentity(fs.statSync(first), fs.statSync(first))).toBe(true)
    expect(sameFileIdentity(fs.statSync(first), fs.statSync(second))).toBe(false)
  })

  test('Windows publication retries only while the source still exists', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pocketrisu-platform-fs-'))
    tempDirs.push(root)
    const source = path.join(root, 'source')
    const destination = path.join(root, 'destination')
    fs.writeFileSync(source, 'new')
    fs.writeFileSync(destination, 'old')
    const fsOps = Object.create(fs) as typeof fs
    const renameSync = vi.fn()
      .mockImplementationOnce(() => { throw Object.assign(new Error('busy'), { code: 'EBUSY' }) })
      .mockImplementation((from, to) => fs.renameSync(from, to))
    fsOps.renameSync = renameSync as typeof fs.renameSync

    renamePublishedFileSync(source, destination, {
      fs: fsOps,
      platform: 'win32',
      delayMs: 0,
    })

    expect(renameSync).toHaveBeenCalledTimes(2)
    expect(fs.readFileSync(destination, 'utf8')).toBe('new')
  })

  test('Windows publication recognizes an operation committed before an error', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'pocketrisu-platform-fs-'))
    tempDirs.push(root)
    const source = path.join(root, 'source')
    const destination = path.join(root, 'destination')
    fs.writeFileSync(source, 'new')
    const promises = fs.promises
    const fsOps = Object.create(promises) as typeof promises
    const rename = vi.fn(async (from: fs.PathLike, to: fs.PathLike) => {
      await promises.rename(from, to)
      throw Object.assign(new Error('scanner raced completion'), { code: 'EPERM' })
    })
    fsOps.rename = rename as typeof promises.rename

    await expect(renamePublishedFile(source, destination, {
      fs: fsOps,
      platform: 'win32',
      delayMs: 0,
    })).resolves.toBeUndefined()
    expect(rename).toHaveBeenCalledTimes(1)
    expect(fs.readFileSync(destination, 'utf8')).toBe('new')
  })
})
