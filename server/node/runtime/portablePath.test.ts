import { describe, expect, test } from 'vitest'
import portablePathPkg from './portablePath.cjs'

const {
  PORTABLE_PATH_ESCAPE_PREFIX,
  decodePortablePathComponent,
  encodeLegacyPathComponent,
  encodePortablePathComponent,
  isPortableWindowsFileName,
  portableFilenameKey,
} = portablePathPkg as {
  PORTABLE_PATH_ESCAPE_PREFIX: string
  decodePortablePathComponent: (value: string) => string | null
  encodeLegacyPathComponent: (value: string) => string
  encodePortablePathComponent: (value: string) => string
  isPortableWindowsFileName: (value: string) => boolean
  portableFilenameKey: (value: string) => string
}

describe('portable filesystem names', () => {
  test.each([
    'CON',
    'con.txt',
    'NUL.tar.gz',
    'COM1',
    'COM¹.log',
    'LPT9',
    'CONIN$',
    'conout$.txt',
    'trailing.',
    'trailing ',
  ])('rejects Windows-incompatible filename %s', name => {
    expect(isPortableWindowsFileName(name)).toBe(false)
  })

  test.each(['normal', 'name.ext', '.hidden', 'unicode-한글'])('accepts portable filename %s', name => {
    expect(isPortableWindowsFileName(name)).toBe(true)
  })

  test.each(['CON', 'CON.txt', 'chat.', 'normal', '.', '..', '한글'])(
    'path component %s round-trips without producing a Windows-reserved name',
    logical => {
      const encoded = encodePortablePathComponent(logical)
      expect(isPortableWindowsFileName(encoded)).toBe(true)
      expect(decodePortablePathComponent(encoded)).toBe(logical)
      if (isPortableWindowsFileName(encodeLegacyPathComponent(logical))
        && !/[A-Z]/.test(encodeLegacyPathComponent(logical))) {
        expect(encoded).toBe(encodeLegacyPathComponent(logical))
      } else {
        expect(encoded.startsWith(PORTABLE_PATH_ESCAPE_PREFIX)).toBe(true)
      }
    },
  )

  test('portable identity folds case and ignored trailing punctuation', () => {
    expect(portableFilenameKey('Example... ')).toBe('example')
  })

  test('case-sensitive logical IDs cannot collide on a Windows filesystem', () => {
    const upper = encodePortablePathComponent('Case')
    const lower = encodePortablePathComponent('case')
    expect(upper.toLowerCase()).not.toBe(lower.toLowerCase())
    expect(decodePortablePathComponent(upper)).toBe('Case')
    expect(decodePortablePathComponent(lower)).toBe('case')
  })
})
