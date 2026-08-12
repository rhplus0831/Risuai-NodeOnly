import { describe, expect, it } from 'vitest'
import selfUpdatePkg from './selfUpdate.cjs'

const { quoteWindowsBatchArgument } = selfUpdatePkg as {
  quoteWindowsBatchArgument: (value: string) => string
}

describe('Windows self-update finalizer', () => {
  it('quotes paths and prevents percent expansion in generated batch files', () => {
    expect(quoteWindowsBatchArgument('C:\\Pocket Risu\\100% ready\\node.exe'))
      .toBe('"C:\\Pocket Risu\\100%% ready\\node.exe"')
  })
})
