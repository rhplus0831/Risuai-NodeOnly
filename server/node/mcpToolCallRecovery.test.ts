import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { afterEach, describe, expect, test } from 'vitest'

const {
  collectMcpToolCallIds,
  mcpToolCallStorageKey,
  parseMcpToolCallSnapshotKey,
  parseMcpToolCallStorageKey,
  scanMcpToolCallIdsFromFile,
} = require('./mcpToolCallRecovery.cjs') as {
  collectMcpToolCallIds: (value: unknown) => Set<string>
  mcpToolCallStorageKey: (callId: string) => string | null
  parseMcpToolCallSnapshotKey: (suffix: string) => { callId: string } | null
  parseMcpToolCallStorageKey: (key: string) => { callId: string; suffix: string } | null
  scanMcpToolCallIdsFromFile: (filePath: string) => Promise<Set<string>>
}

const tempDirectories: string[] = []

afterEach(async () => {
  await Promise.all(tempDirectories.splice(0).map(directory => (
    rm(directory, { recursive: true, force: true })
  )))
})

describe('MCP tool-call recovery helpers', () => {
  test('physical keys round-trip only canonical UTF-8 base64url ids', () => {
    const callId = '도구-call/1'
    const storageKey = mcpToolCallStorageKey(callId)!
    const parsed = parseMcpToolCallStorageKey(storageKey)!

    expect(parsed.callId).toBe(callId)
    expect(parseMcpToolCallSnapshotKey(parsed.suffix)?.callId).toBe(callId)
    expect(parseMcpToolCallStorageKey('cache/mcp-tool-calls/not+base64.json')).toBeNull()
    expect(parseMcpToolCallStorageKey('cache/mcp-tool-calls/../escape.json')).toBeNull()
    expect(parseMcpToolCallStorageKey('cache/other/Zm9v.json')).toBeNull()
  })

  test('collects complete markers recursively and ignores malformed text', () => {
    const ids = collectMcpToolCallIds({
      message: [{ data: '<tool_call>google_search:0\uf100lookup</tool_call>' }],
      swipes: [
        'before <tool_call>550e8400-e29b-41d4-a716-446655440000\uf100search</tool_call> after',
        '<tool_call>call_provider_1\uf100tool.name</tool_call>',
        '<tool_call>toolu_bdrk_01ABC\uf100mcp-tool</tool_call>',
      ],
      malformed: '<tool_call>missing-close\uf100lookup',
      whitespace: '<tool_call>call with spaces\uf100lookup</tool_call>',
      markup: '<tool_call>call-id\uf100<script></tool_call>',
      embeddedSource: 'const marker = `<tool_call>${callId}`;\nconst tail = "\uf100lookup</tool_call>";',
    })
    expect([...ids].sort()).toEqual([
      '550e8400-e29b-41d4-a716-446655440000',
      'call_provider_1',
      'google_search:0',
      'toolu_bdrk_01ABC',
    ])
  })

  test('streaming scan retains markers split across file pages', async () => {
    const directory = await mkdtemp(path.join(tmpdir(), 'mcp-recovery-test-'))
    tempDirectories.push(directory)
    const filePath = path.join(directory, 'database.risudat')
    const marker = '<tool_call>call-cross-page\uf100lookup</tool_call>'
    await writeFile(filePath, Buffer.concat([
      Buffer.alloc(64 * 1024 - 5, 0x61),
      Buffer.from(marker, 'utf8'),
      Buffer.from('<tool_call>incomplete\uf100lookup', 'utf8'),
    ]))

    expect([...await scanMcpToolCallIdsFromFile(filePath)]).toEqual(['call-cross-page'])
  })

  test('streaming scan rejects source fragments with literal marker syntax', async () => {
    const directory = await mkdtemp(path.join(tmpdir(), 'mcp-recovery-source-test-'))
    tempDirectories.push(directory)
    const filePath = path.join(directory, 'database.risudat')
    await writeFile(filePath, [
      'const open = "<tool_call>";',
      'const template = `${providerCall.id}`;',
      'const close = "\uf100lookup</tool_call>";',
      '<tool_call>call-valid:1\uf100lookup</tool_call>',
    ].join('\n'))

    expect([...await scanMcpToolCallIdsFromFile(filePath)])
      .toEqual(['call-valid:1'])
  })
})
