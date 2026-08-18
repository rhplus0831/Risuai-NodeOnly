import { createHash, randomUUID } from 'node:crypto'
import { access, mkdtemp, readdir, rm, unlink, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { afterAll, describe, expect, test } from 'vitest'
import { createClient, type RisuClient } from './helpers/client.js'
import { decodeBackup } from './helpers/decode.js'
import { encodeBackup } from './helpers/encode.js'
import { createSeedBackup } from './helpers/seed.js'
import { spawnServer, type ServerHandle } from './helpers/spawnServer.js'

type ExportJobStatus = {
  scope: string
  target: string
  state: string
  phase: string
  current: number
  total: number
  bytes: number
  totalBytes: number
  error?: string
}

const servers: ServerHandle[] = []

afterAll(async () => {
  await Promise.allSettled(servers.map(server => server.cleanup()))
})

async function createFullJob(
  client: RisuClient,
  target: 'nodeonly' | 'upstream' | 'main' = 'nodeonly',
): Promise<string> {
  const jobId = randomUUID()
  const response = await client.fetch('/api/backup/export/jobs', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ scope: 'full', target, jobId }),
  })
  expect(response.status).toBe(202)
  expect(await response.json()).toMatchObject({
    jobId,
    scope: 'full',
    target,
    state: 'preparing',
  })
  return jobId
}

async function readStatus(client: RisuClient, jobId: string): Promise<ExportJobStatus> {
  const response = await client.fetch(`/api/backup/export/jobs/${jobId}`)
  expect(response.status).toBe(200)
  return await response.json() as ExportJobStatus
}

async function waitForReady(
  client: RisuClient,
  jobId: string,
  timeoutMs = 15_000,
): Promise<ExportJobStatus> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const status = await readStatus(client, jobId)
    if (status.state === 'ready') return status
    if (status.state === 'failed' || status.state === 'cancelled') {
      throw new Error(status.error ?? `Full export job ${status.state}`)
    }
    await new Promise(resolve => setTimeout(resolve, 25))
  }
  throw new Error(`Timed out waiting for full export job ${jobId}`)
}

async function waitForTerminal(
  client: RisuClient,
  jobId: string,
): Promise<ExportJobStatus> {
  const deadline = Date.now() + 10_000
  while (Date.now() < deadline) {
    const status = await readStatus(client, jobId)
    if (status.state === 'ready' || status.state === 'failed' || status.state === 'cancelled') {
      return status
    }
    await new Promise(resolve => setTimeout(resolve, 25))
  }
  throw new Error(`Timed out waiting for terminal full export job ${jobId}`)
}

async function downloadJob(
  client: RisuClient,
  jobId: string,
): Promise<{ archive: Buffer; headers: Headers }> {
  const ready = await waitForReady(client, jobId)
  expect(ready).toMatchObject({ scope: 'full', state: 'ready', phase: 'ready' })
  expect(ready.current).toBe(ready.total)
  expect(ready.bytes).toBe(ready.totalBytes)
  const response = await client.fetch(`/api/backup/export/jobs/${jobId}/download`)
  expect(response.status).toBe(200)
  const archive = Buffer.from(await response.arrayBuffer())
  expect(Number(response.headers.get('content-length'))).toBe(archive.length)
  return { archive, headers: response.headers }
}

async function waitForNoFullPins(cwd: string): Promise<void> {
  const spoolDir = path.join(cwd, 'save', '.partial-export-spool')
  const deadline = Date.now() + 5_000
  while (Date.now() < deadline) {
    const entries = await readdir(spoolDir).catch(() => [])
    if (!entries.some(entry => entry.startsWith('.full-export-'))) return
    await new Promise(resolve => setTimeout(resolve, 20))
  }
  throw new Error('Full export job pins were not cleaned')
}

describe('full export jobs', () => {
  test.each(['nodeonly', 'upstream', 'main'] as const)(
    'prepares and streams a restorable %s target without a second archive spool',
    async target => {
      const source = await spawnServer()
      servers.push(source)
      const client = await createClient(source.port, source.password)
      expect((await client.importBackup(createSeedBackup())).ok).toBe(true)

      const jobId = await createFullJob(client, target)
      await waitForReady(client, jobId)
      expect(await readdir(path.join(source.cwd, 'save', '.partial-export-spool')))
        .not.toContain(`.partial-export-${jobId}`)
      const { archive, headers } = await downloadJob(client, jobId)
      expect(headers.get('x-risu-backup-target')).toBe(target)
      if (target === 'main') {
        expect(headers.get('x-risu-backup-omitted'))
          .toBe('drafts,remembered-mcp-tool-calls')
      }
      expect(decodeBackup(archive).some(entry => entry.name === 'database.risudat'))
        .toBe(true)

      const destination = await spawnServer()
      servers.push(destination)
      const destinationClient = await createClient(destination.port, destination.password)
      expect(await destinationClient.importBackup(archive)).toMatchObject({ ok: true })

      const consumed = await client.fetch(`/api/backup/export/jobs/${jobId}`)
      expect(consumed.status).toBe(404)
    },
    30_000,
  )

  test('preparation outlives the ready-job TTL and creation returns promptly', async () => {
    const gateDir = await mkdtemp(path.join(tmpdir(), 'risu-full-job-gate-'))
    const holdPath = path.join(gateDir, 'hold')
    const enteredPath = path.join(gateDir, 'entered')
    await writeFile(holdPath, 'hold', 'utf8')
    try {
      const source = await spawnServer({
        env: {
          POCKETRISU_TEST_FULL_EXPORT_DURING_PIN_GATE_DIR: gateDir,
          POCKETRISU_TEST_PARTIAL_EXPORT_TTL_MS: '500',
          POCKETRISU_TEST_PARTIAL_EXPORT_GC_INTERVAL_MS: '10',
        },
      })
      servers.push(source)
      const client = await createClient(source.port, source.password)
      expect((await client.importBackup(createSeedBackup())).ok).toBe(true)

      const startedAt = Date.now()
      const jobId = await createFullJob(client)
      expect(Date.now() - startedAt).toBeLessThan(2_000)

      const gateDeadline = Date.now() + 5_000
      while (Date.now() < gateDeadline) {
        try {
          await access(enteredPath)
          break
        } catch {
          await new Promise(resolve => setTimeout(resolve, 10))
        }
      }
      await access(enteredPath)
      await new Promise(resolve => setTimeout(resolve, 650))
      expect(await readStatus(client, jobId)).toMatchObject({
        state: 'preparing',
        phase: 'reserving-disk',
      })

      await unlink(holdPath)
      const { archive } = await downloadJob(client, jobId)
      expect(decodeBackup(archive).some(entry => entry.name === 'database.risudat'))
        .toBe(true)
    } finally {
      await rm(gateDir, { recursive: true, force: true })
    }
  }, 30_000)

  test('cancellation during full pinning releases admission and private files', async () => {
    const gateDir = await mkdtemp(path.join(tmpdir(), 'risu-full-job-cancel-gate-'))
    const holdPath = path.join(gateDir, 'hold')
    const enteredPath = path.join(gateDir, 'entered')
    await writeFile(holdPath, 'hold', 'utf8')
    try {
      const source = await spawnServer({
        env: { POCKETRISU_TEST_FULL_EXPORT_DURING_PIN_GATE_DIR: gateDir },
      })
      servers.push(source)
      const client = await createClient(source.port, source.password)
      expect((await client.importBackup(createSeedBackup())).ok).toBe(true)

      const jobId = await createFullJob(client)
      const gateDeadline = Date.now() + 5_000
      while (Date.now() < gateDeadline) {
        try {
          await access(enteredPath)
          break
        } catch {
          await new Promise(resolve => setTimeout(resolve, 10))
        }
      }
      await access(enteredPath)
      const cancelled = await client.fetch(`/api/backup/export/jobs/${jobId}`, {
        method: 'DELETE',
      })
      expect(cancelled.status).toBe(202)
      await cancelled.arrayBuffer()
      await waitForNoFullPins(source.cwd)
      expect((await client.fetch(`/api/backup/export/jobs/${jobId}`)).status).toBe(404)

      await unlink(holdPath)
      const replacement = await createFullJob(client)
      expect(decodeBackup((await downloadJob(client, replacement)).archive)
        .some(entry => entry.name === 'database.risudat')).toBe(true)
    } finally {
      await rm(gateDir, { recursive: true, force: true })
    }
  }, 30_000)

  test('a tampered hash-named asset fails before the job becomes downloadable', async () => {
    const source = await spawnServer()
    servers.push(source)
    const client = await createClient(source.port, source.password)
    const original = Buffer.from('content-addressed asset bytes')
    const tampered = Buffer.from(original)
    tampered[0] ^= 0xff
    const assetName = `${createHash('sha256').update(original).digest('hex')}.png`
    const seed = Buffer.concat([
      createSeedBackup(),
      encodeBackup([{ name: assetName, data: original }]),
    ])
    expect((await client.importBackup(seed)).ok).toBe(true)
    await writeFile(
      path.join(source.cwd, 'save', 'assets', assetName),
      tampered,
    )

    const jobId = await createFullJob(client)
    const status = await waitForTerminal(client, jobId)
    expect(status.state).toBe('failed')
    expect(status.error).toContain('does not match its asset name')
    expect((await client.fetch(`/api/backup/export/jobs/${jobId}/download`)).status)
      .toBe(409)
    await waitForNoFullPins(source.cwd)
  }, 30_000)

  test('full jobs enforce per-session ownership and the global pin cap', async () => {
    const gateDir = await mkdtemp(path.join(tmpdir(), 'risu-full-job-cap-gate-'))
    const holdPath = path.join(gateDir, 'hold')
    const enteredPath = path.join(gateDir, 'entered')
    await writeFile(holdPath, 'hold', 'utf8')
    try {
      const source = await spawnServer({
        env: { POCKETRISU_TEST_FULL_EXPORT_DURING_PIN_GATE_DIR: gateDir },
      })
      servers.push(source)
      const client = await createClient(source.port, source.password)
      expect((await client.importBackup(createSeedBackup())).ok).toBe(true)

      const create = (sessionId: string, jobId = randomUUID()) => client.fetch(
        '/api/backup/export/jobs',
        {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'x-session-id': sessionId,
          },
          body: JSON.stringify({ scope: 'full', target: 'nodeonly', jobId }),
        },
      )
      const firstId = randomUUID()
      expect((await create('session-a', firstId)).status).toBe(202)

      const gateDeadline = Date.now() + 5_000
      while (Date.now() < gateDeadline) {
        try {
          await access(enteredPath)
          break
        } catch {
          await new Promise(resolve => setTimeout(resolve, 10))
        }
      }
      await access(enteredPath)

      expect((await create('session-a')).status).toBe(409)
      const secondId = randomUUID()
      expect((await create('session-b', secondId)).status).toBe(202)
      expect((await create('session-c')).status).toBe(429)

      for (const [sessionId, jobId] of [
        ['session-a', firstId],
        ['session-b', secondId],
      ] as const) {
        const cancelled = await client.fetch(`/api/backup/export/jobs/${jobId}`, {
          method: 'DELETE',
          headers: { 'x-session-id': sessionId },
        })
        expect(cancelled.status).toBe(202)
        await cancelled.arrayBuffer()
      }
      await waitForNoFullPins(source.cwd)
    } finally {
      await rm(gateDir, { recursive: true, force: true })
    }
  }, 30_000)
})
