import { describe, expect, test } from 'vitest'
import { createClient } from './helpers/client.js'
import { createSeedBackup } from './helpers/seed.js'
import { spawnServer } from './helpers/spawnServer.js'

describe('observability and database-maintenance routes', () => {
  test('round-trips masked client logs and clears them', async () => {
    const server = await spawnServer()
    try {
      const client = await createClient(server.port, server.password)
      const timestamp = 1_725_000_000_123
      const posted = await client.fetch('/api/logs', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify([{
          timestamp,
          level: 'warning',
          message: 'request failed: Bearer super-secret-token-1234567890',
          description: '{"authorization":"Bearer another-secret-token-1234567890"}',
          source: 'compat-observability',
          count: 2,
          platform: 'compat-platform',
          clientId: 'compat-client',
          userAgent: 'compat-agent',
        }]),
      })
      expect(posted.status).toBe(200)
      await expect(posted.json()).resolves.toEqual({ success: true, written: 1 })

      const listed = await client.fetch('/api/logs?origin=client')
      expect(listed.status).toBe(200)
      const body = await listed.json() as {
        success: boolean
        content: Array<Record<string, unknown>>
        total: number
      }
      expect(body).toEqual({
        success: true,
        content: [{
          id: expect.any(Number),
          timestamp,
          level: 'warning',
          origin: 'client',
          message: 'request failed: Bearer [REDACTED_TOKEN]',
          description: '{"authorization":"[REDACTED_TOKEN]"}',
          source: 'compat-observability',
          count: 2,
          platform: 'compat-platform',
          clientId: 'compat-client',
          userAgent: 'compat-agent',
        }],
        total: 1,
      })

      const cleared = await client.fetch('/api/logs', { method: 'DELETE' })
      expect(cleared.status).toBe(200)
      await expect(cleared.json()).resolves.toEqual({ success: true })

      const empty = await client.fetch('/api/logs?origin=client')
      expect(empty.status).toBe(200)
      await expect(empty.json()).resolves.toEqual({
        success: true,
        content: [],
        total: 0,
      })
    } finally {
      await server.cleanup()
    }
  })

  test('runs an authenticated manual WAL checkpoint', async () => {
    const server = await spawnServer()
    try {
      const client = await createClient(server.port, server.password)
      const response = await client.fetch('/api/db/wal-checkpoint', { method: 'POST' })
      expect(response.status).toBe(200)
      expect(await response.json()).toEqual({
        ok: true,
        checkpoint: {
          mode: 'TRUNCATE',
          reason: 'manual-cleanup',
          complete: true,
          busy: 0,
          logFrames: expect.any(Number),
          checkpointedFrames: expect.any(Number),
          attemptedAt: expect.any(Number),
        },
        elapsedMs: expect.any(Number),
        preWalSize: expect.any(Number),
        postWalSize: expect.any(Number),
        reclaimed: expect.any(Number),
      })
    } finally {
      await server.cleanup()
    }
  })

  test('reports authenticated save-volume capacity with the current shape', async () => {
    const server = await spawnServer()
    try {
      const client = await createClient(server.port, server.password)
      const response = await client.fetch('/api/storage/capacity')
      expect(response.status).toBe(200)
      const body = await response.json() as {
        success: boolean
        freeBytes: number | null
      }
      expect(body.success).toBe(true)
      expect(body).toHaveProperty('freeBytes')
      if (body.freeBytes !== null) {
        expect(typeof body.freeBytes).toBe('number')
        expect(Number.isSafeInteger(body.freeBytes)).toBe(true)
        expect(body.freeBytes).toBeGreaterThanOrEqual(0)
      }
    } finally {
      await server.cleanup()
    }
  })

  test('deletes a server backup and reports a repeated delete as missing', async () => {
    const server = await spawnServer()
    try {
      const client = await createClient(server.port, server.password)
      expect((await client.importBackup(createSeedBackup())).ok).toBe(true)

      const saved = await client.fetch('/api/backup/server/save', { method: 'POST' })
      expect(saved.status).toBe(200)
      const events = (await saved.text()).trim().split('\n').map(line => JSON.parse(line))
      const done = events.find(event => event.type === 'done') as { filename?: string } | undefined
      expect(done?.filename).toMatch(/^risu-backup-\d+\.bin$/)
      const filename = done!.filename!

      const listed = await client.fetch('/api/backup/server/list')
      expect(listed.status).toBe(200)
      await expect(listed.json()).resolves.toEqual({
        backups: [{
          filename,
          size: expect.any(Number),
          createdAt: expect.any(Number),
        }],
      })

      const deleted = await client.fetch(`/api/backup/server/${filename}`, { method: 'DELETE' })
      expect(deleted.status).toBe(200)
      await expect(deleted.json()).resolves.toEqual({ ok: true })

      const empty = await client.fetch('/api/backup/server/list')
      expect(empty.status).toBe(200)
      await expect(empty.json()).resolves.toEqual({ backups: [] })

      const deletedAgain = await client.fetch(`/api/backup/server/${filename}`, { method: 'DELETE' })
      expect(deletedAgain.status).toBe(404)
      await expect(deletedAgain.json()).resolves.toEqual({ error: 'Backup file not found' })
    } finally {
      await server.cleanup()
    }
  })
})
