import { createServer, type Server } from 'node:http'
import { fetch as httpFetch } from 'undici'
import { describe, expect, test } from 'vitest'
import WebSocket from 'ws'
import { createClient } from './helpers/client.js'
import { spawnServer } from './helpers/spawnServer.js'

const HOSTED_PROXY_STREAM_BLOCKED_ERROR =
  'PROXY_TARGET_BLOCKED: Local proxy stream jobs are disabled in hosted mode'

interface ProxyJobEvent {
  type: string
  jobId?: string
  status?: number
  headers?: Record<string, string>
  dataBase64?: string
  message?: string
}

async function listenTarget(
  handler: Parameters<typeof createServer>[0],
): Promise<{ server: Server; url: string }> {
  const server = createServer(handler)
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => {
      server.off('error', reject)
      resolve()
    })
  })
  const address = server.address()
  if (!address || typeof address === 'string') {
    throw new Error('Target server did not expose a TCP port')
  }
  return { server, url: `http://127.0.0.1:${address.port}` }
}

async function closeTarget(server: Server): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close(error => error ? reject(error) : resolve())
  })
}

async function collectProxyJobEvents(url: string): Promise<{
  events: ProxyJobEvent[]
  socket: WebSocket
}> {
  const events: ProxyJobEvent[] = []
  const socket = new WebSocket(url)
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error('Timed out waiting for proxy stream events')),
      5_000,
    )
    const fail = (error: Error) => {
      clearTimeout(timeout)
      reject(error)
    }
    socket.once('error', fail)
    socket.on('message', data => {
      const event = JSON.parse(data.toString()) as ProxyJobEvent
      events.push(event)
      if (event.type === 'error') {
        fail(new Error(`Unexpected proxy stream error: ${event.status} ${event.message}`))
      }
      if (event.type === 'done') {
        clearTimeout(timeout)
        socket.off('error', fail)
        resolve()
      }
    })
  })
  return { events, socket }
}

async function websocketUpgradeStatus(url: string): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    const socket = new WebSocket(url)
    const timeout = setTimeout(() => {
      socket.terminate()
      reject(new Error('Timed out waiting for WebSocket upgrade rejection'))
    }, 5_000)
    socket.once('unexpected-response', (_request, response) => {
      clearTimeout(timeout)
      response.resume()
      resolve(response.statusCode ?? 0)
    })
    socket.once('open', () => {
      clearTimeout(timeout)
      socket.close()
      reject(new Error('Deleted proxy job unexpectedly accepted a WebSocket'))
    })
    socket.once('error', error => {
      clearTimeout(timeout)
      reject(error)
    })
  })
}

describe('proxy routes', () => {
  test('rejects proxy-stream job creation without auth', async () => {
    const server = await spawnServer()
    try {
      const response = await httpFetch(`http://127.0.0.1:${server.port}/proxy-stream-jobs`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ url: 'http://127.0.0.1:1', method: 'GET' }),
      })

      expect(response.status).toBe(400)
      await expect(response.json()).resolves.toEqual({ error: 'No auth header' })
    } finally {
      await server.cleanup()
    }
  })

  test('blocks proxy-stream job creation in hub hosting mode', async () => {
    const server = await spawnServer({
      env: { POCKETRISU_HUB_HOSTING: 'true' },
      createBackupsDir: false,
    })
    try {
      const client = await createClient(server.port, server.password)
      const response = await client.fetch('/proxy-stream-jobs', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ url: 'http://127.0.0.1:1', method: 'GET' }),
      })

      expect(response.status).toBe(403)
      await expect(response.json()).resolves.toEqual({
        error: HOSTED_PROXY_STREAM_BLOCKED_ERROR,
      })
    } finally {
      await server.cleanup()
    }
  })

  test('streams a local target over WebSocket and deletes the job', async () => {
    const target = await listenTarget((request, response) => {
      const chunks: Buffer[] = []
      request.on('data', chunk => chunks.push(Buffer.from(chunk)))
      request.on('end', () => {
        response.writeHead(207, {
          'content-type': 'text/plain',
          'x-proxy-fixture': 'stream-job',
        })
        response.write('first:')
        setImmediate(() => response.end(`${Buffer.concat(chunks).toString('utf8')}:last`))
      })
    })
    const server = await spawnServer()
    try {
      const client = await createClient(server.port, server.password)
      const body = Buffer.from('proxy-stream-body', 'utf8')
      const created = await client.fetch('/proxy-stream-jobs', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          url: `${target.url}/stream`,
          method: 'POST',
          headers: { 'content-type': 'text/plain', 'x-fixture-request': 'stream-job' },
          bodyBase64: body.toString('base64'),
          timeoutMs: 5_000,
          heartbeatSec: 5,
        }),
      })
      expect(created.status).toBe(200)
      const { jobId, heartbeatSec } = await created.json() as {
        jobId: string
        heartbeatSec: number
      }
      expect(jobId).toMatch(/^[0-9a-f-]{36}$/)
      expect(heartbeatSec).toBe(5)

      const wsUrl = `ws://127.0.0.1:${server.port}`
        + `/proxy-stream-jobs/${encodeURIComponent(jobId)}/ws`
        + `?risu-auth=${encodeURIComponent(client.token)}`
      const { events, socket } = await collectProxyJobEvents(wsUrl)
      expect(events[0]).toEqual({ type: 'job_accepted', jobId })
      expect(events.find(event => event.type === 'upstream_headers')).toMatchObject({
        type: 'upstream_headers',
        status: 207,
        headers: { 'x-proxy-fixture': 'stream-job' },
      })
      const responseBody = Buffer.concat(events
        .filter(event => event.type === 'chunk')
        .map(event => Buffer.from(event.dataBase64 ?? '', 'base64')))
        .toString('utf8')
      expect(responseBody).toBe('first:proxy-stream-body:last')
      expect(events.at(-1)).toEqual({ type: 'done' })

      const closed = new Promise<void>(resolve => socket.once('close', () => resolve()))
      const deleted = await client.fetch(`/proxy-stream-jobs/${jobId}`, { method: 'DELETE' })
      expect(deleted.status).toBe(200)
      await expect(deleted.json()).resolves.toEqual({ success: true })
      await closed
      await expect(websocketUpgradeStatus(wsUrl)).resolves.toBe(404)
    } finally {
      await server.cleanup()
      await closeTarget(target.server)
    }
  })

  test('forwards an authenticated POST /proxy2 request to loopback in normal mode', async () => {
    const target = await listenTarget((request, response) => {
      const chunks: Buffer[] = []
      request.on('data', chunk => chunks.push(Buffer.from(chunk)))
      request.on('end', () => {
        response.writeHead(202, { 'content-type': 'application/json' })
        response.end(JSON.stringify({
          method: request.method,
          body: Buffer.concat(chunks).toString('utf8'),
          fixtureHeader: request.headers['x-proxy-fixture'],
        }))
      })
    })
    const server = await spawnServer()
    try {
      const client = await createClient(server.port, server.password)
      const response = await client.fetch('/proxy2', {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'risu-url': encodeURIComponent(`${target.url}/reverse`),
          'risu-header': encodeURIComponent(JSON.stringify({
            'content-type': 'application/json',
            'x-proxy-fixture': 'proxy2',
          })),
        },
        body: JSON.stringify({ hello: 'proxy2' }),
      })

      expect(response.status).toBe(202)
      await expect(response.json()).resolves.toEqual({
        method: 'POST',
        body: JSON.stringify({ hello: 'proxy2' }),
        fixtureHeader: 'proxy2',
      })
    } finally {
      await server.cleanup()
      await closeTarget(target.server)
    }
  })
})
