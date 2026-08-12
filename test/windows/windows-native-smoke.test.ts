import { afterAll, describe, expect, test } from 'vitest'
import { randomUUID } from 'node:crypto'
import { mkdir, readdir, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { createClient } from '../compat/helpers/client.js'
import { decodeBackup } from '../compat/helpers/decode.js'
import { encodeBackup } from '../compat/helpers/encode.js'
import { decodeRisuDat } from '../compat/helpers/normalize.js'
import { createSeedBackup } from '../compat/helpers/seed.js'
import { spawnServer, type ServerHandle } from '../compat/helpers/spawnServer.js'
import pluginSaveKeysPkg from '../../server/node/plugin-storage/pluginSaveKeys.cjs'
import portablePathPkg from '../../server/node/runtime/portablePath.cjs'
import utilsPkg from '../../server/node/utils.cjs'

const { encodePluginSaveStorageKey } = pluginSaveKeysPkg as {
  encodePluginSaveStorageKey: (rawKey: string, prefix: string) => string
}
const { encodePortablePathComponent } = portablePathPkg as {
  encodePortablePathComponent: (value: string) => string
}
const { encodeRisuSaveLegacy } = utilsPkg as {
  encodeRisuSaveLegacy: (value: unknown) => Uint8Array
}

const servers: ServerHandle[] = []
const DB_BLOB_HEX = Buffer.from('database/database.bin', 'utf8').toString('hex')

afterAll(async () => {
  await Promise.allSettled(servers.map(server => server.cleanup()))
})

function createWindowsSmokeBackup(): { backup: Buffer, database: Buffer } {
  const entries = decodeBackup(createSeedBackup())
  const databaseEntry = entries.find(entry => entry.name === 'database.risudat')!
  const database = decodeRisuDat(databaseEntry.data) as Record<string, any>
  database.characters[0].chaId = 'CON'
  database.characters[0].chats[0].id = 'AUX'
  database.optimizePluginMemory = false
  database.pluginCustomStorage = { 'windows/smoke': { inline: true } }
  const encodedDatabase = Buffer.from(encodeRisuSaveLegacy(database))
  return {
    database: encodedDatabase,
    backup: encodeBackup(entries.map(entry => (
      entry.name === 'database.risudat'
        ? { ...entry, data: encodedDatabase }
        : entry
    ))),
  }
}

async function waitForSnapshot(client: Awaited<ReturnType<typeof createClient>>) {
  const deadline = Date.now() + 15_000
  while (Date.now() < deadline) {
    const response = await client.fetch('/api/db/snapshots')
    expect(response.status).toBe(200)
    const snapshots = (await response.json() as { snapshots: Array<{ key: string }> }).snapshots
    if (snapshots.length > 0) return snapshots[0]
    await new Promise(resolve => setTimeout(resolve, 50))
  }
  throw new Error('Timed out waiting for a database snapshot')
}

describe('native Windows storage smoke', () => {
  test('runs spool-backed import, snapshot, chat history, plugin transition, and export flows', async () => {
    const source = await spawnServer({
      env: { POCKETRISU_BACKUP_INTERVAL_MS: '0' },
    })
    servers.push(source)
    const client = await createClient(source.port, source.password)
    const fixture = createWindowsSmokeBackup()

    expect((await client.importBackup(fixture.backup)).ok).toBe(true)

    const saveFolder = path.join(source.cwd, 'windows save folder (smoke)')
    await mkdir(saveFolder, { recursive: true })
    await writeFile(path.join(saveFolder, DB_BLOB_HEX), fixture.database)
    const saveFolderImport = await client.fetch('/api/migrate/save-folder/execute', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ path: saveFolder }),
    })
    expect(saveFolderImport.status).toBe(200)
    expect(await saveFolderImport.json()).toMatchObject({ ok: true })

    const chatBytes = Buffer.from(encodeRisuSaveLegacy({
      id: 'AUX',
      name: 'Windows reserved-name chat',
      message: [{ role: 'user', data: 'portable history path' }],
      localLore: [],
      scriptstate: {},
      note: '',
    }))
    const chatWrite = await client.fetch('/api/chat-content/CON/0', {
      method: 'POST',
      headers: {
        'content-type': 'application/octet-stream',
        'x-chat-id': 'AUX',
      },
      body: new Uint8Array(chatBytes),
    })
    expect(chatWrite.status).toBe(200)
    const chatHistoryPath = path.join(
      source.cwd,
      'save',
      'chat-backups',
      encodePortablePathComponent('CON'),
      encodePortablePathComponent('AUX'),
    )
    expect((await readdir(chatHistoryPath)).length).toBeGreaterThan(0)

    const assetBytes = Buffer.from('Windows-reserved asset name remains in KV')
    const assetWrite = await client.fetch('/api/write', {
      method: 'POST',
      headers: {
        'content-type': 'application/octet-stream',
        'file-path': Buffer.from('assets/CON.png', 'utf8').toString('hex'),
      },
      body: new Uint8Array(assetBytes),
    })
    expect(assetWrite.status).toBe(200)
    const assetRead = await client.fetch('/api/read', {
      headers: { 'file-path': Buffer.from('assets/CON.png', 'utf8').toString('hex') },
    })
    expect(Buffer.from(await assetRead.arrayBuffer())).toEqual(assetBytes)

    const inlayBytes = Buffer.from('canonical Windows inlay path')
    const inlayPayload = Buffer.from(JSON.stringify({
      data: `data:application/octet-stream;base64,${inlayBytes.toString('base64')}`,
      ext: 'png',
      name: 'CON.png',
      type: 'image',
    }))
    const inlayWrite = await client.fetch('/api/write', {
      method: 'POST',
      headers: {
        'content-type': 'application/octet-stream',
        'file-path': Buffer.from('inlay/CON', 'utf8').toString('hex'),
      },
      body: new Uint8Array(inlayPayload),
    })
    expect(inlayWrite.status).toBe(200)
    const inlayRead = await client.fetch('/api/read', {
      headers: { 'file-path': Buffer.from('inlay/CON', 'utf8').toString('hex') },
    })
    expect(inlayRead.status).toBe(200)
    const inlayResult = JSON.parse(Buffer.from(await inlayRead.arrayBuffer()).toString('utf8')) as {
      data: string
    }
    expect(Buffer.from(inlayResult.data.slice(inlayResult.data.indexOf(',') + 1), 'base64'))
      .toEqual(inlayBytes)

    const rawKey = 'windows/smoke'
    const storageKey = encodePluginSaveStorageKey(rawKey, 'pluginsave/')
    const value = Buffer.from(JSON.stringify({ inline: true }))
    const transitionId = randomUUID()
    const generation = randomUUID()
    const begin = await client.fetch('/api/plugin-storage/transition/stage/begin', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        version: 2,
        transitionId,
        source: { optimized: false, generation: null, manifest: null },
        targetOptimized: true,
        targetGeneration: generation,
        rows: [{ rawKey, storageKey, size: value.length }],
      }),
    })
    expect(begin.status, await begin.clone().text()).toBe(200)
    const upload = await client.fetch('/api/plugin-storage/transition/stage/upload', {
      method: 'POST',
      headers: {
        'content-type': 'application/octet-stream',
        'x-plugin-storage-transition': transitionId,
        'x-plugin-storage-key': storageKey,
      },
      body: new Uint8Array(value),
    })
    expect(upload.status).toBe(200)
    const finalize = await client.fetch('/api/plugin-storage/transition/stage/finalize', {
      method: 'POST',
      headers: { 'x-plugin-storage-transition': transitionId },
    })
    expect(finalize.status).toBe(200)

    const exported = await client.exportBackup()
    const destination = await spawnServer()
    servers.push(destination)
    const destinationClient = await createClient(destination.port, destination.password)
    expect((await destinationClient.importBackup(exported)).ok).toBe(true)
    const pluginRead = await destinationClient.fetch('/api/read', {
      headers: {
        'file-path': Buffer.from(storageKey, 'utf8').toString('hex'),
        'x-plugin-storage-generation': generation,
      },
    })
    expect(pluginRead.status).toBe(200)
    expect(Buffer.from(await pluginRead.arrayBuffer())).toEqual(value)

    const snapshot = await waitForSnapshot(client)
    const restore = await client.fetch('/api/db/snapshots/restore', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ key: snapshot.key }),
    })
    expect(restore.status).toBe(200)
  }, 90_000)
})
