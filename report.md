# Full local backup failure on large hosted instance

## Status and scope

- Investigated: 2026-08-18 KST
- Affected production user: `qjflsek80`
- Production PocketRisu release: `v1.9.0-f9eac8f0`
- This repository HEAD: `f9eac8f077e6ef6ef62787b28831711adb9598c9`
- User-visible symptom: **Local backup** waits for a long time and then reports `Failed`.
- Client log for the observed attempt: `backup export error: 502` at
  `2026-08-18 02:16:48 KST`.

There are two related defects in this instance:

1. **Primary download failure:** full-export preparation is performed inside one HTTP
   request before response headers are sent. Preparing this user's 127,208 filesystem
   assets takes longer than the client's fixed 25-minute response-header deadline.
2. **Secondary snapshot defect:** historical chats reference remembered MCP tool calls
   whose separately stored payload rows were omitted by an older backup/import. Current
   full exports skip and report these missing rows, but automatic snapshot assembly still
   fails closed and repeatedly retries expensive work.

The first defect directly explains the failed local download. The second defect is not
the current full-export rejection condition on `f9eac8f0`, but it prevents automatic DB
snapshots and adds substantial background load.

## Executive summary

`SaveLocalBackup()` calls `NodeStorage.exportBackup()`, which sends
`GET /api/backup/export`. The client bounds acquisition of the HTTP response at
`AUTHORITATIVE_STORAGE_JOB_TIMEOUT_MS`, currently 25 minutes. Receiving a `Response`
means receiving the headers; the body may stream for longer, but the server does not set
the headers until all full-export sources have been privately pinned and the portable
database has been assembled.

For this user, the server must process 127,208 asset files totaling about 9.79 GB before
it can set headers. The current implementation processes files sequentially. Each normal
filesystem asset is copied to the private pin while hashing, synced individually, and
then the original is read a second time and hashed again. The request exceeded the
25-minute header deadline, was aborted before headers, and surfaced through Caddy as a
502.

The correct fix is to make full exports asynchronous jobs, like partial exports already
are. Preparation should run outside the lifetime of the create/status control requests;
the download request should be opened only after preparation is ready, so it can send
headers immediately. Raising the timeout alone is not a durable fix.

## Production evidence

### Observed local-backup failure

The user's `save/logs.db` contains:

```text
2026-08-18 02:16:48 KST | client | error | backup export error: 502
```

Forward-auth activity for the same user begins around `01:51:52 KST`, approximately
24 minutes 56 seconds before that error. The edge does not keep a per-path access log, so
this is a timing correlation rather than a direct `/api/backup/export` start record. It
matches the compiled client's 25-minute header deadline to within seconds.

### Affected data size

Read-only production inventory at investigation time:

```text
filesystem asset count: 127,208
filesystem asset bytes: 9,787,110,252
whole save directory:   9.7G (du -sh)
chat rows:              317 rows / 32,569,244 raw SQLite bytes
```

The July 29 backup import reported only 45,169 assets; the live instance has since grown
to 127,208 files. Testing only with the older 45k-file inventory will understate the
current failure.

### Resource failures ruled out

At investigation time:

```text
root/save filesystem free: 89G
/tmp free:                 32G
instance state:            active/running
instance restarts:         0
full-export spool files:   0 after cleanup
```

The relevant journal interval contained no `ENOSPC`, OOM kill, process restart, or stale
pin evidence. Disk admission, memory exhaustion, and an instance crash are not the root
cause of the observed attempt.

## Primary failure mechanism in the code

### Client path

1. `src/ts/drive/backuplocal.ts`
   - `SaveLocalBackup()` calls `forageStorage.exportBackup()`.
   - Only after that returns a `Response` does `streamBackupToDisk()` take ownership of
     and stream the body to StreamSaver.
2. `src/ts/storage/nodeStorage.ts`
   - `exportBackup()` uses `boundedAuthFetch(..., 'read',
     AUTHORITATIVE_STORAGE_JOB_TIMEOUT_MS)` for full exports.
   - `AUTHORITATIVE_STORAGE_JOB_TIMEOUT_MS` is
     `AUTHORITATIVE_STORAGE_PAYLOAD_MAX_TIMEOUT_MS`, currently `25 * 60_000`.
   - This bound applies while waiting for response headers. It does not matter that the
     caller would own the body after the `Response` exists because headers are not sent
     during preparation.

### Server path

`GET /api/backup/export` in `server/node/server.cjs` performs all of the following before
calling `res.setHeader(...)`:

1. `pinFullBackupState()`:
   - waits behind destructive imports and flushes pending DB work;
   - captures and validates one SQLite snapshot;
   - enumerates every filesystem asset and inlay;
   - reserves disk;
   - pins the database and every selected entry;
   - processes `plannedEntries` sequentially.
2. `copyBackupExportFile()` for every `source-file`:
   - opens and validates the source identity;
   - copies it page by page while hashing;
   - validates source/path stat identity after the copy;
   - calls `hashBackupExportFile()`, rereading and rehashing the entire source;
   - compares the two hashes;
   - calls `output.sync()` for each temporary file.
3. `buildSelfContainedBackupDatabase()` assembles the portable database.
4. Referenced MCP rows and final archive sizes are selected/preflighted.
5. Only then are `content-type`, `content-disposition`, and `content-length` set.

For a high-cardinality 9.79 GB asset tree, the sequential stat/copy/per-file-sync plus
second-hash work can exceed the fixed 25-minute header wait even when disk capacity is healthy.
When the browser aborts, `createBackupExportAbortTracker()` observes the disconnect and
the route cleans the pins without ever publishing download headers. Caddy can then
surface the failed upstream exchange as the observed 502.

## Recommended primary fix: generalized export jobs

### Do not make timeout extension the main fix

Increasing the timeout may provide a temporary operational workaround, but it preserves
all of the failure properties:

- no useful progress while a large request waits for headers;
- a larger instance can exceed the new limit;
- page/network interruption discards all preparation work;
- proxy/browser behavior still controls a long pre-header operation;
- the user cannot reliably cancel or resume observation of preparation.

### Extend the existing job protocol

The partial-export job implementation already supplies the right lifecycle under
`/api/backup/export/jobs`. Generalize it instead of creating another unrelated
protocol.

Suggested request:

```json
{
  "scope": "full",
  "target": "nodeonly",
  "jobId": "client-generated-canonical-uuid"
}
```

`target` should admit the existing contracts: `nodeonly`, `main`, and `upstream`.

Expected lifecycle:

1. `POST /api/backup/export/jobs`
   - validates auth/session, scope, target, capacity, and stable job ID;
   - reserves job ownership synchronously;
   - returns `202` immediately;
   - starts preparation after the response is committed.
2. `GET /api/backup/export/jobs/:jobId`
   - returns `creating | preparing | ready | failed | cancelled | streaming`;
   - reports a phase plus `current`, `total`, `bytes`, and `totalBytes` where known;
   - returns bounded missing-row warnings and the terminal error for failed jobs.
3. `DELETE /api/backup/export/jobs/:jobId`
   - cancels planning, pinning, database assembly, or a ready job;
   - is idempotent and retains the current cancellation-tombstone behavior.
4. `GET /api/backup/export/jobs/:jobId/download`
   - is allowed only in `ready`;
   - sets download headers immediately;
   - streams the already-prepared entries and database;
   - cleans the job after success, disconnect, cancellation, or TTL expiry.

### Avoid another complete archive copy

A full archive is about 10 GB for this user. Creating one final archive file in addition
to the private full pins would roughly double temporary payload storage.

Prefer a ready job that retains:

- the privately pinned entry files;
- the completed portable database spool;
- a finalized ordered entry manifest and `content-length`;
- warning metadata and the disk reservation.

Once database assembly and entry selection are complete, close the SQLite snapshot; all
download sources should then be file-backed and immutable within the job directory. The
download endpoint can write archive framing plus those files without an additional 10 GB
archive copy. Release the snapshot early, but keep the disk reservation until cleanup.

The existing partial job may continue using a completed archive file if desired. The
general job abstraction does not require the ready artifact representation to be the
same for partial and full jobs.

### Progress and cancellation

Add an optional progress callback to `pinFullBackupState()` and its planning/pinning
helpers. Update progress at a bounded cadence (for example every 100 files or every
250 ms), not on every page, so 127k files do not create excessive bookkeeping.

Useful phases:

```text
queued
validating-database
planning-files
reserving-disk
pinning-files
assembling-database
selecting-rows
ready
streaming
failed / cancelled
```

The client should poll status with ordinary short metadata bounds. Background
preparation should not inherit one total 25-minute HTTP deadline. Download body lifetime
should remain caller-owned as it is today.

Preserve these existing invariants:

- one active full export per session;
- the global full-pin cap (`FULL_EXPORT_MAX_ACTIVE_PINS` is currently 2);
- disk reservations across concurrent jobs and volumes;
- import-barrier ordering and a stable SQLite cut;
- no archive headers until the ready artifact is complete and validated;
- cleanup on cancel, disconnect, job failure, TTL, and process restart/orphan sweep;
- missing-row warning headers on the final download.

## Recommended performance improvements

The job protocol fixes correctness even if preparation remains slow. These optimizations
should follow or accompany it so large backups become practical.

### Remove the second full read for content-addressed assets

Most ordinary asset filenames contain their SHA-256 digest. During the copy,
`copyBackupExportFile()` already hashes the exact bytes written to the pin. For a normal
content-addressed asset:

1. copy once while hashing;
2. validate the pinned hash against the digest in the filename;
3. retain the existing source-FD/path stat checks before and after the copy;
4. reject the entry if either identity or digest validation fails.

This proves the private pin contains the expected content without rereading the source.
Keep the conservative second-source-hash path for legacy/non-content-addressed entries
where no expected digest is available.

Do not replace private copies with hardlinks unless the point-in-time and hostile
in-place mutation guarantees are re-proven. A hardlink shares an inode and is not an
immutable pin in the current tenant model.

### Do not fsync every disposable pin independently

The pin directory is temporary and a process restart invalidates the in-memory job. It is
not a durable committed object that must survive a crash. Per-file `output.sync()` across
127k files is therefore disproportionately expensive. Closing each completed file and
discarding the whole job after restart should be sufficient, provided errors remain
fail-closed. If crash-surviving ready jobs become a future requirement, design an
explicit durable job journal rather than relying on incidental per-file syncs.

### Consider bounded concurrency only after correctness

Sequential planning/pinning has high per-file overhead. Bounded concurrency may help,
especially for many small files, but it must preserve:

- cancellation;
- deterministic manifest ordering;
- bounded open file descriptors and memory;
- disk reservation correctness;
- per-entry error attribution and cleanup.

A small worker count should be benchmarked; unbounded `Promise.all()` over 127k files is
not acceptable.

## Secondary defect: legacy missing MCP payload rows

### How the inconsistency was created

Production history shows:

```text
2026-07-29 13:45:56 KST
[Backup Import] Complete: 45169 assets restored, 4063.5MB processed
release: v1.8.1-832d69bd
```

One currently referenced chat row has `updated_at = 2026-07-29 13:45:51 KST`, within
that import, and contains `<tool_call>` markers including `google_search:0`. The matching
canonical row

```text
cache/mcp-tool-calls/Z29vZ2xlX3NlYXJjaDow.json
```

does not exist. In fact, the live database has no remembered-MCP payload rows. The old
backup format restored chat text but did not preserve this separate namespace. Support
for carrying referenced remembered-tool rows in recovery copies was added later by
`fabdc14a` (`2026-07-31`, `fix: preserve MCP tool calls in recovery copies`).

The first automatic-snapshot failure occurred immediately after the instance upgraded
from v1.8.1 to v1.9.0 on `2026-08-13`:

```text
Error: Missing remembered MCP tool-call row: google_search:0
```

At investigation time this error had been emitted more than 6,400 times.

### Why full export and automatic snapshot behave differently

Release `f9eac8f0` intentionally made recovery-oriented exports tolerant:

- full download creates `createBackupMissingRowsCollector()`;
- it passes `missingRowsCollector.onMissingMcpToolCallRow` to
  `selectReferencedMcpToolCallEntries()`;
- missing payloads are skipped, logged, and reported in response headers.

Therefore, missing MCP rows are **not** the direct full-download rejection on this
release.

Automatic snapshot assembly in `createBackupAndRotate()` calls
`spoolSelfContainedBackupDatabase({ foldMcpToolCalls: true, ... })` without an
`onMissingMcpToolCallRow` callback. The encoder default throws. The snapshot is not
published, `lastBackupTime` does not advance, and subsequent eligible saves retry the
same expensive assembly after the short failure retry delay. This both removes the DB
recovery safety net and increases instance load.

### Recommended snapshot fix

Use the same recovery policy for automatic snapshots that full/server/partial exports
already use:

- provide `onMissingMcpToolCallRow` when calling
  `spoolSelfContainedBackupDatabase()`;
- preserve the visible marker and omit only the payload that is already unavailable;
- successfully publish the rest of the snapshot;
- emit one summarized/rate-limited warning per snapshot rather than one stack trace on
  every save;
- record a missing count and bounded ID list in diagnostics if useful.

Do not fabricate a payload row. The original argument/response body is absent and cannot
be reconstructed from the marker. Do not destructively rewrite chat text merely to make
snapshot validation pass. A separate explicit repair tool may offer marker removal to a
user, but tolerant recovery is the safe default.

### Tighten remembered-tool marker scanning

During a partial-export attempt, the scanner also interpreted an embedded JavaScript
source fragment containing the literal text `<tool_call>` as a remembered-tool marker.
The current scanner permits a marker body up to 64 KiB and searches arbitrary strings,
so a later separator/close sequence inside source text can create a false ID.

Harden `server/node/mcpToolCallRecovery.cjs` without breaking valid historical IDs:

- impose a realistic bounded call-ID length;
- reject whitespace, markup, controls, and multiline IDs;
- require the expected separator and a plausible tool-name field within the same marker;
- add fixtures for known IDs such as `google_search:0`, UUIDs, `call_*`, and
  `toolu_bdrk_*`;
- add a regression fixture containing JavaScript/template source with literal
  `<tool_call>` text and prove it yields no reference.

A conservative candidate ID character set is letters, digits, `_`, `-`, `.`, `:`, but
confirm all currently supported provider ID formats before making it authoritative.

## Implementation map

Primary files and symbols at `f9eac8f0`:

| Area | File | Symbols/routes |
|---|---|---|
| Local-backup UI | `src/ts/drive/backuplocal.ts` | `SaveLocalBackup`, `streamBackupToDisk`, missing-row warning helpers |
| Client protocol | `src/ts/storage/nodeStorage.ts` | `exportBackup`, `boundedAuthFetch`, `AUTHORITATIVE_STORAGE_JOB_TIMEOUT_MS` |
| Full export preparation | `server/node/server.cjs` | `pinFullBackupState`, `planFullBackupFilesystemEntries`, `copyBackupExportFile`, `hashBackupExportFile` |
| Full download route | `server/node/server.cjs` | `GET /api/backup/export`, `cleanupFullBackupState` |
| Existing job model | `server/node/server.cjs` | `/api/backup/export/jobs`, `preparePartialExportJob`, partial-job cleanup/sweep helpers |
| Missing-row policy | `server/node/server.cjs` | `createBackupMissingRowsCollector`, `selectReferencedMcpToolCallEntries`, `warnAndSkipMissingMcpToolCallRow` |
| Automatic snapshots | `server/node/server.cjs` | `createBackupAndRotate`, `spoolSelfContainedBackupDatabase` |
| Marker parsing | `server/node/mcpToolCallRecovery.cjs` | `collectMcpToolCallIds*`, `scanMcpToolCallIdsFromFile`, key encode/parse helpers |
| Client tests | `src/ts/drive/backuplocal.test.ts`, `src/ts/storage/nodeStorageAvailability.test.ts` | export/warning/timeout behavior |
| Server/compat tests | `server/node/*.test.ts`, `test/compat/*backup*` | pinning, cancellation, missing-row, and real-route coverage |

Update `docs/structure/backup-recovery.md` after changing the full-export lifecycle.

## Required tests

### Full export job

- Creating a full job returns `202` promptly even when pinning is test-gated longer than
  `AUTHORITATIVE_STORAGE_JOB_TIMEOUT_MS`.
- Polling reports monotonically sensible file/byte progress and reaches `ready`.
- The ready download returns headers promptly and streams a restorable archive.
- Node-only, main, and upstream target filtering remains unchanged.
- Missing chat/MCP warnings survive the job boundary and appear on the final download.
- Cancellation works during planning, pinning, database assembly, ready, and streaming.
- Client cancellation before the create acknowledgement is handled by the stable-ID
  cancellation tombstone.
- Two concurrent full jobs respect both per-session and global pin admission.
- Disk reservations are retained while ready and released exactly once on every terminal
  path.
- TTL and startup orphan sweep remove all pins/database spools.
- A failed job never exposes a partial download.
- A page reload can rediscover/poll a same-session job if that behavior is intentionally
  supported; otherwise the UI must clearly cancel the abandoned job.

Use injected per-file/page delays and a moderate fixture cardinality for deterministic
timeout tests. Do not make the normal suite create 127k real files or write 10 GB.

### Pin optimization

- A normal hash-named asset is copied once and validated against its filename digest.
- Tampered bytes under a hash name fail before the job becomes ready.
- Source replacement or in-place mutation during the copy fails closed.
- Legacy/non-content-addressed assets retain the stronger fallback verification.
- Cancellation during a copy releases source/output handles and removes the partial pin.

### MCP recovery

- An automatic snapshot with a referenced but missing MCP payload succeeds with a warning
  and remains restorable.
- A present valid payload is folded and restored exactly as before.
- An invalid present payload remains a hard error; do not silently bless corrupt bytes.
- Repeated saves after one degraded snapshot do not create a retry/log storm.
- Embedded source containing `<tool_call>` is not parsed as a marker.
- Valid provider IDs continue to be found by object and file scanners.

## Production acceptance criteria

After deployment, validate against `qjflsek80` without modifying chat content:

1. Full job creation returns within a few seconds.
2. Status shows progress through all 127k assets and can run beyond 25 minutes without
   failing merely because of wall time.
3. Download headers are returned immediately after the job is ready.
4. The completed archive downloads with the declared content length and can be
   inventory-validated/imported into a disposable instance.
5. Missing remembered-tool payloads are reported as warnings, not represented as
   invented data.
6. A new automatic `database/dbbackup-*` snapshot is published.
7. The journal no longer emits repeated `Missing remembered MCP tool-call row:
   google_search:0` stack traces.
8. No full/partial pin, database spool, WAL reader, or disk reservation remains after
   success/cancellation.

## Suggested delivery order

1. **Small hotfix:** make automatic snapshots tolerate and summarize missing MCP payload
   rows; tighten marker scanning if it can be done compatibly in the same patch. This
   restores snapshots and removes the retry storm.
2. **Primary product fix:** generalize export jobs to full/main/upstream preparation and
   switch `NodeStorage.exportBackup()` plus the local-backup UI to create/poll/download.
3. **Performance follow-up:** remove redundant hashing/per-file fsync for disposable
   content-addressed pins and benchmark bounded concurrency.
4. Run the production acceptance checks above before declaring the incident resolved.

## Temporary workarounds and non-fixes

- Increasing the full-export header timeout can be used temporarily, but it is not the
  final fix.
- Partial local backup may finish because it selects far fewer assets, but by design it
  excludes most character/media assets and is not a substitute for a full backup.
- An operator-level filesystem/restic copy protects server data but is not the same
  user-restorable `.bin` interchange artifact.
- Downgrading to a release that ignores the issue is unsafe and can reintroduce other
  storage-format problems.
- Manually inserting a fake `cache/mcp-tool-calls/*` row would make the backup appear
  complete while corrupting semantic history; do not do this.

## Investigation safety

The investigation was read-only. No production database, chat, asset, service, or source
file was modified. This report intentionally contains no chat text, authentication
secret, session token, or backup payload.
