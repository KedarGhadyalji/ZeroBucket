# zerobucket (npm)

**Database-native image storage for Node.js. Your database. Your images. Zero buckets.**

Store small-to-medium image collections (avatars, product photos, uploads) as ordinary
Postgres rows, with validation, optimization, streaming, range requests and access
control built in, and no S3 account to provision for the common case.

This is the Node.js/TypeScript sibling of the [Python package](https://pypi.org/project/zerobucket/).
**It uses the exact same table**, so a Node service and a Python service can share one database
and read each other's images.

> **Status: early (0.8.x).** PostgreSQL, SQLite **and MySQL/MariaDB**, each in classic and dedup modes, with
> S3-compatible object-storage tiering, HEIC (iPhone photo) support and a `zerobucket` command line tool.

## Install

```bash
npm install zerobucket pg sharp          # PostgreSQL
npm install zerobucket better-sqlite3 sharp   # or SQLite (see below): no database server at all
npm install zerobucket mysql2 sharp            # or MySQL / MariaDB (see below)
npm install libheif-js                         # optional: accept HEIC (iPhone) photos, see below
```

`pg` (the Postgres driver), `better-sqlite3` (SQLite), `mysql2` (MySQL/MariaDB) and `sharp` (image decoding/optimizing) are
peer dependencies, so you control their versions. If you only store PDFs through a custom validator, `sharp` is not needed.
`sharp` must be **0.35.5 or newer**: older versions bundle libvips/libheif/librsvg builds with published high-severity
advisories, and this package decodes untrusted uploads with it. A plain `npm install sharp` already gets a fixed version;
if you pinned an older one, upgrade with `npm install sharp@latest`.
`mysql2` must be **3.23.1 or newer** for the same reason: every earlier release has a published advisory (the oldest range
includes critical ones), and 3.23.1 is the first that `npm audit` reports clean.
For [object-storage tiering](#object-storage-tiering) also install `@aws-sdk/client-s3` (optional; loaded only when used).
Requires Node 20+ (SQLite needs Node 22+, see below). Ships ESM and CommonJS with full types.

## Quick start

```ts
import { ZeroBucket } from "zerobucket";

const images = await ZeroBucket.connect({
  connectionString: process.env.DATABASE_URL,
});
// connect() runs the migration up front, so a bad DATABASE_URL fails at startup, not on the first request.
// `new ZeroBucket({...})` also works and connects lazily on first use.

const id = await images.put(fileBuffer, { filename: "avatar.jpg" }); // returns a UUID
const image = await images.get(id); // { data: Buffer, mimeType, width, height, checksumSha256, ... }
const meta = await images.metadata(id); // no bytes pulled
await images.delete(id);
```

The format is detected from the **bytes** (decoded with libvips), never from the filename or a
client-supplied MIME type. Truncated files, decompression bombs (`maxPixels`), and oversized
uploads (`maxBytes`, default 8 MiB) are rejected with specific error classes.

## Serve images over HTTP (full-stack use)

`zerobucket/http` turns a bucket into a standard `(Request) => Response` handler with
**ETag / 304, Range / 206, If-Range, 416, HEAD**, streamed from the database in chunks.

### Next.js (App Router)

```ts
// app/images/[id]/route.ts
import { createImageHandler } from "zerobucket/http";
import { images } from "@/lib/images";

const handler = createImageHandler(images, {
  getContext: async (req) => ({ userId: await currentUserId(req) }), // for your beforeGet hook
});

export async function GET(
  req: Request,
  { params }: { params: Promise<{ id: string }> },
) {
  return handler(req, { id: (await params).id });
}
export { GET as HEAD };
```

Then `<img src="/images/{id}">`. Browsers get proper caching and video-style seeking for free.

### Express / Fastify / `node:http`

```ts
import { createImageHandler, toNodeHandler } from "zerobucket/http";
const serve = toNodeHandler(createImageHandler(images));
app.get("/images/:id", (req, res) => serve(req, res, { id: req.params.id }));
```

### Uploads

```ts
// Next.js route handler
export async function POST(req: Request) {
  const form = await req.formData();
  const file = form.get("file") as File; // File/Blob accepted directly; size-checked before reading
  const id = await images.put(file, {
    optimize: { maxWidth: 1600, format: "webp" },
  });
  return Response.json({ id });
}
```

`put()` accepts a `Buffer`/`Uint8Array`, a file **path** string, a `Blob`/`File`, or a Node/web
stream (read in bounded chunks and rejected as soon as it passes `maxBytes`).

**Cache-Control** defaults to `private, no-cache` (always revalidate; the ETag makes that a cheap
304), which is safe for access-controlled images. For public images pass
`cacheControl: "public, max-age=31536000, immutable"`.

## Access control

```ts
const images = new ZeroBucket<{ userId: string }>({
  connectionString,
  beforeGet: async (imageId, ctx) => (await owner(imageId)) === ctx?.userId,
  beforePut: (ctx) => !!ctx?.userId,
});
await images.get(id, { context: { userId } }); // throws AccessDeniedError if the hook returns false
```

**A hook that throws fails closed**: the original exception propagates; it is never treated as
"allow". Denied calls never reach the database. `exists()` is deliberately not gated.

**Your hook receives the id exactly as the caller supplied it**, before ZeroBucket validates it, so it can be a
malformed string such as one taken straight from a URL. Check the format before using it in SQL (for example against a
UUID pattern, returning `true` for a malformed id so ZeroBucket answers "not found"); otherwise a request for
`/images/garbage` becomes a database error and a `500`. The [example app](https://github.com/KedarGhadyalji/ZeroBucket/tree/main/examples/node-integration)
shows a working hook, plus transactional uploads and serving.

## Streaming and ranges

```ts
for await (const chunk of await images.getStream(id, { chunkSize: 256 * 1024 })) { ... }
await images.streamTo(id, res);                                  // honours backpressure
const web = await images.toWebStream(id);                        // → new Response(web)
await images.getStream(id, { range: { start: 0, end: 1023 } });  // server-side byte range
```

Not-found is checked eagerly (the promise rejects before any chunk). A row deleted mid-stream makes
iteration throw `StorageError` rather than silently truncating. Pass your own transaction as
`connection` for a snapshot-consistent read. This lowers **Node-side** memory; Postgres still
handles the whole `BYTEA` value per chunk request.

## Transactions

Pass any `pg` client to make a call part of **your** transaction. ZeroBucket then does not commit,
roll back or retry:

```ts
const client = await pool.connect();
try {
  await client.query("BEGIN");
  const avatarId = await images.put(buf, { connection: client });
  await client.query("UPDATE users SET avatar_id = $1 WHERE id = $2", [
    avatarId,
    userId,
  ]);
  await client.query("COMMIT"); // row + image commit (or roll back) together
} catch (e) {
  await client.query("ROLLBACK");
  throw e;
} finally {
  client.release();
}
```

Multi-statement operations (`putMany`, dedup `put`/`delete`, tiering) are **atomic even on a bare `pg.Client`
with no open transaction**: ZeroBucket opens its own, or uses a `SAVEPOINT` inside yours so a failure rolls back only
its own work and leaves your transaction usable.

## Batches

`putMany` / `getMany` / `deleteMany` are best-effort per item (one bad file doesn't abort the rest):
check each result's `success` / `error`. `putMany` inserts all valid items in one transaction.

## Optimize pipeline, custom validators

```ts
await images.put(buf, {
  optimize: { maxWidth: 1200, format: "webp", quality: 80 },
});

import { PDFValidator } from "zerobucket";
await images.put(pdfBuf, { validator: new PDFValidator() }); // any ContentValidator works
```

Optimize strips metadata, applies EXIF orientation first, never upscales, and re-validates its own
output. Implement `ContentValidator` (`validate(data, { maxBytes })`) to store other content types.

## HEIC (iPhone photos)

iPhones upload HEIC by default, and most validators reject it. With the optional `libheif-js` installed, HEIC/HEIF
is accepted like any other format: validated by really decoding it, stored **as uploaded** (`image/heic`), and
convertible with `optimize`.

```bash
npm install libheif-js
```

```ts
const id = await images.put(iphonePhoto); // stored unchanged, mimeType "image/heic"
const id2 = await images.put(iphonePhoto, {
  optimize: { format: "webp", maxWidth: 1600 },
}); // convert on the way in
```

- **Convert for the web.** HEIC is only displayable in Safari. Serve a web format by uploading with
  `optimize: { format: "webp" | "jpeg" | "png" }`, as above. HEIC orientation is applied during conversion.
- **Decode only, by design.** There is no HEVC _encoder_ for Node, so `optimize` can convert **from** HEIC but not **to** it:
  `optimize: { format: "heic" }` throws a clear error (Python's `pillow-heif` can encode), and `optimize: true` on a HEIC
  needs an explicit target format instead of silently choosing one.
- **Never blocks your server.** Decoding is ~2 s of CPU for a 12-megapixel photo. It runs in a dedicated worker thread, so
  other requests keep being served (tested), and the worker never keeps your process from exiting.
- **Safe on hostile input.** Pixel bombs are refused from the file header _before_ any decoding (`maxPixels`), truncated or
  corrupt files are rejected with the decoder's own diagnosis, and a missing `libheif-js` is reported as "install it", never as "corrupt image".
- Recognised by content (`ftyp` brands `heic heix hevc heim heis mif1 msf1`), never by file extension: same rules as the Python package, and both
  accept and reject the same files (checked by the conformance harness against `pillow-heif`).
- **License:** `libheif-js` is LGPL-3.0. It is an optional peer dependency that **you** install; it is not bundled with or redistributed by
  this MIT-licensed package. If you use it, review its license for your distribution model.
- If a file contains several images, the first top-level image is used.

## Command line

The package installs a `zerobucket` command (`npx zerobucket ...`, or install globally with `npm i -g zerobucket`).
It has the same commands, flags, messages and exit codes as the Python package's CLI, and additionally works with
**SQLite**, **MySQL/MariaDB** (a `mysql://` or `mariadb://` `--database-url`, or `ZEROBUCKET_DATABASE_URL`) and **dedup
mode**, and understands images that were tiered to object storage. On MySQL the on-disk size is read from
`information_schema` and is approximate (MySQL 8 caches those statistics for up to 24 hours).

```bash
export ZEROBUCKET_DATABASE_URL=postgresql://user:pass@localhost/mydb     # or: --database-url URL
# MySQL/MariaDB:   export ZEROBUCKET_DATABASE_URL=mysql://user:pass@localhost:3306/mydb   # mariadb:// works too
# SQLite instead:  export ZEROBUCKET_SQLITE_PATH=./images.db              # or: --sqlite PATH

zerobucket init                 # create the schema (add --dedup for the dedup tables)
zerobucket info                 # image count, sizes, on-disk size, breakdown by format (+ dedup savings / tiered count)
zerobucket verify               # re-checksum every image; exit 1 and list the ids if anything is corrupted
zerobucket verify --sample 200  # ...or just a random sample (good for a nightly cron job)
zerobucket tier IMAGE_ID --bucket my-images                         # move one image to S3
zerobucket tier --older-than 90 --min-size 500000 --bucket my-images --dry-run   # preview a bulk move
zerobucket tier --all --limit 1000 --bucket my-images --endpoint-url https://<acct>.r2.cloudflarestorage.com
zerobucket migrate --to-dedup   # PostgreSQL: copy classic images into the dedup tables (non-destructive, re-runnable)
zerobucket migrate --to-dedup --skip-tiered   # ...skipping images tiered to object storage instead of refusing
```

- **Exit codes:** `0` success, `1` problems found or an operation failed, `2` usage or connection error. That makes
  `zerobucket verify && deploy` and cron/CI checks work as you would hope.
- `info` and `verify` never create anything: looking at a database that does not exist (or has no schema yet) reports it
  and exits 1 instead of silently creating an empty one.
- `verify` checksums one image at a time (memory stays flat on big tables). Images tiered to object storage are
  skipped with a note unless you also pass `--bucket` (and credentials), in which case they are downloaded and verified too.
- `tier` processes images one at a time and reports each failure; a failed upload leaves that image untouched in the database.
  Credentials come from the AWS SDK's standard chain, or `--aws-access-key-id` / `--aws-secret-access-key`.
- `tier` is not available with `--dedup`. `tier`, and `verify --bucket`, need `@aws-sdk/client-s3`; every other command works without it.
- The same operations are available from code: `new ZeroBucket({...}).tierToObjectStorage(id)`, `migrateClassicToDedup()`, etc.

## Reliability and observability

- Pooling: `poolMinSize` (1), `poolMaxSize` (5), `poolTimeoutMs` (10000), or pass your own `pool`
  (e.g. a tiny pool for serverless).
- Automatic retry (`maxRetries` 3, `retryBaseDelayMs` 100, exponential backoff with jitter) for
  transient errors only (deadlocks, serialization failures, connection loss). A retry **re-runs the
  whole transaction** on a fresh connection; permanent errors are never retried.
  Caveat shared with the Python package: if a connection drops _after_ the server committed a plain
  `put()` but before the reply arrived, a retry can store a duplicate row.
- `onOperation(event)` receives `{ operation, durationMs, success, error, retryCount }` for every
  operation (and once per chunk when streaming). Exceptions in your callback are swallowed.
- The schema migration is serialized with an advisory lock, so many processes starting at once is safe.

## SQLite

No database server at all: one file. Ideal for small apps, single-server deployments, desktop/Electron apps, tests and prototypes.

```ts
const images = new ZeroBucket({ sqlite: "./images.db" }); // npm install better-sqlite3
// ZeroBucket({ sqlite: ":memory:" }) for a throwaway in-process database
```

Everything in this README works on SQLite, **including dedup (`dedup: true`), object-storage tiering, streaming and
byte ranges, hooks, the HTTP handler, optimize and validators**. The file format is identical to the Python
package's `SQLiteBackend`, so a Node app and a Python app can share one `.db` file (verified in both directions, including
tiering and dedup reference counts, by the conformance harness).

How it behaves in Node, and why:

- **Synchronous driver, event loop kept free.** `better-sqlite3` is synchronous (a local file read is microseconds). Lock
  contention is the exception, and ZeroBucket never lets the driver sleep on a lock, which would freeze your whole server.
  Each attempt waits ~25 ms; a `SQLITE_BUSY` is retried with async sleeps up to `busyTimeoutMs` (default 10 s, set via
  `new ZeroBucket({ backend: new SQLiteBackend({ path, busyTimeoutMs }) })`), then fails with a clear `StorageError`.
  A test holds the write lock from a second process and asserts the event loop keeps ticking while ZeroBucket waits.
- **One connection, no interleaving.** Each operation is a single synchronous `BEGIN IMMEDIATE ... COMMIT`, so concurrent
  requests can never run inside each other's transaction. WAL mode is set once, and only if the file is not already WAL.
- **Many processes, one new file** is safe (tested with 8 processes opening a fresh database simultaneously).
- **Tiering locks the whole database** for the duration of the upload (SQLite has no row locks): other processes' writes
  wait, this process's writes queue, reads are never blocked. It uses its own connection, so `tierToObjectStorage` does not
  accept `connection`, and it needs a real file (not `:memory:`). The safety guarantee is unchanged: a failed upload
  rolls back and leaves the row untouched.
- **`connection`** takes your own `better-sqlite3` `Database`. Multi-statement operations use a `SAVEPOINT`, so they
  are atomic whether or not you have an open transaction.
- **Streaming** uses ranged `substr()` reads, so a delete mid-stream raises `StorageError` (like Postgres). SQLite still
  reads the whole blob per chunk, in native memory: this bounds Node-side memory only.
- **Node 22+ for SQLite.** `better-sqlite3` 13 requires Node 22 or newer and crashes on older versions. It ships its Windows,
  macOS and Linux binaries inside the npm package, so no compiler or Visual Studio is needed. Postgres users can stay on Node 20.
  (`better-sqlite3` 12 declares Node 20 support, but it is untested here.)
- Not available on SQLite: `migrateClassicToDedup()` (Python has it for Postgres only, too).

## MySQL / MariaDB

```ts
const images = new ZeroBucket({
  mysql: "mysql://user:password@localhost:3306/mydb",
}); // npm install mysql2
// "mariadb://..." works too. Bring your own pool with ZeroBucket({ mysqlPool }) (never closed by close()).
```

Same API as everywhere else: `put`, `get`, `getMany`, `putMany`, `delete`, `deleteMany`, `exists`, `metadata`, streaming
and ranges, access-control hooks, the optimize pipeline, custom validators, HEIC, `onOperation` metrics, retry with
backoff, pooling (`poolMaxSize`, `poolTimeoutMs`), **dedup mode** (`dedup: true`), **object-storage tiering** and
`connection` (your own transaction). The [`zerobucket` CLI](#command-line) works on it too: pass a `mysql://` or
`mariadb://` `--database-url`.

It uses the Python package's tables, byte for byte (`SHOW CREATE TABLE` is identical for the classic table and for both
dedup tables), so a Node app and a Python app can share one MySQL/MariaDB database. The conformance harness verifies both
directions in both modes, including tiered rows and shared dedup blobs.

As everywhere, dedup and tiering cannot be combined (`dedup: true` with `objectStorage` throws). Copying an existing
classic table into the dedup tables (`migrate --to-dedup`) is PostgreSQL-only, as in the Python package.

What is different from Postgres, and why (each verified against a real MariaDB 10.11):

- **No `RETURNING`.** MySQL 8.0 does not have it (MariaDB 10.5+ does, but one code path serves both), so `delete()` and
  `deleteMany()` run `SELECT ... FOR UPDATE` and then `DELETE` in one transaction. That is also how a tiered row's object
  gets cleaned up.
- **Timestamps are written as UTC strings** (`DATETIME(6)`, naive UTC, like the Python package), so your server's or your
  process's timezone setting cannot change what is stored.
- **Pooling.** `mysql2` opens connections on demand, so `poolMinSize` is accepted for parity but there is no minimum to hold.
  `mysql2` has no acquire timeout, so `poolTimeoutMs` is implemented here: waiting longer than that for a free connection
  throws a clear `StorageError`, which is **not** retried (retrying would only wait again).
- **Retry** covers connection loss, lock-wait timeout (1205), deadlock (1213) and "too many connections". Every retry
  re-runs the **whole** operation on a fresh connection after a full rollback, never a single statement, so a multi-statement
  operation can never be half-applied. A connection that fails is destroyed, never returned to the pool.
- **Migration takes a server-side lock** (`GET_LOCK`), so many processes starting at once do not race on the DDL, and it
  issues **no DDL at all** when the schema is current. (Even a no-op `ALTER TABLE` needs an exclusive metadata lock that
  waits behind every open transaction, and everything else on that table then queues behind _it_.)
- **Dedup** stores each distinct image once (`zerobucket_blobs`, with a `ref_count`) and one row per image
  (`zerobucket_image_refs`). Without `RETURNING`, a delete decrements with `UPDATE ... ref_count = ref_count - n` (which
  takes the row lock) and then removes the blob once the count reaches zero; the foreign key makes deleting a blob that is
  still referenced impossible. A batch groups identical images, touches each distinct blob **once, in sorted checksum
  order**, and decrements in the same order, so overlapping batches cannot deadlock.
- **Batch deletes use one `SELECT ... FOR UPDATE` and then point deletes, never `DELETE ... WHERE id IN (...)`.** On
  MariaDB 10.11, with a small table, the `IN` delete is planned as a full scan (`EXPLAIN` says `type=ALL`), and a scan
  under REPEATABLE READ locks every row it passes, including rows another batch already holds. Two overlapping
  `deleteMany` batches deadlocked in 126 of 300 transactions with an `IN` delete and in 0 with point deletes (also 0 with
  a concurrent writer on the same blobs); `READ COMMITTED` and `FORCE INDEX` alone did not fix it. MySQL 8.0.46 plans the
  same statement as a range over just the listed ids and did **not** deadlock in the same test. Point deletes do not
  depend on the plan, so they are used on both.
- **Large images and `max_allowed_packet`.** An image bigger than the server's limit fails with an error that says so and
  how to raise it (`SET GLOBAL max_allowed_packet = ...`). Images are sent as raw bytes, so the limit applies to the image
  size, not twice that.
- **Tiering** locks only the image being moved (InnoDB row lock) for the duration of the upload: writes to _that_ image wait
  (up to `innodb_lock_wait_timeout`, 50 s by default), every other row and every plain read is unaffected. Postgres behaves
  the same way; SQLite has to lock the whole file.
- **`connection`** takes your own `mysql2/promise` connection. Multi-statement operations first check whether a transaction
  is open: inside one they use a `SAVEPOINT` (a failure undoes only this call's work), in autocommit mode they open and
  commit their own. The adapter never commits, rolls back or retries your transaction. Object cleanup after a `delete` is
  skipped on your connection (it cannot know whether you will commit).
- **Streaming** uses ranged `SUBSTRING()` reads, so a delete mid-stream raises `StorageError` (like Postgres). Tiered images
  stream with real S3 `Range` requests.
- **Tested on MariaDB 10.11 and MySQL 8.0.46** (the MySQL, dedup and CLI test suites, the Node/Python conformance harness
  and the schema comparison all pass on both). Other versions are expected to behave the same but have not been run. Use
  MariaDB 10.2.1+ or MySQL 8.0.16+: older servers accept the `CHECK` constraint syntax but ignore it.

## Deduplication

Store identical bytes once. In dedup mode every `put()` still returns its own id and keeps its own filename,
but images with the same content share a single stored blob with a reference count (think: the same logo or default
avatar uploaded a thousand times).

```ts
const images = new ZeroBucket({ connectionString, dedup: true });
const a = await images.put(logo, { filename: "a.png" });
const b = await images.put(logo, { filename: "b.png" }); // different id, same blob: ref_count = 2
await images.delete(a); // blob stays: ref_count = 1
await images.delete(b); // last reference gone: blob removed
```

- Uses separate tables (`zerobucket_blobs` + `zerobucket_image_refs`, identical to the Python package), so dedup and
  classic data can never be confused. A Node and a Python dedup app share one database and one set of blobs.
- Reference counts are exact under concurrency: 20 simultaneous `put()`s of the same bytes give `ref_count == 20`, and a
  `put()` racing the delete of the last reference never loses or leaks the blob (both are tested).
- Batches (`putMany`, `deleteMany`) take their blob locks in sorted checksum order, so overlapping batches in
  opposite orders cannot deadlock each other.
- Everything else works unchanged: hooks, streaming, ranges, the HTTP handler, optimize, validators, metrics, retry.
- Dedup **cannot be combined with `objectStorage`** (a shared blob has many owners), same rule as Python.

**Moving existing data in:** `await images.migrateClassicToDedup()` copies every classic image into the dedup tables,
keeping each id, and never modifies the classic table. It runs in small batches (bounded memory), is safe to re-run
(already-migrated ids are skipped and counted), and refuses up front if any classic image is tiered to object storage
(their bytes are not in the database to copy, and dedup mode has no tiering), changing nothing. If you would rather
migrate everything else, pass `{ skipTiered: true }` (CLI: `--skip-tiered`): tiered images are left untouched in the
classic table, counted in `tieredSkipped`, and **will not exist in the dedup tables**, so only use it if you can live
without them there.

## Object-storage tiering

For the minority of apps whose image volume outgrows what is economical to keep in a database
(backups, replication lag, per-GB price), move individual images to any S3-compatible store (AWS S3, MinIO,
Cloudflare R2, Backblaze B2, DigitalOcean Spaces). It is **explicit and opt-in**: `put()` never tiers
anything by itself, and nothing changes for you until you configure it.

```ts
import { ZeroBucket, ObjectStorage } from "zerobucket";

const images = new ZeroBucket({
  connectionString: process.env.DATABASE_URL,
  objectStorage: new ObjectStorage({ bucket: "my-images" }), // bucket must already exist
  // endpoint: "https://<account>.r2.cloudflarestorage.com", region: "auto"   // for non-AWS stores
});

await images.tierToObjectStorage(id); // true = moved now, false = already tiered (safe to re-run a backfill)
await images.get(id); // unchanged: get / getMany / getStream / HTTP handler all read transparently
await images.delete(id); // removes the row, then the object
```

- **Safe by construction.** The upload runs _inside_ the database transaction that locks the row. If it fails,
  the transaction rolls back and the row is left byte-for-byte untouched: there is never a moment when the bytes
  exist nowhere, or when a row claims to be tiered without a finished upload.
- **Real range requests.** Tiered images stream with genuine S3 `Range` GETs, so a `bytes=1000-1999` request fetches
  exactly those bytes (strictly better than the Postgres `substring()` approach).
- **Locking tradeoff.** The row lock is held for the whole upload, so concurrent _writes to that one image_ wait. Other
  rows and plain reads are never blocked. Treat tiering as a maintenance operation, not a request-path call.
- **Integrity checks.** A tiered object whose size no longer matches the database, or a missing/short object, raises
  `StorageError`. It never returns wrong data and never silently truncates a stream.
- **Same keys as Python.** Objects are named by the image's UUID with no prefix, so the Node and Python packages
  can read, tier and delete each other's objects in one bucket (verified by the conformance harness).
- **Deleting inside your own transaction** (`delete(id, { connection })`) deliberately does **not** remove the
  object: we cannot know whether your transaction will commit, and deleting early would turn a rollback into data
  loss. After you commit, remove it yourself with `objectStorage.delete(id)`.
- Failures to remove an object after a normal delete never fail the delete (the row is already gone); they are
  reported through `onOperation` as `object_storage_delete` events.
- Credentials come from the AWS SDK's standard chain (env vars, shared config, IAM role) or `credentials: {...}`.

## Shared database with the Python package

Same table (`zerobucket_images`), same checksum (SHA-256 hex of the stored bytes), same tiering
columns and CHECK constraint. A repeatable [conformance harness](./conformance) checks both
directions (Node writes → Python reads, and the reverse) and that both packages accept/reject the
same corpus of valid and corrupt files identically.

## Differences from the Python package

Python column = zerobucket on PyPI **0.25.1 or later** (several of its behaviours below were fixed in 0.25.1; older versions
differ).

|                                              | Python                                                                | npm                                                                                                                                |
| -------------------------------------------- | --------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| API                                          | sync + async clients                                                  | one async client                                                                                                                   |
| Malformed id (`"abc"`)                       | `StorageError` from Postgres                                          | treated as not found (`ImageNotFoundError`, no DB round trip), friendlier for `/images/:id` routes                                 |
| `optimize`                                   | Pillow; EXIF stripped, not applied                                    | sharp; EXIF **applied then stripped**; transparency → **white** for JPEG                                                           |
| `putMany` inserts                            | pipelined                                                             | one transaction, statements run sequentially                                                                                       |
| Formats                                      | JPEG, PNG, WebP, HEIC (`[heic]` extra)                                | JPEG, PNG, WebP, HEIC (optional `libheif-js`)                                                                                      |
| Convert TO HEIC with `optimize`              | yes (pillow-heif)                                                     | no (no HEVC encoder for Node); converting FROM HEIC works; `optimize: true` on a HEIC needs an explicit `format`                   |
| Event duration                               | `duration_seconds`                                                    | `durationMs`                                                                                                                       |
| `migrateClassicToDedup`, tiered classic rows | always **skips** them and reports `tiered_rows_skipped`               | **refuses** by default (changes nothing); `skipTiered: true` / `--skip-tiered` skips and reports like Python                       |
| `migrateClassicToDedup`, other behaviour     | batched, safe to re-run (server-side cursor)                          | batched, safe to re-run (keyset paging, advisory lock so concurrent runs cannot race)                                              |
| SQLite API                                   | synchronous `SQLiteBackend` + separate async backend                  | one async client; the sync driver runs on the event loop, lock waits are async (never block it)                                    |
| SQLite `onOperation` / busy retry            | none                                                                  | `onOperation` events and async `SQLITE_BUSY` retry (`busyTimeoutMs`)                                                               |
| SQLite stream deleted mid-read               | sync adapter keeps reading a WAL snapshot (async adapter raises)      | raises `StorageError`, like Postgres; use `connection` in a transaction for a snapshot                                             |
| MySQL `onOperation` / retry                  | sync `MySQLBackend` yes; `AsyncMySQLBackend` none                     | yes (same client for everything)                                                                                                   |
| CLI                                          | PostgreSQL, classic mode only; `verify` skips tiered rows with a note | PostgreSQL, SQLite **and** MySQL/MariaDB, classic **and** dedup; `verify --bucket` also checks tiered images; `migrate --to-dedup` |
| `connection=` without an open transaction    | multi-statement ops rely on the driver's implicit transaction         | always atomic (own transaction or savepoint)                                                                                       |
| S3 delete failure after `delete()`           | raises after the row is gone                                          | never fails the delete; reported via `onOperation`                                                                                 |
| Options                                      | snake_case kwargs                                                     | camelCase options object                                                                                                           |

Both packages now leave a tiered image's object in place on `delete(id, connection=...)` (a rolled-back transaction can
never lose data), take dedup batch locks in sorted checksum order (overlapping batches cannot deadlock), and skip the
MySQL `ALTER TABLE` at startup when the schema is current.

## Limitations (read before using in production)

- **Images live in your database.** That is the point (one backup, one transaction, no extra service), but it means
  this is not built for very large files or high-volume media: every image goes through the database and counts toward
  its size, its backups and its replication. The default size limit is 8 MiB (`maxBytes`). For large video or
  millions of big files, use object storage directly.
- **Backups and space need attention.** A whole-database dump now includes every image, and deleting images does not
  give disk space back on its own (PostgreSQL needs autovacuum, MySQL `OPTIMIZE TABLE`, SQLite `VACUUM`). SQLite
  files must be backed up with `.backup`, not copied. See the
  [Operations Guide](https://github.com/KedarGhadyalji/ZeroBucket/blob/main/docs/OPERATIONS.md).
- **Dedup and tiering cannot be combined** (a shared blob has many owners): `dedup: true` with `objectStorage` throws.
- **`migrate --to-dedup` is PostgreSQL-only**, as in the Python package. Tiered classic images cannot be migrated (see
  Deduplication for `skipTiered`).
- **HEIC can be read and converted from, but not written.** There is no HEVC encoder for Node, so `optimize` cannot
  produce HEIC.
- **SQLite allows one writer at a time**; lock waits are async and never block the event loop, but write-heavy
  workloads belong on PostgreSQL or MySQL/MariaDB.
- **Tested versions:** CI runs PostgreSQL 16 and MariaDB 10.11; MySQL 8.0.46 was also run by hand. Other versions are
  expected to behave the same but have not been run.

## Runtime notes

Node 20+ for Postgres and MySQL/MariaDB, Node 22+ for SQLite. Bun/Deno: expected to work, not yet tested. Edge runtimes (Cloudflare Workers, Vercel Edge) cannot open raw
Postgres TCP connections through `pg` without a proxy such as Hyperdrive; use the Node runtime for these routes.
In serverless, keep `poolMaxSize` small (1-2) or inject a pool shared across invocations.

## Security

Content is validated by decoding, never by trusting names or headers; responses carry
`X-Content-Type-Options: nosniff`; 500 responses never leak internals (use `onError` to log). The
`PDFValidator` is a sanity gate (magic bytes + size), not a PDF sanitizer. See the main repo's
`SECURITY.md`.

## License

MIT
