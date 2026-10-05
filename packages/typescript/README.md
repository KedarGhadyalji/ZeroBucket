# zerobucket (npm)

**Database-native image storage for Node.js. Your database. Your images. Zero buckets.**

Store small-to-medium image collections (avatars, product photos, uploads) as ordinary
Postgres rows, with validation, optimization, streaming, range requests and access
control built in, and no S3 account to provision for the common case.

This is the Node.js/TypeScript sibling of the [Python package](https://pypi.org/project/zerobucket/).
**It uses the exact same table**, so a Node service and a Python service can share one database
and read each other's images.

> **Status: early (0.4.x).** PostgreSQL **and SQLite**, each in classic and dedup modes, with S3-compatible
> object-storage tiering. MySQL, a CLI and HEIC are on the roadmap (see [What's not here yet](#whats-not-here-yet)).

## Install

```bash
npm install zerobucket pg sharp          # PostgreSQL
npm install zerobucket better-sqlite3 sharp   # or SQLite (see below): no database server at all
```

`pg` (the Postgres driver), `better-sqlite3` (SQLite) and `sharp` (image decoding/optimizing) are peer dependencies, so you
control their versions. If you only store PDFs through a custom validator, `sharp` is not needed.
For [object-storage tiering](#object-storage-tiering) also install `@aws-sdk/client-s3` (optional; loaded only when used).
Requires Node 20+. Ships ESM and CommonJS with full types.

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
- Not available on SQLite: `migrateClassicToDedup()` (Python has it for Postgres only, too).

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
(already-migrated ids are skipped and counted), and refuses up front if any classic image is tiered to object storage.

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

|                                                | Python                                                                     | npm                                                                                                |
| ---------------------------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| API                                            | sync + async clients                                                       | one async client                                                                                   |
| Malformed id (`"abc"`)                         | `StorageError` from Postgres                                               | treated as not found (`ImageNotFoundError`, no DB round trip), friendlier for `/images/:id` routes |
| `optimize`                                     | Pillow; EXIF stripped, not applied                                         | sharp; EXIF **applied then stripped**; transparency → **white** for JPEG                           |
| `putMany` inserts                              | pipelined                                                                  | one transaction, statements run sequentially                                                       |
| Formats                                        | JPEG, PNG, WebP, HEIC (extra)                                              | JPEG, PNG, WebP (HEIC not yet)                                                                     |
| Event duration                                 | `duration_seconds`                                                         | `durationMs`                                                                                       |
| `migrateClassicToDedup`                        | loads the whole table into memory; fails if re-run or if any row is tiered | batched, safe to re-run, refuses tiered rows with a clear message                                  |
| Dedup `putMany` / `deleteMany` locking         | arbitrary order (overlapping batches can deadlock)                         | sorted checksum order (cannot deadlock)                                                            |
| SQLite API                                     | synchronous `SQLiteBackend` + separate async backend                       | one async client; the sync driver runs on the event loop, lock waits are async (never block it)    |
| SQLite `onOperation` / busy retry              | none                                                                       | `onOperation` events and async `SQLITE_BUSY` retry (`busyTimeoutMs`)                               |
| SQLite stream deleted mid-read                 | sync adapter keeps reading a WAL snapshot (async adapter raises)           | raises `StorageError`, like Postgres; use `connection` in a transaction for a snapshot             |
| `connection=` without an open transaction      | multi-statement ops rely on the driver's implicit transaction              | always atomic (own transaction or savepoint)                                                       |
| `delete(id, connection=...)` on a tiered image | deletes the S3 object immediately                                          | leaves the object until you delete it after commit (a rolled-back transaction can never lose data) |
| S3 delete failure after `delete()`             | raises after the row is gone                                               | never fails the delete; reported via `onOperation`                                                 |
| Options                                        | snake_case kwargs                                                          | camelCase options object                                                                           |

## What's not here yet

Planned, in order: **MySQL/MariaDB**, the `zerobucket` **CLI**, and **HEIC**.
Tiering and dedup cannot be combined (same rule as the Python package).

## Runtime notes

Node 20+. Bun/Deno: expected to work, not yet tested. Edge runtimes (Cloudflare Workers, Vercel Edge) cannot open raw
Postgres TCP connections through `pg` without a proxy such as Hyperdrive; use the Node runtime for these routes.
In serverless, keep `poolMaxSize` small (1-2) or inject a pool shared across invocations.

## Security

Content is validated by decoding, never by trusting names or headers; responses carry
`X-Content-Type-Options: nosniff`; 500 responses never leak internals (use `onError` to log). The
`PDFValidator` is a sanity gate (magic bytes + size), not a PDF sanitizer. See the main repo's
`SECURITY.md`.

## License

MIT
