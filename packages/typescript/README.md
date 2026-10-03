# zerobucket (npm)

**Database-native image storage for Node.js. Your database. Your images. Zero buckets.**

Store small-to-medium image collections (avatars, product photos, uploads) as ordinary
Postgres rows, with validation, optimization, streaming, range requests and access
control built in, and no S3 account to provision for the common case.

This is the Node.js/TypeScript sibling of the [Python package](https://pypi.org/project/zerobucket/).
**It uses the exact same table**, so a Node service and a Python service can share one database
and read each other's images.

> **Status: 0.1.0.** PostgreSQL, classic mode. Dedup, object-storage tiering, SQLite and MySQL
> are on the roadmap (see [What's not here yet](#whats-not-here-yet)).

## Install

```bash
npm install zerobucket pg sharp
```

`pg` (the Postgres driver) and `sharp` (image decoding/optimizing) are peer dependencies, so you
control their versions. If you only store PDFs through a custom validator, `sharp` is not needed.
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

## Shared database with the Python package

Same table (`zerobucket_images`), same checksum (SHA-256 hex of the stored bytes), same tiering
columns and CHECK constraint. A repeatable [conformance harness](./conformance) checks both
directions (Node writes → Python reads, and the reverse) and that both packages accept/reject the
same corpus of valid and corrupt files identically.

## Differences from the Python package

|                        | Python                             | npm                                                                                                |
| ---------------------- | ---------------------------------- | -------------------------------------------------------------------------------------------------- |
| API                    | sync + async clients               | one async client                                                                                   |
| Malformed id (`"abc"`) | `StorageError` from Postgres       | treated as not found (`ImageNotFoundError`, no DB round trip), friendlier for `/images/:id` routes |
| `optimize`             | Pillow; EXIF stripped, not applied | sharp; EXIF **applied then stripped**; transparency → **white** for JPEG                           |
| `putMany` inserts      | pipelined                          | one transaction, statements run sequentially                                                       |
| Formats                | JPEG, PNG, WebP, HEIC (extra)      | JPEG, PNG, WebP (HEIC not yet)                                                                     |
| Event duration         | `duration_seconds`                 | `durationMs`                                                                                       |
| Options                | snake_case kwargs                  | camelCase options object                                                                           |

## What's not here yet

Planned, in order: object-storage **tiering** (S3-compatible), **dedup** mode, **SQLite**, **MySQL/MariaDB**.
Until tiering lands: reading a row that the Python package tiered to S3 throws a clear `StorageError`
(never wrong data), and **deleting** such a row from Node removes the database row but leaves the S3
object behind (the same documented orphan behaviour as Python without `object_storage`).
Delete tiered images from Python if you use tiering.

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
