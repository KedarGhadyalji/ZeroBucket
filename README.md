# ZeroBucket

**Your database. Your images. Zero buckets.**

ZeroBucket is a database-native image storage library. It lets you store
and retrieve images using the PostgreSQL database you already have,
instead of standing up a separate object-storage service.

```python
from zerobucket import ZeroBucket

images = ZeroBucket(database_url=DATABASE_URL)

image_id = images.put("avatar.jpg")

image = images.get(image_id)
print(image.mime_type)   # "image/jpeg"
print(image.size)        # 1116478
print(image.data)        # raw bytes, ready to serve
```

## The problem

A typical small application ends up with two storage systems:

```
Application
 ├── PostgreSQL        (users, orders, everything else)
 └── S3 / Cloudinary    (just the images)
```

That second box brings its own credentials, its own SDK, its own
failure modes, its own bill, and a second thing to configure correctly
before your app works in a fresh environment.

For a prototype, an internal tool, a small SaaS app, or a portfolio
project, that's often more infrastructure than the images justify.

## What ZeroBucket does instead

```
Application
 └── PostgreSQL
      └── Images
```

Images are stored as raw bytes in a `BYTEA` column, right next to the
rest of your data. One database, one backup strategy, one thing to run.

**What ZeroBucket does _not_ do:** magically compress images into smaller
strings, eliminate the storage cost of the bytes themselves, or replace
S3 for large files or high-traffic media workloads. See
[Limitations](#limitations) below -- this is a deliberate scope decision,
not an oversight.

## Why not just use a `BYTEA` column directly?

You can -- ZeroBucket doesn't do anything a few lines of `psycopg`
couldn't. What it adds:

- **Content-based validation.** Every image is decoded and its format
  detected from actual bytes, not trusted from a filename or a
  client-supplied `Content-Type` header. Corrupted, truncated, and
  mismatched-extension files are rejected before they reach the database.
- **Decompression-bomb protection.** A tiny compressed file that decodes
  to an enormous pixel grid is rejected, not silently allocated.
- **A clean API that doesn't leak the storage detail.** You call `put()`
  and `get()`. You never construct SQL, handle a `bytea` type in your
  driver, or think about escaping.
- **A storage-backend abstraction underneath**, so a future non-Postgres
  or non-database backend doesn't mean rewriting application code.

## Installation

```bash
pip install zerobucket
```

Requires Python 3.10+ and a PostgreSQL 13+ database (uses
`gen_random_uuid()`, built in since Postgres 13).

## Usage

```python
from zerobucket import ZeroBucket, ImageNotFoundError, ImageValidationError

images = ZeroBucket(database_url="postgresql://user:pass@localhost/mydb")

# put() accepts a file path, raw bytes, or a file-like object
image_id = images.put("photo.jpg")
image_id = images.put(open("photo.jpg", "rb"))
image_id = images.put(request.files["avatar"])  # framework upload objects

# get() returns everything you need to serve the image
image = images.get(image_id)

# metadata() is the cheap version -- no bytes pulled over the wire
info = images.metadata(image_id)

# exists() / delete()
images.exists(image_id)
images.delete(image_id)

try:
    images.get("some-id-that-does-not-exist")
except ImageNotFoundError:
    ...

try:
    images.put("not-actually-an-image.txt")
except ImageValidationError:
    ...
```

### Batch operations

```python
results = images.put_many([open("a.jpg", "rb"), open("b.jpg", "rb"), "bad.txt"])
for r in results:
    if r.success:
        print(r.index, "->", r.image_id)
    else:
        print(r.index, "failed:", r.error)

fetched = images.get_many([id1, id2, "some-id-that-does-not-exist"])
deleted = images.delete_many([id1, id2])
```

Best-effort, not all-or-nothing: one bad image in a batch of 1000 doesn't
abort the other 999 -- check `.success`/`.error` per item. `put_many()`
still validates/optimizes each image individually in Python (that work
is inherently per-image), but batches the actual database writes into
one pipelined round trip via `executemany()` rather than one round trip
per image. `get_many()`/`delete_many()` are genuine single-query batch
operations (`WHERE id = ANY(...)`). All three accept the same
`connection=` parameter as their single-item counterparts.

### Serving from a web API

```python
from flask import Flask, Response

app = Flask(__name__)
images = ZeroBucket(database_url=DATABASE_URL)

@app.route("/images/<image_id>")
def serve_image(image_id):
    image = images.get(image_id)  # raises ImageNotFoundError -> 404, handle as usual
    return Response(image.data, mimetype=image.mime_type)
```

No manual decoding, no Base64, no internal transformation. `image.data`
is the same bytes you'd get from `open(path, "rb").read()`.

### Optimizing images (compression)

`optimize=True` unlocks metadata stripping, resizing, and quality-based
re-encoding -- off by default, so `put()` stores your exact input bytes
unless you opt in:

```python
image_id = images.put(
    "photo.jpg",
    optimize=True,
    max_width=1600,      # downscale if wider, aspect ratio preserved
    format="webp",       # optional re-encode target: "jpeg", "png", "webp"
    quality=90,           # 1-100, JPEG/WebP only; omit for data-backed defaults
)
```

Defaults (`quality=90` JPEG, `quality=88` WebP) aren't guessed -- they're
backed by real SSIM measurements across multiple content types. Typical
photos see 70-95% size reduction with no visible quality loss; dense
fine-texture content (foliage, fabric) sees smaller but still real gains.
**One thing this data caught: don't convert flat/graphic content (UI
screenshots, logos) to JPEG** -- it can make them _larger_, not smaller.
See [`benchmarks/COMPRESSION_RESULTS.md`](benchmarks/COMPRESSION_RESULTS.md)
for the full methodology and numbers.

## Supported formats

JPEG, PNG, WebP built in. Format is detected from file content, not from
filename extension or client-supplied MIME type.

**HEIC/HEIF** (the default format for iPhone photos) is supported via an
optional dependency, since it requires a native library and we don't want
to force that on everyone who doesn't need it:

```bash
pip install zerobucket[heic]
```

Once installed, HEIC works exactly like any other format -- `put()`
accepts it, content-sniffing detects it correctly, and `optimize=True,
format="jpeg"` converts it if you plan to serve images directly to
browsers (most browsers still can't display HEIC natively, unlike JPEG/
WebP/PNG). Without `zerobucket[heic]` installed, uploading a HEIC file
raises a clear error telling you to install the extra, rather than a
confusing "corrupted image" message.

## Transactions

By default, every `put()`/`get()`/`delete()`/`exists()`/`metadata()` call
uses its own separate, independently-committing database connection --
**not** whatever transaction your application might currently be in, even
if you're using the exact same database. This was verified directly
during development, not assumed: an image `put()` was shown to survive
even when a concurrent application transaction (on a different
connection) rolled back. If you're relying on "my image write rolls back
with the rest of my transaction" without doing anything extra, it
currently does not.

To make ZeroBucket participate in your own transaction -- so a `put()`
rolls back if the rest of your write fails, avoiding an orphaned image
row with no corresponding application record -- pass your own open
`psycopg` connection via `connection=`:

```python
import psycopg

conn = psycopg.connect(DATABASE_URL)
conn.autocommit = False

try:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO users (email) VALUES (%s) RETURNING id", (email,))
        user_id = cur.fetchone()[0]

    # Same transaction as the INSERT above -- if anything below raises,
    # neither the user row nor the image row will be committed.
    image_id = images.put(avatar_file, connection=conn)

    with conn.cursor() as cur:
        cur.execute("UPDATE users SET avatar_id = %s WHERE id = %s", (image_id, user_id))

    conn.commit()
except Exception:
    conn.rollback()
    raise
finally:
    conn.close()
```

Without `connection=`, this is exactly the "orphaned upload, DB row never
got created" class of bug that a separate object-storage service is also
prone to -- the _possibility_ of avoiding it is one of the real
advantages of storing images in your primary database, but only when you
actually use `connection=` to get it. It isn't automatic.

## Retry behavior

Transient database errors (connection drops, deadlocks, serialization
failures) are automatically retried with exponential backoff -- up to 3
times by default, configurable:

```python
images = ZeroBucket(
    database_url=DATABASE_URL,
    max_retries=5,          # default: 3. Set to 0 to disable entirely.
    retry_base_delay=0.2,   # default: 0.1 seconds, doubles each retry, capped at 2s, plus jitter
)
```

**Important interaction with `connection=`:** automatic retry only
applies when ZeroBucket is using its own internal connection pool (the
default). If you pass your own `connection=` to participate in your own
transaction, that call is retried **zero** times, regardless of
`max_retries` -- retrying a statement on a connection you're managing
yourself could silently corrupt your transaction's semantics (a
serialization failure normally means restarting the _whole_ transaction
from your application's perspective, not replaying one statement inside
it). That decision has to stay yours when you hold the connection.

Not every database error is retried -- only ones verified to represent a
genuinely transient condition (SQLSTATE-classified: serialization
failures, deadlocks, connection-level failures, admin shutdown). A
constraint violation or bad query is never retried, since it would fail
identically every time.

## Size limits

Default maximum: **8MB per image**, configurable via `max_bytes=`.

This isn't an arbitrary number -- see
[`benchmarks/RESULTS.md`](benchmarks/RESULTS.md) for measured latency and
memory cost across image sizes. In short:

| Range      | Behavior                                                                                              |
| ---------- | ----------------------------------------------------------------------------------------------------- |
| up to ~1MB | Sub-50ms writes, sub-10ms reads, no measurable memory pressure. Feels like any other database write.  |
| 1-5MB      | Fine for occasional uploads (avatars, scans, product photos). Noticeable latency under load.          |
| 5-8MB      | Real latency and memory cost per request. Usable, but don't put it in a tight request loop.           |
| above 8MB  | Rejected by default. Raise `max_bytes` if you understand the tradeoff, or use object storage instead. |

## Limitations

Be honest with yourself about whether ZeroBucket fits your workload:

- **Not for large files or high-volume media.** Full images are read
  into memory on both the client and server side of every request.
  There is no streaming, no range requests, no CDN. If you're serving
  millions of images a day or storing video, use S3 (or similar) instead.
- **No deduplication yet.** Uploading the same image twice stores it
  twice. A SHA-256 checksum is recorded on every row so this can be
  added later without a schema change, but it isn't wired up in v1 --
  doing it correctly requires reference counting so that deleting one
  copy doesn't corrupt another, and that's more complexity than a v1
  should carry.
- **No image resizing/optimization pipeline yet.** ZeroBucket stores
  what you give it. It validates and rejects bad input; it doesn't
  transform good input (yet).
- **PostgreSQL only, for now.** The storage layer is abstracted
  specifically so SQLite/MySQL adapters can be added without touching
  application code, but only the Postgres adapter exists today.
- **Storing bytes in BYTEA does not compress them further than they
  already are.** JPEG/WebP files are already near-maximum entropy;
  Postgres's TOAST compression does essentially nothing to them (see
  benchmark results). ZeroBucket saves you _infrastructure_, not
  storage bytes.

## Project structure

```
zerobucket/
├── packages/
│   └── python/          # this package
│       ├── src/zerobucket/
│       │   ├── client.py       # the public ZeroBucket class
│       │   ├── validation.py   # content-based format/size/corruption checks
│       │   ├── exceptions.py
│       │   ├── types.py        # Image, ImageMetadata
│       │   └── adapters/
│       │       ├── base.py     # StorageBackend interface (bytes in/out only)
│       │       └── postgres.py # the only implementation so far
│       └── tests/
├── benchmarks/
│   ├── run_benchmark.py
│   └── RESULTS.md
├── docs/
└── examples/
```

## Development

```bash
cd packages/python
pip install -e ".[dev]"
pytest                 # unit tests run standalone; integration tests need
                        # ZEROBUCKET_TEST_DATABASE_URL pointed at a real Postgres
ruff check src/ tests/
```

## CLI

```bash
zerobucket init      # create the zerobucket_images table and indexes if missing
zerobucket migrate   # currently the same as init -- no versioned migrations yet
zerobucket info      # image count, total size, on-disk size, breakdown by format
zerobucket verify    # re-checksum every stored image to detect corruption
zerobucket verify --sample 100   # check a random sample instead of everything
```

All commands take `--database-url`, or read `ZEROBUCKET_DATABASE_URL` from
the environment:

```bash
export ZEROBUCKET_DATABASE_URL=postgresql://user:pass@localhost/mydb
zerobucket info
```

`verify` exits with status 1 if any mismatches are found (and prints
which image IDs), so it's usable as a cron job or CI check, not just an
interactive command. It streams one image's bytes at a time rather than
loading the whole table into memory -- safe to run against a large table.

## Operations

Running this with real, growing data? See
[`docs/OPERATIONS.md`](docs/OPERATIONS.md) for backup strategy (your
nightly `pg_dump` will include every image byte unless you split it out)
and autovacuum tuning notes specific to a BYTEA-heavy table.

## Roadmap

Not yet built, tracked honestly rather than implied:

- [ ] TypeScript/npm package with an equivalent API
- [x] Optional resize/format-conversion pipeline (`optimize=True, max_width=...`)
- [x] Optional HEIC/HEIF support (`pip install zerobucket[heic]`)
- [x] Transaction participation via `connection=` (put/get/delete/exists/metadata)
- [ ] Deduplication with reference counting
- [ ] SQLite and MySQL adapters
- [x] CLI (`zerobucket init`, `zerobucket migrate`, `zerobucket info`, `zerobucket verify`)
- [ ] Optional object-storage backend for files that outgrow the database tier
- [ ] `asyncpg` / async client support
- [x] Batch operations (`put_many`/`get_many`/`delete_many`)
- [x] Retry/backoff policy for transient database errors
- [ ] `before_get(image_id, context) -> bool` authorization hook

## License

MIT
