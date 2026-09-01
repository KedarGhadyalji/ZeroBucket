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

### Streaming reads/writes for large files

`get()` always returns the complete image as one `bytes` object -- fine
for typical avatar/photo-sized uploads, but it means the full image sits
in Python memory (and gets copied around) even if all you're doing is
piping it straight back out to an HTTP response. `get_stream()` avoids
that:

```python
for chunk in images.get_stream(image_id, chunk_size=1024 * 1024):
    response.write(chunk)

# or, equivalently, let ZeroBucket do the loop:
total_bytes = images.stream_to(image_id, response, chunk_size=1024 * 1024)
```

Implemented via repeated `substring(data FROM offset FOR length)` range
queries -- only one chunk is ever held in Python memory at a time, and it
works identically in classic and dedup mode. `stream_to()` writes
straight to anything with a `.write(bytes)` method (an open file, a
FastAPI/Flask response, a socket) and returns the total byte count.

Two honest limitations, worth knowing before you reach for this:

- **This is a Python-side memory optimization, not a Postgres-side one.**
  The server still handles the full stored value the way it always has
  for a BYTEA column (TOAST detoast, etc.) -- `get_stream()` doesn't
  change what Postgres does, only what your application process holds
  onto while consuming the result.
- **It's not HTTP range/partial-content support.** The full image is
  still transferred every time, just paced out in pieces instead of
  handed over all at once -- there's no way to ask for "just bytes
  200-400" of a stored image. `before_get`/range-request support is a
  separate, not-yet-built roadmap item.

On the write side, `put()` reads a file-like input (an open file, a
framework upload object) in bounded chunks and rejects an oversized
upload as soon as it's read one byte past `max_bytes`, instead of first
buffering the whole thing into memory. This bounds peak memory for a
_rejected_ oversized upload -- it does not turn `put()` into unbounded-
size streaming ingestion, since checksum computation and image
validation (Pillow decode) both require the complete bytes for anything
that's actually within the cap. `bytes`/path input is unaffected --
those are already fully in memory (or a single `read_bytes()` call)
before `put()` sees them.

Concurrency note: without `connection=` spanning the whole `get_stream()`
call, each chunk is its own round trip with no snapshot isolation across
chunks. If the row is deleted mid-stream by something else, the next
chunk fetch raises `StorageError` rather than silently handing back a
short/truncated stream -- pass your own open `connection=` (see
[Transactions](#transactions)) if you need a guaranteed-consistent read
across the whole stream despite concurrent writers.

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

## Custom content types (PDFs and beyond)

ZeroBucket validates everything as an image by default -- unchanged, and
that stays true for every existing caller. For content ZeroBucket
doesn't natively understand, pass your own validator to `put()`:

```python
from zerobucket import ZeroBucket
from zerobucket.validators.pdf import PDFValidator

images = ZeroBucket(database_url=DATABASE_URL)
pdf_validator = PDFValidator()

doc_id = images.put(pdf_bytes, validator=pdf_validator)
doc = images.get(doc_id)  # get() needs zero special-casing -- always has
print(doc.mime_type)  # "application/pdf"
```

Everything else works identically regardless of which validator produced
a row: `connection=` (transactional atomicity), automatic retry,
`put_many()`/`get_many()`/`delete_many()`, `exists()`, `delete()`. This
isn't incidental -- `width`/`height` were already nullable in the schema
and the `Image` type before this feature existed (only images have
natural dimensions), so the entire read path needed zero changes to
support non-image content; only `put()`/`put_many()` needed the hook.

**Why this instead of native PDF support?** A PDF is a categorically
richer, more dangerous format to fully secure than a raster image
(embeddable JavaScript, forms, launch actions) -- absorbing that
directly into ZeroBucket's core would mean either quietly under-securing
it or meaningfully expanding what "database-native image storage"
promises to guarantee. The pluggable hook lets you opt into that
tradeoff explicitly, for the content type you actually need, without
ZeroBucket claiming to have solved PDF security for you.

**Write your own validator** by implementing `ContentValidator`:

```python
from zerobucket import ContentValidator, ValidatedContent

class MyValidator(ContentValidator):
    def validate(self, data: bytes, *, max_bytes: int) -> ValidatedContent:
        if len(data) > max_bytes:
            raise MyValidationError("too big")
        # ... your own content-sniffed checks here ...
        return ValidatedContent(mime_type="application/x-my-format", size_bytes=len(data))
```

`optimize=True` is incompatible with `validator=` and raises immediately
if both are given -- the resize/re-encode pipeline is Pillow-based and
image-specific.

See [`zerobucket/validators/pdf.py`](https://github.com/KedarGhadyalji/ZeroBucket/blob/main/packages/python/src/zerobucket/validators/pdf.py)
for a complete reference implementation, including an explicit note on
what it does and doesn't protect against.

## Deduplication

Opt-in content-addressed storage: byte-identical uploads share one
underlying stored copy, reference-counted, with the bytes actually
deleted only when the last referencing id is deleted.

```python
images = ZeroBucket(database_url=DATABASE_URL, dedup=True)

id1 = images.put("photo.jpg")
id2 = images.put("photo.jpg")  # identical content

# Two distinct, independently-usable ids, as always --
assert id1 != id2
# -- but the bytes are stored exactly once, referenced twice.

images.delete(id1)  # the stored bytes survive -- id2 still references them
images.get(id2)     # still works fine

images.delete(id2)  # NOW the bytes are actually deleted (last reference gone)
```

**Why this defaults to `False`:** enabling it requires an actual schema
change -- content lives in a separate `zerobucket_blobs` table (keyed by
checksum, with a `ref_count`), referenced by `zerobucket_image_refs`.
These are deliberately **different tables** from classic mode's
`zerobucket_images` -- flipping `dedup=True` does not retroactively
touch, migrate, or dedupe any existing classic-mode data, and the two
modes can safely coexist against the same database (proven by a
dedicated test, not just claimed).

**Verified before being built, not assumed:** the underlying
`INSERT ... ON CONFLICT DO UPDATE` upsert pattern was tested under real
concurrent load (20 threads incrementing the same counter) before any
application code was written on top of it -- confirmed race-safe, zero
lost updates. A further test exercises this through the real
`ZeroBucket` client with 15 concurrent `put()` calls for identical
content and confirms the final reference count is exact.

### Migrating existing classic-mode data

```python
from zerobucket import ZeroBucket, migrate_classic_to_dedup

dedup_images = ZeroBucket(database_url=DATABASE_URL, dedup=True)
summary = migrate_classic_to_dedup(dedup_images._backend)
print(summary)  # {'images_migrated': N, 'distinct_blobs_created': M, 'duplicate_references_found': N-M}
```

**Non-destructive** -- copies every row from the classic
`zerobucket_images` table into the dedup tables, preserving every
existing id exactly (so anything already referencing those ids keeps
working), and does not modify or delete the original table. Verify the
result, then clean up the old table yourself once you're confident.
Known limitation, stated plainly: this loads the whole classic table
into memory as one transaction -- fine for typical small/medium
datasets, not built as a streaming tool for huge tables.

## Tuning and observability

Two things worth knowing before you try to optimize anything: how to
tune the connection pool, and how to actually measure what's happening.

**Pool sizing** was previously hardcoded; now configurable:

```python
images = ZeroBucket(
    database_url=DATABASE_URL,
    pool_min_size=2,    # default: 1
    pool_max_size=10,   # default: 5 -- raise this under real concurrent load
    pool_timeout=15,    # default: 10 -- seconds to wait for a pooled connection
)
```

**`on_operation`** is a callback fired after every storage operation
completes (success or failure), with timing, retry count, and error
info -- wire it to whatever metrics backend you actually use (Prometheus,
StatsD, plain logging). ZeroBucket doesn't ship a specific integration,
deliberately, to keep the core dependency footprint small:

```python
from zerobucket import ZeroBucket, OperationEvent

def on_operation(event: OperationEvent) -> None:
    print(f"{event.operation}: {event.duration_seconds:.3f}s "
          f"success={event.success} retries={event.retry_count}")
    # or: my_metrics_client.histogram(f"zerobucket.{event.operation}.duration", event.duration_seconds)

images = ZeroBucket(database_url=DATABASE_URL, on_operation=on_operation)
```

`operation` is one of `"put"`, `"put_many"`, `"get"`, `"get_stream"`,
`"get_many"`, `"get_metadata"`, `"delete"`, `"delete_many"`, `"exists"`,
`"migrate"` -- dedup-mode operations report the same names as their
classic-mode counterparts, since from a metrics perspective it's still
logically the same operation regardless of storage mode underneath. Note
that a single `get_stream()` call emits one `get_metadata` event (the
initial size/not-found lookup) followed by one `get_stream` event _per
chunk fetched_, not one event for the whole call -- each is a genuinely
separate round trip, consistent with every other operation being
measured per round trip rather than per logical method call.

A subtle but important point, worth being explicit about: `get()` on a
missing id reports `success=True` in the event (the _database query_
succeeded -- it correctly found no matching row) even though `get()`
itself then raises `ImageNotFoundError` to the caller. The event
measures the storage operation, not your application-level outcome.

**Safety guarantee, tested directly:** an exception raised inside your
`on_operation` callback is caught and silently ignored -- a bug in your
metrics code can never break a real image operation. This also means
such bugs won't be visible to you unless you test the callback itself
separately.

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

- **Not for large files or high-volume media.** `get_stream()`/
  `stream_to()` avoid holding a full image in _Python_ memory during a
  read, but Postgres itself still handles the full stored value the same
  way it always has for a BYTEA column -- there is no true reduction in
  server-side memory/IO cost, no HTTP range/partial-content support, and
  no CDN. If you're serving millions of images a day or storing video,
  use S3 (or similar) instead.
- **Deduplication exists but is opt-in, not automatic.** By default
  (`dedup=False`), uploading the same image twice still stores it twice
  -- pass `dedup=True` for content-addressed, reference-counted storage.
  See the [Deduplication](#deduplication) section above.
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
- [x] Deduplication with reference counting (opt-in, `dedup=True`)
- [ ] SQLite and MySQL adapters
- [x] CLI (`zerobucket init`, `zerobucket migrate`, `zerobucket info`, `zerobucket verify`)
- [ ] Optional object-storage backend for files that outgrow the database tier
- [ ] `asyncpg` / async client support
- [x] Batch operations (`put_many`/`get_many`/`delete_many`)
- [x] Retry/backoff policy for transient database errors
- [ ] `before_get(image_id, context) -> bool` authorization hook
- [x] Pluggable content validators (`put(validator=...)`) -- includes a PDF reference implementation
- [x] Streaming reads/writes for large files (`get_stream()`/`stream_to()`; bounded-read writes)
- [ ] Django integration package
- [x] Configurable connection pool sizing (`pool_min_size`/`pool_max_size`/`pool_timeout`)
- [x] `on_operation` observability hook (per-operation timing, retry count, success/failure)

## License

MIT
