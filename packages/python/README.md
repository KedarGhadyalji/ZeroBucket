# ZeroBucket

[![PyPI version](https://img.shields.io/pypi/v/zerobucket.svg)](https://pypi.org/project/zerobucket/)
[![Python versions](https://img.shields.io/pypi/pyversions/zerobucket.svg)](https://pypi.org/project/zerobucket/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/KedarGhadyalji/ZeroBucket/blob/main/LICENSE)
[![CI](https://github.com/KedarGhadyalji/ZeroBucket/actions/workflows/ci.yml/badge.svg)](https://github.com/KedarGhadyalji/ZeroBucket/actions/workflows/ci.yml)

**Your database. Your images. Zero buckets.**

ZeroBucket is a database-native image storage library. It lets you store
and retrieve images using the PostgreSQL database you already have,
instead of standing up a separate object-storage service like S3.

```python
from zerobucket import ZeroBucket

images = ZeroBucket(database_url="postgresql://...")

image_id = images.put("avatar.jpg")

image = images.get(image_id)
print(image.mime_type)   # "image/jpeg"
print(image.size_bytes)  # 1116478
print(image.data)        # raw bytes, ready to serve
```

Full documentation, architecture notes, and benchmark results live in the
[GitHub repository](https://github.com/KedarGhadyalji/ZeroBucket).

## Installation

```bash
pip install zerobucket
```

Requires Python 3.10+ and PostgreSQL 13+ (uses `gen_random_uuid()`, built
in since Postgres 13).

## Quick reference

| Method                                                                                        | What it does                                                                                                                                                                                 |
| --------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `images.put(image, filename=None, optimize=False, max_width=None, format=None, quality=None)` | Validates, optionally optimizes, checksums, and stores an image. Accepts a file path, raw `bytes`, or a file-like object (including framework upload objects). Returns the new image's id.   |
| `images.get(image_id, context=None)`                                                          | Returns an `Image` (data, mime_type, filename, width, height, size_bytes, checksum_sha256). Raises `ImageNotFoundError` if missing, or `AccessDeniedError` if a `before_get` hook denies it. |
| `images.get_stream(image_id, chunk_size=1MB)`                                                 | Like `get()`, but returns an iterator of chunks instead of one `bytes` object -- avoids holding the full image in Python memory at once. Raises `ImageNotFoundError` if missing.             |
| `images.stream_to(image_id, destination, chunk_size=1MB)`                                     | Writes an image's bytes directly to `destination` (anything with `.write(bytes)`), chunk by chunk. Returns total bytes written.                                                              |
| `images.metadata(image_id)`                                                                   | Same fields as `get()` but without the raw bytes -- cheap existence/info check.                                                                                                              |
| `images.exists(image_id)`                                                                     | Returns `True`/`False`.                                                                                                                                                                      |
| `images.delete(image_id)`                                                                     | Deletes the image. Returns `True` if it existed.                                                                                                                                             |
| `images.close()`                                                                              | Releases database connections. `ZeroBucket` also works as a context manager.                                                                                                                 |

```python
from zerobucket import ZeroBucket, ImageNotFoundError, ImageValidationError

images = ZeroBucket(
    database_url="postgresql://user:pass@localhost/mydb",
    max_bytes=8 * 1024 * 1024,  # default: 8MB
)

image_id = images.put("photo.jpg")
image_id = images.put(open("photo.jpg", "rb"))
image_id = images.put(request.files["avatar"])  # framework upload objects

image = images.get(image_id)
info = images.metadata(image_id)

try:
    images.get("nonexistent-id")
except ImageNotFoundError:
    ...

try:
    images.put("not-actually-an-image.txt")
except ImageValidationError:
    ...
```

### Serving from a web API

```python
from flask import Flask, Response

app = Flask(__name__)
images = ZeroBucket(database_url=DATABASE_URL)

@app.route("/images/<image_id>")
def serve_image(image_id):
    image = images.get(image_id)
    return Response(image.data, mimetype=image.mime_type)
```

### Streaming reads for large files

```python
for chunk in images.get_stream(image_id, chunk_size=1024 * 1024):
    response.write(chunk)

# or:
total_bytes = images.stream_to(image_id, response, chunk_size=1024 * 1024)
```

Avoids holding the full image in Python memory at once (one chunk at a
time instead), via ranged `substring()` queries. This is a Python-side
memory optimization only -- Postgres still handles the full stored value
the way it always has, and this isn't HTTP range/partial-content
support. See the [full explanation](https://github.com/KedarGhadyalji/ZeroBucket#streaming-readswrites-for-large-files)
on GitHub, including the mid-stream-delete safety behavior and how
`put()` bounds memory for rejected oversized file-like uploads.

### Access control

```python
def before_get(image_id: str, context: dict | None) -> bool:
    return context is not None and owns(context["user_id"], image_id)

def before_put(context: dict | None) -> bool:
    return context is not None and context.get("user_id") is not None

images = ZeroBucket(
    database_url=DATABASE_URL,
    before_get=before_get,
    before_put=before_put,
)

images.get(image_id, context={"user_id": current_user.id})  # -> AccessDeniedError if denied
```

No built-in ownership/permissions model by default -- these hooks let
you plug in your own check. Denied calls raise `AccessDeniedError` and
never reach the database. A hook that raises fails _closed_ (the
exception propagates, never treated as an implicit allow) -- see the
[full explanation](https://github.com/KedarGhadyalji/ZeroBucket#access-control)
on GitHub for exactly what's gated (`get`/`get_many`/`get_stream`/
`stream_to`/`metadata`, NOT `exists`) and the `put_many`/`get_many`
batch evaluation semantics.

### Optimizing images (compression)

Off by default -- `put()` stores your exact input bytes unless you opt in:

```python
image_id = images.put(
    "photo.jpg",
    optimize=True,
    max_width=1600,      # downscale if wider, aspect ratio preserved
    format="webp",       # optional re-encode target: "jpeg", "png", "webp"
    quality=90,           # 1-100, JPEG/WebP only; omit for data-backed defaults
)
```

Quality defaults (JPEG=90, WebP=88) are backed by measured SSIM data
across multiple content types -- typical photos see 70-95% size
reduction with no visible quality loss. One thing this data caught:
**don't target `format="jpeg"` for flat/graphic content** (screenshots,
logos) -- it can make them larger, not smaller. See
[`COMPRESSION_RESULTS.md`](https://github.com/KedarGhadyalji/ZeroBucket/blob/main/benchmarks/COMPRESSION_RESULTS.md)
on GitHub for the full methodology.

### Async support

```python
from zerobucket import AsyncZeroBucket

images = AsyncZeroBucket(database_url="postgresql://user:pass@localhost/mydb")
image_id = await images.put("photo.jpg")
image = await images.get(image_id)

stream = await images.get_stream(image_id)  # note the await
async for chunk in stream:
    ...

await images.close()  # or: async with AsyncZeroBucket(...) as images:
```

Built on psycopg3's own native async mode -- not the third-party
`asyncpg` package, despite that name having sat on the roadmap for a
while. Zero new dependencies. First-pass scope: core operations +
streaming reads, classic mode only (no `dedup=`, hooks, `on_operation`,
`optimize=`/`validator=`, `connection=`, or retry yet -- tracked
honestly, not silently missing). See the
[full explanation](https://github.com/KedarGhadyalji/ZeroBucket#async-support)
on GitHub, including why `get_stream()` needs an `await` before you can
iterate it, and a **Windows note**: psycopg3's async mode needs a
`SelectorEventLoop`, not the default `ProactorEventLoop` --
`AsyncZeroBucket` raises a clear error telling you how to fix this if
you hit it, instead of a confusing timeout.

## What it validates

- **Format**: JPEG, PNG, WebP built in, plus HEIC/HEIF (iPhone photos) via
  the optional `pip install zerobucket[heic]` extra -- detected from
  actual file content, never from filename extension or a client-supplied
  `Content-Type` header.
- **Corruption**: truncated or malformed images are decoded and rejected
  before they reach the database.
- **Decompression bombs**: a tiny compressed file that decodes to an
  enormous pixel grid is rejected, not silently allocated.
- **Size**: configurable via `max_bytes` (default 8MB) -- see the
  [benchmark results](https://github.com/KedarGhadyalji/ZeroBucket/blob/main/benchmarks/RESULTS.md)
  for why.

## Transactions

By default, `put()`/`get()`/`delete()` each use their own independent
database connection -- **not** your application's own transaction, even
against the same database. Pass your own open `psycopg` connection via
`connection=` to make a write participate in your transaction (e.g. "user

- avatar, atomically, or neither"). See the
  [Transactions section](https://github.com/KedarGhadyalji/ZeroBucket#transactions)
  on GitHub for a worked example -- this was verified by direct experiment
  during development, not assumed.

## Batch operations

```python
results = images.put_many([open("a.jpg", "rb"), open("b.jpg", "rb")])
fetched = images.get_many([id1, id2])
deleted = images.delete_many([id1, id2])
```

Best-effort, not all-or-nothing -- check `.success`/`.error` per item.
`get_many`/`delete_many` are genuine single-query batch operations; see
the [full docs](https://github.com/KedarGhadyalji/ZeroBucket#batch-operations)
on GitHub for what's actually batched vs. still per-item.

## Retry behavior

Transient errors (connection drops, deadlocks, serialization failures)
are automatically retried with exponential backoff (`max_retries=3` by
default). **Important:** passing your own `connection=` disables
automatic retry for that call -- see the
[full explanation](https://github.com/KedarGhadyalji/ZeroBucket#retry-behavior)
on GitHub for why that's a deliberate safety rule, not an oversight.

## Custom content types (PDFs and beyond)

```python
from zerobucket.validators.pdf import PDFValidator

doc_id = images.put(pdf_bytes, validator=PDFValidator())
doc = images.get(doc_id)  # no special handling needed -- ever
```

Everything (transactions, retry, batch ops) works identically regardless
of which validator produced a row. See the
[full explanation](https://github.com/KedarGhadyalji/ZeroBucket#custom-content-types-pdfs-and-beyond)
on GitHub for why this is a pluggable hook rather than native PDF
support built into the core.

## CLI

```bash
zerobucket init      # create the schema if missing
zerobucket info      # image count, total size, breakdown by format
zerobucket verify    # re-checksum every image to detect corruption
```

Takes `--database-url` or reads `ZEROBUCKET_DATABASE_URL` from the
environment. `verify` exits non-zero on any mismatch, so it's usable in
cron/CI. See the
[full CLI docs](https://github.com/KedarGhadyalji/ZeroBucket#cli) on
GitHub.

## Deduplication

```python
images = ZeroBucket(database_url=DATABASE_URL, dedup=True)
id1 = images.put("photo.jpg")
id2 = images.put("photo.jpg")  # identical content -- stored exactly once, referenced twice
```

Opt-in (`dedup=True`), uses separate tables from classic mode, so it's
safe to add later without touching existing data. See the
[full explanation](https://github.com/KedarGhadyalji/ZeroBucket#deduplication)
on GitHub, including the migration path for existing classic-mode data.

## Tuning and observability

```python
images = ZeroBucket(
    database_url=DATABASE_URL,
    pool_max_size=10,       # default: 5
    on_operation=lambda e: print(e.operation, e.duration_seconds, e.success),
)
```

Pool sizing (`pool_min_size`/`pool_max_size`/`pool_timeout`) was
previously hardcoded, now configurable. `on_operation` fires after every
storage operation with timing, retry count, and success/failure --
callback exceptions are caught and never break a real operation. See the
[full explanation](https://github.com/KedarGhadyalji/ZeroBucket#tuning-and-observability)
on GitHub.

## Limitations (read before using in production)

- **Not built for large files or high-volume media.** `get_stream()`
  avoids holding a full image in Python memory during a read (see
  above), but Postgres itself still handles the full stored value the
  same way it always has -- no true server-side memory reduction, no
  HTTP range requests, no CDN.
- **Deduplication is opt-in, not automatic.** Default `dedup=False`
  stores every upload as a separate row; pass `dedup=True` for
  content-addressed, reference-counted storage (see above).
- **PostgreSQL only, for now.** The storage layer is abstracted for
  future adapters, but only Postgres exists today.

See the full
[README](https://github.com/KedarGhadyalji/ZeroBucket#readme) and
[roadmap](https://github.com/KedarGhadyalji/ZeroBucket#roadmap) on
GitHub for more detail.

## License

MIT -- see [LICENSE](https://github.com/KedarGhadyalji/ZeroBucket/blob/main/LICENSE).
