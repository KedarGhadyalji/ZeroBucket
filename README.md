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
screenshots, logos) to JPEG** -- it can make them larger, not smaller.
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

## Roadmap

Not yet built, tracked honestly rather than implied:

- [ ] TypeScript/npm package with an equivalent API
- [x] Optional resize/format-conversion pipeline (`optimize=True, max_width=...`)
- [ ] Deduplication with reference counting
- [ ] SQLite and MySQL adapters
- [ ] CLI (`zerobucket init`, `zerobucket migrate`, `zerobucket info`)
- [ ] Optional object-storage backend for files that outgrow the database tier

## License

MIT
