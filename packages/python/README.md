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

| Method                                                                                        | What it does                                                                                                                                                                               |
| --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `images.put(image, filename=None, optimize=False, max_width=None, format=None, quality=None)` | Validates, optionally optimizes, checksums, and stores an image. Accepts a file path, raw `bytes`, or a file-like object (including framework upload objects). Returns the new image's id. |
| `images.get(image_id)`                                                                        | Returns an `Image` (data, mime_type, filename, width, height, size_bytes, checksum_sha256). Raises `ImageNotFoundError` if missing.                                                        |
| `images.metadata(image_id)`                                                                   | Same fields as `get()` but without the raw bytes -- cheap existence/info check.                                                                                                            |
| `images.exists(image_id)`                                                                     | Returns `True`/`False`.                                                                                                                                                                    |
| `images.delete(image_id)`                                                                     | Deletes the image. Returns `True` if it existed.                                                                                                                                           |
| `images.close()`                                                                              | Releases database connections. `ZeroBucket` also works as a context manager.                                                                                                               |

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

## Limitations (read before using in production)

- **Not built for large files or high-volume media.** Full images are
  read into memory on both ends of every request -- no streaming, no
  range requests, no CDN.
- **No deduplication yet.** A SHA-256 checksum is stored on every row,
  but duplicate uploads currently create duplicate rows.
- **PostgreSQL only, for now.** The storage layer is abstracted for
  future adapters, but only Postgres exists today.

See the full
[README](https://github.com/KedarGhadyalji/ZeroBucket#readme) and
[roadmap](https://github.com/KedarGhadyalji/ZeroBucket#roadmap) on
GitHub for more detail.

## License

MIT -- see [LICENSE](https://github.com/KedarGhadyalji/ZeroBucket/blob/main/LICENSE).
