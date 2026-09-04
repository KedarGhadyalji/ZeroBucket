# Changelog

All notable changes to this project are documented here.

## [0.14.0] - 2026-09-03

### Added

- Object-storage tiering, the second item on Stage 5's roadmap. New
  `ObjectStorage` class (`object_storage.py`), a new `object_storage=`
  parameter on `ZeroBucket`/`PostgresBackend`, and a new
  `tier_to_object_storage(image_id)` method.

### Scope decisions -- confirmed with the requester before writing code, not assumed

Three real design decisions were raised as explicit options before any
implementation started (trigger mechanism, storage target, read-path
transparency), because guessing wrong on any of them would have meant
redoing real work, not adjusting a detail:

- **Trigger: explicit only**, not automatic size-based tiering at
  `put()` time. New images always land in Postgres, unchanged from
  today; `tier_to_object_storage(image_id)` is the only thing that
  moves bytes out, called deliberately (by you, or a script you write).
  Mirrors the existing `migrate_classic_to_dedup()` pattern. No built-in
  bulk/backfill command yet -- you write that loop yourself over
  whatever selection criteria fit your data.
- **Storage target: S3-compatible only, via `boto3`**, added as an
  OPTIONAL dependency (`pip install zerobucket[s3]`) -- not a generic
  pluggable backend interface, and no local-filesystem tiering. Covers
  AWS S3 plus anything speaking the same API (R2, MinIO, B2,
  DigitalOcean Spaces) through one client. `zerobucket.object_storage`
  defers its `import boto3` to `ObjectStorage.__init__` specifically so
  plain `import zerobucket` never requires boto3 at all -- confirmed
  directly: a fresh venv install without the `[s3]` extra has no boto3
  in `pip freeze`, and `import zerobucket` still succeeds.
- **Read path: fully transparent.** `get()`, `get_many()`,
  `get_stream()`, `stream_to()`, `metadata()`, `exists()` all work
  identically regardless of where a given image's bytes actually live.
  `delete()` additionally cleans up the object-storage copy for tiered
  images.

Not available with `dedup=True` in this first pass -- combining
content-addressed storage (one blob shared by many ids) with tiering
was judged a meaningfully bigger, riskier problem than tiering classic-
mode rows. `ZeroBucket(dedup=True, object_storage=...)` raises
`ValueError` immediately at construction.

### The transactional-safety guarantee, and what it costs

`tier_to_object_storage()` uploads to object storage INSIDE the same
Postgres transaction as the row lock (`SELECT ... FOR UPDATE`) and the
subsequent `UPDATE` that flips `storage_backend`. If the upload fails
for any reason, the exception propagates, the whole transaction rolls
back, and the row is left completely untouched -- still fully in
Postgres, exactly as if tiering had never been attempted. There is no
window where an image's bytes exist in neither location, and no window
where a row claims to be tiered but the upload never completed.
Verified with a dedicated test that injects a failing upload and
confirms the row is byte-for-byte unchanged afterward, not just that an
exception was raised.

The cost, stated directly rather than left implicit: this holds a
Postgres row-level lock for the entire duration of the upload -- a real
network call, potentially slow for a large image. A concurrent
`get()`/`delete()`/`tier_to_object_storage()` call on that SAME
`image_id` blocks until tiering finishes; every other row is completely
unaffected. Accepted as a deliberate simplicity/safety tradeoff for
what's expected to be an infrequent, explicitly-triggered maintenance
operation, not a hot request path.

On retry (no caller-supplied `connection=`, a transient mid-operation
failure): the whole operation, including the object-storage upload,
gets replayed. This is safe specifically because the upload key is
deterministic (`str(image_id)`) and S3's `PutObject` overwrites
silently -- a retried upload to the same key is a harmless no-op, not a
correctness risk.

### Schema change -- additive and idempotent, safe against existing data

```sql
ALTER TABLE zerobucket_images ALTER COLUMN data DROP NOT NULL;
ALTER TABLE zerobucket_images ADD COLUMN IF NOT EXISTS storage_backend TEXT NOT NULL DEFAULT 'postgres';
ALTER TABLE zerobucket_images ADD COLUMN IF NOT EXISTS object_storage_bucket TEXT;
ALTER TABLE zerobucket_images ADD COLUMN IF NOT EXISTS object_storage_key TEXT;
-- plus a CHECK constraint (added only if not already present) enforcing:
-- exactly one of (postgres row with data) or (tiered row with a
-- pointer) is ever true, never both, never neither.
```

Runs automatically via the existing `auto_migrate=True` path, same as
every prior schema change in this project. Every row that existed
before this release already satisfies the new `CHECK` constraint
without being touched, via the `storage_backend` column's default —
verified by running this migration against a database with existing
classic-mode rows already in it, not just against a fresh empty schema.

### A genuine capability upgrade, not just parity

`get_stream()` on a tiered image delegates to `ObjectStorage.
download_stream()`, which uses REAL HTTP byte-Range requests against
S3 -- strictly better than the Postgres-backed path's `substring()`-
based approach (see v0.11.0's entry), which still transfers the full
value every time regardless of chunking. Worth stating plainly: tiered
images get a genuinely different, more capable streaming
implementation, not the same one pointed at a different byte source.

### A real bug caught during development, not assumed away

- The first implementation attempt tried testing `ObjectStorage`
  against a standalone `moto_server` subprocess (a real running
  S3-compatible HTTP server) for the most realistic possible test, the
  same philosophy as using real Postgres instead of mocks throughout
  this project. It proved unreliable in this project's sandbox
  specifically: backgrounded server processes did not consistently
  survive between separate tool invocations, and the subprocess itself
  intermittently hung the calling shell entirely (traced to a `pkill`
  invocation stalling with no matching processes present, and separately
  to `moto_server`'s own forking/reload behavior holding output pipes
  open). Switched to `moto.mock_aws()`, an in-process context manager
  that patches botocore's HTTP layer directly -- the standard, widely-
  used way most boto3-based projects test against AWS APIs, not a
  compromise invented only for this environment. Both `object_storage.py`
  itself and the full `tier_to_object_storage()` round trip were
  re-verified against it before proceeding, so this pivot cost a false
  start but not any actual test coverage.
- A copy-paste-derived relative-import bug (`from ..exceptions import
StorageError` inside `object_storage.py`, one directory level too
  deep -- the pattern was copied from `adapters/postgres.py`, which
  really is one level deeper) was caught immediately by actually
  importing the new module, before it ever reached the test suite.

### Files delivered

- New: `object_storage.py`, `tests/test_tiering.py` (16 new tests)
- Changed: `adapters/postgres.py` (schema, `_SELECT_FULL`/`get`/
  `get_many`/`get_stream`/`delete`/`delete_many` updated for tiered
  rows, new `tier_to_object_storage()`, `object_storage=` constructor
  param, `dedup=True` + `object_storage=` rejected at construction),
  `client.py` (`object_storage=` param, `tier_to_object_storage()`
  client method), `__init__.py` (export `ObjectStorage`, version),
  `pyproject.toml` (version, new `s3` optional extra, `boto3`/`moto[s3]`
  added to dev dependencies), `tests/conftest.py` (new `s3_bucket`/
  `object_store`/`tiered_images` fixtures), both READMEs (new
  "Object-storage tiering" section, updated Limitations/quick-reference/
  roadmap checkbox)

223/223 tests pass (16 new), lint clean, mypy shows the same
pre-existing categories of findings as prior releases plus one new,
deliberate, explicitly-commented `type: ignore[attr-defined]`
(`tier_to_object_storage()` is intentionally NOT part of the generic
`StorageBackend` interface -- see client.py's comment for why). Built,
`twine check`ed, and functionally verified end-to-end from a genuinely
fresh venv install of the built wheel TWICE: once without the `[s3]`
extra (confirming `import zerobucket` still works and boto3 is absent
from `pip freeze`, and that constructing `ObjectStorage` without boto3
installed raises a clear `ImportError` with the install instructions),
and once with `[s3]` installed (confirming the full tier → transparent-
read → delete round trip, idempotent re-tiering, and the clear-error
behavior for a second `ZeroBucket` instance without `object_storage=`
encountering an already-tiered row).

## [0.13.0] - 2026-09-02

### Added

- `AsyncZeroBucket`, an async client for the first item on Stage 5's
  remaining roadmap ("async client support"). Built on **psycopg3's own
  native async mode** (`AsyncConnection`/`AsyncConnectionPool`) -- NOT
  the third-party `asyncpg` package the roadmap had named. Verified
  directly from a fresh venv install: `pip freeze` shows no `asyncpg`
  dependency. See "Technical correction" below for why.
- `AsyncPostgresBackend` (`adapters/postgres_async.py`) and
  `AsyncStorageBackend` (`adapters/base_async.py`), the async
  counterparts to `PostgresBackend`/`StorageBackend`. Reuse the exact
  same SQL query strings and schema DDL as the sync adapter (imported,
  not copy-pasted) -- one schema, two ways of executing the same
  queries.
- `AsyncZeroBucket` methods: `put`, `put_many`, `get`, `get_many`,
  `get_stream`, `stream_to`, `metadata`, `exists`, `delete`,
  `delete_many`, `close`, plus `async with` support (`__aenter__`/
  `__aexit__`).

### Technical correction, stated directly rather than silently substituted

- The roadmap said `asyncpg`. This library is built on psycopg3, which
  already ships a real async driver mode using the exact same SQL,
  schema, and connection string as the sync adapter. Adding the literal
  `asyncpg` package on top would have meant maintaining two SQL layers
  against two different drivers for the same feature, for zero benefit
  to an async FastAPI/Django-async caller -- they get the same
  `await zb.get(id)` either way. This was caught and corrected before
  writing any code, not discovered partway through.

### Scope: first pass, not full parity -- stated plainly, not implied

`AsyncZeroBucket` covers core operations + streaming reads, classic mode
only. Deliberately NOT included in this pass (all present on the sync
`ZeroBucket`, none architecturally blocked from reaching async later):

- `dedup=True` (content-addressed storage).
- `before_get`/`before_put` access-control hooks.
- `on_operation` observability hook.
- `optimize=True` (resize/re-encode pipeline) and custom `validator=`.
- `connection=` transaction participation.
- Automatic retry/backoff on transient errors.

This was a scope decision confirmed with the requester up front (options
ranged from "core only" to "full parity with the sync client"), not a
default that emerged from running out of time partway through.

### Two implementation decisions worth being explicit about

- **Image validation and file reading run via `asyncio.to_thread()`.**
  Pillow has no async API; offloading this work to a thread keeps the
  event loop responsive while it runs, at the cost of consuming a
  thread from Python's default executor. A direct, verified benefit:
  `put_many()`'s per-item validation now runs CONCURRENTLY via
  `asyncio.gather`, not serially in a Python loop like the sync
  client's does -- confirmed with a timing test (5 items with an
  artificial 0.2s validation delay each complete well under the ~1.0s a
  serial implementation would take), not just asserted as a property.
- **`get_stream()` is a coroutine that RETURNS an async iterator, not an
  async generator function itself.** `stream = await
images.get_stream(id)` raises `ImageNotFoundError` immediately on
  await, matching the sync client's eager-raise behavior; if this were
  an async generator instead, Python would defer running any of its
  code -- including the not-found check -- until the first `async for`
  iteration, which would silently change when the error surfaces
  compared to every other method in this library. Verified directly: a
  test asserts `get_stream(id)`'s return value from calling it is a
  bare coroutine object before being awaited.

### Bug caught during development, not assumed away

- **Windows: the async test suite failed outright on first real-world
  testing** (24 failures/errors, all one root cause) -- psycopg3's
  async mode cannot run under Windows' default `ProactorEventLoop`,
  only a `SelectorEventLoop`. Confirmed directly against psycopg's own
  installed source (`connection_async.py`): psycopg itself DOES raise a
  clear, specific `InterfaceError` for this -- but `psycopg_pool.
AsyncConnectionPool`'s background connect worker catches that error,
  logs a WARNING per retry attempt, and keeps retrying silently until
  the whole pool times out ~10+ seconds later with a generic
  `PoolTimeout` that buries the real, actionable cause underneath.
  Fixed two ways: (1) `AsyncPostgresBackend._ensure_ready()` now checks
  for this exact condition itself, before calling `pool.open()`, and
  raises a clear, immediate `StorageError` with version-appropriate fix
  guidance instead of waiting through the masked timeout; (2)
  `tests/conftest.py` now sets `WindowsSelectorEventLoopPolicy` at
  import time on `win32`, so the test suite itself actually runs on
  Windows dev machines rather than every async test failing at fixture
  setup. Documented in both READMEs' Async support sections, including
  the exact `asyncio.run()` incantation for Python 3.12+ vs earlier.
  Caught by the person testing this on a real Windows machine before
  deploying -- not found in this sandbox, which is Linux-only and could
  not have surfaced it.
- `psycopg.AsyncCursor.nextset()` and `.rowcount` are NOT awaitable,
  even on the async cursor class -- verified by actually running
  `put_many()` against real Postgres and hitting
  `TypeError: object bool can't be used in 'await' expression`, not
  discovered by reading documentation alone. Fixed before this was
  committed; covered by `put_many`'s existing round-trip tests, which
  would fail immediately if this regressed.

### Lazy initialization, and why

- `AsyncPostgresBackend.__init__` cannot be a coroutine (Python has no
  `async __init__`), so the connection pool is constructed unopened and
  opened -- along with running the schema migration, if
  `auto_migrate=True` -- on the FIRST actual async call, guarded by an
  `asyncio.Lock` so concurrent first-callers can't race to open/migrate
  twice. Verified with a dedicated test: 20 concurrent `exists()` calls
  as the very first thing done with a fresh instance all succeed and
  return consistent results.

### Files delivered

- New: `adapters/base_async.py`, `adapters/postgres_async.py`,
  `async_client.py`, `tests/test_async_client.py` (27 new tests)
- Changed: `__init__.py` (export `AsyncZeroBucket`,
  `AsyncPostgresBackend`), `pyproject.toml` (version, new
  `pytest-asyncio` dev dependency, `asyncio_mode = "auto"`),
  `tests/conftest.py` (new `async_images` fixture), both READMEs (new
  "Async support" section), root `README.md`'s roadmap checkbox

207/207 tests pass (29 new), lint clean, mypy shows only the same
pre-existing categories of findings the sync codebase already carries
(no new categories introduced). Built, `twine check`ed, and functionally
verified end-to-end (put/get, streaming, batch ops, not-found,
concurrent first-call init, context manager) from a genuinely fresh venv
install of the built wheel, not just the source tree -- and confirmed
`pip freeze` on that fresh install shows no `asyncpg` dependency.

Stated honestly: all of the above verification, including the Windows
fix's effect on the test suite, was run in this project's Linux sandbox
-- there is no Windows machine available here. The Windows fix was
derived from reading psycopg's actual installed source directly (not
guessed), and the Linux/macOS suite passing confirms the `sys.platform
== "win32"` guards don't affect non-Windows behavior at all, but the
Windows-specific code paths themselves (both the conftest.py policy
fix and the fast-fail error) still need confirmation on a real Windows
run before this is considered fully verified there.

## [0.12.0] - 2026-09-02

### Added

- `before_get(image_id, context) -> bool` and `before_put(context) -> bool`
  authorization hooks, passed to the `ZeroBucket` constructor. Denying a
  call raises the new `AccessDeniedError` (exported from the package
  root) and never reaches the database.
- `AccessDeniedError(ZeroBucketError)`, with `.operation` and
  `.image_id` (the latter `None` for `before_put` denials).
- `context: dict | None = None` parameter added to `get()`, `get_many()`,
  `get_stream()`, `stream_to()`, `metadata()`, `put()`, and `put_many()`
  -- passed straight through to whichever hook is configured, unused if
  neither is. `ZeroBucket` never inspects `context` itself.

### Scope decisions, stated directly rather than left implicit

- **`before_get` gates**: `get()`, `get_many()` (evaluated once per id,
  independently -- a batch can mix ids from different owners),
  `get_stream()`/`stream_to()` (evaluated once per call, not once per
  chunk -- chunks are an implementation detail of one already-authorized
  read, not separate reads), and `metadata()` (still per-image
  information tied to a specific id).
- **`before_get` does NOT gate `exists()`.** A bare existence check
  returns no image data or metadata; gating it wasn't part of the
  original ask and would double the round-trip cost of what's meant to
  be a cheap check. Callers who need that can gate it themselves at the
  call site -- documented as a stated limitation, not a silent gap.
- **`before_put` gates `put()` and `put_many()`, but `put_many()`
  evaluates it exactly ONCE for the whole call**, not once per item.
  `context` represents who's making the call, not per-item data, so one
  evaluation covers the batch; a denial marks every item's result as
  `error="access denied"` without touching any of them, rather than
  partially processing the batch. Verified with a call-counting test,
  not just asserted in a docstring.
- **A hook that raises fails closed, not open.** If `before_get`/
  `before_put` raise instead of returning a bool, that exception
  propagates directly (single-item calls) or is captured per-item /
  reported for the whole batch (`get_many()`/`put_many()`) -- it is
  NEVER caught and treated as an implicit allow. This is the opposite of
  `on_operation`'s existing behavior (fire-and-forget metrics, where
  swallowing exceptions is the safe default) -- these are security
  decisions, where swallowing would be a real hole. Covered by dedicated
  tests using a hook that deliberately raises, for both single-item and
  batch call shapes.
- **Denied calls never reach the database.** The hook runs before any
  backend/DB call is made (for `get_many()`, denied ids are filtered out
  before the underlying batched query is even issued), so a denial
  produces no `on_operation` event and, for `put()`, does no wasted
  validation/checksum work. Verified directly: one test swaps in a
  backend method that raises `AssertionError` if called, to prove a
  denied `get()` never reaches it, not just that the right exception
  came back.

### Files delivered

- Changed: `exceptions.py` (`AccessDeniedError`), `client.py`
  (`before_get`/`before_put` constructor params, `_check_before_get`/
  `_check_before_put` helpers, `context=` on all seven gated methods),
  `__init__.py` (export `AccessDeniedError`), `pyproject.toml` (version
  only), both READMEs (new "Access control" section, quick-reference
  table, roadmap checkbox)
- New: `tests/test_access_control.py` (27 new tests)

178/178 tests pass (27 new), lint clean.

## [0.11.0] - 2026-09-01

### Added

- `get_stream(image_id, chunk_size=1MB, connection=None)`: retrieve an
  image as an iterator of chunks instead of one complete `bytes` object.
  Implemented via repeated `substring(data FROM offset FOR length)`
  queries -- never materializes the full value in Python memory at once.
  Works identically in classic and dedup mode.
- `stream_to(image_id, destination, chunk_size=1MB, connection=None)`:
  convenience wrapper that loops over `get_stream()` and writes each
  chunk to `destination` (an open file, an HTTP response object,
  anything with `.write(bytes)`), returning the total byte count.
- `DEFAULT_STREAM_CHUNK_SIZE` (1 MiB), exported from the package root.
- `put()`/`put_many()` now read file-like input (an open file, a
  framework upload object) in bounded chunks and reject an oversized
  upload as soon as they've read one byte past `max_bytes`, instead of
  first buffering the entire stream into memory and only then checking
  the size. Peak memory for a rejected oversized upload is now bounded
  by `max_bytes`, not by the (potentially much larger, even unbounded)
  size of the input stream.

### Why this is "streaming reads/writes," specifically, and not more than that

- This does NOT make BYTEA-in-Postgres support arbitrary-size streaming
  ingestion. Checksum computation and image validation (Pillow decode)
  both require the complete byte content -- there is no way to validate
  "is this an undamaged JPEG" from a prefix of the bytes, so a `put()`
  of anything under `max_bytes` still ends up fully in memory here, same
  as before. What changed is bounded, fail-fast rejection of oversized
  input -- a real, if narrower, improvement, not the "streaming writes of
  arbitrarily large files" a bucket-store name might suggest.
- `get_stream()` reduces PYTHON-side memory pressure per read. It does
  NOT reduce POSTGRES-side memory/IO cost -- the server still handles
  the full stored value the same way it always has for a BYTEA column
  (TOAST detoast, etc). It is also not an HTTP range/partial-content
  feature: the full image is still transferred, just paced out in
  pieces, not a subset of it. Both limitations are stated directly in
  `get_stream()`'s docstring and the README, not glossed over.
- Without `connection=` spanning the whole read, each chunk of
  `get_stream()` is its own round trip with no snapshot isolation across
  chunks. A concurrent `delete()` between chunks raises `StorageError`
  rather than silently returning a short/truncated stream -- a truncated
  image passed off as complete would be a much worse failure mode than
  a loud one. Covered by a dedicated test that deletes the row mid-
  stream via a separate connection and confirms the raise, not just
  documented as a claim.

### Verified before being built, not assumed

- `substring()` is 1-indexed and clamps `length` at the value's actual
  end in Postgres, so the final chunk of a stream naturally comes back
  shorter with no special-casing needed -- exercised directly by a test
  using a chunk_size that doesn't evenly divide the image size.
- The bounded-read write path was verified with a counting file-like
  wrapper that a 5MB oversized upload against a 200-byte cap is rejected
  having read only a small, bounded amount -- not the full 5MB -- rather
  than just asserting the exception type.
- `bytes`/path input (already fully in memory, or a single
  `read_bytes()` call) is unaffected: `ImageTooLargeError.size_bytes`
  stays the true exact size there. Only file-like input's reported
  `size_bytes`, when rejected, is a lower bound (wherever reading
  stopped) rather than the stream's true total size -- finding the true
  size would mean reading all of it, which is exactly the cost this
  feature avoids. Both behaviors are covered by dedicated tests.

### Files delivered

- Changed: `adapters/base.py` (new `get_stream` abstract method),
  `adapters/postgres.py` (`get_stream` implementation, chunk-select
  queries, `DEFAULT_STREAM_CHUNK_SIZE`), `client.py` (`get_stream`,
  `stream_to`, bounded `_read_image_input` for file-like input),
  `__init__.py` (export `DEFAULT_STREAM_CHUNK_SIZE`), `pyproject.toml`
  (version only), both READMEs (new streaming section, updated
  Limitations/quick-reference/`on_operation` operation list), root
  `README.md`'s roadmap checkbox
- New: `tests/test_streaming.py` (15 new tests)

151/151 tests pass (15 new), lint clean.

## [0.10.0] - 2026-08-31

### Added

- `pool_min_size`/`pool_max_size`/`pool_timeout` on `ZeroBucket(...)` --
  previously hardcoded (1/5/10) connection pool settings are now
  configurable. Defaults unchanged, so this is purely additive.
- `on_operation` callback: fires after every storage operation (`put`,
  `put_many`, `get`, `get_many`, `get_metadata`, `delete`,
  `delete_many`, `exists`, `migrate`) with an `OperationEvent` (timing,
  success/failure, error, retry count). Wire it to your own metrics
  backend -- ZeroBucket does not ship a specific integration.
- `OperationEvent`, exported from the package root.

### Design notes

- Dedup-mode operations report the SAME operation names as their
  classic-mode counterparts -- logically the same operation from a
  metrics perspective, regardless of storage mode underneath.
- `get()` on a missing id reports `success=True` in its event (the
  database query correctly found no row) even though `get()` itself
  then raises `ImageNotFoundError` to the caller -- the event measures
  the storage operation, not the application-level outcome. Documented
  explicitly and covered by a dedicated test, since this boundary is
  easy to get wrong or leave ambiguous.
- Exceptions raised inside `on_operation` are caught and silently
  ignored -- verified by a dedicated test that a deliberately broken
  callback cannot prevent a real `put()`/`get()`/`delete()` from
  succeeding.
- `connection=` calls always report `retry_count=0` in their event,
  consistent with the existing rule that automatic retry never applies
  on that path (see v0.7.0).

## [0.9.0] - 2026-08-30

### Added

- Opt-in deduplication (`ZeroBucket(dedup=True)`): content-addressed
  storage with reference counting. Byte-identical uploads share one
  stored copy; bytes are only actually deleted when the last
  referencing id is deleted.
- Uses SEPARATE tables from classic mode (`zerobucket_blobs` /
  `zerobucket_image_refs`, not `zerobucket_images`) -- a deliberate
  safety decision so enabling dedup can never collide with or
  misinterpret existing classic-mode data. The two modes can safely
  coexist against the same database (tested directly, not just claimed).
- `migrate_classic_to_dedup()`: a non-destructive, explicit migration
  path for existing classic-mode data -- preserves every original id
  exactly, correctly deduplicates content found along the way, and does
  not modify or delete the source table.
- `put_many()`/`get_many()`/`delete_many()` all work correctly in dedup
  mode, including the tricky cases: repeated identical content within
  one batch correctly accumulates the reference count, and batch deletes
  correctly handle a mix of shared and unique checksums in one call.
- `connection=` (transactional atomicity) works correctly in dedup mode
  too, including the two-table case (a rollback undoes both the blob
  insert and the reference insert together, not just one).

### Verified before being built, not assumed

- The core `INSERT ... ON CONFLICT DO UPDATE` upsert pattern was
  stress-tested under 20 real concurrent threads incrementing the same
  counter, BEFORE any application code was written on top of it --
  confirmed zero lost updates. A further test exercises this through
  the real `ZeroBucket` client with 15 concurrent `put()` calls for
  identical content and confirms the exact reference count.
- Repeated identical checksums within a single `put_many()` batch were
  verified (via raw `executemany` testing first, then through the real
  client) to correctly accumulate the reference count rather than only
  registering the first occurrence.

### Fixed (test infrastructure, not the library)

- Corrected a test-fixture bug where truncating the two dedup tables in
  separate statements failed under Postgres's foreign-key constraints --
  they must be truncated together in one statement.

## [0.8.0] - 2026-08-29

### Added

- Pluggable content validators: `put(validator=...)` and
  `put_many(validator=...)` accept a `ContentValidator` to store content
  types ZeroBucket doesn't natively validate as an image (PDFs, or
  anything else you write a validator for).
- `zerobucket.validators.pdf.PDFValidator`: a complete, real reference
  implementation -- content-sniffed (`%PDF-` magic bytes), configurable
  size ceiling, with an explicit security-scope note (it does not parse
  PDF internals or detect embedded JavaScript/forms -- stated plainly,
  not glossed over).
- `ContentValidator` (ABC) and `ValidatedContent` (result type), exported
  from the package root for writing your own validators.
- `ContentValidationError`, a new base exception. `ImageValidationError`
  now subclasses it instead of `ZeroBucketError` directly -- fully
  backward compatible (verified by a dedicated test): any existing
  `except ImageValidationError` or `except ZeroBucketError` still catches
  exactly what it always did.

### Why a pluggable hook instead of native PDF support

- A PDF is a categorically richer, more dangerous format to fully secure
  than a raster image (embeddable JavaScript, forms, launch actions).
  Absorbing that directly into ZeroBucket's core would mean either
  under-securing it or expanding what "database-native image storage"
  promises to guarantee. The pluggable hook lets adopters opt into that
  tradeoff explicitly, for the specific content type they need.

### Design notes (verified before building, not assumed)

- `width`/`height` were already nullable in both the database schema and
  the `Image`/`ImageMetadata` types before this feature existed --
  confirmed by inspection, not assumed. This meant the entire read path
  (`get`, `get_many`, `exists`, `delete`, `metadata`) needed ZERO changes
  to support non-image content; only the write path (`put`, `put_many`)
  needed the new hook.
- `optimize=True` is incompatible with `validator=` and raises
  immediately with a clear message, rather than letting Pillow fail
  confusingly against bytes that were never claimed to be an image.

## [0.7.0] - 2026-08-28

### Added

- Automatic retry with exponential backoff (+ jitter, capped at 2s) for
  transient database errors -- connection drops, deadlocks,
  serialization failures. Configurable via `max_retries` (default 3) and
  `retry_base_delay` (default 0.1s) on `ZeroBucket(...)`. Set
  `max_retries=0` to disable entirely.
- Classification (`_is_retryable`) is SQLSTATE-based for server-returned
  errors and `OperationalError`-based for connection-level failures --
  verified against real psycopg exception attributes during development
  (`exc.sqlstate`, confirmed empirically rather than assumed), not
  guessed at.

### Important safety rule

- Automatic retry applies ONLY to ZeroBucket's own internally-pooled
  connections. Calls that pass their own `connection=` (see the
  Transactions feature from 0.4.0) are retried **zero** times,
  regardless of `max_retries` -- retrying a statement on a connection
  the caller is managing themselves could silently corrupt their
  transaction's semantics. This interaction is tested directly, not
  just documented.

### Honest limitation

- The retry loop's own logic (does it retry, how many times, does
  connection= correctly bypass it) is tested via controlled failure
  injection at the `_run()` level, not by triggering a genuine live
  network partition or concurrent-transaction deadlock -- those are
  inherently flaky to reproduce deterministically in CI. Classification
  logic (`_is_retryable`) IS tested against real psycopg exception
  instances.

## [0.6.0] - 2026-08-27

### Added

- `put_many()`, `get_many()`, `delete_many()` batch operations.
  Best-effort semantics (one bad item doesn't abort the rest of the
  batch) -- results carry per-item `.success`/`.error`, not a single
  all-or-nothing outcome.
- `get_many()`/`delete_many()` are genuine single-query batch operations
  (`WHERE id = ANY(...)`), not a loop of individual calls.
- `put_many()` still validates/optimizes each image individually in
  Python (inherent per-image work), but batches the actual database
  writes via `executemany(returning=True)` -- verified empirically that
  this preserves input-to-output order correctly (architectural
  guarantee, not an assumption), which is what makes it safe to
  correlate results back to input positions.
- `BatchPutResult`, `BatchGetResult`, `BatchDeleteResult` types,
  exported from the package root.

## [0.5.0] - 2026-08-27

### Added

- `zerobucket` CLI, installed as a real console script
  (`pip install zerobucket` gives you the `zerobucket` command directly):
  - `init` -- create the schema if it doesn't exist
  - `migrate` -- currently identical to `init`; kept as a stable command
    name for when real versioned migrations exist (there's only ever
    been one schema shape so far, so there's nothing to migrate yet --
    documented honestly rather than implying more than exists)
  - `info` -- image count, total size, on-disk size (including TOAST),
    breakdown by format
  - `verify` -- re-checksums every stored image against its recorded
    SHA-256 to detect corruption; streams one image at a time rather
    than loading the whole table into memory, exits non-zero on any
    mismatch (usable in cron/CI), supports `--sample N` for large tables

## [0.4.0] - 2026-08-27

### Added

- `connection=` parameter on `put()`, `get()`, `metadata()`, `exists()`,
  and `delete()` -- pass your own open `psycopg` connection to make an
  operation participate in your application's own transaction (commits
  or rolls back together with the rest of your writes), instead of
  ZeroBucket's default of committing independently on its own internal
  connection pool.
- `docs/OPERATIONS.md`: backup guidance (splitting `pg_dump` so routine
  app backups don't slow down as image data grows) and autovacuum tuning
  notes for the BYTEA-heavy table.

### Important finding

- **Verified by direct experiment, not assumed:** without `connection=`,
  `put()` was shown to commit independently even when a concurrent
  application transaction on a separate connection rolled back -- i.e.
  the "atomic write, no orphaned uploads" guarantee some database-native
  storage designs imply is NOT automatic here. It's real now, but only
  when `connection=` is actually used. See the README's "Transactions"
  section for the full explanation and a worked example.

## [0.3.0] - 2026-08-26

### Added

- HEIC/HEIF format support (iPhone photos), via the optional
  `pip install zerobucket[heic]` extra (`pillow-heif`). Not in the base
  install, since it pulls in a native library and not everyone needs it.
- `put()` accepts HEIC input directly; `optimize=True, format="jpeg"` (or
  `"webp"`) converts it, which matters because most browsers still can't
  display HEIC natively.
- `format="heic"` (or `"heif"`) also works as an `optimize=True` output
  target, for symmetry -- though the primary real-world need is HEIC-in,
  not HEIC-out.
- Uploading a HEIC file without the optional dependency installed now
  raises a clear, actionable error (magic-byte sniffed) instead of a
  confusing "corrupted image" message.

### Notes

- `DEFAULT_HEIC_QUALITY` is a reasonable starting default, _not_ verified
  with the same SSIM measurement process as the JPEG/WebP defaults --
  flagged explicitly in the source rather than implied to be
  equally rigorous.

## [0.2.0] - 2026-08-25

### Added

- `put(optimize=True, max_width=, format=, quality=)`: opt-in image
  optimization -- metadata stripping (EXIF/GPS/ICC), resizing (LANCZOS),
  and quality-based JPEG/WebP re-encoding.
- Quality defaults (JPEG=90, WebP=88) are backed by measured SSIM data
  across multiple content types, not guessed -- see
  `benchmarks/COMPRESSION_RESULTS.md`.
- `OptimizationResult` type, exported from the package root.

### Findings worth knowing about

- Re-encoding flat/graphic content (screenshots, logos) as JPEG can make
  it _larger_, not smaller -- confirmed by measurement, documented in
  `COMPRESSION_RESULTS.md`. Use PNG or WebP for that content instead.
- "Visually lossless" is content-dependent, not just quality-setting
  dependent: dense fine-texture images (foliage, fabric) have a lower
  achievable SSIM ceiling than smooth photos, regardless of quality.

## [0.1.1] - 2026-08-24

### Fixed

- `PostgresBackend` now closes its connection pool cleanly when setup
  fails (e.g. bad credentials), instead of leaking background worker
  threads.
- Connection acquisition timeout reduced from 30s to 10s, so
  misconfiguration surfaces faster.

### Changed

- Expanded PyPI package README with full API reference and usage
  examples (previously a placeholder).

## [0.1.0] - 2026-08-23

### Added

- Initial release: `put`, `get`, `metadata`, `exists`, `delete`.
- PostgreSQL storage adapter (BYTEA-backed).
- Content-based image validation: format sniffing, size limits,
  decompression-bomb protection, corruption detection.
- Test suite (30 tests) covering validation, client behavior, and error
  handling against a real PostgreSQL instance.
