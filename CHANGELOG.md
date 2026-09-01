# Changelog

All notable changes to this project are documented here.

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
