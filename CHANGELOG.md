# Changelog

All notable changes to this project are documented here.

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
