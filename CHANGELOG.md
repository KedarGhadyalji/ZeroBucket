# Changelog

All notable changes to this project are documented here.

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
