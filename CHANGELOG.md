# Changelog

All notable changes to this project are documented here. This project
now ships two independently-versioned packages -- entries are labeled
with which one they apply to. The core `zerobucket` package's version
history continues below unbroken; `django-zerobucket` starts its own
version sequence from 0.1.0.

## [0.22.0] - 2026-09-25

### Added -- MySQL/MariaDB adapter, Phase 3 (dedup mode)

- `MySQLBackend(dedup=True)` -- content-addressed storage with
  reference counting, mirroring the Postgres/SQLite adapters' dedup
  schema exactly in shape: a `zerobucket_blobs` table keyed by
  `checksum_sha256` with a `ref_count`, and a `zerobucket_image_refs`
  table mapping ids to blobs. Every existing method
  (`put`/`put_many`/`get`/`get_many`/`get_stream`/`delete`/
  `delete_many`/`exists`/`metadata`) now branches correctly between
  classic and dedup mode. Same `dedup=True` + `object_storage=`
  rejection (raises `ValueError` immediately at construction) as
  Postgres/SQLite -- dedup mode has no tiering path in this adapter at
  all, not even a schema for it.
- **MySQL's UPSERT syntax is genuinely different from Postgres's/
  SQLite's, not just a different way of writing the same thing**:
  `INSERT ... ON DUPLICATE KEY UPDATE ref_count = ref_count + 1`
  instead of `ON CONFLICT ... DO UPDATE`. Verified this gives the
  identical atomicity guarantee, not just assumed from the syntax: 20
  concurrent threads (each its own connection, its own transaction)
  upserting the SAME checksum produced `ref_count == 20` exactly, not
  less -- InnoDB handling the increment at the row level, not this
  code's own logic. See the dedicated concurrency test.
- No `RETURNING` (see Phase 1/2 entries for the general reasoning)
  means the ref-count decrement on delete can't be one atomic
  statement here the way Postgres's `UPDATE ... RETURNING ref_count`
  is: `delete()`/`delete_many()` instead `SELECT ref_count ... FOR
UPDATE`, compute the new value in Python, then `UPDATE` -- all
  inside the row lock, closing the same race a RETURNING-based atomic
  decrement would close. `delete_many()`'s batch case aggregates how
  many refs each distinct checksum lost (via `collections.Counter`,
  same approach the SQLite adapter uses) and applies each checksum's
  decrement exactly once, not once per ref -- covered by a test with
  30 refs all sharing one blob deleted in a single call.
- **A real bug caught by actually running the migration, not assumed
  safe from the SQL being valid**: the dedup schema's two `CREATE
TABLE` statements were originally one string, same shape as every
  other adapter's schema constant -- but PyMySQL's `cursor.execute()`
  only accepts ONE statement per call by default (no
  `CLIENT.MULTI_STATEMENTS` flag set on the connection). The first
  attempt at `test_dedup_schema_created` failed immediately with a SQL
  syntax error pointing at the SECOND `CREATE TABLE`, not a passing
  test giving false confidence. Fixed by splitting into two separate
  constants (`_DEDUP_SCHEMA_BLOBS`, `_DEDUP_SCHEMA_REFS`) and two
  `execute()` calls.
- 19 new tests (54 total for the MySQL adapter now), all against a
  real MariaDB 10.11 instance -- shared-blob round-trip correctness,
  the within-one-batch ref-count accumulation claim verified directly
  (3 identical `put_many()` items -> `ref_count == 3`, checked against
  the DB, not just that the calls succeeded), correct decrement math
  when `delete_many()` removes some-but-not-all refs to a shared blob,
  blob cleanup happening exactly at `ref_count == 0` (confirmed both
  that it happens and that a still-referenced blob survives), two
  independent ids sharing one blob each streaming their own correct,
  full content, `before_get` hook compatibility, a dedup-specific
  `tier_to_object_storage()` rejection message, and the concurrency
  test above.
- Verified from a **genuinely fresh venv install of the actual built
  wheel** (`zerobucket[mysql]`): a full
  put -> put (identical content, shares the blob) -> get (both) ->
  stream -> delete one ref (other survives) -> delete the other
  (blob now gone) path, against real MariaDB. Full existing suite
  re-verified alongside this: 123 passed (54 MySQL + all SQLite
  sync/async + CLI), lint clean.

## [0.21.1] - 2026-09-24

### Fixed -- Python 3.10 compatibility bug breaking CI (and any real 3.10 install)

- `from datetime import UTC` was used in three files -- `SQLiteBackend`,
  `AsyncSQLiteBackend`, and `MySQLBackend`'s `_now()` helpers. `UTC` as
  a top-level `datetime` module attribute was added in **Python
  3.11** -- this package declares `requires-python = ">=3.10"` and
  CI's matrix tests 3.10/3.11/3.12, so importing any of these three
  adapters on a real Python 3.10 install raised `ImportError: cannot
import name 'UTC' from 'datetime'` immediately at import time, before
  a single test could even be collected.
- **This bug is OLDER than this round's MySQL work**, stated directly
  rather than glossed over: `SQLiteBackend` has had it since v0.16.0,
  `AsyncSQLiteBackend` since v0.19.0. `MySQLBackend` (v0.20.0) just
  repeated the same mistake a third time rather than introducing a new
  one. Root-caused by reading the actual CI failure (`test (3.10)`
  failed and cancelled the 3.11/3.12 jobs via fail-fast, while 3.11
  and 3.12 themselves showed no error of their own -- consistent with
  a 3.10-only import failure, not a real test failure) and confirming
  directly against Python's own changelog/reference which version
  added `datetime.UTC`, rather than guessing from the symptom alone.
- Fix: `from datetime import UTC, datetime` -> `from datetime import
datetime, timezone`, and `datetime.now(UTC)` -> `datetime.now(timezone.utc)`
  in all three files. `timezone.utc` has been available since Python
  3.2 -- identical value, just the older, universally-compatible
  spelling. Searched the entire `src/` tree (both packages) afterward
  to confirm no other occurrence of `datetime.UTC` or Python
  3.11+-only constructs (`typing.Self`, `except*`, `tomllib`) remained
  anywhere.
- Could not run the actual Python 3.10 interpreter in this sandbox to
  confirm directly (Ubuntu 24.04's default archives no longer ship a
  `python3.10` package, and this sandbox's network allowlist doesn't
  include a source with one) -- stated plainly rather than silently
  assumed fixed. Confidence here rests on `timezone.utc` being
  unambiguous, longstanding stdlib API (Python 3.2+, well before this
  project's 3.10 floor) rather than on having reproduced the failure
  locally; the real confirmation is the next CI run actually going
  green on all three matrix versions.
- No functional/behavioral change -- `datetime.now(timezone.utc)` and
  `datetime.now(UTC)` produce identical `datetime` objects; this is a
  compatibility-only fix. Full suite re-verified: 104 passed (35 MySQL
  - all SQLite sync/async + CLI), lint clean.

## [0.21.0] - 2026-09-23

### Added -- MySQL/MariaDB adapter, Phase 2 (streaming reads + object-storage tiering)

- `MySQLBackend.get_stream()` -- ranged `SUBSTRING(data FROM ... FOR
...)` queries, the same SQL-standard form Postgres's `substring()`
  -based streaming uses, confirmed to behave identically on
  MySQL/MariaDB (1-indexed, clamps at the value's end) rather than
  assumed just because the syntax is spelled the same. For a TIERED
  row, delegates to `ObjectStorage.download_stream()` instead -- real
  S3 byte-Range requests.
- `MySQLBackend.tier_to_object_storage()` and `object_storage=` on the
  constructor. Same safety guarantee as every other adapter: the
  object-storage upload happens INSIDE the same transaction as the row
  lock/update, so a failed upload rolls back the whole transaction and
  leaves the row completely untouched -- verified with a test that
  injects a failing upload and confirms the row is byte-for-byte
  unchanged afterward, not just that an exception fired.
- **A genuine, verified improvement over SQLite's equivalent**: MySQL/
  MariaDB's InnoDB storage engine has real per-row locking via `SELECT
... FOR UPDATE` -- unlike SQLite (no row-level locking at all,
  falls back to locking the WHOLE database file via `BEGIN IMMEDIATE`
  for the same safety guarantee), tiering one image on this backend
  never blocks writes to any other row, matching Postgres exactly.
  Confirmed with a dedicated, deterministic test (short
  `innodb_lock_wait_timeout` + a deliberately slowed-down upload): a
  concurrent write to a DIFFERENT row succeeds immediately while
  tiering is in flight, while a concurrent write to the SAME row
  blocks and times out. First attempt at this test used a lock timeout
  longer than the simulated upload delay and silently passed for the
  wrong reason (the second write just waited then succeeded, never
  hit the timeout) -- caught by an isolated debug script showing the
  real wait time, not assumed correct from the first green run;
  widened the gap between the two durations to fix it for real.
- Additive, idempotent migration (`_migrate_tiering()`) adds
  `storage_backend`/`object_storage_bucket`/`object_storage_key` to an
  existing Phase 1 table and a CHECK constraint enforcing "exactly one
  of MySQL-resident or tiered, never both, never neither" -- checked
  via `information_schema` directly rather than relying on `ADD COLUMN
IF NOT EXISTS`, since this project would rather not depend on a
  specific minimum MySQL/MariaDB version being confirmed correct for
  every reader's deployment. Existing rows are untouched (satisfy the
  constraint via the column default). Stated directly: the CHECK
  constraint itself requires MySQL 8.0.16+/MariaDB 10.2.1+ to be
  enforced (both silently ignore it before those versions) -- a
  minimum-version requirement for tiering specifically, not silently
  assumed to work everywhere.
- `delete()`/`delete_many()` now also clean up the tiered object (if
  any) after the MySQL row is gone -- same deliberate ordering as
  every other adapter (row-gone-first, S3-cleanup-best-effort-after):
  a failed S3 delete leaves a harmless orphan, not a data-integrity
  problem.
- 12 new tests (35 total for the MySQL adapter now), all against a
  real MariaDB 10.11 instance plus moto's in-process S3 emulator --
  streaming round-trip correctness, small-chunk exactness, a
  concurrent-delete-mid-stream test confirming this backend raises
  (matching Postgres, NOT sync SQLite's WAL-survives-it behavior --
  each chunk is its own round trip with no held snapshot), tiering
  idempotency, not-found, the row-lock isolation test above, and the
  failed-upload-leaves-row-untouched safety guarantee.
- Verified from a **genuinely fresh venv install of the actual built
  wheel** (`zerobucket[mysql,s3]`): the full
  put -> stream -> tier -> get (now transparently from S3) ->
  stream (now transparently from S3) -> delete (cleans up both sides)
  path, against real MariaDB and mocked S3, not just against the
  source tree. Full existing suite re-verified alongside this: 104
  passed (35 MySQL + all SQLite sync/async + CLI), lint clean.

## [0.20.0] - 2026-09-23

### Added -- MySQL/MariaDB adapter, Phase 1 of a multi-phase build (see roadmap item #1)

- `MySQLBackend` (`zerobucket.adapters.mysql`), the first of the two
  remaining databases from the original roadmap. This phase ships
  **classic-mode core CRUD only**: `put`, `put_many`, `get`,
  `get_many`, `get_metadata`, `delete`, `delete_many`, `exists`. NOT
  yet implemented, tracked as explicit follow-up phases rather than a
  silent gap: `get_stream()` (raises `NotImplementedError` with a
  clear message), object-storage tiering, dedup mode, async support,
  and connection pooling. Same phased approach that shipped the SQLite
  adapter (v0.16.0 -> v0.19.0).
- Uses **PyMySQL** (pure-Python, no compiled extension), installed via
  the new `zerobucket[mysql]` optional extra -- confirmed via two
  separate fresh-venv installs from the actual built wheel that plain
  `import zerobucket` works with PyMySQL completely absent (and raises
  a clear `StorageError`, not an `ImportError`, if you try to construct
  a `MySQLBackend` without it), and that the full put/get/delete round
  trip works for real against a real MariaDB instance once
  `zerobucket[mysql]` is installed.
- Three genuine, verified MySQL/MariaDB-specific design divergences
  from the Postgres and SQLite adapters, documented directly in the
  module rather than left to be discovered:
  - **No `RETURNING`.** MariaDB 10.5+ has it, but real MySQL 8.0 does
    not, on any statement -- confirmed against MySQL's own reference
    docs rather than assumed from the MariaDB instance this was tested
    against. `put()`/`put_many()` don't need it (the id is generated
    in Python before the `INSERT`, same as SQLite). `delete_many()`
    instead uses `SELECT ... FOR UPDATE` immediately before the
    `DELETE`, inside one transaction -- the row lock closes the same
    concurrent-delete race a `RETURNING`-based statement would close,
    just as two statements instead of one. Covered by a dedicated test
    that mixes real and fake ids and confirms only the real ones come
    back as deleted.
  - **Indexes are declared inline inside `CREATE TABLE IF NOT EXISTS`**
    (`INDEX name (col)`), not via a separate `CREATE INDEX IF NOT
EXISTS` the way the Postgres/SQLite adapters both do it. Real MySQL
    8.0 does not support `IF NOT EXISTS` on `CREATE INDEX` at all
    (MariaDB does, since 10.1.4) -- inline index declarations sidestep
    the incompatibility entirely since both engines support the whole
    idempotent `CREATE TABLE IF NOT EXISTS` uniformly.
  - **No connection pool in this phase**, unlike Postgres. Unlike
    SQLite (a local file, where "no pool" is a deliberate, permanent
    design choice), MySQL is a genuinely networked database where
    connection setup has a real cost -- this is stated as a Phase 1
    scope decision to keep, not a judgment that it's the right
    long-term design. `pool_min_size`/`pool_max_size`/`pool_timeout`
    (matching `PostgresBackend`'s own knobs) are explicit follow-up
    work.
- 23 new tests, all against a **real MariaDB 10.11 instance** (this
  project's sandbox) -- round-trip correctness, a large (>100KB, random
  -noise-pixel so PNG compression can't defeat the point) blob, batch
  put/get/delete including the dynamically-sized `IN (%s, %s, ...)`
  placeholder list at 25 ids, `auto_migrate=False`, migration
  idempotency, `connection=` participation in a caller-managed
  transaction, and `get_stream()` raising `NotImplementedError`
  cleanly rather than failing some other way.
- Full existing suite re-verified alongside this: 92 passed (23 new
  MySQL + all SQLite sync/async + CLI), Postgres-dependent tests
  cleanly skipped in this sandbox pass (no reachable Postgres instance
  this round -- the same "environment reset between rounds" situation
  as earlier in this project's history, not a MySQL-adapter-caused
  regression; nothing in `base.py`/`client.py`/the Postgres adapter
  itself was touched this round). Lint clean. `mypy` shows the same
  pre-existing "missing stubs for an optional third-party dependency"
  category already present for `boto3` -- now also for `pymysql`, not
  a new class of issue.

## [0.19.0] - 2026-09-17

### Fixed -- before this reached anyone, caught on TWO real Windows test runs, not one

- Both `SQLiteBackend` and `AsyncSQLiteBackend` now set an explicit
  SQLite busy timeout (10s, at two layers: `timeout=` on connect plus
  an explicit `PRAGMA busy_timeout`) on every connection they open, AND
  set WAL mode (`PRAGMA journal_mode=WAL`) exactly ONCE per backend
  instance rather than on every connection.
- **This took two attempts to actually fix, not one -- both attempts
  driven by real Windows test runs, not assumed correct from reasoning
  alone.** `test_concurrent_first_calls_only_migrate_once` (20
  concurrent connections against a fresh SQLite file) failed on
  Windows with `sqlite3.OperationalError: database is locked`. Neither
  failure reproduced in this project's own Linux-based development
  sandbox -- exactly why testing on the platform it'll actually run on
  matters, the same lesson as v0.13.0's Windows event-loop issue.
  - **First attempt**: added the busy-timeout settings above. This was
    a real, worthwhile fix on its own (Python's `sqlite3` already
    defaults `timeout=` to 5.0s, but that was an implicit, unexamined
    default rather than an explicit, generous, documented one) -- but
    it was NOT sufficient. A second Windows run showed the identical
    failure, and the overall suite ran noticeably slower (114s -> 169s)
    -- the tell that connections were now waiting out most of the
    timeout and still failing, not failing instantly, which pointed at
    a deeper problem the timeout alone couldn't paper over.
  - **Second, actual fix**: the traceback showed the failure happening
    specifically on `PRAGMA journal_mode=WAL` -- a pragma that was
    being re-issued on EVERY connection, when WAL mode is actually a
    durable, file-level setting that only needs to be set once, ever.
    20 connections all racing to perform that conversion simultaneously
    against a brand-new file is a far worse contention pattern than
    ordinary reads/writes, and no amount of busy-timeout patience
    reliably resolves a race in the conversion itself. Moved WAL-mode
    setup to run exactly once per backend instance (inside the same
    lock-guarded readiness/migration step that already existed for
    async, and behind a one-time flag for sync), and removed it
    entirely from the per-operation `_connect()` path in both adapters
    and from `tier_to_object_storage()`'s standalone connection.
- Verified in this sandbox: full suite green, 10 repeated runs of the
  previously-flaky test, and a harsher synthetic stress test (50
  concurrent connections x 10 cycles = 500 total) against a fresh file
  each time, all clean -- plus a direct check that `tier_to_object_storage()`
  still sets WAL mode correctly even when it's the very first operation
  ever performed on an instance (`auto_migrate=False`). None of this
  proves the Windows fix works -- only a third Windows run does -- but
  it does confirm the fix introduces no regressions and the "set once"
  logic itself is correct.

### Added -- closes the last SQLite gap: async support

- `AsyncSQLiteBackend` (`adapters/sqlite_async.py`), built on
  `aiosqlite`. SQLite now has both a sync and an async backend, same as
  Postgres.
- `aiosqlite` added as a new optional extra (`pip install
zerobucket[sqlite-async]`), NOT a hard dependency -- the import is
  deferred into `AsyncSQLiteBackend.__init__` the same way boto3's is
  deferred in `ObjectStorage.__init__`. Verified directly: plain
  `import zerobucket` (and the export of `AsyncSQLiteBackend` itself)
  works with `aiosqlite` completely absent; only actually
  _constructing_ an `AsyncSQLiteBackend` requires it, and does so with
  a clear `ImportError` (install instructions included) rather than a
  raw traceback.

### Scope: matches AsyncPostgresBackend's scope exactly, not by accident

Core CRUD + streaming, classic mode only -- no `dedup=True`, no
`tier_to_object_storage()`, no `on_operation`/retry machinery. This was
a deliberate choice to keep the two async adapters consistent with each
other, rather than let SQLite's async support drift wider than
Postgres's (which was itself deliberately scoped narrower than sync
SQLite/Postgres back in v0.13.0).

### A real, verified driver constraint that shaped this file's design

`aiosqlite.Connection` does not expose `blobopen()` at all -- confirmed
by inspecting its actual method list directly, not assumed absent
because it seemed plausible. Sync `SQLiteBackend.get_stream()` is built
entirely on `blobopen()`'s incremental-BLOB-I/O API; that approach was
simply unavailable here. `AsyncSQLiteBackend.get_stream()` instead uses
SQLite's `substr()` function in repeated ranged queries -- the same
strategy `AsyncPostgresBackend.get_stream()` already uses via
Postgres's `substring()` -- verified to behave correctly (1-indexed,
clamps at the value's actual end) before relying on it.

### A real, verified consequence of that constraint: three backends, two different behaviors for one scenario

This is the one worth understanding before assuming SQLite's two
backends behave identically to each other: sync `SQLiteBackend.get_stream()`
holds one connection/blob handle open for the whole stream and, thanks
to WAL-mode snapshot isolation, SURVIVES a concurrent delete mid-stream
(see v0.17.0's entry). `AsyncSQLiteBackend.get_stream()` issues a
SEPARATE query per chunk instead (no `blobopen()` to hold a snapshot
open with), so a concurrent delete mid-stream IS observed and DOES
raise `StorageError` -- matching `AsyncPostgresBackend`'s behavior, not
sync `SQLiteBackend`'s. Confirmed directly with a dedicated test for
each backend rather than assumed consistent just because both happen to
be "SQLite."

### A real bug caught by actually running it, not assumed away

`put_many()`'s per-row `INSERT ... RETURNING` statements need their
result explicitly drained (`await cur.fetchone()`) before the
transaction can commit under `aiosqlite` -- omitting that raised
`cannot commit transaction - SQL statements in progress`. The SYNC
`sqlite3` driver does not enforce this the same way (the sync adapter's
own `put_many()` never fetches the RETURNING result and works fine) --
a genuine, verified difference in driver behavior, not a copy-paste
mistake carried over from the sync version. Found immediately by
running a real smoke test against a real file before writing any formal
tests, exactly the failure mode this project's "prove it by hand first"
habit exists to catch.

### Files delivered

- New: `adapters/sqlite_async.py`, `tests/test_sqlite_async_adapter.py`
  (16 new tests)
- Changed: `__init__.py` (export `AsyncSQLiteBackend`),
  `pyproject.toml` (version, new `sqlite-async` optional extra,
  `aiosqlite` added to dev dependencies)

299/299 tests pass (16 new), lint clean, mypy shows only the same
pre-existing categories of findings every other adapter file in this
project already carries (confirmed directly against `sqlite.py` itself
for comparison, not assumed). Manually smoke-tested against a real
SQLite file before writing the formal suite -- full CRUD, streaming,
batch operations, the concurrent-delete-mid-stream behavior difference,
and concurrent first-call migration safety, the same "prove it by hand,
then encode the proof as a test" approach used throughout this project.

With this release, `SQLiteBackend`/`AsyncSQLiteBackend` are at real
feature parity with `PostgresBackend`/`AsyncPostgresBackend` (same
scope boundaries on both sides -- async is narrower than sync for both
databases, by the same deliberate design choice each time). MySQL
remains entirely unbuilt.

## [0.18.0] - 2026-09-17

### Added -- SQLite dedup mode, closing the last SQLite gap besides async support

- `SQLiteBackend(dedup=True)` -- content-addressed storage with
  reference counting, mirroring `PostgresBackend`'s dedup schema and
  behavior exactly in shape: separate `zerobucket_blobs`/
  `zerobucket_image_refs` tables, a checksum-keyed blob shared by many
  ids, `ref_count` incremented on each new reference and decremented on
  delete, the blob itself removed once `ref_count` hits zero.
- Every existing SQLite operation (`put`, `put_many`, `get`, `get_many`,
  `get_metadata`, `get_stream`, `delete`, `delete_many`, `exists`) now
  branches correctly between classic and dedup mode -- not a separate
  parallel implementation bolted on, the same methods handle both.
- Same restriction as `PostgresBackend`: `dedup=True` combined with
  `object_storage=` raises `ValueError` immediately at construction.
  Combining content-addressed storage (one blob, many ids) with tiering
  (a specific blob's bytes living in one place or the other) remains
  out of scope for both adapters, not just Postgres.

### What's still explicitly not done for SQLite

Async support (`aiosqlite`) and the `on_operation`/retry-backoff
machinery `PostgresBackend` has. Tracked in the module's own docstring.
MySQL: still nothing built.

### Verified guarantees, not just implemented -- confirmed with real assertions, not only "it runs without error"

- **Within-one-batch ref-count accumulation.** Three identical images in
  one `put_many()` call produce `ref_count == 3` on the shared blob, not
  `1` -- checked directly against the database, the same specific claim
  the Postgres adapter's own dedup implementation verified empirically
  for its `executemany()`-based version.
- **Correct decrement for `delete_many()` across ids sharing one
  checksum.** Deleting 2 of 3 refs to the same blob in a single
  `delete_many()` call leaves `ref_count == 1`, not silently wrong from
  only decrementing once per distinct checksum in the batch.
- **Blob cleanup exactly at zero, not before or after.** Confirmed the
  underlying blob row is actually gone from `zerobucket_blobs` after
  the last referencing id is deleted, and confirmed a shared blob
  survives when only one of its two referencing ids is deleted.
- **Streaming a dedup'd blob through two different ids sharing it both
  deliver correct, independent, full content** -- not just that dedup
  streaming works once.

### Files delivered

- Changed: `adapters/sqlite.py` (dedup schema, all dedup query
  constants, `dedup=True` constructor param plus the
  `dedup`+`object_storage` construction guard, every CRUD/streaming
  method updated to branch on `self._dedup`), `tests/test_sqlite_adapter.py`
  (15 new dedup tests), `pyproject.toml` (version only)

283/283 tests pass (15 new), lint clean. Manually smoke-tested against
a real SQLite file before writing the formal suite (put/get/metadata/
exists/stream, partial-ref deletion, batch operations, blob cleanup at
zero) -- same "prove it works by hand first, then encode that proof as
a test" approach used throughout this project.

## [0.17.0] - 2026-09-17

### Added -- PHASE 2 OF 4, STILL NOT A COMPLETE FEATURE

- `SQLiteBackend.get_stream()` -- built on `sqlite3.Connection.blobopen()`
  (Python >= 3.11), a genuine incremental-BLOB-I/O API. Arguably a more
  natural fit for streaming than the Postgres adapter's own
  `substring()`-based approach, not a lesser one.
- `SQLiteBackend.tier_to_object_storage()` -- same return-value contract
  as the Postgres adapter (None = not found, False = already tiered,
  True = tiered just now), same "upload fails -> row completely
  untouched" safety guarantee, achieved with a different locking
  primitive (see below). `get()`/`get_many()`/`delete()`/`delete_many()`
  updated to handle tiered rows transparently, matching Postgres parity.
- `SQLiteBackend.__init__` now accepts `object_storage=`, same as
  `PostgresBackend`.

### Still explicitly NOT done -- dedup mode, async (aiosqlite), and all of MySQL

Tracked in the module's own docstring, not just here.

### The one real, honestly-documented cost of matching Postgres's tiering safety guarantee on SQLite

SQLite has no per-row locking at all -- it's fundamentally a single-
writer database. `tier_to_object_storage()` uses `BEGIN IMMEDIATE`
instead of `SELECT ... FOR UPDATE`, which preserves the actual safety
property (a failed upload leaves the row completely untouched, no
window where bytes exist in neither location) -- but `BEGIN IMMEDIATE`
locks the ENTIRE database file for writes, not just the one row being
tiered, for the full duration of the upload. On Postgres, tiering one
image doesn't block writes to any other row. On SQLite, it blocks
writes to every other row too (reads are unaffected -- WAL mode allows
concurrent readers alongside a writer). Stated plainly in the code and
here, not glossed over as equivalent to Postgres's behavior, because it
isn't. Verified with a dedicated test using a deterministic technique
(a second connection with a short `busy_timeout` that must fail with
`sqlite3.OperationalError: database is locked` while tiering is in
progress) rather than a fragile timing measurement -- the first version
of this test used timing deltas and gave a false negative; rewritten
after confirming the real locking behavior in isolation first, outside
any project code, before trusting the more complex test built on top of
it.

### A second real, verified-not-assumed behavioral difference from Postgres

Concurrent deletion mid-stream behaves differently between the two
backends, and this was actually discovered by a test failing, not
predicted in advance. On Postgres, each `get_stream()` chunk is a
separate round trip; a row deleted by another connection mid-stream
causes the next chunk fetch to see nothing and raise `StorageError`.
On SQLite, this backend holds ONE connection open for the whole stream,
and `blobopen()`'s underlying read transaction gives it a consistent
snapshot (in WAL mode) of the row as it was when streaming began -- a
concurrent `DELETE` from another connection does NOT interrupt an
in-progress SQLite stream; it completes successfully with the full,
correct original bytes. Confirmed in a minimal isolated script first
(bare `blobopen()`, no project code) before updating the real test and
the method's docstring to describe the verified behavior instead of an
assumed one carried over from the Postgres implementation.

### Files delivered

- Changed: `adapters/sqlite.py` (`get_stream`, `tier_to_object_storage`,
  `object_storage=` constructor param, `get`/`get_many`/`delete`/
  `delete_many` updated for tiered rows), `tests/test_sqlite_adapter.py`
  (12 new tests, including two that were rewritten after their first
  version caught real, verified-not-assumed behavioral differences
  rather than confirming an assumption), `pyproject.toml` (version only)

268/268 tests pass (34 total for the SQLite adapter across both
phases), lint clean. Both real infrastructure pieces exercised for
real: a real SQLite file on disk throughout, and a real boto3 client
against `moto`'s S3 emulator for every tiering test (same approach used
for the Postgres adapter's own v0.14.0 tiering tests).

## [0.16.0] - 2026-09-17

### Added -- PHASE 1 OF 4, NOT A COMPLETE FEATURE YET

- `SQLiteBackend` (`adapters/sqlite.py`), the first of the roadmap's
  last remaining item ("SQLite and MySQL adapters"). Implements
  classic-mode core CRUD only: `put`, `put_many`, `get`, `get_many`,
  `get_metadata`, `delete`, `delete_many`, `exists`, `close`. Use it via
  the existing `backend=` constructor override:
  `ZeroBucket(backend=SQLiteBackend("path/to.db"))` -- no dedicated
  `sqlite://` connection-string auto-detection in `client.py` yet.
- Confirms the `StorageBackend` abstraction genuinely is backend-
  agnostic, not just in theory: image validation, checksumming, and the
  `before_get`/`before_put` access-control hooks all work identically
  against `SQLiteBackend` with zero changes to `client.py` -- verified
  directly with dedicated tests, not assumed because it works for
  Postgres.

### Explicitly NOT done yet -- stated in the module's own docstring, not just here

- `get_stream()` raises `NotImplementedError` with a message pointing
  back at this gap -- not silently missing, not a confusing low-level
  error.
- No dedup mode, no `tier_to_object_storage()`, no async support
  (`aiosqlite`) for SQLite yet.
- No MySQL/MariaDB adapter code at all yet (a real MariaDB instance was
  set up and confirmed reachable via `pymysql`/`asyncmy` during this
  round, in preparation -- no adapter built on top of it yet).

### Two real, honestly-documented design differences from the Postgres adapter

- **No connection pool.** Unlike a networked database, opening a SQLite
  connection is cheap (no network round trip, no auth handshake) --
  this backend opens a fresh connection per operation rather than
  maintaining a pool. Stated in the module docstring as a deliberate
  choice specific to SQLite's local-file nature, not a pattern to copy
  into a hypothetical future networked adapter without re-deriving
  whether it still makes sense there.
- **WAL mode enabled on every connection** (`PRAGMA journal_mode=WAL`).
  SQLite's default rollback-journal mode serializes all readers behind
  a writer; WAL mode allows concurrent readers alongside a single
  writer -- relevant even for a single local file used by more than one
  process/thread.

### A real bug caught by actually running it, not assumed away

- SQLite's `sqlite3.Connection.execute()` rejects multi-statement SQL
  scripts outright (`sqlite3.ProgrammingError: You can only execute one
statement at a time`) -- unlike psycopg, which runs the Postgres
  adapter's multi-statement schema string without complaint. Fixed by
  using `executescript()` for schema migration specifically. Found
  immediately by running the adapter against a real `.db` file before
  writing any tests, not discovered later.

### Files delivered

- New: `adapters/sqlite.py`, `tests/test_sqlite_adapter.py` (22 new
  tests, run against a real SQLite file on disk via `tempfile`, not an
  in-memory mock)
- Changed: `__init__.py` (export `SQLiteBackend`), `pyproject.toml`
  (version only)

256/256 tests pass (22 new), lint clean. No fresh-venv install
verification for this specific phase yet (SQLite ships in Python's
standard library, so there's no new dependency to verify pulls in
correctly the way boto3/Django did for prior features) -- full
build/install verification will happen once this feature is complete
enough to be a rounded release, not mid-phase.

## [django-zerobucket 0.1.0] - 2026-09-04

### Added

- `django-zerobucket`, a new, SEPARATE package (not a module inside the
  core `zerobucket` package -- see "Packaging" below for why) providing
  a Django `Storage` backend adapter: `ZeroBucketStorage`, plugging
  ZeroBucket into Django's existing `FileField`/`ImageField` via the
  `STORAGES` setting. No new model field type to learn -- works with
  existing forms, admin, and migrations the same way `django-storages`'
  S3/GCS backends do for their targets.
- `ServeImageView`, a built-in class-based view streaming stored images
  back over HTTP via ZeroBucket's own `get_stream()` -- what
  `ZeroBucketStorage.url()` reverses to by default. Override-able (see
  below) for anyone who wants images served some other way.
- Three management commands (`zerobucket_info`, `zerobucket_verify`,
  `zerobucket_tier`) -- thin wrappers around the core package's already-
  tested `cmd_info`/`cmd_verify`/`cmd_tier` CLI functions, not
  reimplementations, reading `ZEROBUCKET_DATABASE_URL` from Django
  settings instead of requiring `--database-url` by hand.

### Scope decisions -- decided deliberately for this first pass, not defaulted into

Three real architectural forks were raised as explicit options before
any code was written (how Django should use ZeroBucket at all, how
served images reach HTTP, and how this should be packaged), with the
final call on each delegated back and made explicitly rather than left
implicit:

- **Storage API adapter, not a custom model field.** Judged the higher-
  leverage, more idiomatic choice for Django -- it composes with
  existing `FileField`/`ImageField`, forms, and admin, rather than every
  adopting project having to learn a new field type. A custom field
  remains a possible future addition, not ruled out, just not built.
- **A built-in serving view, override-able.** `Storage.url()` needs a
  usable default or the "drop-in" pitch of choosing the Storage-adapter
  approach over a custom field falls apart (`{{ instance.photo.url }}`
  wouldn't work out of the box). `ServeImageView` is that default,
  designed to be subclassed/replaced (e.g. pointing at a CDN in front of
  tiered S3 objects, or adding auth) rather than hardcoded as the only
  option.
- **Separate PyPI package**, matching how `django-storages` and similar
  Django integrations are conventionally distributed -- NOT bundled
  into the core `zerobucket` package. This keeps Django (a genuinely
  optional, heavy dependency most `zerobucket` users don't have) out of
  the core library's dependency footprint entirely, the same "keep the
  core small" philosophy already behind boto3 being an optional extra
  rather than a hard dependency.

### The one deliberate, honestly-documented difference from typical Django Storage backends

**The `name` a `FileField`/`ImageField` stores is the ZeroBucket image
id (a UUID), not a filesystem-style path.** Every other Django Storage
backend builds a path from `upload_to=` plus the uploaded filename;
ZeroBucket is id-addressed, not path-addressed, so `upload_to=` is
required by Django's field API but effectively ignored here. This is
invisible for normal usage (`.url`, `.read()`, `.open()`, template
`{{ }}` access all work identically) -- it only matters for code that
inspects the raw `name` string expecting something path-shaped. The
original upload filename is NOT lost, though: it's preserved in
ZeroBucket's own metadata (`filename=` passed through to `put()`) and
retrievable via the core client's `metadata()`, even though Django's
own `name` field won't show it. Confirmed with a dedicated test, not
just asserted in the docstring.

A second, smaller deliberate override: `get_available_name()` skips
Django's default collision-avoidance loop (which normally calls
`exists()` repeatedly, appending suffixes until a free name is found)
entirely -- meaningless here, since `_save()` always gets a fresh id
from `put()` regardless of what name was suggested, so the loop would
just be a wasted round trip. Confirmed with a test that it's a genuine
no-op passthrough, not merely documented as one.

### No built-in access control on served images -- stated, not hidden

`ServeImageView` has no authentication/permission checks of its own.
ZeroBucket's core `before_get`/`before_put` hooks exist for exactly
this, but wiring a `context=` through Django's request/auth system into
those hooks is a real design question of its own (whose context -- the
request? the user? something else?) that was judged out of scope for
this first pass rather than answered by guessing. The README documents
the straightforward workaround: wrap the view with Django's own
`login_required` (or similar) the normal way.

### Real infrastructure risk found and fixed during this round, not assumed away

- Partway through, `zerobucket` was discovered installed as a stale,
  non-live copy in site-packages (`hatchling`'s editable-install mode
  had, at some point in this project's session history, materialized as
  a one-time file copy rather than a live-reflecting link) -- meaning
  test runs could silently have been exercising an outdated snapshot of
  the core package rather than its current source. Caught by comparing
  file content and modification timestamps directly between the
  installed copy and the source tree (they matched -- this round's
  testing was NOT actually affected), then fixed by reinstalling and
  confirming, via `zerobucket.__file__`, that the installed package now
  resolves directly to the live source tree. Flagged here as a reminder
  for future rounds, not just fixed silently.

### Testing

Uses `pytest-django` against a REAL PostgreSQL instance, deliberately
in two distinct roles at once (see `tests/django_settings.py`'s module
docstring): Django's own `DATABASES` (its internal tables, plus this
suite's dummy `Product` test model) and `ZEROBUCKET_DATABASE_URL`
(where ZeroBucket actually stores image bytes) -- kept conceptually
separate even though this test setup happens to point both at the same
physical server. Coverage includes the full real-world path, not just
the Storage class in isolation: a genuine Django model with an
`ImageField(storage=ZeroBucketStorage)`, saved and reloaded through the
actual ORM; `ZeroBucketStorage.url()` resolved and then actually
fetched through Django's real HTTP test client and URL resolver;
`zerobucket_tier` exercised via `call_command()` against a real boto3
client (`moto`'s S3 emulator, same approach as the core package's own
v0.14.0/v0.15.0 tiering tests).

### Files delivered

- New package: `packages/django-zerobucket/` -- `src/django_zerobucket/`
  (`storage.py`, `views.py`, `urls.py`, `apps.py`,
  `management/commands/zerobucket_{info,verify,tier}.py`),
  `tests/` (`test_storage.py`, `test_views.py`,
  `test_management_commands.py`, Django settings/urlconf/test-app
  scaffolding), `pyproject.toml`, `README.md`
- Changed (root project): `README.md` (roadmap checkbox, Installation
  section pointer, Project structure diagram)

26/26 tests pass, lint clean. Built, `twine check`ed, and functionally
verified end-to-end from a genuinely fresh venv install of the built
wheel (both `django-zerobucket` and the core `zerobucket` wheel
together) against a real, separately-constructed Django project
directory (not the test suite's own scaffolding) -- confirmed
`save()`/`exists()`/`open()`/`size()`/`url()`/`delete()` and a real
HTTP request through Django's test client all work correctly, and
confirmed boto3 is genuinely absent from a fresh install unless
explicitly requested via `zerobucket[s3]`.

## [0.15.0] - 2026-09-04

### Added

- `zerobucket tier` CLI command -- closes a real gap left by v0.14.0's
  object-storage tiering: `tier_to_object_storage()` had no CLI
  equivalent, even though the sibling `migrate_classic_to_dedup()`
  feature it was explicitly modeled after already had one (`zerobucket
migrate`). Caught while scoping the next round of work, not reported
  by a user -- flagged and fixed before it became one.

### Two forms, one command

- **Single id**: `zerobucket tier IMAGE_ID --bucket my-bucket` -- a thin
  wrapper around exactly what `ZeroBucket.tier_to_object_storage()` does
  from Python, so tiering one image doesn't require writing a script
  just to supply a bucket and credentials.
- **Bulk**: `zerobucket tier --all|--min-size N|--older-than DAYS
[--limit N] [--dry-run] --bucket my-bucket` -- the reference
  implementation of the "backfill script" v0.14.0's docs said callers
  would have to write themselves. This does NOT reverse that release's
  stated scope decision that `put()` never auto-tiers based on size --
  tiering is still only ever triggered explicitly, by something calling
  `tier_to_object_storage()`; this just means the selection-query-plus-
  loop boilerplate doesn't have to be hand-written anymore. Only ever
  selects rows not already tiered -- confirmed with a dedicated test
  that an already-tiered row isn't even a candidate for `--all`, not
  merely re-processed as a no-op.

### Small but deliberate design choices

- `--dry-run` lists exactly what a bulk run would tier without doing
  it -- tiering is consequential (real data movement, real object-
  storage cost, a held Postgres row lock per image for the duration of
  each upload -- see v0.14.0's entry), so previewing a bulk selection
  before committing to it felt worth the small extra surface area.
- Requesting both a single `IMAGE_ID` and a bulk filter (`--all`/
  `--min-size`/`--older-than`) in the same invocation is a usage error
  (exit code 2), not "the id wins" or "the filter wins" silently.
  Same for requesting neither.
- Exits with status 1 if anything failed to tier (not-found ids,
  upload failures), same convention `verify` already uses -- usable in
  a cron job or CI check, not just interactively.
- `tier` is the one CLI command that needs `boto3` (`pip install
zerobucket[s3]`) -- every other command still needs nothing beyond
  the base install, unchanged from before. Running `tier` without
  boto3 installed surfaces `ObjectStorage`'s own clear `ImportError`
  message (with the install instructions in it), not a raw traceback --
  verified with a test that simulates the import failing.

### Files delivered

- Changed: `cli.py` (`cmd_tier`, `_select_tier_candidates`, `tier`
  subparser), `tests/test_cli.py` (11 new tests), `pyproject.toml`
  (version only), both READMEs (CLI section, and the "Object-storage
  tiering" section's wording updated now that a backfill reference
  implementation actually exists)

234/234 tests pass (11 new), lint clean. Verified directly against real
Postgres and a real boto3 client (via `moto`'s S3 emulator, same
approach as v0.14.0) before writing formal tests, then again via the
formal test suite: single-id tiering, idempotent re-tiering, not-found
handling, all three bulk filters (individually confirmed to actually
filter, not just accept the flag), `--limit`, `--dry-run` genuinely not
mutating anything, both usage-error combinations, and the no-boto3
error path.

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
