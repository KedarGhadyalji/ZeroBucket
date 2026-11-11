"""`migrate_classic_to_dedup()` for every sync backend.

One public function, importable as `zerobucket.migrate_classic_to_dedup`. It
picks the right implementation for the backend it is given: PostgreSQL's
lives in `adapters/postgres.py` (it has been there since 0.7.0), SQLite's and
MySQL/MariaDB's are methods of their backends (since 0.31.0).
"""

from __future__ import annotations

from ..exceptions import StorageError


def migrate_classic_to_dedup(dedup_backend, *, batch_size: int = 200) -> dict:
    """One-time, NON-DESTRUCTIVE migration: copy every row of the classic
    `zerobucket_images` table into the dedup tables (`zerobucket_blobs` /
    `zerobucket_image_refs`) of the SAME database, keeping every id (so
    anything that references them keeps working) and deduplicating identical
    content on the way. The classic table is never modified.

    `dedup_backend` is a PostgresBackend, SQLiteBackend or MySQLBackend that
    was created with dedup=True and points at the database holding the classic
    table (for example `ZeroBucket(..., dedup=True)._backend`).

    Safe to re-run: ids already present in the dedup tables are skipped, and
    not counted again in any blob's ref_count. Classic rows tiered to object
    storage have no bytes in the database, so they are skipped and counted as
    `tiered_rows_skipped`. All-or-nothing: the copy is one transaction.

    `batch_size` is how many classic rows are read at a time, so memory use
    does not grow with the size of the table.

    Returns {"images_migrated", "distinct_blobs_created",
    "duplicate_references_found", "already_migrated_skipped",
    "tiered_rows_skipped"}.
    """
    from .mysql import MySQLBackend
    from .postgres import PostgresBackend
    from .postgres import migrate_classic_to_dedup as _migrate_postgres
    from .sqlite import SQLiteBackend

    if isinstance(dedup_backend, PostgresBackend):
        return _migrate_postgres(dedup_backend, batch_size=batch_size)
    if isinstance(dedup_backend, (SQLiteBackend, MySQLBackend)):
        return dedup_backend._migrate_classic_to_dedup(batch_size)  # noqa: SLF001
    raise StorageError(
        "migrate_classic_to_dedup() supports PostgresBackend, SQLiteBackend and "
        f"MySQLBackend, not {type(dedup_backend).__name__}."
    )
