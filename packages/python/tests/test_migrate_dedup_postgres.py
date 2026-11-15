"""0.34.0: the PostgreSQL migrate_classic_to_dedup() now behaves like the SQLite and MySQL ones.

Before 0.34.0 it kept only `created_at` (the migrated rows' `updated_at` became the time of the
migration), reported no `on_operation` event, took no lock (two runs at once made the second
fail with a unique-key error and roll back), migrated nothing silently for `batch_size=0`, and
gave a raw database error for a backend created without dedup=True. SQLite and MySQL/MariaDB
are covered in tests/test_migrate_dedup.py; the older PostgreSQL scenarios (ids, dedup, re-runs,
tiered rows, batches, the missing classic table) stay in tests/test_dedup.py.
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from tests.conftest import TEST_DATABASE_URL
from zerobucket import StorageError, migrate_classic_to_dedup
from zerobucket.adapters.postgres import PostgresBackend

LOCK_KEY = "zerobucket_migrate_dedup"  # the same text the npm package locks on


def _query(sql: str, params=()):
    with psycopg.connect(TEST_DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall() if cur.description else None


def _distinct_jpegs(images_fixture, jpeg_bytes, png_bytes) -> list[str]:
    return [
        images_fixture.put(jpeg_bytes),
        images_fixture.put(jpeg_bytes),
        images_fixture.put(png_bytes),
    ]


# ---- both timestamps are kept ----------------------------------------------------------


def test_created_at_and_updated_at_are_both_kept_exactly(
    dedup_images, images, jpeg_bytes, png_bytes
):
    ids = _distinct_jpegs(images, jpeg_bytes, png_bytes)
    base = datetime(2026, 1, 1, 12, 0, 0, 123456, tzinfo=timezone.utc)
    for i, image_id in enumerate(ids):
        _query(
            "UPDATE zerobucket_images SET created_at = %s, updated_at = %s WHERE id = %s;",
            (base + timedelta(days=i), base + timedelta(days=100 + i), image_id),
        )

    before = _query(
        "SELECT id, created_at, updated_at FROM zerobucket_images ORDER BY id;"
    )
    summary = migrate_classic_to_dedup(dedup_images._backend)  # noqa: SLF001
    assert summary["images_migrated"] == 3

    after = _query(
        "SELECT id, created_at, updated_at FROM zerobucket_image_refs ORDER BY id;"
    )
    assert (
        after == before
    )  # before 0.34.0 the updated_at column here was the migration time
    for image_id in ids:
        images.delete(image_id)


def test_the_documented_repair_restores_updated_at_for_rows_migrated_before_0_34_0(
    dedup_images, images, jpeg_bytes
):
    """The README tells people who already migrated on PostgreSQL to run this one statement. The
    classic table is never modified by a migration, so it still holds every original timestamp.
    """
    image_id = images.put(jpeg_bytes)
    _query(
        "UPDATE zerobucket_images SET updated_at = %s WHERE id = %s;",
        (datetime(2026, 3, 1, tzinfo=timezone.utc), image_id),
    )
    migrate_classic_to_dedup(dedup_images._backend)  # noqa: SLF001
    # What a 0.33.x migration left behind: updated_at = the time of the migration.
    _query(
        "UPDATE zerobucket_image_refs SET updated_at = now() WHERE id = %s;",
        (image_id,),
    )
    assert (
        _query(
            "SELECT r.updated_at = c.updated_at FROM zerobucket_image_refs r "
            "JOIN zerobucket_images c USING (id);"
        )[0][0]
        is False
    )

    _query(
        "UPDATE zerobucket_image_refs r SET updated_at = c.updated_at "
        "FROM zerobucket_images c WHERE c.id = r.id;"
    )
    assert (
        _query(
            "SELECT r.updated_at = c.updated_at FROM zerobucket_image_refs r "
            "JOIN zerobucket_images c USING (id);"
        )[0][0]
        is True
    )


# ---- the event -------------------------------------------------------------------------


def test_the_migration_reports_one_event_on_success(dedup_images, images, jpeg_bytes):
    images.put(jpeg_bytes)
    events = []
    backend = PostgresBackend(TEST_DATABASE_URL, dedup=True, on_operation=events.append)
    try:
        migrate_classic_to_dedup(backend)
    finally:
        backend.close()
    migrate_events = [e for e in events if e.operation == "migrate_to_dedup"]
    assert len(migrate_events) == 1
    assert migrate_events[0].success is True and migrate_events[0].retry_count == 0
    assert migrate_events[0].duration_seconds >= 0


def test_a_failure_part_way_is_reported_and_leaves_the_dedup_tables_untouched(
    dedup_images, images
):
    """All-or-nothing, and the failure is an event too. A unique index on the file name makes the
    copy fail on the third image (two share a name), after two have already been copied.
    """
    from tests.test_migrate_dedup import _jpeg  # a distinct small JPEG per colour

    for i, name in enumerate(["a.jpg", "b.jpg", "same.jpg", "same.jpg"]):
        images.put(_jpeg((i * 40, 10, 10)), filename=name)
    _query(
        "CREATE UNIQUE INDEX zb_unique_name ON zerobucket_image_refs (original_filename);"
    )
    events = []
    backend = PostgresBackend(TEST_DATABASE_URL, dedup=True, on_operation=events.append)
    try:
        with pytest.raises(psycopg.errors.UniqueViolation):
            migrate_classic_to_dedup(backend, batch_size=2)
        assert _query("SELECT count(*) FROM zerobucket_image_refs;")[0][0] == 0
        assert _query("SELECT count(*) FROM zerobucket_blobs;")[0][0] == 0

        _query("DROP INDEX zb_unique_name;")
        assert migrate_classic_to_dedup(backend, batch_size=2)["images_migrated"] == 4
    finally:
        _query("DROP INDEX IF EXISTS zb_unique_name;")
        backend.close()
    assert [e.success for e in events if e.operation == "migrate_to_dedup"] == [
        False,
        True,
    ]


# ---- the lock --------------------------------------------------------------------------


def test_two_migrations_started_at_once_copy_each_image_once(dedup_images, images):
    from tests.test_migrate_dedup import _jpeg

    for i in range(12):
        images.put(_jpeg((i * 15, 80, 40)))
    # Built BEFORE the threads start, so the race is only about the migration itself.
    backends = [PostgresBackend(TEST_DATABASE_URL, dedup=True) for _ in range(2)]
    start = threading.Barrier(2)
    results: list[dict] = []
    errors: list[BaseException] = []

    def worker(backend):
        try:
            start.wait(timeout=10)
            results.append(migrate_classic_to_dedup(backend, batch_size=3))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(b,)) for b in backends]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    for b in backends:
        b.close()

    assert not errors, errors  # before 0.34.0 the second run raised UniqueViolation
    assert len(results) == 2
    assert sum(r["images_migrated"] for r in results) == 12
    assert sum(r["already_migrated_skipped"] for r in results) == 12
    assert _query("SELECT count(*) FROM zerobucket_image_refs;")[0][0] == 12
    assert _query("SELECT sum(ref_count) FROM zerobucket_blobs;")[0][0] == 12


def test_the_migration_waits_for_the_shared_lock_then_runs(
    dedup_images, images, jpeg_bytes
):
    """Holds the very lock the npm package takes from another session; the migration must wait for
    it, then complete once it is released. (This is what makes a Python and a Node migration of
    one database exclude each other.)"""
    images.put(jpeg_bytes)
    holder = psycopg.connect(TEST_DATABASE_URL, autocommit=True)
    holder.execute("SELECT pg_advisory_lock(hashtext(%s));", (LOCK_KEY,))
    done = threading.Event()
    out: list = []

    def run():
        backend = PostgresBackend(TEST_DATABASE_URL, dedup=True)
        try:
            out.append(migrate_classic_to_dedup(backend))
        finally:
            backend.close()
            done.set()

    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert not done.wait(timeout=1.5), "the migration did not wait for the lock"
        assert _query("SELECT count(*) FROM zerobucket_image_refs;")[0][0] == 0
    finally:
        holder.execute("SELECT pg_advisory_unlock(hashtext(%s));", (LOCK_KEY,))
        holder.close()
    thread.join(timeout=30)
    assert done.is_set() and out[0]["images_migrated"] == 1


def test_the_lock_is_released_after_success_and_after_failure(
    dedup_images, images, jpeg_bytes
):
    images.put(jpeg_bytes)
    backend = PostgresBackend(TEST_DATABASE_URL, dedup=True)
    try:
        migrate_classic_to_dedup(backend)
        assert (
            _query("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory';")[0][0]
            == 0
        )

        _query(
            "DROP TABLE zerobucket_images CASCADE;"
        )  # now the migration fails inside the lock
        with pytest.raises(StorageError):
            migrate_classic_to_dedup(backend)
        assert (
            _query("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory';")[0][0]
            == 0
        )
    finally:
        backend.close()
        # put the classic table back for the other tests
        PostgresBackend(TEST_DATABASE_URL).close()


# ---- misuse ----------------------------------------------------------------------------


def test_batch_size_below_one_is_refused_instead_of_migrating_nothing(
    dedup_images, images, jpeg_bytes
):
    images.put(jpeg_bytes)
    with pytest.raises(ValueError, match="batch_size"):
        migrate_classic_to_dedup(dedup_images._backend, batch_size=0)  # noqa: SLF001
    assert _query("SELECT count(*) FROM zerobucket_image_refs;")[0][0] == 0


def test_a_backend_without_dedup_is_refused_clearly(images):
    backend = PostgresBackend(TEST_DATABASE_URL)  # classic, not dedup
    try:
        with pytest.raises(StorageError, match="dedup=True"):
            migrate_classic_to_dedup(backend)
    finally:
        backend.close()
