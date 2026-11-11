"""migrate_classic_to_dedup() on SQLite and MySQL/MariaDB (0.31.0).

The PostgreSQL version has its own tests in test_dedup.py. Here every scenario runs
on both other databases through the same public function, `zerobucket.migrate_classic_to_dedup`:
ids, bytes and timestamps kept, identical content shared, the classic table untouched, safe to
re-run, tiered rows skipped, all-or-nothing on a failure part-way, two migrations at once, and
(MySQL) the named lock released only after the commit.

SQLite needs nothing; the MySQL tests skip when no server is reachable, like the rest of the suite.
"""

from __future__ import annotations

import gc
import io
import os
import sqlite3
import tempfile
import threading
import time

import pytest
from PIL import Image as PILImage

import zerobucket
from zerobucket import StorageError, ZeroBucket, migrate_classic_to_dedup
from zerobucket.adapters.sqlite import SQLiteBackend

TEST_MYSQL_URL = os.environ.get(
    "ZEROBUCKET_TEST_MYSQL_URL",
    "mysql://zerobucket:zerobucket@127.0.0.1:3306/zerobucket_test",
)
TABLES = ("zerobucket_image_refs", "zerobucket_blobs", "zerobucket_images")
SQLITE_SAME_TIME = "2026-01-01T00:00:00+00:00"


def _jpeg(color) -> bytes:
    buf = io.BytesIO()
    PILImage.new("RGB", (40, 30), color=color).save(buf, format="JPEG")
    return buf.getvalue()


RED = _jpeg((200, 0, 0))
BLUE = _jpeg((0, 0, 200))


class Db:
    """One database under test, with just enough raw SQL to set up and inspect it."""

    def __init__(self, kind: str, path: str | None = None):
        self.kind = kind
        self.path = path
        self.ph = "?" if kind == "sqlite" else "%s"

    # -- backends ---------------------------------------------------------
    def classic(self, **kw):
        if self.kind == "sqlite":
            return SQLiteBackend(self.path, **kw)
        from zerobucket.adapters.mysql import MySQLBackend

        return MySQLBackend(TEST_MYSQL_URL, **kw)

    def dedup(self, **kw):
        return self.classic(dedup=True, **kw)

    def seed(self, colors=(RED, RED, BLUE), **put_kw) -> list[str]:
        zb = ZeroBucket(backend=self.classic())
        try:
            return [zb.put(c, **put_kw) for c in colors]
        finally:
            zb.close()

    # -- raw SQL ----------------------------------------------------------
    def _connect(self):
        if self.kind == "sqlite":
            return sqlite3.connect(self.path)
        import pymysql

        from zerobucket.adapters.mysql import _parse_database_url

        return pymysql.connect(**_parse_database_url(TEST_MYSQL_URL))

    def run(self, sql: str, params=()) -> None:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(sql, params)
            conn.commit()
        finally:
            conn.close()

    def query(self, sql: str, params=()) -> list[tuple]:
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
            conn.commit()  # end the read snapshot
            return rows
        finally:
            conn.close()

    def count(self, table: str) -> int:
        return self.query(f"SELECT COUNT(*) FROM {table};")[0][0]

    def same_time(self):
        """A created_at value shared by many rows, in this database's own format."""
        if self.kind == "sqlite":
            return SQLITE_SAME_TIME
        from datetime import datetime

        return datetime(2026, 1, 1, 0, 0, 0, 123456)

    def make_tiered(self, image_id: str) -> None:
        self.run(
            "UPDATE zerobucket_images SET data = NULL, storage_backend = 'object_storage', "
            f"object_storage_bucket = 'b', object_storage_key = 'k' WHERE id = {self.ph};",
            (image_id,),
        )

    def make_duplicate_filenames_fail(self) -> None:
        """Add a unique index on the refs' filename, so copying two rows with the same
        filename fails part-way. (A trigger would do too, but on MySQL with binary logging
        on, which is MySQL 8's default, creating one needs the SUPER privilege.)"""
        if self.kind == "sqlite":
            self.run(
                "CREATE UNIQUE INDEX zb_unique_name ON zerobucket_image_refs (original_filename);"
            )
        else:  # TEXT columns need a prefix length in a MySQL index
            self.run(
                "CREATE UNIQUE INDEX zb_unique_name "
                "ON zerobucket_image_refs (original_filename(100));"
            )

    def allow_duplicate_filenames(self) -> None:
        if self.kind == "sqlite":
            self.run("DROP INDEX zb_unique_name;")
        else:
            self.run("DROP INDEX zb_unique_name ON zerobucket_image_refs;")

    def drop_everything(self) -> None:
        if self.kind == "sqlite":
            return
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute("SET FOREIGN_KEY_CHECKS = 0")
            for table in TABLES:
                cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.execute("SET FOREIGN_KEY_CHECKS = 1")
            conn.commit()
        finally:
            conn.close()


def _remove_db_files(path: str) -> None:
    gc.collect()
    for ext in ("", "-wal", "-shm"):
        target = path + ext
        for attempt in range(10):
            try:
                if os.path.exists(target):
                    os.remove(target)
                break
            except PermissionError:  # Windows releases a closed handle a moment later
                if attempt == 9:
                    raise
                time.sleep(0.1)
                gc.collect()


@pytest.fixture(params=["sqlite", "mysql"])
def db(request):
    kind = request.param
    if kind == "mysql":
        import pymysql

        from zerobucket.adapters.mysql import _parse_database_url

        try:
            pymysql.connect(
                **_parse_database_url(TEST_MYSQL_URL), connect_timeout=3
            ).close()
        except Exception as exc:  # noqa: BLE001
            pytest.skip(f"MySQL/MariaDB not reachable: {exc}")
    path = tempfile.mktemp(suffix=".db") if kind == "sqlite" else None
    d = Db(kind, path)
    d.drop_everything()
    yield d
    d.drop_everything()
    if path:
        _remove_db_files(path)


@pytest.fixture
def mysql_db(db):
    if db.kind != "mysql":
        pytest.skip("MySQL-only test")
    return db


def _migrate(db: Db, **kw):
    backend = db.dedup(**kw.pop("backend_kw", {}))
    try:
        return migrate_classic_to_dedup(backend, **kw)
    finally:
        backend.close()


# ---- the contract -------------------------------------------------------------------


def test_copies_every_row_keeping_ids_bytes_and_timestamps(db):
    ids = db.seed()
    before = db.query(
        "SELECT id, created_at, updated_at FROM zerobucket_images ORDER BY id;"
    )
    events = []
    backend = db.dedup(on_operation=events.append)
    try:
        summary = migrate_classic_to_dedup(backend)
        assert summary == {
            "images_migrated": 3,
            "distinct_blobs_created": 2,
            "duplicate_references_found": 1,
            "already_migrated_skipped": 0,
            "tiered_rows_skipped": 0,
        }
        dedup = ZeroBucket(backend=backend)
        assert dedup.get(ids[0]).data == RED
        assert dedup.get(ids[1]).data == RED
        assert dedup.get(ids[2]).data == BLUE
    finally:
        backend.close()

    after = db.query(
        "SELECT id, created_at, updated_at FROM zerobucket_image_refs ORDER BY id;"
    )
    assert after == before  # ids and BOTH timestamps survive exactly
    assert sorted(
        r[0] for r in db.query("SELECT ref_count FROM zerobucket_blobs;")
    ) == [1, 2]
    assert db.count("zerobucket_images") == 3  # the classic table is untouched
    migrate_events = [e for e in events if e.operation == "migrate_to_dedup"]
    assert [e.success for e in migrate_events] == [True]


def test_without_a_classic_table_it_fails_clearly_and_reports_the_failure(db):
    events = []
    backend = db.dedup(on_operation=events.append)
    try:
        with pytest.raises(StorageError, match="nothing to migrate"):
            migrate_classic_to_dedup(backend)
    finally:
        backend.close()
    assert [e.success for e in events if e.operation == "migrate_to_dedup"] == [False]


def test_can_be_run_again_and_picks_up_only_new_rows(db):
    db.seed()
    assert _migrate(db)["images_migrated"] == 3

    again = _migrate(db)
    assert again["images_migrated"] == 0
    assert again["already_migrated_skipped"] == 3
    assert again["distinct_blobs_created"] == 0

    db.seed(colors=(RED,))  # a new classic row, identical to existing content
    third = _migrate(db)
    assert third["images_migrated"] == 1
    assert third["already_migrated_skipped"] == 3
    assert third["distinct_blobs_created"] == 1
    # RED is now referenced three times, and was not double-counted by the re-runs.
    assert sorted(
        r[0] for r in db.query("SELECT ref_count FROM zerobucket_blobs;")
    ) == [1, 3]


def test_more_rows_than_one_batch_with_the_same_created_at(db):
    colors = [_jpeg((i * 20, 50, 255 - i * 20)) for i in range(11)]
    db.seed(colors=colors)
    db.run(f"UPDATE zerobucket_images SET created_at = {db.ph};", (db.same_time(),))

    summary = _migrate(db, batch_size=3)
    assert summary["images_migrated"] == 11
    assert summary["distinct_blobs_created"] == 11
    assert db.count("zerobucket_image_refs") == 11
    assert _migrate(db, batch_size=3)["already_migrated_skipped"] == 11


def test_tiered_rows_are_skipped_and_counted(db):
    ids = db.seed(colors=(RED, BLUE, _jpeg((9, 9, 9))))
    db.make_tiered(ids[1])

    summary = _migrate(db)
    assert summary["images_migrated"] == 2
    assert summary["tiered_rows_skipped"] == 1
    migrated = {r[0] for r in db.query("SELECT id FROM zerobucket_image_refs;")}
    assert migrated == {ids[0], ids[2]}
    assert (
        db.count("zerobucket_images") == 3
    )  # the tiered row stays in the classic table


def test_content_already_in_the_dedup_tables_is_merged_not_duplicated(db):
    db.seed(colors=(RED,))
    dedup = ZeroBucket(backend=db.dedup())
    try:
        dedup.put(RED)  # same bytes stored directly in dedup mode first
    finally:
        dedup.close()

    assert _migrate(db)["images_migrated"] == 1
    assert db.count("zerobucket_blobs") == 1
    assert db.query("SELECT ref_count FROM zerobucket_blobs;")[0][0] == 2


# ---- all or nothing -------------------------------------------------------------------


def test_a_failure_part_way_leaves_the_dedup_tables_untouched_and_a_rerun_completes(db):
    zb = ZeroBucket(backend=db.classic())
    try:
        # The 4th row repeats the 3rd row's filename, which the unique index below rejects.
        for i, name in enumerate(["a.jpg", "b.jpg", "same.jpg", "same.jpg"]):
            zb.put(_jpeg((i * 40, 10, 10)), filename=name)
    finally:
        zb.close()
    db.dedup().close()  # creates the dedup tables, which the index below needs
    db.make_duplicate_filenames_fail()

    events = []
    backend = db.dedup(on_operation=events.append, max_retries=0)
    try:
        with pytest.raises(StorageError):
            migrate_classic_to_dedup(backend, batch_size=2)
        # The three rows copied BEFORE the failing one were rolled back with it.
        assert db.count("zerobucket_image_refs") == 0
        assert db.count("zerobucket_blobs") == 0

        db.allow_duplicate_filenames()
        summary = migrate_classic_to_dedup(backend, batch_size=2)
    finally:
        backend.close()
    assert summary["images_migrated"] == 4
    assert db.count("zerobucket_image_refs") == 4
    assert [e.success for e in events if e.operation == "migrate_to_dedup"] == [
        False,
        True,
    ]


# ---- two migrations at once --------------------------------------------------------------


def test_two_migrations_started_at_once_copy_each_image_once(db):
    db.seed(colors=[_jpeg((i * 15, 80, 40)) for i in range(5)] + [RED] * 4 + [BLUE] * 3)
    start = threading.Barrier(2)
    results: list[dict] = []
    errors: list[BaseException] = []

    def worker():
        backend = db.dedup()
        try:
            start.wait(timeout=10)
            results.append(migrate_classic_to_dedup(backend, batch_size=2))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)
        finally:
            backend.close()

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert not errors, errors
    assert len(results) == 2
    assert sum(r["images_migrated"] for r in results) == 12
    assert sum(r["already_migrated_skipped"] for r in results) == 12
    assert db.count("zerobucket_image_refs") == 12
    # No blob's ref_count was counted twice.
    assert db.query("SELECT SUM(ref_count) FROM zerobucket_blobs;")[0][0] == 12


def test_the_lock_is_released_only_after_the_transaction_committed(
    mysql_db, monkeypatch
):
    """If the named lock were released before the commit, a second migration could start while
    this one's rows are still invisible to it, and collide with them on the primary key.
    """
    from zerobucket.adapters.mysql import MySQLBackend

    mysql_db.seed()
    seen: list[int] = []
    real_release = MySQLBackend._release_named_lock

    def spy(conn, name):
        # At the moment of release, a DIFFERENT connection must already see every copied row.
        seen.append(mysql_db.count("zerobucket_image_refs"))
        real_release(conn, name)

    monkeypatch.setattr(MySQLBackend, "_release_named_lock", staticmethod(spy))
    _migrate(mysql_db)
    assert seen == [3]


def test_the_lock_is_free_again_after_a_failed_migration(mysql_db):
    backend = (
        mysql_db.dedup()
    )  # no classic table -> the migration fails inside the lock
    try:
        with pytest.raises(StorageError):
            migrate_classic_to_dedup(backend)
    finally:
        backend.close()
    assert mysql_db.query("SELECT IS_FREE_LOCK('zerobucket_migrate_dedup');")[0][0] == 1


# ---- misuse -------------------------------------------------------------------------------


def test_a_classic_backend_is_refused(db):
    db.seed()
    backend = db.classic()
    try:
        with pytest.raises(StorageError, match="dedup=True"):
            migrate_classic_to_dedup(backend)
    finally:
        backend.close()


def test_batch_size_must_be_positive(db):
    db.seed()
    backend = db.dedup()
    try:
        with pytest.raises(ValueError, match="batch_size"):
            migrate_classic_to_dedup(backend, batch_size=0)
    finally:
        backend.close()


def test_an_unsupported_object_is_refused_clearly():
    with pytest.raises(StorageError, match="not object"):
        migrate_classic_to_dedup(object())


def test_the_public_function_is_the_same_object_everywhere():
    from zerobucket.adapters.migrate import migrate_classic_to_dedup as implementation

    assert zerobucket.migrate_classic_to_dedup is implementation
