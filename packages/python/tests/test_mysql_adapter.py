"""Tests for MySQLBackend -- Phase 1 (classic-mode core CRUD) and
Phase 2 (streaming reads + object-storage tiering), see the v0.20.0
and v0.21.0 CHANGELOG entries. NOT yet implemented for MySQL, tracked
as explicit follow-up: dedup mode, async support, connection pooling.
See adapters/mysql.py's module docstring for the full reasoning behind
each phase's design decisions.

Runs against a REAL MySQL/MariaDB instance -- set
ZEROBUCKET_TEST_MYSQL_URL to point at a throwaway database, e.g.:

    export ZEROBUCKET_TEST_MYSQL_URL=mysql://zerobucket:zerobucket@127.0.0.1:3306/zerobucket_test

All tests in this file are skipped cleanly (not failed) if no such
database is reachable, same convention as the Postgres/SQLite suites.
"""

from __future__ import annotations

import os

import pytest

from zerobucket import ImageNotFoundError, ZeroBucket
from zerobucket.adapters.mysql import MySQLBackend
from zerobucket.exceptions import StorageError

TEST_MYSQL_URL = os.environ.get(
    "ZEROBUCKET_TEST_MYSQL_URL",
    "mysql://zerobucket:zerobucket@127.0.0.1:3306/zerobucket_test",
)


@pytest.fixture(scope="session")
def _mysql_available():
    try:
        backend = MySQLBackend(TEST_MYSQL_URL)
        backend.close()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(
            f"No reachable test MySQL/MariaDB database ({TEST_MYSQL_URL}): {exc}"
        )


@pytest.fixture
def mysql_backend(_mysql_available):
    backend = MySQLBackend(TEST_MYSQL_URL)
    conn = backend._connect()  # noqa: SLF001 -- test-only direct truncate
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE zerobucket_images;")
        conn.commit()
    finally:
        conn.close()
    yield backend
    backend.close()


@pytest.fixture
def mysql_images(mysql_backend):
    zb = ZeroBucket(backend=mysql_backend)
    yield zb
    zb.close()


# ---- schema / construction -------------------------------------------


def test_auto_migrate_creates_schema(_mysql_available):
    backend = MySQLBackend(TEST_MYSQL_URL)
    conn = backend._connect()  # noqa: SLF001
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT TABLE_NAME FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'zerobucket_images';"
            )
            assert cur.fetchone() is not None
    finally:
        conn.close()
    backend.close()


def test_migration_is_idempotent(_mysql_available):
    MySQLBackend(TEST_MYSQL_URL).close()
    MySQLBackend(TEST_MYSQL_URL).close()  # must not raise on an already-migrated db


def test_auto_migrate_false_does_not_create_table_when_missing(mysql_backend):
    """Confirms auto_migrate=False is actually honored. Rather than
    standing up a whole separate database (the test user only has
    privileges scoped to zerobucket_test, not global CREATE DATABASE --
    a deliberate, least-privilege test setup, not worth widening just
    for this one test), drops the table within zerobucket_test itself,
    confirms auto_migrate=False leaves it missing, then restores the
    schema before returning so later tests in this file aren't
    affected."""
    conn = mysql_backend._connect()  # noqa: SLF001
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS zerobucket_images;")
        conn.commit()
    finally:
        conn.close()

    backend = MySQLBackend(TEST_MYSQL_URL, auto_migrate=False)
    zb = ZeroBucket(backend=backend)
    try:
        with pytest.raises(StorageError):
            zb.exists("00000000-0000-0000-0000-000000000000")
    finally:
        zb.close()
        MySQLBackend(TEST_MYSQL_URL).close()  # restore schema, auto_migrate=True


# ---- put / get round trip through the full client ---------------------


def test_put_get_round_trip(mysql_images, jpeg_bytes):
    image_id = mysql_images.put(jpeg_bytes)
    image = mysql_images.get(image_id)
    assert image.data == jpeg_bytes
    assert image.mime_type == "image/jpeg"


def test_ids_are_real_uuids_and_unique(mysql_images, jpeg_bytes):
    import uuid

    id_a = mysql_images.put(jpeg_bytes)
    id_b = mysql_images.put(jpeg_bytes)
    uuid.UUID(id_a)
    uuid.UUID(id_b)
    assert id_a != id_b


def test_get_not_found_raises(mysql_images):
    with pytest.raises(ImageNotFoundError):
        mysql_images.get("00000000-0000-0000-0000-000000000000")


def test_put_validates_input(mysql_images):
    """Confirms client.py's validation (Pillow decode, format/size
    checks) runs identically regardless of backend -- proof the
    backend split works as designed, not a MySQL-specific behavior."""
    with pytest.raises(Exception):  # noqa: B017 -- ImageValidationError family
        mysql_images.put(b"not a real image")


def test_filename_preserved(mysql_images, jpeg_bytes):
    image_id = mysql_images.put(jpeg_bytes, filename="photo.jpg")
    assert mysql_images.get(image_id).filename == "photo.jpg"


def test_checksum_present_and_correct(mysql_images, jpeg_bytes):
    import hashlib

    image_id = mysql_images.put(jpeg_bytes)
    image = mysql_images.get(image_id)
    assert image.checksum_sha256 == hashlib.sha256(jpeg_bytes).hexdigest()


def test_large_blob_round_trips_exactly(mysql_images):
    """LONGBLOB should have no trouble with a multi-megabyte payload --
    exercised directly rather than assumed from the column type alone.
    Random noise pixels (not a solid color) so PNG compression can't
    collapse this down to a tiny payload and defeat the point of the
    test."""
    import io
    import os

    from PIL import Image as PILImage

    raw = os.urandom(600 * 450 * 3)
    img = PILImage.frombytes("RGB", (600, 450), raw)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    data = buf.getvalue()
    assert len(data) > 100_000

    image_id = mysql_images.put(data)
    assert mysql_images.get(image_id).data == data


# ---- metadata / exists / delete ----------------------------------------


def test_metadata_without_bytes(mysql_images, jpeg_bytes):
    image_id = mysql_images.put(jpeg_bytes)
    meta = mysql_images.metadata(image_id)
    full = mysql_images.get(image_id)
    assert meta.size_bytes == len(jpeg_bytes) == full.size_bytes
    assert meta.width == full.width
    assert meta.height == full.height


def test_exists_true_then_false(mysql_images, jpeg_bytes):
    image_id = mysql_images.put(jpeg_bytes)
    assert mysql_images.exists(image_id) is True
    mysql_images.delete(image_id)
    assert mysql_images.exists(image_id) is False


def test_delete_returns_true_then_false(mysql_images, jpeg_bytes):
    image_id = mysql_images.put(jpeg_bytes)
    assert mysql_images.delete(image_id) is True
    assert mysql_images.delete(image_id) is False


# ---- batch operations ----------------------------------------------------


def test_put_many_all_succeed(mysql_images, jpeg_bytes, png_bytes):
    results = mysql_images.put_many([jpeg_bytes, png_bytes])
    assert all(r.success for r in results)
    assert len({r.image_id for r in results}) == 2


def test_put_many_partial_failure_is_best_effort(mysql_images, jpeg_bytes):
    results = mysql_images.put_many([jpeg_bytes, b"not an image", jpeg_bytes])
    assert results[0].success
    assert not results[1].success
    assert results[2].success


def test_get_many_mixed_found_and_missing(mysql_images, jpeg_bytes):
    image_id = mysql_images.put(jpeg_bytes)
    results = mysql_images.get_many([image_id, "00000000-0000-0000-0000-000000000000"])
    by_id = {r.image_id: r for r in results}
    assert by_id[image_id].success
    assert not by_id["00000000-0000-0000-0000-000000000000"].success


def test_get_many_empty_list(mysql_images):
    assert mysql_images.get_many([]) == []


def test_delete_many_mixed_existing_and_missing(mysql_images, jpeg_bytes):
    id_a = mysql_images.put(jpeg_bytes)
    id_b = mysql_images.put(jpeg_bytes)
    results = mysql_images.delete_many([id_a, "00000000-0000-0000-0000-000000000000"])
    by_id = {r.image_id: r for r in results}
    assert by_id[id_a].deleted is True
    assert by_id["00000000-0000-0000-0000-000000000000"].deleted is False
    assert mysql_images.exists(id_b) is True  # untouched


def test_delete_many_empty_list(mysql_images):
    assert mysql_images.delete_many([]) == []


def test_delete_many_large_batch_builds_correct_in_clause(mysql_images, jpeg_bytes):
    """Specifically exercises the dynamically-sized IN (%s, %s, ...)
    placeholder list (MySQL's replacement for Postgres's
    = ANY(array), same reasoning as SQLite's equivalent test) with
    enough ids that a naive off-by-one would show up."""
    ids = [mysql_images.put(jpeg_bytes) for _ in range(25)]
    results = mysql_images.delete_many(ids)
    assert all(r.deleted for r in results)
    assert all(not mysql_images.exists(i) for i in ids)


def test_delete_many_only_reports_ids_that_actually_existed(mysql_images, jpeg_bytes):
    """Exercises the SELECT ... FOR UPDATE then DELETE shape directly
    (see MySQLBackend.delete_many's docstring for why there are two
    statements instead of one RETURNING-based statement): a batch mixing
    real and fake ids must delete only the real ones and report exactly
    that set back, not the full input list."""
    id_a = mysql_images.put(jpeg_bytes)
    fake_ids = [f"00000000-0000-0000-0000-{i:012d}" for i in range(5)]
    results = mysql_images.delete_many([id_a, *fake_ids])
    deleted = {r.image_id for r in results if r.deleted}
    assert deleted == {id_a}


# ---- get_stream() (Phase 2) -------------------------------------------


def _bigger_jpeg_bytes():
    import io

    from PIL import Image as PILImage

    img = PILImage.new("RGB", (800, 600), color=(9, 88, 177))
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


def test_get_stream_reconstructs_exact_bytes(mysql_images):
    data = _bigger_jpeg_bytes()
    image_id = mysql_images.put(data)

    chunks = list(mysql_images.get_stream(image_id, chunk_size=1000))
    assert b"".join(chunks) == data
    assert len(chunks) > 1


def test_get_stream_not_found_raises(mysql_images):
    with pytest.raises(ImageNotFoundError):
        mysql_images.get_stream("00000000-0000-0000-0000-000000000000")


def test_get_stream_small_chunk_size_all_but_last_chunk_exact(mysql_images):
    data = _bigger_jpeg_bytes()
    image_id = mysql_images.put(data)
    chunks = list(mysql_images.get_stream(image_id, chunk_size=97))
    for chunk in chunks[:-1]:
        assert len(chunk) == 97
    assert len(chunks[-1]) <= 97


def test_get_stream_raises_on_concurrent_delete_mid_stream(mysql_backend, jpeg_bytes):
    """VERIFIED behavior, matching Postgres (and MySQL async SQLite,
    but NOT sync SQLite -- see that adapter's docstring for why WAL
    snapshot isolation makes it differ): each chunk is its own round
    trip with no held snapshot, so a row deleted by a different
    connection mid-stream causes the next chunk fetch to see nothing
    and raise StorageError rather than silently returning a short
    read."""
    zb = ZeroBucket(backend=mysql_backend)
    data = _bigger_jpeg_bytes()
    image_id = zb.put(data)

    stream = zb.get_stream(image_id, chunk_size=50)
    first_chunk = next(stream)
    assert first_chunk

    other_conn = mysql_backend._connect()  # noqa: SLF001
    with other_conn.cursor() as cur:
        cur.execute("DELETE FROM zerobucket_images WHERE id = %s;", (image_id,))
    other_conn.commit()
    other_conn.close()

    with pytest.raises(StorageError):
        b"".join(stream)

    zb.close()


# ---- tier_to_object_storage() (Phase 2) ---------------------------------


@pytest.fixture
def s3_bucket():
    from moto import mock_aws

    with mock_aws():
        import boto3

        bucket = "zerobucket-mysql-tier-test"
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=bucket)
        yield bucket


@pytest.fixture
def object_store(s3_bucket):
    from zerobucket import ObjectStorage

    return ObjectStorage(s3_bucket, region_name="us-east-1")


@pytest.fixture
def tiered_backend(_mysql_available, object_store):
    backend = MySQLBackend(TEST_MYSQL_URL, object_storage=object_store)
    conn = backend._connect()  # noqa: SLF001
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE zerobucket_images;")
        conn.commit()
    finally:
        conn.close()
    yield backend
    backend.close()


@pytest.fixture
def tiered_images(tiered_backend):
    zb = ZeroBucket(backend=tiered_backend)
    yield zb
    zb.close()


def test_tier_moves_bytes_out_of_mysql(
    tiered_images, tiered_backend, object_store, jpeg_bytes
):
    image_id = tiered_images.put(jpeg_bytes)
    assert tiered_backend.tier_to_object_storage(image_id) is True
    assert object_store.download(image_id) == jpeg_bytes


def test_tier_is_idempotent_no_op_on_second_call(
    tiered_images, tiered_backend, jpeg_bytes
):
    image_id = tiered_images.put(jpeg_bytes)
    assert tiered_backend.tier_to_object_storage(image_id) is True
    assert tiered_backend.tier_to_object_storage(image_id) is False


def test_tier_not_found_returns_none(tiered_backend):
    assert (
        tiered_backend.tier_to_object_storage("00000000-0000-0000-0000-000000000000")
        is None
    )


def test_tier_without_object_storage_configured_raises(mysql_images, jpeg_bytes):
    image_id = mysql_images.put(jpeg_bytes)
    with pytest.raises(StorageError, match="object_storage"):
        mysql_images._backend.tier_to_object_storage(image_id)


def test_get_works_identically_after_tiering(tiered_images, tiered_backend, jpeg_bytes):
    image_id = tiered_images.put(jpeg_bytes)
    tiered_backend.tier_to_object_storage(image_id)
    assert tiered_images.get(image_id).data == jpeg_bytes


def test_get_stream_delegates_to_object_storage_after_tiering(
    tiered_images, tiered_backend
):
    data = _bigger_jpeg_bytes()
    image_id = tiered_images.put(data)
    tiered_backend.tier_to_object_storage(image_id)
    chunks = list(tiered_images.get_stream(image_id, chunk_size=500))
    assert b"".join(chunks) == data


def test_delete_cleans_up_both_mysql_row_and_object_storage(
    tiered_images, tiered_backend, object_store, jpeg_bytes
):
    image_id = tiered_images.put(jpeg_bytes)
    tiered_backend.tier_to_object_storage(image_id)
    assert object_store.exists(image_id) is True

    assert tiered_images.delete(image_id) is True
    assert tiered_images.exists(image_id) is False
    assert object_store.exists(image_id) is False


def test_failed_upload_leaves_row_completely_untouched(
    tiered_images, tiered_backend, object_store, jpeg_bytes
):
    """The core safety guarantee: the upload happens inside the same
    transaction as the row lock/update, so a failed upload rolls back
    the whole transaction, leaving the row exactly as if tiering had
    never been attempted."""
    image_id = tiered_images.put(jpeg_bytes)

    def broken_upload(*args, **kwargs):
        raise RuntimeError("simulated network failure")

    original_upload = object_store.upload
    object_store.upload = broken_upload
    try:
        with pytest.raises(StorageError):
            tiered_backend.tier_to_object_storage(image_id)
    finally:
        object_store.upload = original_upload

    assert tiered_images.get(image_id).data == jpeg_bytes
    conn = tiered_backend._connect()  # noqa: SLF001
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT storage_backend FROM zerobucket_images WHERE id = %s;",
                (image_id,),
            )
            assert cur.fetchone()[0] == "mysql"
    finally:
        conn.close()


def test_tier_row_lock_blocks_writes_to_same_row_but_not_others(
    tiered_backend, object_store, jpeg_bytes
):
    """Confirms tiering's row lock is genuinely per-row (real InnoDB
    `SELECT ... FOR UPDATE`), NOT a whole-table/whole-file lock like
    SQLite's `BEGIN IMMEDIATE` fallback has to use: a concurrent write
    to a DIFFERENT row must succeed immediately while tiering is in
    flight, and a concurrent write to the SAME row must block/time out
    until tiering's transaction finishes. Deterministic (a short
    innodb_lock_wait_timeout + a deliberately slowed-down upload), not
    a flaky timing guess."""
    import threading
    import time

    import pymysql

    zb = ZeroBucket(backend=tiered_backend)
    image_a = zb.put(jpeg_bytes)
    image_b = zb.put(jpeg_bytes)

    real_upload = object_store.upload
    upload_started = threading.Event()

    def slow_upload(*args, **kwargs):
        upload_started.set()
        time.sleep(2.5)  # comfortably longer than innodb_lock_wait_timeout below
        return real_upload(*args, **kwargs)

    object_store.upload = slow_upload

    tier_result = []

    def do_tier():
        tier_result.append(tiered_backend.tier_to_object_storage(image_a))

    tier_thread = threading.Thread(target=do_tier)
    tier_thread.start()
    assert upload_started.wait(timeout=5), "tier never reached the upload step"
    time.sleep(0.05)  # make sure the row lock has definitely been acquired

    other_conn = tiered_backend._connect()  # noqa: SLF001
    try:
        with other_conn.cursor() as cur:
            cur.execute("SET SESSION innodb_lock_wait_timeout = 1;")

            # A write to a DIFFERENT row must NOT be blocked -- real row-level locking.
            start = time.monotonic()
            cur.execute(
                "UPDATE zerobucket_images SET original_filename = 'x' WHERE id = %s;",
                (image_b,),
            )
            elapsed = time.monotonic() - start
            other_conn.commit()
            assert elapsed < 0.3, "write to a DIFFERENT row should not have blocked"

            # A write to the SAME row must block, then time out -- the row lock is real.
            with pytest.raises(pymysql.err.OperationalError, match="Lock wait timeout"):
                cur.execute(
                    "UPDATE zerobucket_images SET original_filename = 'y' WHERE id = %s;",
                    (image_a,),
                )
            other_conn.rollback()
    finally:
        other_conn.close()

    tier_thread.join(timeout=5)
    object_store.upload = real_upload
    zb.close()

    assert tier_result == [True]


# ---- connection= participation -----------------------------------------


def test_connection_participates_in_callers_transaction(mysql_backend, jpeg_bytes):
    """A put() using a caller-supplied connection that's never committed
    must leave no trace -- confirms connection= is honored (not
    committed/rolled back internally) same as every other adapter."""
    conn = mysql_backend._connect()  # noqa: SLF001
    try:
        image_id = mysql_backend.put(
            data=jpeg_bytes,
            mime_type="image/jpeg",
            original_filename=None,
            size_bytes=len(jpeg_bytes),
            width=1,
            height=1,
            checksum_sha256="0" * 64,
            connection=conn,
        )
        # Visible within the same uncommitted connection/transaction.
        assert mysql_backend.exists(image_id, connection=conn) is True
        conn.rollback()
    finally:
        conn.close()

    # Never committed -- must not exist from a fresh connection's view.
    assert mysql_backend.exists(image_id) is False
