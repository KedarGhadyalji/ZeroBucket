"""Tests for SQLiteBackend -- Phase 1 (classic-mode core CRUD, see
v0.16.0) plus Phase 2 (get_stream()/tier_to_object_storage(), this
round). Streaming and tiering are the two features covered here.
NOT yet implemented for SQLite, tracked as explicit follow-up: dedup
mode, async support.

Runs against REAL infrastructure throughout: a real SQLite file on disk
(via tempfile) and, for tiering, a real boto3 client against moto's S3
emulator (same approach used for the Postgres adapter's own tiering
tests) -- not mocked at the boundary being tested.
"""

from __future__ import annotations

import os
import tempfile

import pytest
from moto import mock_aws

from zerobucket import AccessDeniedError, ImageNotFoundError, ZeroBucket
from zerobucket.adapters.sqlite import SQLiteBackend
from zerobucket.exceptions import StorageError
from zerobucket.object_storage import ObjectStorage


@pytest.fixture
def sqlite_path():
    path = tempfile.mktemp(suffix=".db")
    yield path
    for ext in ("", "-wal", "-shm"):
        if os.path.exists(path + ext):
            os.remove(path + ext)


@pytest.fixture
def sqlite_images(sqlite_path):
    zb = ZeroBucket(backend=SQLiteBackend(sqlite_path))
    yield zb
    zb.close()


# ---- schema / construction -------------------------------------------


def test_auto_migrate_creates_schema(sqlite_path):
    SQLiteBackend(sqlite_path)
    assert os.path.exists(sqlite_path)

    import sqlite3

    conn = sqlite3.connect(sqlite_path)
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='zerobucket_images';"
    )
    assert cur.fetchone() is not None
    conn.close()


def test_migration_is_idempotent(sqlite_path):
    SQLiteBackend(sqlite_path)
    SQLiteBackend(sqlite_path)  # must not raise on an already-migrated file


def test_auto_migrate_false_does_not_create_schema(sqlite_path):
    SQLiteBackend(sqlite_path, auto_migrate=False)
    zb = ZeroBucket(backend=SQLiteBackend(sqlite_path, auto_migrate=False))
    with pytest.raises(StorageError):
        zb.exists("00000000-0000-0000-0000-000000000000")


# ---- put / get round trip through the full client ---------------------


def test_put_get_round_trip(sqlite_images, jpeg_bytes):
    image_id = sqlite_images.put(jpeg_bytes)
    image = sqlite_images.get(image_id)
    assert image.data == jpeg_bytes
    assert image.mime_type == "image/jpeg"


def test_ids_are_real_uuids_and_unique(sqlite_images, jpeg_bytes):
    import uuid

    id_a = sqlite_images.put(jpeg_bytes)
    id_b = sqlite_images.put(jpeg_bytes)
    uuid.UUID(id_a)
    uuid.UUID(id_b)
    assert id_a != id_b


def test_get_not_found_raises(sqlite_images):
    with pytest.raises(ImageNotFoundError):
        sqlite_images.get("00000000-0000-0000-0000-000000000000")


def test_put_validates_input(sqlite_images):
    """Confirms client.py's validation (Pillow decode, format/size
    checks) runs identically regardless of backend -- this isn't a
    SQLite-specific behavior, it's proof the backend split actually
    works as designed."""
    with pytest.raises(Exception):  # noqa: B017 -- ImageValidationError family
        sqlite_images.put(b"not a real image")


def test_filename_preserved(sqlite_images, jpeg_bytes):
    image_id = sqlite_images.put(jpeg_bytes, filename="photo.jpg")
    assert sqlite_images.get(image_id).filename == "photo.jpg"


def test_checksum_present_and_correct(sqlite_images, jpeg_bytes):
    import hashlib

    image_id = sqlite_images.put(jpeg_bytes)
    image = sqlite_images.get(image_id)
    assert image.checksum_sha256 == hashlib.sha256(jpeg_bytes).hexdigest()


# ---- metadata / exists / delete ----------------------------------------


def test_metadata_without_bytes(sqlite_images, jpeg_bytes):
    image_id = sqlite_images.put(jpeg_bytes)
    meta = sqlite_images.metadata(image_id)
    full = sqlite_images.get(image_id)
    assert meta.size_bytes == len(jpeg_bytes) == full.size_bytes
    assert meta.width == full.width
    assert meta.height == full.height


def test_exists_true_then_false(sqlite_images, jpeg_bytes):
    image_id = sqlite_images.put(jpeg_bytes)
    assert sqlite_images.exists(image_id) is True
    sqlite_images.delete(image_id)
    assert sqlite_images.exists(image_id) is False


def test_delete_returns_true_then_false(sqlite_images, jpeg_bytes):
    image_id = sqlite_images.put(jpeg_bytes)
    assert sqlite_images.delete(image_id) is True
    assert sqlite_images.delete(image_id) is False


# ---- batch operations ----------------------------------------------------


def test_put_many_all_succeed(sqlite_images, jpeg_bytes, png_bytes):
    results = sqlite_images.put_many([jpeg_bytes, png_bytes])
    assert all(r.success for r in results)
    assert len({r.image_id for r in results}) == 2


def test_put_many_partial_failure_is_best_effort(sqlite_images, jpeg_bytes):
    results = sqlite_images.put_many([jpeg_bytes, b"not an image", jpeg_bytes])
    assert results[0].success
    assert not results[1].success
    assert results[2].success
    # Confirms the one transaction covering the whole batch (see
    # SQLiteBackend.put_many's docstring) doesn't mean "all or nothing"
    # at the CLIENT level -- client.py filters bad rows out BEFORE
    # calling the backend, so only the two valid rows ever reach
    # put_many() at all.


def test_get_many_mixed_found_and_missing(sqlite_images, jpeg_bytes):
    image_id = sqlite_images.put(jpeg_bytes)
    results = sqlite_images.get_many([image_id, "00000000-0000-0000-0000-000000000000"])
    by_id = {r.image_id: r for r in results}
    assert by_id[image_id].success
    assert not by_id["00000000-0000-0000-0000-000000000000"].success


def test_get_many_empty_list(sqlite_images):
    assert sqlite_images.get_many([]) == []


def test_delete_many_mixed_existing_and_missing(sqlite_images, jpeg_bytes):
    id_a = sqlite_images.put(jpeg_bytes)
    id_b = sqlite_images.put(jpeg_bytes)
    results = sqlite_images.delete_many([id_a, "00000000-0000-0000-0000-000000000000"])
    by_id = {r.image_id: r for r in results}
    assert by_id[id_a].deleted is True
    assert by_id["00000000-0000-0000-0000-000000000000"].deleted is False
    assert sqlite_images.exists(id_b) is True  # untouched


def test_delete_many_empty_list(sqlite_images):
    assert sqlite_images.delete_many([]) == []


def test_delete_many_large_batch_builds_correct_in_clause(sqlite_images, jpeg_bytes):
    """Specifically exercises the dynamically-sized IN (?, ?, ...)
    placeholder list (SQLite's replacement for Postgres's
    = ANY(array)) with enough ids that a naive off-by-one in building
    the placeholder string would show up."""
    ids = [sqlite_images.put(jpeg_bytes) for _ in range(25)]
    results = sqlite_images.delete_many(ids)
    assert all(r.deleted for r in results)
    assert all(not sqlite_images.exists(i) for i in ids)


# ---- get_stream() ----------------------------------------------------


def _bigger_jpeg_bytes():
    import io

    from PIL import Image as PILImage

    img = PILImage.new("RGB", (800, 600), color=(9, 88, 177))
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


def test_get_stream_reconstructs_exact_bytes(sqlite_images):
    data = _bigger_jpeg_bytes()
    image_id = sqlite_images.put(data)

    chunks = list(sqlite_images.get_stream(image_id, chunk_size=1000))
    assert b"".join(chunks) == data
    assert len(chunks) > 1


def test_get_stream_not_found_raises(sqlite_images):
    with pytest.raises(ImageNotFoundError):
        sqlite_images.get_stream("00000000-0000-0000-0000-000000000000")


def test_get_stream_small_chunk_size_all_but_last_chunk_exact(sqlite_images):
    data = _bigger_jpeg_bytes()
    image_id = sqlite_images.put(data)
    chunks = list(sqlite_images.get_stream(image_id, chunk_size=97))
    for chunk in chunks[:-1]:
        assert len(chunk) == 97
    assert len(chunks[-1]) <= 97


def test_get_stream_survives_concurrent_delete_mid_stream(sqlite_path):
    """VERIFIED, different-from-Postgres behavior, not a bug: SQLite's
    WAL-mode snapshot isolation means a stream already in progress keeps
    reading the row's original data even after a DIFFERENT connection
    deletes it. Confirmed here for the full get_stream() path, not just
    in isolation against bare blobopen() -- see this method's docstring
    for why this differs from the Postgres adapter (which raises
    StorageError in the equivalent scenario) and why that's not
    something to assume is universal across backends."""
    import sqlite3

    backend = SQLiteBackend(sqlite_path)
    zb = ZeroBucket(backend=backend)
    data = _bigger_jpeg_bytes()
    image_id = zb.put(data)

    stream = zb.get_stream(image_id, chunk_size=50)
    first_chunk = next(stream)
    assert first_chunk

    other_conn = sqlite3.connect(sqlite_path)
    other_conn.execute("DELETE FROM zerobucket_images WHERE id = ?;", (image_id,))
    other_conn.commit()
    other_conn.close()

    # The stream survives the concurrent delete and still delivers the
    # complete, correct original bytes.
    rest = b"".join(stream)
    assert first_chunk + rest == data

    # But the row really is gone now, from a fresh connection's view.
    assert zb.exists(image_id) is False
    zb.close()


# ---- tier_to_object_storage() ------------------------------------------


@pytest.fixture
def s3_bucket():
    with mock_aws():
        import boto3

        bucket = "zerobucket-sqlite-tier-test"
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=bucket)
        yield bucket


@pytest.fixture
def object_store(s3_bucket):
    return ObjectStorage(s3_bucket, region_name="us-east-1")


@pytest.fixture
def tiered_backend(sqlite_path, object_store):
    return SQLiteBackend(sqlite_path, object_storage=object_store)


@pytest.fixture
def tiered_images(tiered_backend):
    zb = ZeroBucket(backend=tiered_backend)
    yield zb
    zb.close()


def test_tier_moves_bytes_out_of_sqlite(
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


def test_tier_without_object_storage_configured_raises(sqlite_images, jpeg_bytes):
    image_id = sqlite_images.put(jpeg_bytes)
    with pytest.raises(StorageError, match="object_storage"):
        sqlite_images._backend.tier_to_object_storage(image_id)


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


def test_delete_cleans_up_both_sqlite_row_and_object_storage(
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
    """The core safety guarantee: BEGIN IMMEDIATE means a failed upload
    rolls back the whole transaction, leaving the row exactly as if
    tiering had never been attempted."""
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
    import sqlite3

    conn = sqlite3.connect(tiered_backend._database_path)  # noqa: SLF001
    cur = conn.execute(
        "SELECT storage_backend FROM zerobucket_images WHERE id = ?;", (image_id,)
    )
    assert cur.fetchone()[0] == "sqlite"
    conn.close()


def test_tier_blocks_other_writes_for_its_duration(
    tiered_backend, object_store, jpeg_bytes
):
    """Confirms the documented coarse-lock tradeoff is real, not just
    described in the docstring: BEGIN IMMEDIATE during tiering should
    block a concurrent writer using a separate connection until the
    tiering transaction finishes.

    Uses a deterministic technique, not a timing measurement: a second
    connection with a very short busy_timeout attempts a write WHILE
    the tiering upload is deliberately slowed down and still in
    progress. If tiering is genuinely holding the write lock, that
    second connection's write must fail with `sqlite3.OperationalError:
    database is locked` (verified in isolation first, against a bare
    BEGIN IMMEDIATE with no other project code involved, before writing
    this test around the real tier_to_object_storage() call)."""
    import sqlite3
    import threading
    import time

    zb = ZeroBucket(backend=tiered_backend)
    image_id = zb.put(jpeg_bytes)

    real_upload = object_store.upload
    upload_started = threading.Event()

    def slow_upload(*args, **kwargs):
        upload_started.set()
        time.sleep(0.4)
        return real_upload(*args, **kwargs)

    object_store.upload = slow_upload

    tier_result = []

    def do_tier():
        tier_result.append(tiered_backend.tier_to_object_storage(image_id))

    tier_thread = threading.Thread(target=do_tier)
    tier_thread.start()
    assert upload_started.wait(timeout=5), "tier never reached the upload step"
    time.sleep(0.05)  # make sure BEGIN IMMEDIATE has definitely been issued

    other_conn = sqlite3.connect(
        tiered_backend._database_path, timeout=0.05
    )  # noqa: SLF001
    try:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            other_conn.execute(
                "UPDATE zerobucket_images SET original_filename = 'x' WHERE id = ?;",
                (image_id,),
            )
    finally:
        other_conn.close()

    tier_thread.join(timeout=5)
    object_store.upload = real_upload
    zb.close()

    assert tier_result == [True]


# ---- access-control hooks work identically (backend-agnostic) --------


def test_before_get_hook_works_with_sqlite_backend(sqlite_path, jpeg_bytes):
    """The before_get/before_put hooks live entirely in client.py and
    should work unmodified regardless of backend -- confirmed directly
    against SQLiteBackend, not just assumed because it works for
    Postgres."""
    zb = ZeroBucket(
        backend=SQLiteBackend(sqlite_path),
        before_get=lambda image_id, context: False,
    )
    image_id = zb.put(jpeg_bytes)
    with pytest.raises(AccessDeniedError):
        zb.get(image_id)
    zb.close()


# ---- connection= participation -----------------------------------------


def test_connection_kwarg_shares_one_sqlite_transaction(sqlite_path, jpeg_bytes):
    """A write made on a caller-supplied connection, not yet committed,
    should be readable back on that SAME connection before commit --
    same contract the Postgres adapter's connection= supports."""
    import sqlite3

    backend = SQLiteBackend(sqlite_path)
    zb = ZeroBucket(backend=backend)

    conn = sqlite3.connect(sqlite_path)
    try:
        image_id = zb.put(jpeg_bytes, connection=conn)
        image = zb.get(image_id, connection=conn)
        assert image.data == jpeg_bytes
        conn.rollback()
    finally:
        conn.close()

    # Rolled back -- should not exist via a fresh connection.
    assert zb.exists(image_id) is False
    zb.close()


# ---- dedup mode -----------------------------------------------------------


@pytest.fixture
def dedup_images(sqlite_path):
    zb = ZeroBucket(backend=SQLiteBackend(sqlite_path, dedup=True))
    yield zb
    zb.close()


def test_dedup_plus_object_storage_rejected_at_construction(sqlite_path):
    from zerobucket.object_storage import ObjectStorage

    store = ObjectStorage.__new__(ObjectStorage)  # avoid needing boto3/real creds
    with pytest.raises(ValueError, match="dedup"):
        SQLiteBackend(sqlite_path, dedup=True, object_storage=store)


def test_dedup_schema_created(sqlite_path):
    SQLiteBackend(sqlite_path, dedup=True)
    import sqlite3

    conn = sqlite3.connect(sqlite_path)
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()
    }
    assert "zerobucket_blobs" in tables
    assert "zerobucket_image_refs" in tables
    conn.close()


def test_dedup_put_identical_content_shares_one_blob(
    dedup_images, jpeg_bytes, sqlite_path
):
    id_a = dedup_images.put(jpeg_bytes)
    id_b = dedup_images.put(jpeg_bytes)
    assert id_a != id_b

    import sqlite3

    conn = sqlite3.connect(sqlite_path)
    blob_count = conn.execute("SELECT COUNT(*) FROM zerobucket_blobs;").fetchone()[0]
    ref_count = conn.execute("SELECT ref_count FROM zerobucket_blobs;").fetchone()[0]
    conn.close()
    assert blob_count == 1
    assert ref_count == 2


def test_dedup_get_both_refs_return_correct_bytes(dedup_images, jpeg_bytes):
    id_a = dedup_images.put(jpeg_bytes)
    id_b = dedup_images.put(jpeg_bytes)
    assert dedup_images.get(id_a).data == jpeg_bytes
    assert dedup_images.get(id_b).data == jpeg_bytes


def test_dedup_different_content_gets_different_blobs(
    dedup_images, jpeg_bytes, png_bytes, sqlite_path
):
    dedup_images.put(jpeg_bytes)
    dedup_images.put(png_bytes)
    import sqlite3

    conn = sqlite3.connect(sqlite_path)
    blob_count = conn.execute("SELECT COUNT(*) FROM zerobucket_blobs;").fetchone()[0]
    conn.close()
    assert blob_count == 2


def test_dedup_delete_one_ref_other_survives(dedup_images, jpeg_bytes):
    id_a = dedup_images.put(jpeg_bytes)
    id_b = dedup_images.put(jpeg_bytes)

    assert dedup_images.delete(id_a) is True
    assert dedup_images.exists(id_a) is False
    assert dedup_images.exists(id_b) is True
    assert dedup_images.get(id_b).data == jpeg_bytes


def test_dedup_deleting_last_ref_removes_blob(dedup_images, jpeg_bytes, sqlite_path):
    id_a = dedup_images.put(jpeg_bytes)
    dedup_images.delete(id_a)

    import sqlite3

    conn = sqlite3.connect(sqlite_path)
    blob_count = conn.execute("SELECT COUNT(*) FROM zerobucket_blobs;").fetchone()[0]
    conn.close()
    assert blob_count == 0


def test_dedup_delete_missing_id_returns_false(dedup_images):
    assert dedup_images.delete("00000000-0000-0000-0000-000000000000") is False


def test_dedup_put_many_repeated_content_accumulates_ref_count(
    dedup_images, jpeg_bytes, sqlite_path
):
    """Verifies the within-one-batch ref_count accumulation claim
    directly -- not just that put_many() succeeds, but that the
    resulting ref_count is exactly 3, not 1, matching the same
    guarantee the Postgres adapter verified empirically for its own
    executemany-based implementation."""
    results = dedup_images.put_many([jpeg_bytes, jpeg_bytes, jpeg_bytes])
    assert all(r.success for r in results)
    ids = [r.image_id for r in results]
    assert len(set(ids)) == 3  # three distinct ids

    import sqlite3

    conn = sqlite3.connect(sqlite_path)
    ref_count = conn.execute("SELECT ref_count FROM zerobucket_blobs;").fetchone()[0]
    conn.close()
    assert ref_count == 3


def test_dedup_delete_many_decrements_correctly_for_shared_checksum(
    dedup_images, jpeg_bytes, sqlite_path
):
    """Confirms delete_many()'s Counter-based decrement handles multiple
    ids sharing one checksum within a single call correctly, not just
    one-at-a-time delete()."""
    ids = [
        r.image_id for r in dedup_images.put_many([jpeg_bytes, jpeg_bytes, jpeg_bytes])
    ]

    results = dedup_images.delete_many(ids[:2])  # delete 2 of 3 refs
    assert all(r.deleted for r in results)

    import sqlite3

    conn = sqlite3.connect(sqlite_path)
    ref_count = conn.execute("SELECT ref_count FROM zerobucket_blobs;").fetchone()[0]
    conn.close()
    assert ref_count == 1
    assert dedup_images.exists(ids[2]) is True


def test_dedup_get_many_mixed(dedup_images, jpeg_bytes):
    id_a = dedup_images.put(jpeg_bytes)
    results = dedup_images.get_many([id_a, "00000000-0000-0000-0000-000000000000"])
    by_id = {r.image_id: r for r in results}
    assert by_id[id_a].success
    assert not by_id["00000000-0000-0000-0000-000000000000"].success


def test_dedup_get_stream(dedup_images):
    data = _bigger_jpeg_bytes()
    image_id = dedup_images.put(data)
    chunks = list(dedup_images.get_stream(image_id, chunk_size=1000))
    assert b"".join(chunks) == data


def test_dedup_get_stream_two_refs_same_blob_both_correct(dedup_images):
    """Two ids referencing the SAME blob must each stream their own
    full, correct content -- streaming shouldn't confuse state between
    refs sharing one underlying blob."""
    data = _bigger_jpeg_bytes()
    id_a = dedup_images.put(data)
    id_b = dedup_images.put(data)
    assert b"".join(dedup_images.get_stream(id_a, chunk_size=333)) == data
    assert b"".join(dedup_images.get_stream(id_b, chunk_size=333)) == data


def test_dedup_metadata(dedup_images, jpeg_bytes):
    image_id = dedup_images.put(jpeg_bytes)
    meta = dedup_images.metadata(image_id)
    assert meta.size_bytes == len(jpeg_bytes)


def test_dedup_before_get_hook_still_works(sqlite_path, jpeg_bytes):
    zb = ZeroBucket(
        backend=SQLiteBackend(sqlite_path, dedup=True),
        before_get=lambda image_id, context: False,
    )
    image_id = zb.put(jpeg_bytes)
    with pytest.raises(AccessDeniedError):
        zb.get(image_id)
    zb.close()
