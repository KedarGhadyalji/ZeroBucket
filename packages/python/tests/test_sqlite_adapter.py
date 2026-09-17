"""Tests for SQLiteBackend -- Phase 1 of the SQLite adapter (see
adapters/sqlite.py's module docstring for what's implemented so far:
classic-mode core CRUD only, no streaming/dedup/tiering/async yet).

Runs against a REAL SQLite file on disk (via tempfile), not an
in-memory mock -- same "test against real infrastructure" philosophy
used for the Postgres/S3/Django test suites throughout this project.
Goes through the full ZeroBucket client (via backend=SQLiteBackend(...)),
not the raw backend directly, for most tests -- confirms the client
layer (validation, checksums, hooks) is genuinely backend-agnostic in
practice, not just in theory.
"""

from __future__ import annotations

import os
import tempfile

import pytest

from zerobucket import AccessDeniedError, ImageNotFoundError, ZeroBucket
from zerobucket.adapters.sqlite import SQLiteBackend
from zerobucket.exceptions import StorageError


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


# ---- get_stream is explicitly not implemented yet --------------------


def test_get_stream_raises_not_implemented(sqlite_images, jpeg_bytes):
    """Confirms the Phase 1 gap fails loudly and clearly, not silently
    or with a confusing low-level error."""
    image_id = sqlite_images.put(jpeg_bytes)
    with pytest.raises(NotImplementedError, match="not yet implemented"):
        sqlite_images.get_stream(image_id)


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
