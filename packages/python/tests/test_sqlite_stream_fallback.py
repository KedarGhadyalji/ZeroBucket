"""SQLiteBackend.get_stream() on Pythons without Connection.blobopen() (3.10).

blobopen() (incremental BLOB I/O) arrived in Python 3.11, but this package supports 3.10,
so get_stream() has a fallback that reads the same bytes with substr() range queries
inside one held read transaction. These tests run every scenario through BOTH
implementations by switching the module flag, so the fallback is exercised on 3.11 and
3.12 as well, not only on the one Python where it is the only option. (On 3.10 the
"blobopen" variant is skipped: there is nothing to switch to.)
"""

from __future__ import annotations

import io
import os
import sqlite3
import tempfile

import pytest
from PIL import Image as PILImage

from zerobucket import ZeroBucket
from zerobucket.adapters import sqlite as sqlite_mod
from zerobucket.adapters.sqlite import SQLiteBackend
from zerobucket.exceptions import StorageError


@pytest.fixture(params=["blobopen", "substr"])
def impl(request, monkeypatch):
    if request.param == "blobopen" and not hasattr(sqlite3.Connection, "blobopen"):
        pytest.skip("this Python has no Connection.blobopen (3.10): only the fallback exists")
    monkeypatch.setattr(sqlite_mod, "_HAS_BLOBOPEN", request.param == "blobopen")
    return request.param


@pytest.fixture
def path():
    p = tempfile.mktemp(suffix=".db")
    yield p
    for ext in ("", "-wal", "-shm"):
        if os.path.exists(p + ext):
            os.remove(p + ext)


@pytest.fixture(params=["classic", "dedup"])
def zb(request, path):
    z = ZeroBucket(backend=SQLiteBackend(path, dedup=(request.param == "dedup")))
    yield z
    z.close()


def _noise_jpeg(side: int = 300) -> bytes:
    """A JPEG of random pixels: large for its dimensions and different on every call."""
    img = PILImage.frombytes("RGB", (side, side), os.urandom(side * side * 3))
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=95)
    return buf.getvalue()


def _delete_the_bytes_from_another_connection(path: str, image_id: str, dedup: bool) -> None:
    """Delete the row get_stream() actually reads from: the image row, or in dedup mode the
    shared blob row (deleting only the reference would leave the bytes in place and prove
    nothing)."""
    other = sqlite3.connect(path)
    try:
        if dedup:
            # these tests store exactly one distinct image
            other.execute("DELETE FROM zerobucket_blobs")
        else:
            other.execute("DELETE FROM zerobucket_images WHERE id = ?", (image_id,))
        other.commit()
    finally:
        other.close()


def test_the_flag_reflects_this_python():
    assert sqlite_mod._HAS_BLOBOPEN == hasattr(sqlite3.Connection, "blobopen")


def test_streams_the_exact_bytes_at_awkward_chunk_sizes(impl, zb):
    data = _noise_jpeg()
    image_id = zb.put(data)
    for chunk_size in (1_000, 97, 4_096, len(data), len(data) + 5, 1_048_576):
        chunks = list(zb.get_stream(image_id, chunk_size=chunk_size))
        assert b"".join(chunks) == data, chunk_size
        # every chunk but the last is exactly chunk_size, and none is empty
        assert all(len(c) == chunk_size for c in chunks[:-1]), chunk_size
        assert 0 < len(chunks[-1]) <= chunk_size, chunk_size


def test_a_stream_in_progress_keeps_its_snapshot_when_the_row_is_deleted_elsewhere(impl, zb, path):
    data = _noise_jpeg()
    image_id = zb.put(data)
    stream = zb.get_stream(image_id, chunk_size=500)
    first = next(stream)
    _delete_the_bytes_from_another_connection(path, image_id, dedup=zb._backend._dedup)
    rest = b"".join(stream)
    assert first + rest == data


def test_a_row_deleted_before_the_first_chunk_raises_storage_error(impl, zb, path):
    image_id = zb.put(_noise_jpeg())
    stream = zb.get_stream(image_id, chunk_size=500)  # nothing has been read yet
    _delete_the_bytes_from_another_connection(path, image_id, dedup=zb._backend._dedup)
    with pytest.raises(StorageError):
        next(stream)


def test_an_open_stream_does_not_block_writers_in_other_connections(impl, zb, path):
    image_id = zb.put(_noise_jpeg())
    stream = zb.get_stream(image_id, chunk_size=500)
    next(stream)  # a read transaction is now open
    other = ZeroBucket(backend=SQLiteBackend(path, dedup=zb._backend._dedup))
    try:
        # would raise "database is locked" if the open stream blocked writers
        new_id = other.put(_noise_jpeg(64))
        assert other.exists(new_id)
    finally:
        other.close()
        stream.close()


def test_the_read_transaction_is_released_after_completion_and_after_abandonment(impl, zb, path):
    data = _noise_jpeg()
    image_id = zb.put(data)
    conn = sqlite3.connect(path)
    try:
        assert b"".join(zb.get_stream(image_id, chunk_size=700, connection=conn)) == data
        assert conn.in_transaction is False, "finished stream left a transaction open"

        stream = zb.get_stream(image_id, chunk_size=700, connection=conn)
        next(stream)
        stream.close()  # abandoned half way
        assert conn.in_transaction is False, "abandoned stream left a transaction open"
    finally:
        conn.close()


def test_a_callers_own_open_transaction_is_used_and_left_open(impl, zb, path):
    data = _noise_jpeg()
    image_id = zb.put(data)
    conn = sqlite3.connect(path)
    try:
        conn.execute("BEGIN")
        assert conn.in_transaction
        assert b"".join(zb.get_stream(image_id, chunk_size=700, connection=conn)) == data
        assert conn.in_transaction, "the caller's transaction must not be ended by the stream"
        conn.rollback()
    finally:
        conn.close()


def test_two_references_to_one_dedup_blob_each_stream_correctly(impl, path):
    zb = ZeroBucket(backend=SQLiteBackend(path, dedup=True))
    try:
        data = _noise_jpeg()
        a, b = zb.put(data), zb.put(data)
        assert b"".join(zb.get_stream(a, chunk_size=333)) == data
        assert b"".join(zb.get_stream(b, chunk_size=1_000)) == data
    finally:
        zb.close()
