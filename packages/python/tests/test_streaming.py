"""Tests for the Stage 3 streaming feature:

- get_stream()/stream_to() (streaming reads): reconstructing the exact
  original bytes from chunks, not-found behavior, chunk_size control,
  connection= participation, dedup-mode parity, on_operation events, and
  the "concurrent delete mid-stream raises rather than silently
  truncates" safety rule.
- The write-side companion: put() with a file-like input bounds how much
  it reads before rejecting an oversized upload, instead of buffering the
  whole thing first.
"""

from __future__ import annotations

import io

import pytest
from PIL import Image as PILImage

from zerobucket import DEFAULT_STREAM_CHUNK_SIZE, OperationEvent, ZeroBucket
from zerobucket.exceptions import ImageNotFoundError, ImageTooLargeError


def _jpeg_bytes(size=(300, 200), color=(40, 80, 120)) -> bytes:
    img = PILImage.new("RGB", size, color=color)
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


# ---- get_stream() / stream_to() : basic correctness ----------------------


def test_get_stream_reconstructs_exact_bytes(images):
    data = _jpeg_bytes()
    image_id = images.put(data)

    chunks = list(images.get_stream(image_id, chunk_size=1024))
    assert b"".join(chunks) == data
    # Not a trivially-single-chunk test: a 300x200 JPEG is well over 1KB,
    # so this actually exercises multiple round trips, not just one.
    assert len(chunks) > 1


def test_get_stream_default_chunk_size_matches_constant(images):
    """DEFAULT_STREAM_CHUNK_SIZE is exported specifically so callers can
    reason about/override it -- confirm it's actually what get_stream()
    uses when chunk_size isn't passed, not just a documented default that
    silently drifted from the real one."""
    data = _jpeg_bytes()
    image_id = images.put(data)

    chunks = list(images.get_stream(image_id))
    # A single small JPEG is well under the 1 MiB default, so the whole
    # thing should come back as exactly one chunk.
    assert len(chunks) == 1
    assert chunks[0] == data
    assert DEFAULT_STREAM_CHUNK_SIZE == 1024 * 1024


def test_get_stream_small_chunk_size_forces_many_round_trips(images):
    data = _jpeg_bytes((800, 600))
    image_id = images.put(data)

    chunks = list(images.get_stream(image_id, chunk_size=100))
    assert b"".join(chunks) == data
    # Every chunk except possibly the last should be exactly chunk_size.
    for chunk in chunks[:-1]:
        assert len(chunk) == 100
    assert len(chunks[-1]) <= 100


def test_get_stream_missing_id_raises_before_any_chunk(images):
    """The not-found check happens eagerly (via get_metadata) before the
    generator is even created -- calling get_stream() itself raises,
    the caller doesn't need to start iterating to find out."""
    with pytest.raises(ImageNotFoundError):
        images.get_stream("00000000-0000-0000-0000-000000000000")


def test_stream_to_writes_full_content_and_returns_byte_count(images):
    data = _jpeg_bytes()
    image_id = images.put(data)

    destination = io.BytesIO()
    total = images.stream_to(image_id, destination, chunk_size=512)

    assert total == len(data)
    assert destination.getvalue() == data


def test_stream_to_missing_id_raises(images):
    destination = io.BytesIO()
    with pytest.raises(ImageNotFoundError):
        images.stream_to("00000000-0000-0000-0000-000000000000", destination)


# ---- dedup-mode parity ----------------------------------------------------


def test_get_stream_works_in_dedup_mode(dedup_images):
    data = _jpeg_bytes((400, 300))
    image_id = dedup_images.put(data)

    chunks = list(dedup_images.get_stream(image_id, chunk_size=256))
    assert b"".join(chunks) == data


def test_get_stream_dedup_two_refs_same_blob_both_stream_correctly(dedup_images):
    """Two ids referencing the SAME underlying blob (dedup) must each
    stream their own full content correctly -- streaming shouldn't leak
    or confuse state between refs sharing one blob."""
    data = _jpeg_bytes()
    id_a = dedup_images.put(data)
    id_b = dedup_images.put(data)  # identical content -> same blob, dedup'd
    assert id_a != id_b

    assert b"".join(dedup_images.get_stream(id_a, chunk_size=333)) == data
    assert b"".join(dedup_images.get_stream(id_b, chunk_size=333)) == data


# ---- connection= participation -------------------------------------------


def test_get_stream_with_connection_reads_uncommitted_write(
    images, db_connection_factory, jpeg_bytes
):
    """Same pattern as get()'s connection= tests: a write made on an open
    connection, not yet committed, should be streamable back on that same
    connection before commit."""
    conn = db_connection_factory()
    try:
        image_id = images.put(jpeg_bytes, connection=conn)
        chunks = list(images.get_stream(image_id, chunk_size=200, connection=conn))
        assert b"".join(chunks) == jpeg_bytes
        conn.rollback()
    finally:
        conn.close()

    # Rolled back -- should not exist on ZeroBucket's own pool.
    assert images.exists(image_id) is False


# ---- on_operation events ---------------------------------------------------


def test_get_stream_emits_get_metadata_then_get_stream_events(images, jpeg_bytes):
    events: list[OperationEvent] = []

    from tests.conftest import TEST_DATABASE_URL

    zb = ZeroBucket(database_url=TEST_DATABASE_URL, on_operation=events.append)
    try:
        image_id = zb.put(jpeg_bytes)
        events.clear()

        list(zb.get_stream(image_id, chunk_size=200))

        operations = [e.operation for e in events]
        # One get_metadata (the not-found/size check), then one or more
        # get_stream events (one per chunk round trip) -- not a single
        # opaque "get_stream" event for the whole call.
        assert operations[0] == "get_metadata"
        assert operations.count("get_stream") >= 1
        assert all(e.success for e in events)
    finally:
        zb.close()


# ---- concurrent delete mid-stream: loud failure, not silent truncation ---


def test_get_stream_raises_if_deleted_mid_stream(images, db_connection_factory):
    """If the row is deleted between chunk fetches (no connection=
    holding a snapshot), the next chunk fetch must raise -- silently
    returning a short/truncated stream as if it were the complete image
    would be a much worse failure mode."""
    from zerobucket.exceptions import StorageError

    data = _jpeg_bytes((1000, 800))  # large enough for several small chunks
    image_id = images.put(data)

    stream = images.get_stream(image_id, chunk_size=50)
    first_chunk = next(stream)
    assert first_chunk

    # Delete via a separate, independent connection/commit so the
    # in-progress stream (using ZeroBucket's own pool, connection=None)
    # observes the row as gone on its next round trip.
    conn = db_connection_factory()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM zerobucket_images WHERE id = %s;", (image_id,))
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(StorageError, match="deleted while streaming"):
        list(stream)


# ---- write-side: bounded reads for file-like input over the cap ----------


class _CountingReader:
    """Wraps a bytes payload behind a file-like .read(), counting the
    total number of bytes actually requested/consumed -- used to prove
    put() stops reading a file-like input once it knows the upload is
    oversized, rather than draining it fully first."""

    def __init__(self, data: bytes) -> None:
        self._buf = io.BytesIO(data)
        self.bytes_read = 0

    def read(self, size: int = -1) -> bytes:
        piece = self._buf.read(size)
        self.bytes_read += len(piece)
        return piece


def test_oversized_file_like_input_stops_reading_early(_db_available):
    """A 5MB file-like upload against a 200-byte cap should be rejected
    having read only a small, bounded amount -- not the full 5MB."""
    from tests.conftest import TEST_DATABASE_URL

    huge_payload = b"\xff" * (5 * 1024 * 1024)
    reader = _CountingReader(huge_payload)

    small_images = ZeroBucket(database_url=TEST_DATABASE_URL, max_bytes=200)
    try:
        with pytest.raises(ImageTooLargeError) as exc_info:
            small_images.put(reader)
        # Stopped well short of the full 5MB payload -- bounded by
        # max_bytes, not by the input's true size.
        assert reader.bytes_read <= 200 + 65536
        assert reader.bytes_read < len(huge_payload)
        # The reported size is a lower bound (we stopped reading once we
        # knew it was too large), not a claim about the true total size.
        assert exc_info.value.size_bytes == reader.bytes_read
        assert exc_info.value.max_bytes == 200
    finally:
        small_images.close()


def test_file_like_input_exactly_at_cap_succeeds(_db_available, make_image_bytes):
    from tests.conftest import TEST_DATABASE_URL

    data = make_image_bytes(size=(20, 15))
    exact_images = ZeroBucket(database_url=TEST_DATABASE_URL, max_bytes=len(data))
    try:
        image_id = exact_images.put(io.BytesIO(data))
        assert exact_images.get(image_id).data == data
    finally:
        exact_images.close()


def test_file_like_input_one_byte_over_cap_rejected(_db_available, make_image_bytes):
    from tests.conftest import TEST_DATABASE_URL

    data = make_image_bytes(size=(20, 15))
    under_images = ZeroBucket(database_url=TEST_DATABASE_URL, max_bytes=len(data) - 1)
    try:
        with pytest.raises(ImageTooLargeError):
            under_images.put(io.BytesIO(data))
    finally:
        under_images.close()


def test_bytes_input_still_reports_exact_size_when_oversized(_db_available):
    """Unlike file-like input, `bytes` input is already fully in memory
    by the time put() sees it -- the reported size_bytes there stays the
    TRUE exact size, not a lower bound. Confirms the streaming change to
    file-like handling didn't change bytes/path input behavior."""
    from tests.conftest import TEST_DATABASE_URL

    data = b"\x00" * 5000
    small_images = ZeroBucket(database_url=TEST_DATABASE_URL, max_bytes=200)
    try:
        with pytest.raises(ImageTooLargeError) as exc_info:
            small_images.put(data)
        assert exc_info.value.size_bytes == 5000
    finally:
        small_images.close()
