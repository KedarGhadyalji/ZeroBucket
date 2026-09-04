"""Tests for object-storage tiering (Stage 5's second item):
tier_to_object_storage(), and the transparent get()/get_many()/
get_stream()/stream_to()/delete() behavior for tiered rows.

Uses moto's in-process S3 mock (see conftest.py's s3_bucket/object_store
fixtures for why that approach specifically, over a standalone
moto_server subprocess) -- a real boto3 client talking to a real,
maintained AWS-API emulator, not hand-mocked HTTP calls.

Covers: the round trip (tier -> get/get_many/get_stream/stream_to still
work identically), idempotent re-tiering, not-found semantics, the
not-configured error paths (both "no object_storage on this instance"
and "dedup=True + object_storage= rejected at construction"), delete()
cleaning up the object-storage copy, transactional safety (a failed
upload leaves the row untouched), and that a plain `images` fixture
(no object_storage configured) gets a clear error if it ever encounters
an already-tiered row.
"""

from __future__ import annotations

import io

import pytest
from PIL import Image as PILImage

from zerobucket import ImageNotFoundError, ZeroBucket
from zerobucket.exceptions import StorageError

from .conftest import TEST_DATABASE_URL


def _jpeg_bytes(size=(300, 200), color=(40, 80, 120)) -> bytes:
    img = PILImage.new("RGB", size, color=color)
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


# ---- construction / configuration ------------------------------------


def test_object_storage_plus_dedup_rejected_at_construction(object_store):
    with pytest.raises(ValueError, match="dedup"):
        ZeroBucket(
            database_url=TEST_DATABASE_URL, object_storage=object_store, dedup=True
        )


# ---- tier_to_object_storage(): basic behavior -----------------------------


def test_tier_moves_bytes_out_of_postgres(tiered_images, object_store):
    data = _jpeg_bytes()
    image_id = tiered_images.put(data)

    result = tiered_images.tier_to_object_storage(image_id)
    assert result is True

    # The bytes are genuinely gone from the `data` column now -- verified
    # directly against the row, not just inferred from get() still
    # working (get() working could mask a bug where data silently never
    # actually left Postgres).
    with tiered_images._backend._pool.connection() as conn, conn.cursor() as cur:  # noqa: SLF001
        cur.execute(
            "SELECT data, storage_backend, object_storage_key FROM "
            "zerobucket_images WHERE id = %s;",
            (image_id,),
        )
        row = cur.fetchone()
    assert row[0] is None
    assert row[1] == "object_storage"
    assert row[2] == str(image_id)

    # And the bytes genuinely are in the bucket.
    assert object_store.exists(str(image_id)) is True
    assert object_store.download(str(image_id)) == data


def test_tier_is_idempotent_no_op_on_second_call(tiered_images):
    image_id = tiered_images.put(_jpeg_bytes())
    assert tiered_images.tier_to_object_storage(image_id) is True
    assert tiered_images.tier_to_object_storage(image_id) is False


def test_tier_not_found_raises(tiered_images):
    with pytest.raises(ImageNotFoundError):
        tiered_images.tier_to_object_storage("00000000-0000-0000-0000-000000000000")


def test_tier_without_object_storage_configured_raises(images):
    """The plain `images` fixture has no object_storage= at all."""
    image_id = images.put(_jpeg_bytes())
    with pytest.raises(StorageError, match="object_storage"):
        images.tier_to_object_storage(image_id)


# ---- transparent reads after tiering --------------------------------------


def test_get_works_identically_after_tiering(tiered_images):
    data = _jpeg_bytes()
    image_id = tiered_images.put(data)
    tiered_images.tier_to_object_storage(image_id)

    image = tiered_images.get(image_id)
    assert image.data == data
    assert image.width == 300
    assert image.height == 200


def test_metadata_works_identically_after_tiering(tiered_images):
    data = _jpeg_bytes()
    image_id = tiered_images.put(data)
    tiered_images.tier_to_object_storage(image_id)

    meta = tiered_images.metadata(image_id)
    assert meta.size_bytes == len(data)


def test_get_many_mixed_tiered_and_untiered(tiered_images):
    data_a = _jpeg_bytes((100, 100))
    data_b = _jpeg_bytes((200, 200))
    id_a = tiered_images.put(data_a)
    id_b = tiered_images.put(data_b)
    tiered_images.tier_to_object_storage(id_a)  # only A is tiered

    results = {r.image_id: r for r in tiered_images.get_many([id_a, id_b])}
    assert results[id_a].image.data == data_a
    assert results[id_b].image.data == data_b


def test_get_stream_delegates_to_object_storage_after_tiering(tiered_images):
    data = _jpeg_bytes((800, 600))
    image_id = tiered_images.put(data)
    tiered_images.tier_to_object_storage(image_id)

    chunks = list(tiered_images.get_stream(image_id, chunk_size=500))
    assert b"".join(chunks) == data
    assert len(chunks) > 1


def test_stream_to_works_identically_after_tiering(tiered_images):
    data = _jpeg_bytes()
    image_id = tiered_images.put(data)
    tiered_images.tier_to_object_storage(image_id)

    dest = io.BytesIO()
    total = tiered_images.stream_to(image_id, dest, chunk_size=333)
    assert total == len(data)
    assert dest.getvalue() == data


def test_exists_true_after_tiering(tiered_images):
    image_id = tiered_images.put(_jpeg_bytes())
    tiered_images.tier_to_object_storage(image_id)
    assert tiered_images.exists(image_id) is True


# ---- delete() cleans up the object-storage copy ---------------------------


def test_delete_removes_both_postgres_row_and_object_storage_copy(
    tiered_images, object_store
):
    image_id = tiered_images.put(_jpeg_bytes())
    tiered_images.tier_to_object_storage(image_id)
    assert object_store.exists(str(image_id)) is True

    deleted = tiered_images.delete(image_id)
    assert deleted is True
    assert tiered_images.exists(image_id) is False
    assert object_store.exists(str(image_id)) is False


def test_delete_many_cleans_up_object_storage_for_tiered_items(
    tiered_images, object_store
):
    id_a = tiered_images.put(_jpeg_bytes((50, 50)))
    id_b = tiered_images.put(_jpeg_bytes((60, 60)))
    tiered_images.tier_to_object_storage(id_a)
    tiered_images.tier_to_object_storage(id_b)

    results = tiered_images.delete_many([id_a, id_b])
    assert all(r.deleted for r in results)
    assert object_store.exists(str(id_a)) is False
    assert object_store.exists(str(id_b)) is False


# ---- transactional safety: failed upload leaves the row untouched --------


def test_failed_upload_leaves_row_completely_untouched(tiered_images, object_store):
    """If the object-storage upload fails partway through
    tier_to_object_storage(), the whole DB transaction must roll back --
    the row should be exactly as if tiering was never attempted, not
    half-flipped."""
    data = _jpeg_bytes()
    image_id = tiered_images.put(data)

    def broken_upload(*args, **kwargs):
        raise RuntimeError("simulated network failure")

    original_upload = object_store.upload
    object_store.upload = broken_upload
    try:
        with pytest.raises(StorageError):
            tiered_images.tier_to_object_storage(image_id)
    finally:
        object_store.upload = original_upload

    # Row must be untouched: still fully in Postgres, get() still works
    # the normal way, nothing was ever written to object storage.
    with tiered_images._backend._pool.connection() as conn, conn.cursor() as cur:  # noqa: SLF001
        cur.execute(
            "SELECT data, storage_backend FROM zerobucket_images WHERE id = %s;",
            (image_id,),
        )
        row = cur.fetchone()
    assert bytes(row[0]) == data
    assert row[1] == "postgres"
    assert tiered_images.get(image_id).data == data


# ---- a plain (non-tiering-configured) instance hitting a tiered row ------


def test_plain_instance_get_on_tiered_row_raises_clear_error(
    tiered_images, images, object_store
):
    """A DIFFERENT ZeroBucket instance, pointed at the same database but
    constructed WITHOUT object_storage=, must fail loudly and clearly if
    it ever encounters a row that some other, object_storage-configured
    instance already tiered -- not silently return None/empty bytes."""
    image_id = tiered_images.put(_jpeg_bytes())
    tiered_images.tier_to_object_storage(image_id)

    with pytest.raises(StorageError, match="object_storage"):
        images.get(image_id)


def test_plain_instance_get_stream_on_tiered_row_raises_clear_error(
    tiered_images, images
):
    image_id = tiered_images.put(_jpeg_bytes())
    tiered_images.tier_to_object_storage(image_id)

    with pytest.raises(StorageError, match="object_storage"):
        images.get_stream(image_id)
