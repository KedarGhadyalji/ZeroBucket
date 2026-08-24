"""Integration tests against a real PostgreSQL database.

Requires ZEROBUCKET_TEST_DATABASE_URL (see conftest.py). Tests are skipped
automatically if no database is reachable.
"""

from __future__ import annotations

import io

import pytest

from zerobucket import ZeroBucket
from zerobucket.exceptions import (
    ImageNotFoundError,
    ImageTooLargeError,
    UnsupportedFormatError,
)


def test_put_and_get_round_trip(images, jpeg_bytes):
    image_id = images.put(jpeg_bytes, filename="photo.jpg")
    result = images.get(image_id)

    assert result.data == jpeg_bytes
    assert result.mime_type == "image/jpeg"
    assert result.filename == "photo.jpg"
    assert result.width == 64
    assert result.height == 48
    assert result.size_bytes == len(jpeg_bytes)
    assert len(result.checksum_sha256) == 64


def test_put_from_file_path(images, tmp_path, jpeg_bytes):
    path = tmp_path / "avatar.jpg"
    path.write_bytes(jpeg_bytes)

    image_id = images.put(str(path))
    result = images.get(image_id)

    assert result.data == jpeg_bytes
    assert result.filename == "avatar.jpg"


def test_put_from_pathlib_path(images, tmp_path, jpeg_bytes):
    path = tmp_path / "avatar2.jpg"
    path.write_bytes(jpeg_bytes)

    image_id = images.put(path)
    result = images.get(image_id)

    assert result.data == jpeg_bytes


def test_put_from_file_like_object(images, jpeg_bytes):
    file_obj = io.BytesIO(jpeg_bytes)
    file_obj.name = "upload.jpg"

    image_id = images.put(file_obj)
    result = images.get(image_id)

    assert result.data == jpeg_bytes
    assert result.filename == "upload.jpg"


def test_explicit_filename_overrides_inferred_one(images, tmp_path, jpeg_bytes):
    path = tmp_path / "original_name.jpg"
    path.write_bytes(jpeg_bytes)

    image_id = images.put(str(path), filename="renamed.jpg")
    result = images.get(image_id)

    assert result.filename == "renamed.jpg"


def test_different_formats_all_supported(images, jpeg_bytes, png_bytes, webp_bytes):
    jpeg_id = images.put(jpeg_bytes)
    png_id = images.put(png_bytes)
    webp_id = images.put(webp_bytes)

    assert images.get(jpeg_id).mime_type == "image/jpeg"
    assert images.get(png_id).mime_type == "image/png"
    assert images.get(webp_id).mime_type == "image/webp"


def test_get_missing_image_raises(images):
    fake_id = "00000000-0000-0000-0000-000000000000"
    with pytest.raises(ImageNotFoundError):
        images.get(fake_id)


def test_metadata_missing_image_raises(images):
    fake_id = "00000000-0000-0000-0000-000000000000"
    with pytest.raises(ImageNotFoundError):
        images.metadata(fake_id)


def test_metadata_matches_get_but_excludes_data(images, jpeg_bytes):
    image_id = images.put(jpeg_bytes, filename="a.jpg")
    full = images.get(image_id)
    meta = images.metadata(image_id)

    assert meta.image_id == image_id
    assert meta.mime_type == full.mime_type
    assert meta.filename == full.filename
    assert meta.size_bytes == full.size_bytes
    assert meta.width == full.width
    assert meta.height == full.height
    assert meta.checksum_sha256 == full.checksum_sha256
    assert not hasattr(meta, "data")


def test_exists_true_for_stored_image(images, jpeg_bytes):
    image_id = images.put(jpeg_bytes)
    assert images.exists(image_id) is True


def test_exists_false_for_missing_image(images):
    assert images.exists("00000000-0000-0000-0000-000000000000") is False


def test_delete_removes_image(images, jpeg_bytes):
    image_id = images.put(jpeg_bytes)
    assert images.delete(image_id) is True
    assert images.exists(image_id) is False
    with pytest.raises(ImageNotFoundError):
        images.get(image_id)


def test_delete_missing_image_returns_false(images):
    assert images.delete("00000000-0000-0000-0000-000000000000") is False


def test_uploading_same_image_twice_creates_two_records(images, jpeg_bytes):
    """Dedup is explicitly deferred (see architecture notes) -- verify current,
    documented behavior: duplicate uploads create separate rows with matching
    checksums, rather than silently merging or erroring."""
    id_a = images.put(jpeg_bytes)
    id_b = images.put(jpeg_bytes)

    assert id_a != id_b
    assert images.get(id_a).checksum_sha256 == images.get(id_b).checksum_sha256


def test_size_limit_enforced(_db_available, make_image_bytes):
    from tests.conftest import TEST_DATABASE_URL

    small_images = ZeroBucket(database_url=TEST_DATABASE_URL, max_bytes=200)
    try:
        data = make_image_bytes(size=(200, 200))
        with pytest.raises(ImageTooLargeError):
            small_images.put(data)
    finally:
        small_images.close()


def test_unsupported_format_rejected(images):
    with pytest.raises(UnsupportedFormatError):
        # Minimal valid GIF header + trailer.
        gif = bytes.fromhex(
            "47494638396101000100800000000000ffffff21f90401000000002c00000000010001000002024401003b"
        )
        images.put(gif, filename="test.gif")


def test_concurrent_puts_all_succeed(images, make_image_bytes):
    """Basic concurrency smoke test: N images uploaded via threads all round-trip correctly."""
    import concurrent.futures

    payloads = [make_image_bytes(color=(i % 255, 0, 0)) for i in range(10)]

    def upload(data: bytes) -> tuple[str, bytes]:
        return images.put(data), data

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(upload, payloads))

    ids = [r[0] for r in results]
    assert len(set(ids)) == len(ids)  # all ids unique
    for image_id, original_data in results:
        assert images.get(image_id).data == original_data
