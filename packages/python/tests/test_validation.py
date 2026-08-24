"""Unit tests for image validation. No database required."""

from __future__ import annotations

import io

import pytest
from PIL import Image as PILImage

from zerobucket.exceptions import (
    CorruptedImageError,
    ImageTooLargeError,
    UnsupportedFormatError,
)
from zerobucket.validation import validate_image


def _jpeg(size=(32, 32)) -> bytes:
    img = PILImage.new("RGB", size, color=(10, 20, 30))
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


def test_valid_jpeg_passes():
    data = _jpeg((100, 50))
    result = validate_image(data, max_bytes=10_000_000)
    assert result.mime_type == "image/jpeg"
    assert result.width == 100
    assert result.height == 50
    assert result.size_bytes == len(data)


def test_valid_png_passes():
    img = PILImage.new("RGB", (10, 10))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    result = validate_image(buf.getvalue(), max_bytes=10_000_000)
    assert result.mime_type == "image/png"


def test_valid_webp_passes():
    img = PILImage.new("RGB", (10, 10))
    buf = io.BytesIO()
    img.save(buf, format="WEBP")
    result = validate_image(buf.getvalue(), max_bytes=10_000_000)
    assert result.mime_type == "image/webp"


def test_oversized_image_rejected():
    data = _jpeg((500, 500))
    with pytest.raises(ImageTooLargeError):
        validate_image(data, max_bytes=10)


def test_empty_bytes_rejected():
    with pytest.raises(CorruptedImageError):
        validate_image(b"", max_bytes=10_000_000)


def test_random_bytes_rejected():
    with pytest.raises(CorruptedImageError):
        validate_image(b"not an image, just some random bytes here" * 5, max_bytes=10_000_000)


def test_truncated_image_rejected():
    data = _jpeg((200, 200))
    truncated = data[: len(data) // 3]
    with pytest.raises(CorruptedImageError):
        validate_image(truncated, max_bytes=10_000_000)


def test_gif_rejected_as_unsupported_format():
    img = PILImage.new("RGB", (10, 10))
    buf = io.BytesIO()
    img.save(buf, format="GIF")
    with pytest.raises(UnsupportedFormatError):
        validate_image(buf.getvalue(), max_bytes=10_000_000)


def test_does_not_trust_fake_extension_only_content():
    """A PNG's magic bytes determine its type, regardless of what a caller might claim."""
    img = PILImage.new("RGB", (10, 10))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    data = buf.getvalue()
    # Even though nothing here claims "this is a .jpg", validate_image must
    # detect PNG from content, not trust any external hint.
    result = validate_image(data, max_bytes=10_000_000)
    assert result.mime_type == "image/png"


def test_decompression_bomb_guard():
    """A tiny compressed image that claims an enormous pixel count is rejected."""
    img = PILImage.new("RGB", (20000, 20000), color=(1, 1, 1))
    buf = io.BytesIO()
    img.save(buf, format="PNG", compress_level=9)
    data = buf.getvalue()
    with pytest.raises((ImageTooLargeError, CorruptedImageError)):
        validate_image(data, max_bytes=10_000_000, max_pixels=1_000_000)
