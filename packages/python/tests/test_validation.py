"""Unit tests for image validation. No database required."""

from __future__ import annotations

import io

import pytest
from PIL import Image as PILImage

from zerobucket.exceptions import (
    CorruptedImageError,
    ImageTooLargeError,
    ImageValidationError,
    UnsupportedFormatError,
)
from zerobucket.validation import (
    HEIF_SUPPORT_INSTALLED,
    _looks_like_heic,
    validate_image,
)


def _jpeg(size=(32, 32)) -> bytes:
    img = PILImage.new("RGB", size, color=(10, 20, 30))
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


def _heic(size=(32, 32)) -> bytes:
    img = PILImage.new("RGB", size, color=(10, 20, 30))
    buf = io.BytesIO()
    img.save(buf, format="HEIF", quality=90)
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
        validate_image(
            b"not an image, just some random bytes here" * 5, max_bytes=10_000_000
        )


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


@pytest.mark.skipif(not HEIF_SUPPORT_INSTALLED, reason="pillow-heif not installed")
def test_valid_heic_passes():
    data = _heic((100, 50))
    result = validate_image(data, max_bytes=10_000_000)
    assert result.mime_type == "image/heic"
    assert result.width == 100
    assert result.height == 50


def test_looks_like_heic_detects_real_heic_magic_bytes():
    """Sanity check for the sniffer itself, independent of whether
    pillow-heif is installed -- this only inspects raw bytes."""
    # A real HEIC ftyp box: box size (4 bytes) + 'ftyp' + 'heic' brand.
    heic_like = b"\x00\x00\x00\x1cftypheic\x00\x00\x00\x00" + b"\x00" * 20
    assert _looks_like_heic(heic_like) is True


def test_looks_like_heic_rejects_non_heic_bytes():
    assert _looks_like_heic(b"not an image at all, just text" * 3) is False
    assert _looks_like_heic(_jpeg()) is False
    assert _looks_like_heic(b"") is False
    assert _looks_like_heic(b"short") is False


def test_heic_without_optional_dependency_gives_actionable_error(monkeypatch):
    """If pillow-heif isn't installed, a real HEIC upload should get a
    clear "install this extra" message -- not a generic "corrupted image"
    error that looks like the file itself is broken.

    Simulated via monkeypatch rather than an actual separate environment,
    since pillow-heif IS installed in the test/dev environment (it's in
    the dev extra) -- this only forces the code path, it doesn't prove
    the real uninstalled environment behaves identically. That was
    verified manually in a clean venv during development; see the PR/
    commit notes for that verification.
    """
    import zerobucket.validation as validation_module

    monkeypatch.setattr(validation_module, "HEIF_SUPPORT_INSTALLED", False)

    heic_bytes = b"\x00\x00\x00\x1cftypheic\x00\x00\x00\x00" + b"\x00" * 500
    with pytest.raises(ImageValidationError, match=r"pip install zerobucket\[heic\]"):
        validate_image(heic_bytes, max_bytes=10_000_000)
