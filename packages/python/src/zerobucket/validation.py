"""Image validation.

Deliberately does NOT trust file extensions or client-supplied MIME types.
The actual bytes are decoded with Pillow and the *detected* format is what
gets validated and stored. This is the only source of truth.
"""

from __future__ import annotations

import io
from dataclasses import dataclass

from PIL import Image as PILImage

from .exceptions import (
    CorruptedImageError,
    ImageTooLargeError,
    UnsupportedFormatError,
)

# Pillow format name -> canonical MIME type. Deliberately small allowlist;
# extend this (and SUPPORTED_FORMATS) to add formats, never bypass it.
_FORMAT_TO_MIME = {
    "JPEG": "image/jpeg",
    "PNG": "image/png",
    "WEBP": "image/webp",
}

SUPPORTED_FORMATS = frozenset(_FORMAT_TO_MIME)

# Guard against decompression bombs: reject images that would decode to
# more than this many pixels, regardless of how small the compressed
# bytes are. ~89 megapixels ~= a 12000x7400 image.
DEFAULT_MAX_PIXELS = 89_000_000


@dataclass(frozen=True, slots=True)
class ValidatedImage:
    """Result of successful validation: everything derived from the bytes."""

    mime_type: str
    width: int
    height: int
    size_bytes: int


def validate_image(
    data: bytes,
    *,
    max_bytes: int,
    max_pixels: int = DEFAULT_MAX_PIXELS,
    allowed_formats: frozenset[str] = SUPPORTED_FORMATS,
) -> ValidatedImage:
    """Validate raw image bytes and return derived metadata.

    Raises ImageTooLargeError, UnsupportedFormatError, or CorruptedImageError.
    Never raises for reasons unrelated to the image itself.
    """
    size_bytes = len(data)
    if size_bytes > max_bytes:
        raise ImageTooLargeError(size_bytes, max_bytes)
    if size_bytes == 0:
        raise CorruptedImageError("Image data is empty")

    # Pillow's own decompression-bomb guard, in pixels (not compressed bytes).
    # We set it per-call rather than mutating the module-global so concurrent
    # validate_image() calls with different limits don't race each other.
    original_max_pixels = PILImage.MAX_IMAGE_PIXELS
    try:
        PILImage.MAX_IMAGE_PIXELS = max_pixels
        try:
            with PILImage.open(io.BytesIO(data)) as img:
                detected_format = img.format
                if detected_format not in allowed_formats:
                    raise UnsupportedFormatError(detected_format, allowed_formats)
                width, height = img.size
                # .verify() only checks structural integrity; it does not
                # decode pixel data (and Pillow requires reopening after
                # calling it). Force a full pixel decode below to catch
                # truncated/corrupted image bodies, not just bad headers.
        except UnsupportedFormatError:
            raise
        except PILImage.DecompressionBombError as exc:
            raise ImageTooLargeError(size_bytes, max_bytes) from exc
        except Exception as exc:  # noqa: BLE001 - Pillow raises many exception types
            raise CorruptedImageError(f"Could not decode image: {exc}") from exc

        # Force full pixel decode to catch truncated image bodies that pass
        # header parsing but fail partway through the data.
        try:
            with PILImage.open(io.BytesIO(data)) as img:
                img.load()
        except Exception as exc:  # noqa: BLE001
            raise CorruptedImageError(f"Image data is truncated or corrupted: {exc}") from exc
    finally:
        PILImage.MAX_IMAGE_PIXELS = original_max_pixels

    return ValidatedImage(
        mime_type=_FORMAT_TO_MIME[detected_format],
        width=width,
        height=height,
        size_bytes=size_bytes,
    )
