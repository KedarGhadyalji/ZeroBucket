"""Exception hierarchy for ZeroBucket.

All exceptions inherit from ZeroBucketError so callers can catch broadly
(`except ZeroBucketError`) or narrowly (`except ImageNotFoundError`).
"""

from __future__ import annotations


class ZeroBucketError(Exception):
    """Base class for all ZeroBucket exceptions."""


class ImageValidationError(ZeroBucketError):
    """Raised when an image fails validation (bad format, too large, corrupted, etc.)."""


class ImageTooLargeError(ImageValidationError):
    """Raised when an image exceeds the configured maximum size."""

    def __init__(self, size_bytes: int, max_bytes: int) -> None:
        self.size_bytes = size_bytes
        self.max_bytes = max_bytes
        super().__init__(
            f"Image is {size_bytes} bytes, which exceeds the maximum of {max_bytes} bytes"
        )


class UnsupportedFormatError(ImageValidationError):
    """Raised when an image's detected format is not in the allowed set."""

    def __init__(self, detected_format: str | None, allowed: frozenset[str]) -> None:
        self.detected_format = detected_format
        self.allowed = allowed
        super().__init__(
            f"Detected format {detected_format!r} is not supported. "
            f"Allowed formats: {sorted(allowed)}"
        )


class CorruptedImageError(ImageValidationError):
    """Raised when image bytes cannot be decoded despite having a recognizable header."""


class ImageNotFoundError(ZeroBucketError):
    """Raised when get() or metadata() is called with an image_id that doesn't exist."""

    def __init__(self, image_id: str) -> None:
        self.image_id = image_id
        super().__init__(f"No image found with id {image_id!r}")


class StorageError(ZeroBucketError):
    """Raised for underlying storage/database failures not covered above."""
