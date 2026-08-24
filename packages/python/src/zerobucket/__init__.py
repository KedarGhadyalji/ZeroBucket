"""ZeroBucket: database-native image storage.

    from zerobucket import ZeroBucket
    images = ZeroBucket(database_url="postgresql://...")
    image_id = images.put("avatar.jpg")
    image = images.get(image_id)
"""

from .client import ZeroBucket
from .exceptions import (
    CorruptedImageError,
    ImageNotFoundError,
    ImageTooLargeError,
    ImageValidationError,
    StorageError,
    UnsupportedFormatError,
    ZeroBucketError,
)
from .types import Image, ImageMetadata

__version__ = "0.1.0"

__all__ = [
    "ZeroBucket",
    "Image",
    "ImageMetadata",
    "ZeroBucketError",
    "ImageValidationError",
    "ImageTooLargeError",
    "UnsupportedFormatError",
    "CorruptedImageError",
    "ImageNotFoundError",
    "StorageError",
]
