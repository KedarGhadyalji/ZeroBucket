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
from .optimization import OptimizationResult
from .types import (
    BatchDeleteResult,
    BatchGetResult,
    BatchPutResult,
    Image,
    ImageMetadata,
)

__version__ = "0.6.0"

__all__ = [
    "ZeroBucket",
    "Image",
    "ImageMetadata",
    "OptimizationResult",
    "BatchPutResult",
    "BatchGetResult",
    "BatchDeleteResult",
    "ZeroBucketError",
    "ImageValidationError",
    "ImageTooLargeError",
    "UnsupportedFormatError",
    "CorruptedImageError",
    "ImageNotFoundError",
    "StorageError",
]
