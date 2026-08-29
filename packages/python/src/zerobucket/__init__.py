"""ZeroBucket: database-native image storage.

from zerobucket import ZeroBucket
images = ZeroBucket(database_url="postgresql://...")
image_id = images.put("avatar.jpg")
image = images.get(image_id)
"""

from .client import ZeroBucket
from .content_types import ContentValidator, ValidatedContent
from .exceptions import (
    ContentValidationError,
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

__version__ = "0.8.0"

__all__ = [
    "ZeroBucket",
    "Image",
    "ImageMetadata",
    "OptimizationResult",
    "BatchPutResult",
    "BatchGetResult",
    "BatchDeleteResult",
    "ContentValidator",
    "ValidatedContent",
    "ZeroBucketError",
    "ContentValidationError",
    "ImageValidationError",
    "ImageTooLargeError",
    "UnsupportedFormatError",
    "CorruptedImageError",
    "ImageNotFoundError",
    "StorageError",
]
