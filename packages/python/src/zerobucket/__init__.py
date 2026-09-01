"""ZeroBucket: database-native image storage.

from zerobucket import ZeroBucket
images = ZeroBucket(database_url="postgresql://...")
image_id = images.put("avatar.jpg")
image = images.get(image_id)
"""

from .adapters.postgres import (
    DEFAULT_STREAM_CHUNK_SIZE,
    OperationEvent,
    migrate_classic_to_dedup,
)
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

__version__ = "0.11.0"

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
    "OperationEvent",
    "DEFAULT_STREAM_CHUNK_SIZE",
    "migrate_classic_to_dedup",
    "ZeroBucketError",
    "ContentValidationError",
    "ImageValidationError",
    "ImageTooLargeError",
    "UnsupportedFormatError",
    "CorruptedImageError",
    "ImageNotFoundError",
    "StorageError",
]
