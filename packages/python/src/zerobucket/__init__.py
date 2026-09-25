"""ZeroBucket: database-native image storage.

from zerobucket import ZeroBucket
images = ZeroBucket(database_url="postgresql://...")
image_id = images.put("avatar.jpg")
image = images.get(image_id)
"""

from .adapters.mysql import MySQLBackend
from .adapters.postgres import (
    DEFAULT_STREAM_CHUNK_SIZE,
    OperationEvent,
    migrate_classic_to_dedup,
)
from .adapters.postgres_async import AsyncPostgresBackend
from .adapters.sqlite import SQLiteBackend
from .adapters.sqlite_async import AsyncSQLiteBackend
from .async_client import AsyncZeroBucket
from .client import ZeroBucket
from .content_types import ContentValidator, ValidatedContent
from .exceptions import (
    AccessDeniedError,
    ContentValidationError,
    CorruptedImageError,
    ImageNotFoundError,
    ImageTooLargeError,
    ImageValidationError,
    StorageError,
    UnsupportedFormatError,
    ZeroBucketError,
)
from .object_storage import ObjectStorage
from .optimization import OptimizationResult
from .types import (
    BatchDeleteResult,
    BatchGetResult,
    BatchPutResult,
    Image,
    ImageMetadata,
)

__version__ = "0.21.1"

__all__ = [
    "ZeroBucket",
    "AsyncZeroBucket",
    "AsyncPostgresBackend",
    "SQLiteBackend",
    "AsyncSQLiteBackend",
    "MySQLBackend",
    "ObjectStorage",
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
    "AccessDeniedError",
]
