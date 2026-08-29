"""zerobucket_integration: example app-level wiring for ZeroBucket.

Re-exports the public surface of this package so the rest of your app
can do:

    from zerobucket_integration import images, create_post_with_attachments

instead of reaching into each submodule individually. The submodules
themselves (client, db, documents, records, serving) are still directly
importable if you want something not re-exported here.
"""

from __future__ import annotations

from .client import images
from .db import DATABASE_URL, app_pool
from .documents import (
    DEFAULT_MAX_DOCUMENT_BYTES,
    Document,
    DocumentNotFoundError,
    DocumentValidationError,
    delete_document,
    get_document,
    init_documents_table,
    store_document,
)
from .records import (
    create_post_with_attachments,
    init_posts_table,
    upload_standalone_image,
)
from .serving import bp as serving_blueprint

__all__ = [
    # client.py
    "images",
    # db.py
    "app_pool",
    "DATABASE_URL",
    # documents.py
    "store_document",
    "get_document",
    "delete_document",
    "init_documents_table",
    "Document",
    "DocumentValidationError",
    "DocumentNotFoundError",
    "DEFAULT_MAX_DOCUMENT_BYTES",
    # records.py
    "create_post_with_attachments",
    "upload_standalone_image",
    "init_posts_table",
    # serving.py
    "serving_blueprint",
]
