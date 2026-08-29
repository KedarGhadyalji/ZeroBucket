"""PDF/document storage: BYTEA in the same Postgres database, in a
table separate from zerobucket_images.

Why not just use ZeroBucket for this? ZeroBucket validates content via
Pillow (decode as an image, check width/height, guard against
decompression bombs in *pixel* terms) -- none of that applies to a PDF,
and forcing a PDF through an image-shaped validator would be wrong, not
just inconvenient. This module does the equivalent job for PDFs
specifically: content-sniffed validation (magic bytes, not filename/
Content-Type), a size ceiling, and a checksum -- same principles as
zerobucket's own validation.py, deliberately not the same code, because
the content types are genuinely different.

Why not S3? That reintroduces the exact "second service" cost ZeroBucket
exists to avoid. Storing PDFs as BYTEA in the same database keeps the
same backup story, the same transactional-atomicity option via
connection=, and no new infrastructure -- the same tradeoffs ZeroBucket
makes for images, applied here deliberately, not by accident.

Known limitation, stated plainly: like ZeroBucket's own BYTEA storage,
this is not designed for huge files or high volume -- full document
bytes are loaded into memory on every read/write. Fine for typical PDFs
(contracts, invoices, reports); reconsider for large scanned documents
or high-volume document workloads.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

DEFAULT_MAX_DOCUMENT_BYTES = 20 * 1024 * 1024  # 20MB -- PDFs tend to run
# larger than typical web
# images; adjust for your data


class DocumentValidationError(Exception):
    """Raised when uploaded bytes don't look like a valid PDF, or exceed
    the configured size limit."""


class DocumentNotFoundError(Exception):
    def __init__(self, document_id: str) -> None:
        self.document_id = document_id
        super().__init__(f"No document found with id {document_id!r}")


@dataclass(frozen=True, slots=True)
class Document:
    id: str
    data: bytes
    original_filename: str | None
    size_bytes: int
    checksum_sha256: str


_SCHEMA = """
CREATE TABLE IF NOT EXISTS app_documents (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    data                BYTEA NOT NULL,
    mime_type           TEXT NOT NULL DEFAULT 'application/pdf',
    original_filename   TEXT,
    size_bytes          INTEGER NOT NULL,
    checksum_sha256     CHAR(64) NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_app_documents_checksum ON app_documents (checksum_sha256);
CREATE INDEX IF NOT EXISTS idx_app_documents_created_at ON app_documents (created_at);
"""


def init_documents_table(pool) -> None:
    """Create app_documents if it doesn't exist. Call this once at app
    startup, same as ZeroBucket's own auto-migration on construction."""
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute(_SCHEMA)


def _looks_like_pdf(data: bytes) -> bool:
    """Content-sniffed check, not filename/extension-based -- same
    principle as zerobucket's own format detection. The PDF spec
    requires the file to start with '%PDF-' followed by a version
    number; this is the same check every PDF library and browser uses
    as the first sanity gate."""
    return data[:5] == b"%PDF-"


def _validate(data: bytes, max_bytes: int) -> None:
    if len(data) == 0:
        raise DocumentValidationError("Document data is empty")
    if len(data) > max_bytes:
        raise DocumentValidationError(
            f"Document is {len(data)} bytes, exceeds the maximum of {max_bytes} bytes"
        )
    if not _looks_like_pdf(data):
        raise DocumentValidationError(
            "This does not look like a valid PDF (missing '%PDF-' header). "
            "Content is checked by inspecting the actual bytes, not the "
            "filename or any client-supplied content type."
        )


def store_document(
    pool,
    data: bytes,
    *,
    filename: str | None = None,
    max_bytes: int = DEFAULT_MAX_DOCUMENT_BYTES,
    connection=None,
) -> str:
    """Validate and store a PDF. Returns its id.

    connection: same pattern as ZeroBucket's own connection= -- pass your
        own open psycopg connection to make this commit atomically with
        other writes in the same transaction (e.g. creating a parent
        record and its attached document together). Without it, this
        commits independently on its own connection from `pool`.
    """
    _validate(data, max_bytes)
    checksum = hashlib.sha256(data).hexdigest()

    sql = (
        "INSERT INTO app_documents (data, original_filename, size_bytes, checksum_sha256) "
        "VALUES (%s, %s, %s, %s) RETURNING id;"
    )
    params = (data, filename, len(data), checksum)

    if connection is not None:
        with connection.cursor() as cur:
            cur.execute(sql, params)
            return str(cur.fetchone()[0])
    else:
        with pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            return str(cur.fetchone()[0])


def get_document(pool, document_id: str, *, connection=None) -> Document:
    sql = (
        "SELECT id, data, original_filename, size_bytes, checksum_sha256 "
        "FROM app_documents WHERE id = %s;"
    )
    if connection is not None:
        with connection.cursor() as cur:
            cur.execute(sql, (document_id,))
            row = cur.fetchone()
    else:
        with pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, (document_id,))
            row = cur.fetchone()

    if row is None:
        raise DocumentNotFoundError(document_id)
    return Document(
        id=str(row[0]),
        data=bytes(row[1]),
        original_filename=row[2],
        size_bytes=row[3],
        checksum_sha256=row[4],
    )


def delete_document(pool, document_id: str, *, connection=None) -> bool:
    sql = "DELETE FROM app_documents WHERE id = %s;"
    if connection is not None:
        with connection.cursor() as cur:
            cur.execute(sql, (document_id,))
            return cur.rowcount > 0
    else:
        with pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, (document_id,))
            return cur.rowcount > 0
