"""PostgreSQL storage adapter.

Stores image bytes directly in a BYTEA column. All queries are
parameterized; nothing is ever built via string concatenation.
"""

from __future__ import annotations

from psycopg_pool import ConnectionPool

from ..exceptions import StorageError
from .base import StorageBackend, StoredRecord, StoredRecordMetadata

_SCHEMA = """
CREATE TABLE IF NOT EXISTS zerobucket_images (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    data                BYTEA NOT NULL,
    mime_type           TEXT NOT NULL,
    original_filename   TEXT,
    size_bytes          INTEGER NOT NULL,
    width               INTEGER,
    height              INTEGER,
    checksum_sha256     CHAR(64) NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_zerobucket_checksum ON zerobucket_images (checksum_sha256);
CREATE INDEX IF NOT EXISTS idx_zerobucket_created_at ON zerobucket_images (created_at);
"""

_INSERT = """
INSERT INTO zerobucket_images
    (data, mime_type, original_filename, size_bytes, width, height, checksum_sha256)
VALUES (%s, %s, %s, %s, %s, %s, %s)
RETURNING id;
"""

_SELECT_FULL = """
SELECT id, data, mime_type, original_filename, size_bytes, width, height, checksum_sha256
FROM zerobucket_images
WHERE id = %s;
"""

_SELECT_METADATA = """
SELECT id, mime_type, original_filename, size_bytes, width, height, checksum_sha256
FROM zerobucket_images
WHERE id = %s;
"""

_DELETE = "DELETE FROM zerobucket_images WHERE id = %s;"

_EXISTS = "SELECT 1 FROM zerobucket_images WHERE id = %s;"


class PostgresBackend(StorageBackend):
    """Storage backend for PostgreSQL using BYTEA columns.

    Requires the pgcrypto extension (for gen_random_uuid()) on Postgres < 13.
    Postgres 13+ has gen_random_uuid() built in.
    """

    def __init__(self, database_url: str, *, auto_migrate: bool = True) -> None:
        try:
            self._pool = ConnectionPool(
                database_url, min_size=1, max_size=5, open=True, timeout=10
            )
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Could not connect to PostgreSQL: {exc}") from exc

        if auto_migrate:
            try:
                self.migrate()
            except Exception:
                self._pool.close()
                raise

    def migrate(self) -> None:
        """Create the zerobucket_images table and indexes if they don't exist."""
        try:
            with self._pool.connection() as conn, conn.cursor() as cur:
                cur.execute(_SCHEMA)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Migration failed: {exc}") from exc

    def put(
        self,
        *,
        data: bytes,
        mime_type: str,
        original_filename: str | None,
        size_bytes: int,
        width: int | None,
        height: int | None,
        checksum_sha256: str,
    ) -> str:
        try:
            with self._pool.connection() as conn, conn.cursor() as cur:
                params = (
                    data,
                    mime_type,
                    original_filename,
                    size_bytes,
                    width,
                    height,
                    checksum_sha256,
                )
                cur.execute(_INSERT, params)
                row = cur.fetchone()
                return str(row[0])
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to store image: {exc}") from exc

    def get(self, image_id: str) -> StoredRecord | None:
        try:
            with self._pool.connection() as conn, conn.cursor() as cur:
                cur.execute(_SELECT_FULL, (image_id,))
                row = cur.fetchone()
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to retrieve image: {exc}") from exc
        if row is None:
            return None
        return StoredRecord(
            id=str(row[0]),
            data=bytes(row[1]),
            mime_type=row[2],
            original_filename=row[3],
            size_bytes=row[4],
            width=row[5],
            height=row[6],
            checksum_sha256=row[7],
        )

    def get_metadata(self, image_id: str) -> StoredRecordMetadata | None:
        try:
            with self._pool.connection() as conn, conn.cursor() as cur:
                cur.execute(_SELECT_METADATA, (image_id,))
                row = cur.fetchone()
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to retrieve image metadata: {exc}") from exc
        if row is None:
            return None
        return StoredRecordMetadata(
            id=str(row[0]),
            mime_type=row[1],
            original_filename=row[2],
            size_bytes=row[3],
            width=row[4],
            height=row[5],
            checksum_sha256=row[6],
        )

    def delete(self, image_id: str) -> bool:
        try:
            with self._pool.connection() as conn, conn.cursor() as cur:
                cur.execute(_DELETE, (image_id,))
                return cur.rowcount > 0
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image: {exc}") from exc

    def exists(self, image_id: str) -> bool:
        try:
            with self._pool.connection() as conn, conn.cursor() as cur:
                cur.execute(_EXISTS, (image_id,))
                return cur.fetchone() is not None
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to check image existence: {exc}") from exc

    def close(self) -> None:
        self._pool.close()
