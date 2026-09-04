"""S3-compatible object storage support for tiering images out of
Postgres -- the storage-target half of Stage 5's "object-storage
tiering" roadmap item.

Scope decision, made explicitly rather than by default: S3-compatible
only (via boto3), covering AWS S3 itself plus anything speaking the
same API (Cloudflare R2, MinIO, Backblaze B2, DigitalOcean Spaces,
etc.) through one client, rather than either (a) a fully generic
pluggable backend interface with S3 as one implementation, or (b) also
supporting plain local-filesystem tiering. Revisit if a second, non-S3-
API target is actually needed -- there's no principled reason this
couldn't grow a pluggable interface later, it just isn't one yet, since
building that abstraction before a second real implementation exists
would be guessing at a shape nothing has tested.

`boto3` is an OPTIONAL dependency (`pip install zerobucket[s3]`), not a
hard requirement -- most ZeroBucket users never tier anything and
shouldn't need an AWS SDK installed. The import is deferred into
ObjectStorage.__init__ specifically so that importing `zerobucket`
itself never requires boto3 to be installed at all.
"""

from __future__ import annotations

from collections.abc import Iterator

from .exceptions import StorageError

# 1 MiB -- same default as DEFAULT_STREAM_CHUNK_SIZE in adapters/postgres.py,
# for the same reasoning (Python-side peak-memory/round-trip-count tradeoff).
DEFAULT_STREAM_CHUNK_SIZE = 1024 * 1024


class ObjectStorage:
    """Thin wrapper around a boto3 S3 client, scoped to exactly what
    tiering needs: upload, download (whole or ranged/streamed), delete,
    exists. Not a general-purpose S3 client wrapper -- if you need more
    than this, use boto3 directly.

    Construction never touches the network -- boto3.client() itself
    doesn't connect eagerly, it just configures a client. The bucket is
    NOT created for you (unlike Postgres's auto_migrate=True schema
    creation) -- deliberately: bucket creation involves choices (region,
    versioning, encryption, lifecycle policies, public-access blocking)
    that are yours to make deliberately for anything touching real
    object storage and real money, not something a library should do
    silently on your behalf the way "CREATE TABLE IF NOT EXISTS" is safe
    to do for you. Create the bucket yourself first.
    """

    def __init__(
        self,
        bucket: str,
        *,
        endpoint_url: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        region_name: str | None = None,
    ) -> None:
        """
        bucket: the S3 (or S3-compatible) bucket to store tiered image
            bytes in. Must already exist -- see class docstring.
        endpoint_url: set this for anything that isn't AWS S3 itself --
            e.g. "https://<account>.r2.cloudflarestorage.com" for
            Cloudflare R2, "http://localhost:9000" for a local MinIO,
            etc. Leave as None for real AWS S3.
        aws_access_key_id / aws_secret_access_key: explicit credentials.
            Leave both as None to fall back to boto3's own standard
            credential resolution (environment variables, ~/.aws/credentials,
            an IAM role, etc.) -- the same chain any other boto3-based
            tool on your machine already uses. Explicit here is for
            convenience/testing (e.g. pointing at a local MinIO/moto
            server with throwaway credentials), not the recommended way
            to hand real AWS credentials to a long-lived process.
        region_name: forwarded to boto3 as-is. Required by some
            S3-compatible services even when endpoint_url makes the
            actual region concept meaningless to them (e.g. MinIO) --
            boto3 itself requires SOME value be present in that case.
        """
        try:
            import boto3
        except ImportError as exc:
            raise ImportError(
                "Object-storage tiering requires boto3. Install it with "
                "`pip install zerobucket[s3]` (or `pip install boto3` "
                "directly)."
            ) from exc

        self._bucket = bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

    @property
    def bucket(self) -> str:
        return self._bucket

    def upload(self, key: str, data: bytes, *, mime_type: str) -> None:
        """Upload `data` to `key` in this bucket. Overwrites silently if
        `key` already exists (S3's own PutObject semantics -- no
        separate "create vs overwrite" concept at this layer)."""
        try:
            self._client.put_object(
                Bucket=self._bucket, Key=key, Body=data, ContentType=mime_type
            )
        except Exception as exc:  # noqa: BLE001
            raise StorageError(
                f"Failed to upload {key!r} to object storage: {exc}"
            ) from exc

    def download(self, key: str) -> bytes:
        """Download and return the full object at `key`."""
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
            return response["Body"].read()
        except Exception as exc:  # noqa: BLE001
            raise StorageError(
                f"Failed to download {key!r} from object storage: {exc}"
            ) from exc

    def download_stream(
        self, key: str, *, chunk_size: int = DEFAULT_STREAM_CHUNK_SIZE
    ) -> Iterator[bytes]:
        """Stream the object at `key` in chunks, using S3's own byte-Range
        support -- a genuine capability upgrade over Postgres-backed
        get_stream(), worth being explicit about rather than leaving
        implied: real HTTP Range requests mean this actually avoids
        transferring bytes you don't ask for, whereas Postgres's
        substring()-based streaming (see adapters/postgres.py) still
        transfers the full value every time, just paced out in pieces.
        This method still reads the whole object start-to-finish
        (tiering's get_stream() always wants the whole image), so this
        particular call doesn't exploit partial-range fetching -- it's
        mentioned because the underlying capability existing at all is a
        real, stated difference from the Postgres-backed path, and
        matters if range-request support (a separate, not-yet-built
        roadmap item) is ever added on top of tiered images specifically.
        """
        try:
            head = self._client.head_object(Bucket=self._bucket, Key=key)
            total_size = head["ContentLength"]
        except Exception as exc:  # noqa: BLE001
            raise StorageError(
                f"Failed to read {key!r} from object storage: {exc}"
            ) from exc

        def generator() -> Iterator[bytes]:
            offset = 0
            while offset < total_size:
                end = min(offset + chunk_size, total_size) - 1
                try:
                    response = self._client.get_object(
                        Bucket=self._bucket,
                        Key=key,
                        Range=f"bytes={offset}-{end}",
                    )
                    chunk = response["Body"].read()
                except Exception as exc:  # noqa: BLE001
                    raise StorageError(
                        f"Failed to stream {key!r} from object storage: {exc}"
                    ) from exc
                if not chunk:
                    raise StorageError(
                        f"Object {key!r} was deleted or emptied while "
                        f"streaming (delivered {offset} of {total_size} bytes)."
                    )
                yield chunk
                offset += len(chunk)

        return generator()

    def delete(self, key: str) -> None:
        """Delete the object at `key`. Like S3's own DeleteObject, this
        does NOT error if `key` doesn't exist -- deleting something
        that's already gone is treated as success, same idempotent
        philosophy as Postgres DELETE in this codebase (see delete()'s
        docstring on PostgresBackend)."""
        try:
            self._client.delete_object(Bucket=self._bucket, Key=key)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(
                f"Failed to delete {key!r} from object storage: {exc}"
            ) from exc

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
            return True
        except Exception as exc:  # noqa: BLE001
            error_code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if error_code in ("404", "NoSuchKey"):
                return False
            raise StorageError(
                f"Failed to check existence of {key!r} in object storage: {exc}"
            ) from exc
