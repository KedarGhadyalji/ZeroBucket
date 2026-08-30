"""Tests for dedup=True (content-addressed storage with reference counting).

These use the `dedup_images` fixture (separate tables from the classic
`images` fixture -- see conftest.py and postgres.py's module docstring).
Several tests inspect the underlying zerobucket_blobs table directly via
raw SQL specifically to prove the actual storage behavior (one blob row,
correct ref_count), not just that the public API returns plausible
results.
"""

from __future__ import annotations

import threading

import psycopg
import pytest

from tests.conftest import TEST_DATABASE_URL
from zerobucket.exceptions import ImageNotFoundError


def _raw_blob_row(checksum: str):
    conn = psycopg.connect(TEST_DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ref_count FROM zerobucket_blobs WHERE checksum_sha256 = %s;",
                (checksum,),
            )
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def _raw_blob_count() -> int:
    conn = psycopg.connect(TEST_DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM zerobucket_blobs;")
            return cur.fetchone()[0]
    finally:
        conn.close()


def test_identical_uploads_share_one_blob_row(dedup_images, jpeg_bytes):
    id1 = dedup_images.put(jpeg_bytes)
    id2 = dedup_images.put(jpeg_bytes)

    assert id1 != id2  # different logical ids, as always

    meta = dedup_images.metadata(id1)
    checksum = meta.checksum_sha256

    assert _raw_blob_count() == 1  # only ONE physical copy of the bytes
    assert _raw_blob_row(checksum) == 2  # referenced by exactly 2 ids

    # Both ids independently retrievable, both return the correct bytes.
    assert dedup_images.get(id1).data == jpeg_bytes
    assert dedup_images.get(id2).data == jpeg_bytes


def test_different_uploads_get_separate_blobs(dedup_images, jpeg_bytes, png_bytes):
    id1 = dedup_images.put(jpeg_bytes)
    id2 = dedup_images.put(png_bytes)

    assert _raw_blob_count() == 2

    checksum1 = dedup_images.metadata(id1).checksum_sha256
    checksum2 = dedup_images.metadata(id2).checksum_sha256
    assert checksum1 != checksum2
    assert _raw_blob_row(checksum1) == 1
    assert _raw_blob_row(checksum2) == 1


def test_deleting_one_of_two_references_keeps_the_blob_alive(dedup_images, jpeg_bytes):
    id1 = dedup_images.put(jpeg_bytes)
    id2 = dedup_images.put(jpeg_bytes)
    checksum = dedup_images.metadata(id1).checksum_sha256

    dedup_images.delete(id1)

    assert dedup_images.exists(id1) is False
    assert dedup_images.exists(id2) is True  # the OTHER reference is untouched
    assert dedup_images.get(id2).data == jpeg_bytes  # bytes are still there
    assert _raw_blob_row(checksum) == 1  # ref_count correctly decremented, not deleted
    assert _raw_blob_count() == 1  # blob row itself still exists


def test_deleting_the_last_reference_actually_deletes_the_blob(
    dedup_images, jpeg_bytes
):
    id1 = dedup_images.put(jpeg_bytes)
    id2 = dedup_images.put(jpeg_bytes)
    checksum = dedup_images.metadata(id1).checksum_sha256

    dedup_images.delete(id1)
    assert _raw_blob_row(checksum) == 1  # one reference left

    dedup_images.delete(id2)
    assert _raw_blob_row(checksum) is None  # blob genuinely gone now
    assert _raw_blob_count() == 0


def test_delete_missing_id_returns_false_and_touches_nothing(dedup_images, jpeg_bytes):
    dedup_images.put(jpeg_bytes)
    assert _raw_blob_count() == 1

    result = dedup_images.delete("00000000-0000-0000-0000-000000000000")
    assert result is False
    assert _raw_blob_count() == 1  # untouched


def test_get_missing_id_raises_not_found(dedup_images):
    with pytest.raises(ImageNotFoundError):
        dedup_images.get("00000000-0000-0000-0000-000000000000")


def test_metadata_has_no_dimensions_mismatch_with_get(dedup_images, jpeg_bytes):
    """Sanity check that the JOIN-based dedup queries return identical
    metadata whether fetched via get() or metadata()."""
    image_id = dedup_images.put(jpeg_bytes)
    full = dedup_images.get(image_id)
    meta = dedup_images.metadata(image_id)

    assert meta.mime_type == full.mime_type
    assert meta.width == full.width
    assert meta.height == full.height
    assert meta.checksum_sha256 == full.checksum_sha256


def test_concurrent_identical_uploads_produce_correct_ref_count(
    dedup_images, jpeg_bytes
):
    """The real thing, not a simulation: N real ZeroBucket.put() calls
    for the SAME content, from real threads, hitting the real dedup
    client. Confirms no lost updates under genuine concurrent load."""
    N = 15
    ids = []
    lock = threading.Lock()

    def upload():
        image_id = dedup_images.put(jpeg_bytes)
        with lock:
            ids.append(image_id)

    threads = [threading.Thread(target=upload) for _ in range(N)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(ids) == N
    assert len(set(ids)) == N  # every id genuinely unique

    checksum = dedup_images.metadata(ids[0]).checksum_sha256
    assert _raw_blob_count() == 1  # still just one physical copy
    assert _raw_blob_row(checksum) == N  # ref_count exactly matches, no lost updates

    # Clean up.
    for image_id in ids:
        dedup_images.delete(image_id)
    assert _raw_blob_count() == 0


def test_put_many_with_duplicate_content_within_one_batch(
    dedup_images, jpeg_bytes, png_bytes
):
    """The same content appearing 3 times within a single put_many()
    batch must correctly accumulate ref_count to 3, not 1 -- verified
    empirically as a prerequisite before this method was written; this
    test proves it holds through the real public API too."""
    batch = [jpeg_bytes, jpeg_bytes, jpeg_bytes, png_bytes]
    results = dedup_images.put_many(batch)

    assert all(r.success for r in results)
    jpeg_ids = [results[0].image_id, results[1].image_id, results[2].image_id]
    png_id = results[3].image_id

    assert len(set(jpeg_ids)) == 3  # 3 distinct logical ids

    jpeg_checksum = dedup_images.metadata(jpeg_ids[0]).checksum_sha256
    png_checksum = dedup_images.metadata(png_id).checksum_sha256

    assert _raw_blob_row(jpeg_checksum) == 3
    assert _raw_blob_row(png_checksum) == 1
    assert _raw_blob_count() == 2  # only 2 distinct physical blobs total


def test_delete_many_correctly_handles_shared_and_unique_checksums(
    dedup_images, jpeg_bytes, png_bytes
):
    """A batch delete where some deleted ids share a checksum (partial
    decrement, blob survives) and others are the sole reference to their
    checksum (blob fully deleted) -- both outcomes correct in one call."""
    jpeg_id1 = dedup_images.put(jpeg_bytes)
    jpeg_id2 = dedup_images.put(jpeg_bytes)  # shares a blob with jpeg_id1
    png_id = dedup_images.put(png_bytes)  # sole reference to its blob

    jpeg_checksum = dedup_images.metadata(jpeg_id1).checksum_sha256
    png_checksum = dedup_images.metadata(png_id).checksum_sha256
    assert _raw_blob_row(jpeg_checksum) == 2
    assert _raw_blob_row(png_checksum) == 1

    # Delete ONE of the two jpeg refs, and the sole png ref, in one batch.
    results = dedup_images.delete_many([jpeg_id1, png_id])

    assert all(r.deleted for r in results)
    assert _raw_blob_row(jpeg_checksum) == 1  # jpeg blob survives, decremented
    assert _raw_blob_row(png_checksum) is None  # png blob fully gone
    assert dedup_images.exists(jpeg_id2) is True  # the surviving jpeg ref still works
    assert dedup_images.get(jpeg_id2).data == jpeg_bytes

    dedup_images.delete(jpeg_id2)
    assert _raw_blob_count() == 0


def test_connection_rollback_undoes_both_blob_and_ref_together(
    dedup_images, jpeg_bytes, db_connection_factory
):
    """The trickiest atomicity case: a dedup put() touches TWO tables
    (blobs + refs). A rollback must undo BOTH together, not leave an
    orphaned blob with a stale ref_count and no referencing id."""
    conn = db_connection_factory()
    conn.autocommit = False
    try:
        image_id = dedup_images.put(jpeg_bytes, connection=conn)
        assert dedup_images.exists(image_id, connection=conn) is True

        conn.rollback()

        assert dedup_images.exists(image_id) is False
        assert (
            _raw_blob_count() == 0
        )  # the blob insert was rolled back too, not left dangling
    finally:
        conn.close()


def test_dedup_mode_does_not_interfere_with_classic_mode_tables(
    dedup_images, images, jpeg_bytes
):
    """Both a classic-mode `images` instance and a dedup-mode
    `dedup_images` instance operating against the SAME database
    concurrently -- proving the separate-table design genuinely prevents
    any collision, not just in theory."""
    classic_id = images.put(jpeg_bytes)
    dedup_id = dedup_images.put(jpeg_bytes)

    assert images.get(classic_id).data == jpeg_bytes
    assert dedup_images.get(dedup_id).data == jpeg_bytes

    # Classic mode's table has its own row; dedup mode's tables are
    # entirely separate -- deleting from one must not affect the other.
    images.delete(classic_id)
    assert dedup_images.exists(dedup_id) is True

    dedup_images.delete(dedup_id)


def test_migrate_classic_to_dedup_preserves_ids_and_dedupes_and_is_nondestructive(
    dedup_images, images, jpeg_bytes, png_bytes
):
    """The real migration path, exercised end to end: classic-mode data
    (including a genuine duplicate) migrated into dedup tables, original
    ids preserved exactly, original table left completely untouched."""
    from zerobucket.adapters.postgres import migrate_classic_to_dedup

    # Classic-mode data, deliberately including a duplicate upload.
    id_a = images.put(jpeg_bytes)
    id_b = images.put(jpeg_bytes)  # same content as id_a -- a real duplicate
    id_c = images.put(png_bytes)

    summary = migrate_classic_to_dedup(dedup_images._backend)  # noqa: SLF001

    assert summary["images_migrated"] == 3
    assert summary["distinct_blobs_created"] == 2  # jpeg blob + png blob
    assert summary["duplicate_references_found"] == 1

    # The EXACT SAME ids now work through the dedup-mode instance.
    assert dedup_images.get(id_a).data == jpeg_bytes
    assert dedup_images.get(id_b).data == jpeg_bytes
    assert dedup_images.get(id_c).data == png_bytes

    # The duplicate correctly shares one blob, with ref_count == 2.
    jpeg_checksum = dedup_images.metadata(id_a).checksum_sha256
    assert _raw_blob_row(jpeg_checksum) == 2

    # Non-destructive: the ORIGINAL classic table is completely untouched.
    assert images.exists(id_a) is True
    assert images.exists(id_b) is True
    assert images.exists(id_c) is True
    assert images.get(id_a).data == jpeg_bytes

    images.delete(id_a)
    images.delete(id_b)
    images.delete(id_c)


def test_migrate_classic_to_dedup_raises_clearly_if_no_classic_table(dedup_images):
    """Calling the migration against a database with no classic table
    at all should fail with a clear message, not a confusing raw SQL
    error."""
    from zerobucket.adapters.postgres import migrate_classic_to_dedup
    from zerobucket.exceptions import StorageError

    conn = psycopg.connect(TEST_DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS zerobucket_images;")
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(StorageError, match="No classic zerobucket_images table"):
        migrate_classic_to_dedup(dedup_images._backend)  # noqa: SLF001
