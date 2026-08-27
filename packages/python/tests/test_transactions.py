"""Tests for the connection= parameter: does put()/delete() actually
participate in the caller's own transaction when given one, and does the
default behavior (no connection given) remain independently-committing,
exactly as it always has?

These mirror a real experiment run during development: without
connection=, a put() survives even when the caller's own separate
transaction rolls back -- proving the two are NOT atomic by default. That
finding is what motivated this feature; these tests lock in both the old
default behavior and the new opt-in behavior so neither regresses.
"""

from __future__ import annotations

import io

import psycopg
from PIL import Image as PILImage


def _jpeg_bytes() -> bytes:
    img = PILImage.new("RGB", (40, 30), color=(10, 20, 30))
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


def test_put_without_connection_commits_independently_of_caller_transaction(images):
    """Default (unchanged) behavior: put() ALWAYS commits on its own,
    regardless of what the caller's own separate connection does
    afterward. This is the current, real behavior -- not an aspiration --
    and this test exists specifically so it can't silently change without
    someone noticing."""
    from tests.conftest import TEST_DATABASE_URL

    app_conn = psycopg.connect(TEST_DATABASE_URL)
    app_conn.autocommit = False
    try:
        with app_conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS _test_app_scratch (id serial primary key);"
            )
        app_conn.commit()

        with app_conn.cursor() as cur:
            cur.execute("INSERT INTO _test_app_scratch DEFAULT VALUES;")

        image_id = images.put(_jpeg_bytes())  # no connection= passed

        app_conn.rollback()

        with app_conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM _test_app_scratch;")
            assert cur.fetchone()[0] == 0  # app's own insert was rolled back

        # But the image was NOT rolled back -- it committed independently.
        assert images.exists(image_id) is True
    finally:
        with app_conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS _test_app_scratch;")
        app_conn.commit()
        app_conn.close()


def test_put_with_connection_rolls_back_with_caller_transaction(images):
    """The new opt-in behavior: passing connection= makes put() a real
    part of the caller's transaction -- if the caller rolls back, the
    image row is gone too, same as any other write in that transaction."""
    from tests.conftest import TEST_DATABASE_URL

    app_conn = psycopg.connect(TEST_DATABASE_URL)
    app_conn.autocommit = False
    try:
        image_id = images.put(_jpeg_bytes(), connection=app_conn)

        # Readable within the SAME still-open transaction (read-your-writes).
        assert images.exists(image_id, connection=app_conn) is True

        app_conn.rollback()

        # After rollback, the image must be gone -- it was never committed.
        assert images.exists(image_id) is False
    finally:
        app_conn.close()


def test_put_with_connection_commits_with_caller_transaction(images):
    """The success path: passing connection= and then committing the
    caller's own transaction commits the image too."""
    from tests.conftest import TEST_DATABASE_URL

    app_conn = psycopg.connect(TEST_DATABASE_URL)
    app_conn.autocommit = False
    try:
        image_id = images.put(_jpeg_bytes(), connection=app_conn)
        app_conn.commit()

        assert images.exists(image_id) is True
        images.delete(image_id)
    finally:
        app_conn.close()


def test_delete_with_connection_rolls_back_with_caller_transaction(images):
    """Symmetric case for delete(): a delete() done inside a caller's
    transaction is undone if that transaction rolls back."""
    from tests.conftest import TEST_DATABASE_URL

    image_id = images.put(_jpeg_bytes())
    assert images.exists(image_id) is True

    app_conn = psycopg.connect(TEST_DATABASE_URL)
    app_conn.autocommit = False
    try:
        images.delete(image_id, connection=app_conn)
        # Within the same open transaction, it looks deleted.
        assert images.exists(image_id, connection=app_conn) is False

        app_conn.rollback()

        # After rollback, the image is back -- the delete never committed.
        assert images.exists(image_id) is True
    finally:
        app_conn.close()
        images.delete(image_id)


def test_get_with_connection_sees_uncommitted_write_in_same_transaction(images):
    """Read-your-writes: get() with the same open connection can see a
    row that was put() in that same transaction but not yet committed --
    proving they really do share one transaction, not just coincidence."""
    from tests.conftest import TEST_DATABASE_URL

    app_conn = psycopg.connect(TEST_DATABASE_URL)
    app_conn.autocommit = False
    try:
        data = _jpeg_bytes()
        image_id = images.put(data, connection=app_conn)

        # A separate, ordinary ZeroBucket call (its own connection, no
        # connection= passed) should NOT see this uncommitted row yet.
        assert images.exists(image_id) is False

        # But reading through the SAME transaction sees it fine.
        result = images.get(image_id, connection=app_conn)
        assert result.data == data

        app_conn.rollback()
        assert images.exists(image_id) is False
    finally:
        app_conn.close()
