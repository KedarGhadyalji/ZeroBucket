"""Tests for the zerobucket CLI.

Most tests call the cmd_* functions directly with a constructed
argparse.Namespace (fast, precise). One test (test_entry_point_works_end_to_end)
actually invokes the installed `zerobucket` console script via subprocess,
to prove the [project.scripts] entry point genuinely works -- calling
cmd_* functions directly would not catch a broken entry-point registration.
"""

from __future__ import annotations

import argparse
import io
import subprocess
import sys

import psycopg
import pytest
from PIL import Image as PILImage

from zerobucket.cli import cmd_info, cmd_init, cmd_migrate, cmd_tier, cmd_verify


def _args(database_url: str, sample: int | None = None) -> argparse.Namespace:
    return argparse.Namespace(database_url=database_url, sample=sample)


def _tier_args(
    database_url: str,
    bucket: str,
    *,
    image_id: str | None = None,
    min_size: int | None = None,
    older_than: int | None = None,
    all: bool = False,  # noqa: A002 -- matches the CLI flag name exactly
    limit: int | None = None,
    dry_run: bool = False,
) -> argparse.Namespace:
    return argparse.Namespace(
        database_url=database_url,
        image_id=image_id,
        bucket=bucket,
        endpoint_url=None,
        region="us-east-1",
        aws_access_key_id=None,
        aws_secret_access_key=None,
        min_size=min_size,
        older_than=older_than,
        all=all,
        limit=limit,
        dry_run=dry_run,
    )


def _jpeg_bytes(color=(10, 20, 30)) -> bytes:
    img = PILImage.new("RGB", (40, 30), color=color)
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


def test_init_creates_schema(_db_available, capsys):
    from tests.conftest import TEST_DATABASE_URL

    cmd_init(_args(TEST_DATABASE_URL))
    out = capsys.readouterr().out
    assert "ready" in out.lower()

    # Confirm the table genuinely exists now, via a raw connection.
    conn = psycopg.connect(TEST_DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('zerobucket_images');")
            assert cur.fetchone()[0] is not None
    finally:
        conn.close()


def test_migrate_runs_without_error_and_notes_limitation(_db_available, capsys):
    from tests.conftest import TEST_DATABASE_URL

    cmd_migrate(_args(TEST_DATABASE_URL))
    out = capsys.readouterr().out
    assert "does not have versioned migrations" in out
    assert "ready" in out.lower()


def test_info_reports_correct_count_and_format_breakdown(images, capsys):
    from tests.conftest import TEST_DATABASE_URL

    id1 = images.put(_jpeg_bytes((1, 2, 3)))
    id2 = images.put(_jpeg_bytes((4, 5, 6)))

    cmd_info(_args(TEST_DATABASE_URL))
    out = capsys.readouterr().out

    assert "image(s)" in out
    assert "image/jpeg" in out

    images.delete(id1)
    images.delete(id2)


def test_info_on_empty_table_does_not_crash(images, capsys):
    from tests.conftest import TEST_DATABASE_URL

    cmd_info(_args(TEST_DATABASE_URL))
    out = capsys.readouterr().out
    assert "0 image(s)" in out


def test_verify_passes_on_healthy_data(images, capsys):
    from tests.conftest import TEST_DATABASE_URL

    id1 = images.put(_jpeg_bytes((1, 2, 3)))
    id2 = images.put(_jpeg_bytes((4, 5, 6)))

    cmd_verify(_args(TEST_DATABASE_URL))
    out = capsys.readouterr().out
    assert "OK" in out
    assert "mismatch" not in out.lower() or "no checksum mismatches" in out

    images.delete(id1)
    images.delete(id2)


def test_verify_detects_tampered_data(images, capsys):
    """Directly tamper with a row's bytes via raw SQL (bypassing the
    client entirely, simulating real corruption) and confirm verify
    actually catches it."""
    from tests.conftest import TEST_DATABASE_URL

    image_id = images.put(_jpeg_bytes())

    conn = psycopg.connect(TEST_DATABASE_URL)
    try:
        with conn.cursor() as cur:
            # Corrupt the stored bytes without touching the checksum column
            # -- exactly what silent bit-rot or bad manual tampering would
            # look like.
            cur.execute(
                "UPDATE zerobucket_images SET data = %s WHERE id = %s;",
                (b"corrupted-not-a-real-image", image_id),
            )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(SystemExit) as exc_info:
        cmd_verify(_args(TEST_DATABASE_URL))
    assert exc_info.value.code == 1

    out = capsys.readouterr().out
    assert "FAILED" in out
    assert image_id in out

    images.delete(image_id)


def test_verify_with_sample_checks_fewer_rows(images, capsys):
    from tests.conftest import TEST_DATABASE_URL

    ids = [images.put(_jpeg_bytes((i, i, i))) for i in range(5)]

    cmd_verify(_args(TEST_DATABASE_URL, sample=2))
    out = capsys.readouterr().out
    assert "Verifying 2 image(s)" in out

    for image_id in ids:
        images.delete(image_id)


def test_no_database_url_exits_with_usage_error(monkeypatch, capsys):
    monkeypatch.delenv("ZEROBUCKET_DATABASE_URL", raising=False)
    with pytest.raises(SystemExit) as exc_info:
        cmd_init(_args(None))
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "no database url" in err.lower()


def test_entry_point_works_end_to_end(_db_available):
    """Actually invoke the installed `zerobucket` console script, not
    just the Python functions -- this is the only test that would catch
    a broken [project.scripts] registration in pyproject.toml."""
    from tests.conftest import TEST_DATABASE_URL

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "zerobucket.cli",
            "info",
            "--database-url",
            TEST_DATABASE_URL,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    assert "image(s)" in result.stdout


def test_version_flag():
    result = subprocess.run(
        [sys.executable, "-m", "zerobucket.cli", "--version"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0
    assert "zerobucket" in result.stdout.lower()


# ---- tier ------------------------------------------------------------------
# Uses the s3_bucket fixture from conftest.py (moto's in-process mock_aws()
# -- see that fixture's docstring for why not a standalone moto_server).


def test_tier_single_id(images, s3_bucket, capsys):
    from tests.conftest import TEST_DATABASE_URL

    image_id = images.put(_jpeg_bytes())

    cmd_tier(_tier_args(TEST_DATABASE_URL, s3_bucket, image_id=image_id))
    out = capsys.readouterr().out
    assert "Tiered: 1, already tiered (skipped): 0, failed: 0" in out

    with psycopg.connect(TEST_DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT storage_backend FROM zerobucket_images WHERE id = %s;",
            (image_id,),
        )
        assert cur.fetchone()[0] == "object_storage"


def test_tier_single_id_already_tiered_is_reported_as_skipped(
    images, s3_bucket, capsys
):
    from tests.conftest import TEST_DATABASE_URL

    image_id = images.put(_jpeg_bytes())
    cmd_tier(_tier_args(TEST_DATABASE_URL, s3_bucket, image_id=image_id))
    capsys.readouterr()  # discard first run's output

    cmd_tier(_tier_args(TEST_DATABASE_URL, s3_bucket, image_id=image_id))
    out = capsys.readouterr().out
    assert "Tiered: 0, already tiered (skipped): 1, failed: 0" in out


def test_tier_single_id_not_found_reported_as_failed(images, s3_bucket, capsys):
    from tests.conftest import TEST_DATABASE_URL

    with pytest.raises(SystemExit) as exc_info:
        cmd_tier(
            _tier_args(
                TEST_DATABASE_URL,
                s3_bucket,
                image_id="00000000-0000-0000-0000-000000000000",
            )
        )
    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "Tiered: 0, already tiered (skipped): 0, failed: 1" in captured.out
    assert "NOT FOUND" in captured.err


def test_tier_no_selection_given_exits_with_usage_error(capsys):
    with pytest.raises(SystemExit) as exc_info:
        cmd_tier(_tier_args("postgresql://irrelevant", "some-bucket"))
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "specify either" in err.lower()


def test_tier_image_id_combined_with_bulk_filter_exits_with_usage_error(capsys):
    with pytest.raises(SystemExit) as exc_info:
        cmd_tier(
            _tier_args(
                "postgresql://irrelevant",
                "some-bucket",
                image_id="some-id",
                all=True,
            )
        )
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "can't combine" in err.lower()


def test_tier_bulk_all_only_selects_untiered_images(images, s3_bucket, capsys):
    from tests.conftest import TEST_DATABASE_URL

    data = _jpeg_bytes()
    already_tiered_id = images.put(data)
    untiered_id = images.put(data)

    cmd_tier(_tier_args(TEST_DATABASE_URL, s3_bucket, image_id=already_tiered_id))
    capsys.readouterr()

    cmd_tier(_tier_args(TEST_DATABASE_URL, s3_bucket, all=True))
    out = capsys.readouterr().out
    # Only the untiered one should have been picked up and tiered --
    # the already-tiered one isn't even a candidate (see
    # _select_tier_candidates' docstring), not merely skipped as a no-op.
    assert "Tiered: 1, already tiered (skipped): 0, failed: 0" in out

    with psycopg.connect(TEST_DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, storage_backend FROM zerobucket_images ORDER BY created_at;"
        )
        rows = {str(row[0]): row[1] for row in cur.fetchall()}
    assert rows[already_tiered_id] == "object_storage"
    assert rows[untiered_id] == "object_storage"


def test_tier_bulk_min_size_filter(images, s3_bucket, capsys):
    from tests.conftest import TEST_DATABASE_URL

    small_id = images.put(_jpeg_bytes())
    big_id = images.put(_jpeg_bytes(color=(9, 9, 9)))

    with psycopg.connect(TEST_DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT size_bytes FROM zerobucket_images WHERE id = %s;", (small_id,)
        )
        small_size = cur.fetchone()[0]
        # Force a real, unambiguous size difference rather than relying
        # on JPEG compression happening to produce one -- this test is
        # about the >= comparison working correctly, not about how JPEG
        # compresses solid colors.
        cur.execute(
            "UPDATE zerobucket_images SET size_bytes = size_bytes + 1000000 "
            "WHERE id = %s;",
            (big_id,),
        )
        conn.commit()

    cmd_tier(_tier_args(TEST_DATABASE_URL, s3_bucket, min_size=small_size + 1000))
    out = capsys.readouterr().out
    assert "Tiered: 1, already tiered (skipped): 0, failed: 0" in out

    with psycopg.connect(TEST_DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, storage_backend FROM zerobucket_images ORDER BY created_at;"
        )
        rows = {str(row[0]): row[1] for row in cur.fetchall()}
    assert rows[small_id] == "postgres"
    assert rows[big_id] == "object_storage"


def test_tier_dry_run_does_not_actually_tier(images, s3_bucket, capsys):
    from tests.conftest import TEST_DATABASE_URL

    image_id = images.put(_jpeg_bytes())

    cmd_tier(_tier_args(TEST_DATABASE_URL, s3_bucket, all=True, dry_run=True))
    out = capsys.readouterr().out
    assert "Would tier 1 image(s)" in out
    assert image_id in out

    with psycopg.connect(TEST_DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT storage_backend FROM zerobucket_images WHERE id = %s;",
            (image_id,),
        )
        assert cur.fetchone()[0] == "postgres"


def test_tier_bulk_limit_caps_how_many_are_processed(images, s3_bucket, capsys):
    from tests.conftest import TEST_DATABASE_URL

    ids = [images.put(_jpeg_bytes(color=(i, i, i))) for i in range(1, 4)]

    cmd_tier(_tier_args(TEST_DATABASE_URL, s3_bucket, all=True, limit=1))
    out = capsys.readouterr().out
    assert "Tiered: 1, already tiered (skipped): 0, failed: 0" in out

    with psycopg.connect(TEST_DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute("SELECT storage_backend FROM zerobucket_images;")
        backends = [row[0] for row in cur.fetchall()]
    assert backends.count("object_storage") == 1
    assert backends.count("postgres") == len(ids) - 1


def test_tier_bulk_no_matches_reports_clearly_and_does_not_error(
    images, s3_bucket, capsys
):
    from tests.conftest import TEST_DATABASE_URL

    images.put(_jpeg_bytes())  # exists, but won't match an absurd min_size

    cmd_tier(_tier_args(TEST_DATABASE_URL, s3_bucket, min_size=10**9))
    out = capsys.readouterr().out
    assert "No matching images to tier" in out


def test_tier_without_boto3_installed_gives_clear_error(
    images, s3_bucket, capsys, monkeypatch
):
    """Simulates boto3 not being installed by making the import fail --
    confirms the CLI surfaces ObjectStorage's own clear ImportError
    message (see object_storage.py) rather than a raw traceback."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "boto3":
            raise ImportError("No module named 'boto3'")
        return real_import(name, *args, **kwargs)

    from tests.conftest import TEST_DATABASE_URL

    image_id = images.put(_jpeg_bytes())

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(SystemExit) as exc_info:
        cmd_tier(_tier_args(TEST_DATABASE_URL, s3_bucket, image_id=image_id))
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "zerobucket[s3]" in err
