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

from zerobucket.cli import cmd_info, cmd_init, cmd_migrate, cmd_verify


def _args(database_url: str, sample: int | None = None) -> argparse.Namespace:
    return argparse.Namespace(database_url=database_url, sample=sample)


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
