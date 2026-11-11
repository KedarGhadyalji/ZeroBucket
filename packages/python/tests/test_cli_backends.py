"""CLI parity tests (0.28.0): the same commands on SQLite, PostgreSQL and
MySQL/MariaDB, in classic and dedup mode, plus verifying images that were
tiered to object storage.

Every scenario runs through `main(argv)` -- argparse included -- so flag
spelling, exit codes and output are what a user actually gets. S3 is moto's
in-process mock (the `s3_bucket` fixture); SQLite needs nothing; PostgreSQL
and MySQL tests skip when no server is reachable.
"""

from __future__ import annotations

import contextlib
import gc
import io
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone

import pytest
from PIL import Image as PILImage

from zerobucket import ZeroBucket
from zerobucket.adapters.sqlite import SQLiteBackend
from zerobucket.cli import _url_target, main

from .conftest import TEST_DATABASE_URL

TEST_MYSQL_URL = os.environ.get(
    "ZEROBUCKET_TEST_MYSQL_URL",
    "mysql://zerobucket:zerobucket@127.0.0.1:3306/zerobucket_test",
)
NIL = "00000000-0000-0000-0000-000000000000"
KINDS = ["sqlite", "postgres", "mysql"]
MODES = [False, True]  # dedup off / on


def _jpeg(color=(10, 20, 30)) -> bytes:
    buf = io.BytesIO()
    PILImage.new("RGB", (40, 30), color=color).save(buf, format="JPEG")
    return buf.getvalue()


def run(capsys, *argv: str) -> tuple[int, str, str]:
    """Run the CLI like a user would; return (exit code, stdout, stderr)."""
    code = 0
    try:
        main(list(argv))
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else (0 if exc.code is None else 1)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def _remove_db_files(path: str) -> None:
    gc.collect()
    for ext in ("", "-wal", "-shm"):
        target = path + ext
        for attempt in range(10):
            try:
                if os.path.exists(target):
                    os.remove(target)
                break
            except PermissionError:  # Windows releases a closed handle a moment later
                if attempt == 9:
                    raise
                time.sleep(0.1)
                gc.collect()


@pytest.fixture(autouse=True)
def _no_ambient_database(monkeypatch):
    monkeypatch.delenv("ZEROBUCKET_DATABASE_URL", raising=False)
    monkeypatch.delenv("ZEROBUCKET_SQLITE_PATH", raising=False)


class Target:
    """One database under test: how to name it on the command line, open a
    backend on it, backdate a row, and (for sqlite) corrupt one."""

    def __init__(self, kind: str, path: str | None = None):
        self.kind = kind
        self.path = path
        self.url = {"postgres": TEST_DATABASE_URL, "mysql": TEST_MYSQL_URL}.get(kind)

    @property
    def args(self) -> list[str]:
        return (
            ["--sqlite", self.path]
            if self.kind == "sqlite"
            else ["--database-url", self.url]
        )

    def backend(self, dedup: bool = False, **kw):
        if self.kind == "sqlite":
            return SQLiteBackend(self.path, dedup=dedup, **kw)
        if self.kind == "postgres":
            from zerobucket.adapters.postgres import PostgresBackend

            return PostgresBackend(self.url, dedup=dedup, **kw)
        from zerobucket.adapters.mysql import MySQLBackend

        return MySQLBackend(self.url, dedup=dedup, **kw)

    def seed(self, dedup: bool = False, colors=((200, 0, 0), (200, 0, 0), (0, 0, 200))):
        """Store images (two identical by default) and return their ids."""
        zb = ZeroBucket(backend=self.backend(dedup))
        try:
            return [zb.put(_jpeg(c)) for c in colors]
        finally:
            zb.close()

    def backdate(self, image_id: str, days: int) -> None:
        if self.kind == "sqlite":
            when = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
            with contextlib.closing(sqlite3.connect(self.path)) as c:
                c.execute(
                    "UPDATE zerobucket_images SET created_at = ? WHERE id = ?",
                    (when, image_id),
                )
                c.commit()
        elif self.kind == "postgres":
            import psycopg

            with psycopg.connect(self.url, autocommit=True) as c:
                c.execute(
                    "UPDATE zerobucket_images SET created_at = now() - make_interval(days => %s) "
                    "WHERE id = %s",
                    (days, image_id),
                )
        else:
            import pymysql

            from zerobucket.adapters.mysql import _parse_database_url

            conn = pymysql.connect(**_parse_database_url(self.url), autocommit=True)
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE zerobucket_images "
                        "SET created_at = UTC_TIMESTAMP(6) - INTERVAL %s DAY WHERE id = %s",
                        (days, image_id),
                    )
            finally:
                conn.close()


def _drop_everything(target: Target) -> None:
    if target.kind == "postgres":
        import psycopg

        with psycopg.connect(target.url, autocommit=True) as c:
            c.execute(
                "DROP TABLE IF EXISTS zerobucket_image_refs, "
                "zerobucket_blobs, zerobucket_images CASCADE"
            )
    elif target.kind == "mysql":
        import pymysql

        from zerobucket.adapters.mysql import _parse_database_url

        conn = pymysql.connect(**_parse_database_url(target.url), autocommit=True)
        try:
            with conn.cursor() as cur:
                cur.execute("SET FOREIGN_KEY_CHECKS = 0")
                for table in (
                    "zerobucket_image_refs",
                    "zerobucket_blobs",
                    "zerobucket_images",
                ):
                    cur.execute(f"DROP TABLE IF EXISTS {table}")
                cur.execute("SET FOREIGN_KEY_CHECKS = 1")
        finally:
            conn.close()


@pytest.fixture(params=KINDS)
def target(request):
    kind = request.param
    if kind == "postgres":
        request.getfixturevalue("_db_available")  # skips when unreachable
    elif kind == "mysql":
        import pymysql

        from zerobucket.adapters.mysql import _parse_database_url

        try:
            pymysql.connect(
                **_parse_database_url(TEST_MYSQL_URL), connect_timeout=3
            ).close()
        except Exception as exc:  # noqa: BLE001
            pytest.skip(f"MySQL/MariaDB not reachable: {exc}")
    path = tempfile.mktemp(suffix=".db") if kind == "sqlite" else None
    t = Target(kind, path)
    _drop_everything(t)
    yield t
    _drop_everything(t)
    if path:
        _remove_db_files(path)


@pytest.fixture
def sqlite_target():
    path = tempfile.mktemp(suffix=".db")
    yield Target("sqlite", path)
    _remove_db_files(path)


# ---- init / info / verify, every backend, classic and dedup ---------------------


@pytest.mark.parametrize("dedup", MODES)
def test_init_info_verify_roundtrip(target, dedup, capsys):
    flag = ["--dedup"] if dedup else []
    code, out, _ = run(capsys, "init", *target.args, *flag)
    assert code == 0
    assert "tables and indexes are ready" in out or "table and indexes are ready" in out
    if dedup:
        assert "zerobucket_blobs and zerobucket_image_refs" in out

    target.seed(dedup)  # 3 images, two of them identical

    code, out, _ = run(capsys, "info", *target.args, *flag)
    assert code == 0
    assert "3 image(s)" in out
    assert "image/jpeg" in out
    if dedup:
        assert "sharing 2 distinct blob(s)" in out
        assert "Physical bytes after dedup" in out
        assert "saved" in out
    else:
        assert "zerobucket_images: 3 image(s)" in out
        assert "Tiered to object storage" not in out  # nothing tiered yet
    assert "On-disk size" in out

    code, out, _ = run(capsys, "verify", *target.args, *flag)
    assert code == 0
    assert "Verifying 3 image(s)" in out
    assert "OK: all 3 image(s) verified, no checksum mismatches." in out


@pytest.mark.parametrize("dedup", MODES)
def test_info_before_init_reports_missing_tables(target, dedup, capsys):
    if target.kind == "sqlite":
        with contextlib.closing(sqlite3.connect(target.path)):
            pass  # an existing but empty file
    code, out, _ = run(capsys, "info", *target.args, *(["--dedup"] if dedup else []))
    assert code == 1
    assert "does not exist yet" in out or "do not exist yet" in out


@pytest.mark.parametrize("dedup", MODES)
def test_verify_sample_checks_fewer(target, dedup, capsys):
    target.seed(dedup)
    code, out, _ = run(
        capsys, "verify", *target.args, "--sample", "2", *(["--dedup"] if dedup else [])
    )
    assert code == 0
    assert "Verifying 2 image(s)" in out


@pytest.mark.parametrize("dedup", MODES)
def test_verify_detects_corruption_in_sqlite(sqlite_target, dedup, capsys):
    ids = sqlite_target.seed(dedup)
    table = "zerobucket_blobs" if dedup else "zerobucket_images"
    with contextlib.closing(sqlite3.connect(sqlite_target.path)) as c:
        c.execute(f"UPDATE {table} SET data = ?", (b"corrupt",))
        c.commit()
    code, out, _ = run(
        capsys, "verify", *sqlite_target.args, *(["--dedup"] if dedup else [])
    )
    assert code == 1
    assert "FAILED" in out
    assert any(i in out for i in ids)
    assert "OK:" not in out


def test_info_dedup_savings_are_accurate(sqlite_target, capsys):
    sqlite_target.seed(True, colors=((9, 9, 9),) * 4)  # four identical images
    code, out, _ = run(capsys, "info", *sqlite_target.args, "--dedup")
    assert code == 0
    assert "4 image(s) sharing 1 distinct blob(s)" in out
    assert "(saved " in out and "75.0%" in out  # 3 of 4 copies are not stored


# ---- tiering: tier on every backend, verify with and without a bucket ---------------


def _tier_args(target: Target, bucket: str, *extra: str) -> list[str]:
    return ["tier", *extra, *target.args, "--bucket", bucket, "--region", "us-east-1"]


def test_tier_then_info_and_verify_with_and_without_bucket(target, s3_bucket, capsys):
    ids = target.seed(False, colors=((1, 1, 1), (50, 60, 70)))
    code, out, err = run(capsys, *_tier_args(target, s3_bucket, ids[0]))
    assert code == 0, err
    assert "Tiered: 1, already tiered (skipped): 0, failed: 0" in out

    code, out, _ = run(capsys, "info", *target.args)
    assert code == 0
    assert "Tiered to object storage:" in out and "1 image(s)" in out

    # Without a bucket, the tiered image is skipped and the run says how to include it.
    code, out, _ = run(capsys, "verify", *target.args)
    assert code == 0
    assert "OK: all 1 image(s) verified" in out
    assert "1 image(s) tiered to object storage were skipped" in out

    # With the bucket, both are checked and nothing is skipped.
    code, out, err = run(
        capsys, "verify", *target.args, "--bucket", s3_bucket, "--region", "us-east-1"
    )
    assert code == 0, err
    assert "OK: all 2 image(s) verified, no checksum mismatches." in out
    assert "were skipped" not in out


def test_verify_with_bucket_detects_a_corrupted_object(target, s3_bucket, capsys):
    import boto3

    ids = target.seed(False, colors=((1, 1, 1),))
    assert run(capsys, *_tier_args(target, s3_bucket, ids[0]))[0] == 0

    s3 = boto3.client("s3", region_name="us-east-1")
    key = s3.list_objects_v2(Bucket=s3_bucket)["Contents"][0]["Key"]
    size = s3.head_object(Bucket=s3_bucket, Key=key)["ContentLength"]
    s3.put_object(
        Bucket=s3_bucket, Key=key, Body=b"X" * size
    )  # same length, wrong bytes

    code, out, _ = run(
        capsys, "verify", *target.args, "--bucket", s3_bucket, "--region", "us-east-1"
    )
    assert code == 1
    assert ids[0] in out
    assert "OK:" not in out


def test_tier_bulk_filters_and_dry_run(target, s3_bucket, capsys):
    ids = target.seed(False, colors=((1, 1, 1), (50, 60, 70), (90, 10, 10)))
    target.backdate(ids[0], 10)

    code, out, _ = run(
        capsys, *_tier_args(target, s3_bucket, "--older-than", "5", "--dry-run")
    )
    assert code == 0
    assert "Would tier 1 image(s):" in out and ids[0] in out and ids[1] not in out

    code, out, _ = run(capsys, *_tier_args(target, s3_bucket, "--all", "--limit", "1"))
    assert code == 0
    assert "Tiered: 1, already tiered (skipped): 0, failed: 0" in out

    code, out, _ = run(
        capsys, *_tier_args(target, s3_bucket, "--all")
    )  # the remaining two
    assert code == 0
    assert "Tiered: 2, already tiered (skipped): 0, failed: 0" in out

    code, out, _ = run(capsys, *_tier_args(target, s3_bucket, "--all"))
    assert code == 0
    assert "No matching images to tier (or all already tiered)." in out


# ---- migrate --to-dedup (every database, since 0.31.0) ---------------------------------


def test_migrate_to_dedup_copies_and_is_rerunnable(target, capsys):
    target.seed(False)  # 3 classic images, two identical
    code, out, err = run(capsys, "migrate", "--to-dedup", *target.args)
    assert code == 0, err
    assert "Migrated 3 image(s) into 2 distinct blob(s)." in out
    assert "Duplicate references found: 1" in out
    assert "The classic table was not modified." in out

    code, out, _ = run(capsys, "info", *target.args, "--dedup")
    assert "3 image(s) sharing 2 distinct blob(s)" in out
    code, out, _ = run(capsys, "info", *target.args)
    assert "zerobucket_images: 3 image(s)" in out  # classic table untouched

    code, out, _ = run(
        capsys, "migrate", "--to-dedup", *target.args
    )  # re-run: nothing new
    assert code == 0
    assert "Migrated 0 image(s)" in out
    assert "Already migrated (skipped): 3" in out


def test_migrate_to_dedup_refuses_tiered_rows_unless_skip_tiered(
    target, s3_bucket, capsys
):
    ids = target.seed(False, colors=((1, 1, 1), (50, 60, 70)))
    assert run(capsys, *_tier_args(target, s3_bucket, ids[0]))[0] == 0

    code, out, _ = run(capsys, "migrate", "--to-dedup", *target.args)
    assert code == 1
    assert "1 classic image(s) are tiered to object storage" in out
    assert "--skip-tiered" in out

    code, out, err = run(capsys, "migrate", "--to-dedup", "--skip-tiered", *target.args)
    assert code == 0, err
    assert "Migrated 1 image(s) into 1 distinct blob(s)." in out
    assert "Tiered to object storage (skipped, NOT in the dedup tables): 1" in out


def test_migrate_to_dedup_without_a_classic_table_fails_cleanly(target, capsys):
    run(capsys, "init", *target.args, "--dedup")  # dedup tables only
    code, out, _ = run(capsys, "migrate", "--to-dedup", *target.args)
    assert code == 1
    assert "nothing to migrate" in out.lower() or "no classic" in out.lower()


def test_migrate_to_dedup_does_not_create_a_missing_sqlite_file(sqlite_target, capsys):
    code, out, _ = run(capsys, "migrate", "--to-dedup", *sqlite_target.args)
    assert code == 1
    assert "does not exist" in out
    assert not os.path.exists(sqlite_target.path)


def test_skip_tiered_alone_is_a_usage_error(sqlite_target, capsys):
    code, _, err = run(capsys, "migrate", "--skip-tiered", *sqlite_target.args)
    assert code == 2
    assert "--skip-tiered only applies with --to-dedup" in err


# ---- usage errors and edge cases ---------------------------------------------------------------


def test_sqlite_and_database_url_together_is_a_usage_error(capsys):
    code, _, err = run(
        capsys, "info", "--sqlite", "x.db", "--database-url", "postgresql://u@h/d"
    )
    assert code == 2
    assert "choose one database" in err


def test_both_environment_variables_set_is_a_usage_error(monkeypatch, capsys):
    monkeypatch.setenv("ZEROBUCKET_DATABASE_URL", "postgresql://u@h/d")
    monkeypatch.setenv("ZEROBUCKET_SQLITE_PATH", "x.db")
    code, _, err = run(capsys, "info")
    assert code == 2
    assert "both ZEROBUCKET_DATABASE_URL and ZEROBUCKET_SQLITE_PATH" in err


def test_nothing_given_mentions_both_ways_to_choose_a_database(capsys):
    code, _, err = run(capsys, "info")
    assert code == 2
    assert "no database url provided" in err.lower()
    assert "--sqlite" in err and "ZEROBUCKET_SQLITE_PATH" in err


def test_sqlite_path_from_the_environment(sqlite_target, monkeypatch, capsys):
    sqlite_target.seed(False)
    monkeypatch.setenv("ZEROBUCKET_SQLITE_PATH", sqlite_target.path)
    code, out, _ = run(capsys, "info")
    assert code == 0
    assert "zerobucket_images: 3 image(s)" in out


def test_flags_beat_the_environment(sqlite_target, monkeypatch, capsys):
    sqlite_target.seed(False)
    monkeypatch.setenv(
        "ZEROBUCKET_DATABASE_URL", "postgresql://nobody:x@127.0.0.1:1/none"
    )
    code, out, _ = run(capsys, "info", *sqlite_target.args)
    assert code == 0
    assert "3 image(s)" in out


def test_missing_sqlite_file_is_reported_and_not_created(sqlite_target, capsys):
    for command in ("info", "verify"):
        code, out, _ = run(capsys, command, *sqlite_target.args)
        assert code == 1
        assert "does not exist yet" in out
    assert not os.path.exists(sqlite_target.path)  # reading must never create the file


def test_unreachable_postgres_is_a_connection_error(capsys):
    code, _, err = run(
        capsys, "info", "--database-url", "postgresql://nobody:x@127.0.0.1:1/none"
    )
    assert code == 2
    assert "could not connect" in err


def test_tier_and_verify_option_validation(sqlite_target, capsys):
    base = ["tier", *sqlite_target.args, "--bucket", "b"]
    code, _, err = run(capsys, *base, "--all", "--dedup")
    assert code == 2 and "not supported with --dedup" in err
    code, _, err = run(capsys, *base, "--all", "--limit", "0")
    assert code == 2 and "--limit must be an integer >= 1" in err
    code, _, err = run(capsys, *base, "--all", "--aws-access-key-id", "only-one")
    assert (
        code == 2 and "pass both --aws-access-key-id and --aws-secret-access-key" in err
    )
    code, _, err = run(capsys, "verify", *sqlite_target.args, "--sample", "0")
    assert code == 2 and "--sample must be an integer >= 1" in err
    code, _, err = run(
        capsys, "verify", *sqlite_target.args, "--bucket", "b", "--dedup"
    )
    assert code == 2 and "--bucket does not apply with --dedup" in err


@pytest.mark.parametrize(
    ("url", "kind"),
    [
        ("postgresql://u@h/d", "postgres"),
        ("postgres://u@h/d", "postgres"),
        ("mysql://u@h/d", "mysql"),
        ("MYSQL://u@h/d", "mysql"),
        ("mariadb://u@h/d", "mysql"),
    ],
)
def test_url_scheme_selects_the_backend(url, kind):
    assert _url_target(url).kind == kind


def test_entry_point_runs_sqlite_end_to_end(sqlite_target):
    sqlite_target.seed(False)
    result = subprocess.run(
        [sys.executable, "-m", "zerobucket.cli", "info", *sqlite_target.args],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0
    assert "zerobucket_images: 3 image(s)" in result.stdout


# ---- the new backend methods the CLI is built on --------------------------------------------


def test_list_images_filters(target, s3_bucket, capsys):
    ids = target.seed(False, colors=((1, 1, 1), (50, 60, 70), (90, 10, 10)))
    target.backdate(ids[0], 10)
    backend = target.backend()
    try:
        assert [e.id for e in backend.list_images()][0] == ids[0]  # oldest first
        assert {e.id for e in backend.list_images()} == set(ids)
        assert [e.id for e in backend.list_images(older_than_days=5)] == [ids[0]]
        assert len(backend.list_images(limit=2)) == 2
        assert len(backend.list_images(sample=2)) == 2
        assert backend.list_images(min_size=10**9) == []
        assert not any(e.tiered for e in backend.list_images())
    finally:
        backend.close()

    assert run(capsys, *_tier_args(target, s3_bucket, ids[1]))[0] == 0
    backend = target.backend()
    try:
        entries = {e.id: e.tiered for e in backend.list_images()}
        assert entries[ids[1]] is True and entries[ids[0]] is False
        assert {e.id for e in backend.list_images(only_untiered=True)} == {
            ids[0],
            ids[2],
        }
    finally:
        backend.close()


def test_list_images_rejects_negative_numbers(sqlite_target):
    sqlite_target.seed(False)
    backend = sqlite_target.backend()
    for name in ("min_size", "older_than_days", "limit", "sample"):
        with pytest.raises(ValueError, match=name):
            backend.list_images(**{name: -1})


def test_get_info_is_none_when_tables_are_missing(target):
    if target.kind == "sqlite":
        with contextlib.closing(sqlite3.connect(target.path)):
            pass
    for dedup in MODES:
        backend = target.backend(dedup, auto_migrate=False)
        try:
            assert backend.get_info() is None
        finally:
            backend.close()


def test_get_info_matches_across_backends(target):
    target.seed(False)
    backend = target.backend()
    try:
        info = backend.get_info()
    finally:
        backend.close()
    assert info.mode == "classic" and info.count == 3
    assert info.total_bytes == sum(f.bytes for f in info.by_format)
    assert info.tiered.count == 0 and info.dedup is None
    assert info.oldest is not None and info.newest is not None
