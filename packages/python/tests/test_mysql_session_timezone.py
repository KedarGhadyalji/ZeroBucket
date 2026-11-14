"""0.33.1: `older_than_days` (and so `zerobucket tier --older-than`) is right whatever time zone the
MySQL session is in.

created_at is stored as naive UTC, so the age comparison must use UTC_TIMESTAMP(), never NOW()
(NOW() is in the session's time zone). Until now both test suites ran on UTC servers, where
NOW() and UTC_TIMESTAMP() agree, so a wrong NOW() would have passed every test. Here the session
time zone is set per connection (no privilege needed) to near-extreme offsets, +13:00 (the most
MariaDB accepts) and -12:00, and the images are 23 and 25 hours old: with NOW() the +13:00 session
counts the 23-hour-old one as older than a day, and the -12:00 session misses the 25-hour-old one.
"""

from __future__ import annotations

import io
import os
from datetime import datetime, timedelta, timezone

import pymysql
import pytest
from PIL import Image as PILImage

from zerobucket import ZeroBucket
from zerobucket.adapters.mysql import MySQLBackend, _parse_database_url

URL = os.environ.get(
    "ZEROBUCKET_TEST_MYSQL_URL",
    "mysql://zerobucket:zerobucket@127.0.0.1:3306/zerobucket_test",
)
TABLES = ("zerobucket_image_refs", "zerobucket_blobs", "zerobucket_images")


def _jpeg(color) -> bytes:
    buf = io.BytesIO()
    PILImage.new("RGB", (40, 30), color=color).save(buf, format="JPEG")
    return buf.getvalue()


def _raw():
    return pymysql.connect(**_parse_database_url(URL))


@pytest.fixture(autouse=True)
def clean_tables():
    try:
        _raw().close()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"MySQL/MariaDB not reachable: {exc}")

    def drop():
        conn = _raw()
        cur = conn.cursor()
        cur.execute("SET FOREIGN_KEY_CHECKS = 0")
        for table in TABLES:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        cur.execute("SET FOREIGN_KEY_CHECKS = 1")
        conn.commit()
        conn.close()

    drop()
    yield
    drop()


@pytest.fixture(params=["+13:00", "-12:00", "+00:00"])
def session_zone(request, monkeypatch):
    """Every connection the backend opens starts with SET time_zone = <offset>."""
    zone = request.param
    real_connect = pymysql.connect

    def connect(*args, **kwargs):
        existing = kwargs.get("init_command")
        command = f"SET time_zone = '{zone}'"
        kwargs["init_command"] = f"{existing}; {command}" if existing else command
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(pymysql, "connect", connect)
    return zone


def _utc_ago(**delta) -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(**delta)


def _session_time_zone(backend) -> str:
    def work(conn):
        with conn.cursor() as cur:
            cur.execute("SELECT @@session.time_zone;")
            return cur.fetchone()[0]

    return backend._run(None, work, operation="tz_probe")  # noqa: SLF001


@pytest.mark.parametrize("dedup", [False, True], ids=["classic", "dedup"])
def test_older_than_days_uses_utc_whatever_the_session_time_zone(session_zone, dedup):
    backend = MySQLBackend(URL, dedup=dedup)
    zb = ZeroBucket(backend=backend)
    try:
        # The setup must really have changed the session, or this test proves nothing.
        assert _session_time_zone(backend) == session_zone

        recent = zb.put(_jpeg((10, 10, 10)))  # 23 hours old, below
        older = zb.put(_jpeg((20, 20, 20)))  # 25 hours old, above
        ancient = zb.put(_jpeg((30, 30, 30)))  # 40 days old, above
        table = "zerobucket_image_refs" if dedup else "zerobucket_images"

        conn = _raw()
        cur = conn.cursor()
        for image_id, when in (
            (recent, _utc_ago(hours=23)),
            (older, _utc_ago(hours=25)),
            (ancient, _utc_ago(days=40)),
        ):
            cur.execute(
                f"UPDATE {table} SET created_at = %s WHERE id = %s", (when, image_id)
            )
        conn.commit()
        conn.close()

        entries = backend.list_images(older_than_days=1)
        assert {e.id for e in entries} == {older, ancient}
        # And the boundary the other way: nothing is older than 60 days.
        assert backend.list_images(older_than_days=60) == []
    finally:
        zb.close()
