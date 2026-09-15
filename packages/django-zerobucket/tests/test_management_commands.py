"""Tests for the management commands, using Django's real
call_command() -- through actual command dispatch/argument parsing, not
calling handle() directly.

sys.exit() calls inside the wrapped zerobucket.cli functions propagate
as SystemExit through call_command() the same as they would from the
standalone CLI (see each command module's docstring) -- tests that
expect a failure exit wrap the call in pytest.raises(SystemExit).
"""

from __future__ import annotations

import pytest
from django.core.files.base import ContentFile
from django.core.management import call_command
from moto import mock_aws

from django_zerobucket.storage import ZeroBucketStorage
from tests.django_settings import ZEROBUCKET_DATABASE_URL


def _storage() -> ZeroBucketStorage:
    return ZeroBucketStorage(database_url=ZEROBUCKET_DATABASE_URL)


# ---- zerobucket_info --------------------------------------------------


def test_info_reports_image_count(zb_client, jpeg_bytes, capsys):
    storage = _storage()
    storage.save("a.jpg", ContentFile(jpeg_bytes))
    storage.save("b.jpg", ContentFile(jpeg_bytes))

    call_command("zerobucket_info")
    out = capsys.readouterr().out
    assert "2" in out


# ---- zerobucket_verify --------------------------------------------------


def test_verify_passes_on_healthy_data(zb_client, jpeg_bytes, capsys):
    storage = _storage()
    storage.save("a.jpg", ContentFile(jpeg_bytes))

    call_command("zerobucket_verify")
    out = capsys.readouterr().out
    assert "OK" in out


def test_verify_detects_tampering(zb_client, jpeg_bytes, capsys):
    storage = _storage()
    name = storage.save("a.jpg", ContentFile(jpeg_bytes))

    with zb_client._backend._pool.connection() as conn, conn.cursor() as cur:  # noqa: SLF001
        cur.execute(
            "UPDATE zerobucket_images SET data = %s WHERE id = %s;",
            (b"\x00" * len(jpeg_bytes), name),
        )
        conn.commit()

    with pytest.raises(SystemExit) as exc_info:
        call_command("zerobucket_verify")
    assert exc_info.value.code == 1
    out = capsys.readouterr().out
    assert "FAILED" in out


# ---- zerobucket_tier --------------------------------------------------


def test_tier_single_id(zb_client, jpeg_bytes, capsys):
    with mock_aws():
        import boto3

        boto3.client("s3", region_name="us-east-1").create_bucket(
            Bucket="django-zb-cli-test"
        )

        storage = _storage()
        name = storage.save("a.jpg", ContentFile(jpeg_bytes))

        call_command(
            "zerobucket_tier",
            name,
            bucket="django-zb-cli-test",
            region="us-east-1",
        )
        out = capsys.readouterr().out
        assert "Tiered: 1, already tiered (skipped): 0, failed: 0" in out


def test_tier_no_selection_exits_with_usage_error(capsys):
    with pytest.raises(SystemExit) as exc_info:
        call_command("zerobucket_tier", bucket="some-bucket")
    assert exc_info.value.code == 2
