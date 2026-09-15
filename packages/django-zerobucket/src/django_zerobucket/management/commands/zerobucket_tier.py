"""./manage.py zerobucket_tier -- thin wrapper around zerobucket.cli's
cmd_tier. Same argument shape as the standalone `zerobucket tier` CLI
command (see that command's docs for the full explanation of single-id
vs bulk selection, --dry-run, and the transactional-safety guarantee of
tier_to_object_storage() underneath) -- this just supplies
database_url from Django settings instead of requiring --database-url,
and requires `pip install zerobucket[s3]` the same way the standalone
command does (boto3 stays an optional dependency here too, not pulled
in by installing django-zerobucket itself).
"""

from __future__ import annotations

import argparse

from django.conf import settings
from django.core.management.base import BaseCommand
from zerobucket.cli import cmd_tier


class Command(BaseCommand):
    help = "Move image(s) from Postgres into S3-compatible object storage."

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "image_id",
            nargs="?",
            default=None,
            help="Tier a single image by id. Omit and use --all/--min-size/"
            "--older-than instead for bulk selection.",
        )
        parser.add_argument(
            "--bucket",
            required=True,
            help="S3(-compatible) bucket -- must already exist.",
        )
        parser.add_argument("--endpoint-url", default=None)
        parser.add_argument("--region", default=None)
        parser.add_argument("--aws-access-key-id", default=None)
        parser.add_argument("--aws-secret-access-key", default=None)
        parser.add_argument("--min-size", type=int, default=None, metavar="BYTES")
        parser.add_argument("--older-than", type=int, default=None, metavar="DAYS")
        parser.add_argument("--all", action="store_true")
        parser.add_argument("--limit", type=int, default=None)
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options) -> None:
        cmd_tier(
            argparse.Namespace(
                database_url=getattr(settings, "ZEROBUCKET_DATABASE_URL", None),
                image_id=options["image_id"],
                bucket=options["bucket"],
                endpoint_url=options["endpoint_url"],
                region=options["region"],
                aws_access_key_id=options["aws_access_key_id"],
                aws_secret_access_key=options["aws_secret_access_key"],
                min_size=options["min_size"],
                older_than=options["older_than"],
                all=options["all"],
                limit=options["limit"],
                dry_run=options["dry_run"],
            )
        )
