"""./manage.py zerobucket_verify -- thin wrapper around
zerobucket.cli's cmd_verify. Note that cmd_verify calls sys.exit(1) on
any checksum mismatch (and sys.exit(2) on a bad/missing database URL) --
that propagates as this management command's process exit code
unchanged, the same way it would from the standalone CLI, making this
usable in a cron job or CI step exactly like `zerobucket verify` is.
"""

from __future__ import annotations

import argparse

from django.conf import settings
from django.core.management.base import BaseCommand
from zerobucket.cli import cmd_verify


class Command(BaseCommand):
    help = "Re-checksum stored images to detect corruption."

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--sample",
            type=int,
            default=None,
            help="Check a random sample of this many images instead of every one.",
        )

    def handle(self, *args, **options) -> None:
        cmd_verify(
            argparse.Namespace(
                database_url=getattr(settings, "ZEROBUCKET_DATABASE_URL", None),
                sample=options["sample"],
            )
        )
