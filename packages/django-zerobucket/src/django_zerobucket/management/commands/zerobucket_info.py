"""./manage.py zerobucket_info -- thin wrapper around zerobucket.cli's
cmd_info, reusing that exact, already-tested implementation rather than
reimplementing it. Supplies database_url from the
ZEROBUCKET_DATABASE_URL Django setting (falling back further to the
ZEROBUCKET_DATABASE_URL environment variable, same as the standalone
CLI, since cmd_info's own _resolve_database_url already does that) so
you don't have to pass --database-url by hand in a Django project that
already has it configured.
"""

from __future__ import annotations

import argparse

from django.conf import settings
from django.core.management.base import BaseCommand
from zerobucket.cli import cmd_info


class Command(BaseCommand):
    help = "Report ZeroBucket storage stats: image count, total size, format breakdown."

    def handle(self, *args, **options) -> None:
        cmd_info(
            argparse.Namespace(
                database_url=getattr(settings, "ZEROBUCKET_DATABASE_URL", None)
            )
        )
