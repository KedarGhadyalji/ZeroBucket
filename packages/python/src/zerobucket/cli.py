"""zerobucket CLI.

    zerobucket init      -- create the schema if it doesn't exist
    zerobucket migrate   -- apply schema migrations (currently: same as init)
    zerobucket info      -- storage stats: count, size, breakdown by format
    zerobucket verify    -- re-checksum stored images to detect corruption

Uses only argparse and psycopg (both already dependencies) -- no new
dependency was added just for the CLI, consistent with keeping this
project's dependency footprint deliberately small.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys

import psycopg

from . import __version__
from .adapters.postgres import PostgresBackend


def _resolve_database_url(args: argparse.Namespace) -> str:
    url = args.database_url or os.environ.get("ZEROBUCKET_DATABASE_URL")
    if not url:
        print(
            "Error: no database URL provided. Pass --database-url or set "
            "the ZEROBUCKET_DATABASE_URL environment variable.",
            file=sys.stderr,
        )
        sys.exit(2)
    return url


def cmd_init(args: argparse.Namespace) -> None:
    url = _resolve_database_url(args)
    try:
        backend = PostgresBackend(url)  # auto_migrate=True by default
    except Exception as exc:  # noqa: BLE001
        print(f"Error: could not initialize schema: {exc}", file=sys.stderr)
        sys.exit(2)
    print("zerobucket_images table and indexes are ready.")
    backend.close()


def cmd_migrate(args: argparse.Namespace) -> None:
    # Honest limitation: there is no versioned migration system yet --
    # the schema has only ever had one shape. This currently does exactly
    # what init does. Kept as a separate command (rather than just an
    # alias) so it's a stable command name once real migrations exist.
    print(
        "Note: zerobucket does not have versioned migrations yet -- this "
        "currently just ensures the base schema exists, same as 'init'."
    )
    cmd_init(args)


def cmd_info(args: argparse.Namespace) -> None:
    url = _resolve_database_url(args)
    try:
        conn = psycopg.connect(url)
    except Exception as exc:  # noqa: BLE001
        print(f"Error: could not connect: {exc}", file=sys.stderr)
        sys.exit(2)

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('zerobucket_images');")
            if cur.fetchone()[0] is None:
                print(
                    "zerobucket_images table does not exist yet. Run 'zerobucket init' first."
                )
                sys.exit(1)

            cur.execute(
                "SELECT count(*), coalesce(sum(size_bytes), 0), min(created_at), max(created_at) "
                "FROM zerobucket_images;"
            )
            count, total_bytes, oldest, newest = cur.fetchone()

            cur.execute(
                "SELECT pg_size_pretty(pg_total_relation_size('zerobucket_images'));"
            )
            on_disk_pretty = cur.fetchone()[0]

            cur.execute(
                "SELECT mime_type, count(*), coalesce(sum(size_bytes), 0) "
                "FROM zerobucket_images GROUP BY mime_type ORDER BY count(*) DESC;"
            )
            by_format = cur.fetchall()

        print(f"zerobucket_images: {count} image(s)")
        print(f"  Total stored bytes (application-recorded): {total_bytes:,} bytes")
        print(f"  On-disk size (table + TOAST + indexes):    {on_disk_pretty}")
        if oldest is not None:
            print(f"  Oldest: {oldest}")
            print(f"  Newest: {newest}")
        if by_format:
            print("  By format:")
            for mime_type, fmt_count, fmt_bytes in by_format:
                print(
                    f"    {mime_type:<12} {fmt_count:>8} image(s)  {fmt_bytes:>14,} bytes"
                )
    finally:
        conn.close()


def cmd_verify(args: argparse.Namespace) -> None:
    url = _resolve_database_url(args)
    try:
        conn = psycopg.connect(url)
    except Exception as exc:  # noqa: BLE001
        print(f"Error: could not connect: {exc}", file=sys.stderr)
        sys.exit(2)

    mismatches: list[str] = []
    checked = 0
    try:
        with conn.cursor() as id_cur:
            if args.sample:
                id_cur.execute(
                    "SELECT id FROM zerobucket_images ORDER BY random() LIMIT %s;",
                    (args.sample,),
                )
            else:
                id_cur.execute("SELECT id FROM zerobucket_images ORDER BY created_at;")
            ids = [row[0] for row in id_cur.fetchall()]

        total = len(ids)
        if total == 0:
            print("No images to verify.")
            return

        print(f"Verifying {total} image(s)...")
        # Fetch and check one row's bytes at a time -- deliberately not
        # loading the whole table's image data into memory at once, since
        # this table can legitimately be large (see docs/OPERATIONS.md).
        for i, image_id in enumerate(ids, 1):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT data, checksum_sha256 FROM zerobucket_images WHERE id = %s;",
                    (image_id,),
                )
                row = cur.fetchone()
            if row is None:
                continue  # deleted between listing and checking -- not corruption
            data, stored_checksum = row
            actual_checksum = hashlib.sha256(bytes(data)).hexdigest()
            checked += 1
            if actual_checksum != stored_checksum:
                mismatches.append(str(image_id))
            if i % 100 == 0 or i == total:
                print(f"  {i}/{total} checked...")
    finally:
        conn.close()

    print()
    if mismatches:
        print(
            f"FAILED: {len(mismatches)} of {checked} image(s) have a checksum "
            f"mismatch (possible corruption):"
        )
        for image_id in mismatches:
            print(f"  {image_id}")
        sys.exit(1)
    else:
        print(f"OK: all {checked} image(s) verified, no checksum mismatches.")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="zerobucket",
        description="ZeroBucket CLI -- database-native image storage.",
    )
    parser.add_argument(
        "--version", action="version", version=f"zerobucket {__version__}"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--database-url",
        default=None,
        help="PostgreSQL connection string. Falls back to ZEROBUCKET_DATABASE_URL env var.",
    )

    p_init = subparsers.add_parser(
        "init",
        parents=[common],
        help="Create the zerobucket_images table and indexes if missing.",
    )
    p_init.set_defaults(func=cmd_init)

    p_migrate = subparsers.add_parser(
        "migrate",
        parents=[common],
        help="Apply schema migrations (currently: same as init).",
    )
    p_migrate.set_defaults(func=cmd_migrate)

    p_info = subparsers.add_parser(
        "info",
        parents=[common],
        help="Show storage stats: count, size, breakdown by format.",
    )
    p_info.set_defaults(func=cmd_info)

    p_verify = subparsers.add_parser(
        "verify",
        parents=[common],
        help="Re-checksum stored images to detect corruption.",
    )
    p_verify.add_argument(
        "--sample",
        type=int,
        default=None,
        help="Check a random sample of N images instead of every image.",
    )
    p_verify.set_defaults(func=cmd_verify)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
