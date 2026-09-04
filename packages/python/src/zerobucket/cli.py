"""zerobucket CLI.

    zerobucket init      -- create the schema if it doesn't exist
    zerobucket migrate   -- apply schema migrations (currently: same as init)
    zerobucket info      -- storage stats: count, size, breakdown by format
    zerobucket verify    -- re-checksum stored images to detect corruption
    zerobucket tier      -- move image(s) into S3-compatible object storage

Uses only argparse and psycopg (both already dependencies) -- no new
dependency was added just for the CLI, consistent with keeping this
project's dependency footprint deliberately small. The one exception is
`tier`, which needs boto3 (`pip install zerobucket[s3]`) -- but that
import is deferred into ObjectStorage.__init__ the same way it is
everywhere else in this project (see object_storage.py), so running any
OTHER command still never requires boto3 to be installed.
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


def _select_tier_candidates(
    conn: psycopg.Connection, args: argparse.Namespace
) -> list[str]:
    """Bulk selection for `tier`'s --min-size/--older-than/--all filters.
    Only ever returns ids still storage_backend='postgres' -- an already-
    tiered row simply isn't a candidate, rather than being selected and
    then relying on tier_to_object_storage()'s own idempotent no-op to
    skip it silently (that no-op still exists as a safety net, but the
    query not even selecting it makes a --dry-run listing accurate)."""
    conditions = ["storage_backend = 'postgres'"]
    params: dict[str, object] = {}
    if args.min_size is not None:
        conditions.append("size_bytes >= %(min_size)s")
        params["min_size"] = args.min_size
    if args.older_than is not None:
        conditions.append(
            "created_at <= now() - (%(older_than_days)s || ' days')::interval"
        )
        params["older_than_days"] = args.older_than
    sql = f"SELECT id FROM zerobucket_images WHERE {' AND '.join(conditions)} ORDER BY created_at"
    if args.limit is not None:
        sql += " LIMIT %(limit)s"
        params["limit"] = args.limit
    sql += ";"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [str(row[0]) for row in cur.fetchall()]


def cmd_tier(args: argparse.Namespace) -> None:
    """Single-id form (`zerobucket tier IMAGE_ID --bucket ...`) is a thin
    wrapper around exactly what ZeroBucket.tier_to_object_storage() does
    from Python -- this exists so tiering one image doesn't require
    writing a Python script just to supply a bucket and credentials.

    Bulk form (--all / --min-size / --older-than) is the reference
    implementation of the "backfill script" this project's docs
    previously said callers would have to write themselves -- providing
    one here doesn't reverse that scope decision (tier_to_object_storage()
    itself still never runs automatically from put()), it just means you
    don't have to write the selection-query-plus-loop boilerplate by
    hand. Processes ids ONE AT A TIME, sequentially -- no concurrency,
    same documented tradeoff as get_many()'s handling of multiple tiered
    rows (see PostgresBackend.get_many()'s docstring): this is expected
    to be an infrequent maintenance operation, not a hot path, and each
    tier_to_object_storage() call already holds a row lock for its
    duration (see that method's docstring) -- there's no benefit to
    racing multiple of those against each other from one CLI invocation.
    """
    bulk_selection_given = (
        args.all or args.min_size is not None or args.older_than is not None
    )
    if args.image_id and bulk_selection_given:
        print(
            "Error: can't combine a single IMAGE_ID with --all/--min-size/--older-than.",
            file=sys.stderr,
        )
        sys.exit(2)
    if not args.image_id and not bulk_selection_given:
        print(
            "Error: specify either a single IMAGE_ID to tier, or a bulk "
            "selection filter (--all, --min-size, and/or --older-than).",
            file=sys.stderr,
        )
        sys.exit(2)

    url = _resolve_database_url(args)

    from .object_storage import ObjectStorage

    try:
        store = ObjectStorage(
            args.bucket,
            endpoint_url=args.endpoint_url,
            aws_access_key_id=args.aws_access_key_id,
            aws_secret_access_key=args.aws_secret_access_key,
            region_name=args.region,
        )
    except ImportError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(2)

    try:
        backend = PostgresBackend(url, object_storage=store)
    except Exception as exc:  # noqa: BLE001
        print(f"Error: could not connect: {exc}", file=sys.stderr)
        sys.exit(2)

    try:
        if args.image_id:
            candidate_ids = [args.image_id]
        else:
            with psycopg.connect(url) as select_conn:
                candidate_ids = _select_tier_candidates(select_conn, args)
            if not candidate_ids:
                print("No matching images to tier (or all already tiered).")
                return

        if args.dry_run:
            print(f"Would tier {len(candidate_ids)} image(s):")
            for image_id in candidate_ids:
                print(f"  {image_id}")
            return

        tiered = skipped = failed = 0
        total = len(candidate_ids)
        for i, image_id in enumerate(candidate_ids, 1):
            try:
                result = backend.tier_to_object_storage(image_id)
            except Exception as exc:  # noqa: BLE001
                print(f"  FAILED {image_id}: {exc}", file=sys.stderr)
                failed += 1
            else:
                if result is None:
                    print(f"  NOT FOUND {image_id}", file=sys.stderr)
                    failed += 1
                elif result is False:
                    skipped += 1
                else:
                    tiered += 1
            if total > 1 and (i % 50 == 0 or i == total):
                print(f"  {i}/{total} processed...")

        print()
        print(
            f"Tiered: {tiered}, already tiered (skipped): {skipped}, failed: {failed}"
        )
        if failed:
            sys.exit(1)
    finally:
        backend.close()


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

    p_tier = subparsers.add_parser(
        "tier",
        parents=[common],
        help="Move image(s) from Postgres into S3-compatible object storage.",
    )
    p_tier.add_argument(
        "image_id",
        nargs="?",
        default=None,
        help="Tier a single image by id. Omit and use --all/--min-size/"
        "--older-than instead for bulk selection.",
    )
    p_tier.add_argument(
        "--bucket", required=True, help="S3(-compatible) bucket -- must already exist."
    )
    p_tier.add_argument(
        "--endpoint-url",
        default=None,
        help="For non-AWS S3-compatible services (Cloudflare R2, MinIO, etc). "
        "Omit for real AWS S3.",
    )
    p_tier.add_argument(
        "--region",
        default=None,
        help="AWS region. Required by some S3-compatible services even when "
        "the region concept is meaningless to them.",
    )
    p_tier.add_argument(
        "--aws-access-key-id",
        default=None,
        help="Falls back to boto3's standard credential resolution "
        "(environment, ~/.aws/credentials, IAM role) if omitted.",
    )
    p_tier.add_argument("--aws-secret-access-key", default=None)
    p_tier.add_argument(
        "--min-size",
        type=int,
        default=None,
        metavar="BYTES",
        help="Bulk: only tier images at least this many bytes.",
    )
    p_tier.add_argument(
        "--older-than",
        type=int,
        default=None,
        metavar="DAYS",
        help="Bulk: only tier images created at least this many days ago.",
    )
    p_tier.add_argument(
        "--all",
        action="store_true",
        help="Bulk: tier every not-yet-tiered image (combine with "
        "--min-size/--older-than to narrow, or use alone for everything).",
    )
    p_tier.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Bulk: cap how many images one run processes.",
    )
    p_tier.add_argument(
        "--dry-run",
        action="store_true",
        help="List what would be tiered without actually doing it.",
    )
    p_tier.set_defaults(func=cmd_tier)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
