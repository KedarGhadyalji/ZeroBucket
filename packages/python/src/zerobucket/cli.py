"""zerobucket CLI.

    zerobucket init      -- create the schema if it doesn't exist
    zerobucket migrate   -- apply schema migrations (currently: same as init);
                            --to-dedup copies classic -> dedup (PostgreSQL only)
    zerobucket info      -- storage stats: count, size, breakdown by format
    zerobucket verify    -- re-checksum stored images to detect corruption
    zerobucket tier      -- move image(s) into S3-compatible object storage

Works on PostgreSQL, SQLite AND MySQL/MariaDB, in classic AND dedup mode, the
same as the npm package's CLI:

    --database-url URL   PostgreSQL (postgresql://) or MySQL/MariaDB (mysql://,
                         mariadb://)             (env: ZEROBUCKET_DATABASE_URL)
    --sqlite PATH        a SQLite database file  (env: ZEROBUCKET_SQLITE_PATH)
    --dedup              operate on the dedup-mode tables instead of the classic one

Exit codes: 0 success, 1 problems found or an operation failed, 2 usage or
connection error.

Uses only argparse (plus the drivers the package already depends on) -- no new
dependency was added just for the CLI. MySQL support needs `zerobucket[mysql]`
and `tier` / `verify --bucket` need boto3 (`pip install zerobucket[s3]`); both
are imported only when used, so every other command still works without them.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sys
from typing import NamedTuple, NoReturn

import psycopg

from . import __version__
from .adapters.base import StorageBackend


class _Target(NamedTuple):
    kind: str  # "postgres", "mysql" or "sqlite"
    value: str  # a connection URL, or a file path for sqlite


def _opt(args: argparse.Namespace, name: str, default=None):
    """Read an optional argument. Tests (and any caller) may build a Namespace
    that predates an option, so a missing attribute means "not given"."""
    return getattr(args, name, default)


def _fail(message: str, code: int = 2) -> NoReturn:
    print(message, file=sys.stderr)
    sys.exit(code)


def _url_target(url: str) -> _Target:
    """mysql:// and mariadb:// select MySQL/MariaDB; anything else is PostgreSQL."""
    scheme = url.split("://", 1)[0].lower() if "://" in url else ""
    return _Target("mysql" if scheme in ("mysql", "mariadb") else "postgres", url)


def _resolve_target(args: argparse.Namespace) -> _Target:
    sqlite_path = _opt(args, "sqlite")
    url = _opt(args, "database_url")
    if sqlite_path and url:
        _fail("Error: choose one database: --sqlite OR --database-url, not both.")
    if sqlite_path:
        return _Target("sqlite", sqlite_path)
    if url:
        return _url_target(url)
    env_url = os.environ.get("ZEROBUCKET_DATABASE_URL")
    env_path = os.environ.get("ZEROBUCKET_SQLITE_PATH")
    if env_url and env_path:
        _fail(
            "Error: both ZEROBUCKET_DATABASE_URL and ZEROBUCKET_SQLITE_PATH are set. "
            "Pass --database-url or --sqlite to choose."
        )
    if env_url:
        return _url_target(env_url)
    if env_path:
        return _Target("sqlite", env_path)
    _fail(
        "Error: no database URL provided. Pass --database-url (PostgreSQL or "
        "MySQL/MariaDB) or --sqlite PATH, or set the ZEROBUCKET_DATABASE_URL / "
        "ZEROBUCKET_SQLITE_PATH environment variable."
    )


def _resolve_database_url(args: argparse.Namespace) -> str:
    """Kept for callers that only ever dealt with a URL."""
    return _resolve_target(args).value


def _require_sqlite_file(target: _Target, hint: str) -> None:
    if (
        target.kind == "sqlite"
        and target.value != ":memory:"
        and not os.path.exists(target.value)
    ):
        print(f"SQLite database file {target.value!r} does not exist yet. {hint}")
        sys.exit(1)


def _probe(target: _Target, what: str) -> None:
    """Fail fast (exit 2) on a connection problem, instead of waiting out a
    connection pool's timeout. SQLite has nothing to probe."""
    try:
        if target.kind == "postgres":
            psycopg.connect(target.value, connect_timeout=10).close()
        elif target.kind == "mysql":
            import pymysql

            from .adapters.mysql import _parse_database_url

            pymysql.connect(
                **_parse_database_url(target.value), connect_timeout=10
            ).close()
    except ImportError as exc:
        _fail(
            f"Error: MySQL/MariaDB support is not installed ({exc}). "
            "Run: pip install zerobucket[mysql]"
        )
    except Exception as exc:  # noqa: BLE001
        _fail(f"Error: could not {what}: {exc}")


def _open_backend(
    target: _Target,
    *,
    dedup: bool,
    auto_migrate: bool,
    object_storage=None,
    what: str = "connect",
) -> StorageBackend:
    """Open the backend for `target`. Retries are off: a CLI should report a
    failure promptly, not sit through backoff delays."""
    _probe(target, what)
    try:
        if target.kind == "sqlite":
            from .adapters.sqlite import SQLiteBackend

            return SQLiteBackend(
                target.value,
                dedup=dedup,
                auto_migrate=auto_migrate,
                object_storage=object_storage,
                max_retries=0,
            )
        if target.kind == "mysql":
            from .adapters.mysql import MySQLBackend

            return MySQLBackend(
                target.value,
                dedup=dedup,
                auto_migrate=auto_migrate,
                object_storage=object_storage,
                max_retries=0,
            )
        from .adapters.postgres import PostgresBackend

        return PostgresBackend(
            target.value,
            dedup=dedup,
            auto_migrate=auto_migrate,
            object_storage=object_storage,
            max_retries=0,
        )
    except Exception as exc:  # noqa: BLE001
        _fail(f"Error: could not {what}: {exc}")


def _num(n: int) -> str:
    return f"{n:,}"


def _build_object_storage(args: argparse.Namespace):
    """An ObjectStorage from the --bucket/--endpoint-url/... options (pure
    argument handling plus a lazy boto3 import; no network call)."""
    bucket = _opt(args, "bucket")
    if not bucket:
        _fail("Error: --bucket is required.")
    key_id = _opt(args, "aws_access_key_id")
    secret = _opt(args, "aws_secret_access_key")
    if bool(key_id) != bool(secret):
        _fail(
            "Error: pass both --aws-access-key-id and --aws-secret-access-key, or neither."
        )
    from .object_storage import ObjectStorage

    try:
        return ObjectStorage(
            bucket,
            endpoint_url=_opt(args, "endpoint_url"),
            aws_access_key_id=key_id,
            aws_secret_access_key=secret,
            region_name=_opt(args, "region"),
        )
    except ImportError as exc:
        _fail(f"Error: {exc}")


def _at_least(args: argparse.Namespace, name: str, minimum: int) -> int | None:
    value = _opt(args, name)
    if value is None:
        return None
    if value < minimum:
        _fail(f"Error: --{name.replace('_', '-')} must be an integer >= {minimum}.")
    return value


# ---- commands -----------------------------------------------------------------


def cmd_init(args: argparse.Namespace) -> None:
    target = _resolve_target(args)
    dedup = bool(_opt(args, "dedup"))
    backend = _open_backend(
        target, dedup=dedup, auto_migrate=True, what="initialize schema"
    )
    print(
        "zerobucket_blobs and zerobucket_image_refs tables and indexes are ready."
        if dedup
        else "zerobucket_images table and indexes are ready."
    )
    backend.close()


def _migrate_to_dedup(args: argparse.Namespace) -> None:
    target = _resolve_target(args)
    if target.kind != "postgres":
        _fail("Error: --to-dedup is only supported for PostgreSQL.")
    skip_tiered = bool(_opt(args, "skip_tiered"))

    # Refuse up front when classic rows are tiered, unless told to skip them:
    # their bytes are not in the database, and dedup mode has no tiering.
    classic = _open_backend(target, dedup=False, auto_migrate=False)
    try:
        info = classic.get_info()
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}")
        sys.exit(1)
    finally:
        classic.close()
    tiered_count = info.tiered.count if info and info.tiered else 0
    if tiered_count > 0 and not skip_tiered:
        print(
            f"Error: {tiered_count} classic image(s) are tiered to object storage, so their "
            "bytes are not in the database to migrate. Dedup mode does not support tiering. "
            "Migrate only an un-tiered table, bring those images back first, or pass "
            "--skip-tiered to skip them and migrate the rest."
        )
        sys.exit(1)

    from .adapters.postgres import migrate_classic_to_dedup

    backend = _open_backend(target, dedup=True, auto_migrate=True)
    try:
        result = migrate_classic_to_dedup(backend)
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}")
        sys.exit(1)
    finally:
        backend.close()
    print(
        f"Migrated {_num(result['images_migrated'])} image(s) into "
        f"{_num(result['distinct_blobs_created'])} distinct blob(s)."
    )
    print(f"  Duplicate references found: {_num(result['duplicate_references_found'])}")
    print(f"  Already migrated (skipped): {_num(result['already_migrated_skipped'])}")
    if result["tiered_rows_skipped"] > 0:
        print(
            "  Tiered to object storage (skipped, NOT in the dedup tables): "
            f"{_num(result['tiered_rows_skipped'])}"
        )
    print("The classic table was not modified.")


def cmd_migrate(args: argparse.Namespace) -> None:
    to_dedup = bool(_opt(args, "to_dedup"))
    if _opt(args, "skip_tiered") and not to_dedup:
        _fail("Error: --skip-tiered only applies with --to-dedup.")
    if to_dedup:
        _migrate_to_dedup(args)
        return
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
    target = _resolve_target(args)
    dedup = bool(_opt(args, "dedup"))
    _require_sqlite_file(target, "Run 'zerobucket init --sqlite PATH' first.")
    backend = _open_backend(target, dedup=dedup, auto_migrate=False)
    try:
        try:
            info = backend.get_info()
        except Exception as exc:  # noqa: BLE001
            _fail(f"Error: could not read storage info: {exc}", 1)
        if info is None:
            print(
                "zerobucket_blobs / zerobucket_image_refs tables do not exist yet. "
                "Run 'zerobucket init --dedup' first."
                if dedup
                else "zerobucket_images table does not exist yet. Run 'zerobucket init' first."
            )
            sys.exit(1)
        if info.mode == "dedup":
            print(
                f"zerobucket_image_refs: {_num(info.count)} image(s) sharing "
                f"{_num(info.dedup.blobs)} distinct blob(s)"
            )
        else:
            print(f"zerobucket_images: {_num(info.count)} image(s)")
        print(
            f"  Total stored bytes (application-recorded): {_num(info.total_bytes)} bytes"
        )
        if info.dedup is not None:
            saved = info.total_bytes - info.dedup.stored_bytes
            pct = (
                f"{saved / info.total_bytes * 100:.1f}"
                if info.total_bytes > 0
                else "0.0"
            )
            print(
                f"  Physical bytes after dedup:                "
                f"{_num(info.dedup.stored_bytes)} bytes (saved {_num(saved)} bytes, {pct}%)"
            )
        if info.on_disk:
            label = {
                "postgres": "On-disk size (table + TOAST + indexes):   ",
                "mysql": "On-disk size (data + indexes, approximate):",
                "sqlite": "On-disk size (database file + WAL):       ",
            }[target.kind]
            print(f"  {label} {info.on_disk}")
        if info.tiered is not None and info.tiered.count > 0:
            print(
                f"  Tiered to object storage:                  {_num(info.tiered.count)} "
                f"image(s), {_num(info.tiered.bytes)} bytes (not in the database)"
            )
        if info.oldest is not None:
            print(f"  Oldest: {info.oldest}")
            print(f"  Newest: {info.newest}")
        if info.by_format:
            print("  By format:")
            for fmt in info.by_format:
                print(
                    f"    {fmt.mime_type:<12} {fmt.count:>8} image(s)  {_num(fmt.bytes):>14} bytes"
                )
    finally:
        backend.close()


def cmd_verify(args: argparse.Namespace) -> None:
    sample = _at_least(args, "sample", 1)
    dedup = bool(_opt(args, "dedup"))
    store = None
    if _opt(args, "bucket"):
        if dedup:
            _fail(
                "Error: --bucket does not apply with --dedup (dedup mode has no tiering)."
            )
        store = _build_object_storage(args)
    target = _resolve_target(args)
    _require_sqlite_file(target, "Nothing to verify.")
    backend = _open_backend(
        target, dedup=dedup, auto_migrate=False, object_storage=store
    )

    mismatches: list[str] = []
    errors: list[str] = []
    checked = 0
    tiered_skipped = 0
    try:
        try:
            entries = backend.list_images(sample=sample)
        except Exception as exc:  # noqa: BLE001
            print(f"Error: {exc} (has 'zerobucket init' been run?)")
            sys.exit(1)
        total = len(entries)
        if total == 0:
            print("No images to verify.")
            return
        print(f"Verifying {_num(total)} image(s)...")
        # One image at a time, deliberately: this table can legitimately be
        # large (see docs/OPERATIONS.md), so never hold more than one image's
        # bytes in memory.
        for i, entry in enumerate(entries, 1):
            if entry.tiered and store is None:
                tiered_skipped += 1
            else:
                try:
                    record = backend.get(entry.id)
                    if (
                        record is not None
                    ):  # None: deleted since listing, not corruption
                        checked += 1
                        actual = hashlib.sha256(bytes(record.data)).hexdigest()
                        if actual != record.checksum_sha256.strip():
                            mismatches.append(entry.id)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"{entry.id}: {exc}")
            if i % 100 == 0 or i == total:
                print(f"  {i}/{total} checked...")
    finally:
        backend.close()

    print()
    if mismatches:
        print(
            f"FAILED: {len(mismatches)} of {checked} image(s) have a checksum "
            "mismatch (possible corruption):"
        )
        for image_id in mismatches:
            print(f"  {image_id}")
    if errors:
        print(f"ERROR: could not read {len(errors)} image(s):")
        for line in errors:
            print(f"  {line}")
    if tiered_skipped:
        print(
            f"Note: {tiered_skipped} image(s) tiered to object storage were skipped. "
            "Pass --bucket (and credentials) to verify them too."
        )
    if mismatches or errors:
        sys.exit(1)
    print(f"OK: all {checked} image(s) verified, no checksum mismatches.")


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
    tier_to_object_storage() call already holds a lock for its duration
    (see that method's docstring) -- there's no benefit to racing
    multiple of those against each other from one CLI invocation.
    """
    image_id = _opt(args, "image_id")
    min_size = _at_least(args, "min_size", 0)
    older_than = _at_least(args, "older_than", 0)
    limit = _at_least(args, "limit", 1)
    bulk_selection_given = (
        bool(_opt(args, "all")) or min_size is not None or older_than is not None
    )
    if image_id and bulk_selection_given:
        _fail(
            "Error: can't combine a single IMAGE_ID with --all/--min-size/--older-than."
        )
    if not image_id and not bulk_selection_given:
        _fail(
            "Error: specify either a single IMAGE_ID to tier, or a bulk "
            "selection filter (--all, --min-size, and/or --older-than)."
        )
    if _opt(args, "dedup"):
        _fail(
            "Error: tier is not supported with --dedup (a deduplicated blob "
            "can be shared by many images)."
        )

    store = _build_object_storage(args)  # argument checks first, before any network I/O
    target = _resolve_target(args)
    _require_sqlite_file(target, "Nothing to tier.")
    backend = _open_backend(
        target, dedup=False, auto_migrate=False, object_storage=store
    )

    try:
        if image_id:
            candidate_ids = [image_id]
        else:
            candidate_ids = [
                e.id
                for e in backend.list_images(
                    min_size=min_size,
                    older_than_days=older_than,
                    only_untiered=True,
                    limit=limit,
                )
            ]
            if not candidate_ids:
                print("No matching images to tier (or all already tiered).")
                return

        if _opt(args, "dry_run"):
            print(f"Would tier {len(candidate_ids)} image(s):")
            for cid in candidate_ids:
                print(f"  {cid}")
            return

        tiered = skipped = failed = 0
        total = len(candidate_ids)
        for i, cid in enumerate(candidate_ids, 1):
            try:
                result = backend.tier_to_object_storage(cid)
            except Exception as exc:  # noqa: BLE001
                print(f"  FAILED {cid}: {exc}", file=sys.stderr)
                failed += 1
            else:
                if result is None:
                    print(f"  NOT FOUND {cid}", file=sys.stderr)
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


def _add_object_storage_options(
    parser: argparse.ArgumentParser, *, required: bool
) -> None:
    parser.add_argument(
        "--bucket",
        required=required,
        default=None,
        help=(
            "S3(-compatible) bucket -- must already exist."
            if required
            else "Also verify images tiered to this S3(-compatible) bucket."
        ),
    )
    parser.add_argument(
        "--endpoint-url",
        default=None,
        help="For non-AWS S3-compatible services (Cloudflare R2, MinIO, etc). "
        "Omit for real AWS S3.",
    )
    parser.add_argument(
        "--region",
        default=None,
        help="AWS region. Required by some S3-compatible services even when "
        "the region concept is meaningless to them.",
    )
    parser.add_argument(
        "--aws-access-key-id",
        default=None,
        help="Falls back to boto3's standard credential resolution "
        "(environment, ~/.aws/credentials, IAM role) if omitted.",
    )
    parser.add_argument("--aws-secret-access-key", default=None)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="zerobucket",
        description="ZeroBucket CLI -- database-native image storage "
        "(PostgreSQL, SQLite, MySQL/MariaDB).",
        epilog="Exit codes: 0 success, 1 problems found or an operation failed, "
        "2 usage or connection error.",
    )
    parser.add_argument(
        "--version", action="version", version=f"zerobucket {__version__}"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--database-url",
        default=None,
        help="PostgreSQL (postgresql://) or MySQL/MariaDB (mysql://, mariadb://) "
        "connection string. Falls back to the ZEROBUCKET_DATABASE_URL env var.",
    )
    common.add_argument(
        "--sqlite",
        default=None,
        metavar="PATH",
        help="SQLite database file. Falls back to the ZEROBUCKET_SQLITE_PATH env var.",
    )
    common.add_argument(
        "--dedup",
        action="store_true",
        help="Operate on the dedup-mode tables instead of the classic table.",
    )

    p_init = subparsers.add_parser(
        "init",
        parents=[common],
        help="Create the schema (tables and indexes) if missing.",
    )
    p_init.set_defaults(func=cmd_init)

    p_migrate = subparsers.add_parser(
        "migrate",
        parents=[common],
        help="Apply schema migrations (currently: same as init).",
    )
    p_migrate.add_argument(
        "--to-dedup",
        action="store_true",
        help="Copy every classic image into the dedup tables (PostgreSQL only; non-destructive).",
    )
    p_migrate.add_argument(
        "--skip-tiered",
        action="store_true",
        help="With --to-dedup: skip images tiered to object storage instead of refusing.",
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
    _add_object_storage_options(p_verify, required=False)
    p_verify.set_defaults(func=cmd_verify)

    p_tier = subparsers.add_parser(
        "tier",
        parents=[common],
        help="Move image(s) from the database into S3-compatible object storage.",
    )
    p_tier.add_argument(
        "image_id",
        nargs="?",
        default=None,
        help="Tier a single image by id. Omit and use --all/--min-size/"
        "--older-than instead for bulk selection.",
    )
    _add_object_storage_options(p_tier, required=True)
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
