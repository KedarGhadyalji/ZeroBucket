"""Benchmark ZeroBucket across the six target sizes from the master spec:
10KB, 100KB, 500KB, 1MB, 5MB, 10MB.

Measures, per size:
  - validation time (format sniff + decode + decompression-bomb check)
  - put() time (validation + checksum + INSERT)
  - get() time (SELECT + row materialization)
  - peak RSS delta during put() and get()
  - resulting on-disk row size in Postgres (pg_column_size)

Usage:
    ZEROBUCKET_TEST_DATABASE_URL=postgresql://... python3 benchmarks/run_benchmark.py
"""

from __future__ import annotations

import os
import statistics
import sys
import time

import psutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "packages", "python", "src"))

from generate_test_images import TARGET_SIZES, make_image_of_size  # noqa: E402

from zerobucket import ZeroBucket  # noqa: E402
from zerobucket.validation import validate_image  # noqa: E402

DATABASE_URL = os.environ.get(
    "ZEROBUCKET_TEST_DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/zerobucket_test",
)

REPEATS = 5
PROCESS = psutil.Process(os.getpid())


def _rss_mb() -> float:
    return PROCESS.memory_info().rss / (1024 * 1024)


def _time_and_memory(fn, *args, **kwargs):
    rss_before = _rss_mb()
    t0 = time.perf_counter()
    result = fn(*args, **kwargs)
    elapsed = time.perf_counter() - t0
    rss_after = _rss_mb()
    return result, elapsed, rss_after - rss_before


def benchmark_size(label: str, target_bytes: int, images: ZeroBucket) -> dict:
    data = make_image_of_size(target_bytes)
    actual_size = len(data)

    validation_times = []
    put_times = []
    get_times = []
    put_mem_deltas = []
    get_mem_deltas = []
    stored_ids = []

    for _ in range(REPEATS):
        # Isolated validation timing (not counted inside put(), measured separately
        # so we can see how much of put() latency is validation vs. the DB write).
        _, v_elapsed, _ = _time_and_memory(
            validate_image, data, max_bytes=20 * 1024 * 1024
        )
        validation_times.append(v_elapsed)

        image_id, p_elapsed, p_mem = _time_and_memory(images.put, data)
        put_times.append(p_elapsed)
        put_mem_deltas.append(p_mem)
        stored_ids.append(image_id)

        _, g_elapsed, g_mem = _time_and_memory(images.get, image_id)
        get_times.append(g_elapsed)
        get_mem_deltas.append(g_mem)

    # On-disk size as Postgres actually stores it (TOAST-compressed BYTEA
    # reports its compressed size via pg_column_size).
    with images._backend._pool.connection() as conn, conn.cursor() as cur:  # noqa: SLF001
        cur.execute(
            "SELECT pg_column_size(data) FROM zerobucket_images WHERE id = %s",
            (stored_ids[-1],),
        )
        on_disk_bytes = cur.fetchone()[0]

    for image_id in stored_ids:
        images.delete(image_id)

    return {
        "label": label,
        "actual_size_kb": actual_size / 1024,
        "on_disk_kb": on_disk_bytes / 1024,
        "validation_ms": statistics.median(validation_times) * 1000,
        "put_ms": statistics.median(put_times) * 1000,
        "get_ms": statistics.median(get_times) * 1000,
        "put_mem_mb": statistics.median(put_mem_deltas),
        "get_mem_mb": statistics.median(get_mem_deltas),
    }


def main() -> None:
    images = ZeroBucket(database_url=DATABASE_URL, max_bytes=20 * 1024 * 1024)
    with images._backend._pool.connection() as conn, conn.cursor() as cur:  # noqa: SLF001
        cur.execute("TRUNCATE TABLE zerobucket_images;")

    rows = []
    for label, target in TARGET_SIZES.items():
        print(f"Benchmarking {label}...", file=sys.stderr)
        rows.append(benchmark_size(label, target, images))

    images.close()

    header = (
        f"{'Size':<8} {'Actual':>9} {'On-disk':>9} {'Validate':>10} "
        f"{'Put':>9} {'Get':>9} {'PutMem':>9} {'GetMem':>9}"
    )
    print(header)
    print("-" * len(header))
    for r in rows:
        print(
            f"{r['label']:<8} "
            f"{r['actual_size_kb']:>7.0f}KB "
            f"{r['on_disk_kb']:>7.0f}KB "
            f"{r['validation_ms']:>8.1f}ms "
            f"{r['put_ms']:>7.1f}ms "
            f"{r['get_ms']:>7.1f}ms "
            f"{r['put_mem_mb']:>7.1f}MB "
            f"{r['get_mem_mb']:>7.1f}MB"
        )

    # Write machine-readable results too.
    import csv

    out_path = os.path.join(os.path.dirname(__file__), "results.csv")
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nWrote {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
