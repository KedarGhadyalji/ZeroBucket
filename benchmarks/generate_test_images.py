"""Generate test images at approximate target sizes for benchmarking.

Uses random-noise RGB images (noise compresses poorly and predictably,
unlike photos) and binary-searches image dimensions at a fixed JPEG
quality to land close to each target size.
"""

from __future__ import annotations

import io
import random

from PIL import Image as PILImage

TARGET_SIZES = {
    "10KB": 10 * 1024,
    "100KB": 100 * 1024,
    "500KB": 500 * 1024,
    "1MB": 1 * 1024 * 1024,
    "5MB": 5 * 1024 * 1024,
    "10MB": 10 * 1024 * 1024,
}

_QUALITY = 90


def _noise_jpeg(side: int, seed: int = 42) -> bytes:
    rng = random.Random(seed)
    img = PILImage.new("RGB", (side, side))
    # Random per-pixel noise is expensive to generate one-by-one; use a
    # small tile of random pixels and scale it up, which still compresses
    # unpredictably enough to approximate a "hard to compress" photo while
    # staying fast to generate at multi-megapixel sizes.
    tile_side = max(8, side // 16)
    tile = PILImage.new("RGB", (tile_side, tile_side))
    pixels = [
        (rng.randint(0, 255), rng.randint(0, 255), rng.randint(0, 255))
        for _ in range(tile_side * tile_side)
    ]
    tile.putdata(pixels)
    img.paste(tile.resize((side, side), PILImage.Resampling.NEAREST))

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=_QUALITY)
    return buf.getvalue()


def make_image_of_size(target_bytes: int, *, tolerance: float = 0.15) -> bytes:
    """Binary-search image side length to land within `tolerance` of target_bytes."""
    low, high = 8, 4000
    best = _noise_jpeg(high)

    # Expand upper bound if even the largest candidate is too small
    # (relevant for the 10MB target).
    while len(best) < target_bytes and high < 20000:
        high = int(high * 1.5)
        best = _noise_jpeg(high)

    for _ in range(16):
        mid = (low + high) // 2
        candidate = _noise_jpeg(mid)
        size = len(candidate)
        if abs(size - target_bytes) < abs(len(best) - target_bytes):
            best = candidate
        if size < target_bytes:
            low = mid
        else:
            high = mid
        if abs(size - target_bytes) <= target_bytes * tolerance:
            best = candidate
            break

    return best


if __name__ == "__main__":
    for label, target in TARGET_SIZES.items():
        data = make_image_of_size(target)
        actual_kb = len(data) / 1024
        target_kb = target / 1024
        print(f"{label}: target={target_kb:.0f}KB actual={actual_kb:.0f}KB")
