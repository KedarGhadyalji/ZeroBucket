"""Procedurally generated test images that approximate real photo statistics.

Pure random noise (used in benchmarks/generate_test_images.py) is a
worst-case for compression -- it has no spatial structure, so JPEG/WebP
can't exploit smoothness or repetition at all. Real photos are mostly
smooth gradients and repeated texture with localized detail, which is
exactly what these generators produce, without pulling in any externally
licensed images.

All images are generated with numpy for speed at multi-megapixel sizes.
"""

from __future__ import annotations

import io

import numpy as np
from PIL import Image as PILImage


def gradient_landscape(size: tuple[int, int] = (1600, 1200), seed: int = 1) -> bytes:
    """A sky-to-ground gradient with a horizon and a few soft shapes.

    Approximates a landscape photo: large smooth regions (sky) that
    compress very well, plus a lower-detail "ground" band. This is the
    kind of image that shows the biggest gains from quality reduction.
    """
    rng = np.random.default_rng(seed)
    w, h = size
    horizon = int(h * 0.6)

    img = np.zeros((h, w, 3), dtype=np.uint8)

    # Sky: vertical gradient blue -> pale, with very slight horizontal noise
    # (avoids unrealistically perfect banding). Noise amplitude is kept low
    # deliberately -- this approximates a camera's own in-camera JPEG
    # output, which has already had sensor noise reduced, not a raw
    # sensor capture. Heavier synthetic noise here would be closer to
    # high-ISO grain, which genuinely doesn't survive lossy recompression
    # well regardless of quality setting (see RESULTS.md).
    sky_top = np.array([80, 130, 220])
    sky_bottom = np.array([200, 220, 245])
    t = np.linspace(0, 1, horizon).reshape(-1, 1, 1)
    sky = sky_top * (1 - t) + sky_bottom * t
    sky = sky + rng.normal(0, 0.6, size=(horizon, w, 3))
    img[:horizon] = np.clip(sky, 0, 255).astype(np.uint8)

    # Ground: darker gradient with more texture noise (grass-like), still
    # calibrated to a realistic already-compressed-photo noise floor.
    ground_top = np.array([90, 130, 60])
    ground_bottom = np.array([60, 90, 40])
    t2 = np.linspace(0, 1, h - horizon).reshape(-1, 1, 1)
    ground = ground_top * (1 - t2) + ground_bottom * t2
    ground = ground + rng.normal(0, 1.5, size=(h - horizon, w, 3))
    img[horizon:] = np.clip(ground, 0, 255).astype(np.uint8)

    pil_img = PILImage.fromarray(img, mode="RGB")
    buf = io.BytesIO()
    pil_img.save(buf, format="JPEG", quality=97)  # near-source-quality "original"
    return buf.getvalue()


def textured_portrait(size: tuple[int, int] = (1200, 1600), seed: int = 2) -> bytes:
    """A soft radial (vignette-style) gradient with fine texture noise.

    Approximates a shallow-depth-of-field portrait: a smooth, bright
    center falling off into a noisier, darker edge. This is a harder
    case for compression than a flat gradient: smooth regions sit right
    next to higher-frequency detail, which is where quality-reduction
    artifacts (blocking, ringing) tend to show up first.
    """
    rng = np.random.default_rng(seed)
    w, h = size
    yy, xx = np.mgrid[0:h, 0:w]
    cx, cy = w / 2, h * 0.4
    dist = np.sqrt((xx - cx) ** 2 + (yy - cy) ** 2)
    dist_norm = dist / dist.max()

    base = np.array([220, 180, 160])  # skin-tone-ish base
    edge = np.array([60, 50, 55])  # dark background falloff
    t = np.clip(dist_norm * 1.4, 0, 1).reshape(h, w, 1)
    img = base * (1 - t) + edge * t

    # Fine texture noise layered on top, stronger near the "hairline" band.
    # Calibrated to a realistic already-compressed-photo noise floor, same
    # reasoning as gradient_landscape above.
    fine_noise = rng.normal(0, 1.2, size=(h, w, 3))
    band = np.exp(-((dist_norm - 0.3) ** 2) / 0.02).reshape(h, w, 1)
    img = img + fine_noise * (1 + band * 2)

    img = np.clip(img, 0, 255).astype(np.uint8)
    pil_img = PILImage.fromarray(img, mode="RGB")
    buf = io.BytesIO()
    pil_img.save(buf, format="JPEG", quality=97)
    return buf.getvalue()


def busy_texture(size: tuple[int, int] = (1200, 1200), seed: int = 3) -> bytes:
    """Dense, repeated high-frequency texture, approximating foliage/gravel/fabric.

    This is the worst case for compression -- lots of genuine
    high-frequency detail that resists compression the most, and where
    an overly aggressive quality setting will show artifacts first.
    """
    rng = np.random.default_rng(seed)
    w, h = size

    # Layered noise at a few frequencies, summed, to approximate the
    # 1/f-ish spectrum of natural texture rather than flat white noise.
    img = np.zeros((h, w, 3), dtype=np.float64)
    for scale, weight in [(4, 0.5), (16, 0.3), (64, 0.15), (256, 0.05)]:
        small = rng.normal(128, 40, size=(max(1, h // scale), max(1, w // scale), 3))
        layer = np.array(
            PILImage.fromarray(np.clip(small, 0, 255).astype(np.uint8)).resize(
                (w, h), PILImage.Resampling.BICUBIC
            )
        ).astype(np.float64)
        img += layer * weight

    img = np.clip(img, 0, 255).astype(np.uint8)
    pil_img = PILImage.fromarray(img, mode="RGB")
    buf = io.BytesIO()
    pil_img.save(buf, format="JPEG", quality=97)
    return buf.getvalue()


def flat_graphic(size: tuple[int, int] = (800, 600), seed: int = 4) -> bytes:
    """Flat-color shapes with sharp edges, approximating a UI screenshot or logo.

    PNG's actual use case -- large flat regions and hard edges, where
    lossless compression works well and lossy JPEG would introduce
    visible ringing around the sharp edges.
    """
    rng = np.random.default_rng(seed)
    w, h = size
    img = np.full((h, w, 3), 245, dtype=np.uint8)  # near-white background

    # A few flat-colored rectangles, hard edges.
    for _ in range(6):
        x0, y0 = rng.integers(0, w - 50), rng.integers(0, h - 50)
        bw, bh = rng.integers(40, w // 3), rng.integers(30, h // 3)
        color = rng.integers(0, 200, size=3)
        x1, y1 = min(w, x0 + bw), min(h, y0 + bh)
        img[y0:y1, x0:x1] = color

    pil_img = PILImage.fromarray(img, mode="RGB")
    buf = io.BytesIO()
    pil_img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


ALL_FIXTURES = {
    "gradient_landscape": gradient_landscape,
    "textured_portrait": textured_portrait,
    "busy_texture": busy_texture,
    "flat_graphic": flat_graphic,
}
