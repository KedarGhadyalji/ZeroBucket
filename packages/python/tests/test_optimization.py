"""Tests for zerobucket.optimization.

The SSIM tests are the important ones here: they turn "these defaults
look fine" from an assumption into an enforced regression check. If a
future change to defaults or the encode pipeline silently drops quality,
these tests fail -- they don't just measure file size.

SSIM_FLOOR_BY_FIXTURE is deliberately NOT one blanket number. Measured
data (see benchmarks/COMPRESSION_RESULTS.md) showed that smooth/flat
content (gradient_landscape, flat_graphic) comfortably clears a strict
0.98 "visually lossless" bar, while content with genuine dense fine
texture (textured_portrait, busy_texture) tops out lower -- around
0.95-0.97 -- regardless of quality setting, because that detail is close
to incompressible. Holding every fixture to the same 0.98 floor produced
false-looking failures on the two harder fixtures even though the
*compressor* wasn't doing anything wrong; the honest fix was different
floors per content type, each with a small margin below its measured
ceiling, not one number that doesn't fit all content.
"""

from __future__ import annotations

import io

import numpy as np
import pytest
from PIL import Image as PILImage
from skimage.metrics import structural_similarity as ssim

from zerobucket.exceptions import ImageTooLargeError, ImageValidationError
from zerobucket.optimization import (
    DEFAULT_JPEG_QUALITY,
    DEFAULT_WEBP_QUALITY,
    optimize_image,
)

from .photo_fixtures import ALL_FIXTURES

# Per-fixture floor, set with a small margin below the measured SSIM at
# our default quality settings (see COMPRESSION_RESULTS.md for the raw
# sweep this came from). "smooth" content is held to a strict, genuinely
# visually-lossless bar; "textured" content is held to the honest
# achievable ceiling for that kind of detail.
SSIM_FLOOR_BY_FIXTURE = {
    "gradient_landscape": 0.98,
    "flat_graphic": 0.98,
    "textured_portrait": 0.94,
    "busy_texture": 0.92,
}

_MAX_BYTES = 20 * 1024 * 1024


def _decode_rgb(data: bytes) -> np.ndarray:
    with PILImage.open(io.BytesIO(data)) as img:
        return np.array(img.convert("RGB"))


def _compute_ssim(original: bytes, optimized: bytes) -> float:
    orig_arr = _decode_rgb(original)
    opt_arr = _decode_rgb(optimized)

    if orig_arr.shape != opt_arr.shape:
        # optimize_image may have resized; compare at the optimized
        # resolution by downscaling the original the same way, so SSIM
        # measures re-encoding quality, not resolution difference.
        opt_h, opt_w = opt_arr.shape[:2]
        orig_img = PILImage.fromarray(orig_arr).resize(
            (opt_w, opt_h), PILImage.Resampling.LANCZOS
        )
        orig_arr = np.array(orig_img)

    return ssim(orig_arr, opt_arr, channel_axis=-1, data_range=255)


@pytest.mark.parametrize("fixture_name", list(ALL_FIXTURES.keys()))
def test_default_jpeg_quality_meets_content_appropriate_floor(fixture_name):
    fixture_fn = ALL_FIXTURES[fixture_name]
    original = fixture_fn()
    floor = SSIM_FLOOR_BY_FIXTURE[fixture_name]

    result = optimize_image(original, target_format="jpeg", max_bytes=_MAX_BYTES)

    score = _compute_ssim(original, result.data)
    assert score >= floor, (
        f"{fixture_name}: SSIM {score:.4f} fell below its floor of {floor} "
        f"at default JPEG quality={DEFAULT_JPEG_QUALITY}"
    )


@pytest.mark.parametrize("fixture_name", list(ALL_FIXTURES.keys()))
def test_default_webp_quality_meets_content_appropriate_floor(fixture_name):
    fixture_fn = ALL_FIXTURES[fixture_name]
    original = fixture_fn()
    floor = SSIM_FLOOR_BY_FIXTURE[fixture_name]

    result = optimize_image(original, target_format="webp", max_bytes=_MAX_BYTES)

    score = _compute_ssim(original, result.data)
    assert score >= floor, (
        f"{fixture_name}: SSIM {score:.4f} fell below its floor of {floor} "
        f"at default WebP quality={DEFAULT_WEBP_QUALITY}"
    )


def test_low_quality_actually_degrades_ssim():
    """Sanity check for the test methodology itself: a deliberately bad
    quality setting MUST score noticeably worse than the default. If this
    test ever fails, the SSIM harness above isn't actually measuring
    anything meaningful."""
    original = ALL_FIXTURES["busy_texture"]()

    good = optimize_image(
        original,
        target_format="jpeg",
        quality=DEFAULT_JPEG_QUALITY,
        max_bytes=_MAX_BYTES,
    )
    bad = optimize_image(
        original, target_format="jpeg", quality=15, max_bytes=_MAX_BYTES
    )

    good_score = _compute_ssim(original, good.data)
    bad_score = _compute_ssim(original, bad.data)

    assert bad_score < good_score
    assert bad_score < SSIM_FLOOR_BY_FIXTURE["busy_texture"], (
        "Expected quality=15 to score clearly worse than our default -- if "
        "it didn't, the busy_texture fixture may not be a hard enough case."
    )


def test_optimization_reduces_file_size():
    original = ALL_FIXTURES["gradient_landscape"]()
    result = optimize_image(original, target_format="jpeg", max_bytes=_MAX_BYTES)

    assert result.size_bytes < result.original_size_bytes
    assert result.bytes_saved > 0
    assert result.percent_saved > 0


def test_metadata_is_stripped():
    """Re-saving through Pillow with no info dict passed through should
    drop EXIF, even when optimize_image doesn't change format or quality."""
    img = PILImage.new("RGB", (100, 100), color=(200, 100, 50))
    exif = img.getexif()
    exif[0x010E] = "Test image description"  # ImageDescription tag
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=95, exif=exif)
    original = buf.getvalue()

    # Confirm the EXIF actually made it into the original first, or this
    # test would trivially pass for the wrong reason.
    with PILImage.open(io.BytesIO(original)) as check:
        assert check.getexif().get(0x010E) == "Test image description"

    result = optimize_image(original, target_format="jpeg", max_bytes=_MAX_BYTES)

    with PILImage.open(io.BytesIO(result.data)) as optimized_img:
        assert optimized_img.getexif().get(0x010E) is None


def test_max_width_resizes_and_preserves_aspect_ratio():
    original = ALL_FIXTURES["gradient_landscape"]()  # 1600x1200
    result = optimize_image(original, max_width=800, max_bytes=_MAX_BYTES)

    assert result.width == 800
    # 1600x1200 has a 4:3 ratio; 800-wide should be 600 tall.
    assert result.height == 600


def test_no_resize_when_already_narrower_than_max_width():
    original = ALL_FIXTURES["gradient_landscape"]()  # 1600 wide
    result = optimize_image(original, max_width=3000, max_bytes=_MAX_BYTES)

    assert result.width == 1600


def test_png_ignores_quality_but_still_processes():
    """PNG has no lossy quality knob -- quality= should be a silent no-op,
    not an error, and the output should still be a valid, smaller-or-equal
    PNG."""
    original = ALL_FIXTURES["flat_graphic"]()
    result = optimize_image(
        original, target_format="png", quality=10, max_bytes=_MAX_BYTES
    )
    assert result.mime_type == "image/png"
    assert result.size_bytes <= result.original_size_bytes


def test_png_to_webp_conversion():
    original = ALL_FIXTURES["flat_graphic"]()
    result = optimize_image(original, target_format="webp", max_bytes=_MAX_BYTES)
    assert result.mime_type == "image/webp"


def test_rgba_png_converts_to_jpeg_without_error():
    """JPEG has no alpha channel -- converting an RGBA source must not crash."""
    img = PILImage.new("RGBA", (100, 100), color=(200, 100, 50, 128))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    original = buf.getvalue()

    result = optimize_image(original, target_format="jpeg", max_bytes=_MAX_BYTES)
    assert result.mime_type == "image/jpeg"


def test_unsupported_target_format_rejected():
    original = ALL_FIXTURES["flat_graphic"]()
    with pytest.raises(ImageValidationError):
        optimize_image(original, target_format="gif", max_bytes=_MAX_BYTES)


def test_optimized_output_size_still_enforces_max_bytes():
    original = ALL_FIXTURES["busy_texture"]()
    with pytest.raises(ImageTooLargeError):
        optimize_image(original, target_format="jpeg", quality=100, max_bytes=100)
