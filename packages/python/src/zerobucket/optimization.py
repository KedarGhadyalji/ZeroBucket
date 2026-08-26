"""Image optimization: metadata stripping, resizing, and re-encoding.

Design principle: this module only ever runs on bytes that have ALREADY
passed validate_image(). It does not replace validation -- optimize_image()
re-validates its own output before returning, so a malformed re-encode
never silently reaches storage.

Quality defaults (JPEG=90, WebP=88) were chosen empirically, not guessed:
see packages/python/tests/test_optimization.py and
benchmarks/COMPRESSION_RESULTS.md for the actual SSIM measurements behind
these numbers. In short -- smooth/flat content (skies, graphics, most
typical photos) stays comfortably above SSIM 0.98 ("visually lossless")
at these settings while saving 70-95% of file size. Content with genuine
dense fine texture (heavy foliage, fabric close-ups) tops out lower
(SSIM ~0.95-0.97) regardless of quality setting, because that detail is
close to incompressible -- that's a property of the content, not a flaw
in these defaults. If you're storing that kind of image and pixel-level
fidelity matters, raise quality accordingly (see COMPRESSION_RESULTS.md
for the actual tradeoff curve).
"""

from __future__ import annotations

import io
from dataclasses import dataclass

from PIL import Image as PILImage

from .exceptions import ImageValidationError
from .validation import (
    DEFAULT_MAX_PIXELS,
    HEIF_SUPPORT_INSTALLED,
    SUPPORTED_FORMATS,
    validate_image,
)

# Normalizes user-facing target_format strings to what Pillow's save()
# actually expects. Most formats are their own name uppercased, but HEIC
# is the one exception: Pillow (via pillow-heif) only recognizes the save
# format string "HEIF", never "HEIC" -- even though "HEIC" is what the
# file extension and most people call it. Verified empirically; passing
# format="HEIC" directly to Pillow raises KeyError, not a graceful error.
_FORMAT_ALIASES = {
    "HEIC": "HEIF",
}

DEFAULT_JPEG_QUALITY = 90
DEFAULT_WEBP_QUALITY = 88
# NOT measured with the same SSIM methodology as the two defaults above
# (see benchmarks/COMPRESSION_RESULTS.md) -- this is a reasonable starting
# point, not a verified claim. Treat it as provisional until it's been
# through the same measurement process.
DEFAULT_HEIC_QUALITY = 90


@dataclass(frozen=True, slots=True)
class OptimizationResult:
    """Output of optimize_image(): the final bytes plus what changed."""

    data: bytes
    mime_type: str
    width: int
    height: int
    size_bytes: int
    original_size_bytes: int

    @property
    def bytes_saved(self) -> int:
        return self.original_size_bytes - self.size_bytes

    @property
    def percent_saved(self) -> float:
        if self.original_size_bytes == 0:
            return 0.0
        return (self.bytes_saved / self.original_size_bytes) * 100


def optimize_image(
    data: bytes,
    *,
    max_width: int | None = None,
    target_format: str | None = None,
    quality: int | None = None,
    max_bytes: int,
    max_pixels: int = DEFAULT_MAX_PIXELS,
) -> OptimizationResult:
    """Strip metadata, optionally resize and re-encode.

    Args:
        data: Original, already-validated image bytes.
        max_width: If the image is wider than this, downscale it
            (aspect ratio preserved, LANCZOS resampling -- the sharpest
            standard downscale filter, chosen specifically to avoid
            introducing blur that a cheaper filter like BILINEAR would).
        target_format: "jpeg", "png", "webp", or "heic"/"heif". None keeps
            the original format. Converting a PNG to WebP/JPEG is usually
            the right call when the PNG is actually a photo, not
            flat-color art. Converting TO heic requires the optional
            pillow-heif dependency; if it's missing, this raises
            ImageValidationError with an install hint rather than
            producing an unclear internal error.
        quality: 1-100. Only meaningful for JPEG/WebP output -- PNG has
            no lossy quality knob and this is ignored for PNG targets.
            None uses DEFAULT_JPEG_QUALITY / DEFAULT_WEBP_QUALITY.
        max_bytes: Same size ceiling used by validate_image(); enforced
            again on the re-encoded output.
        max_pixels: Same decompression-bomb ceiling used by
            validate_image(); enforced again on the re-encoded output.

    Returns:
        OptimizationResult with the final bytes and size comparison.

    Raises:
        ImageValidationError (or a subclass) if the re-encoded output
        somehow fails re-validation. This should not happen in normal
        operation -- it's a defense-in-depth check, not an expected path.
    """
    original_size = len(data)

    with PILImage.open(io.BytesIO(data)) as img:
        source_format = img.format
        # Force a full decode now (not just header parsing) so a lazy
        # Pillow image doesn't defer errors to the .save() call below,
        # where they'd be harder to attribute to "optimization broke it"
        # versus "the input was already bad" (validate_image already
        # ruled out the latter, but we don't re-trust that here).
        img.load()

        # Normalize to RGB before any lossy re-encode target that doesn't
        # support the source mode (e.g. a PNG with an alpha channel going
        # to JPEG, which has no alpha channel at all).
        working = img

        output_format = (target_format or source_format or "JPEG").upper()
        output_format = _FORMAT_ALIASES.get(output_format, output_format)
        if output_format == "JPEG" and working.mode in ("RGBA", "P", "LA"):
            working = working.convert("RGB")
        # Note: unlike JPEG, HEIF encoding via pillow-heif handles RGBA
        # source images fine (verified empirically) -- no forced conversion
        # needed there.

        if max_width is not None and working.width > max_width:
            ratio = max_width / working.width
            new_height = max(1, round(working.height * ratio))
            working = working.resize(
                (max_width, new_height), PILImage.Resampling.LANCZOS
            )

        save_kwargs: dict = {}
        if output_format == "JPEG":
            save_kwargs = {
                "quality": quality or DEFAULT_JPEG_QUALITY,
                "optimize": True,
                "progressive": True,
            }
        elif output_format == "WEBP":
            save_kwargs = {
                "quality": quality or DEFAULT_WEBP_QUALITY,
                "method": 6,  # slowest, best-compression effort; fine for
                # a one-time encode on upload, not a hot path
            }
        elif output_format == "HEIF":
            if not HEIF_SUPPORT_INSTALLED:
                raise ImageValidationError(
                    "Converting to HEIC/HEIF requires an optional dependency. "
                    "Install it with: pip install zerobucket[heic]"
                )
            save_kwargs = {"quality": quality or DEFAULT_HEIC_QUALITY}
        elif output_format == "PNG":
            # No lossy quality knob for PNG. `quality` is silently ignored
            # here by design -- see the docstring above.
            save_kwargs = {"optimize": True}
        else:
            raise ImageValidationError(
                f"Cannot re-encode to unsupported format {output_format!r}. "
                f"Supported: {sorted(SUPPORTED_FORMATS)}"
            )

        # Re-saving from a freshly loaded pixel buffer, with no `exif=` or
        # `icc_profile=` kwarg passed through, is what actually strips
        # metadata -- we simply never carry `img.info` forward.
        buf = io.BytesIO()
        working.save(buf, format=output_format, **save_kwargs)
        optimized_bytes = buf.getvalue()

    # Defense in depth: re-validate our own output before it's trusted.
    revalidated = validate_image(
        optimized_bytes, max_bytes=max_bytes, max_pixels=max_pixels
    )

    return OptimizationResult(
        data=optimized_bytes,
        mime_type=revalidated.mime_type,
        width=revalidated.width,
        height=revalidated.height,
        size_bytes=revalidated.size_bytes,
        original_size_bytes=original_size,
    )
