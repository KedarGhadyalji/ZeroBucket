# ZeroBucket Compression Results

**Method:** four procedurally generated test images approximating
different real-photo content types (see
`packages/python/tests/photo_fixtures.py` -- generated, not downloaded,
to avoid any licensing ambiguity in the repo). Each was re-encoded at a
range of JPEG/WebP quality settings and compared against the original
using SSIM (structural similarity), the standard objective proxy for
"would a human notice." Single-scale SSIM (via `scikit-image`) is used
here -- a known **stricter** metric than multi-scale SSIM, so these
numbers are conservative relative to some published "visually lossless"
thresholds elsewhere.

Run the sweep yourself: see `packages/python/tests/test_optimization.py`.

## The core finding: "imperceptible" depends heavily on content, not just quality setting

| Content type         | What it represents                                           | SSIM ceiling, even at very high quality             |
| -------------------- | ------------------------------------------------------------ | --------------------------------------------------- |
| `gradient_landscape` | Sky/ground gradient -- typical smooth-region photo content   | ~0.99                                               |
| `flat_graphic`       | UI screenshot / logo -- flat color, hard edges               | ~0.997                                              |
| `textured_portrait`  | Vignette + fine texture band -- smooth region next to detail | ~0.98 (needs quality ~95+)                          |
| `busy_texture`       | Dense multi-scale noise -- foliage/fabric-like               | ~0.97, plateaus and barely improves past quality 90 |

The last two never comfortably clear a strict 0.98 SSIM bar no matter how
high the quality setting goes, because that fine detail is close to
incompressible -- this is a property of the _content_, not a limitation
we can tune away. Holding every image to one blanket SSIM target would
mean either failing correctly-compressed detailed photos, or setting
quality so high that typical smooth photos get almost no size benefit.
The honest fix was different acceptance floors per content type, not one
number pretending to fit all content -- see `SSIM_FLOOR_BY_FIXTURE` in
the test file.

## Chosen defaults, and what they actually deliver

**`DEFAULT_JPEG_QUALITY = 90`, `DEFAULT_WEBP_QUALITY = 88`**

| Content type       | Format | Original | Compressed | Saved            | SSIM  |
| ------------------ | ------ | -------- | ---------- | ---------------- | ----- |
| gradient_landscape | JPEG   | 274KB    | 73KB       | 73%              | 0.987 |
| gradient_landscape | WebP   | 274KB    | 13KB       | 95%              | 0.985 |
| textured_portrait  | JPEG   | 441KB    | 131KB      | 70%              | 0.964 |
| textured_portrait  | WebP   | 441KB    | 37KB       | 92%              | 0.956 |
| busy_texture       | JPEG   | 667KB    | 334KB      | 50%              | 0.946 |
| busy_texture       | WebP   | 667KB    | 255KB      | 62%              | 0.948 |
| flat_graphic       | JPEG   | 2KB      | 8KB        | **-250% (grew)** | 0.996 |
| flat_graphic       | WebP   | 2KB      | 2KB        | 30%              | 0.997 |

## An important anti-pattern this data caught: don't convert flat/graphic content to JPEG

`flat_graphic` **grew 4x** when re-encoded as JPEG (2KB → 8KB). JPEG's
block-based DCT encoding is fundamentally bad at flat color regions with
hard edges -- it spends bits on artifacts _around_ the edges that a
format like PNG or WebP doesn't need at all. WebP, by contrast, handled
the same image correctly (2KB → 2KB, with real quality headroom to
spare).

**Practical implication:** `optimize_image(..., target_format="jpeg")`
is the wrong call for screenshots, logos, or UI graphics -- keep those as
PNG, or convert to WebP if size matters, never JPEG. This isn't
documented as a warning inside the library itself (v1 doesn't attempt to
auto-detect "is this actually a photo"), but it's worth knowing before
you pick a blanket `format=` setting for a whole application.

## Practical guidance by use case

- **Typical user-uploaded photos** (the common case): defaults are safe,
  expect 70-95% size reduction with no visible quality loss.
- **Screenshots, logos, UI graphics**: don't target JPEG. Stay PNG, or
  target WebP if you want the size reduction.
- **Dense-texture photos** (close-up fabric, foliage, gravel): defaults
  still produce real savings (50-62%) at good quality, but if you're
  storing this content professionally (product photography, print),
  consider raising `quality=` toward 95+ and accept a smaller size win in
  exchange for tighter fidelity.

## Caveats

- Single-container benchmark environment, same caveats as
  `benchmarks/RESULTS.md` regarding hardware-specific absolute numbers --
  the SSIM/quality relationships here are about compression behavior,
  not affected by hardware, but re-run locally if you want numbers for
  your own representative image set.
- Test fixtures are procedurally generated, calibrated to approximate
  realistic camera-JPEG noise levels (not raw sensor noise, which is
  much heavier and would make everything look artificially harder to
  compress than real-world photos actually are). See
  `photo_fixtures.py` for exact generation parameters if you want to
  reproduce or extend this.
- SSIM is an imperfect proxy for human perception, particularly around
  denoising effects (removing fine random noise can score as "different"
  under SSIM even when it doesn't look worse, or looks cleaner, to a
  viewer). Treat these numbers as a rigorous floor, not a substitute for
  occasionally looking at actual output.
