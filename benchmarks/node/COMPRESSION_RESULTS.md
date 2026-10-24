# ZeroBucket npm Compression Results

The npm counterpart of [`../COMPRESSION_RESULTS.md`](../COMPRESSION_RESULTS.md). It measures `optimizeImage()` /
`put(..., { optimize })` on sharp: how much each content type shrinks and how much of the picture survives, measured with
SSIM (structural similarity, the usual objective proxy for "would a person notice").

Run it yourself: `npm run compression` (see [README.md](README.md)).

**Method.** Four procedurally generated images that follow the Python package's recipes
(`packages/python/tests/photo_fixtures.py`): a smooth landscape, a vignetted portrait with fine texture, dense
foliage-like noise, and a flat-colour graphic. They are generated, not downloaded, so there is no licensing question. They
use a seeded generator in this repo rather than numpy, so the **recipes match but the pixels do not**: the originals are
within a few KB of the Python ones, not identical. SSIM is single-scale, 7x7 window, averaged over RGB, the same
definition the Python tests use through scikit-image. The implementation here (`src/ssim.ts`) was checked against
scikit-image on 12 original/optimized pairs and agreed to within 6e-12.

## Defaults: JPEG quality 90, WebP quality 88

| Content type       | Format | Original | Compressed | Saved            | SSIM  |
| ------------------ | ------ | -------- | ---------- | ---------------- | ----- |
| gradient_landscape | JPEG   | 238KB    | 33KB       | 86%              | 0.987 |
| gradient_landscape | WebP   | 238KB    | 13KB       | 94%              | 0.985 |
| textured_portrait  | JPEG   | 406KB    | 71KB       | 82%              | 0.961 |
| textured_portrait  | WebP   | 406KB    | 30KB       | 93%              | 0.955 |
| busy_texture       | JPEG   | 633KB    | 326KB      | 49%              | 0.964 |
| busy_texture       | WebP   | 633KB    | 260KB      | 59%              | 0.945 |
| flat_graphic       | JPEG   | 4KB      | 8KB        | **-118% (grew)** | 0.996 |
| flat_graphic       | WebP   | 4KB      | 2KB        | 43%              | 0.997 |

Encoding takes 130-290ms for the photo-sized fixtures (JPEG roughly half of WebP), 33-47ms for the small graphic, on one core.

## JPEG quality sweep: SSIM (size)

| Content type       | q50           | q70           | q80           | q90           | q95           | q98           |
| ------------------ | ------------- | ------------- | ------------- | ------------- | ------------- | ------------- |
| gradient_landscape | 0.983 (8KB)   | 0.984 (9KB)   | 0.985 (13KB)  | 0.987 (33KB)  | 0.990 (87KB)  | 0.997 (248KB) |
| textured_portrait  | 0.951 (9KB)   | 0.953 (13KB)  | 0.955 (26KB)  | 0.961 (71KB)  | 0.973 (177KB) | 0.993 (415KB) |
| busy_texture       | 0.863 (121KB) | 0.914 (172KB) | 0.939 (222KB) | 0.964 (326KB) | 0.978 (448KB) | 0.984 (640KB) |
| flat_graphic       | 0.987 (5KB)   | 0.992 (6KB)   | 0.994 (7KB)   | 0.996 (8KB)   | 0.997 (9KB)   | 0.997 (10KB)  |

## What this tells us

**"Imperceptible" depends on the content, not just the quality number**, the same conclusion as the Python package. Smooth
images barely move in SSIM between quality 50 and 90 (0.983 to 0.987) while shrinking to a fraction of the size. Dense
texture is the opposite: 0.863 at quality 50, 0.964 at 90, and still only 0.984 at 98. If you store detail-heavy photos
professionally, raise `quality` and accept a smaller saving.

**Do not convert flat graphics to JPEG.** The flat graphic grew by more than double as a JPEG (4KB to 8KB) while WebP shrank it by
43%. Keep screenshots, logos and UI graphics as PNG, or use WebP if size matters.

**The defaults deliver the 70-95% saving the README claims on typical photo content** (smooth and lightly textured: 82-94%),
and about 50-60% on dense texture.

## npm and Python do not produce the same bytes

The same original files were run through the Python package's `optimize_image()` (Pillow, `compare_python.py`) and scored
with scikit-image. SSIM is within 0.004 everywhere except dense-texture JPEG (0.964 npm vs 0.941 Python, so npm is both
smaller and higher there); sizes are not the same:

| Content type       | Format | Original | npm (sharp) | Python (Pillow) | SSIM npm / Python |
| ------------------ | ------ | -------- | ----------- | --------------- | ----------------- |
| gradient_landscape | JPEG   | 237KB    | 33KB (86%)  | 73KB (69%)      | 0.987 / 0.987     |
| gradient_landscape | WebP   | 237KB    | 13KB (94%)  | 13KB (94%)      | 0.985 / 0.985     |
| textured_portrait  | JPEG   | 406KB    | 71KB (82%)  | 130KB (68%)     | 0.961 / 0.964     |
| textured_portrait  | WebP   | 406KB    | 30KB (93%)  | 30KB (93%)      | 0.955 / 0.955     |
| busy_texture       | JPEG   | 633KB    | 326KB (49%) | 333KB (47%)     | 0.964 / 0.941     |
| busy_texture       | WebP   | 633KB    | 260KB (59%) | 260KB (59%)     | 0.945 / 0.945     |
| flat_graphic       | JPEG   | 3-4KB    | 8KB (grew)  | 9KB (grew)      | 0.996 / 0.995     |
| flat_graphic       | WebP   | 3-4KB    | 2KB (43%)   | 2KB (43%)       | 0.997 / 0.997     |

The npm package encodes JPEG with sharp's `mozjpeg` preset (plus progressive output). On smooth and lightly textured
photos that gives **JPEGs roughly half the size of the Python package's for almost the same SSIM**. WebP output is
effectively identical, and dense texture is close in size. I confirmed the cause by encoding one file with each option:
sharp with progressive output but no mozjpeg gives 73KB, the same as Pillow's 73KB, and adding `mozjpeg: true` gives 33KB. If you share a
database between the two packages and re-optimize on either side, expect JPEG sizes to differ.

## Caveats

- Procedural fixtures approximate camera-JPEG statistics; they are not photographs. Re-run on your own images before relying
  on any exact figure.
- SSIM is an imperfect proxy for perception, especially around noise removal. Treat these numbers as a floor and look at
  real output occasionally.
- The two packages' fixtures are not pixel-identical, so the first two tables are comparable to Python's published
  `COMPRESSION_RESULTS.md` in kind only. The npm-vs-Python table above uses identical files and is the like-for-like one.
- Timings are from one shared single-core container.
