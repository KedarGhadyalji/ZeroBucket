"""Regenerates the HEIC test fixtures with an independent encoder (pillow-heif / libheif + x265).

    pip install pillow pillow-heif
    python make_heic_fixtures.py

Fixtures are checked in so the test suite needs no HEIC encoder. Output is deterministic in content (seeded noise).
"""
import random
from PIL import Image
import pillow_heif

pillow_heif.register_heif_opener()
random.seed(7)


def noisy(w, h):
    im = Image.new("RGB", (w, h))
    px = im.load()
    for y in range(h):
        for x in range(w):
            px[x, y] = ((x * 255 // w) ^ random.randint(0, 40), (y * 255 // h), ((x + y) * 255 // (w + h)) ^ random.randint(0, 40))
    return im


noisy(160, 120).save("small.heic", quality=60)   # 160x120 landscape
noisy(90, 160).save("tall.heic", quality=60)     # 90x160 portrait

# Large (12 MP) but smooth, so it is only a few KB on disk while still costing ~2 s of CPU to decode.
# Used to prove HEIC decoding runs off the event loop.
g = Image.linear_gradient("L").resize((400, 300))
a = g.transpose(Image.Transpose.FLIP_TOP_BOTTOM)
b = g.rotate(90, expand=True).resize((400, 300))
Image.merge("RGB", (g, a, b)).resize((4000, 3000), Image.Resampling.BILINEAR).save("large_smooth.heic", quality=10)