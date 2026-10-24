"""Run the Python package's optimize_image() on the SAME original files the npm sweep used, and score both with
scikit-image's SSIM, so sharp (npm) and Pillow (Python) can be compared on identical input bytes.

    npm run compression -- --dump ./fixtures
    pip install zerobucket scikit-image
    python compare_python.py ./fixtures
"""

import io
import sys
from pathlib import Path

import numpy as np
from PIL import Image
from skimage.metrics import structural_similarity as ssim

from zerobucket.optimization import optimize_image


def rgb(data: bytes) -> np.ndarray:
    with Image.open(io.BytesIO(data)) as img:
        return np.array(img.convert("RGB"))


folder = Path(sys.argv[1] if len(sys.argv) > 1 else "fixtures")
print("| Content type | Format | Original | Compressed | Saved | SSIM |")
print("|---|---|---|---|---|---|")
for path in sorted(folder.glob("*.orig.*")):
    original = path.read_bytes()
    for fmt in ("jpeg", "webp"):
        out = optimize_image(original, target_format=fmt, max_bytes=64 * 1024 * 1024)
        saved = round((1 - out.size_bytes / len(original)) * 100)
        s = ssim(rgb(original), rgb(out.data), channel_axis=-1, data_range=255)
        note = f"**{saved}% (grew)**" if saved < 0 else f"{saved}%"
        print(
            f"| {path.name.split('.')[0]} | {fmt.upper()} | {len(original)//1024}KB | {out.size_bytes//1024}KB | {note} | {s:.3f} |"
        )
