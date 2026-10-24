/**
 * Deterministic test images.
 *
 * Two families, mirroring the Python package's benchmarks:
 *  - noise JPEGs at a target byte size (hard to compress, like benchmarks/generate_test_images.py), for latency runs;
 *  - four procedurally generated "photo-like" fixtures (same recipes as packages/python/tests/photo_fixtures.py),
 *    for compression-quality runs.
 *
 * Everything is generated, nothing is downloaded, so there is no licensing question. The random numbers come from a
 * seeded generator in this file, not from numpy, so the pixels are NOT identical to the Python fixtures: the recipes
 * and parameters are, which makes results comparable in kind, not digit for digit.
 */
import sharp from "sharp";

export const TARGET_SIZES: Record<string, number> = {
  "10KB": 10 * 1024,
  "100KB": 100 * 1024,
  "500KB": 500 * 1024,
  "1MB": 1024 * 1024,
  "5MB": 5 * 1024 * 1024,
  "10MB": 10 * 1024 * 1024,
};

/** mulberry32: a tiny seeded PRNG returning floats in [0, 1). */
export function makeRng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Normal-distributed numbers from a uniform generator (Box-Muller). */
export function makeGaussian(
  rand: () => number,
): (mean: number, sd: number) => number {
  let spare: number | null = null;
  return (mean, sd) => {
    if (spare !== null) {
      const v = spare;
      spare = null;
      return mean + sd * v;
    }
    let u = 0;
    while (u === 0) u = rand();
    const w = rand();
    const r = Math.sqrt(-2 * Math.log(u));
    spare = r * Math.sin(2 * Math.PI * w);
    return mean + sd * r * Math.cos(2 * Math.PI * w);
  };
}

const clamp8 = (v: number) => (v < 0 ? 0 : v > 255 ? 255 : Math.floor(v));

// ---------------------------------------------------------------------------------------------------------------
// Latency family: random-noise JPEGs at a target size
// ---------------------------------------------------------------------------------------------------------------

/** A square noise JPEG: a small random tile scaled up, so it stays hard to compress but is quick to generate. */
async function noiseJpeg(side: number, seed = 42): Promise<Buffer> {
  const rand = makeRng(seed);
  const tileSide = Math.max(8, Math.floor(side / 16));
  const tile = Buffer.alloc(tileSide * tileSide * 3);
  for (let i = 0; i < tile.length; i++) tile[i] = Math.floor(rand() * 256);
  return sharp(tile, {
    raw: { width: tileSide, height: tileSide, channels: 3 },
  })
    .resize(side, side, { kernel: "nearest" })
    .jpeg({ quality: 90 })
    .toBuffer();
}

/** Binary-search the image side length until the JPEG lands within `tolerance` of `targetBytes`. */
export async function makeImageOfSize(
  targetBytes: number,
  tolerance = 0.15,
): Promise<Buffer> {
  let low = 8;
  let high = 4000;
  let best = await noiseJpeg(high);
  while (best.length < targetBytes && high < 20000) {
    high = Math.floor(high * 1.5);
    best = await noiseJpeg(high);
  }
  for (let i = 0; i < 16; i++) {
    const mid = Math.floor((low + high) / 2);
    const candidate = await noiseJpeg(mid);
    if (
      Math.abs(candidate.length - targetBytes) <
      Math.abs(best.length - targetBytes)
    )
      best = candidate;
    if (candidate.length < targetBytes) low = mid;
    else high = mid;
    if (Math.abs(candidate.length - targetBytes) <= targetBytes * tolerance) {
      best = candidate;
      break;
    }
  }
  return best;
}

// ---------------------------------------------------------------------------------------------------------------
// Compression family: four photo-like fixtures
// ---------------------------------------------------------------------------------------------------------------

type Rgb = [number, number, number];

/** Sky-to-ground gradient with a horizon: large smooth regions, low noise. */
export async function gradientLandscape(
  width = 1600,
  height = 1200,
  seed = 1,
): Promise<Buffer> {
  const gauss = makeGaussian(makeRng(seed));
  const horizon = Math.floor(height * 0.6);
  const skyTop: Rgb = [80, 130, 220];
  const skyBottom: Rgb = [200, 220, 245];
  const groundTop: Rgb = [90, 130, 60];
  const groundBottom: Rgb = [60, 90, 40];
  const raw = Buffer.alloc(width * height * 3);
  for (let y = 0; y < height; y++) {
    const inSky = y < horizon;
    const rows = inSky ? horizon : height - horizon;
    const t = rows > 1 ? (inSky ? y : y - horizon) / (rows - 1) : 0;
    const a = inSky ? skyTop : groundTop;
    const b = inSky ? skyBottom : groundBottom;
    const sd = inSky ? 0.6 : 1.5;
    for (let x = 0; x < width; x++) {
      const o = (y * width + x) * 3;
      for (let c = 0; c < 3; c++)
        raw[o + c] = clamp8(a[c]! * (1 - t) + b[c]! * t + gauss(0, sd));
    }
  }
  return sharp(raw, { raw: { width, height, channels: 3 } })
    .jpeg({ quality: 97 })
    .toBuffer();
}

/** Soft vignette with fine texture, strongest in a band near the "hairline". */
export async function texturedPortrait(
  width = 1200,
  height = 1600,
  seed = 2,
): Promise<Buffer> {
  const gauss = makeGaussian(makeRng(seed));
  const cx = width / 2;
  const cy = height * 0.4;
  // Largest distance from (cx, cy) to any pixel centre, used to normalise.
  let maxDist = 0;
  for (const y of [0, height - 1])
    for (const x of [0, width - 1])
      maxDist = Math.max(maxDist, Math.hypot(x - cx, y - cy));
  const base: Rgb = [220, 180, 160];
  const edge: Rgb = [60, 50, 55];
  const raw = Buffer.alloc(width * height * 3);
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const dn = Math.hypot(x - cx, y - cy) / maxDist;
      const t = Math.min(1, Math.max(0, dn * 1.4));
      const band = Math.exp(-((dn - 0.3) ** 2) / 0.02);
      const o = (y * width + x) * 3;
      for (let c = 0; c < 3; c++) {
        raw[o + c] = clamp8(
          base[c]! * (1 - t) + edge[c]! * t + gauss(0, 1.2) * (1 + band * 2),
        );
      }
    }
  }
  return sharp(raw, { raw: { width, height, channels: 3 } })
    .jpeg({ quality: 97 })
    .toBuffer();
}

/** Dense multi-scale noise, approximating foliage, gravel or fabric: the worst case for compression. */
export async function busyTexture(
  width = 1200,
  height = 1200,
  seed = 3,
): Promise<Buffer> {
  const gauss = makeGaussian(makeRng(seed));
  const acc = new Float64Array(width * height * 3);
  for (const [scale, weight] of [
    [4, 0.5],
    [16, 0.3],
    [64, 0.15],
    [256, 0.05],
  ] as const) {
    const sw = Math.max(1, Math.floor(width / scale));
    const sh = Math.max(1, Math.floor(height / scale));
    const small = Buffer.alloc(sw * sh * 3);
    for (let i = 0; i < small.length; i++) small[i] = clamp8(gauss(128, 40));
    const layer = await sharp(small, {
      raw: { width: sw, height: sh, channels: 3 },
    })
      .resize(width, height, { kernel: "cubic" })
      .raw()
      .toBuffer();
    for (let i = 0; i < acc.length; i++) acc[i]! += layer[i]! * weight;
  }
  const raw = Buffer.alloc(acc.length);
  for (let i = 0; i < acc.length; i++) raw[i] = clamp8(acc[i]!);
  return sharp(raw, { raw: { width, height, channels: 3 } })
    .jpeg({ quality: 97 })
    .toBuffer();
}

/** Flat colour rectangles with hard edges, like a UI screenshot or logo. A PNG, as such content should be. */
export async function flatGraphic(
  width = 800,
  height = 600,
  seed = 4,
): Promise<Buffer> {
  const rand = makeRng(seed);
  const int = (lo: number, hi: number) => lo + Math.floor(rand() * (hi - lo));
  const raw = Buffer.alloc(width * height * 3, 245);
  for (let i = 0; i < 6; i++) {
    const x0 = int(0, width - 50);
    const y0 = int(0, height - 50);
    const bw = int(40, Math.floor(width / 3));
    const bh = int(30, Math.floor(height / 3));
    const color = [int(0, 200), int(0, 200), int(0, 200)];
    for (let y = y0; y < Math.min(height, y0 + bh); y++) {
      for (let x = x0; x < Math.min(width, x0 + bw); x++) {
        const o = (y * width + x) * 3;
        raw[o] = color[0]!;
        raw[o + 1] = color[1]!;
        raw[o + 2] = color[2]!;
      }
    }
  }
  return sharp(raw, { raw: { width, height, channels: 3 } })
    .png({ compressionLevel: 9 })
    .toBuffer();
}

export const FIXTURES: Record<string, () => Promise<Buffer>> = {
  gradient_landscape: () => gradientLandscape(),
  textured_portrait: () => texturedPortrait(),
  busy_texture: () => busyTexture(),
  flat_graphic: () => flatGraphic(),
};
