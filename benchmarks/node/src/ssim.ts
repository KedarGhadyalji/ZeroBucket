/**
 * Structural similarity (SSIM), written to match scikit-image's `structural_similarity(a, b, channel_axis=-1,
 * data_range=255)` with its defaults, which is what the Python package's compression tests use: a 7x7 uniform
 * window, sample covariance, K1 = 0.01, K2 = 0.03, the mean taken over windows that fit fully inside the image, and
 * the result averaged over the three colour channels.
 *
 * Integral images make each window O(1). Values are shifted by 128 first: SSIM is shift-invariant for the
 * variances and covariance, and the shift keeps the sums small enough that floating-point cancellation stays far
 * below the fourth decimal place.
 */
import sharp from "sharp";

export interface RawRgb {
  data: Buffer;
  width: number;
  height: number;
}

export async function decodeRgb(input: Buffer): Promise<RawRgb> {
  const { data, info } = await sharp(input)
    .removeAlpha()
    .toColourspace("srgb")
    .raw()
    .toBuffer({ resolveWithObject: true });
  return { data, width: info.width, height: info.height };
}

const WIN = 7;
const NP = WIN * WIN;
const COV_NORM = NP / (NP - 1);
const R = 255;
const C1 = (0.01 * R) ** 2;
const C2 = (0.03 * R) ** 2;

/** Sum table where cell (y+1, x+1) holds the sum over rows 0..y and columns 0..x. */
function integral(
  w: number,
  h: number,
  value: (i: number) => number,
): Float64Array {
  const stride = w + 1;
  const out = new Float64Array(stride * (h + 1));
  for (let y = 0; y < h; y++) {
    let rowSum = 0;
    for (let x = 0; x < w; x++) {
      rowSum += value(y * w + x);
      out[(y + 1) * stride + x + 1] = out[y * stride + x + 1]! + rowSum;
    }
  }
  return out;
}

function windowSum(
  t: Float64Array,
  stride: number,
  x: number,
  y: number,
): number {
  return (
    t[(y + WIN) * stride + x + WIN]! -
    t[y * stride + x + WIN]! -
    t[(y + WIN) * stride + x]! +
    t[y * stride + x]!
  );
}

export function ssim(a: RawRgb, b: RawRgb): number {
  if (a.width !== b.width || a.height !== b.height)
    throw new Error("ssim: images must have the same dimensions");
  const { width: w, height: h } = a;
  if (w < WIN || h < WIN)
    throw new Error("ssim: image smaller than the 7x7 window");
  const stride = w + 1;
  let total = 0;
  for (let c = 0; c < 3; c++) {
    const X = (i: number) => a.data[i * 3 + c]! - 128;
    const Y = (i: number) => b.data[i * 3 + c]! - 128;
    const sx = integral(w, h, X);
    const sy = integral(w, h, Y);
    const sxx = integral(w, h, (i) => X(i) * X(i));
    const syy = integral(w, h, (i) => Y(i) * Y(i));
    const sxy = integral(w, h, (i) => X(i) * Y(i));
    let sum = 0;
    let n = 0;
    for (let y = 0; y + WIN <= h; y++) {
      for (let x = 0; x + WIN <= w; x++) {
        const ux = windowSum(sx, stride, x, y) / NP;
        const uy = windowSum(sy, stride, x, y) / NP;
        const vx = COV_NORM * (windowSum(sxx, stride, x, y) / NP - ux * ux);
        const vy = COV_NORM * (windowSum(syy, stride, x, y) / NP - uy * uy);
        const vxy = COV_NORM * (windowSum(sxy, stride, x, y) / NP - ux * uy);
        // The +128 shift is folded back in for the means, which are the only terms that are not shift-invariant.
        const mx = ux + 128;
        const my = uy + 128;
        sum +=
          ((2 * mx * my + C1) * (2 * vxy + C2)) /
          ((mx * mx + my * my + C1) * (vx + vy + C2));
        n++;
      }
    }
    total += sum / n;
  }
  return total / 3;
}
