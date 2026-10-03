import sharp from "sharp";
import { ZeroBucket, type ZeroBucketOptions } from "../src/index.js";

export const DATABASE_URL = process.env.ZEROBUCKET_TEST_DATABASE_URL;

export type Fmt = "jpeg" | "png" | "webp";

/** A real, decodable test image with deterministic-ish noisy content (so it does not compress to nothing). */
export async function makeImage(
  fmt: Fmt = "png",
  width = 64,
  height = 48,
  opts: { alpha?: boolean; seed?: number } = {},
): Promise<Buffer> {
  const channels = opts.alpha ? 4 : 3;
  const raw = Buffer.alloc(width * height * channels);
  let s = (opts.seed ?? 1) >>> 0 || 1;
  for (let i = 0; i < raw.length; i++) {
    s = (s * 1664525 + 1013904223) >>> 0;
    raw[i] = (s >>> 24) & 0xff;
  }
  return sharp(raw, { raw: { width, height, channels } })[fmt]().toBuffer();
}

export async function makeGif(): Promise<Buffer> {
  return sharp({
    create: { width: 8, height: 8, channels: 3, background: "#f00" },
  })
    .gif()
    .toBuffer();
}

export function newBucket<C = unknown>(
  opts: ZeroBucketOptions<C> = {},
): ZeroBucket<C> {
  return new ZeroBucket<C>({ connectionString: DATABASE_URL, ...opts });
}
