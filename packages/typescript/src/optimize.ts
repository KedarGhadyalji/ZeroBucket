/**
 * Optimize pipeline: strip metadata, optionally downscale and re-encode.
 * Mirrors the Python package's optimize_image(), implemented on sharp.
 *
 * Differences from the Python/Pillow version, both deliberate:
 *  - EXIF orientation is applied (auto-rotate) BEFORE metadata is stripped.
 *    Stripping EXIF without applying it would leave phone photos sideways.
 *  - Transparent PNG/WebP converted to JPEG is flattened onto white, not black.
 */
import { ImageValidationError } from "./errors.js";
import {
  DEFAULT_MAX_PIXELS,
  SUPPORTED_FORMATS,
  loadSharp,
  validateImage,
} from "./validation.js";

export const DEFAULT_JPEG_QUALITY = 90;
export const DEFAULT_WEBP_QUALITY = 88;

export interface OptimizeOptions {
  /** Downscale (aspect ratio preserved, Lanczos) if wider than this. Never upscales. */
  maxWidth?: number;
  /** "jpeg" | "png" | "webp". Defaults to the source format. */
  format?: string;
  /** 1-100, JPEG/WebP only (ignored for PNG). */
  quality?: number;
}

export interface OptimizationResult {
  data: Buffer;
  mimeType: string;
  width: number;
  height: number;
  sizeBytes: number;
  originalSizeBytes: number;
}

const FORMAT_ALIASES: Record<string, string> = { jpg: "jpeg" };

export async function optimizeImage(
  data: Buffer,
  opts: OptimizeOptions & { maxBytes: number; maxPixels?: number },
): Promise<OptimizationResult> {
  const { maxWidth, quality, maxBytes, maxPixels = DEFAULT_MAX_PIXELS } = opts;
  const sharp = await loadSharp();
  const originalSizeBytes = data.length;

  const base = sharp(data, { limitInputPixels: maxPixels, failOn: "error" });
  const meta = await base.metadata();
  const requested = (opts.format ?? meta.format ?? "jpeg").toLowerCase();
  const target = FORMAT_ALIASES[requested] ?? requested;
  if (!SUPPORTED_FORMATS.includes(target)) {
    throw new ImageValidationError(
      `Cannot re-encode to unsupported format ${JSON.stringify(target)}. Supported: ${JSON.stringify(
        [...SUPPORTED_FORMATS].sort(),
      )}`,
    );
  }

  // .rotate() with no args = auto-orient from EXIF. sharp drops all metadata on output by default.
  let pipeline = sharp(data, {
    limitInputPixels: maxPixels,
    failOn: "error",
  }).rotate();
  if (maxWidth !== undefined) {
    pipeline = pipeline.resize({
      width: maxWidth,
      fit: "inside",
      withoutEnlargement: true,
      kernel: "lanczos3",
    });
  }

  if (target === "jpeg") {
    pipeline = pipeline
      .flatten({ background: "#ffffff" })
      .jpeg({
        quality: quality ?? DEFAULT_JPEG_QUALITY,
        mozjpeg: true,
        progressive: true,
      });
  } else if (target === "webp") {
    pipeline = pipeline.webp({
      quality: quality ?? DEFAULT_WEBP_QUALITY,
      effort: 6,
    });
  } else {
    pipeline = pipeline.png({ compressionLevel: 9 }); // no lossy knob; quality ignored by design
  }

  const out = await pipeline.toBuffer();

  // Defense in depth: re-validate our own output before it is trusted.
  const revalidated = await validateImage(out, { maxBytes, maxPixels });
  return {
    data: out,
    mimeType: revalidated.mimeType,
    width: revalidated.width,
    height: revalidated.height,
    sizeBytes: revalidated.sizeBytes,
    originalSizeBytes,
  };
}
