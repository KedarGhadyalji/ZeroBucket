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
  decodeHeic,
  heicAvailable,
  HEIC_INSTALL_HINT,
  looksLikeHeic,
} from "./heic.js";
import { DEFAULT_MAX_PIXELS, loadSharp, validateImage } from "./validation.js";

/** Formats the optimizer can WRITE. (HEIC can be read but not written: there is no HEVC encoder for Node.) */
const ENCODABLE_FORMATS = ["jpeg", "png", "webp"] as const;

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

export async function optimizeImage(
  data: Buffer,
  opts: OptimizeOptions & { maxBytes: number; maxPixels?: number },
): Promise<OptimizationResult> {
  const { maxWidth, quality, maxBytes, maxPixels = DEFAULT_MAX_PIXELS } = opts;
  const sharp = await loadSharp();
  const originalSizeBytes = data.length;

  const requestedRaw = opts.format?.toLowerCase();
  const requested = requestedRaw === "jpg" ? "jpeg" : requestedRaw;
  if (requested === "heic" || requested === "heif") {
    throw new ImageValidationError(
      "Encoding to HEIC is not available in the Node package (there is no HEVC encoder for Node). " +
        "Optimize to jpeg, webp or png instead, or store the original HEIC without `optimize`.",
    );
  }

  // Where the pixels come from: HEIC is decoded by libheif-js (in a worker thread) to raw RGBA that sharp then
  // re-encodes; everything else is read by sharp directly. EXIF is irrelevant for raw input (HEIC orientation
  // is already applied by the decoder), and sharp strips metadata on output either way.
  let input: Buffer = data;
  let inputOptions: Parameters<typeof sharp>[1] = {
    limitInputPixels: maxPixels,
    failOn: "error",
  };
  let sourceFormat: string | undefined;
  if (looksLikeHeic(data)) {
    if (!heicAvailable()) {
      throw new ImageValidationError(
        `This looks like a HEIC/HEIF image, which requires an optional dependency. Install it with: ${HEIC_INSTALL_HINT}`,
      );
    }
    if (!requested) {
      throw new ImageValidationError(
        "Optimizing a HEIC image needs an explicit output format, because re-encoding to HEIC is not available in the " +
          "Node package. Pass optimize: { format: 'jpeg' | 'webp' | 'png' } (for example { format: 'webp', maxWidth: 1600 }).",
      );
    }
    const decoded = await decodeHeic(data);
    input = decoded.rgba;
    inputOptions = {
      raw: { width: decoded.width, height: decoded.height, channels: 4 },
    };
    sourceFormat = "heic";
  } else {
    sourceFormat = (await sharp(data, inputOptions).metadata()).format;
  }
  const target = requested ?? sourceFormat ?? "jpeg";
  if (!(ENCODABLE_FORMATS as readonly string[]).includes(target)) {
    throw new ImageValidationError(
      `Cannot re-encode to unsupported format ${JSON.stringify(target)}. Supported: ${JSON.stringify([...ENCODABLE_FORMATS].sort())}`,
    );
  }

  // .rotate() with no args = auto-orient from EXIF. sharp drops all metadata on output by default.
  let pipeline = sharp(input, inputOptions).rotate();
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
