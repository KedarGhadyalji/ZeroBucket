/**
 * Image validation. Never trusts file extensions or client-supplied MIME types:
 * the bytes are decoded with sharp (libvips) and the *detected* format is what
 * gets validated and stored.
 *
 * sharp is an optional peer dependency, imported lazily, so `import "zerobucket"`
 * works without it (e.g. when you only store PDFs through a custom validator).
 */
import {
  CorruptedImageError,
  ImageTooLargeError,
  ImageValidationError,
  StorageError,
  UnsupportedFormatError,
} from "./errors.js";

/** sharp format name -> canonical MIME type. Deliberately small allowlist. */
const FORMAT_TO_MIME: Record<string, string> = {
  jpeg: "image/jpeg",
  png: "image/png",
  webp: "image/webp",
};

export const SUPPORTED_FORMATS: readonly string[] = Object.keys(FORMAT_TO_MIME);

/** Reject images that would decode to more pixels than this, regardless of compressed size (~89 MP). */
export const DEFAULT_MAX_PIXELS = 89_000_000;

export interface ValidatedImage {
  mimeType: string;
  width: number;
  height: number;
  sizeBytes: number;
}

type Sharp = typeof import("sharp");
let sharpPromise: Promise<Sharp> | undefined;

/** Lazily load sharp, with an actionable error if it is not installed. */
export function loadSharp(): Promise<Sharp> {
  sharpPromise ??= import("sharp").then(
    // ESM gives { default: sharp }; some CJS interop gives sharp directly.
    (m) =>
      (m as unknown as { default?: Sharp }).default ?? (m as unknown as Sharp),
    (cause) => {
      sharpPromise = undefined;
      throw new StorageError(
        "Image validation requires the optional dependency 'sharp'. Install it with: npm install sharp",
        { cause },
      );
    },
  );
  return sharpPromise;
}

export async function validateImage(
  data: Buffer,
  opts: {
    maxBytes: number;
    maxPixels?: number;
    allowedFormats?: readonly string[];
  },
): Promise<ValidatedImage> {
  const {
    maxBytes,
    maxPixels = DEFAULT_MAX_PIXELS,
    allowedFormats = SUPPORTED_FORMATS,
  } = opts;
  const sizeBytes = data.length;
  if (sizeBytes > maxBytes) throw new ImageTooLargeError(sizeBytes, maxBytes);
  if (sizeBytes === 0) throw new CorruptedImageError("Image data is empty");

  const sharp = await loadSharp();
  // failOn "error": truncated / corrupt bodies throw instead of silently decoding partially.
  const make = () =>
    sharp(data, { limitInputPixels: maxPixels, failOn: "error" });

  let format: string | undefined;
  let width: number | undefined;
  let height: number | undefined;
  try {
    const meta = await make().metadata();
    format = meta.format;
    width = meta.width;
    height = meta.height;
  } catch (exc) {
    if (isPixelLimit(exc)) throw new ImageTooLargeError(sizeBytes, maxBytes);
    throw new CorruptedImageError(
      `Could not decode image: ${errMessage(exc)}`,
      { cause: exc },
    );
  }

  if (
    !format ||
    !allowedFormats.includes(format) ||
    !(format in FORMAT_TO_MIME)
  ) {
    throw new UnsupportedFormatError(format ?? null, allowedFormats);
  }
  if (!width || !height)
    throw new CorruptedImageError("Image has no readable dimensions");

  // Header parsing is not enough: force a full pixel decode to catch truncated bodies.
  try {
    await make().stats();
  } catch (exc) {
    if (isPixelLimit(exc)) throw new ImageTooLargeError(sizeBytes, maxBytes);
    throw new CorruptedImageError(
      `Image data is truncated or corrupted: ${errMessage(exc)}`,
      {
        cause: exc,
      },
    );
  }

  return { mimeType: FORMAT_TO_MIME[format]!, width, height, sizeBytes };
}

function isPixelLimit(exc: unknown): boolean {
  return /pixel limit/i.test(errMessage(exc));
}

export function errMessage(exc: unknown): string {
  return exc instanceof Error ? exc.message : String(exc);
}

export { ImageValidationError };
