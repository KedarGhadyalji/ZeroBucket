/**
 * Image validation. Never trusts file extensions or client-supplied MIME types:
 * the bytes are decoded with sharp (libvips) and the *detected* format is what
 * gets validated and stored.
 *
 * sharp is an optional peer dependency, imported lazily, so `import "zerobucket"`
 * works without it (e.g. when you only store PDFs through a custom validator).
 */
import {
  HEIC_INSTALL_HINT,
  heicAvailable,
  heicInfo,
  heicValidate,
  looksLikeHeic,
} from "./heic.js";
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
  heic: "image/heic", // optional: needs `npm install libheif-js` (see heic.ts)
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

// sharp <= 0.34 types the module as `export =` (the namespace itself is callable); sharp >= 0.35 ships separate
// ESM typings where the callable is the namespace's `default`. Resolve to the callable either way.
type SharpModule = typeof import("sharp");
type Sharp = SharpModule extends (...args: never[]) => unknown
  ? SharpModule
  : SharpModule extends { default: infer D }
    ? D
    : never;
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

  // sharp's prebuilt libvips cannot decode HEIC, so it is handled (in a worker thread) by the optional libheif-js.
  if (looksLikeHeic(data))
    return validateHeic(data, { maxBytes, maxPixels, allowedFormats });

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

async function validateHeic(
  data: Buffer,
  {
    maxBytes,
    maxPixels,
    allowedFormats,
  }: { maxBytes: number; maxPixels: number; allowedFormats: readonly string[] },
): Promise<ValidatedImage> {
  const sizeBytes = data.length;
  // Same order as the Python package: a missing optional dependency is reported as such, never as "corrupt".
  if (!heicAvailable()) {
    throw new ImageValidationError(
      `This looks like a HEIC/HEIF image, which requires an optional dependency. Install it with: ${HEIC_INSTALL_HINT}`,
    );
  }
  if (!allowedFormats.includes("heic"))
    throw new UnsupportedFormatError("heic", allowedFormats);

  let width: number;
  let height: number;
  try {
    ({ width, height } = await heicInfo(data)); // header only: cheap, and lets us refuse a pixel bomb BEFORE decoding
  } catch (exc) {
    if (exc instanceof StorageError) throw exc; // the decoder worker failed: not the image's fault
    throw new CorruptedImageError(
      `Could not decode image: ${errMessage(exc)}`,
      { cause: exc },
    );
  }
  if (!width || !height)
    throw new CorruptedImageError("Image has no readable dimensions");
  if (width * height > maxPixels)
    throw new ImageTooLargeError(sizeBytes, maxBytes);

  try {
    await heicValidate(data); // full pixel decode, like Pillow's img.load(): catches truncated bodies
  } catch (exc) {
    if (exc instanceof StorageError) throw exc;
    throw new CorruptedImageError(
      `Image data is truncated or corrupted: ${errMessage(exc)}`,
      { cause: exc },
    );
  }
  return { mimeType: FORMAT_TO_MIME.heic!, width, height, sizeBytes };
}

function isPixelLimit(exc: unknown): boolean {
  return /pixel limit/i.test(errMessage(exc));
}

export function errMessage(exc: unknown): string {
  return exc instanceof Error ? exc.message : String(exc);
}

export { ImageValidationError };