/**
 * Error hierarchy for ZeroBucket. Mirrors the Python package's exceptions
 * one-to-one so behaviour is portable across languages.
 *
 * Catch broadly with `instanceof ZeroBucketError`, or narrowly with a
 * subclass such as `ImageNotFoundError`.
 */

export class ZeroBucketError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = new.target.name;
  }
}

/** Any content (image or custom-validated) failed validation. */
export class ContentValidationError extends ZeroBucketError {}

/** An image failed validation (bad format, too large, corrupted, ...). */
export class ImageValidationError extends ContentValidationError {}

export class ImageTooLargeError extends ImageValidationError {
  constructor(
    public readonly sizeBytes: number,
    public readonly maxBytes: number,
  ) {
    super(
      `Image is ${sizeBytes} bytes, which exceeds the maximum of ${maxBytes} bytes`,
    );
  }
}

export class UnsupportedFormatError extends ImageValidationError {
  constructor(
    public readonly detectedFormat: string | null,
    public readonly allowed: readonly string[],
  ) {
    super(
      `Detected format ${JSON.stringify(detectedFormat)} is not supported. ` +
        `Allowed formats: ${JSON.stringify([...allowed].sort())}`,
    );
  }
}

/** Image bytes could not be decoded despite a recognizable header. */
export class CorruptedImageError extends ImageValidationError {}

/** `get()`, `getStream()` or `metadata()` was called with an id that does not exist. */
export class ImageNotFoundError extends ZeroBucketError {
  constructor(public readonly imageId: string) {
    super(`No image found with id ${JSON.stringify(imageId)}`);
  }
}

/** Underlying storage / database failure not covered above. */
export class StorageError extends ZeroBucketError {}

/**
 * A `beforeGet` / `beforePut` hook denied the operation by returning false.
 *
 * If the hook itself throws, that exception propagates unchanged: it is NOT
 * wrapped in AccessDeniedError and NOT treated as an implicit allow. A broken
 * authorization check must fail closed, never open.
 */
export class AccessDeniedError extends ZeroBucketError {
  constructor(
    public readonly operation: string,
    public readonly imageId?: string,
  ) {
    super(
      imageId !== undefined
        ? `${operation} denied for image ${JSON.stringify(imageId)} by the beforeGet hook`
        : `${operation} denied by the beforePut hook`,
    );
  }
}
