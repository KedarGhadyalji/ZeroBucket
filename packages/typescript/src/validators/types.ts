/** Pluggable content validation (the npm equivalent of Python's ContentValidator). */

export interface ValidatedContent {
  mimeType: string;
  sizeBytes: number;
  width?: number | null;
  height?: number | null;
}

/**
 * Implement this to let `put()` / `putMany()` accept a content type ZeroBucket
 * does not natively understand. It must throw (ideally a
 * `ContentValidationError`) when the content is invalid, and must trust nothing
 * but the bytes themselves.
 */
export interface ContentValidator {
  validate(
    data: Buffer,
    opts: { maxBytes: number },
  ): ValidatedContent | Promise<ValidatedContent>;
}
