import { ContentValidationError } from "../errors.js";
import type { ContentValidator, ValidatedContent } from "./types.js";

export const DEFAULT_MAX_PDF_BYTES = 20 * 1024 * 1024;

/**
 * Validates that bytes look like a PDF: `%PDF-` magic bytes, non-empty, under a
 * size ceiling. Deliberately minimal.
 *
 * Security note: this does NOT parse the PDF object structure and does not
 * detect embedded JavaScript, forms or launch actions. Treat it as a sanity
 * gate, not a security boundary, if you serve user-uploaded PDFs to other users.
 */
export class PDFValidator implements ContentValidator {
  constructor(private readonly opts: { maxBytes?: number } = {}) {}

  validate(data: Buffer, { maxBytes }: { maxBytes: number }): ValidatedContent {
    const effectiveMax = Math.min(
      maxBytes,
      this.opts.maxBytes ?? DEFAULT_MAX_PDF_BYTES,
    );
    if (data.length === 0)
      throw new ContentValidationError("PDF data is empty");
    if (data.length > effectiveMax) {
      throw new ContentValidationError(
        `PDF is ${data.length} bytes, exceeds the maximum of ${effectiveMax} bytes`,
      );
    }
    // The spec allows junk before the header; real readers scan the first 1024 bytes.
    if (!data.subarray(0, 1024).includes("%PDF-")) {
      throw new ContentValidationError(
        "This does not look like a valid PDF (missing '%PDF-' header). Content is " +
          "checked by inspecting the actual bytes, not the filename or any client-supplied content type.",
      );
    }
    return {
      mimeType: "application/pdf",
      sizeBytes: data.length,
      width: null,
      height: null,
    };
  }
}
