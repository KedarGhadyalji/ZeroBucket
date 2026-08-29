"""PDF validation for use with ZeroBucket's custom-validator hook.

    from zerobucket import ZeroBucket
    from zerobucket.validators.pdf import PDFValidator

    images = ZeroBucket(database_url=DATABASE_URL)
    pdf_validator = PDFValidator()

    doc_id = images.put(pdf_bytes, validator=pdf_validator)
    doc = images.get(doc_id)  # get() needs no special handling at all
    print(doc.mime_type)  # "application/pdf"

This is a real reference implementation, not a stub: content-sniffed
validation (the '%PDF-' magic bytes, not filename/Content-Type), a
configurable size ceiling, and nothing else -- deliberately minimal,
matching the same "validate what the format actually needs, nothing
more" principle as the built-in image validator. It does not attempt to
fully parse the PDF structure (no page count, no embedded-JavaScript
detection, no font/object graph validation) -- see PDFValidator's class
docstring for what that means for your security posture if you accept
PDFs from untrusted uploaders.
"""

from __future__ import annotations

from ..content_types import ContentValidator, ValidatedContent
from ..exceptions import ContentValidationError

DEFAULT_MAX_PDF_BYTES = 20 * 1024 * 1024  # 20MB -- PDFs commonly run
# larger than typical web
# images; override for your data


class PDFValidator(ContentValidator):
    """Validates that bytes are a well-formed-enough PDF: correct magic
    bytes, non-empty, within a size ceiling.

    Security note, stated plainly rather than implied: this does NOT
    parse the PDF's internal object structure, and does not detect or
    strip embedded JavaScript, forms, or launch actions -- a PDF is a
    much richer, more dangerous format to fully secure than a raster
    image (this was the exact reasoning for NOT building native PDF
    support directly into ZeroBucket's core -- see the project's
    CHANGELOG/README history). If you accept PDFs from untrusted
    uploaders and serve them back to other users, treat this validator
    as a basic sanity gate, not a security boundary -- consider pairing
    it with a dedicated PDF-sanitizing library appropriate to your
    threat model.
    """

    def __init__(self, *, max_bytes: int = DEFAULT_MAX_PDF_BYTES) -> None:
        self._default_max_bytes = max_bytes

    def validate(self, data: bytes, *, max_bytes: int) -> ValidatedContent:
        # `max_bytes` here is ZeroBucket's own instance-level ceiling
        # (ZeroBucket(max_bytes=...)); this validator's own configured
        # limit is a further, independent ceiling -- the effective limit
        # is whichever is smaller, since both are enforced.
        effective_max = min(max_bytes, self._default_max_bytes)

        if len(data) == 0:
            raise ContentValidationError("PDF data is empty")
        if len(data) > effective_max:
            raise ContentValidationError(
                f"PDF is {len(data)} bytes, exceeds the maximum of {effective_max} bytes"
            )
        if not self._looks_like_pdf(data):
            raise ContentValidationError(
                "This does not look like a valid PDF (missing '%PDF-' header). "
                "Content is checked by inspecting the actual bytes, not the "
                "filename or any client-supplied content type."
            )

        return ValidatedContent(mime_type="application/pdf", size_bytes=len(data))

    @staticmethod
    def _looks_like_pdf(data: bytes) -> bool:
        """The PDF spec requires the file to start with '%PDF-' followed
        by a version number -- the same first check every PDF library
        and browser uses."""
        return data[:5] == b"%PDF-"
