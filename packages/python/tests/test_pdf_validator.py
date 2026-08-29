"""Tests for zerobucket.validators.pdf.PDFValidator. No database required."""

from __future__ import annotations

import pytest

from zerobucket.exceptions import ContentValidationError
from zerobucket.validators.pdf import DEFAULT_MAX_PDF_BYTES, PDFValidator


def _fake_pdf(extra: bytes = b"") -> bytes:
    return b"%PDF-1.4\n" + extra + b"\n%%EOF"


def test_valid_pdf_passes():
    validator = PDFValidator()
    data = _fake_pdf(b"some content here")
    result = validator.validate(data, max_bytes=10_000_000)
    assert result.mime_type == "application/pdf"
    assert result.size_bytes == len(data)
    assert result.width is None
    assert result.height is None


def test_missing_pdf_header_rejected():
    validator = PDFValidator()
    with pytest.raises(ContentValidationError, match="does not look like a valid PDF"):
        validator.validate(b"not a pdf at all", max_bytes=10_000_000)


def test_empty_bytes_rejected():
    validator = PDFValidator()
    with pytest.raises(ContentValidationError, match="empty"):
        validator.validate(b"", max_bytes=10_000_000)


def test_oversized_pdf_rejected_by_instance_level_max():
    validator = PDFValidator(max_bytes=100)
    data = _fake_pdf(b"x" * 200)
    with pytest.raises(ContentValidationError, match="exceeds the maximum"):
        validator.validate(
            data, max_bytes=10_000_000
        )  # ZeroBucket's own limit is generous


def test_oversized_pdf_rejected_by_call_level_max():
    """The effective limit is whichever of the two ceilings is smaller --
    here the validator's own default is generous, but ZeroBucket's
    instance-level max_bytes (passed in at call time) is the tighter one."""
    validator = PDFValidator()  # default 20MB
    data = _fake_pdf(b"x" * 200)
    with pytest.raises(ContentValidationError, match="exceeds the maximum"):
        validator.validate(data, max_bytes=100)


def test_default_max_bytes_is_reasonable_for_pdfs():
    # Sanity check on the shipped default -- PDFs commonly run larger
    # than typical web images, so this should be meaningfully bigger
    # than ZeroBucket's own 8MB image default, not the same number.
    assert DEFAULT_MAX_PDF_BYTES > 8 * 1024 * 1024


def test_pdf_like_content_with_wrong_version_prefix_format_still_passes():
    """Only the '%PDF-' prefix itself is checked, not a specific version
    number -- any PDF version should pass this basic sniff."""
    validator = PDFValidator()
    for version in [b"1.0", b"1.7", b"2.0"]:
        data = b"%PDF-" + version + b"\nsome content\n%%EOF"
        result = validator.validate(data, max_bytes=10_000_000)
        assert result.mime_type == "application/pdf"


def test_jpeg_bytes_rejected_as_pdf():
    """Cross-format sanity check: a real JPEG's magic bytes must not be
    mistaken for a PDF."""
    jpeg_magic = b"\xff\xd8\xff\xe0" + b"\x00" * 20
    validator = PDFValidator()
    with pytest.raises(ContentValidationError):
        validator.validate(jpeg_magic, max_bytes=10_000_000)
