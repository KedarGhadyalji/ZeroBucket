"""Integration tests for the custom validator hook (put(validator=...)),
against a real PostgreSQL database.

The point of these tests is specifically to prove the claim made in
content_types.py's docstring: a custom validator plugs into the exact
same storage/transaction/batch machinery as the built-in image
validator, with ZERO special-casing needed elsewhere. Each test below
mirrors an existing image-focused test (transactions, batch ops) but
using PDFValidator instead, to prove that claim rather than just assert it.
"""

from __future__ import annotations

import pytest

from zerobucket.exceptions import (
    ContentValidationError,
    ImageValidationError,
    ZeroBucketError,
)
from zerobucket.validators.pdf import PDFValidator


def _fake_pdf(tag: bytes = b"content") -> bytes:
    return b"%PDF-1.4\n" + tag + b"\n%%EOF"


def test_put_and_get_pdf_round_trip(images):
    validator = PDFValidator()
    data = _fake_pdf(b"hello world")

    doc_id = images.put(data, filename="report.pdf", validator=validator)
    result = images.get(doc_id)

    assert result.data == data
    assert result.mime_type == "application/pdf"
    assert result.filename == "report.pdf"
    assert result.width is None
    assert result.height is None
    assert len(result.checksum_sha256) == 64

    images.delete(doc_id)


def test_pdf_and_image_coexist_in_the_same_table(images, jpeg_bytes):
    """The same ZeroBucket instance, same underlying table, storing both
    an image (default validator) and a PDF (custom validator) side by
    side -- proving they don't interfere with each other."""
    image_id = images.put(jpeg_bytes)
    pdf_id = images.put(_fake_pdf(), validator=PDFValidator())

    img = images.get(image_id)
    doc = images.get(pdf_id)

    assert img.mime_type == "image/jpeg"
    assert img.width is not None  # images have real dimensions
    assert doc.mime_type == "application/pdf"
    assert doc.width is None  # PDFs, via this validator, don't

    images.delete(image_id)
    images.delete(pdf_id)


def test_invalid_pdf_raises_content_validation_error(images):
    with pytest.raises(ContentValidationError):
        images.put(b"not a pdf", validator=PDFValidator())


def test_content_validation_error_is_a_zerobucket_error(images):
    """Confirms the exception hierarchy claim directly: catching the
    broad ZeroBucketError catches a custom validator's failure too, not
    just built-in image validation failures."""
    with pytest.raises(ZeroBucketError):
        images.put(b"not a pdf", validator=PDFValidator())


def test_image_validation_error_still_a_content_validation_error(images):
    """Backward-compatibility proof: ImageValidationError (which existed
    before custom validators did) is now ALSO a ContentValidationError,
    so existing code catching ImageValidationError is unaffected, and
    new code catching ContentValidationError catches both kinds
    uniformly."""
    with pytest.raises(ContentValidationError):
        images.put(b"not an image either", filename="fake.jpg")
    # And the original, specific exception type is still exactly right:
    with pytest.raises(ImageValidationError):
        images.put(b"not an image either", filename="fake.jpg")


def test_optimize_true_with_validator_raises_immediately(images):
    """optimize=True assumes the built-in image pipeline -- must fail
    clearly and immediately when combined with a custom validator,
    rather than trying (and failing confusingly) to run Pillow against
    non-image bytes."""
    with pytest.raises(ImageValidationError, match="optimize=True is not supported"):
        images.put(_fake_pdf(), validator=PDFValidator(), optimize=True)


def test_put_pdf_with_connection_participates_in_transaction(
    images, db_connection_factory
):
    """Mirrors test_transactions.py's image-focused tests exactly, but
    for a custom-validated PDF -- proving connection= atomicity works
    identically regardless of which validator produced the row."""
    conn = db_connection_factory()
    conn.autocommit = False
    try:
        doc_id = images.put(_fake_pdf(), validator=PDFValidator(), connection=conn)
        assert images.exists(doc_id, connection=conn) is True

        conn.rollback()

        assert images.exists(doc_id) is False
    finally:
        conn.close()


def test_put_many_pdfs_with_validator(images):
    """Mirrors test_batch.py's put_many best-effort semantics, but for
    PDFs -- one bad item shouldn't abort the good ones, same as images."""
    batch = [_fake_pdf(b"one"), b"not a pdf", _fake_pdf(b"three")]
    results = images.put_many(batch, validator=PDFValidator())

    assert results[0].success is True
    assert results[1].success is False
    assert results[2].success is True

    doc1 = images.get(results[0].image_id)
    doc3 = images.get(results[2].image_id)
    assert doc1.mime_type == "application/pdf"
    assert doc3.mime_type == "application/pdf"

    images.delete(results[0].image_id)
    images.delete(results[2].image_id)
