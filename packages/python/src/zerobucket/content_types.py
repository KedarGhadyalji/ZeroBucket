"""Pluggable content validation.

ZeroBucket's default behavior (no changes here) validates everything as
an image via Pillow -- that's unchanged and remains the default for
every existing caller. This module adds an OPT-IN escape hatch: pass
your own ContentValidator to put()/put_many() to store non-image content
(PDFs, etc.) through the exact same storage, transaction (connection=),
retry, and batch machinery, without ZeroBucket itself needing to know
anything about that content type.

Why this shape, specifically:

- Explicit, not sniffed. You pass `validator=` on the specific put()
  call where you want non-default validation -- ZeroBucket does not try
  to auto-detect "is this a PDF or an image" by inspecting bytes across
  multiple registered validators. Auto-detection across arbitrary
  validator types invites exactly the kind of ambiguous, hard-to-audit
  behavior the image validator's own "always sniff content, never trust
  claims" principle exists to avoid -- the fix for that principle is
  explicitness at the call site, not guessing between validators.

- Read paths need ZERO changes. get()/exists()/delete()/metadata() (and
  their batch equivalents) already work generically -- width/height were
  already nullable in the schema and the Image type before this feature
  existed (verified directly against both, not assumed), since not
  every stored row needs to be an image. Only put()/put_many() (the
  write path, where format-specific validation happens) needed a hook.

- optimize=True is incompatible with a custom validator and raises
  immediately, rather than trying to run Pillow's decode/resize/re-encode
  pipeline against bytes that were never claimed to be an image in the
  first place.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ValidatedContent:
    """Result of a custom validator's validate() call.

    width/height default to None -- most non-image content has no
    natural width/height. A custom validator for a content type that
    DOES have inherent dimensions (e.g. an SVG's viewBox) is free to
    populate them; the storage layer and Image type already support it.
    """

    mime_type: str
    size_bytes: int
    width: int | None = None
    height: int | None = None


class ContentValidator(ABC):
    """Implement this to let put()/put_many() accept a content type
    ZeroBucket doesn't natively understand.

    See zerobucket.validators.pdf.PDFValidator for a complete, real
    reference implementation.
    """

    @abstractmethod
    def validate(self, data: bytes, *, max_bytes: int) -> ValidatedContent:
        """Validate raw bytes and return derived metadata.

        Must raise (ideally a ContentValidationError or subclass) if the
        content is invalid -- empty, oversized, wrong format, corrupted,
        whatever "invalid" means for this content type. Must NOT trust
        anything about the content other than the bytes themselves (no
        filename, no caller-supplied claims) -- same principle the
        built-in image validator follows.
        """
