# Security Policy

## Reporting a vulnerability

Please do not open a public GitHub issue for security vulnerabilities.
Instead, email the maintainers (address TBD once the repository is
published) with a description of the issue and reproduction steps.

## Scope

ZeroBucket treats all uploaded images as untrusted input. Areas of
particular interest for security review:

- Image parsing (decompression bombs, malformed files, parser
  vulnerabilities in Pillow itself)
- SQL query construction (all queries must remain parameterized --
  see `packages/python/src/zerobucket/adapters/postgres.py`)
- Size and pixel-count limits (`validation.py`)
- MIME/format trust (must be derived from content, never from filename
  or client-supplied headers)

## Supported versions

Pre-1.0: only the latest released version receives security fixes.
