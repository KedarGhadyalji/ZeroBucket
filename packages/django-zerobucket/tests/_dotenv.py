"""Load ZEROBUCKET_* test settings from the repository's `.env` file.

The npm tests have always read `.env`; this gives the Python tests the same
behaviour so one file configures every suite and nothing has to be `set` in
each new terminal window.

Rules (the same as the npm side):
- `.env` lives at the repository root (copy `.env.example` to `.env`).
- A value in `.env` WINS over a variable already set in the shell.
- Only keys starting with `ZEROBUCKET_` are read; nothing else is touched.
- A key with an empty value is ignored, so it can never blank out a real one.
- Passwords are never printed. Only the NAMES of the loaded keys are reported.

No dependency: a tiny parser (comments, `export KEY=...`, single or double
quotes, a UTF-8 BOM as written by Windows Notepad). A copy of this file lives
in packages/django-zerobucket/tests/ because the two suites are separate
packages; keep them identical.
"""

from __future__ import annotations

import os
from collections.abc import MutableMapping
from pathlib import Path

PREFIX = "ZEROBUCKET_"
DEFAULT_PATH = Path(__file__).resolve().parents[3] / ".env"


def parse_dotenv(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        key, sep, value = line.partition("=")
        key = key.strip()
        if not sep or not key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] in ("'", '"') and value.endswith(value[0]):
            value = value[1:-1]
        else:
            comment = value.find(" #")  # an inline comment needs a space before it
            if comment != -1:
                value = value[:comment].rstrip()
        values[key] = value
    return values


def load_dotenv(
    path: Path | None = None, environ: MutableMapping[str, str] | None = None
) -> list[str]:
    """Apply the ZEROBUCKET_* values from `path` to `environ` (default:
    os.environ). Returns the names that were set; an absent or unreadable
    file returns an empty list."""
    target = os.environ if environ is None else environ
    try:
        text = (path or DEFAULT_PATH).read_text(encoding="utf-8-sig")
    except OSError:
        return []
    loaded: list[str] = []
    for key, value in parse_dotenv(text).items():
        if key.startswith(PREFIX) and value != "":
            target[key] = value
            loaded.append(key)
    return loaded
