"""The one ZeroBucket client this app uses. Import `images` from here
everywhere else -- don't construct a second ZeroBucket() instance
elsewhere (each instance owns its own connection pool; you want one
pool, not several competing for connections).

Config decisions made here, and why:

- max_retries / retry_base_delay: left at the library defaults (3
  retries, 0.1s base). Measured worst-case added backoff for 3 retries
  at this base delay is under ~1 second total (0.1-0.2s, 0.2-0.3s,
  0.4-0.5s, before the retried round trips themselves) -- cheap enough
  that fast-failing to save that ~1s isn't worth losing real resilience
  against transient blips (a deadlock or dropped connection that would
  otherwise surface as a failed upload for something that would have
  succeeded moments later). If a specific endpoint has a much tighter
  latency budget than the rest of the app, override per-call is not
  currently possible (retry config is set at construction, not per
  put()) -- that's a real current limitation, not an oversight; construct
  a second, differently-configured ZeroBucket() instance for that
  specific endpoint if you hit this in practice.

- max_bytes: left at the library default (8MB). See
  benchmarks/RESULTS.md in the zerobucket repo for the latency/memory
  data behind that number if you need to reconsider it for this app.
"""

from __future__ import annotations

from zerobucket import ZeroBucket

from .db import DATABASE_URL

images = ZeroBucket(database_url=DATABASE_URL)
