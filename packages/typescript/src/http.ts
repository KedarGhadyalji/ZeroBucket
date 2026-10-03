/**
 * HTTP serving helpers: `import { createImageHandler } from "zerobucket/http"`.
 *
 * `createImageHandler()` returns a Web-standard `(Request) => Promise<Response>`,
 * which is what Next.js route handlers, Hono, Remix, SvelteKit, Bun, Deno and
 * Cloudflare-style runtimes all speak. `toNodeHandler()` adapts it to
 * Express / Fastify / plain `node:http`.
 *
 * Implements: ETag (the content SHA-256) + If-None-Match -> 304; Range /
 * If-Range -> 206 (single byte range) with 416 for unsatisfiable ranges;
 * HEAD; X-Content-Type-Options: nosniff. Bytes are streamed in chunks, never
 * fully buffered.
 */
import type { IncomingMessage, ServerResponse } from "node:http";
import { Readable } from "node:stream";
import { AccessDeniedError, ImageNotFoundError } from "./errors.js";
import type { ZeroBucket } from "./client.js";

export type ImageHandler = (
  req: Request,
  ctx?: { id?: string },
) => Promise<Response>;

export interface ImageHandlerOptions<C = unknown> {
  /** Extract the id from the request. Default: the last non-empty path segment. Ignored if `ctx.id` is passed. */
  getId?: (req: Request) => string | undefined;
  /** Build the context handed to your beforeGet hook (e.g. the signed-in user from cookies). */
  getContext?: (req: Request) => C | undefined | Promise<C | undefined>;
  /**
   * Cache-Control header. The default, "private, no-cache", is safe for
   * access-controlled images: browsers always revalidate, and the ETag makes
   * that a cheap 304. For public images pass e.g. "public, max-age=31536000, immutable"
   * (ids are never reused and content never mutates).
   */
  cacheControl?: string;
  /** "inline" (default) or "attachment" (adds the stored filename). */
  disposition?: "inline" | "attachment";
  /** Bytes per DB read while streaming. Default 1 MiB. */
  chunkSize?: number;
  /** Called with unexpected errors (anything other than not-found / denied) so you can log them. The response is a generic 500. */
  onError?: (error: unknown, req: Request) => void;
}

export function createImageHandler<C = unknown>(
  images: ZeroBucket<C>,
  options: ImageHandlerOptions<C> = {},
): ImageHandler {
  const cacheControl = options.cacheControl ?? "private, no-cache";

  return async function handle(req, ctx) {
    if (req.method !== "GET" && req.method !== "HEAD") {
      return new Response("Method Not Allowed", {
        status: 405,
        headers: { Allow: "GET, HEAD" },
      });
    }

    const id = ctx?.id ?? (options.getId ?? defaultGetId)(req);
    if (!id) return notFound();

    try {
      const context = options.getContext
        ? await options.getContext(req)
        : undefined;
      const meta = await images.metadata(id, { context });
      const etag = `"${meta.checksumSha256}"`;
      const size = meta.sizeBytes;

      const base: Record<string, string> = {
        ETag: etag,
        "Cache-Control": cacheControl,
        "Accept-Ranges": "bytes",
        "X-Content-Type-Options": "nosniff",
        "Content-Type": meta.mimeType,
      };
      if (options.disposition === "attachment") {
        base["Content-Disposition"] = attachmentHeader(
          meta.filename ?? "download",
        );
      }

      if (etagMatches(req.headers.get("if-none-match"), etag)) {
        return new Response(null, {
          status: 304,
          headers: { ETag: etag, "Cache-Control": cacheControl },
        });
      }

      // Range is honoured unless If-Range is present and does not match our (strong) ETag.
      let range: ReturnType<typeof parseRange> = null;
      const rangeHeader = req.headers.get("range");
      const ifRange = req.headers.get("if-range");
      if (rangeHeader && (ifRange === null || ifRange.trim() === etag)) {
        range = parseRange(rangeHeader, size);
      }

      if (range === "unsatisfiable") {
        return new Response(null, {
          status: 416,
          headers: {
            ...base,
            "Content-Range": `bytes */${size}`,
            "Content-Length": "0",
          },
        });
      }

      const status = range ? 206 : 200;
      const length = range ? range.end - range.start + 1 : size;
      const headers: Record<string, string> = {
        ...base,
        "Content-Length": String(length),
      };
      if (range)
        headers["Content-Range"] = `bytes ${range.start}-${range.end}/${size}`;

      if (req.method === "HEAD") return new Response(null, { status, headers });

      const body = await images.toWebStream(id, {
        context,
        chunkSize: options.chunkSize,
        range: range ? { start: range.start, end: range.end } : undefined,
      });
      return new Response(body, { status, headers });
    } catch (exc) {
      if (exc instanceof ImageNotFoundError) return notFound();
      if (exc instanceof AccessDeniedError)
        return new Response("Forbidden", { status: 403 });
      options.onError?.(exc, req);
      return new Response("Internal Server Error", { status: 500 });
    }
  };
}

// ---- Node adapter --------------------------------------------------------

/**
 * Adapt an ImageHandler to Node's `(req, res)` signature (Express, Fastify's raw
 * handler, plain `http.createServer`).
 *
 *   app.get("/images/:id", (req, res) => nodeHandler(req, res, { id: req.params.id }));
 */
export function toNodeHandler(handler: ImageHandler) {
  return async function nodeHandler(
    req: IncomingMessage,
    res: ServerResponse,
    ctx?: { id?: string },
  ): Promise<void> {
    const host = req.headers.host ?? "localhost";
    const headers = new Headers();
    for (const [k, v] of Object.entries(req.headers)) {
      if (Array.isArray(v)) v.forEach((x) => headers.append(k, x));
      else if (v !== undefined) headers.set(k, v);
    }
    const request = new Request(`http://${host}${req.url ?? "/"}`, {
      method: req.method,
      headers,
    });
    const response = await handler(request, ctx);

    res.writeHead(response.status, Object.fromEntries(response.headers));
    if (!response.body) {
      res.end();
      return;
    }
    const nodeStream = Readable.fromWeb(
      response.body as import("node:stream/web").ReadableStream,
    );
    // If the client disconnects, stop reading from the database.
    res.on("close", () => nodeStream.destroy());
    nodeStream.on("error", () => res.destroy());
    nodeStream.pipe(res);
  };
}

// ---- helpers (exported for tests) ----------------------------------------

function notFound(): Response {
  return new Response("Not Found", { status: 404 });
}

function defaultGetId(req: Request): string | undefined {
  const segments = new URL(req.url).pathname.split("/").filter(Boolean);
  const last = segments[segments.length - 1];
  return last ? decodeURIComponent(last) : undefined;
}

/** Weak comparison, per RFC 9110 for If-None-Match. */
export function etagMatches(header: string | null, etag: string): boolean {
  if (!header) return false;
  if (header.trim() === "*") return true;
  const strip = (t: string) => t.trim().replace(/^W\//, "");
  return header.split(",").some((t) => strip(t) === etag);
}

/**
 * Parse a single `bytes=` range against a resource of `size` bytes.
 *  - returns a clamped inclusive range
 *  - "unsatisfiable" -> respond 416
 *  - null            -> ignore the header and serve the full body (syntax we don't handle, incl. multi-range)
 */
export function parseRange(
  header: string,
  size: number,
): { start: number; end: number } | "unsatisfiable" | null {
  const m = /^bytes=(\d*)-(\d*)$/i.exec(header.trim());
  if (!m) return null; // also rejects multi-range ("bytes=0-1,5-6")
  const [, a, b] = m as unknown as [string, string, string];
  if (a === "" && b === "") return null;

  if (a === "") {
    const n = Number(b); // suffix: last n bytes
    if (n === 0) return "unsatisfiable";
    return { start: Math.max(0, size - n), end: size - 1 };
  }
  const start = Number(a);
  if (start >= size) return "unsatisfiable";
  if (b === "") return { start, end: size - 1 };
  const end = Number(b);
  if (end < start) return null; // syntactically invalid -> ignore
  return { start, end: Math.min(end, size - 1) };
}

function attachmentHeader(filename: string): string {
  const ascii = filename.replace(/[^\x20-\x7e]/g, "_").replace(/["\\]/g, "_");
  return `attachment; filename="${ascii}"; filename*=UTF-8''${encodeURIComponent(filename)}`;
}
