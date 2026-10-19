/**
 * A tiny HTTP server on `node:http` only (no framework), so the ideas are
 * visible. In Next.js / Express / Fastify the same three calls apply:
 * `files.put(file)`, `createImageHandler(files, ...)` and (for Node servers)
 * `toNodeHandler(...)`. See the zerobucket README for the framework snippets.
 */
import { createServer, type IncomingMessage, type Server } from "node:http";
import { createImageHandler, toNodeHandler } from "zerobucket/http";
import { ContentValidationError } from "zerobucket";
import { files } from "./db.js";
import { createPost, getPost } from "./posts.js";

const MAX_BODY_BYTES = 25 * 1024 * 1024;

// Immutable is safe here: ids are never reused and an image's bytes never
// change under the same id. (Remove it if your app ever edits in place.)
const serveImage = toNodeHandler(
  createImageHandler(files, {
    cacheControl: "public, max-age=31536000, immutable",
    getContext: () => ({ kind: "image" as const }),
  }),
);

// PDFs download rather than render in your origin, and revalidate each time.
const serveDocument = toNodeHandler(
  createImageHandler(files, {
    disposition: "attachment",
    getContext: () => ({ kind: "document" as const }),
  }),
);

class BodyTooLarge extends Error {}

/** Read the request body (bounded) and let the platform parse multipart for us. */
async function readForm(req: IncomingMessage): Promise<FormData> {
  const chunks: Buffer[] = [];
  let total = 0;
  for await (const chunk of req) {
    total += (chunk as Buffer).length;
    if (total > MAX_BODY_BYTES) throw new BodyTooLarge();
    chunks.push(chunk as Buffer);
  }
  const headers = new Headers();
  headers.set("content-type", String(req.headers["content-type"] ?? ""));
  return new Request("http://localhost/", {
    method: "POST",
    headers,
    body: Buffer.concat(chunks),
  }).formData();
}

function json(
  res: import("node:http").ServerResponse,
  status: number,
  body: unknown,
) {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
}

const PAGE = `<!doctype html><meta charset="utf-8"><title>ZeroBucket example</title>
<h1>New post</h1>
<form method="post" action="/posts" enctype="multipart/form-data">
  <p><input name="title" placeholder="Title" required></p>
  <p>Image: <input type="file" name="image" accept="image/*"></p>
  <p>PDF: <input type="file" name="document" accept="application/pdf"></p>
  <p><button>Create</button></p>
</form>`;

export function createApp(): Server {
  return createServer(async (req, res) => {
    try {
      const url = new URL(req.url ?? "/", "http://localhost");
      const method = req.method ?? "GET";

      if (method === "GET" && url.pathname === "/") {
        res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
        return void res.end(PAGE);
      }

      if (method === "POST" && url.pathname === "/posts") {
        const form = await readForm(req);
        const title = String(form.get("title") ?? "").trim();
        if (!title) return json(res, 400, { error: "title is required" });
        const image = form.get("image");
        const document = form.get("document");
        const post = await createPost({
          title,
          // An empty file input arrives as a 0-byte File: treat it as "none".
          image: image instanceof File && image.size > 0 ? image : undefined,
          document:
            document instanceof File && document.size > 0
              ? document
              : undefined,
        });
        return json(res, 201, {
          ...post,
          imageUrl: post.imageId && `/images/${post.imageId}`,
          documentUrl: post.documentId && `/documents/${post.documentId}`,
        });
      }

      const post = url.pathname.match(/^\/posts\/(\d+)$/);
      if (method === "GET" && post) {
        const found = await getPost(Number(post[1]));
        return found
          ? json(res, 200, found)
          : json(res, 404, { error: "not found" });
      }

      const image = url.pathname.match(/^\/images\/([^/]+)$/);
      if ((method === "GET" || method === "HEAD") && image)
        return await serveImage(req, res, { id: image[1] });

      const doc = url.pathname.match(/^\/documents\/([^/]+)$/);
      if ((method === "GET" || method === "HEAD") && doc)
        return await serveDocument(req, res, { id: doc[1] });

      json(res, 404, { error: "not found" });
    } catch (err) {
      if (err instanceof BodyTooLarge)
        return json(res, 413, { error: "request too large" });
      // Bad image / not a PDF / too large: the caller's fault, not ours.
      if (err instanceof ContentValidationError)
        return json(res, 400, { error: err.message });
      console.error(err);
      json(res, 500, { error: "internal error" });
    }
  });
}
