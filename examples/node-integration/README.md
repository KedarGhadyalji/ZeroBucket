# Node integration example

A small, runnable full-stack app built on the [`zerobucket`](../../packages/typescript/README.md) npm package. It is
the npm counterpart of the Python example in [`../zerobucket_integration`](../zerobucket_integration), and shows the
things that are easy to get wrong when you wire this up yourself:

| Idea                                                                                                   | Where                                  |
| ------------------------------------------------------------------------------------------------------ | -------------------------------------- |
| **One transaction** for your own row, an image and a PDF: if anything fails, nothing is left behind    | [`src/posts.ts`](src/posts.ts)         |
| **One database, one connection pool** shared by your app and ZeroBucket                                | [`src/db.ts`](src/db.ts)               |
| **Serving** with ETag / 304, Range / 206, `nosniff`, long caching for images and `attachment` for PDFs | [`src/server.ts`](src/server.ts)       |
| **Access control** so `/images/:id` cannot serve a PDF's bytes and vice versa                          | [`src/db.ts`](src/db.ts) (`beforeGet`) |
| **Image optimizing** (resize, WebP, strips metadata, applies EXIF rotation) and a **PDF validator**    | [`src/posts.ts`](src/posts.ts)         |
| An **end-to-end check** that proves all of the above over real HTTP                                    | [`src/verify.ts`](src/verify.ts)       |

It uses only `node:http` (no web framework) so the ideas stay visible. In Next.js, Express or Fastify the same calls
apply: `files.put(file)`, `createImageHandler(files, ...)`, and `toNodeHandler(...)` for Node servers (the main README
has the framework snippets).

## Run it

You need Node 20 or newer and a PostgreSQL database you can write to.

```bash
cd examples/node-integration
npm install
export DATABASE_URL=postgresql://postgres:YOUR_PASSWORD@localhost:5432/mydb   # Windows: set DATABASE_URL=...
npm start                       # http://localhost:3000, with an upload form
```

ZeroBucket creates its own table on first use, and the example creates one small table, `example_posts`. Use a
scratch database, not production.

If the `zerobucket` version this example asks for is newer than the one on npm, install your local build instead
(from the repo root: `npm run build --prefix packages/typescript`, `npm pack --prefix packages/typescript`, then
`npm install ./packages/typescript/zerobucket-*.tgz` in this folder).

## Check that it works

```bash
npm run verify
```

This starts the server on a free port, drives it over real HTTP and exits `0` only if every check passes. It cleans up
after itself. It checks, among other things, that:

- an upload with an image and a PDF succeeds, and the image is re-encoded as WebP;
- `If-None-Match` returns `304` and `Range: bytes=0-9` returns `206` with 10 bytes;
- an image id is refused (`403`) on the documents route and a PDF id on the images route;
- a malformed id is a `404`, not a `500` (this exercises the library, not example code);
- **a failed upload leaves nothing behind**: an upload whose image is valid but whose "PDF" is not returns `400`, and
  afterwards there is no post row **and** the already-written image is gone too.

`npm run typecheck` type-checks the example.

## Things this example deliberately keeps simple

- **No login.** A real app would put the signed-in user in the call context and check ownership in `beforeGet`. The
  hook here only checks that an id belongs to the right kind of attachment.
- **Your hook only sees well-formed ids.** ZeroBucket answers a malformed id with "not found" before `beforeGet` runs,
  so the hook can use the id in SQL as-is. (On zerobucket 0.8.x the hook received the raw string and had to validate it
  first, which is why this example requires 0.9.0.)
- **A denied id is a `403`.** The handler answers `403` when the hook says no, including for ids that no post
  references, so a missing id and a forbidden id look the same to the caller.
- **Upload size.** The body is capped at 25 MB and ZeroBucket's `maxBytes` at 20 MiB. Adjust both to your needs.
- **PDFs live in the database too.** That keeps one backup and one transaction, but see the Limitations section of the
  main README before storing large or high-volume files.
