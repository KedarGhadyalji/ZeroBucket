/**
 * One database, one pool.
 *
 * The app's own tables and ZeroBucket's tables live in the SAME PostgreSQL
 * database and share ONE connection pool. That is what makes the headline
 * feature possible (see posts.ts): a post row, its image and its PDF can be
 * written in a single transaction, so there is no "post exists but its image
 * upload silently failed" class of bug, and nothing to clean up afterwards.
 */
import { Pool } from "pg";
import { PDFValidator, ZeroBucket } from "zerobucket";

export const DATABASE_URL =
  process.env.DATABASE_URL ??
  "postgresql://postgres:postgres@localhost:5432/zerobucket_test";

/** Your app's pool. ZeroBucket borrows it and never closes it. */
export const pool = new Pool({ connectionString: DATABASE_URL, max: 10 });

/** What a request is asking for. Passed to the access hook below. */
export type Kind = "image" | "document";

/**
 * Images and PDFs both go through ZeroBucket; the `kind` in the call context
 * decides which of the post's columns must reference the id before it is
 * served. Without this, /images/<id> would happily serve a PDF's bytes (and
 * /documents/<id> an image's), because both live in the same table.
 *
 * Calls WITHOUT a context are your own server-side code, so they are
 * allowed. Only requests that carry a context are checked. A hook that
 * throws fails closed: the request is never treated as allowed.
 */
export const files = new ZeroBucket<{ kind: Kind }>({
  pool,
  // PDFs are bounded by this too (PDFValidator caps at min(maxBytes, 20 MiB)).
  maxBytes: 20 * 1024 * 1024,
  beforeGet: async (id, ctx) => {
    if (!ctx) return true;
    // `id` is always a well-formed UUID here: ZeroBucket answers a malformed id with
    // "not found" before your hook runs, so it is safe to use in SQL as-is.
    const column = ctx.kind === "image" ? "image_id" : "document_id";
    const { rowCount } = await pool.query(
      `SELECT 1 FROM example_posts WHERE ${column} = $1`,
      [id],
    );
    return (rowCount ?? 0) > 0;
  },
});

/** Content validator for PDFs (magic bytes + size; a sanity gate, not a sanitizer). */
export const pdf = new PDFValidator();

export async function initSchema(): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS example_posts (
      id          SERIAL PRIMARY KEY,
      title       TEXT NOT NULL,
      image_id    UUID,
      document_id UUID,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);
}
