/**
 * Atomic writes: a post row + an image + a PDF in ONE transaction.
 *
 * Passing `connection: client` makes every ZeroBucket call part of YOUR
 * transaction. ZeroBucket does not commit or roll back; you do. If anything
 * fails (a corrupt image, a file that is not a PDF, a crash), the post, the
 * image and the document all disappear together.
 */
import { files, pdf, pool } from "./db.js";

export interface NewPost {
  title: string;
  image?: Blob;
  document?: Blob;
}

export interface Post {
  id: number;
  title: string;
  imageId: string | null;
  documentId: string | null;
}

export async function createPost(input: NewPost): Promise<Post> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const { rows } = await client.query(
      "INSERT INTO example_posts (title) VALUES ($1) RETURNING id",
      [input.title],
    );
    const id: number = rows[0].id;

    // Same `client` as the INSERT above, so it is the same transaction.
    const imageId = input.image
      ? await files.put(input.image, {
          connection: client,
          // Resize and re-encode: strips metadata (GPS!), applies EXIF rotation.
          optimize: { maxWidth: 1600, format: "webp" },
        })
      : null;

    const documentId = input.document
      ? await files.put(input.document, { connection: client, validator: pdf })
      : null;

    await client.query(
      "UPDATE example_posts SET image_id = $1, document_id = $2 WHERE id = $3",
      [imageId, documentId, id],
    );

    await client.query("COMMIT");
    return { id, title: input.title, imageId, documentId };
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

export async function getPost(id: number): Promise<Post | null> {
  const { rows } = await pool.query(
    "SELECT id, title, image_id, document_id FROM example_posts WHERE id = $1",
    [id],
  );
  if (rows.length === 0) return null;
  const r = rows[0];
  return {
    id: r.id,
    title: r.title,
    imageId: r.image_id,
    documentId: r.document_id,
  };
}
