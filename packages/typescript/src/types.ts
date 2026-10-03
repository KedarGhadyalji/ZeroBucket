/** Public data types returned by the ZeroBucket SDK. */

/** A retrieved image, ready to be served or written to disk. `data` is raw bytes, never Base64. */
export interface Image {
  data: Buffer;
  mimeType: string;
  filename: string | null;
  sizeBytes: number;
  width: number | null;
  height: number | null;
  checksumSha256: string;
}

/** Metadata about a stored image, without the pixel data. */
export interface ImageMetadata {
  imageId: string;
  mimeType: string;
  filename: string | null;
  sizeBytes: number;
  width: number | null;
  height: number | null;
  checksumSha256: string;
}

/** Outcome of one item in a `putMany()` call. Batches are best-effort, not all-or-nothing. */
export interface BatchPutResult {
  /** Position in the input array. */
  index: number;
  imageId: string | null;
  error: string | null;
  success: boolean;
}

/** Outcome of one id in a `getMany()` call. A missing id is not an exception here. */
export interface BatchGetResult {
  imageId: string;
  image: Image | null;
  error: string | null;
  success: boolean;
}

/** Outcome of one id in a `deleteMany()` call. `deleted: false` with no error means "did not exist". */
export interface BatchDeleteResult {
  imageId: string;
  deleted: boolean;
  error: string | null;
  success: boolean;
}

/** Emitted to `onOperation` after every storage operation (success or failure). */
export interface OperationEvent {
  operation: string;
  durationMs: number;
  success: boolean;
  error: string | null;
  retryCount: number;
}

/**
 * Anything with a pg-style `query()`: a `pg.Client`, a `pg.PoolClient`, or a
 * transaction handle from a wrapper. Pass one as `connection` to make a
 * ZeroBucket call participate in YOUR transaction. ZeroBucket then neither
 * commits, rolls back, nor retries.
 */
export interface Queryable {
  query(
    text: string,
    values?: unknown[],
  ): Promise<{ rows: any[]; rowCount: number | null }>;
}

/** The row shape a storage backend persists (after validation and checksumming). */
export interface PreparedRow {
  data: Buffer;
  mimeType: string;
  originalFilename: string | null;
  sizeBytes: number;
  width: number | null;
  height: number | null;
  checksumSha256: string;
}
