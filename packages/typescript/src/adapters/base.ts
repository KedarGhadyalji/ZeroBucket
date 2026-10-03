/**
 * Storage backend interface.
 *
 * CRITICAL DESIGN RULE (same as the Python package): backends know nothing about
 * images. They store and retrieve rows of bytes + metadata columns. All
 * image-specific logic (validation, optimization, checksums) lives in client.ts.
 *
 * Every method accepts an optional `connection`. When omitted, the backend uses
 * its own pool: each call commits independently and transient failures are
 * retried. When provided, the backend uses it directly and does NOT commit,
 * roll back or retry. That is what lets a call join the caller's transaction.
 */
import type { PreparedRow, Queryable } from "../types.js";

export interface StoredRecord {
  id: string;
  data: Buffer;
  mimeType: string;
  originalFilename: string | null;
  sizeBytes: number;
  width: number | null;
  height: number | null;
  checksumSha256: string;
}

export type StoredRecordMetadata = Omit<StoredRecord, "data">;

export interface CallOptions {
  connection?: Queryable;
}

export interface StreamOptions extends CallOptions {
  chunkSize: number;
  /** Inclusive byte range [start, end]; `end` is clamped to the object size. Omit for the whole object. */
  range?: { start: number; end: number };
}

export interface StorageBackend {
  /** Resolve once the backend is connected and migrated. Safe to call repeatedly. */
  ready(): Promise<void>;
  put(row: PreparedRow, opts?: CallOptions): Promise<string>;
  putMany(rows: PreparedRow[], opts?: CallOptions): Promise<string[]>;
  get(id: string, opts?: CallOptions): Promise<StoredRecord | null>;
  /** Missing ids are simply absent. Result order is NOT guaranteed to match input. */
  getMany(ids: string[], opts?: CallOptions): Promise<StoredRecord[]>;
  getMetadata(
    id: string,
    opts?: CallOptions,
  ): Promise<StoredRecordMetadata | null>;
  /**
   * Resolves to null if the id does not exist (checked eagerly, before any
   * chunk is read). A row deleted mid-stream makes iteration throw
   * StorageError rather than silently truncating.
   */
  getStream(
    id: string,
    opts: StreamOptions,
  ): Promise<AsyncIterable<Buffer> | null>;
  delete(id: string, opts?: CallOptions): Promise<boolean>;
  /** Returns the ids that were actually deleted. */
  deleteMany(ids: string[], opts?: CallOptions): Promise<string[]>;
  exists(id: string, opts?: CallOptions): Promise<boolean>;
  close(): Promise<void>;
}
