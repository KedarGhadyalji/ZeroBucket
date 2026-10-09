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
import type { Connection, PreparedRow } from "../types.js";

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
  connection?: Connection;
}

export interface StreamOptions extends CallOptions {
  chunkSize: number;
  /** Inclusive byte range [start, end]; `end` is clamped to the object size. Omit for the whole object. */
  range?: { start: number; end: number };
}

/** Filters for maintenance listings (CLI `verify` / `tier`). All optional; combined with AND. */
export interface ImageListOptions {
  /** Return a random sample of this many images instead of all of them. */
  sample?: number;
  limit?: number;
  /** Only images with size_bytes >= this. */
  minSize?: number;
  /** Only images created at least this many days ago. */
  olderThanDays?: number;
  /** Only images whose bytes are still in the database (not tiered). */
  onlyUntiered?: boolean;
}
export interface ImageListEntry {
  id: string;
  /** Bytes live in object storage, not in the database. */
  tiered: boolean;
}

/** Storage statistics for `zerobucket info`. */
export interface StorageInfo {
  mode: "classic" | "dedup";
  /** Number of images (references, in dedup mode). */
  count: number;
  /** Sum of every image's size_bytes as recorded by the application (logical size). */
  totalBytes: number;
  oldest: string | null;
  newest: string | null;
  /** Human-readable on-disk size, if the database can tell. */
  onDisk: string | null;
  byFormat: { mimeType: string; count: number; bytes: number }[];
  /** Classic mode: images currently living in object storage. */
  tiered?: { count: number; bytes: number };
  /** Dedup mode: distinct stored blobs and the bytes they physically occupy. */
  dedup?: { blobs: number; storedBytes: number };
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
  /** null = no such image, false = already tiered (no-op), true = tiered now. Needs object storage configured. */
  tierToObjectStorage(id: string, opts?: CallOptions): Promise<boolean | null>;
  /** Dedup-mode instances only: copy the classic table into the dedup tables (non-destructive). */
  migrateClassicToDedup?(): Promise<{
    imagesMigrated: number;
    distinctBlobsCreated: number;
    duplicateReferencesFound: number;
    alreadyMigrated: number;
  }>;
  /** Maintenance: storage statistics, or null if the tables do not exist yet. */
  getInfo?(): Promise<StorageInfo | null>;
  /** Maintenance: list ids matching the filters (oldest first, or random when sampling). */
  listImages?(opts?: ImageListOptions): Promise<ImageListEntry[]>;
  close(): Promise<void>;
}
