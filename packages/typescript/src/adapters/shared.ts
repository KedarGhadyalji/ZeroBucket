/** Helpers shared by every storage adapter (kept here so Postgres and SQLite cannot drift apart). */
import { StorageError } from "../errors.js";
import type { ObjectStorageLike } from "../object-storage.js";
import type { StoredRecord } from "./base.js";

/** 1 MiB. Default chunk size for getStream(). */
export const DEFAULT_STREAM_CHUNK_SIZE = 1024 * 1024;

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** True if `id` is a syntactically valid UUID. Malformed ids can never exist, so they short-circuit to "not found". */
export function isValidId(id: string): boolean {
  return typeof id === "string" && UUID_RE.test(id);
}

export function msg(exc: unknown): string {
  return exc instanceof Error ? exc.message : String(exc);
}

export function wrap(prefix: string, exc: unknown): StorageError {
  return exc instanceof StorageError
    ? exc
    : new StorageError(`${prefix}: ${msg(exc)}`, { cause: exc });
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function mapLimit<T, R>(
  items: T[],
  limit: number,
  fn: (item: T) => Promise<R>,
): Promise<R[]> {
  const out: R[] = new Array(items.length);
  let next = 0;
  await Promise.all(
    Array.from({ length: Math.min(limit, items.length) }, async () => {
      while (next < items.length) {
        const i = next++;
        out[i] = await fn(items[i]!);
      }
    }),
  );
  return out;
}

export function tieredWithoutStorage(
  id: string,
  key: string | null,
): StorageError {
  return new StorageError(
    `Image ${JSON.stringify(id)} is stored in object storage (key=${JSON.stringify(key)}) ` +
      "but this ZeroBucket was constructed without `objectStorage`. Configure it with the " +
      "same bucket and credentials that were used to tier this image.",
  );
}

/** Turn a DB row into a record, fetching the bytes from object storage if the row is tiered. */
export async function hydrateRow(
  row: any,
  id: string,
  os: ObjectStorageLike | undefined,
): Promise<StoredRecord> {
  let data: Buffer;
  if (row.storage_backend === "object_storage") {
    if (!os) throw tieredWithoutStorage(id, row.object_storage_key);
    data = await os.download(row.object_storage_key);
    if (data.length !== row.size_bytes) {
      throw new StorageError(
        `Object ${JSON.stringify(row.object_storage_key)} for image ${JSON.stringify(id)} has ${data.length} bytes ` +
          `but the database records ${row.size_bytes}. The stored object is corrupted or was replaced.`,
      );
    }
  } else {
    data = row.data as Buffer;
  }
  return {
    id: String(row.id),
    data,
    mimeType: row.mime_type,
    originalFilename: row.original_filename,
    sizeBytes: row.size_bytes,
    width: row.width,
    height: row.height,
    checksumSha256: row.checksum_sha256,
  };
}

/** Ranged reads straight from the object store; fails loudly rather than ever truncating. */
export async function* streamFromObjectStorage(
  os: ObjectStorageLike,
  id: string,
  key: string,
  start: number,
  end: number,
  chunkSize: number,
): AsyncGenerator<Buffer> {
  let offset = start;
  while (offset <= end) {
    const last = Math.min(offset + chunkSize - 1, end);
    const expected = last - offset + 1;
    const chunk = await os.downloadRange(key, offset, last);
    if (chunk.length !== expected) {
      throw new StorageError(
        `Object ${JSON.stringify(key)} for image ${JSON.stringify(id)} returned ${chunk.length} bytes for a ${expected}-byte range; ` +
          "the stored object is shorter than the database records.",
      );
    }
    yield chunk;
    offset = last + 1;
  }
}

/** 1024-based, like Postgres' pg_size_pretty: "812 bytes", "13.2 kB", "4.0 MB". */
export function prettySize(bytes: number): string {
  if (bytes < 1024) return `${bytes} bytes`;
  const units = ["kB", "MB", "GB", "TB"];
  let v = bytes / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v.toFixed(1)} ${units[i]}`;
}
