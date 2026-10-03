/**
 * PostgreSQL storage adapter (classic mode).
 *
 * Uses the EXACT schema the Python package creates (zerobucket_images, including
 * the object-storage tiering columns and CHECK constraint), so a Node app and a
 * Python app can share one database and read each other's rows.
 *
 * Lessons from the Python build, applied here from day one:
 *  - Retry re-runs the WHOLE unit of work on a fresh connection after a full
 *    ROLLBACK. It never replays a single statement of a multi-statement
 *    transaction (the "half-applied put" hazard).
 *  - Retry never applies when the caller passes `connection` (the caller owns
 *    the transaction; restarting it is their decision).
 *  - Pool acquire has an explicit timeout.
 *  - Migration runs under an advisory lock so N concurrent first-callers do not
 *    race on CREATE TABLE.
 */
import type { Pool, PoolClient, PoolConfig } from "pg";
import { StorageError } from "../errors.js";
import type { ObjectStorageLike } from "../object-storage.js";
import type { OperationEvent, PreparedRow, Queryable } from "../types.js";
import type {
  CallOptions,
  StorageBackend,
  StoredRecord,
  StoredRecordMetadata,
  StreamOptions,
} from "./base.js";

/** 1 MiB. Default chunk size for getStream(). */
export const DEFAULT_STREAM_CHUNK_SIZE = 1024 * 1024;

const SCHEMA = `
CREATE TABLE IF NOT EXISTS zerobucket_images (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    data                BYTEA NOT NULL,
    mime_type           TEXT NOT NULL,
    original_filename   TEXT,
    size_bytes          INTEGER NOT NULL,
    width               INTEGER,
    height              INTEGER,
    checksum_sha256     CHAR(64) NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_zerobucket_checksum ON zerobucket_images (checksum_sha256);
CREATE INDEX IF NOT EXISTS idx_zerobucket_created_at ON zerobucket_images (created_at);

-- Object-storage tiering columns (additive; identical to the Python package).
ALTER TABLE zerobucket_images ALTER COLUMN data DROP NOT NULL;
ALTER TABLE zerobucket_images
    ADD COLUMN IF NOT EXISTS storage_backend TEXT NOT NULL DEFAULT 'postgres';
ALTER TABLE zerobucket_images ADD COLUMN IF NOT EXISTS object_storage_bucket TEXT;
ALTER TABLE zerobucket_images ADD COLUMN IF NOT EXISTS object_storage_key TEXT;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'zerobucket_storage_location_check'
    ) THEN
        ALTER TABLE zerobucket_images ADD CONSTRAINT zerobucket_storage_location_check
        CHECK (
            (storage_backend = 'postgres'
                AND data IS NOT NULL
                AND object_storage_key IS NULL
                AND object_storage_bucket IS NULL)
            OR
            (storage_backend = 'object_storage'
                AND data IS NULL
                AND object_storage_key IS NOT NULL
                AND object_storage_bucket IS NOT NULL)
        );
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_zerobucket_storage_backend
    ON zerobucket_images (storage_backend) WHERE storage_backend != 'postgres';
`;

const INSERT = `
INSERT INTO zerobucket_images
    (data, mime_type, original_filename, size_bytes, width, height, checksum_sha256)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING id;`;

const SELECT_FULL = `
SELECT id, data, mime_type, original_filename, size_bytes, width, height,
       checksum_sha256, storage_backend, object_storage_bucket, object_storage_key
FROM zerobucket_images WHERE id = $1;`;

const SELECT_MANY = `
SELECT id, data, mime_type, original_filename, size_bytes, width, height,
       checksum_sha256, storage_backend, object_storage_bucket, object_storage_key
FROM zerobucket_images WHERE id = ANY($1::uuid[]);`;

const SELECT_METADATA = `
SELECT id, mime_type, original_filename, size_bytes, width, height, checksum_sha256
FROM zerobucket_images WHERE id = $1;`;

// substring() is 1-indexed and clamps `length` at the value's end.
const SELECT_CHUNK = `SELECT substring(data FROM $1::int FOR $2::int) AS chunk FROM zerobucket_images WHERE id = $3;`;
const SELECT_STREAM_INFO = `SELECT size_bytes, storage_backend, object_storage_key FROM zerobucket_images WHERE id = $1;`;
const DELETE_RETURNING = `DELETE FROM zerobucket_images WHERE id = $1 RETURNING storage_backend, object_storage_key;`;
const DELETE_MANY = `DELETE FROM zerobucket_images WHERE id = ANY($1::uuid[]) RETURNING id, storage_backend, object_storage_key;`;
const EXISTS = `SELECT 1 FROM zerobucket_images WHERE id = $1;`;

// FOR UPDATE: a per-row lock held until the transaction ends (see tierToObjectStorage).
const SELECT_FOR_TIERING = `SELECT data, mime_type, size_bytes, storage_backend FROM zerobucket_images WHERE id = $1 FOR UPDATE;`;
const UPDATE_AFTER_TIERING = `UPDATE zerobucket_images SET data = NULL, storage_backend = 'object_storage', object_storage_bucket = $1, object_storage_key = $2, updated_at = now() WHERE id = $3;`;

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** True if `id` is a syntactically valid UUID. Malformed ids can never exist, so they short-circuit to "not found". */
export function isValidId(id: string): boolean {
  return typeof id === "string" && UUID_RE.test(id);
}

// SQLSTATEs worth retrying: transient conditions only, never constraint/syntax errors.
const RETRYABLE_SQLSTATES = new Set([
  "40001", // serialization_failure
  "40P01", // deadlock_detected
  "08000", // connection_exception
  "08003", // connection_does_not_exist
  "08006", // connection_failure
  "08001", // sqlclient_unable_to_establish_sqlconnection
  "08004", // sqlserver_rejected_establishment_of_sqlconnection
  "57P01", // admin_shutdown
  "57P02", // crash_shutdown
  "57P03", // cannot_connect_now
  "53000", // insufficient_resources
  "53300", // too_many_connections
]);
const RETRYABLE_NODE_CODES = new Set([
  "ECONNRESET",
  "ECONNREFUSED",
  "ETIMEDOUT",
  "EPIPE",
  "ECONNABORTED",
]);
const CONNECTION_MESSAGE_RE =
  /connection terminated|connection timeout|timeout exceeded when trying to connect|client has encountered a connection error/i;

/** Transient (worth retrying) vs. permanent (fails identically every time). */
export function isRetryable(exc: unknown): boolean {
  if (!(exc instanceof Error)) return false;
  const code = (exc as { code?: unknown }).code;
  if (typeof code === "string") {
    if (RETRYABLE_SQLSTATES.has(code) || RETRYABLE_NODE_CODES.has(code))
      return true;
  }
  return CONNECTION_MESSAGE_RE.test(exc.message);
}

/** Errors after which the pooled connection must be destroyed, not returned to the pool. */
function isConnectionLevel(exc: unknown): boolean {
  if (!(exc instanceof Error)) return false;
  const code = (exc as { code?: unknown }).code;
  if (
    typeof code === "string" &&
    (RETRYABLE_NODE_CODES.has(code) ||
      code.startsWith("08") ||
      code.startsWith("57"))
  ) {
    return true;
  }
  return CONNECTION_MESSAGE_RE.test(exc.message);
}

const MAX_BACKOFF_MS = 2000;

export function backoffDelayMs(attempt: number, baseDelayMs: number): number {
  const exponential = baseDelayMs * 2 ** (attempt - 1);
  const jitter = Math.random() * baseDelayMs;
  return Math.min(exponential + jitter, MAX_BACKOFF_MS);
}

export interface PostgresBackendOptions {
  /** postgresql:// connection string. Required unless `pool` is given. */
  connectionString?: string;
  /** Bring your own pg.Pool (not closed by `close()`). Pool sizing options are then ignored. */
  pool?: Pool;
  /** Create the schema on first use. Default true. */
  autoMigrate?: boolean;
  /** Retries for transient errors on pooled calls. Default 3; 0 disables. */
  maxRetries?: number;
  /** Base for exponential backoff with jitter (capped at 2s). Default 100. */
  retryBaseDelayMs?: number;
  poolMinSize?: number;
  poolMaxSize?: number;
  /** Max wait for a pooled connection. Default 10000. */
  poolTimeoutMs?: number;
  onOperation?: (event: OperationEvent) => void;
  /**
   * Enables transparent reads of tiered rows and `tierToObjectStorage()`.
   * Configure it with the same bucket/credentials used to tier the images.
   */
  objectStorage?: ObjectStorageLike;
}

type Work<T> = (q: Queryable) => Promise<T>;

export class PostgresBackend implements StorageBackend {
  private pool: Pool | undefined;
  private readonly ownsPool: boolean;
  private readyPromise: Promise<void> | undefined;
  private closed = false;
  private readonly maxRetries: number;
  private readonly retryBaseDelayMs: number;
  private readonly autoMigrate: boolean;

  constructor(private readonly opts: PostgresBackendOptions) {
    if (!opts.connectionString && !opts.pool) {
      throw new TypeError(
        "PostgresBackend requires either `connectionString` or `pool`",
      );
    }
    this.ownsPool = !opts.pool;
    this.pool = opts.pool;
    this.maxRetries = opts.maxRetries ?? 3;
    this.retryBaseDelayMs = opts.retryBaseDelayMs ?? 100;
    this.autoMigrate = opts.autoMigrate ?? true;
  }

  // ---- lifecycle -------------------------------------------------------

  ready(): Promise<void> {
    if (this.closed)
      return Promise.reject(
        new StorageError("This ZeroBucket instance has been closed"),
      );
    this.readyPromise ??= this.init().catch((exc) => {
      this.readyPromise = undefined; // allow a later attempt (e.g. the DB was briefly down)
      throw exc;
    });
    return this.readyPromise;
  }

  private async init(): Promise<void> {
    if (!this.pool) {
      let pg: { Pool: typeof Pool };
      try {
        const mod = (await import("pg")) as unknown as {
          default?: { Pool: typeof Pool };
          Pool: typeof Pool;
        };
        pg = mod.default ?? mod; // CJS package: ESM gives { default }, some interop gives it directly
      } catch (cause) {
        throw new StorageError(
          "PostgresBackend requires the 'pg' package. Install it with: npm install pg",
          { cause },
        );
      }
      const config: PoolConfig = {
        connectionString: this.opts.connectionString,
        min: this.opts.poolMinSize ?? 1,
        max: this.opts.poolMaxSize ?? 5,
        connectionTimeoutMillis: this.opts.poolTimeoutMs ?? 10_000,
      };
      this.pool = new pg.Pool(config);
      // An idle client erroring (server restart, network drop) must not crash the process.
      this.pool.on("error", () => {});
    }
    try {
      if (this.autoMigrate) await this.migrate();
      else await this.pool.query("SELECT 1");
    } catch (exc) {
      if (this.ownsPool) {
        await this.pool.end().catch(() => {});
        this.pool = undefined;
      }
      throw exc instanceof StorageError
        ? exc
        : new StorageError(`Could not connect to PostgreSQL: ${msg(exc)}`, {
            cause: exc,
          });
    }
  }

  /** Create the schema if missing. Serialized across processes with an advisory lock. */
  async migrate(): Promise<void> {
    const pool = this.pool!;
    let client: PoolClient;
    try {
      client = await pool.connect();
    } catch (exc) {
      throw new StorageError(`Could not connect to PostgreSQL: ${msg(exc)}`, {
        cause: exc,
      });
    }
    try {
      await client.query("BEGIN");
      await client.query(
        "SELECT pg_advisory_xact_lock(hashtext('zerobucket_migrate'))",
      );
      await client.query(SCHEMA);
      await client.query("COMMIT");
      client.release();
    } catch (exc) {
      await client.query("ROLLBACK").catch(() => {});
      client.release(true);
      throw new StorageError(`Migration failed: ${msg(exc)}`, { cause: exc });
    }
  }

  async close(): Promise<void> {
    this.closed = true;
    const pool = this.pool;
    this.pool = undefined;
    this.readyPromise = undefined;
    if (pool && this.ownsPool) await pool.end();
  }

  // ---- execution core --------------------------------------------------

  private emit(
    operation: string,
    start: number,
    success: boolean,
    error: string | null,
    retryCount: number,
  ): void {
    const cb = this.opts.onOperation;
    if (!cb) return;
    try {
      cb({
        operation,
        durationMs: performance.now() - start,
        success,
        error,
        retryCount,
      });
    } catch {
      // A bug in a metrics callback must never break a real operation.
    }
  }

  /**
   * Run `work` and return its result, emitting exactly one OperationEvent.
   *
   * connection given  -> run once on it, no BEGIN/COMMIT, no retry.
   * connection absent -> pooled; `tx` wraps the work in BEGIN/COMMIT; transient
   *                      failures retry the WHOLE closure on a fresh connection.
   */
  private async run<T>(
    operation: string,
    connection: Queryable | undefined,
    work: Work<T>,
    { tx = false }: { tx?: boolean } = {},
  ): Promise<T> {
    if (connection) {
      const start = performance.now();
      try {
        const result = await work(connection);
        this.emit(operation, start, true, null, 0);
        return result;
      } catch (exc) {
        this.emit(operation, start, false, msg(exc), 0);
        throw exc;
      }
    }

    await this.ready();
    const start = performance.now();
    let attempt = 0;
    for (;;) {
      let client: PoolClient | undefined;
      let destroy = false;
      try {
        client = await this.pool!.connect();
        if (tx) await client.query("BEGIN");
        const result = await work(client);
        if (tx) await client.query("COMMIT");
        this.emit(operation, start, true, null, attempt);
        return result;
      } catch (exc) {
        if (client && tx) {
          try {
            await client.query("ROLLBACK");
          } catch {
            destroy = true; // cannot trust a connection that cannot roll back
          }
        }
        if (isConnectionLevel(exc)) destroy = true;
        attempt += 1;
        if (attempt > this.maxRetries || !isRetryable(exc)) {
          this.emit(operation, start, false, msg(exc), attempt - 1);
          throw exc;
        }
      } finally {
        client?.release(destroy);
      }
      await sleep(backoffDelayMs(attempt, this.retryBaseDelayMs));
    }
  }

  // ---- StorageBackend --------------------------------------------------

  async put(
    row: PreparedRow,
    { connection }: CallOptions = {},
  ): Promise<string> {
    try {
      return await this.run("put", connection, async (q) => {
        const res = await q.query(INSERT, rowParams(row));
        return String(res.rows[0].id);
      });
    } catch (exc) {
      throw wrap("Failed to store image", exc);
    }
  }

  async putMany(
    rows: PreparedRow[],
    { connection }: CallOptions = {},
  ): Promise<string[]> {
    if (rows.length === 0) return [];
    try {
      // One transaction. A transient failure rolls back EVERYTHING, so a retry
      // from scratch cannot create duplicates. Statements are awaited one at a
      // time on the single connection (queuing concurrent queries on one pg
      // client is deprecated and breaks in pg@9), so result order matches input
      // order by construction.
      return await this.run(
        "put_many",
        connection,
        async (q) => {
          const ids: string[] = [];
          for (const r of rows)
            ids.push(String((await q.query(INSERT, rowParams(r))).rows[0].id));
          return ids;
        },
        { tx: true },
      );
    } catch (exc) {
      throw wrap("Failed to store image batch", exc);
    }
  }

  async get(
    id: string,
    { connection }: CallOptions = {},
  ): Promise<StoredRecord | null> {
    if (!isValidId(id)) return null;
    let row: any;
    try {
      row = await this.run(
        "get",
        connection,
        async (q) => (await q.query(SELECT_FULL, [id])).rows[0],
      );
    } catch (exc) {
      throw wrap("Failed to retrieve image", exc);
    }
    return row ? this.hydrate(row, id) : null;
  }

  async getMany(
    ids: string[],
    { connection }: CallOptions = {},
  ): Promise<StoredRecord[]> {
    const valid = ids.filter(isValidId);
    if (valid.length === 0) return [];
    let rows: any[];
    try {
      rows = await this.run(
        "get_many",
        connection,
        async (q) => (await q.query(SELECT_MANY, [valid])).rows,
      );
    } catch (exc) {
      throw wrap("Failed to retrieve image batch", exc);
    }
    return mapLimit(rows, 4, (r) => this.hydrate(r, String(r.id)));
  }

  async getMetadata(
    id: string,
    { connection }: CallOptions = {},
  ): Promise<StoredRecordMetadata | null> {
    if (!isValidId(id)) return null;
    let row: any;
    try {
      row = await this.run(
        "get_metadata",
        connection,
        async (q) => (await q.query(SELECT_METADATA, [id])).rows[0],
      );
    } catch (exc) {
      throw wrap("Failed to retrieve image metadata", exc);
    }
    if (!row) return null;
    return {
      id: String(row.id),
      mimeType: row.mime_type,
      originalFilename: row.original_filename,
      sizeBytes: row.size_bytes,
      width: row.width,
      height: row.height,
      checksumSha256: row.checksum_sha256,
    };
  }

  async getStream(
    id: string,
    opts: StreamOptions,
  ): Promise<AsyncIterable<Buffer> | null> {
    if (!isValidId(id)) return null;
    const { connection, chunkSize } = opts;
    if (!Number.isInteger(chunkSize) || chunkSize <= 0)
      throw new RangeError("chunkSize must be a positive integer");

    let info: any;
    try {
      info = await this.run(
        "get_metadata",
        connection,
        async (q) => (await q.query(SELECT_STREAM_INFO, [id])).rows[0],
      );
    } catch (exc) {
      throw wrap("Failed to retrieve image metadata", exc);
    }
    if (!info) return null;
    const totalSize: number = info.size_bytes;
    const start = opts.range?.start ?? 0;
    const end = Math.min(opts.range?.end ?? totalSize - 1, totalSize - 1); // inclusive, clamped

    if (info.storage_backend === "object_storage") {
      const os = this.opts.objectStorage;
      if (!os) throw this.tieredWithoutStorage(id, info.object_storage_key);
      return streamFromObjectStorage(
        os,
        id,
        info.object_storage_key,
        start,
        end,
        chunkSize,
      );
    }
    const self = this;

    async function* generate(): AsyncGenerator<Buffer> {
      let offset = start; // 0-based
      let delivered = 0;
      const wanted = Math.max(0, end - start + 1);
      while (delivered < wanted) {
        const length = Math.min(chunkSize, wanted - delivered);
        let row: any;
        try {
          row = await self.run(
            "get_stream",
            connection,
            async (q) =>
              (await q.query(SELECT_CHUNK, [offset + 1, length, id])).rows[0],
          );
        } catch (exc) {
          throw wrap("Failed to stream image", exc);
        }
        if (!row) {
          throw new StorageError(
            `Image ${JSON.stringify(id)} was deleted while streaming (delivered ${delivered} of ${wanted} bytes). ` +
              "Pass connection with your own open transaction if you need a consistent read across concurrent writers.",
          );
        }
        const chunk: Buffer = row.chunk;
        if (!chunk || chunk.length === 0) {
          throw new StorageError(
            `Image ${JSON.stringify(id)} ended early while streaming (delivered ${delivered} of ${wanted} bytes).`,
          );
        }
        yield chunk;
        offset += chunk.length;
        delivered += chunk.length;
      }
    }
    return generate();
  }

  async delete(id: string, { connection }: CallOptions = {}): Promise<boolean> {
    if (!isValidId(id)) return false;
    try {
      const row = await this.run(
        "delete",
        connection,
        async (q) => (await q.query(DELETE_RETURNING, [id])).rows[0],
      );
      if (row?.storage_backend === "object_storage")
        await this.cleanupObject([row.object_storage_key], connection);
      return row !== undefined;
    } catch (exc) {
      throw wrap("Failed to delete image", exc);
    }
  }

  async deleteMany(
    ids: string[],
    { connection }: CallOptions = {},
  ): Promise<string[]> {
    const valid = ids.filter(isValidId);
    if (valid.length === 0) return [];
    try {
      const rows = await this.run(
        "delete_many",
        connection,
        async (q) => (await q.query(DELETE_MANY, [valid])).rows,
      );
      await this.cleanupObject(
        rows
          .filter((r) => r.storage_backend === "object_storage")
          .map((r) => r.object_storage_key as string),
        connection,
      );
      return rows.map((r) => String(r.id));
    } catch (exc) {
      throw wrap("Failed to delete image batch", exc);
    }
  }

  async exists(id: string, { connection }: CallOptions = {}): Promise<boolean> {
    if (!isValidId(id)) return false;
    try {
      return await this.run(
        "exists",
        connection,
        async (q) => ((await q.query(EXISTS, [id])).rowCount ?? 0) > 0,
      );
    } catch (exc) {
      throw wrap("Failed to check image existence", exc);
    }
  }

  // ---- helpers ---------------------------------------------------------

  /** Turn a DB row into a record, fetching the bytes from object storage if the row is tiered. */
  private async hydrate(row: any, id: string): Promise<StoredRecord> {
    let data: Buffer;
    if (row.storage_backend === "object_storage") {
      const os = this.opts.objectStorage;
      if (!os) throw this.tieredWithoutStorage(id, row.object_storage_key);
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

  /**
   * Remove tiered objects AFTER their rows are gone. Ordering is deliberate:
   * once the row is deleted the image is correctly "not found" to everyone, so
   * a failed object delete only leaves a harmless orphan, whereas deleting the
   * object first and then failing the row delete would leave a row pointing at nothing.
   *
   * Never runs inside a caller-owned transaction (`connection`): we cannot know
   * whether it will commit, and destroying data before the commit outcome is known
   * could leave a rolled-back row pointing at a deleted object. In that case the
   * object stays until you remove it yourself after committing (objectStorage.delete(id)).
   * Failures are reported through onOperation and never fail the delete itself.
   */
  private async cleanupObject(
    keys: string[],
    connection: Queryable | undefined,
  ): Promise<void> {
    const os = this.opts.objectStorage;
    if (!os || connection || keys.length === 0) return;
    await Promise.all(
      keys.map(async (key) => {
        const start = performance.now();
        try {
          await os.delete(key);
          this.emit("object_storage_delete", start, true, null, 0);
        } catch (exc) {
          this.emit("object_storage_delete", start, false, msg(exc), 0);
        }
      }),
    );
  }

  /**
   * Move an image's bytes out of Postgres into object storage, replacing the
   * row's `data` with a pointer. Returns:
   *   null  - no such image
   *   false - already tiered (a safe no-op, so backfills can be re-run)
   *   true  - tiered just now
   *
   * SAFETY: the upload happens INSIDE the same transaction as the row lock
   * (SELECT ... FOR UPDATE) and the UPDATE that flips the row. If the upload
   * fails, the transaction rolls back and the row is untouched: there is no
   * moment when the bytes exist nowhere, or when a row claims to be tiered
   * without a completed upload.
   *
   * TRADEOFF: the row lock is held for the whole upload. Concurrent writes
   * to THIS image (delete, tier) wait; every other row is unaffected, and plain
   * reads are never blocked (Postgres MVCC). Meant for explicit maintenance, not hot paths.
   *
   * Retry is safe: a transient error replays the whole closure, and the
   * deterministic key (the image id) makes the re-upload an idempotent overwrite.
   */
  async tierToObjectStorage(
    id: string,
    { connection }: CallOptions = {},
  ): Promise<boolean | null> {
    const os = this.opts.objectStorage;
    if (!os) {
      throw new StorageError(
        "tierToObjectStorage() requires objectStorage to be configured (new ZeroBucket({ objectStorage: new ObjectStorage({ bucket }) })).",
      );
    }
    if (!isValidId(id)) return null;
    const key = id.toLowerCase();
    try {
      return await this.run(
        "tier_to_object_storage",
        connection,
        async (q) => {
          const row = (await q.query(SELECT_FOR_TIERING, [id])).rows[0];
          if (!row) return null;
          if (row.storage_backend !== "postgres") return false;
          await os.upload(key, row.data as Buffer, { mimeType: row.mime_type });
          await q.query(UPDATE_AFTER_TIERING, [os.bucket, key, id]);
          return true;
        },
        { tx: true },
      );
    } catch (exc) {
      throw wrap("Failed to tier image to object storage", exc);
    }
  }

  private tieredWithoutStorage(id: string, key: string | null): StorageError {
    return new StorageError(
      `Image ${JSON.stringify(id)} is stored in object storage (key=${JSON.stringify(key)}) ` +
        "but this ZeroBucket was constructed without `objectStorage`. Configure it with the " +
        "same bucket and credentials that were used to tier this image.",
    );
  }
}

/** Ranged reads straight from the object store; fails loudly rather than ever truncating. */
async function* streamFromObjectStorage(
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

async function mapLimit<T, R>(
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

function rowParams(r: PreparedRow): unknown[] {
  return [
    r.data,
    r.mimeType,
    r.originalFilename,
    r.sizeBytes,
    r.width,
    r.height,
    r.checksumSha256,
  ];
}

function wrap(prefix: string, exc: unknown): StorageError {
  return exc instanceof StorageError
    ? exc
    : new StorageError(`${prefix}: ${msg(exc)}`, { cause: exc });
}

function msg(exc: unknown): string {
  return exc instanceof Error ? exc.message : String(exc);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
