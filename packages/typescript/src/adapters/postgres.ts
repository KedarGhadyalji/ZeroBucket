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
import type {
  Connection,
  OperationEvent,
  PreparedRow,
  Queryable,
} from "../types.js";
import {
  DEFAULT_STREAM_CHUNK_SIZE,
  hydrateRow,
  isValidId,
  mapLimit,
  msg,
  sleep,
  streamFromObjectStorage,
  tieredWithoutStorage,
  wrap,
} from "./shared.js";
import type {
  CallOptions,
  StorageBackend,
  StoredRecord,
  StoredRecordMetadata,
  StreamOptions,
} from "./base.js";

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

// ---- dedup mode: content-addressed blobs + per-image refs (same schema as the Python package) ----
const DEDUP_SCHEMA = `
CREATE TABLE IF NOT EXISTS zerobucket_blobs (
    checksum_sha256     CHAR(64) PRIMARY KEY,
    data                BYTEA NOT NULL,
    mime_type           TEXT NOT NULL,
    size_bytes          INTEGER NOT NULL,
    width               INTEGER,
    height              INTEGER,
    ref_count           INTEGER NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS zerobucket_image_refs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    checksum_sha256     CHAR(64) NOT NULL REFERENCES zerobucket_blobs(checksum_sha256),
    original_filename   TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_zerobucket_image_refs_checksum ON zerobucket_image_refs (checksum_sha256);
CREATE INDEX IF NOT EXISTS idx_zerobucket_image_refs_created_at ON zerobucket_image_refs (created_at);
`;

// $7 is how many references this call adds (1 for put; N when a batch holds N identical images).
// ON CONFLICT makes the increment atomic at the row level: 20 concurrent puts of the same bytes give ref_count == 20.
const D_UPSERT_BLOB = `
INSERT INTO zerobucket_blobs (checksum_sha256, data, mime_type, size_bytes, width, height, ref_count)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (checksum_sha256) DO UPDATE SET ref_count = zerobucket_blobs.ref_count + EXCLUDED.ref_count;`;
const D_INSERT_REF = `INSERT INTO zerobucket_image_refs (checksum_sha256, original_filename) VALUES ($1, $2) RETURNING id;`;
// Aliased to the classic column names so one row mapper serves both modes.
const D_COLS = `r.id, b.data, b.mime_type, r.original_filename, b.size_bytes, b.width, b.height, r.checksum_sha256,
       'postgres'::text AS storage_backend, NULL::text AS object_storage_bucket, NULL::text AS object_storage_key`;
const D_FROM = `FROM zerobucket_image_refs r JOIN zerobucket_blobs b ON r.checksum_sha256 = b.checksum_sha256`;
const D_DELETE_REF = `DELETE FROM zerobucket_image_refs WHERE id = $1 RETURNING checksum_sha256;`;
const D_DELETE_REFS = `DELETE FROM zerobucket_image_refs WHERE id = ANY($1::uuid[]) RETURNING id, checksum_sha256;`;
const D_DECREMENT = `UPDATE zerobucket_blobs SET ref_count = ref_count - $1 WHERE checksum_sha256 = $2 RETURNING ref_count;`;
const D_DELETE_EMPTY = `DELETE FROM zerobucket_blobs WHERE checksum_sha256 = ANY($1::text[]) AND ref_count <= 0;`;

interface SqlSet {
  schema: string;
  selectFull: string;
  selectMany: string;
  selectMetadata: string;
  selectStreamInfo: string;
  selectChunk: string;
  exists: string;
}
const CLASSIC_SQL: SqlSet = {
  schema: SCHEMA,
  selectFull: SELECT_FULL,
  selectMany: SELECT_MANY,
  selectMetadata: SELECT_METADATA,
  selectStreamInfo: SELECT_STREAM_INFO,
  selectChunk: SELECT_CHUNK,
  exists: EXISTS,
};
const DEDUP_SQL: SqlSet = {
  schema: DEDUP_SCHEMA,
  selectFull: `SELECT ${D_COLS} ${D_FROM} WHERE r.id = $1;`,
  selectMany: `SELECT ${D_COLS} ${D_FROM} WHERE r.id = ANY($1::uuid[]);`,
  selectMetadata: `SELECT r.id, b.mime_type, r.original_filename, b.size_bytes, b.width, b.height, r.checksum_sha256 ${D_FROM} WHERE r.id = $1;`,
  selectStreamInfo: `SELECT b.size_bytes, 'postgres'::text AS storage_backend, NULL::text AS object_storage_key ${D_FROM} WHERE r.id = $1;`,
  selectChunk: `SELECT substring(b.data FROM $1::int FOR $2::int) AS chunk ${D_FROM} WHERE r.id = $3;`,
  exists: `SELECT 1 FROM zerobucket_image_refs WHERE id = $1;`,
};

export { isValidId };

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
  /**
   * Content-addressed mode: identical bytes are stored once (zerobucket_blobs) and each
   * put() adds a lightweight reference (zerobucket_image_refs) with its own id and filename.
   * Separate tables from classic mode, so the two can never be confused. Cannot be combined
   * with `objectStorage`. Same schema as the Python package's dedup mode.
   */
  dedup?: boolean;
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
  private readonly dedup: boolean;
  private readonly sql: SqlSet;

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
    this.dedup = opts.dedup ?? false;
    if (this.dedup && opts.objectStorage) {
      throw new TypeError(
        "objectStorage is not supported together with dedup: true (same restriction as the Python package). " +
          "Tiering moves one image's bytes, but a deduplicated blob can be shared by many images.",
      );
    }
    this.sql = this.dedup ? DEDUP_SQL : CLASSIC_SQL;
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
      await client.query(this.sql.schema);
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
    connection: Connection | undefined,
    work: Work<T>,
    { tx = false }: { tx?: boolean } = {},
  ): Promise<T> {
    if (connection) {
      if (!isQueryable(connection)) {
        throw new TypeError(
          "PostgresBackend expects a pg client/pool client as `connection` (an object with query()), not a SQLite Database.",
        );
      }
      const start = performance.now();
      try {
        const result = tx
          ? await this.atomic(connection, work)
          : await work(connection);
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

  /**
   * Make multi-statement work all-or-nothing on a CALLER-supplied connection.
   *  - Caller has an open transaction -> SAVEPOINT: a failure rolls back only our work and the
   *    caller's transaction stays usable; success joins their transaction.
   *  - No open transaction (an autocommit pg.Client) -> open and commit our own, so a failure
   *    can never leave half-applied state (e.g. an inflated dedup ref_count).
   */
  private async atomic<T>(conn: Queryable, work: Work<T>): Promise<T> {
    let ownTx = false;
    try {
      await conn.query("SAVEPOINT zerobucket_atomic");
    } catch (exc) {
      if ((exc as { code?: string }).code !== "25P01") throw exc; // 25P01 = no active transaction
      ownTx = true;
      await conn.query("BEGIN");
    }
    try {
      const result = await work(conn);
      await conn.query(
        ownTx ? "COMMIT" : "RELEASE SAVEPOINT zerobucket_atomic",
      );
      return result;
    } catch (exc) {
      try {
        await conn.query(
          ownTx ? "ROLLBACK" : "ROLLBACK TO SAVEPOINT zerobucket_atomic",
        );
      } catch {
        // keep the original error
      }
      throw exc;
    }
  }

  // ---- StorageBackend --------------------------------------------------

  async put(
    row: PreparedRow,
    { connection }: CallOptions = {},
  ): Promise<string> {
    try {
      if (this.dedup) {
        return await this.run(
          "put",
          connection,
          async (q) => {
            await q.query(D_UPSERT_BLOB, blobParams(row, 1));
            return String(
              (
                await q.query(D_INSERT_REF, [
                  row.checksumSha256,
                  row.originalFilename,
                ])
              ).rows[0].id,
            );
          },
          { tx: true },
        );
      }
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
      if (this.dedup) {
        return await this.run(
          "put_many",
          connection,
          async (q) => {
            // Identical images within one batch share a blob: count them, upsert each distinct
            // checksum ONCE in sorted order (a global lock order, so two overlapping batches
            // can never deadlock on each other's blob rows), then create the refs in input order.
            const groups = new Map<string, { row: PreparedRow; n: number }>();
            for (const r of rows) {
              const g = groups.get(r.checksumSha256);
              if (g) g.n += 1;
              else groups.set(r.checksumSha256, { row: r, n: 1 });
            }
            for (const checksum of [...groups.keys()].sort()) {
              const g = groups.get(checksum)!;
              await q.query(D_UPSERT_BLOB, blobParams(g.row, g.n));
            }
            const ids: string[] = [];
            for (const r of rows) {
              ids.push(
                String(
                  (
                    await q.query(D_INSERT_REF, [
                      r.checksumSha256,
                      r.originalFilename,
                    ])
                  ).rows[0].id,
                ),
              );
            }
            return ids;
          },
          { tx: true },
        );
      }
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
        async (q) => (await q.query(this.sql.selectFull, [id])).rows[0],
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
        async (q) => (await q.query(this.sql.selectMany, [valid])).rows,
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
        async (q) => (await q.query(this.sql.selectMetadata, [id])).rows[0],
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
        async (q) => (await q.query(this.sql.selectStreamInfo, [id])).rows[0],
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
      if (!os) throw tieredWithoutStorage(id, info.object_storage_key);
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
              (await q.query(self.sql.selectChunk, [offset + 1, length, id]))
                .rows[0],
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
    if (this.dedup) return this.deleteDedup(id, connection);
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
    if (this.dedup) return this.deleteManyDedup(valid, connection);
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
        async (q) => ((await q.query(this.sql.exists, [id])).rowCount ?? 0) > 0,
      );
    } catch (exc) {
      throw wrap("Failed to check image existence", exc);
    }
  }

  // ---- dedup deletes / migration ---------------------------------------

  private async deleteDedup(
    id: string,
    connection: Connection | undefined,
  ): Promise<boolean> {
    try {
      return await this.run(
        "delete",
        connection,
        async (q) => {
          const ref = (await q.query(D_DELETE_REF, [id])).rows[0];
          if (!ref) return false; // id did not exist: nothing to decrement either
          const checksum = ref.checksum_sha256 as string;
          const dec = (await q.query(D_DECREMENT, [1, checksum])).rows[0];
          if (dec && dec.ref_count <= 0)
            await q.query(D_DELETE_EMPTY, [[checksum]]);
          return true;
        },
        { tx: true },
      );
    } catch (exc) {
      throw wrap("Failed to delete image", exc);
    }
  }

  private async deleteManyDedup(
    ids: string[],
    connection: Connection | undefined,
  ): Promise<string[]> {
    try {
      return await this.run(
        "delete_many",
        connection,
        async (q) => {
          const deleted = (await q.query(D_DELETE_REFS, [ids])).rows;
          if (deleted.length === 0) return [];
          // Several deleted refs may share one blob: decrement each blob by its exact count,
          // in sorted checksum order (global lock order, so concurrent batches cannot deadlock).
          const counts = new Map<string, number>();
          for (const r of deleted)
            counts.set(
              r.checksum_sha256,
              (counts.get(r.checksum_sha256) ?? 0) + 1,
            );
          const emptied: string[] = [];
          for (const checksum of [...counts.keys()].sort()) {
            const dec = (
              await q.query(D_DECREMENT, [counts.get(checksum), checksum])
            ).rows[0];
            if (dec && dec.ref_count <= 0) emptied.push(checksum);
          }
          if (emptied.length > 0) await q.query(D_DELETE_EMPTY, [emptied]);
          return deleted.map((r) => String(r.id));
        },
        { tx: true },
      );
    } catch (exc) {
      throw wrap("Failed to delete image batch", exc);
    }
  }

  /**
   * One-time, NON-DESTRUCTIVE copy of every row in the classic `zerobucket_images` table into this
   * dedup instance's tables, preserving every id (external references keep working) and
   * deduplicating identical content on the way. The classic table is never modified.
   *
   * Differences from the Python version, all deliberate: rows are copied in small batches (bounded
   * memory instead of loading the whole table); re-running is safe (already-migrated ids are
   * skipped and reported, not an error); and classic rows already tiered to object storage are
   * refused up front with a clear message (their bytes are not in the database to copy).
   * Still all-or-nothing: one transaction.
   */
  async migrateClassicToDedup(): Promise<{
    imagesMigrated: number;
    distinctBlobsCreated: number;
    duplicateReferencesFound: number;
    alreadyMigrated: number;
  }> {
    if (!this.dedup) {
      throw new StorageError(
        "migrateClassicToDedup() must be called on an instance created with dedup: true.",
      );
    }
    await this.ready();
    const client = await this.pool!.connect();
    let destroy = false;
    try {
      await client.query("BEGIN");
      await client.query(
        "SELECT pg_advisory_xact_lock(hashtext('zerobucket_migrate_dedup'))",
      );
      if (
        !(await client.query("SELECT to_regclass('zerobucket_images') AS t"))
          .rows[0].t
      ) {
        throw new StorageError(
          "No classic zerobucket_images table found in this database -- nothing to migrate.",
        );
      }
      const hasTierCol = (
        await client.query(
          "SELECT 1 FROM information_schema.columns WHERE table_name = 'zerobucket_images' AND column_name = 'storage_backend'",
        )
      ).rowCount;
      if (hasTierCol) {
        const tiered = (
          await client.query(
            "SELECT count(*)::int AS n FROM zerobucket_images WHERE storage_backend <> 'postgres'",
          )
        ).rows[0].n;
        if (tiered > 0) {
          throw new StorageError(
            `${tiered} classic image(s) are tiered to object storage, so their bytes are not in the database to migrate. ` +
              "Dedup mode does not support tiering. Migrate only an un-tiered table, or bring those images back first.",
          );
        }
      }

      const seen = new Set<string>();
      let migrated = 0;
      let already = 0;
      // created_at travels as TEXT, never a JS Date: Postgres keeps microseconds, a Date keeps only
      // milliseconds, and a truncated keyset cursor sorts before its own row (infinite loop).
      let cursor: { createdAt: string; id: string } | null = null;
      const BATCH = 20;
      for (;;) {
        const page: { rows: any[] } = cursor
          ? await client.query(
              `SELECT id, data, mime_type, original_filename, size_bytes, width, height, checksum_sha256, created_at::text AS created_at_txt
               FROM zerobucket_images WHERE (created_at, id) > ($1::timestamptz, $2::uuid) ORDER BY created_at, id LIMIT ${BATCH}`,
              [cursor.createdAt, cursor.id],
            )
          : await client.query(
              `SELECT id, data, mime_type, original_filename, size_bytes, width, height, checksum_sha256, created_at::text AS created_at_txt
               FROM zerobucket_images ORDER BY created_at, id LIMIT ${BATCH}`,
            );
        if (page.rows.length === 0) break;
        const last = page.rows[page.rows.length - 1];
        if (
          cursor &&
          cursor.createdAt === last.created_at_txt &&
          cursor.id === last.id
        ) {
          throw new StorageError(
            "Migration made no progress (keyset cursor did not advance); aborting instead of looping.",
          );
        }
        cursor = { createdAt: last.created_at_txt, id: last.id };

        const have = new Set(
          (
            await client.query(
              "SELECT id FROM zerobucket_image_refs WHERE id = ANY($1::uuid[])",
              [page.rows.map((r) => r.id)],
            )
          ).rows.map((r) => String(r.id)),
        );
        const fresh = page.rows.filter((r) => !have.has(String(r.id)));
        already += page.rows.length - fresh.length;

        const groups = new Map<string, { row: any; n: number }>();
        for (const r of fresh) {
          const g = groups.get(r.checksum_sha256);
          if (g) g.n += 1;
          else groups.set(r.checksum_sha256, { row: r, n: 1 });
        }
        for (const checksum of [...groups.keys()].sort()) {
          const { row, n } = groups.get(checksum)!;
          await client.query(D_UPSERT_BLOB, [
            checksum,
            row.data,
            row.mime_type,
            row.size_bytes,
            row.width,
            row.height,
            n,
          ]);
        }
        for (const r of fresh) {
          await client.query(
            "INSERT INTO zerobucket_image_refs (id, checksum_sha256, original_filename, created_at) VALUES ($1, $2, $3, $4::timestamptz)",
            [r.id, r.checksum_sha256, r.original_filename, r.created_at_txt],
          );
          seen.add(r.checksum_sha256);
          migrated += 1;
        }
      }
      await client.query("COMMIT");
      return {
        imagesMigrated: migrated,
        distinctBlobsCreated: seen.size,
        duplicateReferencesFound: migrated - seen.size,
        alreadyMigrated: already,
      };
    } catch (exc) {
      try {
        await client.query("ROLLBACK");
      } catch {
        destroy = true;
      }
      throw exc instanceof StorageError
        ? exc
        : new StorageError(`Migration to dedup failed: ${msg(exc)}`, {
            cause: exc,
          });
    } finally {
      client.release(destroy);
    }
  }

  // ---- helpers ---------------------------------------------------------

  private hydrate(row: any, id: string): Promise<StoredRecord> {
    return hydrateRow(row, id, this.opts.objectStorage);
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
    connection: Connection | undefined,
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
    if (this.dedup) {
      throw new StorageError(
        "tierToObjectStorage() is not supported in dedup mode (a blob may be shared by many images).",
      );
    }
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
}

function blobParams(r: PreparedRow, refs: number): unknown[] {
  return [
    r.checksumSha256,
    r.data,
    r.mimeType,
    r.sizeBytes,
    r.width,
    r.height,
    refs,
  ];
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

function isQueryable(c: Connection): c is Queryable {
  return typeof (c as Queryable).query === "function";
}
