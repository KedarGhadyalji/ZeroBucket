/**
 * The public ZeroBucket SDK entry point.
 *
 *   const images = new ZeroBucket({ connectionString: process.env.DATABASE_URL });
 *   const id = await images.put(fileBuffer);
 *   const image = await images.get(id);
 *
 * The developer never thinks about BYTEA, checksums or connection pooling.
 */
import { createHash } from "node:crypto";
import { readFile, stat } from "node:fs/promises";
import { basename } from "node:path";
import type { Pool } from "pg";
import type { CallOptions, StorageBackend } from "./adapters/base.js";
import {
  DEFAULT_STREAM_CHUNK_SIZE,
  PostgresBackend,
} from "./adapters/postgres.js";
import {
  AccessDeniedError,
  ImageNotFoundError,
  ImageTooLargeError,
  ImageValidationError,
} from "./errors.js";
import { type OptimizeOptions, optimizeImage } from "./optimize.js";
import type {
  BatchDeleteResult,
  BatchGetResult,
  BatchPutResult,
  Image,
  ImageMetadata,
  OperationEvent,
  PreparedRow,
  Queryable,
} from "./types.js";
import {
  DEFAULT_MAX_PIXELS,
  SUPPORTED_FORMATS,
  validateImage,
} from "./validation.js";
import type { ContentValidator } from "./validators/types.js";

/** 8 MiB: a practical ceiling for "small app" images. Override with `maxBytes`. */
export const DEFAULT_MAX_BYTES = 8 * 1024 * 1024;

/**
 * What put() accepts:
 *  - Buffer / Uint8Array / ArrayBuffer: the bytes themselves
 *  - string: a filesystem PATH (never raw content)
 *  - Blob / File (e.g. from `request.formData()`)
 *  - Node Readable or web ReadableStream (read in bounded chunks)
 */
export type ImageInput =
  | Buffer
  | Uint8Array
  | ArrayBuffer
  | string
  | Blob
  | AsyncIterable<Uint8Array | string>;

export type BeforeGetHook<C = unknown> = (
  imageId: string,
  context: C | undefined,
) => boolean | Promise<boolean>;
export type BeforePutHook<C = unknown> = (
  context: C | undefined,
) => boolean | Promise<boolean>;

export interface ZeroBucketOptions<C = unknown> {
  /** PostgreSQL connection string. */
  connectionString?: string;
  /** Bring your own pg.Pool. It is NOT closed by close(). */
  pool?: Pool;
  /** Advanced: inject a custom StorageBackend instead of building a PostgresBackend. */
  backend?: StorageBackend;

  /** Max accepted size in bytes. Default 8 MiB. */
  maxBytes?: number;
  /** Decoded-pixel ceiling that rejects decompression bombs. Default ~89 MP. */
  maxPixels?: number;
  /** Accepted image formats. Default ["jpeg", "png", "webp"]. */
  allowedFormats?: readonly string[];

  autoMigrate?: boolean;
  maxRetries?: number;
  retryBaseDelayMs?: number;
  poolMinSize?: number;
  poolMaxSize?: number;
  poolTimeoutMs?: number;
  /** Fire-and-forget observability. Exceptions thrown inside it are swallowed. */
  onOperation?: (event: OperationEvent) => void;

  /**
   * Authorization hook for get / getMany (per id) / getStream / streamTo / metadata.
   * Return false to deny. A hook that THROWS fails closed: the exception
   * propagates (or is captured per item in getMany) and is never treated as allow.
   * NOT called for exists().
   */
  beforeGet?: BeforeGetHook<C>;
  /** Authorization hook for put / putMany (evaluated once per call). Same fail-closed rule. */
  beforePut?: BeforePutHook<C>;
}

interface PutBase<C> {
  /** Display filename stored in metadata (never used for validation). */
  filename?: string;
  /** `true` = strip metadata only; or pass { maxWidth, format, quality } to resize/re-encode. */
  optimize?: boolean | OptimizeOptions;
  /** Store non-image content through the same machinery. Incompatible with `optimize`. */
  validator?: ContentValidator;
  /** Join your own transaction. ZeroBucket then does not commit, roll back or retry. */
  connection?: Queryable;
  /** Passed to beforePut. */
  context?: C;
}
export type PutOptions<C = unknown> = PutBase<C>;
export interface PutManyOptions<C = unknown> extends Omit<
  PutBase<C>,
  "filename"
> {
  /** Same length as `images`; use null/undefined entries to fall back to each input's own name. */
  filenames?: (string | null | undefined)[];
}
export interface GetOptions<C = unknown> {
  connection?: Queryable;
  context?: C;
}
export interface StreamOptions<C = unknown> extends GetOptions<C> {
  /** Bytes per chunk. Default 1 MiB. */
  chunkSize?: number;
  /** Inclusive byte range, e.g. { start: 0, end: 1023 }. `end` is clamped to the object size. */
  range?: { start: number; end?: number };
}

/** Anything with write(); e.g. fs.WriteStream, http.ServerResponse, or a custom sink. Backpressure is honoured. */
export interface WritableLike {
  write(chunk: Buffer): boolean | void | Promise<void>;
  once?(event: "drain", listener: () => void): unknown;
}

/** Bounded concurrency for per-item validation work. */
const PREPARE_CONCURRENCY = 4;

export class ZeroBucket<C = unknown> {
  private readonly backend: StorageBackend;
  private readonly maxBytes: number;
  private readonly maxPixels: number;
  private readonly allowedFormats: readonly string[];
  private readonly beforeGet: BeforeGetHook<C> | undefined;
  private readonly beforePut: BeforePutHook<C> | undefined;

  constructor(options: ZeroBucketOptions<C> = {}) {
    this.backend =
      options.backend ??
      new PostgresBackend({
        connectionString: options.connectionString,
        pool: options.pool,
        autoMigrate: options.autoMigrate,
        maxRetries: options.maxRetries,
        retryBaseDelayMs: options.retryBaseDelayMs,
        poolMinSize: options.poolMinSize,
        poolMaxSize: options.poolMaxSize,
        poolTimeoutMs: options.poolTimeoutMs,
        onOperation: options.onOperation,
      });
    this.maxBytes = options.maxBytes ?? DEFAULT_MAX_BYTES;
    this.maxPixels = options.maxPixels ?? DEFAULT_MAX_PIXELS;
    this.allowedFormats = options.allowedFormats ?? SUPPORTED_FORMATS;
    this.beforeGet = options.beforeGet;
    this.beforePut = options.beforePut;
  }

  /** Construct AND connect/migrate immediately, so a bad DATABASE_URL fails at startup, not on first request. */
  static async connect<C = unknown>(
    options: ZeroBucketOptions<C> = {},
  ): Promise<ZeroBucket<C>> {
    const zb = new ZeroBucket<C>(options);
    await zb.ready();
    return zb;
  }

  /** Resolves once connected and migrated. Called automatically on first use. */
  ready(): Promise<void> {
    return this.backend.ready();
  }

  // ---- hooks -----------------------------------------------------------

  private async checkBeforeGet(
    imageId: string,
    context: C | undefined,
  ): Promise<void> {
    if (!this.beforeGet) return;
    if (!(await this.beforeGet(imageId, context)))
      throw new AccessDeniedError("get", imageId);
  }

  private async checkBeforePut(context: C | undefined): Promise<void> {
    if (!this.beforePut) return;
    if (!(await this.beforePut(context))) throw new AccessDeniedError("put");
  }

  // ---- write path ------------------------------------------------------

  private async prepareRow(
    image: ImageInput,
    o: PutBase<C> & { filename?: string | null },
  ): Promise<PreparedRow> {
    const { data, filename } = await readInput(
      image,
      o.filename ?? undefined,
      this.maxBytes,
    );

    let finalData: Buffer;
    let mimeType: string;
    let width: number | null;
    let height: number | null;
    let sizeBytes: number;

    if (o.validator) {
      if (o.optimize) {
        throw new ImageValidationError(
          "optimize is not supported together with a custom validator: the optimize pipeline " +
            "(resize/re-encode) is image-specific. Validate/transform custom content types yourself before put().",
        );
      }
      const v = await o.validator.validate(data, { maxBytes: this.maxBytes });
      finalData = data;
      mimeType = v.mimeType;
      width = v.width ?? null;
      height = v.height ?? null;
      sizeBytes = v.sizeBytes;
    } else {
      const validated = await validateImage(data, {
        maxBytes: this.maxBytes,
        maxPixels: this.maxPixels,
        allowedFormats: this.allowedFormats,
      });
      if (o.optimize) {
        const opts = o.optimize === true ? {} : o.optimize;
        const r = await optimizeImage(data, {
          ...opts,
          maxBytes: this.maxBytes,
          maxPixels: this.maxPixels,
        });
        finalData = r.data;
        mimeType = r.mimeType;
        width = r.width;
        height = r.height;
        sizeBytes = r.sizeBytes;
      } else {
        finalData = data;
        mimeType = validated.mimeType;
        width = validated.width;
        height = validated.height;
        sizeBytes = validated.sizeBytes;
      }
    }

    return {
      data: finalData,
      mimeType,
      originalFilename: filename ?? null,
      sizeBytes,
      width,
      height,
      checksumSha256: createHash("sha256").update(finalData).digest("hex"),
    };
  }

  /** Validate, optionally optimize, checksum and store one image. Returns its id (a UUID). */
  async put(image: ImageInput, options: PutOptions<C> = {}): Promise<string> {
    await this.checkBeforePut(options.context);
    const row = await this.prepareRow(image, options);
    return this.backend.put(row, { connection: options.connection });
  }

  /**
   * Store many images. Best-effort, not all-or-nothing: one bad image does not
   * abort the rest. Check each result's `success` / `error`. Settings apply to
   * every item. Valid items are inserted in ONE transaction.
   */
  async putMany(
    images: ImageInput[],
    options: PutManyOptions<C> = {},
  ): Promise<BatchPutResult[]> {
    const { filenames } = options;
    if (filenames && filenames.length !== images.length) {
      throw new RangeError(
        "filenames must be the same length as images if provided",
      );
    }

    const fail = (index: number, error: string): BatchPutResult => ({
      index,
      imageId: null,
      error,
      success: false,
    });

    if (this.beforePut) {
      try {
        if (!(await this.beforePut(options.context)))
          return images.map((_, i) => fail(i, "access denied"));
      } catch (exc) {
        return images.map((_, i) => fail(i, errText(exc))); // fail closed, per item
      }
    }

    const results: (BatchPutResult | undefined)[] = new Array(images.length);
    const prepared: { index: number; row: PreparedRow }[] = [];

    await mapLimit(images, PREPARE_CONCURRENCY, async (image, i) => {
      try {
        const row = await this.prepareRow(image, {
          ...options,
          filename: filenames?.[i] ?? undefined,
        });
        prepared.push({ index: i, row });
      } catch (exc) {
        results[i] = fail(i, errText(exc));
      }
    });
    prepared.sort((a, b) => a.index - b.index);

    if (prepared.length > 0) {
      try {
        const ids = await this.backend.putMany(
          prepared.map((p) => p.row),
          { connection: options.connection },
        );
        prepared.forEach((p, n) => {
          results[p.index] = {
            index: p.index,
            imageId: ids[n]!,
            error: null,
            success: true,
          };
        });
      } catch (exc) {
        // The whole DB batch failed; every validated item shared that transaction.
        for (const p of prepared)
          results[p.index] = fail(p.index, errText(exc));
      }
    }
    return results as BatchPutResult[];
  }

  // ---- read path -------------------------------------------------------

  /** Retrieve a full image including bytes. Throws ImageNotFoundError if missing. */
  async get(imageId: string, options: GetOptions<C> = {}): Promise<Image> {
    await this.checkBeforeGet(imageId, options.context);
    const record = await this.backend.get(imageId, {
      connection: options.connection,
    });
    if (!record) throw new ImageNotFoundError(imageId);
    return {
      data: record.data,
      mimeType: record.mimeType,
      filename: record.originalFilename,
      sizeBytes: record.sizeBytes,
      width: record.width,
      height: record.height,
      checksumSha256: record.checksumSha256,
    };
  }

  /**
   * Retrieve many images in one query. Results come back in input order; a
   * missing id is `error: "not found"`, not an exception. A hook denial or hook
   * exception affects only that id.
   */
  async getMany(
    imageIds: string[],
    options: GetOptions<C> = {},
  ): Promise<BatchGetResult[]> {
    const results: (BatchGetResult | undefined)[] = new Array(imageIds.length);
    const allowedIdx: number[] = [];

    for (let i = 0; i < imageIds.length; i++) {
      const imageId = imageIds[i]!;
      if (this.beforeGet) {
        try {
          if (!(await this.beforeGet(imageId, options.context))) {
            results[i] = {
              imageId,
              image: null,
              error: "access denied",
              success: false,
            };
            continue;
          }
        } catch (exc) {
          results[i] = {
            imageId,
            image: null,
            error: errText(exc),
            success: false,
          };
          continue;
        }
      }
      allowedIdx.push(i);
    }

    if (allowedIdx.length > 0) {
      const records = await this.backend.getMany(
        allowedIdx.map((i) => imageIds[i]!),
        { connection: options.connection },
      );
      const byId = new Map(records.map((r) => [r.id.toLowerCase(), r]));
      for (const i of allowedIdx) {
        const imageId = imageIds[i]!;
        const r = byId.get(imageId.toLowerCase());
        results[i] = r
          ? {
              imageId,
              image: {
                data: r.data,
                mimeType: r.mimeType,
                filename: r.originalFilename,
                sizeBytes: r.sizeBytes,
                width: r.width,
                height: r.height,
                checksumSha256: r.checksumSha256,
              },
              error: null,
              success: true,
            }
          : { imageId, image: null, error: "not found", success: false };
      }
    }
    return results as BatchGetResult[];
  }

  /**
   * Stream an image's bytes in chunks without holding the whole image in memory.
   * Not-found is checked eagerly: the returned promise rejects before any chunk
   * is read. A concurrent delete mid-stream makes iteration throw StorageError
   * rather than silently truncating. Pass `range` for partial reads.
   *
   * Honest limitation: reduces Node-side memory, not Postgres-side (the server
   * still handles the full BYTEA value per chunk request).
   */
  async getStream(
    imageId: string,
    options: StreamOptions<C> = {},
  ): Promise<AsyncIterable<Buffer>> {
    await this.checkBeforeGet(imageId, options.context);
    const { range } = options;
    if (range) {
      if (
        !Number.isInteger(range.start) ||
        range.start < 0 ||
        (range.end !== undefined &&
          (!Number.isInteger(range.end) || range.end < range.start))
      ) {
        throw new RangeError(
          "range must be { start >= 0, end >= start } with integer values",
        );
      }
    }
    const stream = await this.backend.getStream(imageId, {
      chunkSize: options.chunkSize ?? DEFAULT_STREAM_CHUNK_SIZE,
      connection: options.connection,
      range: range
        ? { start: range.start, end: range.end ?? Number.MAX_SAFE_INTEGER }
        : undefined,
    });
    if (!stream) throw new ImageNotFoundError(imageId);
    return stream;
  }

  /** Same as getStream(), as a Web `ReadableStream` you can hand straight to `new Response(...)`. */
  async toWebStream(
    imageId: string,
    options: StreamOptions<C> = {},
  ): Promise<ReadableStream<Uint8Array>> {
    const iterable = await this.getStream(imageId, options);
    const iterator = iterable[Symbol.asyncIterator]();
    return new ReadableStream<Uint8Array>({
      async pull(controller) {
        try {
          const { value, done } = await iterator.next();
          if (done) controller.close();
          else
            controller.enqueue(
              new Uint8Array(value.buffer, value.byteOffset, value.byteLength),
            );
        } catch (exc) {
          controller.error(exc);
        }
      },
      async cancel() {
        await iterator.return?.();
      },
    });
  }

  /** Write an image to `destination` chunk by chunk (honouring backpressure). Returns bytes written. Does not end the destination. */
  async streamTo(
    imageId: string,
    destination: WritableLike,
    options: StreamOptions<C> = {},
  ): Promise<number> {
    let total = 0;
    for await (const chunk of await this.getStream(imageId, options)) {
      const result = destination.write(chunk);
      if (result === false && typeof destination.once === "function") {
        await new Promise<void>((resolve) =>
          destination.once!("drain", resolve),
        );
      } else if (
        result &&
        typeof (result as Promise<void>).then === "function"
      ) {
        await result;
      }
      total += chunk.length;
    }
    return total;
  }

  /** Metadata without pulling the bytes. Gated by the same beforeGet hook as get(). */
  async metadata(
    imageId: string,
    options: GetOptions<C> = {},
  ): Promise<ImageMetadata> {
    await this.checkBeforeGet(imageId, options.context);
    const r = await this.backend.getMetadata(imageId, {
      connection: options.connection,
    });
    if (!r) throw new ImageNotFoundError(imageId);
    return {
      imageId: r.id,
      mimeType: r.mimeType,
      filename: r.originalFilename,
      sizeBytes: r.sizeBytes,
      width: r.width,
      height: r.height,
      checksumSha256: r.checksumSha256,
    };
  }

  /** Whether an image exists. Deliberately NOT gated by beforeGet (returns no data or metadata). */
  exists(
    imageId: string,
    options: Pick<CallOptions, "connection"> = {},
  ): Promise<boolean> {
    return this.backend.exists(imageId, options);
  }

  /** Delete an image. Returns true if it existed. */
  delete(
    imageId: string,
    options: Pick<CallOptions, "connection"> = {},
  ): Promise<boolean> {
    return this.backend.delete(imageId, options);
  }

  /** Delete many in one query. Results are in input order; a missing id is `deleted: false`, not an error. */
  async deleteMany(
    imageIds: string[],
    options: Pick<CallOptions, "connection"> = {},
  ): Promise<BatchDeleteResult[]> {
    if (imageIds.length === 0) return [];
    const deleted = new Set(
      (await this.backend.deleteMany(imageIds, options)).map((id) =>
        id.toLowerCase(),
      ),
    );
    return imageIds.map((imageId) => ({
      imageId,
      deleted: deleted.has(imageId.toLowerCase()),
      error: null,
      success: true,
    }));
  }

  /** Release the pool (if ZeroBucket created it). */
  close(): Promise<void> {
    return this.backend.close();
  }
}

// ---- input handling ----------------------------------------------------

/**
 * Read any supported input into a Buffer. Streams are consumed in bounded
 * chunks and rejected as soon as they pass maxBytes (reported size is then a
 * lower bound), rather than buffering an arbitrarily large upload first.
 * Files and Blobs are size-checked BEFORE their bytes are read.
 */
export async function readInput(
  image: ImageInput,
  filename: string | undefined,
  maxBytes: number,
): Promise<{ data: Buffer; filename: string | undefined }> {
  if (typeof image === "string") {
    const size = (await stat(image)).size;
    if (size > maxBytes) throw new ImageTooLargeError(size, maxBytes);
    return {
      data: await readFile(image),
      filename: filename ?? basename(image),
    };
  }
  if (Buffer.isBuffer(image)) return { data: image, filename };
  if (image instanceof Uint8Array)
    return {
      data: Buffer.from(image.buffer, image.byteOffset, image.byteLength),
      filename,
    };
  if (image instanceof ArrayBuffer)
    return { data: Buffer.from(image), filename };
  if (typeof Blob !== "undefined" && image instanceof Blob) {
    if (image.size > maxBytes)
      throw new ImageTooLargeError(image.size, maxBytes);
    const name = (image as { name?: unknown }).name;
    return {
      data: Buffer.from(await image.arrayBuffer()),
      filename: filename ?? (typeof name === "string" ? name : undefined),
    };
  }
  if (
    image &&
    typeof (image as AsyncIterable<unknown>)[Symbol.asyncIterator] ===
      "function"
  ) {
    const chunks: Buffer[] = [];
    let total = 0;
    for await (const chunk of image as AsyncIterable<Uint8Array | string>) {
      const buf =
        typeof chunk === "string"
          ? Buffer.from(chunk)
          : Buffer.from(chunk.buffer, chunk.byteOffset, chunk.byteLength);
      total += buf.length;
      if (total > maxBytes) {
        (image as { destroy?: () => void }).destroy?.();
        throw new ImageTooLargeError(total, maxBytes);
      }
      chunks.push(buf);
    }
    const p = (image as { path?: unknown }).path;
    return {
      data: Buffer.concat(chunks, total),
      filename: filename ?? (typeof p === "string" ? basename(p) : undefined),
    };
  }
  throw new TypeError(
    "Unsupported image input. Pass a Buffer, Uint8Array, file path string, Blob/File, or a readable stream.",
  );
}

async function mapLimit<T>(
  items: T[],
  limit: number,
  fn: (item: T, index: number) => Promise<void>,
): Promise<void> {
  let next = 0;
  const workers = Array.from(
    { length: Math.min(limit, items.length) },
    async () => {
      while (next < items.length) {
        const i = next++;
        await fn(items[i]!, i);
      }
    },
  );
  await Promise.all(workers);
}

function errText(exc: unknown): string {
  return exc instanceof Error ? exc.message : String(exc);
}
