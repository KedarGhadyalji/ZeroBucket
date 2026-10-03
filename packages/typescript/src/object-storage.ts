/**
 * S3-compatible object storage for tiering (AWS S3, MinIO, Cloudflare R2,
 * Backblaze B2, DigitalOcean Spaces, ...).
 *
 * Scoped to exactly what tiering needs: upload, download, ranged download,
 * idempotent delete, exists. The bucket must already exist: bucket creation
 * involves choices (region, versioning, lifecycle) that belong to you, not to
 * a storage library.
 *
 * `@aws-sdk/client-s3` is an optional peer dependency, imported lazily on first
 * use, so `import "zerobucket"` works without it.
 *
 * Objects are keyed by the image's UUID, with no prefix. That is the same key
 * scheme the Python package uses, so either package can read, tier and clean
 * up objects the other one created in the same bucket.
 */
import { StorageError } from "./errors.js";

/** The surface the Postgres adapter needs. Implement this to plug in a non-S3 store (or a test double). */
export interface ObjectStorageLike {
  readonly bucket: string;
  upload(key: string, data: Buffer, opts: { mimeType: string }): Promise<void>;
  download(key: string): Promise<Buffer>;
  /** Inclusive byte range, fetched with a real HTTP Range request. */
  downloadRange(
    key: string,
    start: number,
    endInclusive: number,
  ): Promise<Buffer>;
  /** Idempotent: deleting a missing key is not an error. */
  delete(key: string): Promise<void>;
  exists(key: string): Promise<boolean>;
}

export interface ObjectStorageOptions {
  /** Must already exist. */
  bucket: string;
  /** Set for anything that is not AWS S3 itself (MinIO, R2, B2, Spaces, ...). */
  endpoint?: string;
  /** Default: AWS_REGION / AWS_DEFAULT_REGION, else "us-east-1". Many S3-compatible services ignore it but the SDK requires one. */
  region?: string;
  /** Default: the SDK's standard chain (env vars, shared config, IAM role, ...). */
  credentials?: {
    accessKeyId: string;
    secretAccessKey: string;
    sessionToken?: string;
  };
  /** Path-style addressing (`endpoint/bucket/key`). Default: true when `endpoint` is set, else false. */
  forcePathStyle?: boolean;
  /** Bring your own configured S3Client (advanced). */
  client?: unknown;
}

type Sdk = typeof import("@aws-sdk/client-s3");
type S3 = import("@aws-sdk/client-s3").S3Client;

export class ObjectStorage implements ObjectStorageLike {
  readonly bucket: string;
  private clientPromise: Promise<{ sdk: Sdk; client: S3 }> | undefined;

  constructor(private readonly opts: ObjectStorageOptions) {
    if (!opts.bucket)
      throw new TypeError("ObjectStorage requires a bucket name");
    this.bucket = opts.bucket;
  }

  private connection(): Promise<{ sdk: Sdk; client: S3 }> {
    this.clientPromise ??= (async () => {
      let sdk: Sdk;
      try {
        sdk = await import("@aws-sdk/client-s3");
      } catch (cause) {
        throw new StorageError(
          "Object-storage tiering requires the optional dependency '@aws-sdk/client-s3'. " +
            "Install it with: npm install @aws-sdk/client-s3",
          { cause },
        );
      }
      const o = this.opts;
      const client =
        (o.client as S3 | undefined) ??
        new sdk.S3Client({
          endpoint: o.endpoint,
          region:
            o.region ??
            process.env.AWS_REGION ??
            process.env.AWS_DEFAULT_REGION ??
            "us-east-1",
          credentials: o.credentials,
          forcePathStyle: o.forcePathStyle ?? o.endpoint !== undefined,
        });
      return { sdk, client };
    })().catch((exc) => {
      this.clientPromise = undefined;
      throw exc;
    });
    return this.clientPromise;
  }

  async upload(
    key: string,
    data: Buffer,
    { mimeType }: { mimeType: string },
  ): Promise<void> {
    const { sdk, client } = await this.connection();
    try {
      await client.send(
        new sdk.PutObjectCommand({
          Bucket: this.bucket,
          Key: key,
          Body: data,
          ContentType: mimeType,
        }),
      );
    } catch (exc) {
      throw new StorageError(
        `Failed to upload ${JSON.stringify(key)} to bucket ${JSON.stringify(this.bucket)}: ${msg(exc)}`,
        { cause: exc },
      );
    }
  }

  async download(key: string): Promise<Buffer> {
    return this.get(key, undefined);
  }

  async downloadRange(
    key: string,
    start: number,
    endInclusive: number,
  ): Promise<Buffer> {
    return this.get(key, `bytes=${start}-${endInclusive}`);
  }

  private async get(key: string, range: string | undefined): Promise<Buffer> {
    const { sdk, client } = await this.connection();
    try {
      const res = await client.send(
        new sdk.GetObjectCommand({
          Bucket: this.bucket,
          Key: key,
          Range: range,
        }),
      );
      return Buffer.from(await res.Body!.transformToByteArray());
    } catch (exc) {
      throw new StorageError(
        `Failed to download ${JSON.stringify(key)} from bucket ${JSON.stringify(this.bucket)}: ${msg(exc)}`,
        { cause: exc },
      );
    }
  }

  async delete(key: string): Promise<void> {
    const { sdk, client } = await this.connection();
    try {
      // S3 DeleteObject succeeds for keys that do not exist, so this is idempotent by nature.
      await client.send(
        new sdk.DeleteObjectCommand({ Bucket: this.bucket, Key: key }),
      );
    } catch (exc) {
      throw new StorageError(
        `Failed to delete ${JSON.stringify(key)} from bucket ${JSON.stringify(this.bucket)}: ${msg(exc)}`,
        { cause: exc },
      );
    }
  }

  async exists(key: string): Promise<boolean> {
    const { sdk, client } = await this.connection();
    try {
      await client.send(
        new sdk.HeadObjectCommand({ Bucket: this.bucket, Key: key }),
      );
      return true;
    } catch (exc) {
      const e = exc as {
        name?: string;
        $metadata?: { httpStatusCode?: number };
      };
      if (
        e.name === "NotFound" ||
        e.name === "NoSuchKey" ||
        e.$metadata?.httpStatusCode === 404
      )
        return false;
      throw new StorageError(
        `Failed to check ${JSON.stringify(key)} in bucket ${JSON.stringify(this.bucket)}: ${msg(exc)}`,
        { cause: exc },
      );
    }
  }
}

function msg(exc: unknown): string {
  return exc instanceof Error ? exc.message : String(exc);
}
