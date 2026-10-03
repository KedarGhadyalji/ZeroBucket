export { ZeroBucket, DEFAULT_MAX_BYTES, readInput } from "./client.js";
export type {
  ImageInput,
  ZeroBucketOptions,
  PutOptions,
  PutManyOptions,
  GetOptions,
  StreamOptions,
  WritableLike,
  BeforeGetHook,
  BeforePutHook,
} from "./client.js";

export {
  PostgresBackend,
  DEFAULT_STREAM_CHUNK_SIZE,
} from "./adapters/postgres.js";
export type { PostgresBackendOptions } from "./adapters/postgres.js";
export type {
  StorageBackend,
  StoredRecord,
  StoredRecordMetadata,
  CallOptions,
} from "./adapters/base.js";

export type {
  Image,
  ImageMetadata,
  BatchPutResult,
  BatchGetResult,
  BatchDeleteResult,
  OperationEvent,
  Queryable,
  PreparedRow,
} from "./types.js";

export {
  ZeroBucketError,
  ContentValidationError,
  ImageValidationError,
  ImageTooLargeError,
  UnsupportedFormatError,
  CorruptedImageError,
  ImageNotFoundError,
  StorageError,
  AccessDeniedError,
} from "./errors.js";

export {
  validateImage,
  SUPPORTED_FORMATS,
  DEFAULT_MAX_PIXELS,
} from "./validation.js";
export type { ValidatedImage } from "./validation.js";
export { optimizeImage } from "./optimize.js";
export type { OptimizeOptions, OptimizationResult } from "./optimize.js";

export { PDFValidator } from "./validators/pdf.js";
export type { ContentValidator, ValidatedContent } from "./validators/types.js";
