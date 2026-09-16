import { VarveValidationError } from "./errors.js";
import type { ConnectOptions, Int64, Row, TableConfig } from "./types.js";

export const I64_MIN = -9_223_372_036_854_775_808n;
export const I64_MAX = 9_223_372_036_854_775_807n;
const encoder = new TextEncoder();
const tablePattern = /^[a-z](?:[a-z0-9]|_(?!_)){0,62}$/;
const configKeys = new Set(["shards", "window_us", "late_after_us", "retention_us", "archive_after_us", "rollup_widths_us", "rollup_retention_us", "idempotency_window_us"]);

function fail(message: string): never { throw new VarveValidationError(message); }
export function validatePositiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) fail(`${name} must be a positive safe integer`);
  return value;
}
export function validateConnectOptions(options: ConnectOptions): void {
  for (const key of ["connectTimeoutMs", "authTimeoutMs", "requestTimeoutMs", "closeTimeoutMs", "maxPendingRequests", "maxPendingBytes", "maxFrameBytes"] as const) {
    const value = options[key];
    if (value !== undefined) validatePositiveInteger(value, key);
  }
  if (options.maxPendingRequests !== undefined && options.maxPendingRequests > 1_024) fail("maxPendingRequests must not exceed 1024");
  if (options.maxPendingBytes !== undefined && options.maxPendingBytes > 64 * 1024 * 1024) fail("maxPendingBytes must not exceed 64 MiB");
  if (options.maxFrameBytes !== undefined && options.maxFrameBytes > 64 * 1024 * 1024) fail("maxFrameBytes must not exceed 64 MiB");
  if (options.token !== undefined && typeof options.token !== "string") fail("token must be a string");
}
export function validateTimeout(value: number | undefined, fallback: number): number {
  return value === undefined ? fallback : validatePositiveInteger(value, "timeoutMs");
}
export function validateUrl(input: string | URL): URL {
  let url: URL;
  try { url = new URL(input); } catch (cause) { throw new VarveValidationError("url must be an absolute ws: or wss: URL", { cause }); }
  if (url.protocol !== "ws:" && url.protocol !== "wss:") fail("url must use ws: or wss:");
  if (url.username || url.password || url.search || url.hash) fail("WebSocket URL must not contain credentials, query parameters, or a fragment");
  return url;
}
export function validateTableName(name: string): void {
  if (typeof name !== "string" || !tablePattern.test(name) || encoder.encode(name).byteLength > 63) fail("table name must be 1..63 ASCII bytes, start with a-z, and use a-z, 0-9, or single underscores");
}
export function validateRequestId(requestId: string): void {
  if (typeof requestId !== "string" || requestId.length === 0 || requestId.includes("\0") || encoder.encode(requestId).byteLength > 256) fail("requestId must be 1..256 non-NUL UTF-8 bytes");
}
export function validateInt64(value: Int64, name: string): bigint {
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) fail(`${name} number must be a safe integer; use bigint outside the safe range`);
  } else if (typeof value !== "bigint") fail(`${name} must be a safe integer number or bigint`);
  const integer = BigInt(value);
  if (integer < I64_MIN || integer > I64_MAX) fail(`${name} must fit signed i64`);
  return integer;
}
function validateSizedString(value: unknown, name: string, minimum: number, maximum: number): asserts value is string {
  if (typeof value !== "string") fail(`${name} must be a string`);
  const bytes = encoder.encode(value).byteLength;
  if (bytes < minimum || bytes > maximum || value.includes("\0")) fail(`${name} must be ${minimum}..${maximum} non-NUL UTF-8 bytes`);
}
export function validateRow(row: Row, index?: number): void {
  if (typeof row !== "object" || row === null || Array.isArray(row)) fail("row must be an object");
  const prefix = index === undefined ? "row" : `rows[${index}]`;
  validateInt64(row.timestamp_us, `${prefix}.timestamp_us`);
  validateSizedString(row.tenant, `${prefix}.tenant`, 1, 256);
  validateSizedString(row.series, `${prefix}.series`, 1, 1024);
  if (typeof row.value !== "number" || !Number.isFinite(row.value)) fail(`${prefix}.value must be a finite number`);
  if (row.tags === undefined) return;
  if (typeof row.tags !== "object" || row.tags === null || Array.isArray(row.tags)) fail(`${prefix}.tags must be a string-to-string object`);
  const prototype = Object.getPrototypeOf(row.tags) as unknown;
  if (prototype !== Object.prototype && prototype !== null) fail(`${prefix}.tags must be a plain string-to-string object`);
  const entries = Object.entries(row.tags);
  if (entries.length > 32) fail(`${prefix}.tags must contain at most 32 entries`);
  for (const [key, value] of entries) {
    validateSizedString(key, `${prefix}.tags key`, 1, 128);
    validateSizedString(value, `${prefix}.tags[${JSON.stringify(key)}]`, 0, 1024);
  }
}
function validatePositiveInt64(value: Int64 | null | undefined, name: string): void {
  if (value === undefined || value === null) return;
  if (validateInt64(value, name) <= 0n) fail(`${name} must be positive`);
}
export function validateTableConfig(config: TableConfig): void {
  if (typeof config !== "object" || config === null || Array.isArray(config)) fail("config must be an object");
  for (const key of Object.keys(config)) if (!configKeys.has(key)) fail(`unknown table config field: ${key}`);
  if (config.shards !== undefined && (!Number.isSafeInteger(config.shards) || config.shards < 1 || config.shards > 1024)) fail("config.shards must be an integer from 1 to 1024");
  validatePositiveInt64(config.window_us, "config.window_us");
  validatePositiveInt64(config.late_after_us, "config.late_after_us");
  validatePositiveInt64(config.retention_us, "config.retention_us");
  validatePositiveInt64(config.archive_after_us, "config.archive_after_us");
  validatePositiveInt64(config.rollup_retention_us, "config.rollup_retention_us");
  validatePositiveInt64(config.idempotency_window_us, "config.idempotency_window_us");
  if (config.rollup_widths_us !== undefined) {
    if (!Array.isArray(config.rollup_widths_us) || config.rollup_widths_us.length > 16) fail("config.rollup_widths_us must be an array with at most 16 entries");
    const seen = new Set<string>();
    config.rollup_widths_us.forEach((width, index) => {
      validatePositiveInt64(width, `config.rollup_widths_us[${index}]`);
      const key = String(width);
      if (seen.has(key)) fail("config.rollup_widths_us must not contain duplicates");
      seen.add(key);
    });
  }
}
export function validateSql(sql: string): void { if (typeof sql !== "string" || sql.length === 0) fail("sql must be a nonempty string"); }
