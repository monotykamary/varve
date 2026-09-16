export type Int64 = number | bigint;
export type JsonInteger = number | bigint;
export type JsonPrimitive = string | number | bigint | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };

export interface Row {
  timestamp_us: Int64;
  tenant: string;
  series: string;
  value: number;
  tags?: Record<string, string>;
}

export interface TableConfig {
  shards?: number;
  window_us?: Int64;
  late_after_us?: Int64 | null;
  retention_us?: Int64 | null;
  archive_after_us?: Int64 | null;
  rollup_widths_us?: Int64[];
  rollup_retention_us?: Int64 | null;
  idempotency_window_us?: Int64 | null;
}

export interface WriteReceipt {
  sequence: JsonInteger;
  rows: JsonInteger;
  duplicate: boolean;
  durability: string;
}

export interface Status {
  database_id: string;
  sequence: JsonInteger;
  checkpoint_sequence: JsonInteger;
  remote_sequence: JsonInteger;
  unshipped_batches: JsonInteger;
  hot_rows: JsonInteger;
  hot_bytes: JsonInteger;
  wal_bytes: JsonInteger;
  disk_bytes: JsonInteger;
  metadata_bytes: JsonInteger;
  decoded_cache_bytes: JsonInteger;
  disk_cache_bytes: JsonInteger;
  tables: JsonInteger;
  segments: JsonInteger;
  rollup_groups: JsonInteger;
  idempotency_keys: JsonInteger;
  active_queries: JsonInteger;
  active_snapshots: JsonInteger;
  fenced: string | null;
  last_maintenance_error: string | null;
}

export interface RequestOptions {
  signal?: AbortSignal;
  timeoutMs?: number;
}

export interface CloseOptions {
  timeoutMs?: number;
}

export interface WebSocketLike {
  readonly readyState: number;
  /** Accurate native count of queued UTF-8 payload bytes, including cancelled requests. */
  readonly bufferedAmount: number;
  readonly protocol: string;
  binaryType?: BinaryType;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: string, listener: (event: unknown) => void): void;
  removeEventListener(type: string, listener: (event: unknown) => void): void;
}

export interface WebSocketConstructor {
  new (url: string | URL, protocols?: string | string[]): WebSocketLike;
}

export interface ConnectOptions {
  token?: string;
  WebSocket?: WebSocketConstructor;
  signal?: AbortSignal;
  connectTimeoutMs?: number;
  authTimeoutMs?: number;
  requestTimeoutMs?: number;
  closeTimeoutMs?: number;
  maxPendingRequests?: number;
  /** Admission budget: new frame bytes plus existing pending bytes and native bufferedAmount. */
  maxPendingBytes?: number;
  maxFrameBytes?: number;
}

export interface VarveClient {
  insert(table: string, row: Row, requestId: string, options?: RequestOptions): Promise<WriteReceipt>;
  insertBatch(table: string, rows: readonly Row[], requestId: string, options?: RequestOptions): Promise<WriteReceipt>;
  query<T extends JsonValue = JsonObject>(sql: string, options?: RequestOptions): Promise<T>;
  createTable<T extends JsonValue = JsonObject>(name: string, config?: TableConfig, options?: RequestOptions): Promise<T>;
  status(options?: RequestOptions): Promise<Status>;
  ping(options?: RequestOptions): Promise<{ pong: true }>;
  close(options?: CloseOptions): Promise<void>;
}
