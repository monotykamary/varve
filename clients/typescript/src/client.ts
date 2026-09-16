import { parse, parseNumberAndBigInt, stringify } from "lossless-json";
import {
  VarveAbortError,
  VarveAdmissionError,
  VarveAmbiguousOutcomeError,
  VarveAuthenticationError,
  VarveClientLimitError,
  VarveConnectionError,
  VarveProtocolError,
  VarveRequestTimeoutError,
  VarveRpcError,
  VarveValidationError,
} from "./errors.js";
import type {
  CloseOptions,
  ConnectOptions,
  JsonObject,
  JsonValue,
  RequestOptions,
  Row,
  Status,
  TableConfig,
  VarveClient,
  WebSocketConstructor,
  WebSocketLike,
  WriteReceipt,
} from "./types.js";
import {
  validateConnectOptions,
  validateRequestId,
  validateRow,
  validateSql,
  validateTableConfig,
  validateTableName,
  validateTimeout,
  validateUrl,
} from "./validation.js";

const PROTOCOL = "varve.v1";
const OPEN = 1;
const DEFAULT_CONNECT_TIMEOUT_MS = 5_000;
const DEFAULT_AUTH_TIMEOUT_MS = 5_000;
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const DEFAULT_CLOSE_TIMEOUT_MS = 5_000;
const DEFAULT_MAX_PENDING_REQUESTS = 32;
const DEFAULT_MAX_PENDING_BYTES = 8 * 1024 * 1024;
const DEFAULT_MAX_FRAME_BYTES = 8 * 1024 * 1024;
const encoder = new TextEncoder();

type State = "connecting" | "authenticating" | "open" | "closing" | "closed";
type Params = Record<string, unknown>;
type Resolver = (value: unknown) => void;

type Startup = {
  resolve: () => void;
  reject: (error: unknown) => void;
  timer: ReturnType<typeof setTimeout>;
  removeAbort?: () => void;
};

type Pending = {
  id: string;
  method: string;
  requestId?: string;
  mutation: boolean;
  bytes: number;
  resolve: Resolver;
  reject: (error: unknown) => void;
  timer: ReturnType<typeof setTimeout>;
  removeAbort?: () => void;
};

type RpcErrorShape = { code: number; message: string };

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasOwn(value: object, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(value, key);
}

function encode(value: unknown): string {
  const text = stringify(value);
  if (typeof text !== "string") throw new VarveValidationError("request cannot be encoded as JSON");
  return text;
}

function decode(text: string): unknown {
  return parse(text, null, parseNumberAndBigInt);
}

function parseRpcError(value: unknown): RpcErrorShape | undefined {
  if (!isRecord(value) || typeof value.message !== "string") return undefined;
  const rawCode = value.code;
  if (typeof rawCode !== "number" && typeof rawCode !== "bigint") return undefined;
  const code = Number(rawCode);
  if (!Number.isSafeInteger(code)) return undefined;
  return { code, message: value.message };
}

function mutationContext(pending: Pending): { method: string; rpcId: string; requestId?: string } {
  return pending.requestId === undefined
    ? { method: pending.method, rpcId: pending.id }
    : { method: pending.method, rpcId: pending.id, requestId: pending.requestId };
}

class Client implements VarveClient {
  private state: State = "connecting";
  private readonly pending = new Map<string, Pending>();
  private pendingBytes = 0;
  private nextId = 1n;
  private startup: Startup | undefined;
  private closePromise: Promise<void> | undefined;
  private closeResolve: (() => void) | undefined;
  private closeTimer: ReturnType<typeof setTimeout> | undefined;

  private readonly requestTimeoutMs: number;
  private readonly closeTimeoutMs: number;
  private readonly maxPendingRequests: number;
  private readonly maxPendingBytes: number;
  private readonly maxFrameBytes: number;

  private readonly onOpenListener = (): void => this.onOpen();
  private readonly onMessageListener = (event: unknown): void => this.onMessage(event);
  private readonly onCloseListener = (): void => this.onClose();
  private readonly onErrorListener = (): void => this.onError();

  constructor(
    private readonly socket: WebSocketLike,
    private readonly token: string,
    private readonly connectTimeoutMs: number,
    private readonly authTimeoutMs: number,
    options: ConnectOptions,
  ) {
    this.requestTimeoutMs = options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
    this.closeTimeoutMs = options.closeTimeoutMs ?? DEFAULT_CLOSE_TIMEOUT_MS;
    this.maxPendingRequests = options.maxPendingRequests ?? DEFAULT_MAX_PENDING_REQUESTS;
    this.maxPendingBytes = options.maxPendingBytes ?? DEFAULT_MAX_PENDING_BYTES;
    this.maxFrameBytes = options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES;
    this.socket.addEventListener("open", this.onOpenListener);
    this.socket.addEventListener("message", this.onMessageListener);
    this.socket.addEventListener("close", this.onCloseListener);
    this.socket.addEventListener("error", this.onErrorListener);
  }

  initialize(signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) {
      this.failConnection(new VarveAbortError("connection aborted before opening", false));
      this.safeClose();
      return Promise.reject(new VarveAbortError("connection aborted before opening", false));
    }
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.failConnection(new VarveConnectionError("WebSocket connect timed out"));
        this.safeClose();
      }, this.connectTimeoutMs);
      const startup: Startup = { resolve, reject, timer };
      if (signal !== undefined) {
        const abort = (): void => {
          this.failConnection(new VarveAbortError("connection or authentication aborted", false));
          this.safeClose();
        };
        signal.addEventListener("abort", abort, { once: true });
        startup.removeAbort = () => signal.removeEventListener("abort", abort);
      }
      this.startup = startup;
    });
  }

  insert(table: string, row: Row, requestId: string, options?: RequestOptions): Promise<WriteReceipt> {
    return this.insertBatch(table, [row], requestId, options);
  }

  insertBatch(table: string, rows: readonly Row[], requestId: string, options?: RequestOptions): Promise<WriteReceipt> {
    validateTableName(table);
    validateRequestId(requestId);
    if (!Array.isArray(rows) || rows.length === 0) throw new VarveValidationError("rows must be a nonempty array");
    rows.forEach((row, index) => validateRow(row, index));
    return this.request("write", { table, request_id: requestId, rows }, options, true, requestId) as Promise<WriteReceipt>;
  }

  query<T extends JsonValue = JsonObject>(sql: string, options?: RequestOptions): Promise<T> {
    validateSql(sql);
    // SQL includes management CALLs; do not infer read-only safety from text.
    return this.request("query", { sql }, options, true) as Promise<T>;
  }

  createTable<T extends JsonValue = JsonObject>(name: string, config: TableConfig = {}, options?: RequestOptions): Promise<T> {
    validateTableName(name);
    validateTableConfig(config);
    return this.request("create", { name, config }, options, true) as Promise<T>;
  }

  status(options?: RequestOptions): Promise<Status> {
    return this.request("status", {}, options, false) as Promise<Status>;
  }

  ping(options?: RequestOptions): Promise<{ pong: true }> {
    return this.request("ping", {}, options, false) as Promise<{ pong: true }>;
  }

  close(options: CloseOptions = {}): Promise<void> {
    if (this.state === "closed") return Promise.resolve();
    if (this.closePromise !== undefined) return this.closePromise;
    const timeoutMs = validateTimeout(options.timeoutMs, this.closeTimeoutMs);
    this.state = "closing";
    this.rejectAll(new VarveConnectionError("client closed before a response was received"));
    this.closePromise = new Promise<void>((resolve, reject) => {
      this.closeResolve = resolve;
      this.closeTimer = setTimeout(() => {
        const error = new VarveConnectionError("WebSocket close timed out");
        this.finalizeClosed();
        reject(error);
      }, timeoutMs);
      try {
        this.socket.close(1000, "client close");
      } catch (cause) {
        const error = new VarveConnectionError("WebSocket close failed", { cause });
        this.finalizeClosed();
        reject(error);
      }
    });
    return this.closePromise;
  }

  private onOpen(): void {
    if (this.state !== "connecting" || this.startup === undefined) return;
    clearTimeout(this.startup.timer);
    if (this.socket.protocol !== PROTOCOL) {
      this.failConnection(new VarveProtocolError(`server did not negotiate ${PROTOCOL}`));
      this.safeClose();
      return;
    }
    this.state = "authenticating";
    this.startup.timer = setTimeout(() => {
      this.failConnection(new VarveAuthenticationError("authentication timed out"));
      this.safeClose();
    }, this.authTimeoutMs);
    try {
      const frame = encode({ jsonrpc: "2.0", id: "auth", method: "auth", params: { token: this.token } });
      const bytes = encoder.encode(frame).byteLength;
      if (bytes > this.maxFrameBytes) throw new VarveClientLimitError("authentication frame exceeds maxFrameBytes");
      if (bytes > this.maxPendingBytes - this.bufferedBytes()) throw new VarveClientLimitError("authentication frame exceeds maxPendingBytes");
      this.socket.send(frame);
    } catch (cause) {
      this.failConnection(cause instanceof Error ? cause : new VarveConnectionError("authentication send failed", { cause }));
      this.safeClose();
    }
  }

  private onMessage(event: unknown): void {
    const data = isRecord(event) ? event.data : undefined;
    if (typeof data !== "string") {
      this.failConnection(new VarveProtocolError("Varve requires text WebSocket frames"));
      this.safeClose();
      return;
    }
    if (encoder.encode(data).byteLength > this.maxFrameBytes) {
      this.failConnection(new VarveProtocolError("response frame exceeds maxFrameBytes"));
      this.safeClose();
      return;
    }
    let response: unknown;
    try {
      response = decode(data);
    } catch (cause) {
      this.failConnection(new VarveProtocolError("server sent invalid JSON", { cause }));
      this.safeClose();
      return;
    }
    if (!isRecord(response) || response.jsonrpc !== "2.0" || typeof response.id !== "string") {
      this.failConnection(new VarveProtocolError("server sent an invalid JSON-RPC response"));
      this.safeClose();
      return;
    }
    if (this.state === "authenticating") {
      this.handleAuth(response);
      return;
    }
    if (this.state !== "open") return;
    const pending = this.pending.get(response.id);
    if (pending === undefined) return;
    const hasResult = hasOwn(response, "result");
    const hasError = hasOwn(response, "error");
    if (hasResult === hasError) {
      this.failConnection(new VarveProtocolError("JSON-RPC response must contain exactly one of result or error"));
      this.safeClose();
      return;
    }
    if (hasError) {
      const rpcError = parseRpcError(response.error);
      if (rpcError === undefined) {
        // Keep the correlation until interruption handling classifies sent mutations.
        this.failConnection(new VarveProtocolError("server sent an invalid JSON-RPC error"));
        this.safeClose();
        return;
      }
      this.removePending(pending);
      pending.reject(this.rpcFailure(pending, rpcError));
    } else {
      this.removePending(pending);
      pending.resolve(response.result);
    }
  }

  private handleAuth(response: Record<string, unknown>): void {
    if (response.id !== "auth") {
      this.failConnection(new VarveProtocolError("authentication response used an unexpected ID"));
      this.safeClose();
      return;
    }
    const rpcError = parseRpcError(response.error);
    if (rpcError !== undefined) {
      this.failConnection(new VarveAuthenticationError(rpcError.message));
      this.safeClose();
      return;
    }
    if (!isRecord(response.result) || (response.result.protocol !== 1 && response.result.protocol !== 1n)) {
      this.failConnection(new VarveProtocolError("server returned an unsupported protocol version"));
      this.safeClose();
      return;
    }
    this.state = "open";
    const startup = this.takeStartup();
    startup?.resolve();
  }

  private onClose(): void {
    if (this.state === "closing") {
      const resolve = this.closeResolve;
      this.finalizeClosed();
      resolve?.();
      return;
    }
    if (this.state !== "closed") this.failConnection(new VarveConnectionError("WebSocket disconnected"));
  }

  private onError(): void {
    if (this.state === "closed" || this.state === "closing") return;
    this.failConnection(new VarveConnectionError("WebSocket transport error"));
    this.safeClose();
  }

  private bufferedBytes(): number {
    let bytes: unknown;
    try {
      bytes = this.socket.bufferedAmount;
    } catch (cause) {
      throw new VarveProtocolError("WebSocket bufferedAmount is unavailable", { cause });
    }
    if (typeof bytes !== "number" || !Number.isSafeInteger(bytes) || bytes < 0) {
      throw new VarveProtocolError("WebSocket bufferedAmount must be a nonnegative safe integer byte count");
    }
    return bytes;
  }

  private request(
    method: string,
    params: Params,
    options: RequestOptions | undefined,
    mutation: boolean,
    requestId?: string,
  ): Promise<unknown> {
    if (this.state !== "open" || this.socket.readyState !== OPEN) {
      return Promise.reject(new VarveConnectionError("client is not connected"));
    }
    if (options?.signal?.aborted) return Promise.reject(new VarveAbortError("request aborted before admission", false));
    const timeoutMs = validateTimeout(options?.timeoutMs, this.requestTimeoutMs);
    if (this.pending.size >= this.maxPendingRequests) {
      return Promise.reject(new VarveClientLimitError("maxPendingRequests exceeded; request was not sent"));
    }
    let bufferedBytes: number;
    try {
      bufferedBytes = this.bufferedBytes();
    } catch (cause) {
      const error = cause instanceof Error ? cause : new VarveProtocolError("invalid WebSocket byte accounting", { cause });
      this.failConnection(error);
      this.safeClose();
      return Promise.reject(error);
    }
    const id = `v1-${this.nextId.toString(36)}`;
    this.nextId += 1n;
    let frame: string;
    try {
      frame = encode({ jsonrpc: "2.0", id, method, params });
    } catch (cause) {
      return Promise.reject(cause);
    }
    const bytes = encoder.encode(frame).byteLength;
    if (bytes > this.maxFrameBytes) return Promise.reject(new VarveClientLimitError("request exceeds maxFrameBytes; request was not sent"));
    // Correlation cleanup cannot release native frames. Conservatively double-charge
    // pending frames still buffered, and reuse outbound credit only as the socket drains.
    if (bytes > this.maxPendingBytes - bufferedBytes - this.pendingBytes) {
      return Promise.reject(new VarveClientLimitError("maxPendingBytes exceeded; request was not sent"));
    }
    return new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        const pending = this.pending.get(id);
        if (pending === undefined) return;
        this.removePending(pending);
        reject(this.interruptionFailure(pending, new VarveRequestTimeoutError("accepted request timed out locally")));
      }, timeoutMs);
      const pending: Pending = { id, method, mutation, bytes, resolve, reject, timer };
      if (requestId !== undefined) pending.requestId = requestId;
      if (options?.signal !== undefined) {
        const signal = options.signal;
        const abort = (): void => {
          const active = this.pending.get(id);
          if (active === undefined) return;
          this.removePending(active);
          reject(this.interruptionFailure(active, new VarveAbortError("accepted request aborted locally", true)));
        };
        signal.addEventListener("abort", abort, { once: true });
        pending.removeAbort = () => signal.removeEventListener("abort", abort);
      }
      this.pending.set(id, pending);
      this.pendingBytes += bytes;
      try {
        this.socket.send(frame);
      } catch (cause) {
        this.removePending(pending);
        reject(this.interruptionFailure(pending, new VarveConnectionError("WebSocket send failed", { cause })));
      }
    });
  }

  private rpcFailure(pending: Pending, error: RpcErrorShape): Error {
    if (error.code === -32003) return new VarveAdmissionError(error.code, error.message, pending.id, pending.method);
    const rpc = new VarveRpcError(error.code, error.message, pending.id, pending.method);
    if (pending.mutation && error.code === -32000) return this.ambiguousFailure(pending, rpc);
    if (error.code === -32001) return new VarveAuthenticationError(error.message, { cause: rpc });
    return rpc;
  }

  private interruptionFailure(pending: Pending, cause: Error): Error {
    return pending.mutation ? this.ambiguousFailure(pending, cause) : cause;
  }

  private ambiguousFailure(pending: Pending, cause: Error): VarveAmbiguousOutcomeError {
    const guidance = pending.requestId === undefined
      ? "Inspect server state before retrying this mutation."
      : `Retry only with the same request ID ${JSON.stringify(pending.requestId)} and an identical payload.`;
    return new VarveAmbiguousOutcomeError(`The ${pending.method} outcome is ambiguous. ${guidance}`, mutationContext(pending), { cause });
  }

  private removePending(pending: Pending): void {
    if (!this.pending.delete(pending.id)) return;
    clearTimeout(pending.timer);
    pending.removeAbort?.();
    this.pendingBytes -= pending.bytes;
  }

  private rejectAll(cause: Error): void {
    for (const pending of [...this.pending.values()]) {
      this.removePending(pending);
      pending.reject(this.interruptionFailure(pending, cause));
    }
  }

  private takeStartup(): Startup | undefined {
    const startup = this.startup;
    if (startup === undefined) return undefined;
    clearTimeout(startup.timer);
    startup.removeAbort?.();
    this.startup = undefined;
    return startup;
  }

  private failConnection(error: Error): void {
    if (this.state === "closed") return;
    this.state = "closed";
    const startup = this.takeStartup();
    startup?.reject(error);
    this.rejectAll(error);
    if (this.closeTimer !== undefined) clearTimeout(this.closeTimer);
    this.detach();
  }

  private finalizeClosed(): void {
    if (this.closeTimer !== undefined) clearTimeout(this.closeTimer);
    this.closeTimer = undefined;
    this.state = "closed";
    this.closeResolve = undefined;
    this.detach();
  }

  private safeClose(): void {
    try { this.socket.close(1000, "client failure"); } catch { /* transport already failed */ }
  }

  private detach(): void {
    this.socket.removeEventListener("open", this.onOpenListener);
    this.socket.removeEventListener("message", this.onMessageListener);
    this.socket.removeEventListener("close", this.onCloseListener);
    this.socket.removeEventListener("error", this.onErrorListener);
  }
}

export async function connect(urlInput: string | URL, options: ConnectOptions = {}): Promise<VarveClient> {
  validateConnectOptions(options);
  const url = validateUrl(urlInput);
  const WebSocketImpl = options.WebSocket ?? (globalThis.WebSocket as unknown as WebSocketConstructor | undefined);
  if (WebSocketImpl === undefined) throw new VarveConnectionError("no global WebSocket is available; inject ConnectOptions.WebSocket");
  let socket: WebSocketLike;
  try {
    socket = new WebSocketImpl(url, PROTOCOL);
  } catch (cause) {
    throw new VarveConnectionError("WebSocket construction failed", { cause });
  }
  const client = new Client(
    socket,
    options.token ?? "",
    options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS,
    options.authTimeoutMs ?? DEFAULT_AUTH_TIMEOUT_MS,
    options,
  );
  await client.initialize(options.signal);
  return client;
}
