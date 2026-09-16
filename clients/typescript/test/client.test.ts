import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { parse, parseNumberAndBigInt } from "lossless-json";
import {
  connect,
  VarveAbortError,
  VarveAdmissionError,
  VarveAmbiguousOutcomeError,
  VarveAuthenticationError,
  VarveClientLimitError,
  VarveConnectionError,
  VarveRequestTimeoutError,
  VarveProtocolError,
  VarveRpcError,
  VarveValidationError,
  type ConnectOptions,
  type VarveClient,
  type WebSocketLike,
} from "../src/index.js";

const fixtures = JSON.parse(await readFile(new URL("../../test/fixtures/protocol.json", import.meta.url), "utf8")) as Record<string, string>;

type Listener = (event: unknown) => void;
type SendHandler = (socket: MockWebSocket, frame: string) => void;

class MockWebSocket implements WebSocketLike {
  static instances: MockWebSocket[] = [];
  static authFrame: string | undefined = fixtures.authSuccess!;
  static sendHandler: SendHandler | undefined;
  static autoOpen = true;
  static emitClose = true;
  readonly url: string;
  readonly requestedProtocols: string | string[] | undefined;
  protocol = "varve.v1";
  readyState = 0;
  bufferedAmount = 0;
  retained: string[] = [];
  retainSends = false;
  sent: string[] = [];
  private readonly listeners = new Map<string, Set<Listener>>();

  constructor(url: string | URL, protocols?: string | string[]) {
    this.url = String(url);
    this.requestedProtocols = protocols;
    MockWebSocket.instances.push(this);
    if (MockWebSocket.autoOpen) {
      queueMicrotask(() => {
        this.readyState = 1;
        this.emit("open", {});
      });
    }
  }

  send(data: string): void {
    if (this.readyState !== 1) throw new Error("not open");
    this.sent.push(data);
    if (this.retainSends) {
      this.retained.push(data);
      this.bufferedAmount += new TextEncoder().encode(data).byteLength;
    }
    const decoded = parse(data, null, parseNumberAndBigInt) as { id: string };
    if (decoded.id === "auth") {
      if (MockWebSocket.authFrame !== undefined) {
        queueMicrotask(() => this.receive(MockWebSocket.authFrame!));
      }
    } else {
      MockWebSocket.sendHandler?.(this, data);
    }
  }

  close(): void {
    if (this.readyState === 3) return;
    this.readyState = 3;
    if (MockWebSocket.emitClose) queueMicrotask(() => this.emit("close", {}));
  }

  addEventListener(type: string, listener: Listener): void {
    const listeners = this.listeners.get(type) ?? new Set<Listener>();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: string, listener: Listener): void {
    this.listeners.get(type)?.delete(listener);
  }

  receive(frame: string): void {
    this.emit("message", { data: frame });
  }

  respond(id: string, result: unknown): void {
    const text = JSON.stringify({ jsonrpc: "2.0", id, result });
    this.receive(text);
  }

  rpcError(id: string, code: number, message: string): void {
    this.receive(JSON.stringify({ jsonrpc: "2.0", id, error: { code, message } }));
  }

  listenerCount(): number {
    return [...this.listeners.values()].reduce((sum, listeners) => sum + listeners.size, 0);
  }

  private emit(type: string, event: unknown): void {
    for (const listener of [...(this.listeners.get(type) ?? [])]) listener(event);
  }
}

function decodeFrame(frame: string): { id: string; method: string; params: Record<string, unknown> } {
  return parse(frame, null, parseNumberAndBigInt) as { id: string; method: string; params: Record<string, unknown> };
}

async function client(options: ConnectOptions = {}): Promise<[VarveClient, MockWebSocket]> {
  MockWebSocket.instances = [];
  MockWebSocket.authFrame = fixtures.authSuccess!;
  MockWebSocket.sendHandler = undefined;
  MockWebSocket.autoOpen = true;
  MockWebSocket.emitClose = true;
  const connected = await connect("ws://127.0.0.1:7878/v1/ws", { WebSocket: MockWebSocket, token: "test-token", ...options });
  return [connected, MockWebSocket.instances[0]!];
}

test("uses the fixed subprotocol and sends the token only in the first frame", async () => {
  const [connected, socket] = await client();
  assert.equal(socket.url, "ws://127.0.0.1:7878/v1/ws");
  assert.equal(socket.requestedProtocols, "varve.v1");
  assert.equal(socket.sent[0], fixtures.authRequest);
  assert.ok(!socket.url.includes("test-token"));
  await connected.close();
});

test("bounds connect and authentication deadlines", async () => {
  MockWebSocket.instances = [];
  MockWebSocket.autoOpen = false;
  await assert.rejects(
    connect("ws://localhost/v1/ws", { WebSocket: MockWebSocket, connectTimeoutMs: 5 }),
    VarveConnectionError,
  );
  assert.equal(MockWebSocket.instances[0]!.listenerCount(), 0);

  MockWebSocket.autoOpen = true;
  MockWebSocket.authFrame = undefined;
  await assert.rejects(
    connect("ws://localhost/v1/ws", { WebSocket: MockWebSocket, authTimeoutMs: 5 }),
    VarveAuthenticationError,
  );
  assert.equal(MockWebSocket.instances.at(-1)!.listenerCount(), 0);
});

test("multiplexes unique IDs and accepts out-of-order responses", async () => {
  const [connected, socket] = await client();
  const ping = connected.ping();
  const status = connected.status();
  const first = decodeFrame(socket.sent[1]!);
  const second = decodeFrame(socket.sent[2]!);
  assert.notEqual(first.id, second.id);
  socket.respond(second.id, { database_id: "db", sequence: 2, control_root_bytes: 128, derived_encoded_bytes: 256, derived_resident_bytes: 512, derived_working_bytes: 0 });
  socket.respond(first.id, { pong: true });
  const snapshot = await status;
  assert.equal(snapshot.sequence, 2n);
  assert.equal(snapshot.control_root_bytes, 128n);
  assert.equal(snapshot.derived_encoded_bytes, 256n);
  assert.equal(snapshot.derived_resident_bytes, 512n);
  assert.equal(snapshot.derived_working_bytes, 0n);
  assert.deepEqual(await ping, { pong: true });
  await connected.close();
});

test("preserves bigint numeric literals and rejects unsafe numeric timestamps", async () => {
  const [connected, socket] = await client();
  const promise = connected.insert("metrics", {
    timestamp_us: -9_223_372_036_854_775_808n,
    tenant: "acme",
    series: "cpu",
    value: 1.25,
    tags: { host: "a" },
  }, "request-1");
  const request = decodeFrame(socket.sent[1]!);
  assert.match(socket.sent[1]!, /"timestamp_us":-9223372036854775808/);
  assert.equal(request.params.request_id, "request-1");
  socket.receive(fixtures.largeReceipt!.replace("REPLACE_ID", request.id));
  const receipt = await promise;
  assert.equal(receipt.sequence, 18_446_744_073_709_551_615n);
  assert.equal(receipt.rows, 1n);
  assert.throws(() => connected.insert("metrics", {
    timestamp_us: Number.MAX_SAFE_INTEGER + 1,
    tenant: "acme",
    series: "cpu",
    value: 1,
  }, "request-2"), VarveValidationError);
  assert.throws(() => connected.insert("metrics", {
    timestamp_us: 1,
    tenant: "acme",
    series: "cpu",
    value: Number.POSITIVE_INFINITY,
  }, "request-3"), VarveValidationError);
  assert.throws(() => connected.insert("metrics", {
    timestamp_us: 1,
    tenant: "acme",
    series: "cpu",
    value: 1,
    tags: { host: 1 as unknown as string },
  }, "request-4"), VarveValidationError);
  assert.throws(() => connected.insert("metrics", {
    timestamp_us: 1,
    tenant: "acme",
    series: "cpu",
    value: 1,
    tags: new Date() as unknown as Record<string, string>,
  }, "request-5"), VarveValidationError);
  await connected.close();
});

test("reports authentication, RPC, admission, and ambiguous write errors distinctly", async () => {
  MockWebSocket.instances = [];
  MockWebSocket.authFrame = fixtures.authUnauthorized!;
  await assert.rejects(
    connect("ws://localhost/v1/ws", { WebSocket: MockWebSocket, token: "wrong" }),
    VarveAuthenticationError,
  );

  const [connected, socket] = await client();
  const query = connected.query("SELECT 1");
  let frame = decodeFrame(socket.sent.at(-1)!);
  socket.rpcError(frame.id, -32602, "invalid parameters");
  await assert.rejects(query, (error: unknown) => error instanceof VarveRpcError && error.code === -32602);

  const rejected = connected.insert("metrics", { timestamp_us: 1, tenant: "a", series: "s", value: 1 }, "admission-id");
  frame = decodeFrame(socket.sent.at(-1)!);
  socket.rpcError(frame.id, -32003, "request rejected before admission");
  await assert.rejects(rejected, (error: unknown) => error instanceof VarveAdmissionError && error.admitted === false);

  const ambiguous = connected.insert("metrics", { timestamp_us: 2, tenant: "a", series: "s", value: 1 }, "stable-id");
  frame = decodeFrame(socket.sent.at(-1)!);
  socket.rpcError(frame.id, -32000, "operation failed; outcome may be committed");
  await assert.rejects(ambiguous, (error: unknown) => error instanceof VarveAmbiguousOutcomeError && error.requestId === "stable-id");
  await connected.close();
});

test("enforces concurrent pending and pending-byte limits before sending", async () => {
  const [bounded, socket] = await client({ maxPendingRequests: 1 });
  const first = bounded.ping();
  await assert.rejects(bounded.status(), VarveClientLimitError);
  assert.equal(socket.sent.length, 2);
  const firstFrame = decodeFrame(socket.sent[1]!);
  socket.respond(firstFrame.id, { pong: true });
  await first;
  await bounded.close();

  const [byteBounded, byteSocket] = await client({ maxPendingBytes: 159 });
  const pending = byteBounded.query("SELECT 123456789012345678901234567890");
  await assert.rejects(byteBounded.ping(), VarveClientLimitError);
  assert.equal(byteSocket.sent.length, 2);
  const pendingFrame = decodeFrame(byteSocket.sent[1]!);
  byteSocket.respond(pendingFrame.id, {});
  await pending;
  await byteBounded.close();
});

test("cancelled correlations do not release retained outbound byte credit", async () => {
  const [connected, socket] = await client({ maxPendingRequests: 1, maxPendingBytes: 512 });
  socket.retainSends = true;
  let rejected = 0;
  try {
    for (let index = 0; index < 100; index++) {
      const controller = new AbortController();
      const request = connected.query("SELECT '🪨'", { signal: controller.signal });
      controller.abort();
      await assert.rejects(request, (error: unknown) => {
        if (error instanceof VarveClientLimitError) rejected++;
        return error instanceof VarveClientLimitError || error instanceof VarveAmbiguousOutcomeError;
      });
    }
    assert.ok(socket.bufferedAmount <= 512, `retained ${socket.retained.length} frames / ${socket.bufferedAmount} bytes`);
    assert.equal(socket.retained.length, 6);
    assert.equal(socket.bufferedAmount, 474);
    assert.equal(rejected, 94);
    assert.equal(socket.bufferedAmount, socket.retained.reduce((sum, frame) => sum + new TextEncoder().encode(frame).byteLength, 0));

    // Only actual transport drain/discard restores outbound credit.
    socket.retained = [];
    socket.bufferedAmount = 0;
    const next = connected.ping();
    socket.respond(decodeFrame(socket.sent.at(-1)!).id, { pong: true });
    assert.deepEqual(await next, { pong: true });
  } finally {
    await connected.close();
  }
});

test("response and timeout cleanup cannot bypass native outbound accounting", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  for (const cleanup of ["response", "timeout"] as const) {
    const [connected, socket] = await client({ maxPendingRequests: 1, maxPendingBytes: 512, requestTimeoutMs: 10 });
    socket.retainSends = true;
    try {
      // This fits once, but not twice, even after the correlation is gone.
      const request = connected.query(`SELECT '${"🪨".repeat(50)}'`);
      if (cleanup === "response") {
        socket.respond(decodeFrame(socket.sent.at(-1)!).id, {});
        await request;
      } else {
        t.mock.timers.tick(10);
        await assert.rejects(request, VarveAmbiguousOutcomeError);
      }
      await assert.rejects(connected.query(`SELECT '${"🪨".repeat(50)}'`), VarveClientLimitError);
      assert.equal(socket.retained.length, 1);
      assert.ok(socket.bufferedAmount <= 512);
    } finally {
      await connected.close();
    }
  }
});

test("pending and buffered bytes are conservatively combined", async () => {
  const [connected, socket] = await client({ maxPendingBytes: 512 });
  socket.retainSends = true;
  try {
    const request = connected.query(`SELECT '${"x".repeat(125)}'`);
    const id = decodeFrame(socket.sent.at(-1)!).id;
    // Neither pending bytes nor buffered bytes alone would reject this frame.
    await assert.rejects(connected.query(`SELECT '${"x".repeat(125)}'`), VarveClientLimitError);
    socket.respond(id, {});
    await request;
  } finally {
    await connected.close();
  }
});

test("invalid injected bufferedAmount values fail closed before transmission", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  for (const value of [undefined, false, "0", -1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, 0n]) {
    const [connected, socket] = await client();
    Object.defineProperty(socket, "bufferedAmount", { value, configurable: true });
    const before = socket.sent.length;
    try {
      const request = connected.ping({ timeoutMs: 1 });
      t.mock.timers.tick(1);
      await assert.rejects(request, VarveProtocolError);
      assert.equal(socket.sent.length, before);
      assert.equal(socket.readyState, 3);
    } finally {
      await connected.close();
    }
  }
});

test("throwing byte counters fail closed and preserve existing mutation ambiguity", async () => {
  const [connected, socket] = await client();
  const cause = new Error("counter unavailable");
  try {
    const write = connected.insert("metrics", { timestamp_us: 1, tenant: "a", series: "s", value: 1 }, "counter-id");
    const before = socket.sent.length;
    Object.defineProperty(socket, "bufferedAmount", { get() { throw cause; } });
    await assert.rejects(connected.ping(), (error: unknown) => error instanceof VarveProtocolError && error.cause === cause);
    await assert.rejects(write, (error: unknown) => error instanceof VarveAmbiguousOutcomeError
      && error.cause instanceof VarveProtocolError && error.cause.cause === cause);
    assert.equal(socket.sent.length, before);
    assert.equal(socket.readyState, 3);
  } finally {
    await connected.close();
  }
});

test("SQL aborted before admission remains an ordinary abort and is never sent", async () => {
  const [connected, socket] = await client();
  try {
    const controller = new AbortController();
    controller.abort();
    const before = socket.sent.length;
    await assert.rejects(connected.query("CALL varve_create_table('metrics')", { signal: controller.signal }),
      (error: unknown) => error instanceof VarveAbortError && error.admitted === false);
    assert.equal(socket.sent.length, before);
  } finally {
    await connected.close();
  }
});

test("all SQL interruptions remain ambiguous, including management CALLs and SELECT timeouts", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  for (const sql of ["CALL varve_create_table('metrics')", "/* comment */ CALL varve_create_table('metrics')", "SELECT 1"]) {
    for (const interruption of ["abort", "timeout", "disconnect", "operation-error"] as const) {
      const [connected, socket] = await client({ requestTimeoutMs: 10 });
      try {
        const controller = new AbortController();
        const request = connected.query(sql, { signal: controller.signal });
        if (interruption === "abort") controller.abort();
        if (interruption === "timeout") t.mock.timers.tick(10);
        if (interruption === "disconnect") socket.close();
        if (interruption === "operation-error") socket.rpcError(decodeFrame(socket.sent.at(-1)!).id, -32000, "operation failed");
        await assert.rejects(request, (error: unknown) => {
          assert.ok(error instanceof VarveAmbiguousOutcomeError);
          assert.equal(error.method, "query");
          assert.equal(error.requestId, undefined);
          const causeType = interruption === "abort" ? VarveAbortError
            : interruption === "timeout" ? VarveRequestTimeoutError
            : interruption === "disconnect" ? VarveConnectionError : VarveRpcError;
          assert.ok(error.cause instanceof causeType);
          return true;
        });
      } finally {
        await connected.close();
      }
    }
  }
});

test("malformed responses preserve sent write ambiguity and protocol cause", async () => {
  for (const response of [
    (id: string) => JSON.stringify({ jsonrpc: "2.0", id, error: { message: "broken" } }),
    (id: string) => JSON.stringify({ jsonrpc: "2.0", id, error: { code: -32000, message: "broken" }, result: {} }),
    (id: string) => JSON.stringify({ jsonrpc: "wrong", id, result: {} }),
    () => "{invalid JSON",
  ]) {
    const [connected, socket] = await client();
    try {
      const write = connected.insert("metrics", { timestamp_us: 1, tenant: "a", series: "s", value: 1 }, "malformed-id");
      const id = decodeFrame(socket.sent.at(-1)!).id;
      socket.receive(response(id));
      await assert.rejects(write, (error: unknown) => {
        assert.ok(error instanceof VarveAmbiguousOutcomeError);
        assert.equal(error.requestId, "malformed-id");
        assert.equal(error.rpcId, id);
        assert.ok(error.cause instanceof VarveProtocolError);
        return true;
      });
    } finally {
      await connected.close();
    }
  }
});

test("timeouts and aborts release capacity while writes remain ambiguous", async () => {
  const [connected, socket] = await client({ maxPendingRequests: 1, requestTimeoutMs: 15 });
  await assert.rejects(connected.ping(), VarveRequestTimeoutError);
  const next = connected.ping();
  const nextFrame = decodeFrame(socket.sent.at(-1)!);
  socket.respond(nextFrame.id, { pong: true });
  await next;

  const timedWrite = connected.insert("metrics", { timestamp_us: 1, tenant: "a", series: "s", value: 1 }, "timeout-id");
  await assert.rejects(timedWrite, (error: unknown) => error instanceof VarveAmbiguousOutcomeError && error.requestId === "timeout-id");

  const controller = new AbortController();
  const aborted = connected.ping({ signal: controller.signal, timeoutMs: 1_000 });
  controller.abort();
  await assert.rejects(aborted, (error: unknown) => error instanceof Error && error.name === "AbortError");
  const afterAbort = connected.ping();
  const afterAbortFrame = decodeFrame(socket.sent.at(-1)!);
  socket.respond(afterAbortFrame.id, { pong: true });
  await afterAbort;
  await connected.close();
});

test("close rejects and cleans all pending requests without reconnecting", async () => {
  const [connected, socket] = await client({ requestTimeoutMs: 1_000 });
  const read = connected.ping();
  const write = connected.insert("metrics", { timestamp_us: 1, tenant: "a", series: "s", value: 1 }, "close-id");
  await connected.close();
  await assert.rejects(read, VarveConnectionError);
  await assert.rejects(write, VarveAmbiguousOutcomeError);
  assert.equal(socket.listenerCount(), 0);
  assert.equal(MockWebSocket.instances.length, 1);
});

test("bounds graceful close and removes listeners when the peer does not reply", async () => {
  const [connected, socket] = await client({ closeTimeoutMs: 5 });
  MockWebSocket.emitClose = false;
  await assert.rejects(connected.close(), VarveConnectionError);
  assert.equal(socket.listenerCount(), 0);
});

test("rejects credentials and query data in the WebSocket URL", async () => {
  await assert.rejects(connect("ws://token@localhost/v1/ws", { WebSocket: MockWebSocket }), VarveValidationError);
  await assert.rejects(connect("ws://localhost/v1/ws?token=secret", { WebSocket: MockWebSocket }), VarveValidationError);
  await assert.rejects(connect("ws://localhost/v1/ws", { WebSocket: MockWebSocket, maxPendingRequests: 1_025 }), VarveValidationError);
});
