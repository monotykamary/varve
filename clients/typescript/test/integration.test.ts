import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { connect, VarveAuthenticationError } from "../src/index.js";

const binary = process.env.VARVE_TEST_BINARY;
if (!binary) throw new Error("VARVE_TEST_BINARY is required; integration coverage needs an actual Varve server binary");

async function unusedPort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("failed to allocate test port");
  await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  return address.port;
}

async function waitForReady(url: string, child: ChildProcess, stderr: () => string): Promise<void> {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) throw new Error(`Varve exited during startup (${child.exitCode}): ${stderr()}`);
    try {
      const response = await fetch(url);
      if (response.ok) return;
    } catch { /* server is still starting */ }
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  throw new Error(`timed out waiting for Varve: ${stderr()}`);
}

async function stop(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null) return;
  child.kill("SIGINT");
  await new Promise<void>(resolve => {
    const timer = setTimeout(() => { child.kill("SIGKILL"); resolve(); }, 5_000);
    timer.unref();
    child.once("exit", () => { clearTimeout(timer); resolve(); });
  });
}

test("runs the public client against an actual Varve server", { timeout: 30_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), "varve-ts-client-"));
  const port = await unusedPort();
  const token = "integration-token-at-least-32-bytes";
  let stderr = "";
  const child = spawn(binary, ["--data", join(directory, "data"), "serve", "--port", String(port)], {
    env: { ...process.env, VARVE_API_TOKEN: token },
    stdio: ["ignore", "ignore", "pipe"],
  });
  child.stderr?.setEncoding("utf8");
  child.stderr?.on("data", chunk => { stderr += String(chunk); });
  try {
    await waitForReady(`http://127.0.0.1:${port}/ready`, child, () => stderr);
    await assert.rejects(connect(`ws://127.0.0.1:${port}/v1/ws`, { token: "wrong-token" }), VarveAuthenticationError);
    const client = await connect(`ws://127.0.0.1:${port}/v1/ws`, { token });
    assert.deepEqual(await client.ping(), { pong: true });
    await client.createTable("metrics", {});
    const first = await client.insert("metrics", {
      timestamp_us: 9_007_199_254_740_993n,
      tenant: "acme",
      series: "cpu",
      value: 1.5,
      tags: { host: "a" },
    }, "integration-one");
    assert.equal(first.rows, 1n);
    const batch = await client.insertBatch("metrics", [
      { timestamp_us: 2n, tenant: "acme", series: "cpu", value: 2, tags: { host: "a" } },
      { timestamp_us: 3n, tenant: "acme", series: "cpu", value: 3, tags: { host: "b" } },
    ], "integration-batch");
    assert.equal(batch.rows, 2n);
    const concurrent = await Promise.all([4n, 5n, 6n].map(timestamp => client.insert(
      "metrics",
      { timestamp_us: timestamp, tenant: "acme", series: "cpu", value: Number(timestamp) },
      `integration-${timestamp}`,
    )));
    assert.equal(concurrent.length, 3);
    const result = await client.query("SELECT count(*) AS count FROM metrics");
    assert.deepEqual(result, [{ count: 6n }]);
    const timestamps = await client.query("SELECT timestamp_us FROM metrics ORDER BY timestamp_us");
    assert.deepEqual(timestamps, [2n, 3n, 4n, 5n, 6n, 9_007_199_254_740_993n].map(timestamp_us => ({ timestamp_us })));
    const status = await client.status();
    assert.equal(status.tables, 1n);
    await client.close();
  } finally {
    await stop(child);
    await rm(directory, { recursive: true, force: true });
  }
});
