import { connect, type Row } from "@monotykamary/varve";

async function mapConcurrent<T, R>(values: readonly T[], concurrency: number, work: (value: T, index: number) => Promise<R>): Promise<R[]> {
  const results = new Array<R>(values.length);
  let next = 0;
  async function worker(): Promise<void> {
    while (next < values.length) {
      const index = next++;
      results[index] = await work(values[index]!, index);
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, values.length) }, worker));
  return results;
}

const client = await connect("ws://127.0.0.1:7878/v1/ws", {
  token: "",
  maxPendingRequests: 4,
});
const rows: Row[] = Array.from({ length: 100 }, (_, index) => ({
  timestamp_us: 1_735_689_600_000_000n + BigInt(index),
  tenant: "acme",
  series: "cpu",
  value: index / 100,
}));

try {
  const receipts = await mapConcurrent(rows, 4, (row, index) =>
    client.insert("metrics", row, `single-${index}`));
  console.log(`committed ${receipts.length} independent writes`);
} finally {
  await client.close();
}
