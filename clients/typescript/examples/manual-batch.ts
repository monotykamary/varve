import { connect, type Row } from "@monotykamary/varve";

const client = await connect("ws://127.0.0.1:7878/v1/ws", { token: "" });
const rows: Row[] = [
  { timestamp_us: 1_735_689_600_000_000n, tenant: "acme", series: "cpu", value: 0.61, tags: { host: "a" } },
  { timestamp_us: 1_735_689_600_100_000n, tenant: "acme", series: "cpu", value: 0.64, tags: { host: "a" } },
];

try {
  const receipt = await client.insertBatch("metrics", rows, "batch-2025-01-01T00:00:00Z");
  console.log(receipt);
} finally {
  await client.close();
}
