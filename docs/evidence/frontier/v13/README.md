# v13 — S13 baseline failed on a transport disconnect

**Failed pilot, not a completed comparison or performance win.** S13's source, binary, profile and driver were attested, and the corrected controller passed startup. Baseline `acct101` then failed during mixed traffic with `aiohttp.ServerDisconnectedError`. Stress `acct102` and restart verification did not run. The last complete pair remains [v11](../v11/README.md).

## Observed results (Varve / Timescale)

| Completed or partial stage | Varve | Timescale |
| --- | ---: | ---: |
| Initial durable ingest, 250,000 rows/backend | 62.2k rows/s | 86.5k rows/s |
| Initial acknowledgement p95 | 130.04 ms | 138.08 ms |
| **Partial** mixed acknowledgement p95 | 54.08 ms | 42.95 ms |
| **Partial** concurrent-read p95, 40 samples/backend | 263.02 ms | 17.99 ms |

All 1,000 timed static-query checks completed before failure. Broad group/count cells favored Varve; selective/aggregate/window cells generally did not. The partial mixed interval offered 211,000 rows, recorded 210,000 paired acknowledgements, dropped none before admission, and classified **1,000 rows as failed or ambiguous**. The driver ledger's 460,000 common watermark is **not** a verified recovered frontier. No retry was added and no missing stress/recovery result is supplied.

## Timeline and provenance

- 05:31:59 UTC: reused actual S13 image as Varve `4aa57f43-7e00-4f15-a512-d1575a399632`; one controller, PID 67945.
- 05:32:31: exact binary `9366f866b775d56ebf827d540b9fd62e845950f2f15004e6c7be62299ec3f628`, source manifest `f57157e947995785f0d226b2dd80791b100d32527418898e579d07962edb8a9d`, fresh UUID and 31 phases attested.
- References: Timescale `cb2283a0-5849-47cd-bfaa-32eb036f4441`, driver `2daf52ca-e5a3-42fe-ac7a-2cc7fe40a133`. Driver bytes, database versions, nonroot UID, region, CPU/memory caps and synchronous durability passed the unchanged gates.
- 05:33:37–05:34:44: baseline ran and failed; the controller preserved its report/raw samples and stopped rather than launching stress.
- 05:35:46: all three exact deployments verified REMOVED, zero active benchmark deployments, original application unchanged, volumes/caps retained.

Qualified source remains `58f405adf7d472fdccf0011bc0bcc3f041201f9792ec1a7ab372002b1803944a`: 86 inputs, 410 Rust + 19 TypeScript checks, one live-S3 test ignored. Eighteen TS unit results reuse an exact-source-verified log; the actual-service check reran. Runtime profile remains `215f100b13ba1ba3d012d39117fa1895ed7978657434057a93390c6f359aa089`; effective driver remains `23e05928cf287a4f8b59433f2079cf63610a256dabbb75138a2ea5081442df23`. No rebuild or workload/resource/retention retuning occurred in this retry.

## Evidence versus hypothesis

Varve answered the post-failure metrics probe. Observed cgroup peak was **514,551,808 bytes**, with zero OOM events; HTTP application errors, request timeouts and rejected connections were zero. Connection-lifetime timeouts increased **0 → 4**. The server's default absolute connection lifetime is 65 seconds; failure occurred roughly 66 seconds after driver start. Transport retirement is a strong investigation lead, **not a proven per-connection cause**. No failed-request socket trace exists, and the bounded platform logs do not demonstrate a crash.

The new append-accounting phase recorded 460 observations, 0.376740 cumulative seconds and 2.570 ms maximum. Its observation interval includes untimed boundaries and overlaps other timers; it is not exclusive CPU attribution or evidence that the overall latency problem is solved.

## Monitor correction and preservation

The first offline handoff had 203 cases. Independent review caught a reference gate that could ignore Varve PENDING. Before any activation, Main preserved all 75 original inputs and changed exactly that call to require all three roles under the same 300-second budget. The actual-status/actual-call regression failed on the old controller and passed on the correction, including bounded nonconvergence. Independent re-review approved it. Final freeze: 43 controls / 78 inputs / 208 bound cases; 72 affected cases reran and 136 unchanged-source cases were reused. API-call timeout overshoot remains an explicitly documented inherited limitation.

The archive retains raw reports, observations, metrics, bounded deployment logs, actual receipts, source/driver bytes, both control checkpoints and independent reviews. Known deployment token/password values were checked absent before publication. Historical operational helpers contain original paths and must not be activated from the archive.

Verify stored hashes, raw latency summaries, static-oracle counts, source/driver identities, failure accounting and exact cleanup without network or load:

```sh
node docs/evidence/frontier/v13/witness/verify.mjs
```

No performance win, zero-ambiguity outcome, restart proof or production/distributed guarantee is claimed.
