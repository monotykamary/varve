# Evaluation results — 2026-09-15

**Verdict: working, hosted experimental evaluation; not production-certified.** Source is in this repository. The authenticated example is at **https://varve-production.up.railway.app**; `/health` and `/ready` are public, operator endpoints require `VARVE_API_TOKEN` from the Railway service variables. Do not publish the token or place irreplaceable data here.

## Verified implementation

Rust owns ingestion, fsynced WAL, metadata, time/shard partitions, hot memory, bounded caches, Parquet, conditional S3 publication, recovery, lifecycle policy, named numeric continuous aggregates and durable interval-job definitions. DuckDB v2 alpha executes restricted read-only SQL in subprocesses. This is not PostgreSQL compatibility, arbitrary-SQL IVM, tenant isolation, an OS SQL sandbox or distributed HA.

The final local harness passed **138 top-level Rust tests**, **14 Python tests**, formatting, strict all-target Clippy, the 2,000-row mixed workload, the 256-row local protocol/restore drill and RustSec audit (275 dependencies scanned). The opt-in live-S3 test remains excluded from the default harness; live transport and engine behavior were exercised separately below. Hosted GitHub CI was configured, not run. See `FINAL_REVIEW.md` for the witnessed fixes and new regression boundaries, and `AUDIT_RECONCILIATION.md` for remaining source/performance gaps identified when the delayed earlier audit was reconciled.

## Local artifact hygiene

After verification, the current normal executable passed the documented two-row CLI path, including disabled fault hooks, hot SQL, checkpoint, remote ship, cold restore and exact SQL. Only this repository's confirmed `target/` was cleaned, reclaiming **2.3 GiB**. The working executables remain in `.tools/` (139 MiB total); future Cargo builds/tests regenerate their disposable cache. No local container images or DuckDB C++ build were required.

## Live cloud evidence

- One vCPU, approximately 1 GB cgroup limit; one replica; persistent `/data`; daemon UID/GID 10001.
- HTTPS identity matched the privately observed database. Unauthenticated status/metrics were rejected. Authentication preceded body reads; normal SQL worked; filesystem/network/settings probes were denied.
- The release-built `varve-cloud-probe` passed against the actual dedicated S3-compatible bucket with **4,000 rows**: conditional create/update, stale-token rejection, fresh tokens for identical head payloads, immutable collisions, bounded reads, paginated listing, bulk deletion, Parquet/archive, SQL, named aggregates, fresh-directory remote restore, independent raw expiration and remote GC. Its random child prefix was `probe-f4c87505-e9f2-45e6-9352-6ac96bc4a1c7`; it did not transfer the example's main namespace.
- A public HTTPS run completed **100,000 rows** in **51.440 seconds**, approximately **1,944 rows/s**, with two writers, 512-row batches and no retries/errors. Raw and named-aggregate count/sum both matched `100000 / 6243750`. Expired request IDs were rejected without reinsertion.
- A separate **20,000-row** run completed in **10.775 seconds**, no retries/errors. Its raw rows were expired while its aggregate retained `20000 / 1248750`; the first table still matched `100000 / 6243750` afterward.

| 100,000-row run | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| Write batch round trip | 459.7 ms | 862.2 ms | 1252.6 ms |
| Concurrent SQL round trip | 812.9 ms | 1150.6 ms | 1175.9 ms |

Observed cgroup memory peak was **194.45 MiB**; daemon high-water RSS was **177,496 KiB**. The resource-sampling window consumed 13.716 CPU seconds across 61.963 wall seconds. Container metrics include children, cache and operational probes. Timings include network, queuing, filesystem durability and SQL subprocess startup; this compressible synthetic workload is **not a competitive benchmark or a production SLO**.

## Restart incident and recovery

The direct `railway restart` request timed out after 60 seconds. Its deployment still reported `SUCCESS`, but SSH reported an exited container and ingress was unavailable. The available logs did not establish a root cause. This is an unresolved instance-lifecycle qualification issue, not a successful restart claim.

Redeploying the **existing artifact**, without new source or changing/deleting the volume, created a new replica and restored service. All three executable hashes were identical. The database UUID was unchanged, and read-only queries verified all 100,000 raw rows and the exact named-aggregate count/sum. This proves volume-backed recreate/reopen persistence for this run; it does not prove automatic failover, hardware power-loss safety or a recovery-time guarantee.

A private probe also observed the documented conservative `/ready` 503 while maintenance held the state lock. Bounded retry succeeded. `/health` is liveness; do not treat a transient busy readiness hint as corruption or hide persistently failed readiness with unlimited retries.

## Provenance and remaining gates

- Verified source build: `e57b9f08-4852-4db8-9b0e-da6ef49e7452`.
- Current recreated deployment: `dbd59af2-5c4c-4e8e-a452-72914ca69528`, observed `SUCCESS` and checked over HTTPS and SSH.
- Current replica: `6c3d1955-e2c1-4185-b7f4-b4fcf85b4784` (previously `398e95d4-8566-4921-bee8-4d30f237f4d7`).
- Uploaded-tree manifest SHA-256: `578647d1fd0ad65013cc5d917a0d8028b8fdf91a4363cac2bb8b1cd0d5cb2809`. Post-run documentation is newer than this uploaded snapshot.
- Runtime-input manifest SHA-256: `35339dfeb77e5e315593cc66f6e297919f17aad1a6a1c6b5b9b596839176b145`.
- Varve binary SHA-256: `16888b054c6d36bf3ebf47862d707fcc31641ac491bea91df5d4975ca39872f6`.
- DuckDB binary SHA-256: `1dd0a1596505613a439dced4fb5f5800471badec82b2dc3b2c0efbd46fafbf6d` (`v2.0.0-alpha41533`, upstream commit `10de957379`).
- Cloud-probe binary SHA-256: `500d283da0af9d9a2463c8bbddbe8e567cce940103e0bdf46e2c52e3ee29b488`.

Sanitized machine-readable results are in `evidence/evaluation.json`. Detailed temporary reports contain synthetic data only, but the neighboring private credential files must never be published. The raw fixture has a one-hour policy and will expire; its named aggregate is independently retained. The second raw fixture was already cleared deliberately.

Stable DuckDB qualification, external audit, long-duration soak, realistic cardinality/capacity tests, power-loss/outage drills, the restart incident and broader availability behavior remain release gates. Single-node local fsync is not remote acknowledgement. Older binaries cannot read the upgraded S3 head envelope; do not roll back onto that namespace without a tested migration. The running evaluation, volume and bucket can continue to incur charges; no workspace spending limit was changed.
