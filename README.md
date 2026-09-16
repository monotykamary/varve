<p align="center">
  <img src="https://raw.githubusercontent.com/monotykamary/varve/main/media/cover.svg" alt="Varve — time settles into layers: hot rows, cached reads, Parquet, and asynchronous object-store recovery" width="1100" />
</p>

<h1 align="center">Varve</h1>
<p align="center"><strong>Time settles into layers.</strong><br/>A local-first, tiered time-series database in Rust. DuckDB SQL on top. Your storage underneath.</p>
<p align="center">
  <a href="https://github.com/monotykamary/varve/actions/workflows/ci.yml"><img src="https://github.com/monotykamary/varve/actions/workflows/ci.yml/badge.svg" alt="CI" /></a>
  <img src="https://img.shields.io/badge/Rust-2024_edition-d99972?style=flat-square&amp;logo=rust&amp;logoColor=white" alt="Rust, 2024 edition" />
  <img src="https://img.shields.io/badge/DuckDB-v2_alpha-eacb87?style=flat-square" alt="DuckDB v2 alpha" />
  <img src="https://img.shields.io/badge/status-experimental-80c9bd?style=flat-square" alt="Experimental" />
  <a href="https://www.npmjs.com/package/@monotykamary/varve"><img src="https://img.shields.io/npm/v/%40monotykamary%2Fvarve?style=flat-square&amp;color=80c9bd" alt="npm version" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-9fb4df?style=flat-square" alt="Apache-2.0 license" /></a>
</p>
<p align="center"><a href="#quick-start">Quick start</a> · <a href="#the-data-path">Architecture</a> · <a href="#sql-management">SQL</a> · <a href="#durability-contract">Durability</a> · <a href="#verification">Evidence</a> · <a href="#documentation">Docs</a></p>

> **Experimental v0.1. Real storage, explicit boundaries.**
>
> A working single-node database with Parquet, WAL recovery, object-store publication, lifecycle jobs and process-level tests. Not a production-readiness claim, PostgreSQL replacement, distributed database or arbitrary-SQL incremental-view engine.

## Why Varve?

Time-series data changes character as it ages. Fresh measurements want cheap appends. Recent history wants fast scans. Older history wants compact storage. Eventually, raw data can disappear while useful summaries remain.

Varve makes that lifecycle a first-class concern **without inheriting an entire PostgreSQL server**. Rust owns the write path, partitions, durability and policy. Arrow/Parquet owns the columnar representation. DuckDB owns SQL execution. The seams are ordinary modules and documented contracts—not a promise that storage correctness is simple.

*A varve is a sediment layer that records a cycle of time. The name fits.*

| Bring | Varve handles | Keep in mind |
| --- | --- | --- |
| Timestamped numeric measurements | Atomic batches, stable shards, event-time windows | Fixed append-only schema, not general relational tables |
| Hot and historical SQL | Pinned snapshots over memory and verified Parquet | DuckDB v2 alpha, bounded subprocess per query |
| Data that should age out | Compaction, archive, raw expiration and independent rollup retention | Explicit policies and capacity limits |
| An S3-compatible object store | Asynchronous WAL/checkpoint publication and integrity-checked restore | Local acknowledgment is **not** remote acknowledgment |

## What is implemented

- Append-only, named numeric time-series tables: signed UTC `timestamp_us`, `tenant`, `series`, finite `value`, and string `tags`.
- Atomic batches acknowledged after local WAL file and directory fsync. Request IDs make retries idempotent; conflicting payloads are rejected.
- Stable hash shards with event-time windows, explicit lateness, sorted Arrow/Parquet ZSTD segments, and checksummed manifests.
- Hot RAM data, bounded decoded RAM cache, local disk segments, bounded downloaded cold cache, and optional S3-compatible backing.
- SQL across hot and persisted data using the real DuckDB v2 engine. Conservative single-table/time-range planning prunes files before cold downloads; complex SQL falls back safely.
- Continuous count, sum, min, max, average and event-time OHLC rollups, plus named aggregate aliases with retained-raw backfill. Ties use committed sequence/ordinal. Raw retention need not destroy derived history.
- WAL-durable lifecycle controls and inspectable, pausable interval jobs through Rust and whitelisted SQL `CALL` operations.
- Scheduled flush, time-window compaction, archive/eviction, raw/rollup expiration, and reachability-based garbage collection.
- Asynchronous remote WAL/checkpoint publication with conditional ownership, integrity-checked restore, and coordinated remote GC/restore locks.
- Rust library, Rust and TypeScript network clients, JSON CLI, authenticated HTTP/WebSocket service, and opt-in loopback PostgreSQL-wire simple queries with SCRAM.
- Bounded Crossbeam ingestion with one batching writer and durable group commit; explicit atomic batches and pipelined single inserts share the same queue.

## The data path

```text
  measurements
       │
       ▼
  validate + admit ──► local WAL + fsync ──► hot rows + incremental rollups
                              │                         │
                              │                    checkpoint
                              │                         ▼
                              │                 sorted ZSTD Parquet
                              │                         │
                              └──── async publish ──────┤
                                                        ▼
                                            S3-compatible object store
                                            WAL + checkpoints + segments

  SQL ──► pinned snapshot ──► hot rows / bounded caches / local Parquet
           DuckDB v2            cold files verified before use
```

**Partition by identity, then time.** A stable hash of `(tenant, series)` chooses the shard; an event-time window chooses the partition. Files sort by tenant, series, timestamp and committed order. Shards are physical layout units—not distributed replicas. See the [architecture](docs/ARCHITECTURE.md).

**Cache is a read optimization, not a durability level.** RAM holds hot rows and a bounded decoded cache; a bounded disk cache serves downloaded cold files. CPU L1/L2/L3 caches remain hardware managed. Varve batches and sorts for locality rather than pretending to control them as storage tiers.

## Quick start

[![Deploy on Railway](https://railway.com/button.svg)](https://railway.com/deploy/varve)

The [verified template](https://github.com/monotykamary/railway-template-varve) provisions one service, a persistent volume and a dedicated bucket with generated credentials. It retains the experimental, single-owner durability boundaries.

Requirements: Rust/Cargo (tested with 1.98.1), a POSIX filesystem with atomic rename, directory fsync and advisory locking, and DuckDB v2 on `PATH`. The server crate is **`varve-storage`**; its binary and Rust library remain **`varve`**. Registry publication is recorded in the [release ledger](docs/CLIENT_INGEST_ACCEPTANCE.md).

```sh
git clone https://github.com/monotykamary/varve.git
cd varve

# If a compatible DuckDB v2 is not already installed:
scripts/install-duckdb.sh
export PATH="$PWD/.tools:$PATH"

# No bundled C++ engine build. Development profiles disable debug symbols/incremental artifacts.
cargo build --locked
./target/debug/varve --data ./varve-data init
./target/debug/varve --data ./varve-data create metrics
./target/debug/varve --data ./varve-data write examples/write.json
./target/debug/varve --data ./varve-data query 'SELECT series, count(*), avg(value) FROM metrics GROUP BY series'
./target/debug/varve --data ./varve-data query 'SELECT bucket_us, count, average, open, close FROM metrics__rollup'
./target/debug/varve --data ./varve-data checkpoint
```

The installer pins `v2.0.0-alpha41533` / commit `10de957379`, checks SHA-256 and extracts only the CLI. Supported pinned downloads: Linux x86_64/glibc and macOS arm64. It writes only `.tools/` and removes its download temporary files. DuckDB v2 is still an alpha dependency here; do not silently substitute v1.5.

### Background service

Stop other commands that have the database open before starting the service:

```sh
./target/debug/varve --data ./varve-data serve --port 8080
curl -s http://127.0.0.1:8080/v1/status
curl -s -H 'Content-Type: application/json' \
  --data '{"sql":"SELECT count(*) FROM metrics"}' \
  http://127.0.0.1:8080/v1/query
```

Loopback is the default. Public binding requires both `--allow-remote` and a strong `VARVE_API_TOKEN`; use TLS termination in front of it. This is one trusted operator, not authenticated multi-tenant SQL. One process owns a data directory. SIGINT/SIGTERM bound network draining and join accepted ingestion; a stalled native fsync can extend total shutdown. Abrupt termination recovers through the local WAL. `/ready` is a minimal nonblocking probe, and operator APIs/metrics require the configured token. See [HTTP controls](docs/CLI.md).

## Clients and persistent connections

Both SDKs keep one WebSocket connection open, correlate concurrent requests, bound pending work and preserve exact timestamps. They **never silently replay a mutation** after disconnect or timeout. HTTP/1.1 already supports keep-alive; WebSocket removes repeated HTTP envelopes and permits pipelining, not zero-round-trip writes.

| Client | Package | Guide |
| --- | --- | --- |
| Rust | `varve-client` | [API, TLS, errors and example](clients/rust/README.md) |
| TypeScript / Node 22+ / browsers | `@monotykamary/varve` | [API, bigint timestamps and browser origins](clients/typescript/README.md) |

Registry installation (publication receipts are recorded in the [release acceptance ledger](docs/CLIENT_INGEST_ACCEPTANCE.md)):

```sh
cargo install varve-storage --version 0.1.0 --locked # CLI; DuckDB remains external
cargo add varve-client@0.1.0                       # Rust network client
bun add @monotykamary/varve@0.1.0                  # TypeScript / Node / browser client
```

```ts
import { connect } from "@monotykamary/varve";

const token = process.env.VARVE_API_TOKEN;
if (!token) throw new Error("VARVE_API_TOKEN is required");
const db = await connect("wss://your-database.example/v1/ws", {
  token,
  maxPendingRequests: 32,
});
await db.createTable("metrics", {});
const row = {
  timestamp_us: BigInt(Date.now()) * 1000n,
  tenant: "demo", series: "cpu", value: 12.5, tags: { host: "one" },
};
await db.insert("metrics", row, "measurement-1");
await db.insertBatch("metrics", [row, { ...row, value: 17.5 }], "batch-1");
console.log(await db.query("SELECT count(*) FROM metrics"));
await db.close();
```

Use `insertBatch` / Rust `insert_batch` when you already have rows in hand. For independent single inserts, bounded concurrent calls let the server group commits; awaiting each call before submitting the next cannot exploit cross-request batching. Success still follows **local WAL fsync**, never just enqueueing.

[Manual CLI/HTTP batches and retry rules](docs/BATCHING.md) · [Queue/group-commit design](docs/INGESTION.md) · [WebSocket and PostgreSQL-wire setup](docs/TRANSPORTS.md)

Browsers require an explicit allowed origin. Tokens belong in the authentication frame, not the URL. The PostgreSQL-wire endpoint is disabled by default; use the transport guide for its intentionally restricted standard-client interface.

### Remote protection and archive

A filesystem store exercises the exact remote-publication protocol without cloud credentials:

```sh
./target/debug/varve --data ./varve-data --remote-dir ./varve-objects ship
./target/debug/varve --data ./restored-data --remote-dir ./varve-objects restore
./target/debug/varve --data ./restored-data --remote-dir ./varve-objects query 'SELECT count(*) FROM metrics'
```

`restored-data` must not exist. Restore fetches metadata and WAL; archived segments download on demand. Keep supplying the same remote-store flags when opening the restored database for queries or service. Local and filesystem-remote roots must be disjoint. Treat restore as ownership transfer, not creation of an active replica; the previous publisher is fenced. Use `--s3` instead of `--remote-dir` for the real S3 adapter; see [remote storage](docs/REMOTE.md).

Create tables with `--table-config examples/table.json` to select explicit retention/archive policies. Durations are microseconds, raw expiration removes rows strictly older than the cutoff, and a scan range is `[start_us, end_us)`. Partition shape remains immutable; lifecycle fields are mutable through the [durable control plane](docs/CONTROL.md). See [configuration defaults and semantics](docs/CONFIGURATION.md). The example write uses historical synthetic timestamps: do not combine it with wall-clock retention unless you intend it to expire.

## SQL management

```sql
CALL varve_create_continuous_aggregate('metrics_minute', 'metrics', 60000000);
SELECT * FROM metrics_minute;
SELECT * FROM varve_policies();
SELECT * FROM varve_jobs();
```

Run each statement separately through `query` or `/v1/query`. These are fixed numeric time-series aggregates, not arbitrary SQL incremental views. See [control-plane APIs and semantics](docs/CONTROL.md), [Railway evaluation procedures](docs/RAILWAY.md), and the [production-hardening ledger](docs/PRODUCTION_ACCEPTANCE.md).

## Durability contract

**An acknowledged local write is not necessarily in S3 yet.** A process crash can recover it from local WAL; permanent loss of the local disk can lose the unshipped tail. `status.unshipped_batches` reports the physical WAL-sequence gap to the last confirmed remote head—not a client-request/row count or a promised recovery-time bound. Multiple inserts can share one group sequence. Upload intervals alone cannot bound the gap during an outage.

The manifest atomically connects raw segments, view state, request receipts and replay position. Queries pin their files while checkpoints/compaction replace manifests. Remote heads reference only fully uploaded immutable dependencies. Remote GC preserves the current published checkpoint and WAL tail, and excludes restore with a fail-closed CAS lock. No automatic lock stealing is implemented.

## Verification

The [live evaluation record](docs/EVALUATION.md) covers 138 Rust tests, 14 Python tests, real S3 restore, a 100,000-row HTTPS workload and independent retention. The hosted example is experimental; the observed restart incident and production qualification gaps are documented, not hidden.

```sh
scripts/verify.sh
# Focused, self-checking mixed workload; uses small self-cleaning temporary data:
cargo run --locked --example workload -- 2000
# Requires explicit isolated S3 credentials/config; never runs by default:
VARVE_LIVE_S3_TEST=true cargo test --test remote_store live_s3_contract_uses_an_isolated_random_prefix -- --ignored --exact
```

Tests cover typed ingestion, corruption rejection, crash publication boundaries, kill/restart HTTP E2E, idempotent replay, out-of-order aggregates, expiration, SQL during compaction, remote outages, cold restore, cache/metadata admission and safe remote object access. See [verification results](docs/VERIFICATION.md), the [acceptance ledger](docs/ACCEPTANCE.md), and [due diligence](docs/DUE_DILIGENCE.md) for actual evidence and remaining risks. The verification harness builds its own executables; local tools, data and build artifacts are ignored by Git.

## Scope and trade-offs

- DuckDB runs in a bounded subprocess per query, with copied hot rows over stdin. No zero-copy integration or persistent worker-pool performance is claimed.
- Arbitrary SQL incremental views, updates/deletes/upserts, schema evolution, distributed transactions and automatic failover are not implemented. PostgreSQL-wire is a loopback-only, SCRAM-authenticated simple-query subset with text result columns—not pgwire TLS, prepared parameters, COPY or PostgreSQL SQL-dialect compatibility.
- Query workers disable disk spilling. A cold SQL snapshot must fit the configured downloaded-cache budget; use time predicates or raise the budget. Native scans process files one at a time with a bounded result budget.
- Admission limits are logical working-set estimates, not OS-enforced RSS/filesystem quotas. Metadata bytes are checked before acknowledgment, with conservative future-segment reservations.
- Request receipts are independent of raw expiration. Legacy IDs retain lifetime receipts; opted-in timed IDs have a bounded retry window and expired IDs are rejected, not reinserted. Receipt/group/metadata caps still refuse excess work rather than silently evict aggregate state. Plan cardinality, widths, retention and batch size; disk-backed aggregate metadata is not implemented.
- Native disk maintenance is serialized with commits. See due diligence for remote I/O isolation and remaining latency limitations.
- S3 tests remain explicitly opt-in. The dedicated Railway bucket has passed a scoped live transport-contract test; final deployment, restore and stress evidence is tracked separately in [the production acceptance ledger](docs/PRODUCTION_ACCEPTANCE.md).

## Small parts, explicit responsibilities

The Rust library is usable without the HTTP server. These are modules in one crate today—not separately versioned services or a distributed control plane.

| Boundary | Implementation |
| --- | --- |
| Typed rows, configuration and stable routing | [`model.rs`](src/model.rs) |
| Local write durability and replay frames | [`wal.rs`](src/wal.rs) |
| Admission, coherent snapshots and checkpoints | [`engine.rs`](src/engine.rs) |
| Sorted Arrow/Parquet storage | [`segment.rs`](src/segment.rs) |
| Conservative pruning and DuckDB execution | [`plan.rs`](src/plan.rs) · [`query.rs`](src/query.rs) |
| Object-store adapters and publication protocol | [`remote.rs`](src/remote.rs) · [`tier.rs`](src/tier.rs) |
| Lifecycle policy, durable controls and job runtime | [`policy.rs`](src/policy.rs) · [`control.rs`](src/control.rs) · [`job_runtime.rs`](src/job_runtime.rs) |
| Bounded ingestion and durable group commit | [`ingest.rs`](src/ingest.rs) · [`engine.rs`](src/engine.rs) |
| HTTP, WebSocket and PostgreSQL-wire service | [`service.rs`](src/service.rs) · [`transport.rs`](src/transport.rs) · [`pg_transport.rs`](src/pg_transport.rs) |
| Standalone network clients and JSON CLI | [`clients/`](clients) · [`main.rs`](src/main.rs) |

## Documentation

| Start here | Go deeper |
| --- | --- |
| [CLI and HTTP](docs/CLI.md) | [SQL execution and limits](docs/QUERY.md) |
| [Configuration](docs/CONFIGURATION.md) | [Continuous aggregates and scheduling](docs/CONTROL.md) |
| [Architecture](docs/ARCHITECTURE.md) | [Remote publication and restore](docs/REMOTE.md) |
| [Operations and recovery](docs/OPERATIONS.md) | [Railway deployment](docs/RAILWAY.md) |
| [Evaluation results](docs/EVALUATION.md) | [Source review and regressions](docs/FINAL_REVIEW.md) |
| [Production acceptance](docs/PRODUCTION_ACCEPTANCE.md) | [Remaining audit gaps](docs/AUDIT_RECONCILIATION.md) |
| [Client/group-commit release](docs/CLIENT_INGEST_ACCEPTANCE.md) | [Client release review and evidence](docs/CLIENT_RELEASE_REVIEW.md) |
| [Publication checks](docs/PUBLICATION.md) | [Design references](docs/REFERENCES.md) |

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md), then run `scripts/verify.sh`. Improvements to correctness, recovery behavior, capacity evidence and replaceable boundaries are especially welcome. Keep durability assertions strong and distinguish measured behavior from guarantees.

No large generated fixtures, container images or bundled DuckDB source trees are required. See [third-party DuckDB licensing](licenses/duckdb.txt).

<p align="center"><strong>Own the write path. Let time shape the storage.</strong><br/><a href="LICENSE">Apache-2.0</a></p>
