# Boundary redesign acceptance ledger

Status: **the frozen `c064a7f0…` candidate passed local and Linux/runtime qualification, followed by two correct Railway pilots—but did not beat Timescale.** Both benchmark backends and the driver were subsequently removed and independently reconciled; volumes/data and the original app were preserved. A later resident-hit setup fix is locally qualified only, not included in those cloud measurements. The third repeat and sustained trial were deliberately not run after the consistent negative pilots.

The goal is a cheap, bounded ingestion/serving path whose durable publication and physical lifecycle do not require rebuilding logical state. Strict `local_fsync` acknowledgments remain the tested success contract. Admission is not acknowledgement; no buffered success is introduced. Making rows durable does not imply evicting them from RAM or columnarizing them.

## Acceptance gates

| ID | Marker | Final local evidence | Status |
| --- | --- | --- | --- |
| B1 | Append WAL I/O does not hold the reader state mutex | Ten commit-boundary tests plus the separate multitable test; real file/directory pauses, direct/group errors, committed raw/derived/receipt snapshots, atomic installation, rename/cleanup accounting interleavings and nonblocking poisoned-gate readiness | passed |
| B2 | Private preparation and safe publication | Touched undo/forward values rather than a full-state append clone; direct/group/control/checkpoint concurrency, corruption/gaps, ambiguous-failure fencing, retry and reopen checks | passed |
| B3 | Explicit durable batching boundary | Publicly exported/documented `Ingestor::flush` and `IngestFlush`; bounded FIFO marker, seven ingestion and three flush integration tests, partial-group hot residency/reopen and explicit terminal success/failure counts | passed |
| B4 | Residency follows logical data, not incidental scope | Real DuckDB scope/checkpoint/append/lineage oracles; unknown-charge and metadata-capacity fallback repairs; independently accounted inactive/historical identities; verified file/decoded identity reuse without trusting arbitrary names or hot Arc remaps | passed |
| B5 | Typed query input | Real typed Arrow/Parquet input, exact finite-float/timestamp/tag tests, measured materialization and strict input limits. Input is copied into disposable DuckDB tables, not zero-copy | passed |
| B6 | Lifecycle, isolation and derived-state coherence | Maintenance-enabled probes and broader lifecycle, snapshot, pins/files/GC, cancellation/reset and budget regressions. No eager tier cascade | passed locally |
| B7 | Short reproducible local feedback | Identical rustfmt-normalized probe; fresh matched release profiles and counterbalanced repeats; exact row/aggregate/receipt/reopen oracles and zero failed/rejected/pending work after drain | passed; measurements below |
| B8 | Independent review and regression qualification | 486 workspace Rust tests, one opt-in SDK real-service test, and four nested crash-worker executions passed (491 executions total), with strict default/fault Clippy, formatting, default-feature CLI smoke and 21 driver tests. Independent closure of WAL race, fallback/ledger findings and verified-segment delta | passed within reviewed scope |
| B9 | New Railway comparison only after B1–B8 | Immutable source/stage manifests, Linux tests and actual release smoke, fresh roots, exact owned IDs, resource/durability/runtime checks, bounded matched trials, recovery and cleanup | two pilots, release crash smoke and cleanup verified; no overall win; third/sustained trials not run |

The local qualification input-manifest digest is `c064a7f0b0f35309501a1356564469f33f4c21efce653c354145f95be80f1d60`. Main's `LOCAL_GATE.json` binds qualification, the matched comparison, binary hashes, public API checks and accepted review evidence. The Linux build exposed a test-only parent/child stdin identity assumption; all 15 query tests and both strict target lint modes were requalified after its correction. Other runtime/test inputs are byte-identical and inherited explicitly from the preserved base receipt, not claimed as rerun. CLI/SDK evidence remains bound to the preserved qualified binary rather than a subsequent Cargo test artifact. Reviews are bounded source findings/closures, not blanket repository or production certification.

The sole ignored Rust test is `live_s3_contract_uses_an_isolated_random_prefix`, which requires explicit live-S3 configuration. It was not executed or claimed passed. These local gates do not establish live-S3 behavior.

## Matched local measurements

These are **library diagnostics, not network throughput or a Timescale comparison**. Each request contains 1,000 rows, with four producers. Depth is the maximum in-flight requests per producer. Configurations match exactly between before and after; throughput includes final drain.

| Profile | Depth | Before rows/s | After rows/s | Change |
| --- | ---: | ---: | ---: | ---: |
| 100k mixed, before then after | 1 | 128,982 | 143,187 | +11.0% |
| 100k mixed repeat, after then before | 1 | 115,845 | 130,037 | +12.3% |
| 100k write-only, after then before | 8 | 236,444 | 262,645 | +11.1% |
| 300k mixed, before then after | 8 | 189,866 | 181,039 | −4.6% |
| 300k mixed repeat, after then before | 8 | 176,063 | 187,669 | +6.6% |

Rows imported into retained query tables across the mixed workload and subsequent query matrix fell **77–82%**. The larger mixed profile is roughly flat, not a demonstrated throughput win. Mixed runs contain only **2–4 timed read samples**, so their p95 values do not establish a latency improvement or SLA. Local machine activity and run-order effects remain limitations.

An initial comparison was correctly rejected because the original 100k mixed baseline used depth 1 while the first candidate run used depth 8. Those receipts were preserved; the table uses fresh, strictly matched pairs instead. Probe formatting differences were normalized without changing the preserved baseline source or binary.

The last qualified **sustained** Railway pair remains **67.1k versus 86.6k rows/s**, Varve versus Timescale, with write p95 **72.4 versus 34.8 ms**. The newer short pilots below are a different workload duration, not a replacement sustained result.

## Railway pilots, 2026-09-18

Both backends used two CPUs, 2 GB memory limits and local durable acknowledgments. Four writers sent 1,000-row batches: 250,000 initial rows plus 50,000 mixed rows at 5,000 rows/s for ten seconds. Each backend ended with 300,000 verified rows. Both runs passed query, tier-transition, aggregate-freshness and final count/sum oracles, with zero dropped or failed/ambiguous rows and no automatic write retries.

| Run / initial order | Varve ingest rows/s | Timescale ingest rows/s | Varve mixed write p95 | Timescale mixed write p95 |
| --- | ---: | ---: | ---: | ---: |
| p1 / Varve first | 60,937 | 106,340 | 65.99 ms | 28.71 ms |
| p2 / Timescale first | 60,382 | 111,562 | 79.49 ms | 30.41 ms |

These are two short pilots, not a capacity frontier or production latency guarantee. Mixed-write percentiles have 50 samples per backend/run; each query-stage cell has 20, and mixed concurrent reads only eight. The actual Linux release durable-write → SIGKILL → reopen → idempotent-retry smoke passed on a separate qualification root. It tests process death, not power loss or HA.

Original reports, logs, outcomes, postload metrics and exact-owned cleanup receipts are preserved with hashes in [the evidence directory](evidence/boundary-rpdx5i/README.md). The first p1 SSH attempt was rejected at the initial UID gate before database admission; p2 initially hit the local `/tmp` symlink-path gate before SSH. Both were reconciled before the one actual admitted run; neither retried an ambiguous write.

## Resident-hit follow-up: local only

The pilot showed working reuse—p1 had two full loads, four delta loads, 235 hits and zero invalidations—but `QueryBuild` still accumulated 17.713 seconds across 241 calls. Source inspection found that even unchanged hits deleted/rebuilt selected-ID bindings through a temporary JSON scanner transaction.

The follow-up records installed selection in the existing, charged `LoadedBatch` metadata. It always builds and validates the complete install plan. Only unchanged schema, raw identities, relations **and selected membership** skip adapter setup; user SQL and acknowledged rollback still execute. Frontier/lineage metadata and accounting still refresh. Selection changes and all prior failure/cleanup rules remain authoritative. This is not SQL-result caching, native embedding or weaker durability.

A cfg(test) call counter on the real worker proves three exchanges on installation (setup/query/rollback), two on an unchanged hit, and three again when switching between already-loaded partial selections. The 37 targeted unit tests and 40 query integration tests passed, followed by the full fault-injection workspace suite, strict Clippy in both feature modes, formatting and whitespace checks. The workspace log contains 490 successful test results including nested child executions; one opt-in live-S3 test remains intentionally ignored. These are not 490 distinct top-level tests.

The unchanged release diagnostic ran before→after→after→before, with 1,024 rows and 20 verified samples for each of six hot/Parquet query cases. Case medians were **135–162 ms before versus 2.8–6.5 ms after**. Both larger baseline fixtures were rejected by the existing high-cardinality derived-memory admission guard; those failures were not hidden or turned into measurements. No budget was weakened. This small fixed-cost diagnostic does not establish large-database speed, ingestion improvement or a Timescale win. The new fix still requires fresh Linux/cloud qualification; previous stage bindings do not authorize changed sources.

## Measurement and scope rules

Separate CPU/handoff diagnostics from end-to-end durable database throughput. Report batch size, bytes, in-flight work and latency with rows/sec. Include failed/rejected/ambiguous work and final drain; never hide backlog. Keep maintenance enabled in lifecycle probes. Compare identical before/after configurations locally and matched offered load on Railway. A four-producer depth-one profile cannot substantiate arbitrary 50k-row commit batches. Nested timers overlap and must not be summed into a fictional total.

Main owns scheduling and Railway activation. The initial dirty tree remains preserved in the private baseline; do not reset or replace it. No package publication or GitHub write is part of this gate. Cloud mutations require a separate source-bound, exact-owned campaign intent after local acceptance; load remains blocked until Linux/runtime/release recovery witnesses pass.

This work does not establish distributed/HA, power-loss or live-S3 guarantees. Local fsync is distinct from remote archival. Native zero-copy DuckDB integration and arbitrary-SQL incremental view maintenance are not claimed. The immutable WAL frame's per-epoch publication cost remains real; it is not hidden behind weaker acknowledgments.
