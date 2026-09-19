# Inside-out rebuild

Status: architectural reset and integrated qualification in progress, 2026-09-18. Replacement components now reach the real engine and server; the gate table distinguishes exercised paths from remaining qualification. All compilation, executable tests, profiling and load generation for this campaign run on Railway. No local Cargo, Docker builds or benchmark fixtures.

## Original requirement

The original request was to make ordinary single inserts efficient through the Crossbeam/Disruptor approach: "ingest into a ring buffer to flush and saturate localized writes". Later clarifications emphasized hot-path accessibility, batching/checkpoints, locality/residency and no dropped data. A bounded channel in front of the old shared-state engine is not the end state.

Crossbeam supplies transport primitives, not persistence or broadcast dependencies. Cloned receivers distribute messages; they do not deliver every event to WAL, views and storage. A ring never overwrites outstanding work. Neither batching nor a queue proves an order-of-magnitude durable-throughput advantage.

## Replacement data plane

```text
persistent clients / batch API
    -> bounded credits + owned validated input
    -> sequenced microbatches
    -> partition-local preparation owners
           |-> sequential framed journal -> durable frontier D
           |-> private raw + derived preparation -> prepared frontier P
    -> coherent immutable publication through min(D, P)
    -> visible frontier V -> durable receipts

published immutable batches
    |-> native DuckDB snapshot scanners
    |-> checkpoint / Parquet / compaction workers
    |-> sealed journal segments + dependencies -> S3 publication
```

This is a dependency graph, not a thread per box. Begin with one journal/sequencing owner and bounded partition owners. Cross-partition requests require one atomic commit descriptor; completing one partition cannot acknowledge the request.

## Contracts

1. Admission is not success. A successful receipt remains durable, idempotent and query-visible with required aggregate updates. Cancellation before admission consumes no slot; dropping a receipt receiver after admission does not discard its write. Ambiguous I/O fences the writer.
2. Mutable commit state has an explicit owner. Readers pin immutable versions. Preparation returns private validated deltas instead of mutating and undoing committed maps. Publication references immutable partition versions rather than copying the entire database.
3. Frontiers are contiguous prefixes, not maximum observed sequence numbers: sequenced `S`, prepared `P`, locally synced `D`, visible `V <= min(D,P)`, checkpointed `C <= V`, remotely confirmed `R`. Rejected/cancelled slots require explicit terminal handling so they cannot strand progress.
4. Local completion requires durability and visibility. Any future object-store-confirmed mode also requires remote coverage. Local fsync is not S3 durability. No periodic upload interval bounds remote data loss during an outage.
5. Credits cover queued, preparing, in-flight, resident and pinned data. Ownership transfers charges; it never makes live memory free. Required consumers gate reclamation until work is consumed or transferred into separately charged storage. Deadlines and overload remain visible; no shedding or unsafe retry.
6. Shutdown closes admission, drains admitted work, completes terminal outcomes and joins owned workers. Native fsync can still extend shutdown; no false hard-timeout guarantee.

## Architectural replacements

### Journal

Replace per-group temporary-file/fsync/rename/directory-fsync publication with a versioned append-oriented segmented journal. Coalesce writes and sync by bounded row/byte/age limits. Creation, rotation and sealing retain namespace durability barriers; ordinary appends do not create a namespace transaction per group.

Define framing, checksums, commit markers, contiguous recovery, incomplete-tail handling, fencing, bounded replay and reclamation before enabling the format. Complete corrupt frames and interior gaps fail closed. Do not claim incomplete-tail handling can diagnose arbitrary post-acknowledgment hardware corruption. Sealed immutable segments are the S3 upload unit; publish remote heads only after dependencies upload. Old databases require an explicit migration/read-compatibility contract.

### Hot storage and SQL

Keep immutable typed batches resident across durable checkpoints when budgets allow. Durability, columnarization, eviction, archival and expiration are distinct transitions. Partition-local builders prepare data; published batches serve readers.

Replace CLI workers and copied shadow relations with native DuckDB scanners over pinned batches and verified Parquet. Verify the pinned v2 library/API identity first. Buffers and file pins outlive scans, interruption and teardown. Bound and measure unavoidable vector conversion; native is not automatically zero-copy. No per-query full relation materialization or hot-data staging file is the normal serving path. DuckDB executes SQL; it does not become Varve's primary storage engine.

Native execution changes failure isolation and resource containment. Interruption, joining, allocator limits and trusted-SQL boundaries require tests; embedding alone does not establish a process RSS or security guarantee.

### Views and lifecycle

View operators expose initialization, batch application, versioned read state, checkpoint and restore. Initial operators preserve current time-series aggregates, late arrivals and exact first/last tie rules. General SQL incremental maintenance needs explicitly supported forms and separate acceptance tests; renaming fixed rollups does not implement it.

Partition ownership follows identity and event time. Physical partitions are not automatically distributed writers. Maintenance prepares immutable replacements off-owner, then submits version-checked publication. Slow S3 upload reads sealed disk segments rather than holding hot-ring slots indefinitely; bounded unshipped disk eventually backpressures ingestion.

Reusable boundaries: admission/sequencing, journal, partition state/snapshots, view operators, lifecycle/object storage and SQL scanners. Separate crates only for real independently testable contracts, not cosmetic file movement.

## Acceptance ledger

Preserve dirty work, existing client/wire contracts and independent recovery/data oracles. Do not overwrite existing deployments, volumes, bucket namespaces, historical evidence or old campaign approvals.

| Gate | Required evidence | State |
| --- | --- | --- |
| R0 | Original intent recovered; isolated cloud-only qualification workflow | passed for this checkpoint: pinned Rust 1.98.1, source-only transfer, all executable work on Railway |
| R1 | Owner/sequencer path wired end to end; credits, barriers, coherent publication, cancellation and shutdown | transfer/pressure and owned-handoff tests passed on Railway; cleanup-time defects were repaired and independently reviewed. reviewed repair is deployed with passing short smokes/exact checks; partition-owner implementation and full-load qualification remain open |
| R2 | Segmented journal wired to commit/recovery; crash/corruption matrix, format and migration contract | commit/recovery, reclamation, pressure retry and explicit migration exercised; missing-manifest authority repair passed all 9 journal-engine tests, including unchanged artifacts and exact restored replay |
| R3 | Native scanners wired to public SQL; exact hot/cold/view results, cancellation/pins/bounds, no hot-input staging | native dispatch/parity, cancellation, multi-chunk lifetime and archive/restore exercised; all 12 focused native tests passed after applying thread/memory limits before open; the corrected source is in the verified smoke deployment; production session reuse and sustained performance remain open |
| R4 | Versioned view/lifecycle interfaces; late data, retention and snapshots across checkpoints/compaction | native SQL + incremental view + checkpoint/archive/restore path passed; broader lifecycle/concurrency matrix pending |
| R5 | Persistent single-row and batched clients; no missing work; exact raw/derived/restart/idempotency oracles on Railway | real HTTP SIGKILL replay/deduplication, live SDK/S3 checks and grouped oracles passed; exact 1,074-row/aggregate-group comparison passed before and after both database restarts; larger persistent-client qualification remains open |
| R6 | Matched-resource/durability Timescale comparison: ingestion, fresh views, useful analytics, maintenance and intended-arrival tails | not passed: new low-load smokes conserve data and meet scheduling, but single-row ingestion is 292 vs 700 rows/s; batch-128 is 34,286 vs 21,521 rows/s only in a short run; SQL latency and sustained capacity remain open |
| R7 | Coherent source review, binary identity, migration/rollback; remove superseded paths only after acceptance | pending |

Ordinary single-row traffic is mandatory. Batch-1000-only results cannot substantiate the original request. Report admission, durable completion and fresh visibility separately. Count backlog/drain and checkpoint cycles; measure CPU, peak memory and disk amplification. Failed or ambiguous runs remain failed evidence. Native integration and segmented WAL are hypotheses, not guaranteed Timescale wins.

## First component checkpoint

[Source-bound evidence and cloud resume instructions](evidence/inside-out-components-20260918/README.md) preserve the raw failures, final recipe output, exact source and scope limitations in under 1 MiB.

New public modules `flow` and `journal` replace two architectural mechanisms in isolation. The executable `examples/flow_journal_probe.rs` connects them to private preparation and immutable publication. It is intentionally not relabeled as a server, a time-series implementation, or a throughput benchmark.

The corrected Railway proof offered 4,096 individual events from four producers with 16 outstanding requests each. All 4,096 completed after durable visibility and reopened with exact independent identities. They formed 128 journal groups across six segments, with six namespace barriers rather than one per request/group. The encoded journal was 82,624 bytes; final ring credits were zero. Group counts are workload/scheduling observations, not a sustained-rate result.

Review found three real flow issues: stale peer-frontier inspection could miss reclamation, an ineligible blocking waiter could swallow another producer's wake, and coalescing could acquire more work after fencing. Two deterministic actual regressions failed before correction. The independent-completion memory model witnesses the old weak-memory counterexample and checks the corrected lock-before-inspection protocol with preemption bound two. It is not a model of the full queue or storage stack. Corrected real-thread tests, component proof, strict Clippy and formatting passed remotely. No correctness certificate or production readiness is inferred.

The prior unfinished rollup test also passed remotely. Of 52 baseline query tests, one initially failed because DuckDB was missing from PATH; that exact case passed after the environment correction. All nine baseline ingestion integration tests passed. Historical failure logs remain evidence, not silently rewritten successes.

At that first component checkpoint, engine wiring, journal-backed state, native SQL and lifecycle/object publication remained unimplemented. The integration checkpoint below supersedes those implementation gaps without claiming the remaining load/production gates have passed. The existing deployed app has not been migrated.

## Integration checkpoint

The real server Ingestor now uses the bounded ownership pipeline. `Database` selects a persisted segmented-journal format, commits/replays raw rows, aggregates and receipts, reclaims checkpoint-covered segments and refuses unsafe legacy migration. Remote publication carries verified immutable sealed journal bytes under the existing head/CAS protocol.

`duckdb_library` activates the pinned in-process Rust adapter. Native queries share captured immutable batches, borrow rollup/catalog inputs with Rust lifetimes, and keep file pins through joined cancellation and private environment/database teardown. No CLI fallback or hot-input serialization is used in the native-only end-to-end tests. `/v1/status` reports the journal format and native library/header/version identity. HTTP timeout/drop signals query cancellation; admitted writes retain their separate finish-and-deduplicate contract.

Observed Railway checks include real native primitive/temporal/nested/JSON output parity, exact scalar decimal strings, nested numeric compatibility, non-finite JSON rejection, private allowlist isolation, multi-chunk sorted strings, output limits, deadlines and a deterministic pre-activation interrupt race. Full-engine checks cover incremental views across checkpoint/archive, mixed cold/hot SQL, filesystem-object restore, active cancellation pin release, and HTTP SIGKILL/restart receipt replay. Filesystem-object tests are not live S3 conformance, and sandbox tests are not a matched-resource Timescale result.

`config/railway-rebuilt.json` and the image’s `/app/config-rebuilt.json` select the integrated backend for isolated qualification. The default deployed configuration remains unchanged: do not silently migrate existing volumes. Full regression, coherent review, release identity and matched-resource performance gates remain open.

## Qualification review checkpoint

[Integrated evidence](evidence/inside-out-integrated-20260918/README.md) separates deployed-image results from later source repairs. The two database services have observed matching 2-CPU / approximately 2-GB cgroup ceilings. Live S3 and both SDK paths were exercised without using local compilation or fixtures.

Review identified an authority bug: missing `manifest.bin` could initialize over journal history. The repair now refuses journal artifacts independently of the process flag and before publishing a replacement UUID/root. All nine journal-engine tests pass, including active/sealed acknowledged history, malformed entries, symlinks, unchanged bytes and exact replay after restoring the original manifest.

Native database construction now receives owned `threads` and `memory_limit` options through the pinned C ABI, rather than setting them after open. A direct pre-configuration settings test and the native parity/cancellation/lifetime suite pass. These are source-level corrections, not evidence of a deployed performance gain.

The ordinary-insert comparator now gives Timescale one atomic autocommit statement for receipt plus event, not separate BEGIN/COPY/COMMIT round trips for a single row. Multi-row batches retain COPY. Earlier single-row COPY timings are not a qualified win. The earlier corrected `ior6_smoke_03` diagnostic remains `overloaded`: no missing/pending/ambiguous rows were observed, but its intended-arrival scheduling gate failed. Later source-bound smokes are recorded separately below; this historical verdict is unchanged.

A source-bound supplemental verifier compares every raw identity/value/tag/multiplicity and every supported minute count/sum/min/max group. All 1,074 rows and groups matched before and after restarting both owned databases. It preserves the overloaded verdict and does not claim first/last tie equivalence, sustained throughput, crash-under-load qualification or production readiness.

Ownership-backed raw reservations now cover transferred inputs, immutable hot/cache allocations, captured/retired owners and working copies; 13 focused ownership tests and related native/CLI/lifecycle regressions passed remotely. The frozen ownership audit found no actionable accounting defect, not a production guarantee. Native tests were rerun on the later final source.

Private touched-key preparation now replaces production mutate/undo/detach. Tests compare unchanged committed images during preparation, exact mixed duplicate/conflict/floor outcomes, accepted ordinals, discard release and independent replay. The final overlay source passed 92 engine/raw/boundary tests, 12 native tests, both strict Clippy modes and formatting. The 97 selected integration tests passed before the final test-only annotation. Independent review found one inherited early-pressure-checkpoint durable-retry classification defect; its repair and targeted regression are part of the next owned-epoch gate. The old undo helper remains only as a test oracle.

The next source checkpoint adds a real `Send + 'static` `PreparedEpoch`, an owned exclusive publication lease, and a consuming `DurableEpoch` obtained only after sync. Four ingestion stages now separate static validation, private epoch preparation, durability plus atomic installation, and terminal completion. Uninstalled durable-token drop and caught publication panic fence the writer. Ring frontiers count contiguous terminal slots, not physical WAL maxima; no separate public durable-frontier metric is claimed. The final source passed 98 engine/raw/boundary, 12 flow, 6 ingestion-unit, 12 native and 119 selected integration tests, both strict Clippy modes and formatting. Both the early-checkpoint classification defect and an adjacent successful-checkpoint receipt-pruning defect were reproduced and repaired; see the source-bound [owned-epoch evidence](evidence/inside-out-integrated-20260918/owned-epoch/acceptance.md).

Transferable raw input credit, typed pressure waiting, incremental scan construction and bounded native scanner scratch now have a separate qualified source checkpoint: manifest `5244039ae491f1ccd3481b630967b7127435572cf35a775e13e21ad26b5b33e3`. Final-source checks passed 107 engine/raw/boundary, 12 flow, 10 ingestion-unit, 19 native and 121 selected integration tests, both strict Clippy modes and formatting. Unchanged public regression assertions fail on the earlier source and pass on this source for the empty-scan over-reservation and admitted-burst second-copy rejection. Retained capacities and actual callback-owner instances remain charged; ingestion and native scratch do not borrow dedicated maintenance credit.

The later source-bound follow-up passed 109 engine/raw/boundary, 12 flow, 11 ingestion-unit, 19 native and 121 selected integration tests, both strict Clippy modes and formatting. It adds post-sync failure/completion coverage and observable gate fencing; see [follow-up acceptance](evidence/inside-out-integrated-20260918/raw-pressure/followup/acceptance.md). Independent review then found native owner and encoded-frame credit-release ordering defects not exercised by those end-state checks. Those defects were subsequently repaired with cleanup-time error/panic regressions: 282 tests, both strict Clippy modes and formatting passed on source manifest `13e9198e00e6d11a26c89def8fd775a2a326236f7bd844cfdef288883e4441c0`. Independent review closed the findings; Root integrated only the ten owned paths and verified all 128 source hashes. See [repair evidence](evidence/inside-out-integrated-20260918/raw-safety/README.md) and [review closeout](evidence/inside-out-integrated-20260918/raw-safety/review.md). Runtime/deployment qualification remains separate.

The reviewed repair was deployed as `cb0fca35-a170-4192-a4ba-d43b41a75443`; all 128 qualified source paths match its runtime manifest. [Runtime evidence](evidence/inside-out-integrated-20260918/raw-safety-runtime/README.md) records matched-limit short smokes, all-row/view verification for both fresh datasets, and complete verification of the previous dataset after upgrade. Single-row throughput and SQL latency still trail Timescale; the short batch-128 win is not sustained qualification.

Preparation still holds global publication authority; independent partition owners are being implemented against the reviewed repair baseline. Native database reuse has only a bounded C feasibility probe, not a production implementation. Measured WAL-path time and a disposable-file sync diagnostic motivate a separate recovery-safe layout design, not a durability-barrier shortcut. Effective sustained-profile headroom, SDK/live-S3 requalification, native reuse and sustained matched-resource comparison remain open.

## Cloud ownership and resource limits

Campaign: `varve-inside-out-20260918`.
Project: `8caffa15-0158-4822-a6c2-cb405bddc62d` (`varve-evaluation`).
Environment: `5ec35c82-c1ea-4aa4-b7a5-b89e4c4b9ed1`.
Initial sandbox: `d61734d7-61eb-4658-aa85-bae3a52e6373`, 15-minute idle timeout, no private network, service variables or production credentials. This was qualification compute, not a matched-resource benchmark backend. It was explicitly destroyed after the first component checkpoint, and the sandbox list was verified empty. Exactly one reusable remote build checkpoint remains: `varve-inside-out-20260918-d61734d7`; see the evidence README for scoped resume instructions.

Only lightweight source operations run locally. Stream source-only archives, excluding `.git`, `target`, dependency trees, credentials, database data and old evidence. Bound remote Cargo jobs, fixture sizes and command timeouts. Retain one build cache and compact source-bound results. Destroy owned sandboxes at task end; idle expiry is a backstop, not proof of cleanup. Excluded production services remain untouched. Only the explicitly owned benchmark services are deployment/comparison targets; their exact deployment identities are recorded in the runtime evidence.
