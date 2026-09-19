# Durable group commit and bounded ingestion

## Library API

Reexports from `varve`:

```rust,ignore
pub struct WriteRequest {
    pub table: String,
    pub request_id: String,
    pub rows: Vec<Row>,
    pub now_us: i64,
}
Database::write_group(Vec<WriteRequest>) -> Vec<anyhow::Result<WriteReceipt>>;
Ingestor::new(Database, IngestConfig) -> anyhow::Result<Ingestor>;
Ingestor::submit(&self, WriteRequest)
    -> anyhow::Result<tokio::sync::oneshot::Receiver<anyhow::Result<WriteReceipt>>>;
async Ingestor::submit_wait(&self, WriteRequest)
    -> anyhow::Result<tokio::sync::oneshot::Receiver<anyhow::Result<WriteReceipt>>>;
Ingestor::flush(&self)
    -> anyhow::Result<tokio::sync::oneshot::Receiver<anyhow::Result<IngestFlush>>>;
Ingestor::shutdown(&self) -> anyhow::Result<()>;
Ingestor::stats(&self) -> IngestStats;
Ingestor::traces(&self) -> IngestTraceSnapshot;
```

`Ingestor` is cloneable. `submit` is nonblocking with respect to queue capacity and database I/O; it validates the owned request before briefly locking admission/accounting. The immutable admission proof and byte charge travel through the queue and writer, instead of repeating static validation at each boundary. An immediate error guarantees **not enqueued**. A receiver is only an admission ticket. Only its successful completion acknowledges `local_fsync`. A dropped receiver does not cancel admitted work. Shutdown closes admission across all clones, drains, and joins; concurrent shutdown callers also wait for drain. Last-owner drop does the same best-effort drain. Shutdown/drop are blocking: async applications should explicitly call shutdown through `spawn_blocking` before their runtime exits.

`submit_wait` is the async backpressure counterpart. It validates once, keeps the input owned by the caller's future, and waits for bounded queue/byte credit rather than rejecting transient fullness. It does not create an internal unbounded queue or reserve bytes it cannot admit. An input larger than the entire byte/group budget errors immediately; shutdown wakes waiters with a not-admitted error. Cancellation before admission cannot enqueue; cancellation after receiving the receipt channel does not cancel the write. Bound caller futures independently: their retained inputs are outside `pending_bytes`. HTTP/WebSocket writes use this method inside the existing bounded request slots and request deadlines; connection/request-slot limits and invalid writes can still be explicitly rejected. A finite-capacity system cannot promise unlimited offered work.

Waiting is observable as `admission_waits`, current/peak `waiting_requests`, `admission_wait_ns` and `waits_without_admission` (cancelled/closed waiting futures). `rejected` counts terminal admission errors, not each internal capacity check. `/metrics` exports corresponding `varve_ingest_admission_waits_total`, `varve_ingest_waiting_requests`, `varve_ingest_peak_waiting_requests`, `varve_ingest_admission_wait_seconds_total`, and `varve_ingest_waits_without_admission_total`. Dropped receipt receivers mean the caller abandoned its completion channel, **not** that admitted rows were discarded.

`flush` is a FIFO barrier through the same bounded queue. It forces an incomplete pending group to publish without waiting for the remaining batch deadline. Completion means preceding accepted requests have terminal outcomes; `IngestFlush { sequence, completed, succeeded, failed }` reports cumulative counters since coordinator startup. Failed requests are not declared committed, so inspect `failed` and individual receipts. `sequence` covers preceding successful requests and may also include other database writers. A full/closed queue rejects the barrier without enqueuing it; dropping its receiver does not cancel work. Flush does **not** checkpoint, columnarize, evict hot rows, upload to S3, or alter the acknowledgment contract. Async callers await the receiver; shutdown remains blocking. Public re-export and behavior are exercised by `tests/ingest_flush.rs`.

`IngestConfig` defaults:

| Field | Default |
| --- | --- |
| `queue_capacity: usize` | 1024 |
| `max_pending_bytes: usize` | 16 MiB |
| `max_group_requests: usize` | 128 |
| `max_group_rows: usize` | 10,000 |
| `max_group_bytes: usize` | 4 MiB |
| `max_delay: Duration` | 2 ms |
| `trace_capacity: usize` | 0 (disabled) |

Admission uses a bounded multi-producer Flow ring with four dependency-gated consumers: static validation, private epoch preparation, durable publication plus atomic installation, and terminal completion. Slot and byte credits remain held until every required consumer finishes; release precedes waking completion receivers. A group head owns the prepared epoch and its member slots retain their credit without duplicate payload ownership. Pending admitted slots, including drain barriers, never exceed `queue_capacity`; there is no extra carry queue. Group count, rows, conservative bytes and oldest-admission deadline bound coalescing. An expired deadline stops waiting but may still coalesce already-ready backlog. Blocking notifications avoid idle spinning. Queue capacity is limited to one million slots, physical group count to 1024, delay to at most 60 seconds. Zero delay is supported. Ring frontiers are contiguous terminal-slot prefixes, not physical WAL sequence maxima.

Group row/byte limits are clamped to engine batch/hot/WAL bounds at construction. The pending-byte charge includes conservative escaped-JSON space, row/vector allocations and retained string capacities. It is intentionally larger than typical encoded JSON and is **not an RSS or OS disk quota**. Pending credit and raw allocation credit are distinct bounds. Engine raw/working, metadata, query and disk budgets cover their documented scopes; caller-owned and transport buffers require independent bounds. `submit_wait` quota-pressure behavior is being strengthened as tracked in [INSIDE_OUT_REBUILD.md](INSIDE_OUT_REBUILD.md); the earlier allocation-conservation tests alone do not establish progress under every profile.

`IngestStats` is a fixed-size serializable snapshot: saturating submitted/rejected/completed/succeeded/failed/dropped-receiver/group counters, current and peak pending requests/bytes, and closed state. `groups` counts worker flushes, not physical frames: an all-retry/invalid flush writes no frame. A failed completion can mean ambiguous publication; retry the same request ID rather than assuming no write occurred.

## Opt-in causal diagnostics

`trace_capacity` retains the last N coordinator groups; default zero does not collect traces. The service reads `VARVE_INGEST_TRACE_CAPACITY`. Capacity is at most 1024 groups and `capacity * max_group_requests` at most 65,536, bounding stored sequence identities. `Ingestor::traces()` and authenticated `GET /v1/diagnostics/ingest` return capacity, eviction count and group records. Collection does not change batching or durability. Treat tracing as an explicit diagnostic configuration, not an invisible benchmark modification.

Records include group ID, request/row counts, successful receipt sequences, failures/duplicates, oldest/newest admission-to-execution wait, service time and phase observations from the preparation and publication workers. Queue time includes group formation; validation before admission and HTTP transport are not included. Sequence IDs join fresh HTTP receipts to the group; duplicate retries can name older sequences. Nested phases overlap. Each worker captures only its own thread/database observations, and the group merges those captures rather than including unrelated concurrent maintenance. `admission_checkpoint` encloses pressure-triggered checkpoint calls, including waiting for preparation. A trace is recorded before receipts are delivered; it is not acknowledgment delivery time.

History is diagnostic, non-durable and may be evicted. It contains no table names, user rows or request-ID strings. Empty history with capacity zero is disabled capture, not zero-cost work. Snapshot cloning/serialization consumes bounded additional memory. No complete trace coverage or p99 claim follows when history was evicted. Tests cover exact two-sync attribution, pressure-checkpoint correlation, thread/database isolation, unwind restoration, auth, bounds and unchanged receipt/reopen behavior.

## Storage protocol

A group uses one checksummed `AppendGroup { items }` operation and one physical sequence. Each accepted item carries its table, ID, row digest, rows and `now_us` admission clock. Direct writes preserve the legacy `Append` operation. The selected backend determines publication: legacy immutable frames synchronize a temporary file, rename it and synchronize the directory; the segmented journal synchronizes appended data, with additional namespace barriers for creation, rotation and reclamation. Neither path acknowledges before its required durability barriers.

State-dependent admission constructs touched aggregate values, receipts and hot batches in a private overlay against committed state. A `Send + 'static` `PreparedEpoch` retains private prepared data, the encoded record, reservations and an owned exclusive publication lease across the worker handoff. No OS mutex guard moves between workers, and committed row/receipt/aggregate maps are not mutated and undone to obtain the delta. Sync yields a consuming `DurableEpoch`; only its atomic installation advances visibility and permits successful new-write completion. File synchronization occurs outside the reader-state mutex. Dropping an uninstalled durable epoch or unwinding publication fences subsequent writes, even when the caller catches the panic.

Readers can observe preceding committed data while append sync is paused. Structural publication and GC wait on the same publication authority. Exact/frozen-prefix checkpoint preparation is separately governed: a bounded admission retry discards private work, releases authority where permitted, then revalidates on return. Pre-I/O rejection discards the private delta; ambiguous WAL I/O fences and recovery determines the durable outcome. No durability barrier was dropped. State-dependent preparation remains globally serialized, independent partition owners are not implemented, and root/control fsync or synchronous fallback checkpointing can still hold state.

No full-catalog clone is made per group. Canonical request items remain in private prepared owners. A borrowed WAL view leaves those inputs intact across bounded retries; neither the first digest nor live replanning allocates a full row-JSON buffer. Replay independently validates decoded input. Conversion into live stored rows and WAL encoding still allocate; this is not a zero-copy or RSS guarantee. Private overlay working space has an explicit metadata-byte limit, also charging 64 bytes per provisional receipt's group proof. Metadata accounting uses encoded-size deltas and reserves future segment-reference space. Before per-request admission, each grouped receipt receives a 64-character lowercase-hex placeholder: the optional `group_fingerprint` field costs exactly 87 encoded JSON bytes, including its key, separator and quotes. Rejected items are excluded as before. The final accepted operation is encoded once; its canonical payload supplies the domain-separated group proof without another JSON serialization. Its fixed-length hex proof has exactly the placeholder's encoded size. Successful live publication installs the privately prepared forward rows/maps; historical recovery still reconstructs and validates them through the replay path. No post-ack metadata budget exemption or full-catalog rescan is needed. Existing checkpoint implementation still clones the catalog and serializes it; group pressure can trigger those checkpoints. Current receipt/rollup totals scan bounded table metadata rather than maintaining another durable counter.

Results preserve input order. Bad rows, unknown tables, ID conflicts, aggregate overflow and per-item metadata admission failures are isolated. Same-ID/same-data requests in one group return one new receipt followed by duplicates; conflicting data is rejected. A failed publication turns all receipts depending on that new physical sequence into errors, including provisional duplicates, while already durable retries remain successful. Row ordinals are contiguous across accepted items **including across tables**, so `(sequence, ordinal)` is globally unique within a group. OHLC still sorts by event timestamp, then sequence, then ordinal.

The direct `write_group` API divides large vectors into bounded physical groups; it is not a transaction across the whole vector. Requests outside its conservative grouping envelope use the direct-write mode of the same commit pipeline, preserving legacy `Append` bytes and exact single-request admission outside the conservative grouping envelope. Caller-owned input/output vectors naturally scale with caller input; use `Ingestor` for bounded pending submission. Tight metadata/private-overlay admission can reject an item that a smaller group or an explicit checkpoint would admit. There is no unbounded retry or automatic splitting on aggregate-metadata pressure. Existing legacy-ID registry exhaustion remains a hard error; committed IDs are never silently forgotten. Timed-ID pruning is checkpoint-based: when an incoming request first advances the floor while the registry is already full, an explicit checkpoint followed by a same-ID retry may be needed. A checkpoint retry discards the private overlay; provisional row/receipt/aggregate maps were never installed.

With `checkpoint_frozen_prefix=true`, shared direct/grouped pressure uses at most one off-lock checkpoint/revalidation attempt, including hot/WAL capacity and metadata-headroom pressure. Private tentative changes are discarded before unlocking; canonical inputs are retained by ownership and receive the actual new sequence/proof on retry. Rollback floor baselines are refreshed after reacquiring state, so another caller's persisted floor cannot be lowered. Input-ordered, already durable duplicate outcomes and their successful clock advances survive unrelated new-request failures; provisional duplicates do not. A receipt-only root can reclaim capacity without advancing the checkpoint sequence. A stale/no-progress candidate is a bounded admission error, not permission to bypass limits or silently fall back to locked preparation. Direct `write` pressure remains locked.

Capture, the final manifest commit, installation and obsolete-WAL namespace removal/accounting still hold state. After those changes are installed, the frozen-prefix path releases state and the writer gate before synchronizing the obsolete-WAL directory and destroying private retired state. A consumed-once `RetiredWalPrefix` carries the still-required directory barrier; successful checkpoint return still waits for it. Pins, preparation ownership and working reservations remain live through reclamation. New appends have higher sequences and retain both of their own WAL durability barriers. Broad garbage collection is deferred by the prefix path. Neither this flag nor the added phase metrics changes WAL/root formats, fsync acknowledgement semantics, hot age/pressure policy, or the single-node durability boundary.

## Recovery and compatibility

The existing envelope magic/version and legacy `Append` decoding remain readable. New operations and fields are deliberately unknown to older binaries: **binary rollback after group writes is not supported**. Group proofs remain in checkpoints after WAL retirement, and older strict decoders reject those receipt fields too. Checkpointing does not make an older binary operationally safe; use a verified migration/export plan and include remote history in that assessment.

Every new grouped `ReceiptEntry` persists `group_fingerprint: Some(<64 lowercase hex characters>)`. It is BLAKE3 over `varve/append-group-proof/v1\0` followed by the typed Serde JSON encoding of the **entire WAL Record**: format version, sequence, operation tag, ordered items, table/ID/digest/ordered rows and each item's optional clock. Hashing streams into the hasher rather than allocating another group-sized buffer. This typed encoding (including field order and omission rules) is part of the proof format and must remain stable. Receipt proofs survive checkpoint/WAL retirement and bind even members whose timed IDs are later pruned.

Legacy single receipts deserialize a missing proof as `None` and omit it when serialized, preserving their prior encoding. Legacy group WAL items may omit `now_us`; replay keeps that absence in the fingerprint rather than inventing a clock. All new grouped items store `Some(now_us)`. Local replay and remote restore derive the same proof from the complete record. A legacy receipt alone cannot reconstruct a group proof.

Replay checks contiguous physical sequences, checksum, version, group/request/row bounds, each digest, unique receipts, table existence, durable ID floors, row validity, retention, aggregate arithmetic, hot state and metadata limits. A committed corrupt or oversized group fails closed. Remote prefix verification checks every item plus its full group fingerprint: a remote `AppendGroup` requires a present, equal local proof (missing/legacy proof fails closed); a remote single `Append` requires no local group proof. Remote checkpoint receipts compare optional proofs exactly in addition to sequence, row count and payload digest. Missing metadata cannot be used to infer an order or a group membership from a single matching request. Local acknowledgment is still not remote acknowledgment; loss of an unshipped local tail remains possible. No distributed or production durability guarantee is inferred from these local tests.

## Acceptance ledger

| Check | Evidence | Status |
| --- | --- | --- |
| One physical publication for independent writes; retries/conflicts; cross-table ordinals; late OHLC | `tests/group_commit.rs` | pass |
| Invalid rows and aggregate overflow isolated | `tests/group_commit.rs` | pass |
| Engine row/receipt bounds; checkpoint/reopen; timed floors; durable retry at capacity | `tests/group_commit.rs` | pass |
| Remote restore/reconciliation: matching first receipt cannot mask later ID/digest/row-count divergence | `tests/group_commit.rs` | pass |
| Whole-group proof survives checkpoint/WAL retirement: reversed tied-timestamp OHLC, pruned proper subset, differing clocks and missing proof reject for WAL and checkpoint prefixes | `reconciliation_rejects_*`, `reconciliation_requires_group_clocks_and_durable_proof` in `tests/group_commit.rs` | pass; order/subset failures witnessed before fix |
| Proof bytes admitted before publication; exact-limit/one-byte-short isolation; exact cached metadata through undo, replay and checkpoint; legacy single encoding unchanged | `engine::tests::group_proof_metadata_admission_is_exact_before_publication`, `legacy_single_receipt_encoding_is_unchanged`, `append_metadata_delta_matches_exact_serializer` | pass |
| Checksummed corruption, semantic corruption, oversized/empty groups fail closed | `tests/group_commit.rs` | pass |
| Eight publication crash/I/O points; provisional duplicates fail but previously durable retries succeed | feature-gated `tests/group_commit.rs` | pass |
| Crossbeam count/row/byte/time flush; byte admission; dropped receivers; shutdown; concurrency | `tests/ingest.rs` | pass (7 tests) |
| Zero-delay/expired backlog still coalesces without waiting | `ingest::tests::expired_deadline_still_coalesces_ready_backlog` | pass |
| Undo working set is bounded; cached metadata equals exact encoding after rollback/replay | `engine::tests::group_undo_metadata_is_bounded_and_exact` | pass |
| Self-checking single versus queued physical frames and timings | `examples/ingest_bench.rs` | pass (128 and 512 requests) |
| Existing write/replay/remote publication invariants | library (31), engine (14), background I/O (7), publication (13), metadata longevity (3) tests | pass with fault injection |
| Strict targeted Clippy | library, new integration tests, benchmark with `-D warnings` | pass |

Focused grouped-prefix proof checkpoint: 13 group integration tests pass with `fault-injection` (including the subprocess helper). The new order-reversal and timed-ID-pruned proper-subset regressions both failed before the fix because divergent histories incorrectly reconciled at sequence 2. They now reject after all local WAL has been checkpointed away and the local database reopened, against both remote WAL and checkpoint receipts. The order case explicitly witnesses OHLC `(first,last) = (1,2)` versus `(2,1)` for tied timestamps.

Also passed at this checkpoint: engine library tests (8, including legacy clock-less group replay), admission (4), metadata longevity (3), background I/O/proven-prefix recovery (7), engine/archive/restore (14), publication (13), and all-target strict Clippy with and without `fault-injection`. This is focused local evidence, not the owner's later full-suite gate. A parallel reopen initially encountered a transient process-lock error while another test forked a crash child. The new group test file now serializes its crash/reopen probes; all durability/corruption assertions remain unchanged.

A local 512-request probe measured 512 regular WAL frames versus 4 queued frames, about 4578 ms versus 354 ms. Every queued receipt completed successfully; peak charged pending memory was 193,792 bytes, final pending memory zero. The frame count corresponds to 1024 versus 8 successful file-plus-directory WAL sync calls in this implementation, excluding initial table creation/checkpoint work. These are bounded temp-data observations, not a performance SLA or direct hardware power-loss certification.

Commands (configured small test/dev profiles, no bundled DuckDB):

```sh
cargo test --features fault-injection --test group_commit --test ingest
cargo run --example ingest_bench -- 512
# Focused grouped-prefix proof checkpoint:
cargo test --features fault-injection --test group_commit --test metadata_longevity --test background_io --test publication --test engine --test admission
cargo test --features fault-injection --lib engine::tests::
cargo clippy --all-targets --features fault-injection -- -D warnings
cargo clippy --all-targets -- -D warnings
```

Integration dependencies are now present: the root Cargo owner added `crossbeam-channel = "0.5"`, and the explicitly authorized `src/tier.rs` prefix branch validates every group item. Server owners must still wait for completion receivers before acknowledging and explicitly drain ingestion during shutdown. This implementation does not edit Cargo/service/client files. No commits, pushes, cloud calls or container runs were performed.
