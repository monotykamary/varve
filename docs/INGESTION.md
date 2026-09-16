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
Ingestor::shutdown(&self) -> anyhow::Result<()>;
Ingestor::stats(&self) -> IngestStats;
```

`Ingestor` is cloneable. `submit` is nonblocking with respect to queue capacity and database I/O; it briefly locks admission/accounting and validates the owned request. An immediate error guarantees **not enqueued**. A receiver is only an admission ticket. Only its successful completion acknowledges `local_fsync`. A dropped receiver does not cancel admitted work. Shutdown closes admission across all clones, drains, and joins; concurrent shutdown callers also wait for drain. Last-owner drop does the same best-effort drain. Shutdown/drop are blocking: async applications should explicitly call shutdown through `spawn_blocking` before their runtime exits.

`IngestConfig` defaults:

| Field | Default |
| --- | --- |
| `queue_capacity: usize` | 1024 |
| `max_pending_bytes: usize` | 16 MiB |
| `max_group_requests: usize` | 128 |
| `max_group_rows: usize` | 10,000 |
| `max_group_bytes: usize` | 4 MiB |
| `max_delay: Duration` | 2 ms |

The Crossbeam queue is bounded, multi-producer/single-consumer. Byte credit stays charged while queued, while carried into the next group, and throughout WAL publication. Credit releases before completion wakes a producer. Count, rows, conservative bytes, deadline from oldest admission, and channel disconnect trigger flushes. Blocking `recv`/`recv_timeout` avoid idle spinning. An expired deadline stops waiting but still coalesces already-ready backlog; slow fsync does not force that backlog into singleton frames. One carry request can be outside the queue in addition to the in-flight group; both retain byte credit. Pending request count is bounded by queue capacity + group capacity + one carry. Queue capacity is limited to one million slots, physical group count to 1024, delay to at most 60 seconds. Zero delay is supported.

Group row/byte limits are clamped to engine batch/hot/WAL bounds at construction. The byte charge includes conservative escaped-JSON space, row/vector allocations and retained string capacities. It is intentionally larger than typical encoded JSON and is **not an RSS or OS disk quota**. Input buffers owned by callers, transport buffers, engine state, temporary encoding, and aggregate preparation require separate budgets. Server/client transport owners must bound those independently.

`IngestStats` is a fixed-size serializable snapshot: saturating submitted/rejected/completed/succeeded/failed/dropped-receiver/group counters, current and peak pending requests/bytes, and closed state. `groups` counts worker flushes, not physical frames: an all-retry/invalid flush writes no frame. A failed completion can mean ambiguous publication; retry the same request ID rather than assuming no write occurred.

## Storage protocol

A group uses one checksummed `AppendGroup { items }` WAL record and one physical sequence. Each accepted item carries its own table, ID, row digest, rows and `now_us` admission clock. A physical publication does one temporary-file `sync_all`, same-filesystem rename, and one directory `sync_all`, **not a file fsync for each request**. The existing `Database::write` still writes the legacy `Append` operation.

Admission stages only affected aggregate values, receipt insertions and hot-row appends under the database mutex. A bounded undo log restores provisional changes before calling the WAL publication path, so a capacity-triggered checkpoint cannot persist unpublished rows. After fsync, the entire group is applied under the same mutex before any new receipt is returned. Aggregate preparation is repeated during final application; this trades CPU for reusing the same validated replay path. Unexpected application failure fences the database. Ambiguous WAL I/O errors also fence it, preserving the existing reopen-to-resolve contract.

No full-catalog clone is made per group. Aggregate rollback working space has an explicit metadata-byte limit, also charging 64 bytes per provisional receipt's group proof. Metadata accounting uses encoded-size deltas and reserves future segment-reference space. Before per-request admission, each grouped receipt receives a 64-character lowercase-hex placeholder: the optional `group_fingerprint` field costs exactly 87 encoded JSON bytes, including its key, separator and quotes. Rejected items are undone/excluded as before. After rollback, the final accepted operation is hashed before WAL publication; its fixed-length hex proof has exactly the placeholder's encoded size. Final application and recovery use the same preparation/accounting path with that actual proof. No post-ack metadata budget exemption or full-catalog rescan is needed. Existing checkpoint implementation still clones the catalog and serializes it; group pressure can trigger those checkpoints. Current receipt/rollup totals scan bounded table metadata rather than maintaining another durable counter.

Results preserve input order. Bad rows, unknown tables, ID conflicts, aggregate overflow and per-item metadata admission failures are isolated. Same-ID/same-data requests in one group return one new receipt followed by duplicates; conflicting data is rejected. A failed publication turns all receipts depending on that new physical sequence into errors, including provisional duplicates, while already durable retries remain successful. Row ordinals are contiguous across accepted items **including across tables**, so `(sequence, ordinal)` is globally unique within a group. OHLC still sorts by event timestamp, then sequence, then ordinal.

The direct `write_group` API divides large vectors into bounded physical groups; it is not a transaction across the whole vector. Requests outside its conservative grouping envelope fall back to the unchanged single-write API. Caller-owned input/output vectors naturally scale with caller input; use `Ingestor` for bounded pending submission. Tight metadata/undo admission can reject an item that a smaller group or an explicit checkpoint would admit. There is no unbounded retry or automatic splitting on aggregate-metadata pressure. Existing legacy-ID registry exhaustion remains a hard error; committed IDs are never silently forgotten. Timed-ID pruning is checkpoint-based: when an incoming request first advances the floor while the registry is already full, an explicit checkpoint followed by a same-ID retry may be needed. No checkpoint is allowed while provisional group state is installed.

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
