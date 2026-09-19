# Indexed derived checkpoint state

## Public selection

`varve::RollupSelection<'a>` exposes optional borrowed `tenant`/`series` and `width_us`, `start_us`, `end_us` filters. `Database::select_rollups(table, selection)` returns owned rows; `Database::rollups(table)` delegates to the default unfiltered selection. Time bounds are half-open **bucket-start** bounds, not raw-row timestamps. Reversed bounds reject; equal bounds select nothing.

The resident index is width → tenant → series → bucket → canonical keys. Exact constraints select subtrees; broad queries still visit the selected branches. Output retains canonical-key ordering, binary string comparison, Unicode/tag identity, negative buckets, sequential finite-f64 accumulation and `(timestamp, sequence, ordinal)` ties. The index does no aggregate arithmetic. Append, grouped undo, replay, control changes and retention maintain it; open rebuilds it. Proven SQL width/tenant/series selections use the same index. Unsupported SQL still falls back conservatively.

## Persistence and compatibility

Legacy `VARVEM01` roots contain inline maps. `Config.derived_pages=true` explicitly opts into `VARVEM02` at a successful checkpoint/root publication, not at an append acknowledgement. New readers recognize either format independently of the flag. Once v2 is authoritative, later roots stay v2 even with the flag disabled. Older binaries cannot read v2; changing its header is not a downgrade procedure.

WAL records, remote heads and the runtime catalog retain their existing model versions. The separate wire adapter records v2 authority even for empty page sets. Each table has independently content-addressed rollup and receipt sets, but **one shared checkpoint/replay frontier** still commits both sets, raw references, definitions, retention cutoffs and idempotency rejection floors. This is not independently checkpointed derived state.

Each `VARVED01` page has a strict typed JSON payload, checksum and full-object BLAKE3 address: `derived/<digest>.page`. Fixed-size references contain digest, byte size and entry count—not arbitrary canonical/tag keys. Readers verify envelope, digest, kind, table, sizes, totals, order, uniqueness, canonical keys, source dimension/tag limits, finite aggregates and tie consistency. Float roundtrip decoding preserves supported finite bits, including negative zero. Missing committed aggregate state cannot be reconstructed from raw history that may already have expired.

Unchanged sets reproduce the same immutable objects. Changed sets are re-encoded during checkpoint/control work; this is **not page-local update cost, an out-of-core index, or a resident-memory saving**.

## Admission and hot-path cost

`metadata_max_bytes` remains the control-root/recovery headroom budget. V1 also retains its inline metadata limit. `derived_max_bytes` defaults to 64 MiB (4 KiB–512 MiB) and separately bounds conservative resident maps/indexes, outstanding internal working reservations and encoded derived state. It applies to both formats; a large valid legacy database may require a larger configured derived budget. Insufficient budgets fail closed, not by silently dropping groups or receipts. These are logical admission bounds, not total RSS or all caller-owned allocations.

`derived_page_bytes` is the writer target: 256 KiB by default, 4 KiB–1 MiB and no larger than the derived budget. Readers accept historical pages up to the fixed 1 MiB format cap, not merely today's writer target. A smaller writer target can read an older larger page yet refuse a future write/repack if an existing individual entry cannot fit. Oversized entries reject rather than weakening the limit.

Append admission updates cached exact logical-map JSON sizes/counts and conservative largest-entry sizes in O(touched entries). Adjacent greedy-page pairs bound page count from payload bytes; fixed-size refs bound future root headroom. Decimal growth and hot-row checkpoint reserves are charged before WAL publication. No full page encoding or whole-state resident recount occurs on successful single/group append. Group undo restores prior counters directly. Exact packing/accounting rebuilds remain checkpoint, open and infrequent control/retention work.

Working reservations cover internal snapshots and staged operations globally across tables. Hydration conservatively reserves up to 64 working bytes per encoded page byte before loading; a small budget can reject a format-legal page. Writer/recovery reservations intentionally leave headroom and can reject before the nominal maximum data size is reached.

Live append and map-growing control admission additionally preserve zero-contention checkpoint capacity: `2 × projected resident charge + page workspace ≤ derived_max_bytes`, where v2 page workspace is four writer pages and v1 is zero. Current checkpoints retain cloned catalog maps while building replacement indexes; the new condition prevents admitting state that cannot checkpoint after other reservations drain. It uses the cached projection on append, without scanning maps or encoding pages. Concurrent queries can still temporarily consume working space; this is not an RSS, disk or control-metadata guarantee.

Historical WAL replay does not apply the newer live-writer headroom rule, but its actual resident/working limits remain enforced. Older acknowledged state can therefore reopen yet need an explicitly larger configured budget for its next checkpoint. Budgets must be sized for both retained state and current whole-set checkpoint working demand; this is not out-of-core paging.

## Publication, restore and GC

Prepared maintenance handles raw output, whole-set derived encoding, dependency verification/writes, index construction and accounting outside state with budgeted reservations and active raw/page pins. Root publication requires exact generation, sequence and timed-floor identity; scheduled stale work defers. Compaction freezes its whole root only after selected immutable input I/O and exact descriptor revalidation, and never rebases already-frozen derived state. Initial capture still clones under state; root fsync, disk admission and GC remain serialized. Explicit/control/admission fallbacks remain synchronous. This is not page-local update cost, a lock-free maintenance design, or a frozen-prefix checkpoint that can publish through later appends.

Dependencies are written and synced before the atomic root rename. Derived publication is protected by state/disk-admission serialization; pending raw outputs remain pinned across the off-lock interval. Only successful root publication permits retirement of covered hot rows/WAL. Failed candidates leave unreachable immutable objects, never a partial authoritative replacement.

Shipping captures the exact persisted root and pins its exact page/raw dependencies, rather than substituting newer live maps. Verified pages precede checkpoint upload and head CAS. Restore verifies/hydrates pages before replay and ownership release. Owned-prefix reconciliation hydrates receipts and proves raw/derived cutoffs **and the idempotency floor** are monotonic, including when older receipts were already pruned. Local/remote GC include current-root reachability and active pins. Existing single-owner/CAS restrictions remain; this adds no distributed leader-election guarantee.

## Status meanings

- `metadata_bytes`: current logical inline-catalog footprint, retained for honest accounting even in v2.
- `control_root_bytes`: actual bytes of the last published local root, not an estimate of the next root.
- `derived_encoded_bytes`: page bytes referenced by that persisted root; zero for inline v1. This is not the latest uncheckpointed encoded-state estimate.
- `derived_resident_bytes`: current conservative live maps/index charge.
- `derived_working_bytes`: outstanding internal working reservations, not a peak or RSS measurement.

Prometheus exposes these with the `varve_` prefix. Rust/TypeScript clients keep new fields optional for older servers. See [METRICS.md](METRICS.md).

## Executable evidence and limits

`tests/derived_state.rs` covers opt-in/sticky migration, native selector equivalence, independent budgets, retention, timed receipt floors, FileStore shipping/restore/prefix/GC, missing/corrupt pages, failed uploads and process crashpoints. Pure tests cover resealed duplicates/invalid dimensions, finite bit patterns, bounds and deterministic identities. Engine unit tests check cached accounting against recomputation, zero page-codec calls during append/group staging, undo/reopen consistency, long names, escaping and decimal/page boundaries.

Independent review found and reproduced a pruned-floor rollback and invalid hydrated dimensions; both now have old-fail/new-pass regressions. These checks are single-node/FileStore evidence, not live S3, power-loss, lost-volume, hard-latency, out-of-core or production qualification. The complete release and Railway gates are tracked in [REUSE.md](REUSE.md).
