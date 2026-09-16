# Delayed audit reconciliation

Agent `47d0d22423db448eb0554a718b8911c2` audited an earlier, uncommitted snapshot. Its delayed report and truncated tail were read and compared with the current source. The current implementation demonstrably differs from several cited mechanisms; its **not-production-ready verdict still stands**. This reconciliation is source inspection and existing evidence, not a new test run or certification.

## Finding disposition

| Earlier finding | Current source/evidence | Disposition |
| --- | --- | --- |
| Public service has no authentication | `service.rs` authenticates operator endpoints; `security.rs` covers the boundary. Live HTTPS rejected unauthenticated status/metrics, and the private Linux probe verified auth before body reads. | Superseded for the configured evaluation. This is one trusted-operator credential, not tenant isolation. |
| Cold SQL/native fetches hold global state | `Database::query` and `scan` capture/pin state, leave the lock scope, then call `resolve_segment`; its remote GET also precedes disk admission. `status` scans disk after leaving state. | The specific remote-fetch lock scope is fixed. Snapshot cloning, local writes and shipping capture still do work under state; broad liveness/scaling qualification remains open. |
| Startup eagerly decodes the entire WAL tail | `wal::Records` is an iterator. `wal::records` checks cumulative encoded tail bytes against `wal_max_bytes` before decoding frames. | Whole-tail eager decoding is fixed, but this is not allocation-free recovery: filenames are indexed, one frame is decoded, and `open_locked` checks hot/group/metadata limits **after** `replay` applies that operation. Stricter pre-admission and memory-profile regressions remain work. |
| Permanent receipts and full-manifest serialization on every append | Opt-in timed request IDs persist monotonic floors, reject expired retries and prune receipts at checkpoint. `append_metadata_bytes` uses cached serializer-equivalent deltas. | Mechanisms improved and covered by longevity/delta tests. Catalog/rollup state remains bounded and monolithic; default infinite-idempotency mode can still hit capacity. This is not scalable unlimited indexed metadata. |
| Routine remote GC runs only after raw expiration | `maintain` uses `vacuum_due = remote_vacuum_pending || ship_due` and resumes a persisted owned GC lock. | Routine scheduling is implemented without requiring raw expiration. Long publish/compact/restart object-count qualification is not established by the small cloud drill. |
| Vacuum materializes all keys | Builtin adapters implement `list_page` and bounded deletion; vacuum consumes pages. The compatibility `list` API is capped; custom default pagination is explicitly not allocation-safe. | Builtin full-list requirement removed. No million-key/RSS qualification was performed. |
| Segment size/binding publication evade admission | Segment output is bounded during writing; local publication shares a disk-admission coordinator; head CAS has a physically written/synced worst-case binding reservation; pre-publication checkpoint failures clean unreferenced outputs. ENOSPC/publication regressions passed. | Safety mechanisms improved. `ensure_budget` still recursively scans directories, including on the write path; exact incremental disk counters and general external-free-space/quota protection are not implemented. |
| First CAS ambiguity requires a fresh namespace | Durable exact-head intent precedes CAS; matching same-root recovery preserves the local tail. First-publication and escape-heavy-token crash regressions cover it. | Superseded by the implemented intent protocol. |
| Mutable policies and dynamic aggregates are absent | Versioned WAL operations implement policy changes, named aggregate creation/backfill/drop and job controls; local and cloud tests exercise them. | Superseded for those capabilities. Physical shard/window rewrites, arbitrary-SQL IVM and automatic rollup substitution remain unsupported. |
| No general format migration story | Legacy S3 heads have a specific envelope upgrade path, with downgrade warnings. | A general independent-version, copy-on-write migration/rollback framework and historical fixture matrix remain absent. |
| No actual Railway/provider exercise | The exact artifact passed real-bucket conditional operations and fresh-directory restore; a new replica reopened the same volume with exact 100,000-row count/sum preservation. | More evidence now exists, but not provider certification, power-loss proof, real ambiguous-response fault qualification or an actual lost-volume incident drill. |

## Important coverage limits

- **100,000 rows were not 100,000 request IDs or rollup groups.** The main cloud run used 196 write-batch samples and reported 288 rollup groups. It does not satisfy the requested high-cardinality/long-lived metadata benchmark.
- Existing blocking-store tests exercise background publication and maintenance prefetch. They do not substitute for the precise requested direct cold `query()` and `scan()` regressions that must prove write/status completion while each fetch is blocked.
- Recovery limits are checked per operation, not only after the whole tail, but the decoded operation is already applied when those checks run. Do not advertise a hard RSS guarantee from encoded WAL limits.
- Local filesystem admission walks, shipping manifest/WAL capture under state, checkpoint serialization and bounded cache-directory work can still cause latency to grow with file/metadata count. The small two-table workload is not proof otherwise.
- The direct Railway restart left an exited container despite stale `SUCCESS`; same-artifact redeployment recovered service and data. Root cause remains unresolved. `/ready` may also conservatively return 503 while state is busy.
- There is still no Git commit identifying the candidate. Source-input manifests and matching binary hashes provide the recorded provenance; an immutable release commit/attestation is additional release work.

## Next engineering gates

1. Pre-admit projected replay hot/group/metadata growth before retaining it; test lower-limit reopen with measured allocations.
2. Add the direct blocked cold-query/scan regressions, then reduce remaining state-locked local I/O with explicit snapshot/file-lifetime coordination.
3. Replace per-operation recursive disk walks with reconciled incremental accounting; qualify file-count scaling and external disk-pressure behavior.
4. Run genuinely high-cardinality request/group and million-key GC/restart workloads with explicit resource/latency ceilings.
5. Define general migration/rollback compatibility, resolve the deployment restart incident and complete hardware/provider/long-soak qualification.

See [EVALUATION.md](EVALUATION.md) for observed results and [PRODUCTION_ACCEPTANCE.md](PRODUCTION_ACCEPTANCE.md) for the unchanged non-production release decision. No runtime source or cloud resource was changed by this reconciliation.
