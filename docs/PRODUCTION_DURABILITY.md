# Production durability hardening

This document records the local mechanisms added for publication crash recovery and disk admission. It is not a production certification. Remote durability still begins only when a remote head is conditionally published; locally acknowledged WAL that has not shipped remains outside the remote recovery point.

## Exact remote publication intent

Before a normal ship or remote-GC head CAS, Varve durably writes `remote-publication.intent`. The file has a versioned magic value, bounded payload length, and BLAKE3 checksum. Its payload records:

- a digest of the canonical local database root;
- database and publisher identities;
- the exact intended remote sequence;
- the exact expected predecessor token, including `null` for first publication;
- the exact serialized intended head bytes and their digest; and
- the size of the physical local binding reservation.

The intent and its directory entry are synced before CAS. Recovery does not infer success from matching owner identity. It adopts a head only when the remote bytes exactly equal the intended bytes, the database/root/sequence checks pass, and the expected predecessor agrees with any older durable binding. This permits recovery of the first successful CAS even though no older `remote-binding.json` exists. It also permits later publication recovery without discarding later locally acknowledged WAL.

A remote head that is neither the unchanged expected predecessor nor the exact intended bytes is not adopted. The instance is fenced and the intent is retained for diagnosis. Corrupt, future-version, wrong-root, wrong-database, and sequence-ahead intents fail closed. A CAS error followed by the exact intended remote bytes is treated as an ambiguous successful response; an unchanged predecessor is treated as a failed CAS and the stale intent is removed.

The intent is removed only after `remote-binding.json` is durable. If a process dies after the binding rename but before intent cleanup, reopen verifies that the durable binding and exact remote bytes agree before cleaning up.

## Binding-space reservation

Before CAS, Varve admits both the intent and a conservative binding size against `Config::max_disk_bytes`. It then creates `remote-binding.reserve` by requesting filesystem allocation, writing real zero-filled blocks, and syncing the file and directory. It does not use sparse `set_len` as a claim of reservation. The conservative size includes the exact intended head plus the worst-case JSON expansion of the maximum accepted opaque remote token (64 KiB raw, up to six encoded bytes per raw byte). Invalid tokens after a confirmed or reconciled CAS immediately fence both publication and GC paths while preserving the intent.

After CAS, Varve overwrites that reserved storage with the binding, syncs it, truncates it to the exact binding length, renames it to `remote-binding.json`, and syncs the root directory. Failures after CAS preserve the intent and any usable reservation and fence the live instance. Reopen can complete the exact binding from those artifacts.

The reservation is part of recursive database disk accounting. A shared `Inner::disk_admission` mutex is required around every local `directory_bytes + additional write` admission window. Checkpoint/segment publication, WAL append, cold materialization, policy restoration, manifest writes, and tier binding reservation must all participate before `max_disk_bytes` can be described as a strict in-process logical admission limit. The mutex does not create an OS quota. Filesystem metadata growth, another process writing the filesystem, delayed allocation, quota changes, device failure, and an actual ENOSPC can still make an admitted write fail. Those failures must remain recoverable; they are not evidence that physical capacity was guaranteed.

## Segment publication limit

`segment::write_with_limit(path, rows, max_bytes)` enforces the byte limit in the writer itself. A write that would cross the limit errors before that chunk is written, closes the writer, and removes partial output. The existing `segment::write` remains compatible and delegates with an unbounded limit. Engine publication passes the smaller of the remaining local disk budget and the format limit, so an oversized Parquet file is rejected while it is still an unpublished staging file.

## Fault injection

Process-exit failpoints remain feature-gated by `fault-injection` and `VARVE_FAILPOINT`. Publication adds `remote_publication_intent_persisted` before CAS and retains `remote_head_published` after CAS and before local binding. GC additionally exposes `remote_gc_lock_acquired` and `remote_gc_lock_released` at the corresponding CAS/binding gaps.

`VARVE_IO_FAILPOINT` is also compiled only with `fault-injection`. Values use `name:io` or `name:enospc`. Targeted boundaries include:

- `wal_before_write`, `wal_before_sync`, `wal_before_rename`, and `wal_before_dir_sync`;
- `atomic_<filename>_before_create`, `_before_write`, `_before_sync`, `_before_rename`, and `_before_dir_sync`, including `manifest.bin`;
- `remote_binding_reservation_before_create`, `_before_allocate`, `_during_write`, and `_before_sync`;
- `remote_binding_before_write`, `_before_reservation_sync`, `_before_sync`, and `_before_rename`; and
- `segment_before_create`, `segment_before_write`, and `segment_before_flush`.

Default builds do not read or act on these environment variables. Subprocess tests isolate environment mutation and verify that previously acknowledged batches survive WAL, manifest, and binding failures while a failed new batch is not partly replayed.

## Focused acceptance evidence

The owned patch is exercised by `tests/publication.rs`:

- default mode: 4 tests passed (`cargo test --test publication`);
- fault-injection mode: 13 tests passed (`cargo test --features fault-injection --test publication`);
- remote adapter contract: 15 passed and the explicit live-S3 test remained ignored (`cargo test --test remote_store`);
- existing non-fault background I/O tests: 4 passed (`cargo test --test background_io`);
- existing remote crash matrix: 1 passed (`cargo test --features fault-injection --test engine crash_matrix_remote_publication_boundaries -- --exact --nocapture`); and
- the integrated all-target Clippy gate subsequently passed; this supersedes the earlier patch-local warning in `src/control.rs`. Final candidate status is tracked in `PRODUCTION_ACCEPTANCE.md`.

Main updated both legacy background crash expectations to require immediate exact-intent adoption in the same namespace. The integrated fault-injection `background_io` suite passed 7/7, and the hardened HTTP/security suite passed 4/4. Final all-target verification is tracked in `PRODUCTION_ACCEPTANCE.md`.

## Remaining limits

- Restore lock acquisition/release and administrative `recover_remote_lock` use different lifecycle constraints from an already-open local database. The administrative API has no durable local root in which to record an intent. Those ownership-changing paths must not be represented as covered by the normal database-root publication intent until their API supplies equivalent durable operation storage.
- Remote safety still depends on a provider implementing strongly consistent conditional head updates and stable opaque tokens within the documented local bound.
- A local fsync acknowledgement is not a remote acknowledgement. No scheduler interval bounds data loss during a remote outage.
- Tests with the filesystem adapter exercise single-node protocol behavior only. They do not establish live-S3, distributed, hardware, filesystem, or production guarantees.
