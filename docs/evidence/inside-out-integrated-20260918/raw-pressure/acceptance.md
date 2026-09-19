# Raw pressure acceptance — qualified targeted source

Base source manifest: `86c317d4e103be5f69e55de0852885ecd9ae4f6be5d064a00c0c45d7db538c5d`.
Final source-files.sha256 (128 entries): `5244039ae491f1ccd3481b630967b7127435572cf35a775e13e21ad26b5b33e3`.
Final owned-files.sha256 (19 entries): `e8cdd5692ceeb11e537907e67cbc382acef3572a71d4113b80dbab80c3ceac39`.
Exact required local git root and AGENTS/ARCHITECTURE/ACCEPTANCE verified before edits; remote base and local frozen review verified unchanged afterward. Full local final manifest and all 19 synced owned files pass checksum verification. This is the declared source-manifest scope, not every file in the dirty repository.

- [x] Typed TooLarge versus Pressure, linear atomic resize/shrink/split and after-unlock notification; failed resize leaves credit unchanged. Regular owned pool remains distinct from maintenance/recovery.
- [x] submit_wait registers/enables both raw and closure/capacity notification before retry; raw-only closure/cancel/lost-window and synchronous rejection covered. Full-queue refund cannot self-wake/spin. No locks held across await.
- [x] Borrow-only overlay/checkpoint retries; final borrowed WAL encoding precedes one terminal Row into_iter move. Production materialization cannot reserve a second pool lease. Original credit splits into retained and transient envelopes with no free-credit gap. Compact independently durable clocks replace consumed PreparedWrite inputs.
- [x] Actual canonical JSON count/types/capacities determine envelope. Frame3F includes old+new backing during Vec growth, input Vec capacity is separately charged, and explicit stored Vec avoids in-place retention of excess input capacity. Oversized tenant/series/tag capacities retain pointers and live credit. Prior logical pending/group ceilings remain unchanged.
- [x] Incremental hot/cold scan construction; empty/small16MiB output/128MiB pool; logical and raw-quota failures after partial output return baseline. Allocation-free total ordering preserves sequence/ordinal semantics.
- [x] Native fixed regular-pool lease before preparation/open, cached once-per-owner escaped tag maxima, checked nonalloc counting, reusable global thread-slot callbacks, and actual per-instance UserData/Bind/Global/Local box reservations. Leases survive cancellation join and connection/database teardown; no source-size scratch or pooling. Tests cover escaped tags>64KiB,1025batches,row-count independence,zero/overflow,global concurrency,repeated UNION/selfjoin dynamic-owner highwater and success/error/cancel refunds.
- [x] Final-source engine/raw/boundary107, flow12, ingest10, native19, relevant integrations121 passed. Original raw13 preserved; both reproduced checkpoint-floor fixes pass unchanged. Independent borrowed/model rollup parity and materialized overlay/replay equality retained.
- [x] Strict default Clippy passed in clippy-02.log on final formatted bytes (copied as clippy-default-final.log, not rerun); fault-mode Clippy and fmt-final passed. Build profiles unchanged, jobs2, pinned baseline CLI/native, no bundled DuckDB build.
- [x] Source-only remote workdir, only19owned formatted files synced back. publication_gate, journal/WAL framing, nativeffi/startup, Cargo/profile bytes preserved. No benchmark/SDK/Docker/service/resource/credential/S3 mutation or local build/test/fixtures.

## Evidence and review

`public-baseline-red-03.log` is the exact final public fixture against complete frozen base: both empty scan (134217984 requested versus134217728 available) and admitted burst second-copy conversion fail. Final integrations run passes both unchanged assertions. The earlier red-01 attempt encountered incomplete native-source compilation and is explicitly NOT a behavioral red proof; all failure logs retained. Red-02 is an earlier valid behavioral fixture at2F; red-03 binds final3F envelope. Separate native first compile visibility and first Clippy failures are retained, followed by fixes and final green results.

Read-only Astra03c61a47 audited non-native transfer/pressure against the frozen source; no blocking correctness defect. Minor wording/coverage findings fixed or referred to Main for out-of-scope INGESTION docs. Native Sol169beab0 owned four native source/test files; parent integrated, checked drop ordering/capacities, fixed diagnostic visibility/lints and performed all final tests. Contour unavailable in current registry and its documented parser does not certify Rust. See review.md for exact audit boundary.

## Remaining qualification gaps

No independent partition-owner execution: existing global publication lease semantics deliberately remain. No distributed/production/RSS guarantee, native database pooling, deployed-service rebuild, SDK/live-S3 requalification, or matched-resource benchmark/capacity claim. General catalog/derived/control/SQL/file-authority metadata retains existing independent bounds; returned output and DuckDB arenas remain separate quotas. Main owns later integrated review, deployment and qualification. This task makes no new performance claim.

Shared `/workspace/varve-rebuild/target` is explicitly released; no child has Cargo rights or pending builds.
