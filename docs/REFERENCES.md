# Design references

Primary sources used to assess feasibility and select the architecture:

- [DuckDB v2 preview](https://www.duckdb.org/2026/08/17/duckdb-20-highlights): async I/O, extension/server evolution; this project pins an alpha rather than assuming a stable v2 release.
- [DuckDB Rust client](https://duckdb.org/docs/current/clients/rust/overview): native integration options. v0.1 deliberately uses the installed CLI to avoid a bundled C++ build; it is not zero-copy integration.
- [Apache Arrow Rust](https://github.com/apache/arrow-rs): typed in-memory arrays and Parquet implementation; used directly by Varve.
- [object_store](https://docs.rs/object_store/): provider-independent object API and conditional AWS writes; used directly.
- [AWS S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html): create/update preconditions, not unconditional overwrite.
- [AWS S3 Express append](https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-objects-append.html): append is specific to Express directory buckets; Varve does not assume it.
- [SlateDB introduction](https://slatedb.io/docs/get-started/introduction/) and [tuning](https://slatedb.io/docs/operations/tuning/): object-backed durability, batching and write-latency trade-offs. Architectural precedent, not a dependency.
- [S2 architecture](https://s2.dev/docs/platform/architecture): object-backed logs and multi-zone Express quorums. Architectural precedent, not a dependency.
- [GreptimeDB Mito](https://docs.greptime.com/contributor-guide/datanode/storage-engine/): time-series WAL/memtable/columnar region architecture. Comparison, not copied code.
- [Feldera/DBSP](https://docs.feldera.com/sql/intro/): general incremental SQL is a separate engine/problem; Varve currently implements constrained continuous aggregates.
- [DuckLake maintenance](https://ducklake.select/docs/stable/duckdb/maintenance/recommended_maintenance): snapshot/compaction/GC lifecycle comparison. Varve currently owns its simpler manifest protocol.

None of these sources independently certifies Varve's implementation. Behavior must be demonstrated by this repository's tests and the release qualification checklist.
