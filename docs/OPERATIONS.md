# Operator runbook

Varve is an experimental single-writer database. These procedures make an evaluation inspectable; they do not certify the DuckDB alpha dependency, hardware durability or a production SLO.

## Before accepting important data

- Choose an explicit workload: row/batch sizes, series/tag cardinality, time widths, raw and derived retention, request retry horizon and expected outage duration.
- Use one process and one persistent volume per database directory. Never run writable directory clones as replicas.
- Set a strong operator token, terminate TLS, and restrict access. The SQL subprocess has engine-level restrictions, not an OS sandbox for hostile tenants.
- Qualify the actual S3 endpoint with conditional creates/updates, stale-CAS rejection, immutable collision checks, bounded reads, pagination, bulk deletion and a restore drill. S3 Express/directory-bucket unordered listings are unsupported.
- Record the exact Rust/DuckDB versions, dependency audit, source artifact, configuration and observed deployment ID. Keep recoverable backups before changing versions. Downgrades or manually editing checksummed files are not migrations.

## Capacity and alerts

Scrape protected `/metrics` at a modest interval, such as 15 seconds. `/health` is liveness; `/ready` is a nonblocking availability hint and can return 503 while the state lock is busy. Neither probe certifies remote durability.

| Signal | Suggested response |
| --- | --- |
| `varve_fenced = 1` | Stop new work; preserve files and inspect authenticated status/logs. Do not delete WAL or bindings to force startup. |
| `varve_maintenance_failed = 1` or repeated job errors | Inspect `SELECT * FROM varve_jobs()` and `last_maintenance_error`; fix storage/configuration before retrying. |
| Growing `varve_unshipped_batches` / stalled `varve_remote_sequence` | Investigate object storage, ownership and scheduling. Stop ingress if the potential unshipped loss exceeds your policy. A sequence gap is not seconds or a promised RPO. |
| WAL, disk, metadata, receipt or group usage above a chosen threshold (for example 80%) | Reduce ingress or tune explicit retention/capacity. Check disk free space as well as Varve's logical budget. |
| Rising request timeouts/rejections or sustained CPU/RSS pressure | Lower client concurrency/batch/query scope; inspect queue limits, selected cold working set and platform metrics. |

Numeric gauges include `varve_wal_bytes`, `varve_disk_bytes`, `varve_hot_bytes`, `varve_metadata_bytes`, `varve_idempotency_keys`, `varve_rollup_groups`, both cache byte counts and active queries/snapshots. Compare them with the deployed configuration; no unbounded storage or hard RSS guarantee is implied. Infrastructure/container metrics also include query children and operational probe overhead.

Approximate retained group demand is the sum, over unique widths, of **distinct tenant/series/tag combinations × retained time buckets**. Named aliases sharing a width do not duplicate that state. Choose rollup retention explicitly; no default silently deletes historical summaries to make space. Time-window request IDs bound receipt history when enabled, but do not bound aggregate cardinality or raw segment references.

## Write failures and retries

A receipt marked `local_fsync` protects against process failure, not permanent loss of the local disk before shipment. On a transport error, the batch outcome may be ambiguous. Retry the **same request ID and identical payload**, within its configured horizon; never mint a new ID to hide an ambiguous outcome. An expired timed ID is rejected rather than silently reinserted. See `CONTROL.md` for policy cutover and monotonic-floor semantics.

Admission/ENOSPC failures are not permission to remove recovery files. Pause ingress, add/free capacity outside the live database or perform supported maintenance, then reopen if fenced. Reserve/intent files are protocol state, not disposable junk. See `PRODUCTION_DURABILITY.md`.

## Backup and restore drill

1. Stop ingress and let accepted work settle. Record counts/sums and the current sequence.
2. Checkpoint and ship; verify the confirmed remote sequence covers the intended recovery point.
3. Stop the old writer before transferring ownership. Do not run a second writer against the same namespace.
4. Restore into a **new, nonexistent directory** with the same appropriate configuration and remote namespace. Restored segments can remain cold; query them to verify content, not only the head pointer.
5. Recheck raw counts/sums, named aggregates, policies, job definitions and retry behavior. Record RTO, bytes fetched and the precise recovery point.
6. Treat a failed restore/remote maintenance lock as fail-closed. Preserve artifacts and follow `REMOTE.md`; do not blindly clear or time-steal locks.

The executable `varve-cloud-probe` exercises these mechanics in random child prefixes and never transfers the running evaluation's main namespace. `scripts/verify-stress.py` rechecks a prior synthetic report read-only; it proves restart recovery only when paired with an independently observed process/service restart.

## Upgrade and incident evidence

Keep the old executable and a quiescent backup, test the new version against a restored copy, rerun negative SQL/security probes, then replace the single writer and verify its state. A backup made by casually copying an actively mutating directory is not an atomic snapshot. Checksums detect corruption; they do not defend against an attacker who controls all storage credentials and rewrites a coherent object graph.

Preserve deployment IDs, source/config hashes, redacted logs, checkpoint/remote sequences, exact failing request IDs and idempotency deadlines. Never include operator tokens or S3 keys in issue reports. `docs/RAILWAY.md` contains the isolated evaluation scope and cost caveats; workspace spending limits were not changed.
