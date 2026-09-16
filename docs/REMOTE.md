# Remote storage and recovery

Remote protection is optional. Local batches acknowledge after local WAL fsync, not after S3. A remote head becomes authoritative only after its checksummed checkpoint and explicitly contiguous WAL tail are uploaded. A background interval is not a durability deadline during an outage.

## Filesystem adapter

`--remote-dir /path/to/objects` uses the same immutable-object/CAS-head abstraction as S3. Files are atomically published, checksummed and fsynced; head CAS is process-safe with revision-unique opaque tokens. Local and remote roots must not overlap or nest. A filesystem remote on the same disk does not protect against losing that disk.

## S3 adapter

```sh
export VARVE_S3_BUCKET=your-bucket
export VARVE_S3_PREFIX=varve/your-database
export VARVE_S3_REGION=us-east-1
# Optional: VARVE_S3_ENDPOINT=https://objects.example.net
# Credentials/role configuration use object_store's AWS provider chain.
./target/debug/varve --data ./varve-data --s3 ship
```

`VARVE_S3_PREFIX` is a safe relative prefix with no leading/trailing slash or empty/`.`/`..` components. A bucket root is allowed when omitted; use a dedicated prefix to isolate ownership and garbage collection. `VARVE_S3_ALLOW_HTTP=true` is required for `http://` endpoints. Conditional create/update is mandatory; no unconditional last-writer-wins fallback exists. Verify your provider supports those semantics.

For Railway buckets whose credentials report `urlStyle=virtual-host`, `object_store` 0.14.2 requires the bucket name to be included in the endpoint while virtual-hosted style is enabled:

```sh
export VARVE_S3_BUCKET=<bucket>
export VARVE_S3_REGION=auto
export AWS_VIRTUAL_HOSTED_STYLE_REQUEST=true
export VARVE_S3_ENDPOINT=https://<bucket>.<returned-host>
```

This differs from path-style configuration: with `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` unset/false, the endpoint excludes the bucket (`https://<returned-host>`) and the client places the bucket in the request path. Keep access keys in Railway's private variables and use the AWS credential variables supported by `AmazonS3Builder::from_env`; no Varve-specific credential variables are required.

Requests use explicit 5-second connect and 30-second request timeouts plus a bounded retry policy. Builtin reads check metadata before allocation, enforce bounded streaming and exact lengths. Direct reads/writes have a 128 MiB format cap; core reads narrow the cap to the reference's expected size. Custom RemoteStore adapters must override `get_bounded` for equivalent allocation safety rather than relying on its post-validation fallback.

No Express append feature is required. Express One Zone alone does not provide multi-AZ durability. No credentials are embedded in head objects, checked into this repository or automatically provisioned.


## Head wire versions

Both builtin adapters wrap each head mutation in a checksummed UUID-versioned envelope, including an identical logical payload. S3 still uses its opaque ETag/version token for conditional requests, but the fresh envelope prevents content-derived ETag ABA when a GC lock is released. `RemoteStore::head()` returns the original logical payload, not envelope bytes. S3 reads legacy bare heads and upgrades them on the next successful CAS; envelope corruption fails closed. Older binaries that cannot decode the envelope must not be rolled back onto an upgraded namespace.

## Bounded listing and deletion

`RemoteStore::list_page(prefix, after, limit)` returns keys in strict lexicographic order. `limit` must be `1..=1000`. When `next` is present, pass it unchanged as the exclusive `after` cursor on the next call; `next` is the last key returned, not the first omitted key. A cursor need not still exist, so stale cursors remain valid lexical anchors, but it must be a safe key inside the requested prefix. Pagination is not a snapshot: concurrent insertion of a key at or before the cursor is not revisited.

Builtin adapters retain only `limit + 1` candidate keys. `FileStore` scans the subtree with a depth-only directory stack and a bounded top-k heap, so memory is `O(limit + path depth)` rather than `O(objects)`; each page still scans the subtree and costs approximately `O(objects log limit)`. S3 uses the provider's exclusive offset stream and stops after `limit + 1` user keys. Cursor safety therefore requires a provider that emits strict lexicographic order. Standard general-purpose S3 buckets provide that order; directory buckets/S3 Express and S3-compatible providers that do not guarantee it are unsupported. The adapter rejects any non-increasing order it observes rather than returning a cursor that could silently skip keys.

The legacy `list(prefix)` API remains available, but builtin adapters fail with an error directing callers to `list_page` once a result exceeds 10,000 keys. A custom adapter's default `list_page` implementation calls `list` and is therefore not allocation-safe; custom adapters must override it for bounded operation and strict ordering.

`delete_batch(keys)` accepts at most 1,000 fully validated keys. Its default implementation performs a bounded idempotent loop. S3 passes one bounded stream to `object_store`, which uses S3 `DeleteObjects` when the provider supports bulk deletion. Any short, reordered, or failed provider result is an error; some earlier keys may already have been deleted, so retry the complete failed batch idempotently. A successful return is the number of requested keys processed, including keys already absent under idempotent S3/filesystem deletion semantics.

## Layout

- `segments/<blake3>.parquet`: immutable raw data.
- `manifests/<blake3>.bin`: schemas, raw-file references, rollups, receipts and cutoffs.
- `wal/<sequence>-<blake3>.wal`: committed operations above that checkpoint.
- Adapter-owned control head: conditional version, database/owner identity, checkpoint, WAL tail and optional maintenance lock.

The local binding must match the remote head. Unknown/conflicting heads fence the old publisher rather than overwrite history. Unconfirmed uploads do not reduce the reported recovery gap.

## Restore transfers ownership

```sh
./target/debug/varve --data ./new-directory --remote-dir ./objects restore
```

The destination must not exist. Restore acquires a remote CAS lock, verifies manifest/WAL hashes and continuity, and holds the local process lock through installation/replay. It then releases the remote lock with a new publisher identity. Segments are fetched/verified on demand. Do not keep an old copy active as a second writer.

Local eviction is allowed only for segments belonging to a successfully published remote checkpoint. WAL shipping and local eviction are separate schedules.

## Publication crash recovery

Before normal publication and GC CAS, Varve durably records a checksummed exact-head intent and physically reserves binding storage. Reopen validates the canonical root, database, owner, predecessor and sequence, then adopts only the exact intended remote bytes. This covers a successful **first** publication with no prior binding and preserves later locally acknowledged WAL. It does not infer ownership from a UUID or abandon the namespace to mask an ambiguous result.

When no exact pending intent applies, older-binding reconciliation still requires a proven prefix, including retained request/control history and monotonic cutoffs; insufficient proof or a divergent clone fails closed. Preserve intent/reservation/binding files when diagnosing a failure. `tests/publication.rs` and `first_publication_crash_recovers_the_exact_namespace_and_preserves_local_tail` exercise these boundaries. See [publication and ENOSPC details](PRODUCTION_DURABILITY.md).

Restore and administrative abandoned-lock recovery are separate operator workflows, not covered by this normal open-database intent protocol. A crashed restore may leave an explicit lock requiring the procedure below; never assume automatic failover or silently steal it.

## Vacuum and abandoned locks

Vacuum protects the current head and dependencies, not arbitrary historical snapshots. On versioned object stores, deleting a key can leave noncurrent versions or provider backups: Varve does not securely erase those. Configure noncurrent-version retention separately without deleting live current objects. It excludes restore with a fail-closed CAS lock and coordinates with publishing so it cannot delete in-flight upload dependencies. The persisted maintenance job also vacuums when shipping is due, even without raw expiration; `vacuum-remote` runs it explicitly. Do not configure external bucket lifecycle deletion of live Varve objects.

Abandoned restore/GC locks have no automatic timeout. Inspect with:

```sh
./target/debug/varve --data ./unused --remote-dir ./objects remote-head
```

Owned interrupted GC can resume through `vacuum-remote` when its local binding survives. Otherwise, **stop and confirm the old owner is dead**, then:

```sh
./target/debug/varve --data ./unused --remote-dir ./objects recover-remote-lock \
  --owner <lock-owner-uuid> --confirm-owner-stopped
```

This fences previous publishers; restore into a new directory afterwards. Breaking a live operation's lock invalidates safety. This is operator recovery, not automatic failover.

## Live conformance

The ignored-by-default test requires explicit credentials, permissions and a test bucket; it uses a random child prefix and can incur charges:

```sh
VARVE_LIVE_S3_TEST=true cargo test --test remote_store \
  live_s3_contract_uses_an_isolated_random_prefix -- --ignored --exact
```

The initial implementation pass did not access a live bucket or infer live-S3 certification from filesystem tests.
