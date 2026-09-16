# Final source review and regression record

The integrated source review found four concrete issues. An independent regression pass reproduced their boundaries against the corrected implementation. These are session-local source witnesses and executable probes, not an external security audit or production certification. Structural-tool exposure alone is not treated as full Rust language coverage.

| Finding | Correction | Executable evidence |
| --- | --- | --- |
| A 64 KiB opaque token could exceed its binding reservation after JSON escaping | `tier::binding_reservation_bytes` reserves the exact empty-token binding plus six encoded bytes per maximum raw token byte | Escape-heavy versioned tokens survive ship/reopen and post-CAS crash recovery |
| Invalid tokens after successful CAS could leave a healthy writer | The centralized CAS wrapper fences confirmed/ambiguous publication failures, including GC acquisition and release | Empty/oversized successful tokens and invalid reconciliation HEAD tokens fence writes and recover exact intent |
| Job alteration/drop could return an ordinary error after its authoritative mutation committed | `persist_committed_runtime` reports the committed sequence and fences after private-journal failure | Injected post-WAL ENOSPC survives reopen as the durable definition/drop |
| Failed checkpoint admission could strand unpublished segment outputs | Pre-publication failures clean only unreferenced, unpinned segment outputs; ambiguous manifest publication preserves files for recovery | Repeated tight-budget checkpoint failures do not leak outputs, even with an unrelated snapshot pin |
| S3 content-derived ETags could repeat after an identical payload/GC cycle | S3 and filesystem adapters share a fresh UUID/checksummed head envelope; legacy S3 payloads upgrade on their next CAS | S3 adapter wire-version/corruption unit test and same-payload CAS check in `cloud_probe` |

`tests/audit_regressions.rs` also injects a partial remote bulk deletion and verifies idempotent retry, current-head data, and lock recovery. Both GC lock and release token failures are covered. Run:

```sh
cargo test --locked --features fault-injection --test audit_regressions
cargo test --locked --lib remote::tests::s3_head_envelopes_prevent_aba_and_migrate_legacy_payloads -- --exact
scripts/verify.sh
```

Normal binaries do not honor fault-injection environment variables. Hardware power loss, extended outages, long-duration soak, arbitrary untrusted SQL isolation, cross-version rollback, and distributed failover remain separate qualification work. An upgraded S3 head envelope is not readable by older binaries: do not roll back onto that namespace without an explicitly tested migration.

See [AUDIT_RECONCILIATION.md](AUDIT_RECONCILIATION.md) for the delayed earlier audit, remaining source/performance gaps and limits of the existing coverage. `PRODUCTION_ACCEPTANCE.md` records the release gates; `OPERATIONS.md` covers recovery and alerts.
