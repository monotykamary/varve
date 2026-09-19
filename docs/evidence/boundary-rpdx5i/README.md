# Boundary redesign evidence

Two **old-source** Railway pilots and a subsequent **local-only** resident-hit setup fix. No overall Timescale win, sustained qualification of this candidate, live-S3, HA or power-loss claim.

- `p1-report.json`, `p2-report.json`: original hash-verified benchmark reports, opposite initial orders, all correctness phases passed and zero dropped/failed work.
- Matching logs/outcomes and `p*-postload.json`: original phase output and healthy postload status/metrics.
- `cleanup-*.json`: exact-owned removals independently reconciled; original app baseline unchanged. Data/volumes preserved.
- `resident-hit-{before,after}-{1,2}.json`: exact local release results, 1,024 rows, twenty checked samples/case, six cases; before→after→after→before. Not a cloud result.
- `resident-hit-probe-admission.md`: rejected larger baseline fixtures, deliberately disclosed.
- `resident-hit-{tests,integration,workspace,clippy-fault,clippy-default}.log`: real local checks. Workspace totals include nested crash-worker executions; live S3 is intentionally ignored.
- `local-inputs.json`: current local Rust/SDK source, tests, examples and configuration hashes. Not an independent review or deployment authorization.
- `manifest.json`: exported artifact hashes, before/after binary hashes and measurement commands. Original credential bytes were checked absent before copying; private scope snapshots and approvals were not exported.

The cloud source digest is `c064a7f0b0f35309501a1356564469f33f4c21efce653c354145f95be80f1d60`. The later local fix has different source/binary identities and must not inherit cloud qualification. See [the acceptance ledger](../../BOUNDARY_REDESIGN.md) for conditions and limitations.
