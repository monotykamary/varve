# Retained Railway benchmark evidence

See [the comparison report](../../TIMESCALE_BENCHMARK.md) for results and limitations. This directory contains small synthetic reports, not database files or credentials.

- `railway001.json`: transport-failed first attempt; partial timings are not a completed benchmark.
- `railway002.json`: complete passing 550,000-row baseline with raw samples and independent oracles.
- `railway003.json`, `preparation-probe.json`: zero-row generator failure and blocking/offloaded read-only repro.
- `railway004.json`, `stress-observed.json`: projected-checkpoint admission rejection and per-backend observed rows; no completed 20,000/sec phase.
- `railway002.verify.json`, `recovery-before.json`, `recovery-after.json`: retained-oracle restart verification and unchanged raw/aggregate fingerprints without refresh.
- `varve-source-v*.json`, `runtime-driver*.json`, `infrastructure.json`: immutable source hashes, driver runtime/artifact identity, limits and restart requests. Database cgroup cap values are transcribed live-probe observations; raw driver cgroups are also in reports.
- `*-metrics-railway*.json`, `varve-resources-after.txt`: coarse platform time series and Varve's pre-restart cgroup counters. Peaks can span more than one run; do not infer compression ratios or per-query CPU.
- `removed-deployments.json`, `cleanup.json`: three exact deployment IDs removed; two test volumes retained; original service unchanged.
- `ledger.json`: final acceptance outcomes. `launcher.json` records the last unprivileged verification launch, not every earlier launch.

`MANIFEST.json` hashes the 28 retained evidence files, excluding this explanatory README and the manifest itself. Runtime Varve/PG credential values were checked absent. The image/config and driver hashes distinguish attempts; do not combine them into a fictional single successful run. Small-sample p99 values are not production tail guarantees.

Validation: 17 offline runner tests and 28 affected Rust transport/security tests passed; workspace formatting and strict Clippy passed after the transport change. Contour reports Rust/Python as unsupported with coverage gaps, so its zero findings are not an approval.
