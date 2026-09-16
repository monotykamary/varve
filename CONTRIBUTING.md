# Contributing

Read [architecture](docs/ARCHITECTURE.md), [acceptance](docs/ACCEPTANCE.md), and [due diligence](docs/DUE_DILIGENCE.md) first.

Run `scripts/verify.sh` with DuckDB v2 available. Keep fixtures small and deterministic; use `TempDir`, explicit event-time clocks, real process crash tests for publication boundaries, and ordinary Rust integration tests. Never hide a corruption/replay failure by dropping data or relaxing an assertion. Add a regression test before changing a durability contract.

Treat format version, stable hash vectors, request-id semantics, retention cutoffs, immutable objects, snapshot pins and remote CAS as compatibility boundaries. Persist raw data, derived state and replay progress together. Do not change the wire/on-disk shape without a migration/version decision.

Keep production builds free of the opt-in `fault-injection` feature. Live S3 tests must remain explicitly enabled and isolated; never rely on ambient production credentials. No generic database performance claims from the small development workload probe.

Build frugally: use the configured two-job/debug-free profiles, do not bundle DuckDB source or introduce large generated assets, and do not commit `target/` or `.tools/`. After retaining a needed local executable, `cargo clean` can reclaim build artifacts; it affects only this project's target directory.

Use conventional commit messages (`feat:`, `fix:`, `test:`, `docs:`). Agent-assisted delegation in this project is restricted to Astra or Sol.
