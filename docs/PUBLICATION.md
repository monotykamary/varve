# GitHub publication acceptance

This checkpoint publishes Varve as an **experimental single-node project**, not a production-certified database. The earlier runtime and live-cloud evidence remains in [EVALUATION.md](EVALUATION.md); no new cloud deployment, stress run or production qualification is implied by documentation and repository publication.

| Check | Required evidence | State |
| --- | --- | --- |
| Local verification | `scripts/verify.sh`: format, strict Clippy, fault-enabled Rust tests, mixed workload, local restore protocol drill, Python tests, shell syntax and RustSec | passed: 138 Rust tests, 14 Python tests, both workload/protocol probes, format/Clippy, shell syntax and RustSec (275 dependencies; no findings) |
| README and identity | Branded, accessible SVG; real CI badge; runnable clone/CLI/SQL/restore instructions; local links and anchors resolve | passed: 57 links/anchors checked; cover rasterized and visually inspected; normal-binary two-row CLI/SQL/aggregate/checkpoint/ship/cold-restore probe matched exact results |
| Public metadata | GitHub description/topics, Apache-2.0 license, Cargo repository/readme metadata | passed: public [monotykamary/varve](https://github.com/monotykamary/varve), main branch, 11 topics, quick-start homepage; GitHub recognizes Apache-2.0 |
| Publication hygiene | Review staged files for secrets and generated/data/build artifacts; no credentials in source | passed: 75 staged files; Gitleaks 8.30.1 reported no leaks; build/tools/secrets and every quick-start data directory are ignored |
| Hosted CI | GitHub Actions verification and dependency audit on the published commit | passed: [run 35040563477](https://github.com/monotykamary/varve/actions/runs/35040563477), both verify and audit jobs, source commit `24686cb943e301c556169d94a3c7083b8a14dc35` |
| Profile | Add Varve under standalone projects in `monotykamary/README.md`; preserve unrelated content; push normally | passed: exactly one added project line, pushed as [8f36a8b](https://github.com/monotykamary/monotykamary/commit/8f36a8b773cc3f4137591bf2c2935503c55b13d5) |

The GitHub-rendered README and accessible static SVG were also inspected. Gitleaks scanned the exported staged tree, not ignored local artifacts. Its checksum-verified v8.30.1 scan found no leaks; this is not proof that secrets can never be introduced. Contour disclosed unsupported Rust/Python coverage, so the existing source review and executable tests—not a zero-finding structural score—provide the correctness evidence.

The initial acceptance ledger is explicitly historical. Passing tests does not close the remaining [production gates](PRODUCTION_ACCEPTANCE.md) or [audit gaps](AUDIT_RECONCILIATION.md).
