# Confirmed control-admission negative control

**Expected baseline failure, not a passing repair.** No production code is changed by this checkpoint.

The original qualified132 source inputs (`f7de1894…628d`, unchanged through `11080f9`) were extracted into a fresh Railway sandbox directory. The frozen v1 regression file was added unchanged, with only its test-module registration appended to `engine.rs`. `negative133.sha256`, the before/after gates, exact Cargo manifest-dir and test-binary hashes record the resulting inputs and execution identity.

Rust/Cargo1.98.1, jobs2, offline dependencies and the existing shared target were used. Only Varve package artifacts were cleaned on the source-root switch; no duplicate dependency target was created. No local build, test or fixture ran.

Result: compilation succeeded; exactly one test ran; Cargo exited101 with **apply committed control WAL / derived resident/working byte budget exceeded**. The supervisor exited0 only after checking that exact failure and the source/binary gates. This was not a compilation, fixture-setup or algebra failure.

The reached case used **no native query backend**, a legacy flat WAL and an unpaged root: 128 rollup groups, 37 receipts, 230,164 derived resident bytes and about49KB of files. It reached sequence40/checkpoint39 and fenced after the control commit. The first case panics, so this is not evidence that all four loop combinations ran.

This independently reproduces the inherited control/index-admission defect seen in the live comparison. The candidate repair, additional ownership regressions and full qualification remain pending. `control_admission_tests.rs` is an evidence fixture, not newly registered production-tree coverage; its other tests were not selected here.

`run.sh` preserves the actual supervisor, and the logs, exits and manifests are retained alongside it. Remote and retrieved `negative.log` SHA-256 both equal `4544e66f503d1c26c6ce61356bbd2ee55a8ef3c209da69098a86359d9c507371`; the negative source manifest equals `9da54ffa1e1fc0c0387d2e2cf9dc5039804e29ac177650d7502530da6a083c88`.

At wrap-up, the recovered benchmark still returned readiness HTTP200. No Cargo/compiler process or Cargo lock holder remained. No new deployment, benchmark retry, quota increase or namespace deletion was performed.
