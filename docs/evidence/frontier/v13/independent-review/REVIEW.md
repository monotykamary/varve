# Frozen S13 monitor repair review

## Verdict

**BLOCKED for pilot activation by one operational readiness defect.** The status helper implements the requested two-observation semantics, but the controller can ignore a current `PENDING` Varve result during the reference readiness phase and advance beyond that gate. This is a bounded monitor/controller issue, not a Rust or query proposal.

No other blocker was demonstrated in the reviewed scope. This review provides no live authority and does not authorize configuration, activation, deployment, workload, cleanup, or retry.

## Blocker: reference readiness omits Varve

`status.mjs:47-52` evaluates every recorded deployment and emits `READY` only when the exact deployment detail and its exact active entry are both `SUCCESS`; an owned missing/transitional active observation becomes `PENDING`. But `run-campaign.mjs:15-24` evaluates only the roles passed by its caller, and the post-redeploy call at `run-campaign.mjs:63` passes only `timescale` and `driver`.

Reproducible path:

1. All three exact deployment details report `SUCCESS`.
2. The snapshot reports exact Timescale and driver active entries as `SUCCESS`, while the exact Varve active entry is temporarily absent (the allowed nontransactional transitional case).
3. Actual `status.mjs` output is `{varve:PENDING,timescale:READY,driver:READY}`.
4. Actual `ready(['timescale','driver'],300)` returns immediately and the controller proceeds to runtime probes/installation despite the same poll explicitly saying Varve is not ready.

The isolated mocked VM probe at `status-ready-probe.mjs` composes the actual status source into the actual controller `ready()` source. It passed and reproduced the bypass: references-only returned without sleeping; requesting all three roles waited. No external command, source write, receipt write, or infrastructure mutation occurs in the probe.

Existing coverage does not catch this path: `test-monitor.mjs:74-76` makes the driver, not Varve, pending during reference convergence, while its full-campaign fixtures return all roles ready. A correction needs a regression that feeds actual status output with Varve `PENDING` and references `READY` into the reference-phase controller gate. No source correction was made by this review.

## Confirmed code and frozen evidence

- **Raw evidence and fatal guards:** `status.mjs:11-24` journals API stdout before parsing, parsed envelopes before GraphQL rejection, and the snapshot before workspace/service/domain/cap/volume validation. `status.mjs:44-54` journals deployment detail before ownership/state checks and keeps foreign, multiple, unrecorded, owner, terminal, and unknown states fatal. `status-latest.json` is written only after validation.
- **Readiness rule:** `status.mjs:46-52` allows only transitional states plus `SUCCESS`; exact detail `SUCCESS` plus one exact active `SUCCESS` is the sole `READY` case. Missing active, active transitional/detail success, and the reverse are `PENDING`.
- **One-shot exact image reuse:** `reuse-varve.mjs:34-50` checks the frozen handoff, requires cold owned services and the exact removed first-S13 Varve deployment `a88e2c35-596f-41d4-b383-06857bf43108`, records an exclusive intent before one `usePreviousImageTag:true` mutation, rejects reused/foreign/terminal returned IDs, and records the real returned activation ID. No build path is present.
- **Gate before references:** `run-campaign.mjs:59-63` waits for Varve, captures the real runtime probe, runs `check-varve.mjs`, and only then invokes reference redeployment. `check-varve.mjs:9-37` binds the exact first-S13 binary and manifest, rejects v11 and either prior database UUID, requires the fresh zero-frontier v2 root, pinned profile/resources, and exactly 31 phases.
- **Unchanged workload:** `scope-tests.json` binds 18 adapted helpers and byte-compares the timed workload and campaign suffix against frozen first S13 after only root/namespace/run/receipt/image-ID adaptations. No workload retry or relaxed workload limit was introduced.
- **203 fixture receipts reviewed, not regenerated:** 13 summary + 6 installer + 9 driver attestation + 8 cleanup + 2 configuration payload + 8 redeploy + 8 controller + 29 Varve gate + 15 Varve probe + 18 ownership + 4 analysis + 49 monitor + 25 reuse + 9 cold configuration = 203. The monitor receipt covers the transitional/fatal status matrix, raw retention, convergence, 1200/300-second nonconvergence, and no launch after readiness failure, but has the coupling gap above.
- Permitted read-only checks passed:
  - `freeze-helpers.mjs --check`: 42 controls, 75 files, zero network calls.
  - `verify-ready.mjs`: 203 behavioral fixtures reported, 21 reused driver tests, 86 qualification inputs, `compute_started:false`, `performance_qualified:false`.

## SHA-256 binding

- `status.mjs` — `a84e1cf965f8252dc49de1d51d60a6a1856df7b4826591b3bf55917f659a796d`
- `run-campaign.mjs` — `c187704277b7850d6cf8815af98b6475832cfaebf8a5206a9d9f31cc33161d1d`
- `reuse-varve.mjs` — `0222148f78e9d11d25ce300e148151b89283a1a769fe2d278491ae87c958c7b9`
- `configure.mjs` — `70b3c309c1b1792f565d02bae841ff3a80a7b55d1d4d25ba49a0824a762618d9`
- `check-varve.mjs` — `bbc2e3b052ce119d907514f2f76af97cb4b2a6b8656fb4116935277d5e0ed7bd`
- `stop-attempt.mjs` — `eca90b597d11b192362a4f3597dc8fc5e424121bccec5fcd1841fbc8bfe655ed`
- `freeze-helpers.mjs` — `7b45d7cad069bdf731e052e865fd9f842b3c43000867705007463224d9cca494`
- `verify-ready.mjs` — `33583282c0b19a86961c2ea46794c7fb38bca9c7370bfea9815cd91dccc788ac`
- `helper-freeze.json` — `b75838c503f9ac7d02593437bcf970faeb23fd5a235171d972746792838d1a8a`
- `status-ready-probe.mjs` — `81019d5b03c73053098f25a377e15825fa72646f3ea42b27dceffa2b488644c3`

## Constraints and limitations

The first failed pilot poll's raw snapshot/detail is absent. The retained error identifies the old driver's guard branch, but not the exact observed states or historical cause; the split-read race is independently reproduced only and is not claimed as that cause.

Nominal readiness deadlines can overshoot because a poll begun before the deadline may run through the inherited 180-second helper timeout (with 60-second API-call timeouts), and readiness can be accepted after that in-flight call returns. This predates the repair and is recorded as an inherited limitation, not a new regression.

No network, Railway, SSH, real configuration/deployment/workload, build, Cargo/npm, secret access, delegation, fixture regeneration, repository edit, older-root edit, commit, or live-state verification was performed. `OWNER.json` was not touched. The supplied all-compute-off state was not independently rechecked live.
