# S13 monitor retry — offline preparation

Status: **prepared and frozen for Main review; no activation authorized or performed**. This document is included in `helper-freeze.json`; the final `freeze-helpers.mjs --check` and `verify-ready.mjs` results are the handoff gate. The independent reference-gate blocker was corrected by Main at a new preserved checkpoint; see `MONITOR_REVIEW_RESOLUTION.md`. The corrected gate awaits focused re-review before activation. Live activation/runtime/workload verification remains outstanding, not implied by fixtures.

Only author-owned files under `/tmp/varve-diagnostic.KOUrtH/retry-config/s13-monitor` were written. The mandatory first command verified the exact repository root. Main's `CONTRACT.md`, `OWNER.json`, `remote.mjs`, `before.json`, and `PROGRESS.md` were not edited. Fresh actual `before.json` was preserved; preflight was not rerun. No repository/older-root edits, network, Railway, SSH, configuration, deployment, real workload, Cargo/npm, delegation, or commits occurred. Main independently preserved first-S13 evidence as v12; that concurrent repository change was not this author's work.

## Acceptance ledger

| Check | Evidence | Result |
| --- | --- | --- |
| Exact source/profile/driver unchanged | Qualification `--check`, `verify-stage.mjs`, verbatim receipt checks | Pass: 86 qualified inputs, prior 410 Rust + 19 TS results reused, no suites rerun |
| Pinned previous image, not rebuild | `reuse-varve.mjs`, 25 actual-source VM cases | Pass offline; one Varve-only `usePreviousImageTag:true` mutation, cold/caps/ownership/config/one-shot intent gates |
| Raw observation before every reject | `status.mjs`, monitor fixtures | Pass: stdout before JSON parsing, envelope before GraphQL errors, snapshot before workspace/service/caps/domain/volume/original guards, detail before deployment guards |
| Nontransactional readiness | 35 status cases + 6 API-wrapper cases | Pass: READY requires exact recorded deployment detail SUCCESS and exactly its active ID SUCCESS; owned transition/missing active is PENDING, terminal/unknown/foreign/multiple/unrecorded/owner errors remain fatal |
| Explicit bounded controller readiness | Original 8 controller cases + 8 new readiness cases | Pass: no raw SUCCESS shortcut; convergence waits, 1200-second Varve and 300-second reference nonconvergence abort without workload, early cleanup retained |
| Early image attestation | 29 gate and 15 actual-probe cases | Pass offline: exact first-S13 binary/manifest, different from v11, fresh UUID distinct from both prior databases, fresh zero-frontier v2 root, exact 31 phases, original config/caps/uid/region/DuckDB |
| Cold configuration | 9 actual configure VM cases + original 2 readiness payload cases | Pass: exact profile bytes, new paths, skipDeploys/readback/cold checks, existing intent cannot be silently retried |
| Driver, workload, durability, cleanup unchanged | 18 normalized helper comparisons; exact controller workload and post-readiness flow comparisons; inherited fixture suites | Pass: no SQL/rates/rows/ages/caps/retry/deadline/durability/drop-or-ambiguity changes |
| Final helper bindings | `verify-ready.mjs`, `helper-freeze.json` | Readiness passed; freeze binds all controls/tests/receipts, this document, source/profile and qualification files |

## Historical failure: what is and is not known

Read first-pilot actual PREPARATION/PROGRESS, status/controller sources, last successful status, upload/reference receipts, campaign error/log, cleanup request/response/verified proof, and actual runtime attestation. The 04:39:01 failure poll was not saved by the old helper. Its precise raw active/detail state and failed predicate branch remain unknown. The 04:38:31 retained status shows Varve only; later cleanup observations cannot reconstruct the missing poll. First S13 ran no workload; no comparative result exists.

The split-read race is independently reproduced with synthetic fixtures, not asserted to be the proven historical incident cause. A SUCCESS detail with missing or DEPLOYING owned active state, or SUCCESS active with DEPLOYING detail, now yields PENDING. Neither ID ownership nor terminal-state guards were weakened.

## Changes and evidence handling

Actual first-pilot helper sources were copied, not regenerated from bootstrap. Namespaces are `accounting-s13-r1`, `/results/driver-accounting-s13-r1`; runs are `acct101` and `acct102`. `generate-helpers.mjs`, old `prepare.mjs`, and build-producing `stage.mjs` are deliberately absent. Copied `stage/`, archive and manifest are **unchanged first-S13 image bindings only: do not upload or build them**. `prepare-reuse.mjs` is a one-shot offline provenance check, already executed; do not rerun it over the prepared receipt.

`PRIOR-s13-runtime-varve.json`, `PRIOR-s13-staged.json`, and `PRIOR-s13-prepared.json` are byte-exact prior evidence, explicitly not retry runtime/deployment receipts. Runtime binary/source are unchanged versus first S13 and changed versus v11. The installer destination changed, but its embedded effective driver payload did not. The 21-driver-test log, AST/scope receipt and both review receipts remain verbatim; the 21 tests were not rerun.

The future status journal is `status-observations.jsonl`, append-only by this helper, with timestamp, poll UUID, kind and exact obtained raw observation. Each API stdout/envelope, parsed snapshot/detail and transport failure bytes is retained before its corresponding validation. Early snapshot/GraphQL/transport failure leaves only observations actually obtained; no detail state is invented. `status-latest.json` remains a convenience for validated polls, never the sole evidence. Errors propagate, not swallowed into PENDING. Failure to write evidence also fails closed. No power-loss durability or transactional API-snapshot guarantee is claimed.

`reuse-varve.mjs` records an exclusive activation intent before its single mutation, raw actual response before returned-owner/state validation, and `activation-varve.json` only for a valid returned new owned deployment ID. All status/cleanup/restart consumers now use that receipt, not an invented upload receipt. Ambiguous mutation or invalid returned ownership requires manual reconciliation from intent/response; never blindly retry or manufacture an ID. The controller creates its start marker exclusively and performs Varve attestation before activating the two references. Varve-only early-abort cleanup still removes only the exact recorded owned retry deployment.

## Exact pins and digests

Previous REMOVED deployment images, verified again by the future guarded helpers before mutation:

- Varve: `a88e2c35-596f-41d4-b383-06857bf43108`.
- Timescale: `83232bed-bae0-4503-8195-23a6a527ca9e`.
- Driver: `f0bcb2d9-3faf-46c0-97bf-5b27807dc6ef`.

SHA-256 unless labeled source digest:

| Artifact | Digest |
| --- | --- |
| Qualified source digest | `58f405adf7d472fdccf0011bc0bcc3f041201f9792ec1a7ab372002b1803944a` |
| Runtime Varve binary, exact first S13 | `9366f866b775d56ebf827d540b9fd62e845950f2f15004e6c7be62299ec3f628` |
| Source manifest | `f57157e947995785f0d226b2dd80791b100d32527418898e579d07962edb8a9d` |
| Archive, copied unchanged | `f6ef42d8c50cedb0284ae4bc2f3c0292f2b9d8c5c76c1def0d7ed91c5107e918` |
| Runtime profile / actual trimmed environment payload | `215f100b13ba1ba3d012d39117fa1895ed7978657434057a93390c6f359aa089` |
| Base driver | `f5488311247210b118410c155408a5be5af9ce2d28d6d480585a9c315125f637` |
| Effective driver, unchanged v11 | `23e05928cf287a4f8b59433f2079cf63610a256dabbb75138a2ea5081442df23` |
| PRIOR runtime receipt | `b7c3d431399b272c69fb8cca2e99f715db5bda4c0aab2ffeb63ca0e25de8d55a` |
| Main fresh before.json | `b3449f0bacc18609e73337113e97a92bb03aaed8d57f7764c7913fbcf460ccd1` |
| Main remote.mjs | `9e623b7ad56961a7f17e0beacb9b567e0306b956504cae36b84b691b0928dbad` |
| status.mjs | `a84e1cf965f8252dc49de1d51d60a6a1856df7b4826591b3bf55917f659a796d` |
| run-campaign.mjs | `7ecd9a296ad85ecfc899d7e6d0dee114054a030ba6191c2ee2a4a629142bb0ca` |
| reuse-varve.mjs | `0222148f78e9d11d25ce300e148151b89283a1a769fe2d278491ae87c958c7b9` |
| check-varve.mjs | `bbc2e3b052ce119d907514f2f76af97cb4b2a6b8656fb4116935277d5e0ed7bd` |
| verify-ready.mjs | `7c8a7f404116cd7aa2b3f45e9253fa166f21795fe52c6343be3915aaea38a14b` |

Full per-file digests, including the freeze verifier itself and every test source, are in `helper-freeze.json`. Its own digest is printed by the read-only check below rather than self-referenced here.

## Offline test record

**208 behavioral fixture cases passed**, separate from the reused 21 driver tests and static scope assertions:

- Inherited 117 adapted honestly: summary 13, installer 6, driver attestation 9, cleanup 8, readiness payload 2, reference redeploy 8, controller 8, Varve gate 26, probe 15, ownership 18, phase analyzer 4.
- Varve gate adds 3 (other binary, first-S13 UUID reuse, malformed UUID): gate now 29.
- Monitor adds 49: 35 status cases, 6 actual API-wrapper cases, 8 full-controller cases. Includes exact raw evidence on early cap/project/volume failures, no invented detail after early failure, malformed response/partial transport preservation, convergence and bounded nonconvergence.
- Varve reuse adds 25: exact pin/cold/caps/config/owner/intent, invalid returned response and ambiguous mutation.
- Cold configure adds 9 actual-source cases.
- Main's review correction adds 5 actual status-to-reference-call composition cases: missing active Varve, active Varve DEPLOYING, Varve detail DEPLOYING, immediate all-role readiness, and bounded 300-second nonconvergence while both references stay READY. The new test failed against the old controller (1 poll instead of the required 3), then passed after the one-line role-set strengthening.
- At that checkpoint, Main reran 72 affected behavioral cases (49 monitor + 16 controls + 2 exact configuration + 5 new coupling), plus the normalized scope assertions and full readiness check. The other 136 cases remain bound to unchanged source; no Rust or driver suites were rerun.

The old ownership `missing_active` rejection fixture is now a positive PENDING case, not discarded. Foreign, multiple, unrecorded, owner, terminal, unknown, caps, original and cleanup guards still reject. Full controller timeout fixtures witness 80 polls over 1200s and 20 over 300s using explicit clocks, no workload launch, and abort cleanup. All network/process execution inside these behavioral fixtures is mocked. Python tempfile fixtures stay under this root and clean up; bytecode writes are disabled.

Commands executed before freeze (root abbreviation below):

```
root=/tmp/varve-diagnostic.KOUrtH/retry-config/s13-monitor
node "$root/prepare-reuse.mjs"                    # one time; already done
node "$root/verify-stage.mjs"
# Each actual-source Node fixture was executed:
# test-summary, test-attestation, test-cleanup, test-controls, test-varve,
# test-ownership, test-analysis, test-monitor, test-reuse, test-configure,
# test-scope, test-configuration (.mjs)
PYTHONDONTWRITEBYTECODE=1 python3 -B "$root/test-installer.py"
PYTHONDONTWRITEBYTECODE=1 python3 -B "$root/test-probe-varve.py"
node "$root/verify-ready.mjs"
```

The original 31 Node helper/test modules passed `node --check`; Main separately syntax-checked the five changed/new modules at the correction checkpoint; now 32 Node modules exist. all 9 Python files passed in-memory `compile`; `resources.sh` passed `bash -n`. Exact normalized comparisons cover 18 inherited controls and unchanged timed workload; the campaign comparison permits only the reviewed addition of Varve to the existing 300-second reference readiness gate. Direct old/new source-delta review covered the coherent changed controls. This non-Git root was not represented as a repository Contour review or an independent review. No failed assertion was weakened to pass. Fixture scripts write only fixture receipts; do not rerun them over a frozen handoff without a new review checkpoint.

## Main review and later operations

Read-only handoff checks (safe now; no fixture regeneration):

```
node /tmp/varve-diagnostic.KOUrtH/retry-config/s13-monitor/freeze-helpers.mjs --check
node /tmp/varve-diagnostic.KOUrtH/retry-config/s13-monitor/verify-ready.mjs
```

Only Main, after review and separate infrastructure authorization, may proceed in this order:

1. Retain the frozen before/original/volume/domain/cap constraints; do not rerun preflight or old controllers.
2. Run this root's `configure.mjs` once while all compute is cold. It uses exact profile bytes, `/data/probes/accounting-s13-r1` and `/var/lib/postgresql/data/accounting-s13-r1`, skipDeploys and readback.
3. Run this root's `reuse-varve.mjs` once. It rechecks freeze/readiness and records the real activation ID/response. **No upload, rebuild, or fabricated upload receipt.** If ambiguous, stop for reconciliation.
4. Start exactly ONE this-root `run-campaign.mjs` supervisor, preserving its real PID/log. It waits for explicit Varve READY up to 1200s, probes and gates the exact first-S13 binary/manifest/fresh UUID before references, then redeploys pinned TS/driver images, waits up to 300s for all three roles including Varve, attests/installs the unchanged driver, and follows the unchanged workload/restart/cleanup flow. No additional timed-workload retries or deadlines were added.
5. Preserve every raw journal and failure/result, and verify all owned compute off. Do not rerun until favorable. Partially submitted or unrecorded deployments require manual reconciliation, not weakened ownership guards.

No current activation, runtime, status, workload, or cleanup receipts were generated. The final freeze refuses their presence. The PRIOR receipt is historical only. No throughput, latency improvement, durability certification, distributed, production, or performance claim follows from this preparation. Stop here for Main review.
