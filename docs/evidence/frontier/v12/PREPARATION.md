# S13 pilot offline preparation

Status: **prepared, fixture-checked, frozen; not activated**. Main review and separate authorization remain required. No performance, distributed, production, or remote-runtime claim.

Only `/tmp/varve-diagnostic.KOUrtH/retry-config/s13-pilot` was written. Mandatory first command verified the exact repository root. No repository/older-root edits, delegation, network, Railway, SSH, configure/deploy, actual workload, Cargo/npm, or commits occurred. Main's `CONTRACT.md`, `OWNER.json`, `before.json`, and `remote.mjs` were not edited; preflight was not rerun. No Main `PROGRESS.md` was created or changed. The existing `before.json` remains actual preflight evidence, not a fixture.

## Acceptance ledger

| Check | Offline evidence | Status |
| --- | --- | --- |
| Current qualified source | Existing S13 qualification `--check`: 86 unchanged inputs, 410 Rust + 19 TypeScript, one documented live-S3 ignore | pass; no suites rerun |
| Fresh stage/archive | `stage.mjs`, `verify-stage.mjs`: 56 build files, current source hashes, exact tar member bytes, generated manifest, Docker receipt COPY injection only | pass |
| Unchanged profile | Exact runtime bytes and actual trimmed environment payload SHA; semantic-equivalent compact JSON rejected | pass |
| Driver identity | Base v4 f548 / effective v11 23e kept distinct; current seven scope-file hashes checked; 21-test log and two review receipts copied verbatim | pass; 21-test result reused, not rerun |
| Activation order | First owned-upload status, Varve ready 1200s, probe + new gate, then two reference redeploys and ready 300s | actual controller VM pass |
| New Varve gate | New manifest/qualification, pinned config, fresh v2 zero frontier/empty tables and root, uid/region/caps, exact 31 phases, same DuckDB binary/version, different Varve binary from immutable v11 | actual probe + gate fixtures pass; live gate pending |
| Driver pipeline | Base probe/resources, nonroot reviewed installer, installed driver check, launch file/path/uid/gid attestation | offline fixtures pass; live installation pending |
| Ownership/cleanup | Exact upload cannot be overwritten by reference receipt; status checks owner and active identity; stop/verify use exact receipts; early abort removes only uploaded Varve; verify checks all three roles cold | actual helper VM pass |
| Workload/recovery/reporting | Existing workload rates, rows, SQL, budgets, ages, deadlines, retries, durability, drop/ambiguity rules and physical restart checks retained | source-delta review; real run pending |
| Phase analysis | Existing generic parser accepts 31 phases including append_accounting, rejects missing phase/reset/database mismatch | actual analyzer VM pass |
| Final freeze | 39 controls + source/profile/fixture/qualification receipts, 65 hashed entries | pass |

## Exact identities

All values below are SHA-256 unless identified as a source digest.

| Artifact | Digest |
| --- | --- |
| Qualified 86-input source digest | `58f405adf7d472fdccf0011bc0bcc3f041201f9792ec1a7ab372002b1803944a` |
| `../s13-qualification/qualification.json` | `6a45c542ed3d725ca9db9f2e5dc1947c98b1d98cac4c49d7cec3a1d49d3ff55d` |
| `stage/SOURCE_MANIFEST.json` and `source-manifest.json` | `f57157e947995785f0d226b2dd80791b100d32527418898e579d07962edb8a9d` |
| `source.tar.gz` | `f6ef42d8c50cedb0284ae4bc2f3c0292f2b9d8c5c76c1def0d7ed91c5107e918` |
| `runtime-config.json` / actual trimmed environment payload | `215f100b13ba1ba3d012d39117fa1895ed7978657434057a93390c6f359aa089` |
| `benchmark-config.json` including final newline | `dcb6608540fdfb6c5c0a4518dce871a06616ae7db8e88ff747e6ec602ffb0097` |
| Base driver `benchmark.py` | `f5488311247210b118410c155408a5be5af9ce2d28d6d480585a9c315125f637` |
| Effective driver `benchmark.py` (unchanged v11) | `23e05928cf287a4f8b59433f2079cf63610a256dabbb75138a2ea5081442df23` |
| Generated `install-driver.py`, 22,784 bytes | `956b530c6022fde691e96bdb2a9ea76c554af0a349199fc4a3acd413ca09862b` |
| `driver-scope.json` | `f10292c9602a9c2220a8a5792172708ece3d46b8fedabf33ef55526151271d97` |
| `control-tests.json` | `5f9be1b011d4c114b87b4522c4d8602b095edb79212f7ecb09a1583665ea7748` |
| `helper-freeze.json` | `bd3f1817fbb42c226e5b29ac3b78162fb76eb2f4194cd25073fdef988482d563` |
| Preserved Main `before.json` | `f01d765515aacb8c5770fbe3f0f0363be4bf2c71e86e60e10c21a0d4249e70c2` |
| Preserved Main `remote.mjs` | `d0893df64a359960442d443962b71bedd263ca4c7ba590678b676298b40d3f59` |

Stage directory: `/tmp/varve-diagnostic.KOUrtH/retry-config/s13-pilot/stage`. It is a new current-source context, not the v10/v11 archive. `prepare.mjs` now copies the generated stage manifest and records `database_source_changed: true`, `source_qualification_inputs: 86`, and `effective_driver_unchanged_vs_v11: true`.

## Changes and fixture cases

The current reporting helpers were copied into this root with run IDs `acct001` / `acct002`, data directories `accounting-s13`, and driver directory `/results/driver-accounting-s13`. Old helpers and receipts remain untouched. `generate-helpers.mjs` records the initial bootstrap only; the final reviewed helpers, not bootstrap regeneration, are authoritative. Generation/staging/preparation refuse an existing destination/receipt; do not rerun them to refresh this pilot.

`redeploy.mjs` uses the older two-reference-image design with latest removed images:

- Timescale: `87eb3575-ec4b-4c51-9c62-7eb5285ef41e`.
- Driver: `fd3d1f84-8efc-4d27-8296-a67733599d89`.

It requires the exact uploaded Varve deployment already active and SUCCESS before reference activation; it never redeploys an old Varve image or writes an upload receipt. Missing/mismatched old role ownership fails closed.

`probe-varve.py` adds an authenticated text `/metrics` read and exactly 31 fixed count-series phase labels. `check-varve.mjs` independently validates the new source/config/freshness/identity and rejects the v11 Varve binary; only DuckDB binary/version are required unchanged. No old-Varve image equality claim exists.

Fresh offline fixture results: **117 cases**, separate from the reused 21 driver tests:

- Summary: 13, complete/failed reports, missing readings remain null, invalid IDs rejected.
- Installer: 6, idempotent verified installation, wrong base, corrupted/incomplete destination, source/target symlinks.
- Driver attestation: 9, base/effective/scope hashes, uid/path, durability, existing tables, Python version.
- Cleanup: original 8, normal/abort, unfinished result, foreign active/owner, failed inactive build, existing intent, unrecorded compute.
- Configuration: original 2, actual readiness source accepts exact bytes and rejects compact semantic-equivalent JSON. Its own receipt is supplied only inside the VM to avoid a self-hash bootstrap cycle.
- Redeploy: original 8, two-role success, wrong image ID/caps/owner, existing intent, active reference, wrong returned owner, wrong active uploaded Varve.
- Controller: **8 instead of original 4**. Original success/parse failure/failed workload/wrong effective launch cases retained. Added early Varve probe failure, early Varve gate failure, root launch UID, wrong launch GID. First call/order, installation-before-check, failed-prefix cleanup, no stress/restart after failure are asserted. Fixture clock is explicit.
- New Varve gate: 26 actual-source VM cases. Positive and old binary, manifest/qualification, config bytes/values, CPU/memory, missing/extra/duplicate phases, metrics labels, sequence/hot/root freshness, tables, uid/region/directory, DuckDB binary/version, old database identity, fencing negatives.
- New Varve probe: 15 actual Python-source mock-I/O cases. Positive authenticated `/metrics` and the unchanged five local SELECT observations; manifest/config/caps/phases/freshness/root/uid/directory/region/HTTP-auth negatives. No real HTTP or subprocess ran.
- Exact ownership: 18 additional status/stop/verify cases, including uploaded BUILDING/SUCCESS, both references, foreign active/owner/ID, upload overwrite, missing active/response, mismatched cleanup receipt, and early Varve-only removal/verification.
- Phase analyzer: 4, positive 31-phase delta, missing phase, counter reset, different database.

All passed. Node syntax checks covered the final 28 `.mjs` files; Python `compile` covered 9 `.py` files without bytecode writes. The unchanged shell resource probe and installer template are hash-frozen. `verify-stage.mjs`, final `verify-ready.mjs`, and `freeze-helpers.mjs --check` passed. Review added explicit launch uid/gid checks and analyzer fixtures; only affected control/config readiness receipts were refreshed afterward. No failed check was ignored or softened.

Fixtures execute actual helpers with mocked I/O, in VM memory or cleaned temporary directories. Synthetic deployment/runtime objects never became live receipts. `runtime-config.json` is the requested offline profile, not runtime evidence. No `configured.json`, upload, deployment, campaign, runtime-probe, workload, status, or cleanup receipts were created. `helper-freeze.json` explicitly checked their absence at freeze time.

## Helper freeze and remaining Main operations

`helper-freeze.json` pins every helper/test/template plus stage/archive/profile, Main preflight, all local fixture receipts, verbatim driver review/test receipts, and S13 qualification files. `control-tests.json` additionally pins 26 campaign-related helpers; `verify-ready.mjs` binds actual Varve probe/gate, driver, installer, summary, ownership, configuration and analyzer fixture hashes. It invokes read-only current-source qualification and stage verification, not Cargo or a live probe.

Read-only final checks from the repository cwd:

```
node /tmp/varve-diagnostic.KOUrtH/retry-config/s13-pilot/freeze-helpers.mjs --check
node /tmp/varve-diagnostic.KOUrtH/retry-config/s13-pilot/verify-ready.mjs
```

Do not regenerate receipts or mutate frozen helpers without a new explicit review/fixture checkpoint. A source/profile/driver change invalidates this preparation. The non-Git private root was reviewed by exact helper source/deltas and executable fixtures, not by a repository-wide structural scan or independent reviewer.

Only Main, after review and separate activation authorization, may:

1. Verify the freeze/readiness and retain the original service, volume, domain and resource-cap constraints. Do not rerun frozen preflight or old controllers.
2. Run the cold `configure.mjs` gate: exact pretty runtime payload, new Varve/PG directories, `skipDeploys`, readback, all owned compute still cold. No values have been configured by this preparation.
3. Manually upload/build **this exact new `stage`** for Varve. No upload helper was added. Record the actual returned deployment ID as one JSON object with `deploymentId` in this root's `upload-varve.jsonl` (the helpers parse the whole file as JSON); do not put CLI chatter or a synthetic ID there.
4. Immediately start exactly one `run-campaign.mjs` supervisor for that upload, retaining its real PID/log. It waits for Varve (1200s), probes/checks it before activating references, redeploys the two pinned previous images, waits 300s, probes resources/driver, installs and attests nonroot effective driver, then runs the unchanged baseline/stress, restart fingerprints, and verified cleanup. Early Varve gate failure invokes abort cleanup for Varve alone. A partially submitted/unrecorded reference mutation is fail-closed and requires reconciliation, not blind retry.
5. Preserve all real failures/results and verify zero active owned compute. Do not rerun until favorable. Main may run the offline phase analyzer on actual boundary receipts after collection; overlap is not exclusive CPU time or per-query causality.

No new Varve binary or real runtime identity is available yet. Fixture success is not deployment success, durability certification, or a performance improvement.
