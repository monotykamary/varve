# v12 — S13 built and attested; monitor halted before workload

**Not a performance result.** Neither `acct001` nor `acct002` launched. The last measured comparison remains [v11](../v11/README.md); no improvement or regression follows from this attempt.

## Witnessed sequence (2026-09-17 UTC)

- 04:29:27: uploaded the newly qualified S13 source as deployment `a88e2c35-596f-41d4-b383-06857bf43108`.
- 04:38:37: the actual new Varve binary passed source, configuration, fresh-root, fixed-31-phase, UID, region, resource and unchanged-DuckDB attestation.
- 04:38:51: submitted the pinned reference images as Timescale `83232bed-bae0-4503-8195-23a6a527ca9e` and driver `f0bcb2d9-3faf-46c0-97bf-5b27807dc6ef`.
- 04:39:01: `status.mjs` rejected `Unexpected active deployment driver`; the supervisor halted before driver installation or any workload.
- 04:39:35: cleanup's fresh observation contained exactly the three owned SUCCESS deployments.
- 04:40:10: all three were verified REMOVED, zero benchmark deployments active, original application unchanged, volumes retained and resource caps unchanged.

## What is and is not known

The status helper performs separate snapshot and deployment-detail API reads. Its predicate rejects a SUCCESS detail when the active view is absent or not yet SUCCESS, as well as foreign/multiple active deployments. Treating all such lifecycle differences as fatal is unnecessarily strict: owned transitions can safely remain pending within the existing deadline, without allowing workloads to start early or weakening ownership checks.

**The failed poll was not retained.** `status-latest.json` is the preceding Varve-only successful observation. The exact predicate branch, failed active IDs/statuses, and incident cause cannot be reconstructed. Later cleanup convergence does not prove which condition occurred. No synthetic replacement is supplied. A separate offline repair must journal observed states before rejection, distinguish readiness from ownership, and preserve terminal/unknown/foreign-state rejection.

## Qualification and provenance

- S13 source: `58f405adf7d472fdccf0011bc0bcc3f041201f9792ec1a7ab372002b1803944a`, 86 selected source/config/test inputs.
- 410 Rust checks and 19 TypeScript checks; one live-S3 test ignored. Eighteen TS units reuse an exact-source-verified prior log; the actual-service test reran. Formatting and strict default/all-feature workspace lint passed.
- Runtime binary: `9366f866b775d56ebf827d540b9fd62e845950f2f15004e6c7be62299ec3f628`.
- Runtime source manifest: `f57157e947995785f0d226b2dd80791b100d32527418898e579d07962edb8a9d`, 56 build files.
- Profile remains `215f100b13ba1ba3d012d39117fa1895ed7978657434057a93390c6f359aa089`; effective driver remains `23e05928cf287a4f8b59433f2079cf63610a256dabbb75138a2ea5081442df23`. The effective driver was prepared offline but never installed in this attempt.
- 117 offline control fixtures passed before deployment. Those fixtures did not cover the transitional-readiness case; they are not a runtime certificate.

The archive includes actual receipts, frozen controls, the build archive, the 86-input local qualification archive and logs. Known deployment token/password values were checked absent before publication. Historical helpers contain original absolute paths and must not be activated from this archive.

Verify stored hashes, source archives, qualification counts and actual incident/cleanup facts without network or database load:

```sh
node docs/evidence/frontier/v12/witness/verify.mjs
```

No comparative samples, restart-after-workload proof, distributed guarantee, production-readiness claim or performance win is asserted.
