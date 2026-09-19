# Independent monitor review correction

The original Sol review (`../s13-monitor-review/REVIEW.md`) found one blocker: the reference-phase call checked only Timescale and driver, allowing advancement when the same actual status output said Varve PENDING. Main accepted and corrected that defect before any configuration or activation.

## Preserved checkpoint

All 75 original frozen inputs and original freeze `b75838c503f9ac7d02593437bcf970faeb23fd5a235171d972746792838d1a8a` are preserved under `../s13-monitor-review-checkpoint/`, with byte-hash manifest. The original independent review/probe are unchanged. The revised freeze is a new explicit review checkpoint, not a silently replaced historical certificate.

## Smallest correction

`run-campaign.mjs` now calls `ready(['varve','timescale','driver'],300)` after reference submission. No other executable controller line changed. Status semantics, previous-image reuse, prior attestation, 300/1200-second budgets, SQL, workload rates/rows/ages, durability and cleanup remain unchanged. Nominal readiness deadlines retain the previously documented in-flight API timeout overshoot; no stricter wall-clock guarantee is claimed.

The new `test-readiness-coupling.mjs` executes actual status source and extracts the actual reference call from the controller rather than inventing a role list. Five cases cover both directions of split-read transition, missing active Varve, immediate readiness and nonconvergence. The first execution against the original controller failed as expected: `missing_active ... 1 !== 3`. After correction all five pass, including 20 pending polls and exactly 300 seconds of explicit fixture clock with no advancement. The old review probe remains a demonstration of why a references-only caller is unsafe, not a regression test of the corrected call site.

Updated scope assertions permit only this explicit strengthening in the campaign suffix; no unrelated equality assertion was removed. Full readiness now requires the new five-case receipt and source bindings. Main reran 72 affected behavioral cases and static scope checks; 136 unchanged-source cases remain reused, for 208 bound cases. Source qualification remains 86 inputs / 410 Rust / 19 TypeScript, with no suites rerun.

## Exact revised sources

- Controller: `7ecd9a296ad85ecfc899d7e6d0dee114054a030ba6191c2ee2a4a629142bb0ca`
- Readiness verifier: `7c8a7f404116cd7aa2b3f45e9253fa166f21795fe52c6343be3915aaea38a14b`
- Coupled fixture: `0989b01b6fe50e5f5aab7fbb07d1e354a2fb90f90716ded7bae0a30362f73359`
- Unchanged status: `a84e1cf965f8252dc49de1d51d60a6a1856df7b4826591b3bf55917f659a796d`

No live operation or benchmark result follows from these checks. Focused independent re-review is required before activation. No failed historical poll was reconstructed.
