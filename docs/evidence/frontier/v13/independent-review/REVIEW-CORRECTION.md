# Focused S13 monitor correction re-review

## Verdict

**PASS for the single previously identified blocker.** The actual corrected reference-phase call is now `ready(['varve','timescale','driver'],300)`. Composed with the actual unchanged status implementation, it no longer advances while the same poll reports Varve `PENDING`; the original Varve-PENDING bypass is resolved. This is an offline, bounded verdict only and provides no live or activation authority.

## Witnessed correction

- The preserved and corrected `run-campaign.mjs` files differ by exactly one line: the reference-phase role set changes from `['timescale','driver']` to `['varve','timescale','driver']`.
- A non-writing execution of the new composed regression against the preserved old controller failed at `missing_active` as required: `1 !== 3` polls, demonstrating the old premature advance.
- The same regression against the corrected controller passed all five cases. It extracts the actual 300-second call from the controller and composes it with actual `status.mjs` source; cases cover missing active Varve, active/detail transitional Varve, immediate readiness, and 20 pending polls through an explicit 300-second fixture clock without advancement.
- `test-scope.mjs` restricts the campaign suffix delta to that one role-set strengthening. `verify-ready.mjs` binds the five-case receipt and helper hashes.

All 75 original checkpoint inputs were hash-verified with no mismatches. The original freeze remains `b75838c503f9ac7d02593437bcf970faeb23fd5a235171d972746792838d1a8a`. The permitted read-only checks passed: the revised freeze reports 78 files / 43 controls, and readiness reports 208 bound behavioral cases. The correction accounting is 72 affected cases rerun and 136 unchanged-source cases reused.

## SHA-256 bindings

- preserved controller: `c187704277b7850d6cf8815af98b6475832cfaebf8a5206a9d9f31cc33161d1d`
- corrected controller: `7ecd9a296ad85ecfc899d7e6d0dee114054a030ba6191c2ee2a4a629142bb0ca`
- unchanged status: `a84e1cf965f8252dc49de1d51d60a6a1856df7b4826591b3bf55917f659a796d`
- coupled regression: `0989b01b6fe50e5f5aab7fbb07d1e354a2fb90f90716ded7bae0a30362f73359`
- coupled receipt: `cb1f7aa1b6f8563f113164cdb2c3775e7498aab784c80f84f009ee8d897f5987`
- scope verifier: `2b8dbcae33b993b327e4f82692b19991aa135cd6198a9739839741fecfb2cb57`
- readiness verifier: `7c8a7f404116cd7aa2b3f45e9253fa166f21795fe52c6343be3915aaea38a14b`
- revised freeze (78 files / 43 controls): `cac74043b4f9c36e3a0f1d51268856502c2fd5d38c3a4330be856460f9f556b4`

## Remaining limitation

Nominal readiness deadlines can still overshoot when a poll begun before the deadline runs through the inherited 180-second helper timeout, including 60-second API-call timeouts. This predates the correction and is not resolved or broadened by it.

All compute remained off. No live authority, network, Railway, SSH, configuration, deployment, workload, source/frozen-receipt edit, fixture rerun, secret access, or live-state verification was performed.
