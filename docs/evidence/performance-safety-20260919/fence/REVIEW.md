# Static review checkpoint (not independent qualification)

Review method: bounded source tracing, exact text comparisons and manifest/diff inspection. Graph/contour providers were not available through tool discovery; no structural-tool certification is claimed. No compilation, tests, rustfmt or repository source execution occurred. Independent review is still required.

## Witnessed execution path

- Baseline engine.rs prepare_group_attempts obtains CommitLease, locks State, checks healthy, builds private pending publication, drops State and returns the owned lease with independently durable clocks/results.
- tier.rs ship_with_remote_gate captures under CommitLease, releases it before upload_ship/CAS. compare_and_swap_with_intent records its remote-publication fence under inner.state without CommitLease. Its outer fence_remote handler then tries to acquire CommitLease. RemoteState::still_current compares database ID, owner and token, not duplicate clock floors.
- Baseline PreparedEpoch::sync armed/published without rechecking health, and DurableEpoch::install took/installed publication under State without rechecking health. Thus preparation exclusion did not cover asynchronous remote fencing.
- Backport sync checks only Some(publication), via a closure owning the State guard. The closure ends before returning the owned Result, so both fail(false) and arm/publish occur without the State guard. Install checks healthy under its existing State guard before publication.take(); failure explicitly drops the guard before fail(true), avoiding recursive lock acquisition.
- Exact text comparison after subtracting the two added check blocks reproduces the whole baseline commit_boundary.rs. Therefore fail(), restore/install duplicate-floor calls, encoded/envelope field order, commit lease destruction order and successful install body are unchanged. No durable bytes are removed.
- Subtracting the three fault-test-only waiter additions reproduces the whole baseline publication_gate.rs. Default/non-test behavior, lock/arm/disarm/drop logic and poison semantics remain baseline. No partition production fence() method ported.

## Regression review

The FileStore wrapper permits initial publication, pauses the next captured ship's CAS and performs an actual competing CAS with a JSON whitespace suffix. The stale original CAS plus HEAD mismatch triggers tier's real fence. The fault-only waiter notification establishes that State was fenced before outer ship cleanup contends on the epoch lease; tests do not join ship while retaining that lease. No other commit contender exists until this observation. Sender/drain coverage queues later writes only afterward, so the waiter probe cannot be consumed by unrelated preparation.

Split-stage source asserts a mixed old/new/same-epoch provisional-success epoch before fencing, then exact old receipt preservation and errors for every new physical-sequence result. The timed floor oracle retains170 from the independent retry, not190 from a same-epoch duplicate. Exact WAL/journal directory+file maps cover namespace and bytes; the post-sync map must change and then remain unchanged through failure. Snapshot, sequence/generation, receipt/hot/metadata/accounting and private-credit assertions retain baseline semantics. No-publication coverage and real Ingestor sender/drain coverage are included for both modes. All inherited test/helper text is unchanged.

## Static evidence and limitations

- exact128 source set, only owned3 changes, full128/owned3 hash gates; originals/live-root13 and partition b73 reverified.
- symbols-and-scope.log confirms existing owned_epoch_tests registration and matching fault-test hook cfg.
- static-delta-review.txt mechanically confirms baseline remainders and no added synthetic fence, injected failure or sleep.
- git diff --no-index --check reported exit1 for differing trees and an empty whitespace diagnostic log. This is not a Rust formatting check.

No additional narrow source-level correctness issue identified. Compilation, Clippy/fmt, real assertions and liveness are not proven. A remote fence can arrive after sync's health observation, so publication may already have durable bytes; only installation is serialized against the State fence. An old independently durable duplicate can still succeed, and a commit installed before the fence is not retroactively revoked or prevented from later transport delivery. These intentional limits are not rollback, distributed or production guarantees. Remote permission and independent review remain mandatory.
