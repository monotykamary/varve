# Independent raw/native review — deployment blocked

Candidate manifest `5244039ae491f1ccd3481b630967b7127435572cf35a775e13e21ad26b5b33e3` (128/128 verified); baseline `86c317d4e103be5f69e55de0852885ecd9ae4f6be5d064a00c0c45d7db538c5d` (125/125 verified). The later `0658…` status/test follow-up does not repair these paths. This was read-only review, not a test rerun.

## Native allocation release ordering

Witness: frozen `src/query/native/scan.rs:247-266,333-344,399-411,794-817,1341-1348`.

Each UserData/BindState/GlobalState/LocalState Box contains its own reservation, which refunds while destroying the value, before Box frees the backing storage. The fixed reservation inside Arc<ScannerScratch> likewise drops before its own charged Arc allocation. Last-field placement protects preceding fields, not the enclosing allocation. End-state refund tests do not observe this interval.

Keep reservations outside the allocations they guard, or extract the guard, deallocate the payload, then refund on every destruction and pre-handoff failure path. Fixed credit must outlive all guarded scanner allocations. Any scope exclusion must be explicit and bounded. Add deterministic cleanup-time observations, not just final counters.

## Encoded frame survives credit release

Witness: frozen `src/engine.rs:1463-1520`, `src/write_input.rs:88-127`.

Fallible consuming materialization can drop a taken/split input reservation while the encoded WAL frame is alive. Error/unwind local destruction order also releases remaining envelopes before the earlier encoded local. Establish explicit frame/envelope ownership before materialization, retain the frame portion across failures, and free the frame before returning its credit. Add post-encode error/panic tests observing occupancy during cleanup and preserving floor/duplicate/no-publication semantics.

## Untested capacity hypothesis

Fixed metadata reserves planned Vec capacities, then checks actual capacity after allocation. No allocator over-capacity failure was demonstrated. Resolve guaranteed requested layouts versus Vec-reported capacity and excluded allocator overhead; post-allocation checking alone is not preallocation proof. Do not call this a reproduced failure.

Recorded tests and Clippy/fmt remain valid historical evidence, but do not prove these windows absent. No silent acknowledged-data loss was demonstrated. Deployment is held; narrow repairs take precedence over the partition feature.
