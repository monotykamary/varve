# Resume only the missing stress diagnostic

Acceptance ledger:
1. No DB source/profile changes, no rebuild, no baseline rerun. Validate the parent's frozen qualification and exact removed image provenance. Parent baseline raw report and aborted-controller evidence stay intact.
2. Fresh operational receipts here; source/runtime/source archive remains parent. Preflight all owned services stopped and original untouched. Redeploy exact three removed v9 images with usePreviousImageTag=true, same retained volumes/paths.
3. Verify actual source/config/binary identity, v2 root, non-root Varve, resource caps, region, driver bytes, PostgreSQL/Timescale versions and all durability settings. Do not claim an empty DB. Recreate only minimal hash-bound baseline recovery reference on ephemeral driver; never overwrite a differing reference. Verify retained 550k raw/aggregate/exact-timestamp common fingerprints before loading stress.
4. Stress is only prefix002: unchanged 1M seed, batch1000/writers4,30sec20k/s offered,30query samples,600sec remote hard deadline. Retain all samples, errors, drops and oracle results. Capture phase metrics BEFORE local unpacking; test the unpacker using the actual preserved baseline fixture before any deploy.
5. After stress, verify both namespaces at their exact committed frontiers, restart both DB processes on the same volumes and prove process identity changed plus exact fingerprints preserved. Preserve original baseline recovery fingerprint before/after stress too.
6. Always attempt exact-owned cleanup on success/failure, read back REMOVED and zero active compute. Retain volumes/original service. Unknown submission/cleanup outcomes require read-only reconciliation, never blind mutation retries.

This is a resumed diagnostic pilot with an explicit intervening restart, NOT a fresh uninterrupted acceptance trial and NOT proof of overall performance. Baseline already misses the target and is retained; future changes require new qualification and fresh matched trials. Never use this resume to conceal the controller error or cherry-pick results. No local load/performance work.
