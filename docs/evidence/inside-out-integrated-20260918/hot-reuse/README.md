# Hot database reuse feasibility — pinned C diagnostic

`run01/` preserves source, pin checks, compiler output, all results, exit status and resource boundary. Library/header hashes match the pinned `v2.0.0-alpha41533` artifacts. Source SHA-256: `26fead24c8d6126a04aaac3022060ee238875155f7db6b1eab8d11fa4c70e502`.

The isolated Railway probe passed fresh-connection checks for prior TEMP view, macro, variable and query-log absence. Recreating the same view name returned the new value. Empty file allowlists blocked reading the probe's existing source file; configuration locking blocked enabling external access or widening the allowlist. Thread settings remained fixed.

Eight fresh-connection + scalar-query + disconnect observations on one reused database ranged from approximately 0.34 to 0.51 ms. These are **not Varve query latency, matched-resource benchmarking or capacity results**: the sandbox reported eight online CPUs, no readable cgroup ceilings, and concurrent Cargo work could exist.

No production pooling was implemented. This probe does not test Varve scanner epochs, stale bind resurrection, borrowed-source lifetimes, callback allocation quotas, cancellation handoff or active-plus-idle worker limits. Those remain mandatory before reuse. Cold-file scope changes remain excluded: no supported locked allowlist/cache-revocation proof was established. No services, credentials, buckets or data volumes were changed.
