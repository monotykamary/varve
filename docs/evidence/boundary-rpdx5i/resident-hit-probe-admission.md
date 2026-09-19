# Diagnostic admission

The old query probe chooses rollup widths1/64/256microseconds and a64MiB derived budget. Both32768 and8192-row baseline attempts were rejected by check_derived_checkpoint_headroom before producing a measurement. The candidate binary had not run. This is high-cardinality admission in the unchanged baseline, not evidence of a candidate regression or a measured slow result.

Do not weaken the production headroom guard. The matched fixed-cost diagnostic uses1024rows,20iterations/case, identical before/after flags in before→after→after→before order. Mechanical real-worker call counts are the primary no-op evidence; this small diagnostic cannot establish large-database or Railway throughput.
