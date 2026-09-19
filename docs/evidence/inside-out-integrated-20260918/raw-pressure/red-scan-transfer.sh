#!/bin/bash
set -eu
cd /workspace/varve-rebuild/raw-pressure
e=/workspace/varve-rebuild/raw-pressure-evidence
files='src/raw_memory.rs src/write_input.rs src/engine.rs src/append_overlay.rs src/commit_boundary.rs src/ingest.rs src/ingest_flow.rs src/owned_epoch_tests.rs src/raw_budget_tests.rs src/hot_path_tests.rs src/ingest_flow_tests.rs src/query/native.rs src/query/native/scan.rs src/query/native_tests.rs src/query/native_race_tests.rs'
mkdir -p "$e/pre-red"
for f in $files; do mkdir -p "$e/pre-red/$(dirname "$f")"; cp "$f" "$e/pre-red/$f"; done
restore() { for f in $files; do cp "$e/pre-red/$f" "$f"; done; }
trap restore EXIT
for f in $files; do cp "../owned-epoch/$f" "$f"; done
set +e
bash "$e/check.sh" public-baseline-red-03 cargo test --locked --features fault-injection --test raw_pressure
rc=$?
set -e
test "$rc" -ne 0
grep -q "admitted burst must not need second-copy credit" "$e/public-baseline-red-03.log"
grep -q "raw owned memory budget exceeded" "$e/public-baseline-red-03.log"
echo EXPECTED_BEHAVIORAL_BASELINE_FAILURES
