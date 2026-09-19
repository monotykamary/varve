#!/bin/bash
set -euo pipefail
cd /workspace/varve-rebuild/owned-epoch
e=/workspace/varve-rebuild/owned-epoch-evidence
cp src/engine.rs "$e/engine-before-red.rs"
trap 'cp "$e/engine-before-red.rs" src/engine.rs' EXIT
# Remove only the early-failure repair in this candidate, never edit frozen input.
sed -i '/resolve_terminal_durable(&s, &requests, &mut durable);/d' src/engine.rs
if bash "$e/check.sh" reviewer-red cargo test --locked --lib --features fault-injection initial_pressure_checkpoint_failure; then
    echo 'ERROR: regression did not catch the removed repair'
    exit 1
fi
grep -q 'called.*unwrap.*Err.*outside the idempotency window' "$e/reviewer-red.log"
echo 'EXPECTED_RED_OBSERVED'
