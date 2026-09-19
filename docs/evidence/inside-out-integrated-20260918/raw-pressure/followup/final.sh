#!/bin/bash
set -euo pipefail
e=/workspace/varve-rebuild/raw-pressure-evidence/followup
check=$e/check.sh
(cd /workspace/varve-rebuild/raw-pressure && sha256sum -c ../raw-pressure-evidence/source-files.sha256) > "$e/frozen-before-final.log"
bash "$check" engine-final cargo test --locked --lib --features fault-injection engine::
bash "$check" flow-final cargo test --locked --lib --features fault-injection flow::
bash "$check" ingest-unit-final cargo test --locked --lib --features fault-injection ingest::
bash "$check" native-final cargo test --locked --lib --features fault-injection query::native
bash "$check" integrations-final cargo test --locked --features fault-injection --test raw_pressure --test audit_regressions --test engine --test group_commit --test headroom_recovery --test prefix_admission --test prefix_checkpoint --test prepared_publication --test publication --test rebuilt_engine --test journal_engine --test ingest --test ingest_flush --test ingest_traces
bash "$check" clippy-default-final cargo clippy --locked --all-targets -- -D warnings
bash "$check" clippy-fault-final cargo clippy --locked --all-targets --features fault-injection -- -D warnings
bash "$check" fmt-final cargo fmt --all -- --check
(cd /workspace/varve-rebuild/raw-pressure && sha256sum -c ../raw-pressure-evidence/source-files.sha256) > "$e/frozen-after-final.log"
printf 'FINAL_MATRIX_PASS\n' > "$e/final-status.txt"
