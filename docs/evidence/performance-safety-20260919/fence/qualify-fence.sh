#!/bin/bash
set -euo pipefail
root=/workspace/varve-rebuild/performance-safety-20260919
check="$root/check-fence.sh"
bash "$check" owned-epoch cargo test --locked --offline -p varve-storage --lib --features fault-injection owned_epoch_tests::
bash "$check" commit-boundary cargo test --locked --offline -p varve-storage --lib --features fault-injection commit_boundary
bash "$check" ingest-default cargo test --locked --offline -p varve-storage --lib ingest::
bash "$check" ingest-fault cargo test --locked --offline -p varve-storage --lib --features fault-injection ingest::
bash "$check" integrations-default cargo test --locked --offline -p varve-storage --test group_commit --test ingest --test ingest_flush --test ingest_traces --test journal_engine --test journal_remote --test remote_store
bash "$check" integrations-fault cargo test --locked --offline -p varve-storage --features fault-injection --test group_commit --test ingest --test ingest_flush --test ingest_traces --test journal_engine --test journal_remote --test remote_store
bash "$check" clippy-default cargo clippy --locked --offline --workspace --all-targets -- -D warnings
bash "$check" clippy-fault cargo clippy --locked --offline --workspace --all-targets --features fault-injection -- -D warnings
bash "$check" fmt cargo fmt --all -- --check
printf 'NARROW_FENCE_MATRIX_PASS\n' | tee "$root/fence-evidence/final-status.txt"
