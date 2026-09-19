#!/bin/bash
set -euo pipefail
root=/workspace/varve-rebuild/performance-safety-20260919
e="$root/fence-evidence"
export PATH=/root/.cargo/bin:/workspace/varve-rebuild/baseline/.tools:$PATH
export CARGO_TARGET_DIR=/workspace/varve-rebuild/target CARGO_BUILD_JOBS=2
export VARVE_DUCKDB_CLI=/workspace/varve-rebuild/baseline/.tools/duckdb
export VARVE_DUCKDB_V2_LIBRARY=/workspace/varve-rebuild/baseline/.tools/duckdb-native-v2.0.0-alpha41533-linux-amd64/libduckdb.so
for variant in prearm install; do
  (
    exec 9>/workspace/varve-rebuild/cargo-owner.lock
    flock -n 9
    test ! -e "$root/negative-$variant"
    mkdir "$root/negative-$variant"
    (cd "$root/fence" && tar -cf - .) | tar -xf - -C "$root/negative-$variant"
    cd "$root/negative-$variant"
    sha256sum -c "$e/source-files-final.sha256" > "$e/negative-$variant.base.log"
    cp "$root/negative-$variant.rs" src/commit_boundary.rs
    while IFS= read -r f; do sha256sum "$f"; done < "$e/source-files129.txt" > "$e/negative-$variant.source.sha256"
    printf '%s\n' 'cargo test --locked --offline -p varve-storage --lib --features fault-injection real_remote_cas_fence_before_sync_and_after_sync_preserves_only_old_receipts -- --test-threads=1' > "$e/negative-$variant.command.txt"
    set +e
    timeout 600 cargo test --locked --offline -p varve-storage --lib --features fault-injection real_remote_cas_fence_before_sync_and_after_sync_preserves_only_old_receipts -- --test-threads=1 > "$e/negative-$variant.log" 2>&1
    rc=$?
    set -e
    printf '%s\n' "$rc" > "$e/negative-$variant.exit"
    sha256sum -c "$e/negative-$variant.source.sha256" > "$e/negative-$variant.after.log"
    test "$rc" -eq 101
    grep -q 'test result: FAILED. 0 passed; 1 failed;' "$e/negative-$variant.log"
    grep -q 'panicked at src/owned_epoch_tests.rs:' "$e/negative-$variant.log"
    printf 'NEGATIVE_CONTROL_%s_EXPECTED_ASSERTION_FAILURE\n' "$variant"
    grep -E 'panicked at|assertion failed|test result:' "$e/negative-$variant.log"
  )
done
check="$root/check-fence-final.sh"
bash "$check" workspace-fault-final cargo test --locked --offline --workspace --all-targets --features fault-injection -- --test-threads=2
bash "$check" clippy-default-final cargo clippy --locked --offline --workspace --all-targets -- -D warnings
bash "$check" clippy-fault-final cargo clippy --locked --offline --workspace --all-targets --features fault-injection -- -D warnings
bash "$check" fmt-final cargo fmt --all -- --check
printf 'FENCE_FINAL_MATRIX_PASS\n' | tee "$e/final-status.txt"
