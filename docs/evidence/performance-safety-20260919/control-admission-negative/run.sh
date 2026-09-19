#!/usr/bin/env bash
set -euo pipefail
root=/workspace/varve-rebuild
input="$root/performance-safety-20260919/control-negative-input"
work="$root/performance-safety-20260919/control-negative-v1"
evidence="$root/performance-safety-20260919/control-negative-v1-evidence"
export PATH=/root/.cargo/bin:"$root/baseline/.tools":$PATH
export CARGO_TARGET_DIR="$root/target" CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0
export CARGO_TERM_COLOR=never
exec 9>>"$root/cargo-owner.lock"
flock -n 9
test ! -e "$work"
test ! -e "$evidence"
mkdir "$work" "$evidence"
finish() {
  rc=$?
  printf '%s\n' "$rc" > "$evidence/supervisor.exit"
  date -u +%FT%TZ > "$evidence/finished-at.txt"
  flock -u 9
  exec 9>&-
}
trap finish EXIT
date -u +%FT%TZ > "$evidence/started-at.txt"
(cd "$input" && sha256sum -c transfer.sha256) > "$evidence/transfer-gate.log"
tar -xzf "$input/baseline-source.tar.gz" -C "$work"
(cd "$work" && sha256sum -c "$input/baseline.sha256") > "$evidence/baseline132.log"
cp "$input/control_admission_tests.rs" "$work/src/control_admission_tests.rs"
printf '\n#[cfg(test)]\n#[path = "control_admission_tests.rs"]\nmod control_admission_tests;\n' >> "$work/src/engine.rs"
cd "$work"
find . -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum > "$evidence/negative133.sha256"
find . ! -type f ! -type d -print > "$evidence/special-files.txt"
test ! -s "$evidence/special-files.txt"
rustc --version > "$evidence/toolchain.txt"
cargo --version >> "$evidence/toolchain.txt"
cargo clean --offline --locked -p varve-storage -p varve-client > "$evidence/package-clean.log" 2>&1 9>&-
printf '%s\n' 'cargo test --offline --locked -p varve-storage --lib engine::control_admission_tests::empty_new_table_aggregate_near_budget -- --exact --nocapture --test-threads=1' > "$evidence/negative.command.txt"
set +e
cargo test --offline --locked -p varve-storage --lib engine::control_admission_tests::empty_new_table_aggregate_near_budget -- --exact --nocapture --test-threads=1 > "$evidence/negative.log" 2>&1 9>&-
rc=$?
set -e
printf '%s\n' "$rc" > "$evidence/negative.exit"
sha256sum -c "$evidence/negative133.sha256" > "$evidence/source-after.log"
find "$CARGO_TARGET_DIR/debug/deps" -maxdepth 1 -type f -name 'varve-*.d' -print0 | xargs -0 grep -F "# env-dep:CARGO_MANIFEST_DIR=$work" > "$evidence/manifest-dir.log"
test -s "$evidence/manifest-dir.log"
find "$CARGO_TARGET_DIR/debug/deps" -maxdepth 1 -type f -name 'varve-*' -perm -u+x -print0 | sort -z | xargs -0 sha256sum > "$evidence/test-binaries.sha256"
test "$rc" -eq 101
grep -F 'running 1 test' "$evidence/negative.log" > "$evidence/selected-test.log"
grep -F 'apply committed control WAL' "$evidence/negative.log" > "$evidence/expected-failure.log"
grep -F 'derived resident/working byte budget exceeded' "$evidence/negative.log" >> "$evidence/expected-failure.log"
printf 'UNCHANGED_BASELINE_POSTCOMMIT_BUDGET_NEGATIVE_CONTROL_CONFIRMED\n' > "$evidence/verdict.txt"
cat "$evidence/verdict.txt"
