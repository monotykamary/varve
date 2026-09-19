#!/bin/bash
set -euo pipefail
root=/workspace/varve-rebuild/performance-safety-20260919
cd "$root/fence"
e="$root/fence-evidence"
export PATH=/root/.cargo/bin:/workspace/varve-rebuild/baseline/.tools:$PATH
export CARGO_TARGET_DIR=/workspace/varve-rebuild/target CARGO_BUILD_JOBS=2
export VARVE_DUCKDB_CLI=/workspace/varve-rebuild/baseline/.tools/duckdb
export VARVE_DUCKDB_V2_LIBRARY=/workspace/varve-rebuild/baseline/.tools/duckdb-native-v2.0.0-alpha41533-linux-amd64/libduckdb.so
name=$1; shift
exec 9>/workspace/varve-rebuild/cargo-owner.lock
flock -n 9 || { echo 'Another Cargo owner is active'; exit 90; }
gate() {
  test "$(sha256sum "$e/source-files129.sha256" | cut -c1-64)" = 5be6ec980fe3304065ebd147c31f28997d50f11c82766e2c79e5a139f6cd52f7
  test "$(sha256sum "$e/owned-files.sha256" | cut -c1-64)" = 0f57da983ae979adb908b6cc05a74719954ef6050234ee979b07755bd8f1f94f
  sha256sum -c "$e/source-files129.sha256"
  sha256sum -c "$e/owned-files.sha256"
  test -z "$(find . -type l -print -quit)"
  find . -type f -printf '%P\n' | LC_ALL=C sort > "$e/$name.paths-$1.txt"
  diff -u "$e/source-files129.txt" "$e/$name.paths-$1.txt"
}
gate before > "$e/$name.source-before.log" 2>&1
printf '%q ' "$@" > "$e/$name.command.txt"; printf '\n' >> "$e/$name.command.txt"
set +e
timeout 1200 "$@" > "$e/$name.log" 2>&1
rc=$?
set -e
printf '%s\n' "$rc" > "$e/$name.exit"
gate after > "$e/$name.source-after.log" 2>&1
printf 'CHECK %s EXIT=%s\n' "$name" "$rc"
if [ "$rc" -ne 0 ]; then tail -70 "$e/$name.log"; exit "$rc"; fi
if [ "${1:-}" = cargo ] && [ "${2:-}" = test ]; then
  grep -E '^test result: ok\. [1-9][0-9]* passed;' "$e/$name.log" || { echo 'No meaningful test selection'; exit 91; }
else
  tail -4 "$e/$name.log"
fi
