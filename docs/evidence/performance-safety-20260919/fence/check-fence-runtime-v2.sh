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
  test "$(sha256sum "$e/source-files-final.sha256" | cut -c1-64)" = f884d3592e240c77ef4458ca8fbac073cbc972bdc736e2ec0e6637b3071a359c
  test "$(sha256sum "$e/owned-files-final.sha256" | cut -c1-64)" = 91b3caec81b4cf809a48d35c4786f68268e671e1d87b37e20caf122a50a87935
  sha256sum -c "$e/source-files-final.sha256"
  sha256sum -c "$e/owned-files-final.sha256"
  test ! -L .tools
  test "$(find . -type l -print)" = ./.tools/duckdb
  test "$(readlink .tools/duckdb)" = /workspace/varve-rebuild/baseline/.tools/duckdb
  test "$(sha256sum .tools/duckdb | cut -c1-64)" = 1dd0a1596505613a439dced4fb5f5800471badec82b2dc3b2c0efbd46fafbf6d
  test "$(sha256sum "$VARVE_DUCKDB_V2_LIBRARY" | cut -c1-64)" = 69bdd44e0d2426e7ba44ed14644b54d8bd99a9703e5cbb4aec3f8beef40f817d
  find . -type f -printf '%P\n' | LC_ALL=C sort > "$e/$name.paths-$1.txt"
  diff -u "$e/source-files129.txt" "$e/$name.paths-$1.txt"
}
gate before > "$e/$name.source-before.log" 2>&1
printf '%q ' "$@" > "$e/$name.command.txt"; printf '\n' >> "$e/$name.command.txt"
set +e
timeout 1200 "$@" 9>&- > "$e/$name.log" 2>&1
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
