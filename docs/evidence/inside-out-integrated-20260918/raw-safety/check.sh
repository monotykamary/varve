#!/bin/bash
set -euo pipefail
cd /workspace/varve-rebuild/raw-safety-repair
export PATH=/root/.cargo/bin:/workspace/varve-rebuild/baseline/.tools:$PATH
export CARGO_TARGET_DIR=/workspace/varve-rebuild/target CARGO_BUILD_JOBS=2
export VARVE_DUCKDB_CLI=/workspace/varve-rebuild/baseline/.tools/duckdb
export VARVE_DUCKDB_V2_LIBRARY=/workspace/varve-rebuild/baseline/.tools/duckdb-native-v2.0.0-alpha41533-linux-amd64/libduckdb.so
e=/workspace/varve-rebuild/raw-safety-repair-evidence
name=$1; shift
find . -type f | LC_ALL=C sort | xargs sha256sum > "$e/$name.source-before.sha256"
set +e
"$@" > "$e/$name.log" 2>&1
rc=$?
set -e
find . -type f | LC_ALL=C sort | xargs sha256sum > "$e/$name.source-after.sha256"
cmp "$e/$name.source-before.sha256" "$e/$name.source-after.sha256"
printf 'CHECK_EXIT=%s LOG=%s\n' "$rc" "$e/$name.log"
tail -100 "$e/$name.log"
exit "$rc"
