#!/bin/bash
set -u
cd /workspace/varve-rebuild/owned-epoch
export PATH=/root/.cargo/bin:/workspace/varve-rebuild/baseline/.tools:$PATH
export CARGO_TARGET_DIR=/workspace/varve-rebuild/target CARGO_BUILD_JOBS=2
export VARVE_DUCKDB_CLI=/workspace/varve-rebuild/baseline/.tools/duckdb
export VARVE_DUCKDB_V2_LIBRARY=/workspace/varve-rebuild/baseline/.tools/duckdb-native-v2.0.0-alpha41533-linux-amd64/libduckdb.so
log=/workspace/varve-rebuild/owned-epoch-evidence/$1.log
shift
"$@" > "$log" 2>&1
rc=$?
echo "CHECK_EXIT=$rc LOG=$log"
tail -65 "$log"
exit "$rc"
