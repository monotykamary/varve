#!/bin/bash
set -euo pipefail
root=/workspace/varve-rebuild/performance-safety-20260919
cd "$root/client-benchmark"
e="$root/client-benchmark-evidence"
export PATH=/root/.cargo/bin:/workspace/varve-rebuild/baseline/.tools:$PATH
export VARVE_TEST_BINARY=/workspace/varve-rebuild/target/debug/varve
export VARVE_DUCKDB_CLI=/workspace/varve-rebuild/baseline/.tools/duckdb
export VARVE_DUCKDB_V2_LIBRARY=/workspace/varve-rebuild/baseline/.tools/duckdb-native-v2.0.0-alpha41533-linux-amd64/libduckdb.so
name=$1; shift
sha256sum -c "$e/source.sha256" > "$e/$name.source-before.log"
printf '%q ' "$@" > "$e/$name.command.txt"; printf '\n' >> "$e/$name.command.txt"
set +e
timeout 500 "$@" > "$e/$name.log" 2>&1
rc=$?
set -e
printf '%s\n' "$rc" > "$e/$name.exit"
sha256sum -c "$e/source.sha256" > "$e/$name.source-after.log"
printf 'CHECK %s EXIT=%s\n' "$name" "$rc"
tail -16 "$e/$name.log"
exit "$rc"
