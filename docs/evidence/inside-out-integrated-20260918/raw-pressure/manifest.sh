#!/bin/bash
set -euo pipefail
cd /workspace/varve-rebuild/raw-pressure
e=/workspace/varve-rebuild/raw-pressure-evidence
base=/workspace/varve-rebuild/owned-epoch-evidence/source-files.sha256
{ cut -c67- "$base"; cat "$e/owned-files.txt"; printf 'AGENTS.md\n.cargo/config.toml\n'; } | LC_ALL=C sort -u > "$e/source-files.txt"
xargs sha256sum < "$e/source-files.txt" > "$e/source-files.sha256"
xargs sha256sum < "$e/owned-files.txt" > "$e/owned-files.sha256"
: > "$e/base-differences.txt"
while read -r hash path; do
    actual=$(sha256sum "$path" | cut -c1-64)
    if [ "$actual" != "$hash" ]; then
        echo "$path" >> "$e/base-differences.txt"
        grep -Fxq "$path" "$e/owned-files.txt"
    fi
done < "$base"
sha256sum src/publication_gate.rs src/journal.rs src/journal_tests.rs src/wal.rs src/query/native/ffi.rs src/query/native/startup.rs Cargo.toml Cargo.lock .cargo/config.toml > "$e/preserved-files.sha256"
cd /workspace/varve-rebuild/owned-epoch
sha256sum -c "$base" > "$e/frozen-after.log"
cd /workspace/varve-rebuild/raw-pressure
{
    /root/.cargo/bin/rustc --version
    /root/.cargo/bin/cargo --version
    sha256sum /workspace/varve-rebuild/baseline/.tools/duckdb /workspace/varve-rebuild/baseline/.tools/duckdb-native-v2.0.0-alpha41533-linux-amd64/libduckdb.so
} > "$e/tools.txt"
{
    for suite in engine flow ingest-unit native; do printf '%s ' "$suite"; grep 'test result: ok' "$e/$suite-final.log" | tail -1; done
    awk '/Running tests\// { if (name != "") {print name, last; total += last}; name=$2; last=0 } /test result: ok/ {last=$4} END {print name,last; total+=last; print "INTEGRATIONS_TOTAL",total}' "$e/integrations-final.log"
    cat "$e/final-status.txt"
    sha256sum "$base" "$e/source-files.sha256" "$e/owned-files.sha256"
    printf 'SOURCE_FILE_COUNT='; wc -l < "$e/source-files.sha256"
    printf 'OWNED_FILE_COUNT='; wc -l < "$e/owned-files.sha256"
} | tee "$e/manifest.log"
