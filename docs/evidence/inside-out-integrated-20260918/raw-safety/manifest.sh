#!/bin/bash
set -euo pipefail
cd /workspace/varve-rebuild/raw-safety-repair
e=/workspace/varve-rebuild/raw-safety-repair-evidence
base=/workspace/varve-rebuild/raw-pressure-evidence/followup/source-files.sha256
[ "$(sha256sum "$base" | cut -c1-64)" = 0658a56988a24534b5b61f268274a71f5ed7327d42e92495d88f969dddb61c7f ]
cut -c67- "$base" > "$e/source-files.txt"
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
sha256sum src/journal.rs src/commit_log.rs src/wal.rs src/publication_gate.rs Cargo.toml Cargo.lock .cargo/config.toml > "$e/preserved-files.sha256"
{
 /root/.cargo/bin/rustc --version
 /root/.cargo/bin/cargo --version
 sha256sum /workspace/varve-rebuild/baseline/.tools/duckdb /workspace/varve-rebuild/baseline/.tools/duckdb-native-v2.0.0-alpha41533-linux-amd64/libduckdb.so
 sha256sum "$e/source-files.sha256" "$e/owned-files.sha256"
 wc -l "$e/source-files.sha256" "$e/owned-files.sha256"
} > "$e/manifest.log"
(cd /workspace/varve-rebuild/raw-pressure-followup && sha256sum -c "$base") > "$e/frozen-base-before-final.log"
