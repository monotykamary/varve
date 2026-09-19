#!/bin/bash
set -euo pipefail
cd /workspace/varve-rebuild/owned-epoch
e=/workspace/varve-rebuild/owned-epoch-evidence
base=/workspace/varve-rebuild/private-overlay-evidence/source-files.sha256
{ cut -c67- "$base"; cat "$e/owned-files.txt"; } | LC_ALL=C sort -u > "$e/source-files.txt"
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
sha256sum src/append_overlay.rs src/raw_memory.rs src/journal.rs src/journal_tests.rs src/query/native/ffi.rs src/query/native/scan.rs src/query/native/startup.rs > "$e/preserved-files.sha256"
cd /workspace/varve-rebuild/private-overlay
sha256sum -c "$base" > "$e/frozen-after.log"
sha256sum "$base" "$e/source-files.sha256" "$e/owned-files.sha256"
printf 'SOURCE_FILE_COUNT='; wc -l < "$e/source-files.sha256"
echo OWNED; cat "$e/owned-files.sha256"
echo BASE_DIFFERENCES; cat "$e/base-differences.txt"
echo PRESERVED; cat "$e/preserved-files.sha256"
echo FROZEN_BASE_VERIFIED
