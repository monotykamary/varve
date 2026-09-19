#!/bin/bash
set -euo pipefail
cd /workspace/varve-rebuild/private-overlay
e=/workspace/varve-rebuild/private-overlay-evidence
base=/workspace/varve-rebuild/raw-budget-evidence/source-files.sha256
printf '%s\n' src/append_overlay.rs src/commit_boundary.rs src/commit_boundary_tests.rs src/derived.rs src/engine.rs src/hot_path_tests.rs > "$e/changed-files.txt"
xargs sha256sum < "$e/changed-files.txt" > "$e/changed-files.sha256"
{ cut -c67- "$base"; echo src/append_overlay.rs; } | LC_ALL=C sort -u > "$e/source-files.txt"
xargs sha256sum < "$e/source-files.txt" > "$e/source-files.sha256"
: > "$e/base-differences.txt"
while read -r hash path; do
    actual=$(sha256sum "$path" | cut -c1-64)
    if [ "$actual" != "$hash" ]; then
        echo "$path" >> "$e/base-differences.txt"
        grep -Fxq "$path" "$e/changed-files.txt"
    fi
done < "$base"
# The approved derived.rs exception must be exactly the test-only annotation.
diff -u /workspace/varve-rebuild/raw-budget/src/derived.rs src/derived.rs > "$e/derived-annotation.diff" || [ "$?" = 1 ]
sha256sum "$base" "$e/source-files.sha256" "$e/changed-files.sha256"
echo CHANGED_FILES
cat "$e/changed-files.sha256"
echo PRESERVED_RAW_NATIVE
sha256sum src/raw_memory.rs src/query/native/scan.rs src/query/native/startup.rs
printf 'SOURCE_FILE_COUNT='
wc -l < "$e/source-files.sha256"
echo BASE_DIFFERENCES
cat "$e/base-differences.txt"
echo DERIVED_EXCEPTION
cat "$e/derived-annotation.diff"
# Verify the frozen base itself still matches the supplied manifest.
cd /workspace/varve-rebuild/raw-budget
sha256sum -c "$base" > "$e/frozen-base-verification.log"
echo FROZEN_BASE_VERIFIED
