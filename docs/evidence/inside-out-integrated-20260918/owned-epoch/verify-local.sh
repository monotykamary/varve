#!/bin/bash
set -euo pipefail
root=/Users/monotykamary/VCS/working-remote/open-source/varve
e=$root/.tools/rebuild-20260918/owned-epoch-evidence
cd "$root/.tools/rebuild-20260918/private-overlay-review"
: > "$e/frozen-local-present.sha256"
: > "$e/frozen-local-omitted.txt"
while read -r hash path; do
    if test -f "$path"; then
        printf '%s  %s\n' "$hash" "$path" >> "$e/frozen-local-present.sha256"
    else
        printf '%s\n' "$path" >> "$e/frozen-local-omitted.txt"
    fi
done < "$root/.tools/rebuild-20260918/private-overlay-evidence/source-files.sha256"
shasum -a 256 -c "$e/frozen-local-present.sha256" > "$e/frozen-local-present.log"
printf 'LOCAL_FROZEN_PRESENT_VERIFIED='; wc -l < "$e/frozen-local-present.sha256"
printf 'LOCAL_REVIEW_COPY_OMITTED='; wc -l < "$e/frozen-local-omitted.txt"
cd "$root"
shasum -a 256 -c "$e/owned-files.sha256" > "$e/local-owned-verification.log"
shasum -a 256 "$e/source-files.sha256" "$e/owned-files.sha256"
