#!/bin/bash
set -euo pipefail
cd /workspace/varve-rebuild/raw-pressure-followup
e=/workspace/varve-rebuild/raw-pressure-evidence/followup
sha256sum -c "$e/source-files.sha256" > "$e/final-source-verified.log"
: > "$e/base-differences.txt"
while read -r hash path; do
    actual=$(sha256sum "$path" | cut -c1-64)
    if [ "$actual" != "$hash" ]; then
        echo "$path" >> "$e/base-differences.txt"
        grep -Fxq "$path" "$e/owned-files.txt"
    fi
done < "$e/base-source-files.sha256"
(cd ../raw-pressure && sha256sum -c ../raw-pressure-evidence/source-files.sha256) > "$e/frozen-original-final.log"
(cd ../raw-pressure-qualified && sha256sum -c ../raw-pressure-evidence/source-files.sha256) > "$e/frozen-copy-final.log"
{
    for suite in engine flow ingest-unit native; do printf '%s ' "$suite"; grep 'test result: ok' "$e/$suite-final.log" | tail -1; done
    awk '/Running tests\// {if(name!="") {print name,last; total+=last}; name=$2; last=0} /test result: ok/ {last=$4} END {print name,last; total+=last; print "INTEGRATIONS_TOTAL",total}' "$e/integrations-final.log"
    for check in engine-final flow-final ingest-unit-final native-final integrations-final clippy-default-final clippy-fault-final fmt-final; do
        test "$(grep -c ': OK$' "$e/$check.source-before.log")" = 128
        test "$(grep -c ': OK$' "$e/$check.source-after.log")" = 128
        echo "SOURCE_GATES_PASS $check"
    done
    cat "$e/final-status.txt"
    sha256sum "$e/base-source-files.sha256" "$e/source-files.sha256" "$e/owned-files.sha256"
    echo CHANGED; cat "$e/base-differences.txt"
} | tee "$e/manifest.log"
