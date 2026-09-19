#!/bin/bash
set -euo pipefail
e=/workspace/varve-rebuild/raw-safety-repair-evidence
cd /workspace/varve-rebuild/raw-safety-repair
sha256sum -c "$e/source-files.sha256" > "$e/final-source-verified.log"
test ! -e /workspace/varve-rebuild/raw-safety-repair-qualified
mkdir /workspace/varve-rebuild/raw-safety-repair-qualified
cut -c67- "$e/source-files.sha256" | tar -cf - -T - | tar -xf - -C /workspace/varve-rebuild/raw-safety-repair-qualified
(cd /workspace/varve-rebuild/raw-safety-repair-qualified; sha256sum -c "$e/source-files.sha256") > "$e/frozen-copy-final.log"
{
 for suite in engine raw-memory flow ingest-unit native; do
  printf '%s ' "$suite"; grep 'test result: ok' "$e/$suite-final.log" | tail -1
 done
 awk '/Running tests\// { if (name != "") {print name, last; total += last}; name=$2; last=0 } /test result: ok/ {last=$4} END {print name,last; total+=last; print "INTEGRATIONS_TOTAL",total}' "$e/integrations-final.log"
 cat "$e/final-status.txt"
 sha256sum "$e/source-files.sha256" "$e/owned-files.sha256"
} | tee "$e/final-summary.log"
ps -eo comm,args | awk '$1 == "cargo" || $1 == "rustc" {print}' > "$e/cargo-processes-after-final.log"
printf 'EXCLUSIVE TARGET RETAINED for Root coordination; no owner resume. Final repair commands complete.\n' > "$e/target-status.txt"
