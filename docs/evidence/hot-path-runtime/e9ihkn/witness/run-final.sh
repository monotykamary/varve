#!/bin/bash
set -uo pipefail
cd /Users/monotykamary/VCS/working-remote/open-source/varve || exit 1
export PATH="$PWD/.tools:$PATH"
node /tmp/varve-runtime.E9ihKN/freeze.mjs assert qualified > /tmp/varve-runtime.E9ihKN/source-gate.log 2>&1 || exit 1
cargo test --offline --locked --features fault-injection --no-fail-fast --lib --test engine --test group_commit --test ingest --test prepared_publication --test prefix_checkpoint --test prefix_admission --test publication --test query_residency --test query_workers --test query_typed_inputs --test query_catalog_exposure --test residency --test service -- --test-threads=2 > /tmp/varve-runtime.E9ihKN/runtime-final.log 2>&1
result=$?
printf '%s\n' "$result" > /tmp/varve-runtime.E9ihKN/runtime-final.exit
if [ "$result" -ne 0 ]; then exit "$result"; fi
cargo clippy --offline --locked --workspace --all-targets --all-features -- -D warnings > /tmp/varve-runtime.E9ihKN/clippy-all.log 2>&1 && cargo clippy --offline --locked --workspace --all-targets -- -D warnings > /tmp/varve-runtime.E9ihKN/clippy-default.log 2>&1 && cargo fmt --all -- --check > /tmp/varve-runtime.E9ihKN/fmt.log 2>&1 && git diff --check && cargo build --offline --locked --bin varve > /tmp/varve-runtime.E9ihKN/default-build.log 2>&1
result=$?
if [ "$result" -eq 0 ]; then
  service_binary=$(sed -n 's/.*Running tests\/service.rs (\(.*\))/\1/p' /tmp/varve-runtime.E9ihKN/runtime-final.log | tail -n 1)
  test -x "$service_binary" && "$service_binary" --exact http_ingest_is_idempotent_queries_expected_rows_and_scheduler_ticks --test-threads=1 > /tmp/varve-runtime.E9ihKN/default-http.log 2>&1 && "$service_binary" --exact abrupt_process_death_replays_committed_wal_on_restart --test-threads=1 > /tmp/varve-runtime.E9ihKN/default-restart.log 2>&1
  result=$?
fi
if [ "$result" -eq 0 ]; then node /tmp/varve-runtime.E9ihKN/freeze.mjs assert qualified >> /tmp/varve-runtime.E9ihKN/source-gate.log 2>&1; result=$?; fi
printf '%s\n' "$result" > /tmp/varve-runtime.E9ihKN/final.exit
exit "$result"
