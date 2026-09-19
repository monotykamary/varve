#!/bin/sh
set -eu
cd /Users/monotykamary/VCS/working-remote/open-source/varve
R=/tmp/varve-frontier-gate.JsZmOV
trap 'code=$?; printf "%s\n" "$code" > "$R/qualify-lifetime.exit"' EXIT
cargo test --offline --locked --test service retained_query_metrics_witness_reuse_and_only_new_raw_rows > "$R/lifetime-metric-service.log" 2>&1
cargo build --offline --locked --bin varve > "$R/lifetime-debug-build.log" 2>&1
cp target/debug/varve "$R/lifetime-debug-varve"
shasum -a 256 "$R/lifetime-debug-varve" > "$R/lifetime-debug-binary.sha256"
H=$(shasum -a 256 "$R/lifetime-debug-varve" | cut -d ' ' -f 1)
set +e
"$R/venv/bin/python" benchmarks/timescale/frontier.py --binary "$R/lifetime-debug-varve" --expected-binary-sha256 "$H" --profile "$R/profile.json" --output "$R/lifetime-debug-fresh-01" --rows 307200 --batch 1000 --writers 4 --rate 5000 --seconds 20 --read-interval .1 --read-mode fresh --trace-capacity 512 --max-seconds 240
code=$?
printf '%s\n' "$code" > "$R/lifetime-debug-fresh-01.exit"
set -e
# Debug speed is not release throughput. Still require exact final oracles and no failed/ambiguous writes.
"$R/venv/bin/python" -c 'import json,sys;p=json.load(open(sys.argv[1]));assert p["status"] in ("passed","overloaded") and p["oracles"]["n"]>=307200 and p["load"]["failed"]==p["load"]["pending"]==0 and p["owned_server_reaped"];print("debug correctness verified; measured status="+p["status"])' "$R/lifetime-debug-fresh-01/report.json"
cargo test --offline --locked --workspace --features fault-injection -- --test-threads=2 > "$R/lifetime-workspace.log" 2>&1
cargo clippy --offline --locked --workspace --all-targets --features fault-injection -- -D warnings > "$R/lifetime-clippy-fault.log" 2>&1
cargo clippy --offline --locked --workspace --all-targets -- -D warnings > "$R/lifetime-clippy-default.log" 2>&1
cargo fmt --all -- --check > "$R/lifetime-fmt.log" 2>&1
printf '0\n' > "$R/lifetime-qualification.exit"
cargo build --offline --locked --release --bin varve > "$R/lifetime-release-build.log" 2>&1
cp target/release/varve "$R/lifetime-varve"
shasum -a 256 "$R/lifetime-varve" > "$R/lifetime-binary.sha256"
env -i PATH="$PWD/.tools:$PATH" HOME="$HOME" VARVE_TEST_BINARY="$R/lifetime-varve" cargo test --offline --locked -p varve-client --features service-tests --test real_service -- --test-threads=2 > "$R/lifetime-real-sdk.log" 2>&1
printf '0\n' > "$R/lifetime-release-ready.exit"
