#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Operator intent gate, not proof or attestation of a cloud environment.
[[ "${VARVE_REMOTE_QUALIFICATION:-}" == railway ]] || {
  echo 'Run this campaign only on Railway with VARVE_REMOTE_QUALIFICATION=railway.' >&2
  exit 2
}
[[ "$(uname -s)-$(uname -m)" == Linux-x86_64 ]] || {
  echo 'This qualification recipe is for the pinned Linux x86_64 runtime.' >&2
  exit 2
}
export CARGO_BUILD_JOBS=2
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}"

rustfmt --check --edition 2024 --config skip_children=true \
  src/flow.rs src/flow_tests.rs src/journal.rs src/journal_tests.rs \
  examples/flow_journal_probe.rs probes/flow-memory-model/src/lib.rs
timeout 900 cargo test --locked --lib flow:: -- --test-threads=2
timeout 300 cargo test --locked --lib journal::tests:: -- --test-threads=1
timeout 300 cargo run --locked --example flow_journal_probe
timeout 300 cargo test --locked --manifest-path probes/flow-memory-model/Cargo.toml
timeout 300 cargo clippy --locked --lib --tests --example flow_journal_probe -- -D warnings
timeout 300 cargo clippy --locked --manifest-path probes/flow-memory-model/Cargo.toml --all-targets -- -D warnings

bash scripts/install-duckdb-native.sh
native_dir="$PWD/.tools/duckdb-native-v2.0.0-alpha41533-linux-amd64"
probe_bin="$PWD/.tools/native-scan-probe-stage-a"
gcc -std=c11 -O2 -Wall -Wextra -Werror -I"$native_dir" probes/native_scan.c \
  -L"$native_dir" -Wl,-rpath,"$native_dir" -lduckdb -o "$probe_bin"
timeout 20 "$probe_bin"
echo 'Replacement component checks passed; production engine migration and Timescale comparison remain pending.'
