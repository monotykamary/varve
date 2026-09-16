#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PWD/.tools:$PATH"
command -v duckdb >/dev/null || { echo 'Install DuckDB v2 with scripts/install-duckdb.sh' >&2; exit 1; }
cargo fmt --all -- --check
cargo clippy --locked --all-targets --features fault-injection -- -D warnings
cargo test --locked --all-targets --features fault-injection -- --test-threads=2
cargo run --locked --quiet --example workload -- 2000
cargo run --locked --quiet --example cloud_probe -- --local --rows=256
python3 -m unittest discover -s tests -p 'test_*.py' -v
sh -n scripts/container-entrypoint.sh
if cargo audit --version >/dev/null 2>&1; then
  cargo audit
else
  echo 'cargo-audit unavailable: RustSec audit NOT RUN (CI runs a separate dependency audit).' >&2
fi
