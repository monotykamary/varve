#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PWD/.tools:$PATH"
command -v duckdb >/dev/null || { echo 'Install DuckDB v2 with scripts/install-duckdb.sh' >&2; exit 1; }
export VARVE_DUCKDB_CLI="${VARVE_DUCKDB_CLI:-$PWD/.tools/duckdb}"
export VARVE_DUCKDB_V2_LIBRARY="${VARVE_DUCKDB_V2_LIBRARY:-$PWD/.tools/duckdb-native-v2.0.0-alpha41533-linux-amd64/libduckdb.so}"
[[ -f "$VARVE_DUCKDB_V2_LIBRARY" ]] || { echo 'Install pinned DuckDB native library with scripts/install-duckdb-native.sh' >&2; exit 1; }
command -v node >/dev/null || { echo 'Node 22+ is required for client verification' >&2; exit 1; }
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --features fault-injection -- --test-threads=2
cargo test --locked -p varve-client -- --test-threads=2
cargo run --locked --quiet --example workload -- 2000
cargo run --locked --quiet --example cloud_probe -- --local --rows=256
cargo run --locked --quiet --example cloud_probe -- --local --rows=256 --rebuilt-library="$VARVE_DUCKDB_V2_LIBRARY"
cargo build --locked --bin varve
TARGET_DIR=$(cargo metadata --locked --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
export VARVE_TEST_BINARY="$TARGET_DIR/debug/varve"
cargo test --locked -p varve-client --features service-tests --test real_service -- --test-threads=2
npm ci --prefix clients/typescript
npm run --prefix clients/typescript typecheck
npm test --prefix clients/typescript
npm run --prefix clients/typescript test:integration
NATIVE_CLIENT_CONFIG=$(mktemp)
trap 'rm -f "$NATIVE_CLIENT_CONFIG"' EXIT
python3 -c 'import json,os; print(json.dumps({"segmented_journal":True,"checkpoint_frozen_prefix":True,"derived_pages":True,"duckdb_library":os.environ["VARVE_DUCKDB_V2_LIBRARY"],"query_executable":"/nonexistent/varve-native-client-no-cli"}))' > "$NATIVE_CLIENT_CONFIG"
VARVE_TEST_CONFIG="$NATIVE_CLIENT_CONFIG" cargo test --locked -p varve-client --features service-tests --test real_service -- --test-threads=2
VARVE_TEST_CONFIG="$NATIVE_CLIENT_CONFIG" npm run --prefix clients/typescript test:integration
npm run --prefix clients/typescript pack:check
npm audit --prefix clients/typescript --omit=dev
python3 -m unittest discover -s tests -p 'test_*.py' -v
sh -n scripts/container-entrypoint.sh
if cargo audit --version >/dev/null 2>&1; then
  cargo audit
else
  echo 'cargo-audit unavailable: RustSec audit NOT RUN (CI runs a separate dependency audit).' >&2
fi
