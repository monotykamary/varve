set -eu
cd /Users/monotykamary/VCS/working-remote/open-source/varve
export PATH="$PWD/.tools:$PATH"
export VARVE_TEST_BINARY="$PWD/target/debug/varve"
printf 'GATE formatting\n'
cargo fmt --all -- --check
printf 'GATE strict workspace lint\n'
cargo clippy --offline --locked --workspace --all-targets --all-features -- -D warnings
printf 'GATE Rust libraries binaries and integration tests (no workload examples)\n'
cargo test --offline --locked --workspace --all-features --lib --bins --tests -- --test-threads=2
printf 'GATE Rust documentation tests\n'
cargo test --offline --locked --workspace --all-features --doc
printf 'GATE TypeScript real service against current binary\n'
npm run --prefix clients/typescript test:integration
printf 'ALL_REQUESTED_GATES_PASSED\n'
