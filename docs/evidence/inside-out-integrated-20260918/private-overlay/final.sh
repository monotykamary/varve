#!/bin/bash
set -eu
check=/workspace/varve-rebuild/private-overlay-evidence/check.sh
bash "$check" clippy-default-final cargo clippy --locked --all-targets -- -D warnings
bash "$check" clippy-fault-injection-final cargo clippy --locked --all-targets --features fault-injection -- -D warnings
bash "$check" fmt-final cargo fmt --all -- --check
bash "$check" engine-final cargo test --locked --lib --features fault-injection engine::
bash "$check" native-final cargo test --locked --lib --features fault-injection query::native
