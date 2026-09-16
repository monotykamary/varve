# Registry release — v0.1.0

## Published and verified

[`@monotykamary/varve@0.1.0`](https://www.npmjs.com/package/@monotykamary/varve) was published with `bun publish --access public` from clean source candidate `5ab634c313c8c64b80162399a573d08d134caecb`, after [CI 35061323598](https://github.com/monotykamary/varve/actions/runs/35061323598) passed.

- Registry tarball SHA-1: `3d7ad461f5439bffdc328a30f41ebf656f1b3ff2`.
- Registry SHA-512 integrity was independently recomputed and matched.
- 23 files, 22,069-byte tarball; no validation credentials in any published file.
- A fresh dependency installation, not a workspace import, passed strict TypeScript consumer checking and browser bundling.
- The installed client authenticated, rejected a bad token, inserted one row, a two-row atomic batch and 16 pipelined rows, preserved `9007199254740993n`, proved duplicate receipt handling, and recovered exactly 19 rows after local server shutdown.
- The server for that installed-client probe was the verified local Varve binary, not a claimed crates.io installation.

The npm registry did not supply `gitHead` metadata. Source provenance above is the observed clean publication candidate, not a registry-signed source attestation. Full machine-readable receipts: [registry.json](evidence/registry.json).

## Cargo: account verification required

Both `varve-storage` and `varve-client` v0.1.0 passed `cargo publish --locked --dry-run` packaging/build verification. Actual `cargo publish --locked -p varve-storage` packaged and verified the release candidate, then crates.io rejected its upload:

> HTTP 400: A verified email address is required to publish crates to crates.io. Visit https://crates.io/settings/profile to set and verify your email address.

The client upload was not attempted after the shared account gate was identified. Registry readback returned 404 for both crate names. **Neither Rust crate is published, and clean registry-installed Rust probes remain pending.** This is an account prerequisite, not a build failure; do not bypass validation or substitute a different registry.

After the account owner verifies their email at [crates.io settings](https://crates.io/settings/profile), run from the repository root:

```sh
cargo publish --locked -p varve-storage
cargo publish --locked -p varve-client
```

Then verify both registry versions/checksums, install `varve-storage` into a disposable prefix and run a fresh `varve-client` consumer against it. Update this record and C11 only after those checks pass. No credentials need to be posted in chat or committed.

The separate [Railway template](https://railway.com/deploy/varve) is already published, rendered/button-verified and independent of its deleted validation project. The database remains experimental and depends on the pinned DuckDB v2 alpha.
