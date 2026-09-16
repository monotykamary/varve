# Varve contributor instructions

Read docs/ARCHITECTURE.md and docs/ACCEPTANCE.md before implementing.

Only Astra or Sol may be used for delegation. Autonomous implementers must first verify `git rev-parse --show-toplevel` equals `/Users/monotykamary/VCS/working-remote/open-source/varve`; abort without edits otherwise.

Keep build artifacts small: use configured dev/test profiles, no bundled DuckDB build, no containers, no large fixture downloads. Keep tests deterministic with explicit clocks and tempfile cleanup. Never weaken a durability or corruption assertion merely to pass a test. Do not claim distributed or production guarantees from single-node tests.

Use ordinary comments, never decorative divider blocks. Use conventional commit messages. Keep the acceptance ledger current at review checkpoints.
