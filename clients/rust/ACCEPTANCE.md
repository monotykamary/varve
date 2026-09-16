# Rust client acceptance checkpoint

Local single-node evidence only; no publication, registry-install, cloud, or
production guarantee is claimed. This checkpoint is confined to `clients/rust`.
The repository-wide acceptance ledger is outside this task's ownership.

## P1: retained outbound bytes

Before the fix, `cancelled_calls_keep_queued_frames_charged_until_discarded`
failed deterministically: two cancelled `Client::ping` calls left two frames in
the private command queue, yet all 118 bytes of credit were available (expected
zero). The test was added and run before changing reservation ownership.

Pending correlations and outbound commands now share one byte reservation.
Cancellation releases the call slot, not credit for retained outbound text. The
connection task holds writer credit through `SinkExt::send` (including flush).
On failure or task cancellation, the socket is dropped before writer credit.
Failed close enqueue/ack still joins or aborts the writer. No reconnect/replay
was added, and mutation outcome classification is unchanged.

`src/client/tests.rs` supplies direct queue and injected-sink evidence through
actual client calls and the connection task, without large TCP writes or sleeps:

- Cancelled queued frames prevent further admission until discarded.
- Cancelled writing/queued frames prevent admission until flushed; close cleans up.
- Response cleanup cannot uncharge a blocked writer.
- Flushing cannot uncharge an outstanding correlation.
- Failed close enqueue still joins the writer and releases resources.
- Writer failure and cancellation discard buffered/queued frames and correlations.
- An isolated sink's destructor checks that its last byte credit is still held.
- Full/closed queue rejection releases rejected-call resources only.
- Dropping all clients drains frames and returns credit.

The existing `tests/real_service.rs` cold-start readiness, bounded diagnostic
readback, child-exit checks, and temporary database path were preserved.

## Executed checks

Run from the repository root with `PATH="$PWD/.tools:$PATH"`:

| Command | Result |
| --- | --- |
| `cargo test --locked -p varve-client --lib --tests` | 10 unit, 9 protocol, 3 type tests passed |
| `cargo test --locked -p varve-client --doc` | 1 README doctest passed |
| `VARVE_TEST_BINARY="$PWD/target/debug/varve" cargo test --locked -p varve-client --features service-tests --test real_service -- --nocapture` | 1 real-service test passed against the supplied local binary |
| `cargo clippy --locked -p varve-client --all-targets --all-features -- -D warnings` | passed |
| `cargo fmt -p varve-client --check` | passed |

No root manifest/lockfile or dependencies were changed by this task. README
registry dependency syntax remains intact; there was no pending-publication
boilerplate to remove and no publish evidence was invented.
