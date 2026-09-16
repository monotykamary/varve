# TypeScript client review checkpoint

Scope: `clients/typescript/**` only. Exact Git root verified as `/Users/monotykamary/VCS/working-remote/open-source/varve` before edits. These are local single-node checks, not publication, distributed durability, or production qualification.

## Publication-blocker regressions

The initial regression run reproduced all three review findings (plus invalid transport accounting): 10 tests passed and 4 failed before implementation changes.

| Finding | Before | After / executable evidence |
| --- | --- | --- |
| Cancelled requests released native outbound credit | Retaining mock accumulated 100 frames / 7,965 UTF-8 bytes with `maxPendingRequests: 1`, `maxPendingBytes: 512` | `test/client.test.ts`: 100 attempts retain only 6 frames / 474 bytes; 94 fail admission. Simulated transport drain restores credit. Response and explicit-clock timeout cleanup also cannot bypass the native byte budget. |
| Invalid injected byte counters did not fail closed | Missing counter allowed a send and ordinary request timeout | Missing, boolean, string, negative, fractional, nonfinite, unsafe, bigint, and throwing counters reject without a new send. A throwing counter closes the transport and preserves existing mutation ambiguity and nested cause. |
| SQL management CALLs lost mutation ambiguity | Aborted CALL returned ordinary AbortError | All SQL is potentially mutating, consistent with Rust semantics. CALL, commented CALL, and SELECT are tested for abort, explicit-clock timeout, disconnect, and operation failure. Each retains the underlying cause. Pre-admission abort is still ordinary and unsent. |
| Malformed matching-ID write errors lost mutation ambiguity | `{error:{message:'broken'}}` returned plain ProtocolError | Malformed error, conflicting result/error envelope, invalid JSON-RPC version, and invalid JSON preserve write request ID, RPC ID, and ProtocolError cause through interruption handling. |

Byte admission checks the new frame against the budget minus both existing pending payload bytes and native `bufferedAmount`. UTF-8 payload sizes are used; already-pending native frames are conservatively counted twice. Injected transports must report native-equivalent, accurate counters. This is not a process-memory quota and cannot defend against a custom transport reporting false-but-valid numeric counters. No reconnect or replay was added.

## Verification results

Runtime: Node.js v26.5.0, npm 11.17.0. Commands run from `clients/typescript` unless noted.

| Check | Result |
| --- | --- |
| `npm run typecheck` | pass with repository strict settings |
| `npm run build` | pass |
| Unit suite (`npm test` / `node --test dist/test/client.test.js`) | 18 passed |
| Additional strict no-emit typecheck of both shipped example `.ts` files | pass |
| `PATH=/Users/monotykamary/VCS/working-remote/open-source/varve/.tools:$PATH VARVE_TEST_BINARY=/Users/monotykamary/VCS/working-remote/open-source/varve/target/debug/varve npm run test:integration` | 1 passed against the actual local binary and DuckDB; exact `[{count: 6n}]` and all six ordered timestamps, including `9007199254740993n` |
| `npm run pack:check` | pass; dry-run contains 23 files, approximately 22.4 kB packed / 88.3 kB unpacked |
| `npm audit` | 0 vulnerabilities |

The concurrent example now uses 4 workers / pending requests rather than 16. README documents the default 6 global HTTP/WebSocket slots, raising `VARVE_HTTP_REQUEST_QUEUE` / `--request-queue`, per-connection WS limits, mandatory token authentication for browser Origins even on loopback, and SQL timeout/abort ambiguity. Install instructions contain no temporary maintainer-pending boilerplate and make no registry success claim.

Browser execution was not repeated at this checkpoint; the separately reported native Chromium result belongs to the main agent's evidence. No commits, pushes, registry publication, cloud actions, root manifest/Cargo edits, or root acceptance-ledger edits were performed. This file supplies the TypeScript evidence for the main agent's shared acceptance-ledger update.
