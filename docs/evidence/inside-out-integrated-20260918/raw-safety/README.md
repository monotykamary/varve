# Raw cleanup safety checkpoint

**Source/test evidence, not deployment or production certification.** [Independent review](review.md) closed the reported defects with no findings in scope. Root integrated only the ten owned paths and verified all 128 source hashes. The qualified candidate was subsequently deployed; source-bound runtime checks and short smokes are preserved separately in [raw-safety-runtime](../raw-safety-runtime/). This bundle remains the source/test checkpoint, not production certification.

Full 128-file manifest: `13e9198e00e6d11a26c89def8fd775a2a326236f7bd844cfdef288883e4441c0`. `source.tar.gz` contains exactly those source files, no build cache, native binaries, credentials or database fixtures. Root independently verified all 128 files. `SHA256SUMS` binds the evidence bundle.

The repair addresses native/raw enclosing-allocation release order, encoded-frame credit through materialization errors/panics, and exact requested scanner allocation layouts. See `acceptance.md` for scope and limitations. Behavioral red logs are retained; initial compile/environment/lint misses are identified separately.

Railway checks: **112 engine/raw/boundary + 3 raw-memory + 12 flow + 11 ingestion + 23 native + 121 integration = 282 passing tests**; strict Clippy in default/fault modes and formatting. Nine final commands verified all 128 source hashes before and after execution. See `final-summary.log` and per-command logs/manifests. No tests ran locally.

Runner scripts preserve their original isolated Railway paths and pinned prerequisites. They are forensic recipes, not portable scripts to run blindly against an existing sandbox. Reproduce only in a separately owned cloud environment. The source archive preserves this checkpoint after later checkout changes.

Contour inspected the broader live worktree, not this isolated Rust candidate: 375 coverage/extraction gaps, with JS/TS advisories dominated by retained historical evidence scripts. It does not approve Rust lifetime correctness; artifacts were not rewritten to improve a score.

These source checks alone imply no sustained Timescale win, RSS bound, distributed guarantee or production approval. Subsequent deployment and short-run measurements are preserved separately in `../raw-safety-runtime/`.
