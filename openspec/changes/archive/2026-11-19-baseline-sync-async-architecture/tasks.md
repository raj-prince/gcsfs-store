# Tasks

## 1. Baseline Sync & Async Specification

- [x] 1.1 Document dual-mode loop ownership, method mirroring, telemetry propagation, buffered file handles, deadlock-free close/GC teardown, client teardown ordering, and fork-then-exec safety in `runtime/sync-async-lifecycle`.

## 2. Design Record & Verification

- [x] 2.1 Author `design.md` covering end-to-end sync/async runtime architecture, bridging mechanics, teardown state machines, and known failure modes across `gcsfs` and `fsspec`.
- [x] 2.2 Validate the change, main specs, and archive with `openspec validate --strict --store gcsfs-store`.
