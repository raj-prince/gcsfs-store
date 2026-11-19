# Proposal

## Why

`gcsfs` bridges synchronous Python callers (including file objects, Dask/PyArrow/Ray thread pools, and CLI scripts) onto an asynchronous `aiohttp` and `grpc.aio` I/O stack inherited from `fsspec.asyn.AsyncFileSystem`, while also supporting native `asynchronous=True` callers. This dual-mode execution model carries subtle invariants around event-loop ownership, method mirroring, telemetry `ContextVar` propagation, loop-thread close deadlocks, client/pool teardown ordering, and `fork()` survival that need an explicit baseline spec and architectural design record in `gcsfs-store`.

## What Changes

- Add the baseline specification `runtime/sync-async-lifecycle` defining the behavioral contract for sync and async filesystem and file execution in `gcsfs`.
- Document the complete sync/async runtime architecture, bridging mechanics, teardown protocols, fork-safety constraints, and known failure modes in `design.md`.
- Archive this change under `openspec/changes/archive/` as the canonical design lineage for `openspec/specs/runtime/sync-async-lifecycle/spec.md`.

## Capabilities

### New Capabilities
- `runtime/sync-async-lifecycle`: Dual-mode (`asynchronous=False`/`True`) loop ownership, telemetry-preserving sync/async method mirroring, buffered file sync/async execution (`GCSFile` / `ZonalFile`), deadlock-free close and GC teardown, lazy HTTP/gRPC client lifecycle, and fork-then-exec cache safety.

### Modified Capabilities
<!-- None; this establishes a baseline specification for existing sync/async behavior. -->

## Impact

- Documentation and specifications only (`raj-prince/gcsfs-store`); no runtime code changes to `fsspec/gcsfs`.
- Covers runtime mechanics across `gcsfs/telemetry/wrapper.py`, `gcsfs/telemetry/context.py`, `gcsfs/core.py`, `gcsfs/extended_gcsfs.py`, `gcsfs/zonal_file.py`, `gcsfs/zb_hns_utils.py`, and upstream `fsspec/asyn.py` and `fsspec/spec.py` (`2026.9.0`).
