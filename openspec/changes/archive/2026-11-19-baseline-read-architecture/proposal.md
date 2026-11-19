# Proposal

## Why

The `gcsfs` read surface spans four `fsspec` API families (buffered file streaming, in-memory `cat`/`cat_ranges`, key-value mappings, and local disk downloads) and transparently dispatches across two transports (HTTP REST via `aiohttp` and gRPC Multi-Range Downloader streams for Zonal and Rapid Cache Ultra buckets). Without baseline reference specifications and an accompanying architectural design record in `gcsfs-store`, future read-path changes lack a canonical behavioral contract and shared rationale for thresholds, caching, pooling, and fallback invariants.

## What Changes

- Record baseline OpenSpec specifications for all five read capability areas in `openspec/specs/reads/`.
- Document the end-to-end read architecture, transport dispatch design, prefetcher/caching wiring, MRD pooling lifecycle, buffer assembly strategy, and known trade-offs in `design.md`.
- Preserve this change in `openspec/changes/archive/` as the permanent baseline design lineage behind the `reads/*` main specs.

## Capabilities

### New Capabilities
- `reads/transport-dispatch`: Import-time filesystem class selection, Storage Control layout detection and caching, HTTP vs. gRPC routing predicates, and sync/async bridging.
- `reads/file-streaming`: Buffered file-object reads (`open`, `fsspec.open`, `read_text`, `head`, `tail`, `read_block`), `ZonalFile` vs. `GCSFile` selection, adaptive prefetching and explicit cache overrides, range boundaries, and safe teardown.
- `reads/cat-and-ranges`: Direct in-memory object and byte-slice reads (`cat_file`, `read_bytes`, `cat`, `cat_ranges`, `GCSMap`), concurrent range splitting, and MRD scatter-gather stream allocation.
- `reads/disk-download`: Download-to-disk reads (`get`, `download`, `get_file`), transport-aware concurrency thresholds, `BackgroundPrefetcher` + `os.pwrite` streaming, consistency validation, and partial-file cleanup.
- `reads/grpc-mrd-pooling`: `MRDPoolCache` LRU lifecycle, finalized vs. unfinalized `persisted_size` staleness rules, dynamic stream scaling with round-robin sharing, and `DirectMemmoveBuffer` zero-copy/memmove assembly.

### Modified Capabilities
<!-- None; this change establishes baseline specifications for existing read behavior. -->

## Impact

- Documentation and specifications only (`raj-prince/gcsfs-store`); no runtime code changes to `fsspec/gcsfs`.
- Documents read behavior across `gcsfs/__init__.py`, `gcsfs/core.py`, `gcsfs/extended_gcsfs.py`, `gcsfs/zonal_file.py`, `gcsfs/zb_hns_utils.py`, `gcsfs/caching.py`, `gcsfs/concurrency.py`, `gcsfs/mapping.py`, `gcsfs/telemetry/wrapper.py`, and upstream `fsspec` (`2026.9.0`).
