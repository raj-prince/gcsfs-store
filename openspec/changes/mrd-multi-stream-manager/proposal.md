# Proposal: MRD Multi-Stream Manager

## Why

Reading zonal and high-throughput objects currently constructs isolated `MRDPool` instances per reader or `cat_file` call, resulting in stream multiplication (e.g., 12 streams for 3 concurrent readers), private stream hoarding, ignored routing token (`read_handle`) reuse, and cache fragmentation across local `cache_type` settings. 

The Python SDK (`google-cloud-storage`) provides native multi-streaming and internal `read_handle` reuse directly inside `AsyncMultiRangeDownloader` (MRD). gcsfs can therefore eliminate redundant stream management and replace seven fragmented data structures across `MRDPool` and `MRDPoolCache` with a single, filesystem-level `MRDManager` that coordinates shared active MRDs and an LRU cache for idle finalized objects.

## What Changes

- **SDK Multi-Stream Integration**: Utilize `MultiStreamConfig` in `AsyncMultiRangeDownloader.create_mrd()` to pass stream bounds (`min_streams=1`, `max_streams=concurrency`) directly to the SDK.
- **Unified MRDManager**: Replace `MRDPool` and `MRDPoolCache` with a single filesystem-level manager tracking active MRDs via reference counts and idle MRDs via a bounded LRU cache.
- **Stream Sharing & Read-Handle Reuse**: Share a single multi-stream MRD instance across concurrent readers on the same object `(bucket, object, generation)`, delegating sub-stream load balancing and `read_handle` routing reuse to the SDK.
- **Cache Key Normalization**: Remove `cache_type` from cache keys `(bucket, object, generation)` to prevent pool fragmentation across differing local buffering strategies.
- **Concurrent Acquisition Deduplication**: Deduplicate concurrent `acquire()` operations on uncached objects using shielded pending asyncio futures, ensuring only one underlying MRD is created while allowing surviving waiters to retry if the creator is cancelled or fails.
- **Generation Resolution, Fast-Path Cache Hits & Unfinalized Object Freshness**: Bypass `_info()` metadata RPCs on finalized cache hits when `generation` is explicitly provided, resolve `generation=None` via metadata lookup, prevent idle LRU caching for unfinalized (live-append) objects, and safely refresh active instances when persisted size expands.
- **Process & Event-Loop Safety**: Reset cached MRD state and gRPC transports (`grpc_client`, `_get_grpc_client`, `_get_control_plane_client`) across process forks and asyncio event-loop transitions while retaining fork-inherited gRPC channels to avoid child-process teardown hangs.

## Capabilities

### New Capabilities
- `reader/mrd-manager`: Lifecycle, reference counting, deduplication, stream bounds configuration, and idle stream caching for `AsyncMultiRangeDownloader` across streaming and non-streaming read workflows in gcsfs.

### Modified Capabilities
<!-- None -->

## Impact

- **Affected Code**: `gcsfs/zb_hns_utils.py`, `gcsfs/extended_gcsfs.py`, `gcsfs/zonal_file.py`, and associated test suites.
- **Dependencies**: Requires `google-cloud-storage` with `MultiStreamConfig` support in `AsyncMultiRangeDownloader`.
- **Performance & Resources**: Caps stream creation to GIL-optimal concurrency (1-8 streams per object), eliminates connection thrashing, and lowers memory and file descriptor pressure under concurrent reads.
