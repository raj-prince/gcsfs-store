# Design: MRD Multi-Stream Manager

## Context

In the current implementation, gcsfs manages multi-range downloader streams across two classes (`MRDPool` and `MRDPoolCache`) using seven nested tracking structures. Each open file or `cat_file` call instantiates an isolated `MRDPool` with its own private stream queue, ignoring the `read_handle` routing token returned during the initial stream handshake.

The Python SDK (`google-cloud-storage`) provides native multi-streaming inside `AsyncMultiRangeDownloader` (MRD), configurable via `MultiStreamConfig`. The SDK handles stream scaling, sub-stream request pipelining, and automatic `read_handle` reuse for secondary streams. 

See `proposal.md` for background and problem motivation.

## Goals / Non-Goals

**Goals:**
- Replace `MRDPool` and `MRDPoolCache` in `gcsfs/zb_hns_utils.py` with a single, filesystem-level `MRDManager` (`MRDCache`).
- Integrate `MultiStreamConfig` directly into `init_mrd()` to pass stream bounds (`min_streams`, `max_streams`) to the SDK.
- Maintain at most one active multi-stream MRD per `(bucket, object_name, generation)` with reader reference counting.
- Keep idle finalized MRDs in a bounded LRU cache upon reader completion to eliminate connection establishment overhead on hot read paths.
- Deduplicate concurrent `acquire()` operations on uncached objects using pending asyncio futures.
- Safely resolve `generation=None` via object metadata and protect unfinalized zonal objects from stale reads by bypassing idle caching and refreshing active instances upon size expansion.

**Non-Goals:**
- Implementing range scheduling or bidi-stream load balancing in gcsfs (delegated to Python SDK).
- Supporting idle caching for unfinalized objects while active appends are modifying object length.

## Decisions

### 1. Direct Integration of `MultiStreamConfig`
* **Decision**: Import and use `MultiStreamConfig` directly from `google.cloud.storage.asyncio.async_multi_range_downloader` in `gcsfs/zb_hns_utils.py`. When `concurrency > 1` (or file size heuristics warrant it), construct `MultiStreamConfig(min_streams=1, max_streams=concurrency)` and pass it via `multi_stream_config` to `AsyncMultiRangeDownloader.create_mrd`.
* **Rationale**: Multi-stream capability is treated as a core requirement for this refactor in gcsfs, allowing cleaner code without branching or fallback shims.
* **Alternatives Considered**: Checking version or conditional feature-flagging (unnecessary since gcsfs will standardize on the multi-stream SDK release).

### 2. Architecture of `MRDManager` (Active Map + Inactive LRU)
* **Decision**: 
  - `_active`: Dict mapping `(bucket, object_name, generation)` -> `[mrd, refcount]`. Multiple concurrent readers share the same MRD instance.
  - `_inactive`: `OrderedDict` acting as a bounded LRU cache mapping key -> `mrd` for idle finalized objects whose active readers have dropped to zero.
* **Rationale**: Replaces 7 fragmented data structures with 2 flat collections. Readers simply call `acquire()` and `release()`.
* **Alternatives Considered**: Keeping `MRDPoolCache` returning `MRDPool` proxies (retains unnecessary indirection and per-pool stream hoarding).

### 3. Concurrent Initialization Deduplication via Asyncio Futures
* **Decision**: When `acquire()` misses both active and inactive entries, the first coroutine registers an `asyncio.Future` in `self._pending[key]` and initiates `create_mrd()`. Subsequent concurrent calls await `_pending[key]`.
* **Rationale**: Eliminates race conditions where concurrent readers on the same cold object open duplicate gRPC streams. Using futures avoids managing individual per-key locks and lock lifecycles.
* **Alternatives Considered**: Global lock during creation (serializes creation across different objects), or per-key `asyncio.Lock` (requires extra cleanup logic for orphaned locks).

### 4. Sensible Stream Scaling Heuristics
* **Decision**: Pass `MultiStreamConfig(min_streams=1, max_streams=concurrency)` where concurrency is derived from file size or caller configuration, defaulting to:
  - Size < 1 MB: 1 stream
  - Size < 10 MB: 2 streams
  - Size < 100 MB: 4 streams
  - Hard upper bound: 8 streams
* **Rationale**: Python GIL limitations cap effective single-process throughput at ~2 GB/s, which 4–8 streams fully saturate. Exceeding 8 streams wastes backend connections and memory buffers.

### 5. `generation=None` Resolution & Unfinalized Object Lifecycle
* **Decision**: 
  - When `generation is None`, `acquire()` queries `fs._info(path)` to determine the latest concrete `generation`, `finalized` status (`timeFinalized is not None`), and `size`.
  - Keys in `_active` and `_inactive` are keyed by the resolved concrete generation.
  - For unfinalized objects:
    - If an active entry already exists but `info["size"] > mrd.persisted_size`, the manager creates a new MRD with the updated size, replaces `_active[key]`, and allows existing readers on the older instance to finish and close.
    - On `release()`, if `mrd.finalized` is false, it is closed immediately and NEVER inserted into `_inactive` LRU cache.
* **Rationale**: Appends to unfinalized zonal objects keep the same generation number while bytes grow. Metadata checking ensures new readers always see the latest committed bytes without stale reads.

## Risks / Trade-offs

- **[Process Fork / Loop Teardown]** → Cross-process and cross-event-loop teardown can trigger `RuntimeError` during shutdown.
  * *Mitigation*: Validate `os.getpid()` and `asyncio.get_running_loop()` in `release()` and `close()`, skipping async cleanup if the owning loop or process has terminated.
- **[LRU Eviction Latency]** → Evicting an idle MRD requires async `mrd.close()`, which could block subsequent acquires if awaited inline.
  * *Mitigation*: Evicted MRDs are closed asynchronously via background task or gathered without blocking the cache return.
- **[Metadata Call Overhead on `generation=None`]** → Calling `_info()` on every acquire when `generation is None` incurs an extra metadata roundtrip.
  * *Mitigation*: Necessary for correctness with appendable unfinalized objects; callers reading immutable finalized data can pass explicit `generation` to bypass metadata lookup.
