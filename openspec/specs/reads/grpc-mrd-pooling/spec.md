# reads/grpc-mrd-pooling Specification

## Purpose

Defines observable requirements for the gRPC Multi-Range Downloader (MRD) pool cache, finalized vs. unfinalized object staleness handling, stream scaling, and zero-copy/memmove buffer assembly used by Zonal and RCU read paths.

## Requirements

### Requirement: MRD pool caching and LRU eviction
The extended filesystem SHALL cache idle `AsyncMultiRangeDownloader` instances keyed by `(bucket_name, object_name, generation, cache_type)` with bounded per-key queue capacity (`max_queue_size=16`) and bounded idle pool count (`max_idle_pools=16`), evicting least-recently-used unreferenced keys and closing their MRDs.

#### Scenario: Closing a pool donates idle MRDs to the cache and LRU-evicts excess pools
- **WHEN** an `MRDPool` closes and its key's active reference count drops to zero
- **THEN** idle MRDs up to `max_queue_size` are retained for reuse, excess MRDs are closed, and if idle cached keys exceed `max_idle_pools`, the oldest unreferenced keys are evicted and closed

#### Scenario: Pool acquisition handles missing objects cleanly
- **WHEN** MRD creation or metadata lookup reports gRPC `NotFound` for a target object
- **THEN** the system translates the error to `FileNotFoundError` and cleans up any partially acquired pool state

### Requirement: Finalized vs. unfinalized object freshness and persisted_size
When initializing an `MRDPool`, the system SHALL reuse idle cached MRDs only for finalized objects (`timeFinalized` present) whose cached `persisted_size` is at least the object's current size, and SHALL always create a fresh MRD for unfinalized objects to read the latest server-reported `persisted_size`.

#### Scenario: Unfinalized zonal object opens a fresh MRD
- **WHEN** `MRDPool.initialize()` runs for an object with `timeFinalized is None`
- **THEN** the pool bypasses idle cached MRDs, creates a new MRD stream, and records the server-reported `persisted_size`

#### Scenario: Stale cached MRD from before append and finalize is discarded
- **WHEN** a finalized object has a cached idle MRD whose `persisted_size` is smaller than the current object size
- **THEN** the stale MRD is not reused, is parked for closure when the pool closes, and a fresh MRD is opened instead

### Requirement: Dynamic MRD stream scaling and round-robin sharing
An `MRDPool` SHALL create MRD streams lazily up to its configured `pool_size` outside its condition lock so concurrent callers open streams in parallel, and SHALL share active MRDs round-robin with in-flight reference counting once `pool_size` is reached.

#### Scenario: Concurrent callers beyond pool_size share streams safely
- **WHEN** more concurrent range downloads request MRDs than the pool's `pool_size`
- **THEN** callers reuse active MRDs round-robin without blocking indefinitely, and an MRD is returned to the free queue or closed only when its last in-flight holder finishes

### Requirement: DirectMemmoveBuffer assembly and underfill protection
When assembling concurrent gRPC range downloads into contiguous bytes, the system SHALL enforce non-overlapping `PartialView` slices, apply synchronous backpressure (`max_pending=5`), use zero-copy for single full-range writes, offload copies larger than `128 KiB` to the memmove thread pool, and reject short reads with `BufferError`.

#### Scenario: Single full-range write takes zero-copy fast path
- **WHEN** a `DirectMemmoveBuffer` receives a single write covering `[0, expected_size)`
- **THEN** the buffer stores the received `bytes` object directly without allocating or copying memory

#### Scenario: Short read raises BufferError instead of returning uninitialized memory
- **WHEN** a `PartialView` or `DirectMemmoveBuffer` closes with fewer written bytes than `expected_size`
- **THEN** `BufferError` is raised so uninitialized CPython bytes memory is never exposed to callers

#### Scenario: Large chunk writes offload memmove to thread pool
- **WHEN** a write into `DirectMemmoveBuffer` exceeds `128 KiB` (`THRESHOLD_BYTES_FOR_SCHEDULING`)
- **THEN** `ctypes.memmove` (or `memoryview` copy on non-CPython) is scheduled onto the filesystem's memmove `ThreadPoolExecutor` while writes `<= 128 KiB` execute inline
