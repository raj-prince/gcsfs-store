# Spec Delta

## Purpose

Coordinates the lifecycle, stream reuse, connection sharing, and multi-stream configuration of AsyncMultiRangeDownloader instances across streaming and non-streaming readers in gcsfs.

## ADDED Requirements

### Requirement: Shared Object Downloader Across Readers
The filesystem SHALL maintain at most one active downloader instance per unique object key `(bucket, object_name, generation)` regardless of caller identity or local cache configuration.

#### Scenario: Concurrent readers share active downloader
- **WHEN** multiple concurrent readers request a downloader for the same bucket, object name, and generation
- **THEN** the filesystem returns the same active downloader instance to all readers and increments its reference count for each reader

#### Scenario: Readers with different cache types share active downloader
- **WHEN** two readers open the same object revision with different local cache type settings
- **THEN** both readers share the same underlying downloader instance without creating partitioned pools

### Requirement: Reference Counting and Idle Stream Caching
The filesystem SHALL track active reader references for each downloader and transition finalized downloaders to a bounded idle LRU cache when all references are released.

#### Scenario: Reader release transitions idle finalized downloader to LRU
- **WHEN** an active reader releases its hold on a finalized object and the reference count drops to zero
- **THEN** the downloader is removed from the active registry and placed into the idle LRU cache for future reuse

#### Scenario: Re-acquiring idle object reuses existing downloader
- **WHEN** a reader requests a downloader for an object currently present in the idle LRU cache
- **THEN** the downloader is moved from the idle LRU cache back to the active registry with a reference count of one

#### Scenario: Idle cache capacity eviction closes oldest stream
- **WHEN** an idle downloader is added to the LRU cache and the cache size exceeds the configured maximum idle limit
- **THEN** the least recently used idle downloader is evicted from the cache and closed asynchronously

### Requirement: Concurrent Acquisition Deduplication
The filesystem SHALL deduplicate concurrent downloader initialization requests for the same uncached object revision so that only a single initialization runs.

#### Scenario: Simultaneous initialization requests for uncached object
- **WHEN** multiple concurrent tasks request a downloader for an uncached object simultaneously
- **THEN** only one initialization task runs while all other callers wait and receive the resulting downloader instance

#### Scenario: Creator cancellation or failure allows surviving waiters to retry
- **WHEN** the task performing uncached downloader initialization is cancelled or fails with an exception while other concurrent tasks are waiting
- **THEN** the pending entry is cleared and surviving waiting tasks retry acquisition so one becomes the new creator rather than failing prematurely

#### Scenario: Waiter cancellation does not abort in-flight creator
- **WHEN** a waiting concurrent task is cancelled while the creator task is still initializing the downloader
- **THEN** `CancelledError` is raised immediately in the cancelled waiter without cancelling the shared pending future or aborting the creator task

### Requirement: Appendable and Unfinalized Object Handling
The filesystem SHALL bypass idle stream caching for unfinalized objects and refresh or replace active instances when persisted object size increases.

#### Scenario: Unfinalized object reader completion closes immediately
- **WHEN** a reader releases a downloader for an unfinalized object and the reference count reaches zero
- **THEN** the downloader is closed immediately without being placed into the idle LRU cache

#### Scenario: Unfinalized object size expansion
- **WHEN** a reader requests an active unfinalized object whose current size exceeds the persisted size of the active downloader
- **THEN** the filesystem replaces the active entry with a newly initialized downloader reflecting the expanded size

### Requirement: Generation Resolution and Unfinalized Object Freshness
When a caller requests a downloader with an explicit `generation` that matches a finalized active or idle cached downloader, the filesystem SHALL return the cached downloader immediately without issuing a metadata RPC. When `generation=None` or when an active entry is unfinalized, the filesystem SHALL resolve the latest object metadata from GCS to determine generation, finalization status, and current size, guaranteeing that unfinalized downloaders reflect the latest persisted size.

#### Scenario: Explicit generation on finalized cached object skips metadata lookup
- **WHEN** a reader requests a downloader with an explicit `generation` that already exists as a finalized entry in the active registry or idle LRU cache
- **THEN** the filesystem returns the cached downloader directly without issuing an `fs._info()` metadata call

#### Scenario: Explicit generation on active unfinalized object checks metadata for size expansion
- **WHEN** a reader requests a downloader with an explicit `generation` matching an active unfinalized entry whose current size has grown
- **THEN** the filesystem queries `fs._info()`, detects the expanded size, and replaces the active entry with a fresh downloader

#### Scenario: Unspecified generation resolves to latest object generation
- **WHEN** a reader requests a downloader with `generation=None`
- **THEN** the filesystem queries object metadata, resolves the concrete generation number, and registers the active downloader under the resolved generation key

#### Scenario: Unspecified generation on active unfinalized object with size expansion
- **WHEN** a reader requests a downloader with `generation=None` for an active unfinalized object whose current size exceeds the active downloader's persisted size
- **THEN** the filesystem creates a new downloader with the expanded size, replaces the active registry entry, and allows the previous downloader to close once existing readers complete

#### Scenario: Unspecified generation on unfinalized object never reuses idle cache
- **WHEN** a reader requests a downloader with `generation=None` for an unfinalized object that has no active readers
- **THEN** the filesystem always initializes a fresh downloader and never returns an idle cached instance

### Requirement: Process Fork and Event-Loop Isolation
The filesystem SHALL track the owning process ID and asyncio event loop for both `MRDManager` and gRPC client transports (`grpc_client`, `_get_grpc_client`, `_get_control_plane_client`), resetting cached state whenever the active PID or event loop changes.

#### Scenario: Event-loop or process change resets cached downloaders and gRPC clients
- **WHEN** a filesystem instance is accessed from a new process after `fork()` or from a different asyncio event loop
- **THEN** `MRDManager` clears stale cached entries and `ExtendedGcsFileSystem` resets its gRPC client reference so subsequent operations re-initialize cleanly on the current process and event loop

### Requirement: Multi-Stream Configuration and Stream Scaling Bounds
The filesystem SHALL initialize downloaders with `MultiStreamConfig`, configuring `min_streams=1` and `max_streams` bounded to the target concurrency or object size heuristics.

#### Scenario: Multi-stream initialization with concurrency bounds
- **WHEN** downloader initialization specifies concurrency greater than one
- **THEN** the filesystem supplies `MultiStreamConfig` configured with `min_streams=1` and `max_streams` set to the bounded concurrency

#### Scenario: Single-stream configuration for unconstrained or small requests
- **WHEN** downloader initialization specifies concurrency of one or file size is below the multi-stream threshold
- **THEN** the filesystem supplies `MultiStreamConfig` configured with single-stream bounds
