# reads/cat-and-ranges Specification

## Purpose

Defines observable requirements for direct in-memory object and byte-range reads through `cat_file`, `read_bytes`, `cat`, `cat_ranges`, and `GCSMap` / `get_mapper`, covering sequential vs. concurrent range splitting, scatter-gather batching, and error handling.

## Requirements

### Requirement: Single-object and byte-slice reads via cat_file and read_bytes
The filesystem SHALL support reading a complete object or byte slice `[start, end)` directly into memory via `cat_file` and `read_bytes`, defaulting to `concurrency=1` (sequential) and bypassing file-level read caches.

#### Scenario: Sequential HTTP read with optional Range header
- **WHEN** `cat_file(path, start, end)` or `read_bytes(path, start, end)` is called on a Standard/HNS bucket with default `concurrency=1`
- **THEN** the filesystem issues a single HTTP GET request (with an RFC 7233 `Range` header when `start` or `end` is provided, and up to 6 retries on transient failures) and returns the raw `bytes`

#### Scenario: Concurrent range splitting for large fetches
- **WHEN** `cat_file(path, start, end, concurrency=N)` is called with `N > 1` on a range of at least `10 MiB` (`2 * MIN_CHUNK_SIZE_FOR_CONCURRENCY`)
- **THEN** the filesystem splits the byte range into `min(N, length // 5 MiB)` sub-ranges, fetches them concurrently, cancels remaining tasks on any failure, and returns the concatenated bytes in order

#### Scenario: Range smaller than split threshold runs as single request
- **WHEN** `cat_file` is called with `concurrency > 1` on a range smaller than `2 * MIN_CHUNK_SIZE_FOR_CONCURRENCY` (`10 MiB`)
- **THEN** the filesystem executes a single fetch without splitting into multiple requests

### Requirement: Multi-object concurrent reads via cat and key-value mappings
The filesystem SHALL support reading one or multiple objects (including glob patterns and recursive prefixes) via `cat` and `MutableMapping` wrappers (`get_mapper` / `GCSMap`), executing per-object reads concurrently under a bounded batch size.

#### Scenario: Batch object read via cat or mapping getitems
- **WHEN** `cat(paths)` or `GCSMap.getitems(keys)` is invoked with a list of paths or a recursive directory
- **THEN** the filesystem expands paths as needed, schedules concurrent `_cat_file` coroutines bounded by `batch_size` (default `1280` for nofiles operations), and returns a mapping of path to `bytes` (or raises/returns exceptions per `on_error`)

#### Scenario: Single key lookup via GCSMap
- **WHEN** `mapper[key]` is accessed on a `GCSMap` or `FSMap`
- **THEN** it delegates to `cat` for that object and translates `FileNotFoundError` into `KeyError`

### Requirement: Scatter-gather multi-range reads via cat_ranges
The filesystem SHALL support reading lists of `(path, start, end)` byte ranges via `cat_ranges`, returning results in input order and honoring `on_error` (`"return"` or `"raise"`).

#### Scenario: Standard HTTP buckets fetch each range via bounded coroutines
- **WHEN** `cat_ranges(paths, starts, ends)` is called for Standard/HNS paths with `max_gap=None`
- **THEN** the filesystem schedules one `_cat_file` call per range bounded by `batch_size` and returns the ordered list of byte buffers

#### Scenario: Zonal and RCU buckets batch ranges per object over pooled MRD streams
- **WHEN** `cat_ranges(paths, starts, ends)` is called with `max_gap=None` and includes Zonal or RCU paths
- **THEN** the filesystem groups ranges by object, allocates up to `concurrency` (or `16` default) streams per object via D'Hondt apportionment under a global concurrency semaphore, balances ranges by byte size across streams, and executes batched `download_ranges` RPCs (up to `1000` ranges per RPC)

#### Scenario: Mixed-bucket cat_ranges shares global concurrency bound
- **WHEN** `cat_ranges` receives a mix of Zonal/RCU paths and Standard/HNS paths with `max_gap=None`
- **THEN** Zonal/RCU paths execute via batched MRD streams while Standard/HNS paths execute via HTTP `_cat_file`, all sharing the same global semaphore and returning results in the original input order

#### Scenario: Explicit max_gap delegates to base fsspec behavior
- **WHEN** `cat_ranges` is invoked with `max_gap` set to a non-None value
- **THEN** the extended filesystem delegates to the parent `AsyncFileSystem._cat_ranges` implementation (which currently raises `NotImplementedError`)

### Requirement: Empty range and limit normalization
When resolving `(start, end)` bounds for `cat_file` and `cat_ranges`, the filesystem SHALL normalize `None` and negative offsets consistently and return `b""` without issuing data RPCs for empty ranges.

#### Scenario: Empty slice returns empty bytes immediately
- **WHEN** `cat_file` or `cat_ranges` receives an empty range (`start >= end` after resolution against object size)
- **THEN** the filesystem returns `b""` without requesting object payload bytes from storage
