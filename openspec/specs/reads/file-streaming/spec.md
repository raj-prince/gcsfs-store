# reads/file-streaming Specification

## Purpose

Defines observable requirements for file-object streaming and random-access reads exposed through `open`, `fsspec.open`, `read_text`, `head`, `tail`, and `read_block`, including file class selection, read caching, adaptive background prefetching, and teardown semantics.

## Requirements

### Requirement: File handle class selection on open
When opening an object for reading, the filesystem SHALL return a gRPC-backed file handle for Zonal and RCU buckets whenever the mode does not include `+`, and an HTTP-backed buffered file handle for all other buckets or read-write `+` modes.

#### Scenario: Binary read open on standard or HNS bucket
- **WHEN** `open(path, "rb")` is called on a Standard or HNS-only bucket
- **THEN** the filesystem returns an HTTP-backed buffered file handle initialized with `block_size` defaulting to 5 MiB

#### Scenario: Binary read open on Zonal or RCU bucket
- **WHEN** `open(path, "rb")` is called on a Zonal or RCU bucket
- **THEN** the filesystem returns a gRPC-backed file handle that checks out an MRD pool (default `pool_size=4`) and seeds its file size from the server-reported `persisted_size`

#### Scenario: Opening a bucket-only path without an object key fails
- **WHEN** `open("bucket")` or `open("bucket/")` is called in read mode without an object key
- **THEN** an `OSError` is raised before issuing data reads

### Requirement: Text, compression, and fsspec convenience helpers
The filesystem SHALL support text decoding, compression wrappers, and convenience helpers (`read_text`, `head`, `tail`, `read_block`, `fsspec.open`, `fsspec.open_files`) layered over binary buffered file handles.

#### Scenario: Opening in text or compressed mode wraps binary stream
- **WHEN** `open(path, "r", encoding="utf-8", compression="gzip")` or `read_text(path)` is invoked
- **THEN** the filesystem opens the underlying binary file handle and wraps it with the decompressor and `io.TextIOWrapper`

#### Scenario: Convenience helpers delegate through buffered open
- **WHEN** `head(path, size)`, `tail(path, size)`, or `read_block(path, offset, length, delimiter)` is invoked
- **THEN** the filesystem opens the object in binary read mode, seeks as required, and returns the requested byte slice through the active read cache

### Requirement: Default adaptive read cache selection and override
When opening a file for reading without an explicit `cache_type` on either the call or the filesystem instance, the system SHALL select `"adaptive"` when `USE_EXPERIMENTAL_ADAPTIVE_PREFETCHING` is enabled (default `"true"`) and `"readahead"` otherwise, while honoring any explicit `cache_type` supported by `fsspec` or `gcsfs`.

#### Scenario: Adaptive background prefetcher handles default sequential reads
- **WHEN** a file is opened with default cache settings and read sequentially for 3 or more consecutive reads
- **THEN** the adaptive cache uses `BackgroundPrefetcher` (default `concurrency=4`, `max_prefetch_size=256 MiB`) wired directly to the file handle's asynchronous range fetcher on the filesystem event loop

#### Scenario: Explicit cache type overrides default adaptive cache
- **WHEN** a caller passes an explicit `cache_type` (such as `"readahead"`, `"blockcache"`, `"bytes"`, `"background"`, `"none"`, or `"readahead_chunked"` on a gRPC-backed file)
- **THEN** the file handle initializes that cache strategy and routes cache misses through its range fetcher

### Requirement: Range fetch boundary and empty-range handling
When a buffered file handle fetches a byte range, it SHALL return `b""` without network calls for empty ranges and SHALL translate server `InvalidRange` or `"not satisfiable"` responses into `b""`.

#### Scenario: Empty or zero-length range returns empty bytes
- **WHEN** a read requests a zero-length range (`start >= end` or at/beyond known EOF for HTTP handles)
- **THEN** the fetcher returns `b""` immediately without issuing a network request

#### Scenario: Out-of-bounds range error translates to empty bytes
- **WHEN** the storage backend responds with an `InvalidRange` or `"not satisfiable"` error during a buffered range fetch
- **THEN** the file handle catches the error and returns `b""` rather than propagating an exception

### Requirement: Safe teardown of prefetcher and MRD resources on close
When a buffered file handle closes explicitly or via garbage collection, it SHALL tear down any active background prefetcher and release any checked-out MRD pool back to the cache without deadlocking if invoked from the event loop thread or `__del__`.

#### Scenario: Closing a file handle from event loop thread or finalizer
- **WHEN** `close()` or `__del__` executes on a file handle with an active background prefetcher or MRD pool
- **THEN** resource cleanup completes safely (deferring to a background daemon thread or scheduled task when already on the event loop thread) without deadlocking
