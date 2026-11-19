# Spec Delta

## Purpose

Defines how the GCS filesystem selects between the standard HTTP/REST data path and the extended gRPC Multi-Range Downloader (MRD) data path based on environment configuration and bucket storage layout detection.

## ADDED Requirements

### Requirement: Default extended filesystem selection
When the package is imported or resolved via `fsspec` for the `gs` or `gcs` protocols, the system SHALL expose the extended filesystem implementation unless `GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT` is explicitly set to a falsy value prior to import or extended dependencies cannot be imported.

#### Scenario: Default filesystem resolution enables extended support
- **WHEN** `GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT` is unset or set to `"true"` or `"1"` and extended dependencies are available
- **THEN** `gcsfs.GCSFileSystem` and `fsspec.filesystem("gcs")` resolve to the extended filesystem supporting Zonal, RCU, and HNS buckets

#### Scenario: Disabling extended support forces HTTP-only filesystem
- **WHEN** `GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT` is set to `"false"` before importing `gcsfs`, or extended dependencies raise `ImportError`
- **THEN** `gcsfs.GCSFileSystem` resolves to the base HTTP-only filesystem implementation

### Requirement: Bucket storage layout detection and caching
Before routing a bucket read over gRPC or HTTP, the extended filesystem SHALL inspect the bucket's storage layout via the Storage Control API and cache resolved layouts per bucket while never caching `UNKNOWN` layouts.

#### Scenario: Zonal and Rapid Cache Ultra buckets select gRPC data path
- **WHEN** the Storage Control layout response reports `location_type == "zone"` (RAPID storage class) or `rapid_cache_info.cache_type == "rapid-cache-ultra"`
- **THEN** the filesystem marks the bucket for the gRPC data path and caches the resolved layout

#### Scenario: Standard, HNS-only, and plain Rapid Cache buckets use HTTP data path
- **WHEN** the Storage Control layout response reports a standard bucket, an HNS-only bucket, or `rapid_cache_info.cache_type == "rapid-cache"`
- **THEN** the filesystem routes object data reads through the HTTP REST data path

#### Scenario: Layout lookup failure falls back to HTTP without caching UNKNOWN
- **WHEN** the Storage Control layout lookup raises `NotFound` or an unexpected error (such as against an emulator or missing layout permissions)
- **THEN** the filesystem logs a warning, falls back to the HTTP data path, and does not cache the `UNKNOWN` layout so subsequent requests may retry

### Requirement: Synchronous and asynchronous read method bridging
The filesystem SHALL provide synchronous wrappers for asynchronous read operations (`cat`, `cat_file`, `cat_ranges`, `get`, `get_file`) executing on the filesystem's background event loop while propagating caller telemetry context, and SHALL raise `NotImplementedError` for unsupported `open_async`.

#### Scenario: Synchronous read call dispatches to background event loop
- **WHEN** a caller invokes a synchronous bulk or range read method on an instance initialized with `asynchronous=False`
- **THEN** the filesystem executes the corresponding `_`-prefixed coroutine on its event loop thread and returns the result synchronously

#### Scenario: Calling open_async raises NotImplementedError
- **WHEN** a caller invokes `open_async` on the filesystem
- **THEN** the filesystem raises `NotImplementedError`
