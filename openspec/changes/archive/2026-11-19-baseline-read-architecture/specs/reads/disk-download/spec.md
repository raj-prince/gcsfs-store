# Spec Delta

## Purpose

Defines observable requirements for downloading remote GCS objects to local disk via `get`, `download`, and `get_file`, including sequential vs. concurrent prefetch thresholds, generation pinning, partial-file cleanup, and read consistency verification.

## ADDED Requirements

### Requirement: Multi-file download orchestration via get and download
The filesystem SHALL support downloading single files, file lists, glob patterns, and recursive directory trees via `get` and its alias `download`, creating local parent directories and executing per-file downloads concurrently up to the file-backed `batch_size`.

#### Scenario: Recursive directory download creates local tree
- **WHEN** `get(rdir, ldir, recursive=True)` or `download(rdir, ldir, recursive=True)` is called
- **THEN** the filesystem expands remote paths, validates local path containment, creates local parent directories, and invokes `_get_file` per file with progress callbacks

#### Scenario: Destination pointing to an existing local directory is skipped
- **WHEN** `get_file(rpath, lpath)` is called and `lpath` is an existing local directory
- **THEN** the method returns immediately without overwriting the directory

### Requirement: Transport-aware concurrent vs. sequential download routing
When downloading a file via `get_file`, the filesystem SHALL default to `concurrency=4`, `chunk_size=16 MiB`, and `max_prefetch_size=2 * concurrency * chunk_size` (`128 MiB`), routing large files to concurrent prefetching + `os.pwrite` and small files or custom-header requests to sequential 128 KiB streaming.

#### Scenario: Standard HTTP file above 100 MiB uses concurrent prefetch and pwrite
- **WHEN** `get_file(rpath, lpath)` downloads a Standard/HNS object larger than `100 MiB` with `concurrency > 1` and no custom `headers`
- **THEN** the filesystem pre-allocates `lpath` to the object's size, pins the object `generation`, prefetches `16 MiB` chunks via `BackgroundPrefetcher`, and writes chunks concurrently via `os.pwrite` (falling back to seek+write if unavailable)

#### Scenario: Zonal or RCU file above 5 MiB uses concurrent gRPC MRD prefetch
- **WHEN** `get_file(rpath, lpath)` downloads a Zonal or RCU object larger than `5 MiB` with `concurrency > 1` and no custom `headers`
- **THEN** the filesystem checks out an `MRDPool`, prefetches chunks via gRPC `_cat_file`, and writes them concurrently to disk

#### Scenario: Files at or below threshold or with custom headers use sequential streaming
- **WHEN** `get_file` downloads an object at or below its transport threshold (`100 MiB` for HTTP, `5 MiB` for gRPC), or with `concurrency <= 1`, or with custom `headers`
- **THEN** the filesystem streams `128 KiB` chunks sequentially directly to `lpath` without pre-allocation or concurrent prefetching

### Requirement: Consistency validation during disk downloads
When downloading files to disk, the filesystem SHALL enforce the configured `consistency` mode (`"none"`, `"size"`, `"md5"`, or `"crc32c"`) and SHALL reconcile checksum validation when downloading unfinalized zonal objects whose live `persisted_size` differs from metadata `size`.

#### Scenario: Checksum mismatch fails download and removes partial file
- **WHEN** `consistency` is `"md5"`, `"crc32c"`, or `"size"` and the downloaded bytes do not match the expected response headers or object metadata
- **THEN** the download raises a consistency error (or `aiohttp.ClientError` on truncated read) and deletes the partial local file

#### Scenario: Unfinalized zonal object downgrades hash checks to size consistency
- **WHEN** concurrent gRPC download detects that `mrd_pool.persisted_size` differs from `details["size"]`
- **THEN** the filesystem drops stale `"crc32c"` and `"md5Hash"` metadata, downgrades `"md5"`/`"crc32c"` consistency to `"size"`, and validates against the live `persisted_size`

### Requirement: Partial file cleanup on download interruption or error
When a download fails due to any exception or cancellation (`BaseException`), the filesystem SHALL delete any partially written local target file before propagating the failure.

#### Scenario: Failed download does not leave corrupt local file
- **WHEN** an error or interruption occurs mid-transfer during `get_file`
- **THEN** any partially created file at `lpath` is removed before the exception propagates to the caller
