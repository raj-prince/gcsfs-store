# Proposal

## Why

In `GCSFileSystem._merge`, source paths are processed using `[self.split_path(p)[1] for p in paths]`, which discards the third tuple element (`generation`) returned by `split_path`. Consequently, the `"generation"` property is omitted from each `sourceObjects` entry in the `storage#composeRequest` payload. When callers pass version-pinned paths (e.g. `bucket/key#<generation>` or `bucket/key?generation=<id>`) to `merge` with `version_aware=True`, GCSFS silently composes the live (current) objects instead of the requested historical generations. Additionally, `_merge` fails to validate whether the destination path specifies an object generation, risking silent overwrites of live objects when callers mistakenly supply a versioned destination.

## What Changes

- Modify `GCSFileSystem._merge` in `gcsfs/core.py` to unpack `(bucket, key, generation)` for each path in `paths`.
- Include `"generation": src_generation` in each `sourceObjects` entry when `src_generation` is present.
- Raise `ValueError("Cannot write to specific object generation")` if the destination `path` specifies an object generation, aligning with `_cp_file` and `_pipe_file`.
- Add unit and integration tests verifying that generation IDs are forwarded to the `compose` API request body when present and that version-pinned destination paths are rejected.

## Capabilities

### New Capabilities

- `object-versioning/compose`: Specifications governing version-aware object composition (`merge`) in GCS buckets, including generation preservation for source objects and validation of destination paths.

### Modified Capabilities

*(None)*

## Impact

- **Affected Code**: `gcsfs/core.py` (`GCSFileSystem._merge`) and tests in `gcsfs/tests/test_core.py` / `gcsfs/tests/test_flat_versioned.py`.
- **APIs**: Backward-compatible for unversioned paths; adds support for version-pinned source paths and explicit validation on destination paths.
- **Dependencies**: None.
