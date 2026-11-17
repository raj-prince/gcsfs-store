# Tasks

## 1. Core Implementation

- [ ] 1.1 Validate destination path in `GCSFileSystem._merge` in `gcsfs/core.py` and raise `ValueError("Cannot write to specific object generation")` if generation is present, verifying with unit tests.
- [ ] 1.2 Unpack source generations in `GCSFileSystem._merge` and populate `"generation"` in `sourceObjects` request body entries when present, verifying with unit tests.

## 2. Testing & Verification

- [ ] 2.1 Add unit tests in `gcsfs/tests/test_core.py` verifying `sourceObjects` generation payload forwarding and destination generation rejection.
- [ ] 2.2 Add versioned bucket test in `gcsfs/tests/test_flat_versioned.py` verifying that merging historical object generations composes historical content.
