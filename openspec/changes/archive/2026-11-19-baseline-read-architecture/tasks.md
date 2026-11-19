# Tasks

## 1. Baseline Read Specifications

- [x] 1.1 Document transport selection, layout detection, and sync/async bridging in `reads/transport-dispatch`.
- [x] 1.2 Document buffered file-object reads, cache strategies, and teardown in `reads/file-streaming`.
- [x] 1.3 Document `cat`, `cat_file`, `cat_ranges`, `read_bytes`, and `GCSMap` behavior in `reads/cat-and-ranges`.
- [x] 1.4 Document `get`, `download`, `get_file`, concurrency thresholds, and consistency checks in `reads/disk-download`.
- [x] 1.5 Document `MRDPoolCache`, staleness rules, stream scaling, and `DirectMemmoveBuffer` in `reads/grpc-mrd-pooling`.

## 2. Design Record & Verification

- [x] 2.1 Author `design.md` capturing end-to-end architecture diagrams, technical decisions, thresholds, and trade-offs across `fsspec` and `gcsfs`.
- [x] 2.2 Validate all baseline specs and archived artifacts with `openspec validate --all --strict --archived --store gcsfs-store`.
