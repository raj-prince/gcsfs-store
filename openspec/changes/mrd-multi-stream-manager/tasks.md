# Tasks: MRD Multi-Stream Manager

## 1. Multi-Stream Configuration & Factory

- [x] 1.1 Import `MultiStreamConfig` in `gcsfs/zb_hns_utils.py` and verify clean import from `google.cloud.storage.asyncio.async_multi_range_downloader`
- [x] 1.2 Update `init_mrd` in `gcsfs/zb_hns_utils.py` to pass `multi_stream_config` with bounded stream counts (`min_streams=1`, `max_streams=concurrency`) and verify via unit tests

## 2. Core MRDManager Implementation

- [x] 2.1 Implement `MRDManager` with active reference-counted map and bounded inactive LRU cache in `gcsfs/zb_hns_utils.py`, and verify acquire and release behavior with unit tests
- [x] 2.2 Add concurrent `acquire()` deduplication via shielded pending asyncio futures in `MRDManager` so simultaneous cold requests trigger a single `create_mrd` call, creator cancellation/failure lets surviving waiters retry, and waiter cancellation does not abort the creator
- [x] 2.3 Implement fast-path `_info()` bypass on finalized cache hits when `generation` is specified, `generation=None` metadata resolution, and unfinalized object lifecycle (bypass LRU, immediate close on refcount drop to zero, active instance replacement on size growth), verifying via unit tests
- [x] 2.4 Add PID and event-loop safety checks in `MRDManager` (`get`, `release`, `close`) and `ExtendedGcsFileSystem` (`grpc_client`, `_get_grpc_client`, `_get_control_plane_client`, `loop`), verifying clean reset and teardown across loops and forks

## 3. Read Lifecycle Integration

- [x] 3.1 Update `ExtendedGcsFileSystem` in `gcsfs/extended_gcsfs.py` to use `MRDManager` for `cat_ranges()` and `cat_file()`, and verify range read tests pass
- [x] 3.2 Update `ZonalFile` in `gcsfs/zonal_file.py` to acquire and release downloaders from `MRDManager` during open, read, and close lifecycle, and verify streaming read tests pass
- [x] 3.3 Clean up obsolete `MRDPool` and `MRDPoolCache` references across the codebase while retaining compatibility aliases where needed

## 4. Verification & Testing

- [x] 4.1 Run the complete test suite (`test_zb_hns_utils.py`, `test_zonal.py`, `test_zonal_file.py`, `test_close_from_event_loop.py`) and verify all pass
- [x] 4.2 Execute read microbenchmarks against zonal test configurations to confirm stream count bounds and verify absence of performance regressions
