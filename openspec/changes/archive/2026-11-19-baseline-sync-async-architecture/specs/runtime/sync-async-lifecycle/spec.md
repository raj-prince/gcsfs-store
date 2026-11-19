# Spec Delta

## Purpose

Defines observable behavioral contracts for synchronous and asynchronous execution modes on the GCS filesystem and its buffered file handles, including event-loop ownership, method mirroring, telemetry context propagation, client and file teardown, fork safety, and async file streaming boundaries.

## ADDED Requirements

### Requirement: Dual-mode filesystem initialization and loop ownership
The filesystem SHALL support synchronous (`asynchronous=False`, default) and native asynchronous (`asynchronous=True`) execution modes, binding synchronous instances to a provided event loop or the shared background `"fsspecIO"` event loop while setting `self._loop = None` on `asynchronous=True` instances.

#### Scenario: Synchronous filesystem binds to background or caller-supplied loop
- **WHEN** `GCSFileSystem(asynchronous=False)` is constructed with `loop=None` or an explicit `loop`
- **THEN** the instance exposes that event loop via `self.loop` and executes mirrored synchronous methods on it via `fsspec.asyn.sync`

#### Scenario: Asynchronous filesystem sets loop to None and runs on caller loop
- **WHEN** `GCSFileSystem(asynchronous=True)` is constructed and a `_`-prefixed coroutine method (such as `_cat_file`, `_ls`, `_info`, `_get_file`, `_pipe_file`, `_rm`) is awaited inside a running asyncio loop
- **THEN** `self.loop` is `None`, synchronous mirrored methods raise `RuntimeError("Loop is not running")`, and async clients bind lazily to the caller's running event loop

#### Scenario: Cross-process loop access is rejected after fork
- **WHEN** a filesystem instance created in a parent process has its `loop` property accessed from a forked child process (`os.getpid() != self._pid`)
- **THEN** the filesystem raises `RuntimeError("This class is not fork-safe")`

### Requirement: Telemetry-preserving method mirroring and dynamic dispatch
The filesystem SHALL mirror every eligible `_`-prefixed coroutine and async generator method onto a synchronous public method at class definition time via `mirror_gcs_methods`, resolving subclass overrides dynamically at call time and propagating caller-thread telemetry `ContextVar` state across the thread boundary.

#### Scenario: Synchronous call propagates caller framework telemetry onto event loop thread
- **WHEN** a caller invokes a synchronous filesystem method (or iterates a synchronous mirrored generator) on a caller thread
- **THEN** the wrapper captures call-scoped telemetry once on the caller thread, propagates it via `contextvars` onto the event loop thread, exits the context before yielding generator items, and calls `aclose()` on generator termination

#### Scenario: Subclass overrides and patches on async methods are respected by sync wrappers
- **WHEN** a subclass overrides `_foo` or a caller monkeypatches `fs._foo` or a base `AsyncFileSystem._foo` method
- **THEN** calling the synchronous mirror `fs.foo(...)` or `super()._foo(...)` dynamically resolves the active implementation through the instance and MRO

#### Scenario: Calling a synchronous method from within its own running event loop fails fast
- **WHEN** a caller invokes a synchronous mirrored method while executing on the filesystem's own `self.loop` thread
- **THEN** `fsspec.asyn.sync` raises `NotImplementedError("Calling sync() from within a running loop")` instead of deadlocking

### Requirement: Synchronous file handles and unsupported async file open
Buffered file handles (`GCSFile` and `ZonalFile`) returned by `open()` SHALL execute synchronous file I/O bridged onto `fs.loop`, preserve the opening caller's telemetry framework across worker threads, and the filesystem SHALL reject `open_async` as unsupported.

#### Scenario: File handle preserves caller framework across worker threads and prefetchers
- **WHEN** a `GCSFile` or `ZonalFile` is opened on one thread and subsequently read, flushed, or closed on another thread (such as a `BackgroundPrefetcher`, thread-pool worker, or deferred close thread)
- **THEN** `wrap_file_methods` restores the cached `caller_framework` into the telemetry `ContextVar` for the duration of the method call

#### Scenario: Adaptive prefetcher rebinds to the filesystem event loop
- **WHEN** a file handle is opened with `cache_type="adaptive"` and the underlying filesystem's `loop` differs from the default `fsspec` loop
- **THEN** the file handle closes the initial prefetcher, recreates `BackgroundPrefetcher` on `fs.loop`, wires `producer.fetcher` directly to `_async_fetch_range`, and disarms `cache.close` so teardown is owned by the file handle

#### Scenario: Calling open_async fails cleanly
- **WHEN** a caller invokes `await fs.open_async(path, mode="rb")`
- **THEN** the filesystem raises `NotImplementedError` (or `ValueError` for text or compressed modes)

### Requirement: Deadlock-free file close and garbage collection teardown
Closing or garbage collecting a `GCSFile` or `ZonalFile` SHALL cleanly release its `BackgroundPrefetcher`, `MRDPool`, and `AsyncAppendableObjectWriter` without deadlocking if invoked from the event loop thread, `__del__`, or interpreter shutdown.

#### Scenario: Closing or garbage collecting a file on the event loop thread defers work
- **WHEN** `file.close()` is called while running on `fs.loop`'s thread, or `file.__del__()` fires on any thread
- **THEN** the file marks `_close_deferred = True` (preventing duplicate teardown) and offloads `_close_impl()` to the single bounded `"gcsfs-deferred-close"` daemon thread instead of blocking the event loop or GC thread

#### Scenario: Off-loop explicit close runs synchronously and propagates errors
- **WHEN** `file.close()` is invoked off the event loop thread while the interpreter is not finalizing
- **THEN** `_close_impl()` runs synchronously, executes prefetcher, MRD pool, and AAOW teardown stages independently (so one failure does not leak remaining resources), and re-raises the first error encountered

#### Scenario: Interrupted ZonalFile pool checkout cleans up leaked pool
- **WHEN** `ZonalFile.__init__` is interrupted by `BaseException` (such as `KeyboardInterrupt`) while awaiting `MRDPoolCache.get(...)`
- **THEN** a shared `handoff_lock` ensures either the completing coroutine or the caller's exception handler closes the checked-out `MRDPool` and marks the file closed

#### Scenario: Teardown during interpreter finalization exits safely
- **WHEN** `file.close()`, `file.__del__()`, or `sync_teardown` runs while `sys.is_finalizing()` is true
- **THEN** pending coroutines are closed cleanly and no blocking event-loop waits are attempted

### Requirement: Lazy client initialization and ordered async resource teardown
The filesystem SHALL lazily initialize its `aiohttp.ClientSession`, gRPC data client (`AsyncGrpcClient`), and Storage Control client (`StorageControlAsyncClient`) on the active event loop, and SHALL tear down extended gRPC resources in dependency order (`MRDPoolCache` before transport channels).

#### Scenario: Accessing uninitialized session or grpc_client in async mode raises RuntimeError
- **WHEN** `fs.session` or `fs.grpc_client` is read on an `asynchronous=True` instance before `_set_session()` or `_get_grpc_client()` has been awaited
- **THEN** the property raises `RuntimeError` directing the caller to await initialization first

#### Scenario: Ordered teardown drains MRD pools before closing gRPC transports
- **WHEN** `await fs._close_resources()` executes on `ExtendedGcsFileSystem`
- **THEN** it closes `_mrd_pool_cache` first, then closes `_storage_control_client.transport` and `_grpc_client.grpc_client.transport` independently so a failure in one step still closes the remaining transports

### Requirement: Fork-then-exec safety for cached filesystem instances
Cached filesystem instances SHALL preserve instance-bound sync method reference cycles (`bind_gcs_sync_methods`) so clearing `cls._cache` in a `subprocess.Popen(..., preexec_fn=...)` child does not immediately destroy inherited gRPC/HTTP clients before `execve`, while still allowing garbage collection in the parent process.

#### Scenario: Subprocess fork with preexec_fn survives live cached filesystem
- **WHEN** a parent process holds a cached-only `ExtendedGcsFileSystem` with live HTTP and gRPC clients and spawns a child using `subprocess.Popen(..., preexec_fn=...)`
- **THEN** the child clears `cls._cache` in its `after_in_child` fork hook without triggering immediate client deallocation, completes `execve` without hanging, and leaves parent clients open and usable
