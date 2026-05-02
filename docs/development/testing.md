# Testing

## Running

```bash
cmake --preset default
cmake --build build/default
ctest --preset default
```

The `default` test preset enables parallelism, randomised scheduling, `--output-on-failure`, and a 30-second per-test timeout ceiling. To override parallelism without editing the preset, set `CTEST_PARALLEL_LEVEL` in the environment.

Scoped runs:

```bash
ctest --preset default -L unit          # unit tier only
ctest --preset default -L unit-hot      # one library's unit tests
ctest --preset default -R '.*Recovery.*' # name regex
```

Sanitizers run in CI via dedicated presets (`asan`, `tsan`, `ubsan`) as a build-matrix fan-out. The `asan` and `tsan` jobs use `jobs: 2` to avoid OOM under shadow-memory inflation; `ubsan` runs `jobs: 4`. ASan/TSan are not reliable on Apple Silicon locally; UBSan is, and reproducing findings locally with `cmake --preset ubsan` is supported (requires Clang).

CI sets `UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1:abort_on_error=1` and points at `cmake/ubsan_suppressions.txt`. The `asan` preset combines AddressSanitizer with the default UBSan check group, so `UBSAN_OPTIONS` is set on that job too — a UBSan hit during an asan run aborts the test, just as an ASan hit does. The `ubsan` preset enables the extended UBSan check set; see `docs/development/building.md` for the full check inventory.

Micro-benchmarks are not run via ctest. Build with the `bench` preset and run the binary directly.

## Principles

**P1 — Every test is parallel-safe.** Tests never assume a specific port, PID, path, or execution order. Use `TempDir` for filesystem state and let the OS or kernel assign ports (`--port 0` on the server, discovered via stdout).

**P2 — Tests own time.** Components that take a clock function inject `abyss::testing::TestClock` in tests. Real wall-clock sleeps are a code smell: they make tests slow, flaky, or both.

**P3 — Events, not durations.** When waiting for an observable effect, wait for the signal (atomic flag, `std::future`, condition variable, `ConsumerRpc::PendingCount`) rather than sleeping for "long enough". The one legitimate exception is `TestServer::WaitForReady`, which polls the server's stdout for a single real external event.

**P4 — Lifecycle is proportional.** Unit tests hold no external state. Integration tests use per-test temp dirs. System tests share a server across a suite when the test neither restarts nor asserts against server-wide baselines; otherwise `IsolatedServerTest` spawns a fresh server per test.

**P5 — Tier determines isolation.** The tier a test belongs to is determined by what it exercises, not where it's convenient to put it. A test that needs a RocksDB directory is integration, not unit. A test that binds a socket is system.

## Tiers

| Tier | Exercises | Dependencies | Isolation | Target runtime |
|------|-----------|--------------|-----------|----------------|
| Unit | One class in isolation | Mocks, fakes | `::testing::Test` | < 1s total per binary |
| Component | One subsystem wiring two or three classes | Mocks at subsystem boundaries | `::testing::Test` | < 5s total |
| Integration | Real stores + queue; in-process | Per-test `TempDir` (RocksDB, WAL) | Ad-hoc fixture + `TempDir` | < 10s total |
| System | `abyss-server` over TCP | Child process | `SystemTest` (shared) or `IsolatedServerTest` (per-test) | < 30s total |
| Bench | Encode/hash micro-benchmarks | — | `google/benchmark` | Out of ctest |
| Fuzz | Parser, format decoders | libFuzzer | Fuzz build | Out of ctest |

The 30-second default `TIMEOUT` is a safety net for runaway tests, not a target. A component test that creeps toward 30s is a design problem to investigate.

## Fixtures

### Unit / component

`::testing::Test` with mocks from `tests/support/`:

| Mock | Interface |
|------|-----------|
| `MockQueue` | `core::Queue` |
| `MockHotStore` | `core::HotStore` |
| `MockColdStore` | `core::ColdStore` |

Link via `abyss::test_support`. `TestClock` (also in `tests/support/`) provides `SteadyClockFn` and `WallClockFn` for components that accept injectable clocks.

### Integration

No named base class. Declare a local fixture, allocate an `abyss::testing::TempDir` member, and hand its path to `WalQueue::Open` / `RocksdbStore::Create`. `TempDir` cleans up on destruction.

### System

Two fixtures, in `tests/system/framework/server_fixture.h`:

| Fixture | When to use |
|---------|-------------|
| `SystemTest` | Stateless or data-path-only tests. `FLUSHALL` runs between tests; no restart. Derived `DataCommandTest` adds a probe that skips the test if data commands aren't wired yet. |
| `IsolatedServerTest` | Anything that restarts the server, asserts metrics from a clean baseline, or tests durability across shutdown. Derived `IsolatedDataServerTest` adds the same probe. |

`TestServer` spawns `abyss-server`, passes an inheritable pipe via `--ready-fd`, reads one JSON line (`{"bind":"...","port":N}`) once the listener is bound, and connects over loopback. Same contract on POSIX (fd) and Windows (HANDLE cast to `intptr_t`).

### Known limitation

`gtest_discover_tests` creates one CTest entry per test, so `SetUpTestSuite` fires once per binary invocation. Under CTest parallelism this means shared-server fixtures don't share across tests. The system tier uses `abyss_add_binary_test` instead — one CTest entry for the whole binary, so GoogleTest's per-suite lifecycle amortises server spawns across tests in the same class. If other tiers develop per-suite expensive setup, the same macro applies.

## Fixture writing rules

- No `sleep_for` except `TestServer::WaitForReady` polling the readiness pipe.
- No hardcoded ports. Use `--port 0` and parse the port the OS picked.
- No hardcoded paths. Use `TempDir`.
- No `static` non-const state across tests.
- Do not assume test ordering. `scheduleRandom: true` in the preset will surface leaks.
- Prefer value-typed fixture members over `std::optional<T>` to avoid `bugprone-unchecked-optional-access` noise.
