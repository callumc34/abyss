# Testing Architecture

## Test Pyramid

Abyss tests are organised into five layers, each with distinct cost and value profiles. Lower layers run faster and catch narrower bugs; higher layers run slower and validate system behaviour.

```
                 ┌─────────────────────┐
                 │   Stress / Chaos    │  Minutes. CI-only.
                 │   Fuzz              │  Crash recovery, parser fuzzing.
                 ├─────────────────────┤
                 │   System            │  Seconds. Full server over TCP.
                 │                     │  Real Redis client, protocol coverage.
                 ├─────────────────────┤
                 │   Integration       │  Seconds. Multi-component, in-process.
                 │                     │  Real stores in temp dirs, real WAL.
                 ├─────────────────────┤
                 │   Component         │  Milliseconds. One real component,
                 │                     │  mocked neighbours.
                 ├─────────────────────┤
                 │   Unit              │  Sub-millisecond. Pure logic, no I/O.
                 │                     │  Codecs, parsers, state machines.
                 └─────────────────────┘
```

### Unit

Test a single function or class in isolation. No file I/O, no network, no threads, no real clock. These are fast, deterministic, and stable across refactors.

**What belongs here:**
- Codec round-trips (WAL entry, segment header, cold key, RESP)
- Operation parsing and classification (ops, command registry, predicate flags)
- Pure state machines (CompactedState merge semantics, FlushStrategy)
- Value/error type construction (RespValue, Result, Error)
- Engine logic over fakes: decide for every command and predicate, the hot replayer's residency rule and checks, the recovery coordinator over a mock queue

**What does not:**
- Anything that requires a running store, queue, or server.
- Anything that depends on wall-clock time without a test clock.

### Component

Test one real component with its immediate dependencies mocked or stubbed. Validates that a component correctly implements its contract against its neighbours' interfaces.

**What belongs here:**
- `TieringEngine` with a real `ShardedHotStore`, `Sequencer` and `CompactionBuffer` over `MockQueue` and `MockColdStore` — validates read-path tiering logic and write-path dispatch.
- `RequestPipeline` with a real `TieringEngine` backed by mock stores — validates end-to-end command processing from RESP bytes to RESP bytes.
- `CompactionBuffer` with a test clock — validates absorb/flush lifecycle and the pending order.
- The write path and recovery end to end on `SequencedEngineTest` (`tests/component/sequenced_engine_fixture.h`): a real log, hot store, RocksDB cold store and cold consumers, the consumers driven by hand, and the engine over them. `Restart()` drops everything but the files and recovers a fresh hot store from them, as a process restart does.

**Pattern:** The component under test is real. Anything it calls through an abstract interface is a mock or fake. Anything it creates internally (value types, configs) is real. `SequencedEngineTest` is the exception: its subject is how the engine, hot and cold agree, so every part is real and only the timing is in the test's hands.

### Integration

Test multiple real components wired together, in-process. Uses real file-backed stores and WAL in temporary directories. Validates that components actually integrate — not just that their interfaces type-check.

**What belongs here:**
- Full write path: `TieringEngine` → `WalQueue` → `HotStore` → verify state.
- Full read path: hot miss → buffer hit, hot miss → cold hit.
- Recovery: write entries, destroy stores, replay queue, verify identical state.
- TTL expiry through the full pipeline.

**Isolation:** Each test gets a fresh temporary directory. Tests are independent and parallelisable. RocksDB and WAL instances are created and destroyed per test (or per fixture).

### System

Test the full `abyss-server` binary over TCP using a real Redis client. The server starts as a subprocess on a random port. Tests connect with hiredis and send real Redis commands.

**What belongs here:**
- Per-type command coverage: strings, sets, hashes, sorted sets, generic key ops.
- End-to-end behaviours: eviction, TTL, cache fill on a cold hit, crash recovery.
- Redis protocol compliance: verify responses match Redis for the supported command set.
- Connection lifecycle: HELLO handshake, CLIENT commands, pipelining, QUIT.
- Error responses: unknown commands, arity mismatches, WRONGTYPE.

**Fixture:** `ServerFixture` manages server lifecycle — start, wait-for-ready, connect, stop, cleanup.

### Stress / Chaos / Fuzz

Non-deterministic, long-running tests that explore edge cases automated tests miss.

- **Fuzz:** libFuzzer targets for the RESP parser and WAL decoder. Built with `ABYSS_BUILD_FUZZ=ON` and run locally; not part of CI.
- **Stress:** High-throughput write/read loops, memory pressure, concurrent access under TSAN.
- **Chaos:** SIGKILL during WAL writes, disk-full simulation, crash-recovery loops.

These run in CI only, not during local development.

## Performance harness (sits beside the pyramid)

The performance harness under `tests/perf/` is not a pyramid layer — it answers a different question. The pyramid validates **correctness**: did the code do what we said it would? The harness validates **operational characteristics**: does the system meet its latency and throughput targets in [requirements.md](../design/requirements.md)?

It comprises three substrates — Google Benchmark microbenchmarks (`tests/perf/micro/`), in-process component probes (`tests/perf/probe/`), and a TCP load generator against the full server binary (`tests/perf/load/`) — wired through a shared framework (`tests/perf/framework/`) that handles HdrHistogram recording, coordinated-omission correction, workload YAML parsing, and versioned JSON reporting.

The harness's design and the discipline it enforces are described in [ADP-013](../design/proposals/013-performance-harness.md); the operating guide lives in [docs/development/performance.md](../development/performance.md).

The `perf-framework` label runs the harness's own unit tests in the default `ctest` invocation; the `perf-micro`, `perf-probe`, and `perf-load` labels are excluded from the default run and must be invoked explicitly.

## Test Binary Structure

Each layer and component compiles into its own test binary. This gives O(changed-component) rebuild times and allows CTest to run subsets by label.

```
tests/
├── unit/
│   ├── core/          → abyss_core_tests         [labels: unit, unit-core]
│   ├── queue/         → abyss_queue_tests         [labels: unit, unit-queue]
│   ├── resp/          → abyss_resp_tests          [labels: unit, unit-resp]
│   ├── hot/           → abyss_hot_tests           [labels: unit, unit-hot]
│   ├── cold/          → abyss_cold_tests          [labels: unit, unit-cold]
│   ├── consumer/      → abyss_consumer_tests      [labels: unit, unit-consumer]
│   └── engine/        → abyss_engine_tests        [labels: unit, unit-engine]
├── component/         → abyss_component_tests     [labels: component]
├── integration/       → abyss_integration_tests   [labels: integration]
├── system/            → abyss_system_tests        [labels: system]
├── fuzz/              → abyss_fuzz_*              (one binary per target)
├── bench/             → abyss_bench               [labels: bench]
└── support/           → shared utilities (header-only library)
```

Usage:
```bash
ctest -L unit              # All unit tests (~200ms)
ctest -L unit-consumer     # Just consumer unit tests
ctest -L component         # Component tests (~2s)
ctest -L integration       # Integration tests (~10s)
ctest -L system            # System tests (~30s)
ctest                      # Everything
```

## Write path and recovery tests

The sequencer, decide-then-log and the hot replayer ([ADP-015](../design/proposals/015-write-path-and-durability.md), [ADP-007](../design/proposals/007-recovery.md)) are tested here:

| File | Label | What it shows |
|------|-------|---------------|
| `tests/unit/engine/hot_replayer_test.cpp` (`HotReplayerTest`) | `unit-engine` | Only a frame that replaces its key's state makes a key resident, and a Flush wipes its shard and sets the flush floor; a skipped frame still raises its shard's `appended_at` stamp and drops the key's stub; a Flush leaves nothing from before it; a frame out of place, or a shard short of its end, fails replay; replay reads no clock; the sweep evicts by write time, never an undrained key; at the backpressure ratio with nothing evictable cold is made to drain (with a drained seq that lags), a failed drain fails replay, and a shard still over after a drain asks again only in the next scan batch, and replay fails past twice the limit when the drained seq is pinned. |
| `tests/unit/engine/recovery_coordinator_test.cpp` (`RecoveryCoordinatorTest`) | `unit-engine` | One scan feeds hot from its first retained seq and cold past its commit; a scan that misses frames fails recovery; progress counts every frame to the end; scan errors, cancels and a cold wipe that always fails. |
| `tests/component/hot_replayer_test.cpp` (`HotReplayerRecoveryTest`) | `component` | A key whose earlier frames were reclaimed stays non-resident and reads whole from buffer plus cold; a FLUSH mid-log leaves no pre-flush key, stubs included; a log three times hot's budget replays within the ratio; a restart past a key's eviction window leaves it to cold; with the wall clock shifted by −1 h, 0 and +1 h, hot's state before the sweep is identical, and so are reads after recovery. |
| `tests/component/recovery_coordinator_test.cpp` (`RecoveryCoordinatorScanTest`) | `component` | One scan over a real log rebuilds hot and cold to match a model; a cold wipe that always fails fails recovery within its budget. |
| `tests/component/regression_test.cpp` (`RegressionTest`) | `component` | One test per bug the sequenced write path fixed: #163 (a write to an evicted collection sees its full state), #165 (cross-shard `MSETNX`, `RENAMENX` and `COPY` are atomic, one test each), #167 (a conditional's effect lands at its position), #168 (a cache fill racing a write never rolls cold back), #126 (eviction waits for cold drain), FLUSHDB racing conditionals never throws, recovery replays effects and decides nothing, and #160 (a conditional decided once is logged once). The partial-collection case is `AReclaimedTailKeyStaysNonResidentAndReadsWhole` above. |

The component tests above run on `SequencedEngineTest`. Unit sequencer and decide tests live beside them in `tests/unit/engine/`; component sequencer, loader and read-path tests in `tests/component/`.

## Test Support Library

Shared test utilities live in `tests/support/` as a header-only library (`abyss::test_support`).

### Test Clock

`TestClock` provides deterministic time control for tests that depend on `SteadyClock` or `WallClock`. Components accept a clock function in their config; tests inject a controllable clock, production code uses the default (real clock).

### Mocks

GMock implementations of all abstract interfaces (`MockQueue`, `MockHotStore`, `MockColdStore`). Used in component tests to isolate the unit under test.

### Cold and Buffer Readers

Cold answers no reads itself: the engine loads a key and answers from what the load returns. Tests that check what cold or the buffer holds use the same route:
- `cold_read.h` — `ColdRead` answers a read op from cold alone, loading each key whole and answering as hot answers it.
- `buffer_read.h` — `BufferRead` reads a string key's buffered delta as a GET would.
- `cold_replay.h` — `ReplayCold` replays a cold consumer's shard from its cursor to an end, as recovery's scan feeds it, then finishes the replay.

### Operation Builders

Helper functions for constructing `WriteOp`, `ReadOp`, `QueueEntry`, and `RespCommand` values concisely in tests.

### Temporary Directories

RAII wrapper for creating unique temporary directories per test, with automatic cleanup on destruction.

## Clock Injection

Time-dependent components accept a clock function in their config structs:

```
SingleShardConfig  → steady_clock, wall_clock
ShardedHotStoreConfig → steady_clock, wall_clock
CompactionBuffer constructor → steady_clock
HotReplayer::Config → steady_clock, wall_clock (read once, by the sweep after replay)
```

Default values point to the real clock. Tests override with `TestClock` to control time precisely. This eliminates `sleep()` calls from tests and makes timing-sensitive behaviour deterministic.

Components that already accept time as a parameter (e.g. `EvictExpired(SteadyTime now)`, `SetAccessTime(SteadyTime now)`) continue to work as-is. Clock injection is for internal time reads that callers cannot control.
