# Performance Harness

The performance harness is the tooling Abyss uses to measure whether it meets the targets in [requirements.md](../design/requirements.md). The harness's design is described in [ADP-013](../design/proposals/013-performance-harness.md); this document is operational — how to build it, how to run it, how to read the numbers it produces, and what conditions make those numbers trustworthy.

## What the harness is

Three substrates that share infrastructure but answer different questions:

| Substrate | Question | Runs |
|---|---|---|
| `tests/perf/micro/` | What is the lower-bound cost of one operation in isolation? | Google Benchmark targets via `abyss_bench`. |
| `tests/perf/probe/` | What does the engine deliver to the layer above it? | In-process binaries (`abyss_hot_probe`, `abyss_buffer_probe`, `abyss_cold_probe`, `abyss_write_probe`) that construct real components and exercise them directly. |
| `tests/perf/load/` | What does a real client see? | `abyss_loadgen` — multi-threaded TCP driver against the full `abyss-server` binary using hiredis. |

A shared library — `abyss::perf_framework` under `tests/perf/framework/` — wraps HdrHistogram, parses workload YAML, schedules requests under the wrk2 coordinated-omission discipline, scrapes `/metrics`, and emits the versioned JSON report + `.hgrm` histogram logs.

## Building

Performance binaries are gated by `ABYSS_BUILD_PERF`. The `default` and `bench` presets turn it on; `release` and `container` turn it off so production builds don't carry hdr-histogram or hiredis.

```bash
cmake --preset default          # debug build, framework tests run in default ctest
cmake --preset bench            # release build, perf binaries optimised

cmake --build build/bench --target abyss_hot_probe abyss_buffer_probe abyss_cold_probe abyss_write_probe abyss_loadgen
```

For meaningful numbers use the `bench` preset (Release, `-O2`, no sanitizers). Debug builds are useful only for verifying the harness itself works, not for measuring system performance.

## CTest labels

The default `ctest --preset default` run excludes performance work via a label filter; the framework's own unit tests are an exception and remain in the default run because they finish in under a second.

| Label | In default ctest | What it covers |
|---|---|---|
| `perf-framework` | yes | Unit tests of histogram, scheduler, key distributions, workload parser, reporter, run loop, metrics scraper. |
| `perf-micro` | no | The Google Benchmark suite (`abyss_bench`). |
| `perf-probe` | no | Smoke tests that spawn each probe binary and verify the output JSON schema. |
| `perf-load` | no | Smoke test that spawns `abyss-server` and drives `abyss_loadgen` against it end-to-end. |

To run an excluded label explicitly:

```bash
ctest --test-dir build/default -L perf-probe --output-on-failure
ctest --test-dir build/default -L perf-load  --output-on-failure
```

## Running a probe

Each probe binary is self-contained — it constructs the real Abyss component in-process and drives load against it through the component's own API. No server is involved.

```bash
build/bench/tests/perf/probe/abyss_hot_probe \
    --duration 30 --warmup 5 --workers 4 \
    --key-count 100000 --value-size-bytes 64 \
    --distribution zipfian --zipf-theta 0.99 \
    --mix "hot_get=0.95,hot_apply=0.05" \
    --output /tmp/hot.json --hgrm-dir /tmp/hot-hgrm
```

Common flags across all probes:

| Flag | Default | Meaning |
|---|---|---|
| `--duration` | 30 | Measurement window in seconds. |
| `--warmup` | 5 | Warmup duration; samples in this window are not recorded. |
| `--workers` | 1 | Worker thread count. |
| `--target-rate-ops` | 0 | Aggregate target rate. `0` means closed-loop (drive as hard as the system will go). Open-loop runs apply coordinated-omission correction. |
| `--key-count` | 100 000 | Keyspace size. |
| `--value-size-bytes` | 64 | Value size for SET-shaped ops. |
| `--distribution` | uniform | `uniform`, `zipfian`, or `latest`. |
| `--zipf-theta` | 0.99 | Skew parameter for zipfian/latest. |
| `--mix` | probe-specific | `"NAME=W[,NAME=W]…"`; weights must sum to 1.0. |
| `--output` | stdout | JSON report destination. |
| `--hgrm-dir` | omitted | Directory for per-operation `.hgrm` files. |
| `--gate` | off | Exit code 3 if any per-op target fails. |
| `--seed` | 42 | Base RNG seed. Workers derive their seeds deterministically from this. |

### Probe-specific operation names

- `abyss_hot_probe` — `hot_get`, `hot_apply`. Default targets: `hot_get` p99 ≤ 100µs (R1), `hot_apply` p99 ≤ 5µs (H1). Unknown operation names in `--mix` are rejected.
- `abyss_buffer_probe` — `buffer_read`. Default target: p99 < 50µs.
- `abyss_cold_probe` — `cold_get`, optionally `cold_set`. Default target: GET p99 < 5ms. Also accepts `--data-path` to reuse a pre-populated RocksDB directory; otherwise creates a temporary one.
- `abyss_write_probe` — `write_ack`, plus the calibration operation `device_flush`. See below.

### The write probe

`abyss_write_probe` drives the real in-process write path: the engine's write dispatch, the WAL, and all three consumer pools, built exactly as the server builds them, minus RESP and TCP. Each `write_ack` is a `SET` parsed and canonicalised the way the RESP pipeline does it. Its latency runs until the write is acknowledged.

Before measuring, the probe calibrates the device. It runs `--flush-samples` iterations of a 4 KiB write followed by the WAL's own durable flush primitive on a scratch file in the WAL directory, timing the flush only. The result is reported as the `device_flush` operation, and the write target is derived from it:

| Fsync policy | Target evaluated on `write_ack` |
|---|---|
| `none` | p99 ≤ 20µs (W1, write-path overhead without a device flush) |
| `group_commit`, `per_write` | p99 ≤ 2 × measured `device_flush` p99 + 50µs (W2) |

Point `--wal-path` at the volume under test for authoritative runs. Both `--wal-path` and `--cold-path` must be empty or absent, because the probe does not run recovery; a temporary directory is used when either is omitted.

| Flag | Default | Meaning |
|---|---|---|
| `--wal-path` | temporary | WAL directory on the volume under test |
| `--cold-path` | temporary | Cold store directory |
| `--fsync-policy` | `group_commit` | `group_commit`, `per_write` or `none` |
| `--group-commit-interval-us` | server default | Group-commit window |
| `--shards` | 64 | Shard count |
| `--segment-size-bytes` | server default | WAL segment size; must hold one maximum-size value |
| `--flush-samples` | 1000 | Device flush calibration samples |
| `--flush-concurrency` | `--shards` | Files flushed in parallel for the `device_flush_concurrent` calibration |
| `--prefill-entries` | 0 | Entries appended before measuring, so the run starts deep in a segment |
| `--quiet-threshold-s` | server default | Cold consumer quiet window |
| `--workers` | min(hardware threads, 16) | Concurrent writers. Each one blocks in the engine's write dispatch, as a reactor thread does, so this models reactors, not connections. |

**Calibration.** The concurrent calibration flushes `--flush-concurrency` files in parallel and is reported as `device_flush_concurrent`. Comparing it with `device_flush` separates the device's own floor from contention that parallel flushes create. W2 is always derived from the single-file `device_flush` p99.

**Prefill.** Prefill appends canonical `SET` entries directly to the queue in batches, routed to their owning shards, then waits for every consumer to drain them before measuring. Through the real write path on a slow device, prefill would take hours.

**Evaluating targets.**
- W1 and W2 are defined at ≤ 50% of saturation, so they are evaluated only on open-loop runs (`--target-rate-ops` > 0).
- A closed-loop run reports its targets as not evaluated and never claims a pass.
- `--gate` without a rate is a usage error.

**Run length.** The cold consumer starts flushing, checkpointing and acknowledging only after its quiet window (30 s by default). An authoritative run therefore lasts at least 60 s after warmup, or records a shorter `--quiet-threshold-s`. Prefill at least one deep-position run, because queue read cost depends on how far into a segment the consumers are reading.

**Report contents.**
- `config` records the effective settings: fsync policy, group-commit interval, shard count, segment size, quiet window, value size, workers and prefill.
- `server_metrics` holds registry snapshots at `start` and `end`, so WAL flush and offset persist counts, durations and batch sizes for the run are in the same JSON.
- The server's metrics snapshotter is not started; it takes shard locks once a second and is otherwise absent from this measurement.

**Exit codes.**
- 2: a usage error, a non-empty directory, or `--gate` without a rate.
- 3: a failed gate.
- 4: any operation errored. The report is still written, and errors are never recorded as latency.

## Running the load generator

`abyss_loadgen` is a TCP client. It connects to an already-running `abyss-server` and drives the workload defined in a YAML file.

> **Important caveat for local runs.** The bundled workloads (`tests/perf/workloads/*.yaml`) are intended for runs on perf-grade hardware. They use `target_rate_ops: 0` (closed-loop) and large key spaces, which will saturate every available core and fill memory on a developer laptop. For local exploration always supply a low `target_rate_ops` and short `duration`, or edit the workload file. Closed-loop runs against the local machine can render the system unresponsive.

A safe local invocation looks like this (60-second cap, 4 worker threads, rate-limited to 5 000 ops/s aggregate, small keyspace):

```bash
# In one terminal — start the server with a temp data dir
build/bench/apps/abyss-server/abyss-server \
    --port 16379 --admin-port 16380 --metrics-port 16390 \
    --data-dir /tmp/abyss_perf

# In another terminal — create a throttled workload and run loadgen
cat > /tmp/local.yaml <<'EOF'
name: local-exploration
description: throttled local run; safe for developer laptops
duration_seconds: 30
warmup_seconds: 5
workers: 2
connections_per_worker: 2
target_rate_ops: 5000
key_count: 10000
key_distribution:
  kind: uniform
  seed: 1
value_size_bytes: 64
mix:
  GET: 0.9
  SET: 0.1
preload:
  enabled: true
  key_count: 10000
  value_size_bytes: 64
EOF

build/bench/tests/perf/load/abyss_loadgen \
    --workload /tmp/local.yaml \
    --server 127.0.0.1:16379 \
    --metrics-url http://127.0.0.1:16390 \
    --output /tmp/local-report.json
```

The bundled workloads are reference shapes for authoritative runs:
- `hot_read_heavy.yaml`
- `write_throughput.yaml`: W3, with 256 requests in flight (8 workers × 8 connections × pipeline depth 4)
- `write_loopback_pipelined.yaml`: W1-L, SET-only, 64 connections, pipeline depth 16
- `mixed_read_under_writes.yaml`: R1-L, 90/10 GET/SET, 64 connections, open loop
- `mixed_50_50.yaml`
- `cold_read_aged.yaml`

Write targets are not written into workloads, because they depend on the durability class and are relative to the device or to a comparison server in the same run. Treat them as templates: scale `workers × connections_per_worker`, `target_rate_ops`, and `duration` down before running on a developer machine.

### Loadgen flags

| Flag | Required | Meaning |
|---|---|---|
| `--workload` | yes | Path to a workload YAML file. |
| `--server` | no | `host:port` of the abyss-server RESP listener (default `127.0.0.1:6379`). |
| `--metrics-url` | no | Base URL for `/metrics` scraping. Pass empty to disable. Snapshots are recorded at start, mid and end; a failed scrape records none for that phase. |
| `--output` | no | JSON report path (defaults to stdout). |
| `--hgrm-dir` | no | Directory for per-operation `.hgrm` files. |
| `--gate` | no | Exit code 3 if any target fails. |
| `--skip-preload` | no | Skip the workload's preload phase (useful when re-running against a server that already has the keyspace populated). |
| `--run-id` | no | Override the auto-generated run id. |
| `--sweep` | no | Find the highest sustained open-loop rate (W3); see below. |
| `--sweep-p99-bound-us` | with `--sweep` | Latency bound each sweep step must meet, normally the W2 bound from a write-probe calibration on the same volume. |
| `--sweep-step-seconds` | no | Override the workload's duration for each sweep step. |

**Server identity.** The load generator identifies the server it measured. It reads `HELLO` and, where needed, `INFO server`, so Abyss, Valkey, Redis and Dragonfly are told apart. It records the result as `server: {kind, version}`, and the server's durability settings as `config` (best effort via `CONFIG GET`). The same binary can therefore produce the comparative baselines in ADP-013 §Comparative baselines. Pass `--metrics-url ""` for servers without a `/metrics` endpoint. A failed scrape is reported on stderr and records no snapshot.

**Pipelining and arrivals.**
- Each connection keeps up to the workload's `pipeline_depth` requests in flight (default 1).
- With `arrival: steady`, open-loop requests are scheduled individually.
- With `arrival: burst`, each slot sends `pipeline_depth` requests together, the way a pipelining client behaves. `target_rate_ops` stays in requests per second, so bursts are spaced `pipeline_depth / rate` apart.
- Every request is measured from its slot's intended send time, so waiting for a full window or a slow earlier reply is charged to it.

**Errors.** Error replies are counted per operation and never recorded as latency. Any error makes the load generator exit 4, after writing the report.

**Sweeping W3.**
- `--sweep` starts at the workload's rate and doubles it until a step fails, then bisects until the bracket is within 5%.
- A step passes when it has no errors, its p99 is within `--sweep-p99-bound-us`, and the achieved rate is at least 95% of offered.
- The highest passing rate is reported as `sweep_result_ops`, with every step in `sweep[]`.
- If the starting rate already fails, the sweep halves the rate until a step passes, down to a floor of max(start / 1024, one request/s per connection).
- Only if the floor also fails is there no result, and the run exits 3.
- Steps are not independent. WAL length, hot-set size and compaction-buffer state carry over from one step to the next, and each step records the server's queue position (`queue_entries`, `queue_bytes` from `/metrics`, up to 1 s stale). For a baseline taken before the queue-read fix (#158), use a fresh server per step or a fixed prefill, because read cost there grows with queue position.

**Driver lag and load.** Open-loop runs record a send-lag histogram (actual send minus intended send).
- If send-lag p99 exceeds 50 µs, or 10% of any per-op target, the run is flagged `lagging` and a warning is printed. Treat its latencies as including driver delay.
- Driver CPU is the sum of the worker or connection threads' CPU time over the measured window. In the write probe those threads also execute the engine work inline, as reactor threads do.
- If driver CPU per wall-second exceeds max(1 core, 10% of hardware threads), the run is flagged `overloaded`. The driver is then competing with the server for cores: pin it to separate cores or run it on another host. The driver's CPU affinity is recorded on Linux.

**Waiting for a scheduled send.** Open-loop waits sleep, then spin briefly before each scheduled send.
- On Linux the sleep is an absolute monotonic `clock_nanosleep` with 1 ns timer slack, and the spin is at most 20 µs.
- On macOS, timer coalescing stretches a long sleep by up to about a quarter of its length (capped at 5 ms). The sleep there is taken in steps that each cover half the remaining time, and the spin is at most 200 µs.
- On every platform the spin is at most 1/20 of a thread's slot interval, and is capped so that all driver threads together spin less than half a core.
- Each thread's schedule is offset by its share of a slot, so threads interleave rather than firing in synchronised bursts.
- A pipelining transport must poll without blocking when asked to wait for a deadline that has already passed.

Offered rates are split across connections exactly: the remainder goes one request/s at a time to the first connections, and a connection whose share is zero stays idle. The load generator does not wait for server readiness: start it against a server whose `/ready` already returns 200.

Loadgen supports `GET` and `SET` operations, and rejects a workload whose mix names anything else; expanding command coverage is tracked separately.

## Workload YAML

A workload is a small YAML document. The schema is enforced by the parser in `tests/perf/framework/workload.cpp` — invalid configs are rejected at startup.

```yaml
name: example
description: free text
duration_seconds: 60          # required, > 0
warmup_seconds: 10            # optional, default 0
workers: 4                    # required, >= 1
connections_per_worker: 4     # loadgen only; probes ignore
pipeline_depth: 1             # loadgen only; requests in flight per connection
arrival: steady               # steady | burst; burst requires target_rate_ops > 0
                              # pipeline_depth × value_size_bytes ≤ 1 MiB
target_rate_ops: 0            # 0 = closed-loop
key_count: 100000             # required, > 0
key_distribution:
  kind: zipfian               # uniform | zipfian | latest
  theta: 0.99                 # for zipfian/latest
  seed: 42
value_size_bytes: 64
mix:
  GET: 0.95                   # weights must sum to 1.0
  SET: 0.05
preload:
  enabled: true
  key_count: 100000           # default: matches top-level key_count
  value_size_bytes: 64
targets:
  throughput_ops: 100000      # optional aggregate target
  per_op:
    GET:
      p50_us: 50
      p99_us: 100
      p999_us: 250
```

Op names in `mix` and `targets.per_op` must match. For the load generator they are RESP command names (`GET`, `SET`, …); for probes they are component-API names (`hot_get`, `buffer_read`, `cold_get`, …).

## Output

Two artefacts per run.

### `.hgrm` histogram logs

Per-operation HdrHistogram percentile logs. Industry-standard format readable by `HistogramLogProcessor`, plot.ly tools, jHiccup viewers. This is the lossless ground truth — every recorded sample is preserved (within HdrHistogram's 3-significant-digit precision).

### JSON summary

Single file, machine-consumable, schema-versioned (`schema_version: 1`). Contains:

- `run_id`, `started_at`, `duration_seconds`
- `classification` — `indicative` or `authoritative` (see below)
- `build` — commit, preset, compiler, build_type, sanitizer
- `server` — `{kind, version}` of the server measured (load generator only)
- `host` — os, kernel, cpu_model, hostname
- `workload` — the parsed workload config inlined, including `pipeline_depth` and `arrival`
- `operations.<name>` — `count`, `errors`, `throughput_ops`, `latency_us.{p50,p99,p999,max}`, `noise_floor_cv`, `saturated`, `histogram_b64` (base64-encoded HdrHistogram log)
- `config` — effective probe settings, or the measured server's reported durability settings
- `driver` — `open_loop`, `send_lag_us{count,p50,p99,max}`, `lagging`, `cpu_seconds`, `cpu_per_wall_second`, `overload_threshold_cores`, `overloaded`, `cpu_affinity{available,count,cpus}`
- `sweep[]`, `sweep_result_ops` — W3 sweep steps `{offered_ops, effective_offered_ops, achieved_ops, p99_us, errors, pass, queue_entries, queue_bytes}` and the highest passing rate (load generator `--sweep` only)
- `server_metrics` — start / mid / end snapshots from the abyss-server `/metrics` endpoint (loadgen only)
- `targets[]` — per-target `{metric, target, actual, pass, evaluated}`; `targets_evaluated` and `targets_note` say when targets were not evaluated (closed-loop write probe)
- `pass` — top-level boolean: true iff every evaluated target passed

A one-line summary of the measured server or the in-process setup, its durability setting, and per-operation counts, errors and percentiles is also printed to stderr.

**Comparing across versions.** Compare runs only within the same durability class: the current `fsync_none` against `process_crash`, and `group_commit` against `power_loss`. A default-against-default comparison across the change in acknowledgement point is meaningless.

`abyss_queue_offset_persist_duration_seconds` changes meaning across versions:
- before lazy offset persistence, one observation is one per-acknowledgement file rewrite;
- after it, one observation is one checkpoint persist.

Compare persists per second (the `_count` rate), not durations.

`noise_floor_cv` is the coefficient of variation (`stddev / mean`) of the operation's latency distribution. Use it to judge how much weight to give the percentile numbers — a CV above ~0.3 means the run was noisy and the percentiles are less trustworthy.

## Conditions of measurement

### Local macOS runs are `indicative`, not `authoritative`

The harness tags every run with a host classification in the JSON output:

- `indicative` — the numbers are real measurements but the host can't be trusted to produce repeatable results. macOS hosts always classify as `indicative` regardless of how the run is invoked.
- `authoritative` — the run was performed on a host that meets the harness's authoritative criteria. Currently this requires Linux *and* the environment variable `ABYSS_PERF_AUTHORITATIVE=1`. The latter is an explicit opt-in to be set on a dedicated runner that has had its CPU governor pinned, background services minimised, and so on.

A failing `indicative` run is still a failure — the harness does not silently downgrade it. But when comparing against the PRD targets, only `authoritative` numbers count.

### Why local laptops are unreliable

- Thermal throttling under sustained load can drop CPU clock by 30%+ partway through a long run.
- No CPU pinning, no isolated cores, no governor lock.
- Background processes (browsers, IDEs, indexing daemons) compete for CPU and memory bandwidth.
- macOS Mach scheduling does not guarantee fairness across threads in the way Linux's CFS does for an idle box.

Closed-loop workloads (`target_rate_ops: 0`) magnify all of this — they will saturate every available core, and the system response can render the machine unresponsive.

### CI gating

The harness does **not** gate pull requests on absolute target numbers. There is no GitHub-Actions check that fails the build if `hot_get` p99 is above 100µs. ADP-013 §CI integration explains the reasoning: GitHub runners are not perf-grade hardware, and a flaky p99 threshold check is worse than no check. Tracking the JSON artefact across runs (regression detection on a dedicated runner) is a future workflow, not part of this PR.

## Interpreting the targets

The targets in `requirements.md` are assigned to substrates by ADP-013:

| Target | Authoritative substrate | Cross-check |
|---|---|---|
| H1 hot apply ≤ 5µs | hot probe (`hot_apply`) | micro (`hot_store_bench`) |
| R1 hot read ≤ 100µs | hot probe (`hot_get`) | loadgen (`GET` after hot preload) |
| R1-L read under writes | loadgen (`mixed_read_under_writes.yaml`, vs Valkey in the same run) | memtier |
| W1 write overhead ≤ 20µs | write probe (`write_ack`, `--fsync-policy none`) | — |
| W1-L write over loopback | loadgen (`write_loopback_pipelined.yaml`, vs Valkey in the same run) | memtier |
| W2 durable write | write probe (`write_ack` vs calibrated `device_flush`) | loadgen |
| W3 durable write throughput > 100 000 ops/s | loadgen (`write_throughput.yaml`) | — |
| X1 comparative | loadgen against each server (ADP-013 matrix) | memtier |
| B1 buffer read < 50µs | buffer probe (`buffer_read`) | — |
| C1 cold read < 5ms | cold probe (`cold_get`) | loadgen (`GET` after aging out hot) |
| RC1 recovery < 60s for 24h / 1M | micro (`recovery_bench`) | — |

The definitions behind each ID are in [requirements.md](../design/requirements.md) §Performance Targets.

When the probe and loadgen numbers disagree for the same target, the gap is the cost of the RESP+TCP path plus client-side scheduling — both real and worth knowing.

## Adding workloads

Drop a new YAML file under `tests/perf/workloads/`. The parser will reject invalid configs at load time; there is no need to register the workload in CMake. For repeatability, set `seed` explicitly rather than leaving it at the default 0.

## Adding command coverage to the load generator

`tests/perf/load/loadgen.cpp` dispatches on `op_name` against `kCmdGet` / `kCmdSet`. New RESP commands land here: add the constant, the dispatch branch, and any preload special-casing if the command requires structured input (e.g. hash field/value pairs). Mix weights and targets in the workload YAML automatically pick up the new op name.

## Adding metrics to the JSON output

The reporter emits a fixed schema at `schema_version: 1`. Additions are backwards-compatible at the schema level — downstream consumers should tolerate unknown fields. Removal or rename of an existing field is a breaking change and requires bumping the schema version (`RunReport::kSchemaVersion` in `tests/perf/framework/reporter.h`) and an ADP-013 amendment.
