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

`abyss_write_probe` drives the real in-process engine: the sequencer, the read path, the hot store, the WAL and the cold consumer pool, built as the server builds them, minus RESP and TCP. Each `write_ack` is a write (a `SET` in the fixed shape) validated the way the RESP pipeline does it, and its latency runs until the write is acknowledged. A `read` is a point read through the read path.

Before measuring, the probe calibrates the device. It runs `--flush-samples` iterations of a 4 KiB write followed by the WAL's own durable flush primitive on a scratch file in the WAL directory, timing the flush only. The result is reported as the `device_flush` operation, and the write target is derived from it:

| Durability class | Target evaluated on `write_ack` |
|---|---|
| `process_crash` | p99 ≤ 20µs (W1, write-path overhead; the flush runs in the background) |
| `power_loss` | p99 ≤ 2 × measured `device_flush` p99 + 50µs (W2) |

Point `--wal-path` at the volume under test for authoritative runs. Both `--wal-path` and `--cold-path` must be empty or absent, because the probe does not run recovery; a temporary directory is used when either is omitted.

| Flag | Default | Meaning |
|---|---|---|
| `--wal-path` | temporary | WAL directory on the volume under test |
| `--cold-path` | temporary | Cold store directory |
| `--durability` | `process_crash` | `process_crash` or `power_loss` |
| `--shards` | 64 | Shard count |
| `--segment-size-bytes` | server default | WAL segment size; must hold one maximum-size value |
| `--log-count` | server default | Physical WAL logs, a power of two |
| `--flush-samples` | 1000 | Device flush calibration samples |
| `--flush-concurrency` | `--shards` | Files flushed in parallel for the `device_flush_concurrent` calibration |
| `--prefill-entries` | 0 | Entries appended and drained before measuring, so the run starts with a retained log behind the head |
| `--quiet-threshold-s` | server default | Cold consumer quiet window |
| `--workers` | min(hardware threads, 16) | Concurrent writers. Each one blocks in the engine's write dispatch, as a reactor thread does, so this models reactors, not connections. |
| `--hot-max-memory-bytes` | server default | Hot store memory budget |
| `--fill-doorkeeper` | server default (`true`) | Fill hot from a read only on a key's second miss |
| `--mix` | `write_ack=1` | `write_ack`, plus optionally `read`: a point read of the same key distribution through the engine's read path |
| `--shape` | `fixed` | `fixed`: string keys of 21 bytes, `--value-size-bytes` values, no TTL. `trace`: keys, values, TTLs and key types after the cache traces (below) |
| `--large-fraction` | 0 | Fixed shape: this share of values is 16–64 KiB |
| `--collection-fraction` | 0.2 | Trace shape: this share of keys is sets and sorted sets, half each |
| `--metrics-out` | omitted | Also writes the raw registry scrapes to `<path>.start.prom` and `<path>.end.prom` |
| `--count-faults` | off | Diagnostic: counts page faults (`getrusage`) around each measured write: the writing thread's own on Linux (`RUSAGE_THREAD`), the whole process's on macOS, which has no per-thread count, so there a fault on any thread counts. `config.fault_scope` names which. It adds two system calls to each write's latency. |
| `--hold-wal-flushes` | off | Diagnostic, `process_crash` only: the WAL flushes nothing during the run, so cold, which persists only power-durable entries, persists nothing either. The durability window is raised to its limits (60 s, 4 GiB), so warmup plus duration may be at most 50 s. |

**Workload shape.** Keys are formatted before the run starts, since `snprintf` does not scale across threads on macOS. The `trace` shape follows the Twitter (OSDI '20) and Meta (FAST '20) cache traces, which YCSB's and memtier's defaults misrepresent:
- keys of 10–50 bytes;
- values: 30% of 16–99 bytes, 64% of 100–400 bytes (median about 200), 1% up to 16 KiB and 5% of 16–64 KiB;
- 85% of keys, of every type, are created with a TTL of 60 s, 240 s, 1 h or 1 day with ±10% jitter, and 47% of those TTLs are under 300 s;
- 10% of keys are sets and 10% sorted sets (`--collection-fraction`). A write of one is `SADD` or `ZADD`, or one write in four a `PEXPIRE`; a read is `SISMEMBER` or `ZSCORE`; strings take `SET … PX` and `GET`, so a string's TTL is renewed on each write.

With `--shape trace`, prefill creates each of the first `--prefill-entries` keys once as its type, with a value of at most 400 bytes; a set or sorted set with a TTL takes a `PEXPIRE` after its first write. The probe reads the TTL of up to 20,000 keys spread over the key space, after prefill and again after the run, and reports the share that carry one as `measured.ttl_share_after_prefill` and `measured.ttl_share_at_end`. A collection whose TTL lapses mid-run comes back without one until its next `PEXPIRE`, so the share can fall during a run. `--distribution zipfian` takes any `--zipf-theta` above 0: below 1 it is YCSB's generator, from 1 up rejection-inversion (Hörmann and Derflinger), which needs no O(key count) setup.

**Calibration.** The concurrent calibration flushes `--flush-concurrency` files in parallel and is reported as `device_flush_concurrent`. Comparing it with `device_flush` separates the device's own floor from contention that parallel flushes create. W2 is always derived from the single-file `device_flush` p99.

**Prefill.** The fixed shape prefills through the engine with one `MSET` of up to 1,024 of a shard's keys at a time; the trace shape creates each key with its own writes. Either way the probe then waits for the cold consumers to drain the prefill before measuring.

**Evaluating targets.**
- W1 and W2 are defined at ≤ 50% of saturation, so they are evaluated only on open-loop runs (`--target-rate-ops` > 0).
- A closed-loop run reports its targets as not evaluated and never claims a pass.
- `--gate` without a rate is a usage error.

**Run length.** The cold consumer starts flushing, checkpointing and acknowledging only after its quiet window (30 s by default). An authoritative run therefore lasts at least 60 s after warmup, or records a shorter `--quiet-threshold-s`. A queue read costs the same at any position, through the offset ring or the sparse index, so prefill gives the run a realistic retained log rather than read depth.

**Report contents.**
- `config` records the effective settings: durability class, shard count, segment size, log count, quiet window, value size, workers and prefill.
- `server_metrics` holds registry snapshots at `start`, taken as the warmup ends, and `end`, so WAL flush and offset persist counts, durations and batch sizes cover the same window as the client's latencies. A labelled metric keeps only its last series there.
- `server_histograms` summarises every histogram series that recorded something in that window, labels included: `count`, `mean_us`, and `p50`, `p99` and `p999`, each as `{le_us, beyond, samples}`. `le_us` is the upper bound of the bucket the quantile falls in, never interpolated; `beyond` marks a quantile past the last finite bound; `samples` is how many samples lie at or above that bucket's lower bound, which is what the quantile rests on. The mean is exact.
- `measured` holds what the probe measured itself: the cold consumers' flushed operations and flushes per second over the window (`cold_ops_flushed_per_s`, `cold_flushes_per_s`), the TTL share, and with `--count-faults` the page-fault counts (`fault_writes`, `fault_writes_faulted`, `fault_minor`, `fault_major`, and the same for writes over 100 µs).
- `driver` separates the two reasons a slot is sent late (below).
- The server's metrics snapshotter is not started; it takes shard locks once a second and is otherwise absent from this measurement.

**Driver lag and backlog.** Open loop measures each operation from its intended send time, so a late send is charged to the operations it delays (coordinated omission). A worker runs one operation at a time, and in the write probe it runs the engine inline, so a slot can be late for two reasons. If the worker's previous operation was still running when the slot fell due, the slot waited on the system under test: that is `driver.backlog_us`, from the intended send time to when the previous operation ended. Otherwise the scheduler sent it late: that is `driver.send_lag_us`, and only it decides `driver.lagging`. A run with backlog is valid data; its latencies already carry the wait. The pipelined load generator splits the same way, counting a slot as backlog when the window was full at its intended time. `driver.overloaded` compares the workers' CPU with a threshold; in the write probe the workers run the engine, so it is the engine's CPU, not only the driver's.

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
- `build` — commit (at configure time), source_tree (HEAD plus a hash of uncommitted and untracked changes, stamped at each build), preset, compiler, build_type, sanitizer. The micro-benchmarks' Google Benchmark JSON (`abyss_bench --benchmark_out`) carries the same stamp as `context.source_tree`.
- `server` — `{kind, version}` of the server measured (load generator only)
- `host` — os, kernel, cpu_model, hostname
- `workload` — the parsed workload config inlined, including `pipeline_depth` and `arrival`
- `operations.<name>` — `count`, `errors`, `throughput_ops`, `latency_us.{p50,p99,p999,max}`, `noise_floor_cv`, `saturated`, `histogram_b64` (base64-encoded HdrHistogram log)
- `config` — effective probe settings, or the measured server's reported durability settings
- `driver` — `open_loop`, `send_lag_us{count,p50,p99,max}`, `backlog_us{count,p50,p99,max}`, `lagging`, `cpu_seconds`, `cpu_per_wall_second`, `overload_threshold_cores`, `overloaded`, `cpu_affinity{available,count,cpus}`
- `sweep[]`, `sweep_result_ops` — W3 sweep steps `{offered_ops, effective_offered_ops, achieved_ops, p99_us, errors, pass, queue_entries, queue_bytes}` and the highest passing rate (load generator `--sweep` only)
- `server_metrics` — start / mid / end snapshots from the abyss-server `/metrics` endpoint (loadgen), or of the in-process registry (write probe); one series per metric name
- `server_histograms[]` — write probe only: `{series, count, mean_us, p50, p99, p999}` per histogram series over the measured window, each quantile `{le_us, beyond, samples}`
- `measured` — write probe only: what the probe measured of its own workload (TTL share, page faults)
- `targets[]` — per-target `{metric, target, actual, pass, evaluated}`; `targets_evaluated` and `targets_note` say when targets were not evaluated (closed-loop write probe)
- `pass` — top-level boolean: true iff every evaluated target passed

A one-line summary of the measured server or the in-process setup, its durability setting, and per-operation counts, errors and percentiles is also printed to stderr.

**Comparing across versions.** Compare runs only within the same durability class. A build from before durability classes running `fsync_none` compares with `process_crash`, and `group_commit` with `power_loss`. A default-against-default comparison across the change in acknowledgement point is meaningless. Report flushes per write (`rate(abyss_wal_flush_duration_seconds_count) / rate(abyss_queue_appended_total)`) alongside `power_loss` results, so a change in batching is visible apart from a change in device speed.

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
| W1 write overhead ≤ 20µs | write probe (`write_ack`, `--durability process_crash`) | — |
| W1-L write over loopback | loadgen (`write_loopback_pipelined.yaml`, vs Valkey in the same run) | memtier |
| W2 durable write | write probe (`write_ack` vs calibrated `device_flush`) | loadgen |
| W3 durable write throughput > 100 000 ops/s | loadgen (`write_throughput.yaml`) | — |
| X1 comparative | loadgen against each server (ADP-013 matrix) | memtier |
| B1 buffer read < 50µs | buffer probe (`buffer_read`) | — |
| C1 cold read < 5ms | cold probe (`cold_get`) | loadgen (`GET` after aging out hot) |
| RC1 recovery < 60s for 24h / 1M | micro (`recovery_bench`) | — |

The definitions behind each ID are in [requirements.md](../design/requirements.md) §Performance Targets.

When the probe and loadgen numbers disagree for the same target, the gap is the cost of the RESP+TCP path plus client-side scheduling — both real and worth knowing.

## Sequenced write path measurements (macOS, indicative)

These were taken on one laptop while the sequenced write path (ADP-015, #160) was finished. Every number here is **macOS and indicative**: it shows where time goes and what to look at on Linux, not what the design can reach.

- **Host.** Apple M4 Pro (10 performance and 4 efficiency cores), 24 GB, internal SSD with APFS, Darwin 25.6. The host ran endpoint agents, one with an Endpoint Security file-event extension, a virtual machine, and other agents' builds and test runs, and it was loaded throughout. Each W1 run waited up to 15 minutes for no compiler, linter or test binary to be running and a one-minute load average of at most 5; later runs waited up to 5 minutes for a load of at most 6. A run that reached its cap started anyway, and the gate was checked only at the start: a second check at the end of each run often found a compiler or tests running. The load at each run's start and end, the wait, and that check are kept beside its JSON, and each table below gives its runs' load. The same cell varied by several times at p99 from run to run, so every repeated cell is given as the median and the range over **all** its runs.
- **Build.** The `bench` preset (Release, Apple clang 21, `-O3`), in process, so there is no RESP or TCP in any figure. The probe runs up to the memory section come from one build of source tree `5dc726e+dirty.939d007e0dd3` (`build.source_tree` in each probe JSON), and the RC1 and per-buffered-key benchmarks ran on `abyss_bench` from that same build. The hot-memory rows and cold's apply of collections come from a later build, of tree `5dc726e+dirty.5b1923ce76e3`, which adds later changes to cold's apply and to the hot store's accounting; the earlier tables were not re-run on it. The benchmark JSONs from these runs carry no tree, since `abyss_bench` stamps its JSON (`context.source_tree`) only from later builds; each binary they ran (`context.executable`) came from the same build as a probe, and the tree given is that probe's. The allocation counts are older still, as their section says.
- **Defaults.** Unless a table says otherwise: 64 shards, one log, `process_crash`, a 5 s cold quiet window so that cold flushes inside the window, 60 s measured after 5 s of warmup, and 100,000 keys with 64-byte values.
- **Driver columns.** *Lagging* is the driver's own send lag (its p99 over 50 µs, or over 10% of a p99 target: 2 µs for W1). *Backlog* is the share of slots that fell due while the worker's previous operation was still running, and how long they waited: the engine's backlog, which the latencies already carry, not a fault of the driver. *Overloaded* is the workers' CPU over 1.4 cores; the workers run the engine inline, so this is engine CPU. A run with backlog or overload is valid data. Every table of probe runs carries these three or names the runs whose flags it shares; a closed loop has no schedule to lag or backlog, and a benchmark has no driver.
- **Reads.** libc++'s `std::shared_mutex` takes an internal mutex in `lock_shared` and `unlock_shared`, so readers of one shard serialise (#185). No read figure here is the design's read ceiling.
- **Histograms.** Server-side figures come from the run's `server_histograms`, scraped from the end of warmup to the end of the run, the window the client latencies cover. A quantile is the upper bound of its bucket, written `≤x`, with the number of samples at or above that bucket's lower bound in brackets where the tail is thin: `≤3,000 (7)` is seven samples of 2 ms or more. Lock holds and write stages have buckets at 1, 1.5, 2, 3, 5 and 7.5 ms. Write stages are timed on about one write in 64, chosen at random.

### W1: a write at `process_crash` (target p99 < 20 µs)

> **Placeholder: the W1 headline on the final tree (#197).** The nine 5,000/s runs are to be re-run on the final, rebased tree on a quiet host. Until then: tree **TBD**, host load **TBD**, p50 **TBD** µs, p99 **TBD** µs, p99.9 **TBD** µs, lagging, overloaded and backlog **TBD**. Every W1 figure below is from the second round of these measurements, on tree `5dc726e+dirty.939d007e0dd3` and a loaded host.

One worker, open loop, round 2's runs (tree `939d…`, loaded host). Each cell is the median over all its runs, with the range in brackets.

| Rate | Runs | p50 µs | p99 µs | p99.9 µs | Lagging | Overloaded | Backlogged slots | Backlog p99 ms |
|---|---|---|---|---|---|---|---|---|
| **5,000/s** | 9 | 4.2 (3.8–4.4) | **52 (39–7,030)** | 1,370 (1,180–29,600) | 1 of 9 | 0 of 9 | 0.68% (0.54–4.1%) | 4.5 (3.4–38) |
| 2,000/s | 3 | 7.7 (6.1–7.7) | 780 (86–1,310) | 6,650 (2,620–7,410) | 2 of 3 | 0 of 3 | 1.2% (0.39–1.6%) | 9.2 (8.9–36) |
| 20,000/s | 3 | 2.8 (2.7–2.8) | 590 (540–740) | 1,820 (1,600–7,930) | 0 of 3 | 0 of 3 | 2.9% (2.4–3.5%) | 4.8 (3.5–22) |
| 120,000/s | 1 | 2.7 | 1,750 | 7,560 | no | no | 28% | 3.8 |
| closed loop, 20 s | 1 | 2.7 | 7.4 | 340 | — | no | — | — |

**Host load.** The nine 5,000/s runs started at a one-minute load of 2.5–5.3 and ended at 2.9–7.6. A compiler or test binary was running at the end of six of them (runs 1–5 and 7), and of the first 2,000/s and 20,000/s runs and the closed loop. Run 7 waited out the gate's full 15 minutes and started at 5.28, above the gate's limit of 5.

**W1 misses on macOS: p99 52 µs at 5,000/s, the median of nine runs** (39 µs to 7 ms). The closed loop ran 221,000 writes/s for 20 s, so 5,000/s is 2% of saturation. Before the sequencer, the physical log (#181, 26cab14) measured p50 13 µs and p99 87 µs at 5,000/s with two workers in an 8 s run, its driver lagging. That comparison is across durability-window and driver changes, and an 8 s run meets fewer of the slow events a 60 s one does (see W2), so it is indicative only.

- Three runs lagged: 5,000/s run 6, which met a 58 ms stall and had a p99 of 7 ms, and two of the 2,000/s runs, where the worker sleeps between writes and sometimes wakes late on a loaded host. In the rest the driver's own send lag had a p99 under 1 µs, except the first 2,000/s run's 1.66 µs, still under the 2 µs threshold.
- **Backlog is the stalls.** 0.5–4% of slots fell due while the previous write was still running, and waited 3–40 ms at p99. A stall of a few milliseconds delays the next 15–20 writes at 5,000/s, and more at 20,000/s, which is why the p99 rises with the rate while the p50 falls.

Stages of a write at 5,000/s, the nine runs pooled (42,383 sampled writes; µs). Their driver flags and load are the 5,000/s row's above:

| Stage | p50 | p99 | p99.9 | Mean | Over 100 µs | Over 1 ms |
|---|---|---|---|---|---|---|
| `admit` | ≤0.25 | ≤0.5 | ≤5 | 0.24 | 0 | 0 |
| `lock_wait` | ≤0.25 | ≤1 | ≤5 | 0.25 | 1 | 0 |
| `decide` | ≤0.25 | ≤0.5 | ≤2.5 | 0.13 | 0 | 0 |
| `entries` | ≤0.25 | ≤1 | ≤5 | 0.20 | 2 | 0 |
| **`reserve`** | ≤0.5 | **≤25** | **≤500** | 2.15 | **109** | **17** |
| `apply` | ≤1 | ≤5 | ≤50 | 1.35 | 20 | 5 |
| `complete` | ≤2.5 | ≤10 | ≤25 | 1.86 | 2 | 0 |
| `durable` | ≤0.25 | ≤0.25 | ≤1 | 0.07 | 0 | 0 |
| lock hold | ≤2.5 | ≤25 | ≤500 | 3.68 | 130 | 22 |

The probe's figure also includes building and validating the command. Of the 130 sampled holds over 100 µs, 109 were a `reserve` over 100 µs and 20 an `apply`. The other stages' p99s are each a few µs. A sum of p99s is not the p99 of the sum, so this does not show that W1 would hold without the stalls; it shows only that no other single stage is near the budget.

**What stalls `reserve`: an A/B inside Abyss, and it is confounded.** Reserve encodes each small frame straight into the memory-mapped log while the write holds its hot shard. Three alternating pairs of 40 s runs at 5,000/s counted page faults around every write (`--count-faults`), with the WAL's flushes running as normal or held for the whole run (`--hold-wal-flushes`, which keeps the flush thread from syncing at all). The extra system calls add about 1–2 µs to every write. The runs started and ended at a load of 4.0–5.8, with a compiler or tests running at the end of three of the six.

| WAL flushes | Write p99 µs | Write p99.9 µs | Writes over 100 µs | … of them with a fault | Writes with a fault | Minor faults | Major faults | Cold flushes/s | Lagging | Overloaded | Backlogged slots |
|---|---|---|---|---|---|---|---|---|---|---|---|
| running (10,200 flushes a run) | 46–63 | 1,090–1,490 | 410–542 | 98% | 13,100–14,600 | 11,800–13,300 | 1,570–1,860 | 3,820 | 0 of 3 | 0 of 3 | 0.50–0.69% |
| held | 30–37 | 490–700 | 261–274 | 92–95% | 3,570–4,020 | 2,450–2,780 | 1,680–1,860 | **0** | 0 of 3 | 0 of 3 | 0.24–0.33% |

Of 200,000 writes a run. With the flushes held, sampled `reserve`s over 100 µs went from 14 to 10 of about 9,300, and over 1 ms from 2 to 0, and the writes over 100 µs fell by 45%.

**Two confounds mean this does not show what stalls `reserve`:**
- **Holding the WAL's flushes also stops cold.** Cold persists only power-durable entries, so in the held runs it flushed nothing (`measured.cold_flushes_per_s` 0, against about 3,820 with the flushes running): no cold flush thread, no RocksDB writes, and none of their faults or syncs. The A/B compares the WAL and cold both flushing against neither, not the WAL's flushes alone.
- **On macOS the fault count is the whole process's.** `--count-faults` reads `getrusage(RUSAGE_SELF)` there, since macOS has no per-thread fault count, so a fault taken by cold, RocksDB or the flush thread while a write was in flight counts as the write's. That 92–98% of the writes over 100 µs coincided with a fault, against 2–7% of all writes, does not show that the writing thread faulted, and with cold idle the held runs had fewer other threads to fault at all.

**So the attribution is a hypothesis, untested on macOS:** a flush writes the tail page back and leaves it clean, so the next frame written into it faults to dirty it again (holding the flushes removed about 10,000 minor faults a run, about one per flush), and those re-dirty faults would be about half the stalls; most of the rest would be the first touch of each new 16 KiB page of the log, about 1,500 in a run at this rate. Cold's own faults and CPU, gone from the held runs, fit the same counts. Abyss's `reserve` also appends to the shard's position index and may switch segment under the same lock; neither shows in these counts.

**The Linux plan (#197, which owns W1):**
- Run the same A/B with `--count-faults`, which on Linux reads `getrusage(RUSAGE_THREAD)` on each writer thread, so a write counts only its own thread's faults.
- Add an arm with cold's flushing disabled in both modes (`--quiet-threshold-s 3600`; cold still absorbs, but flushed nothing in W2's 60 s idle runs), so the WAL's flushes are the only difference. Both arms are in [To reproduce on Linux](#to-reproduce-on-linux).
- On Linux a store into a clean page under write-back waits only on a device that needs stable pages, so the re-dirty stalls should be far rarer; the first-touch faults remain. The result decides whether the WAL's small frames keep being written into the mapping under the hot lock.
- **Options if Linux shows it too:** pre-fault the pages ahead of the tail, which removes the first-touch faults; write frames with `pwrite`, which takes no fault on the mapping, at a system call per frame (not measured here); or encode small frames after the hot lock is released, as large frames already are, so a stall no longer blocks the shard, though it still holds up the appends behind it in the log.

**Heap allocations under the hold.** These counts are from the first round of these measurements, on an earlier tree: `HoldAllocTest` in a Debug build, libc++. A small `SET` over a resident key made 29 allocations while it held its shard; after the changes below it makes 16, and a Release build in the second round also counted 16:

| Where | Before | After |
|---|---|---|
| `ShardLocks` | 1 | 0 |
| Deciding | 5 | 5 |
| Building the log entry | 10 | 4 |
| `Reserve` | 8 | 5 |
| Applying | 5 | 2 |
| **Total** | **29** | **16** |

The change keeps the log entries in one vector grouped by shard, with no table for a write to one shard; applies a one-shard write straight from the decision; keeps `Reserve`'s locks and totals in per-thread scratch; and reserves the lock's horizon table before locking. With the sequencer's part alone, a one-worker closed loop went from 239–249k to 256–287k writes/s, the hold's p50 from 1.25–1.45 to 0.84–0.86 µs, and the `entries` stage's mean from 0.21 to 0.08 µs (three alternating 8 s runs each, on binaries of that earlier tree built before and after the change; not re-run since). The remaining 16 are the decision's own vectors and strings, the log entry's argument vector and its copies of the key and the value, the `Reservation`'s ranges, futures, filler and an already-ready promise, and the apply's reply vector and the stored value. The `KeyLookup` `std::function` allocates nothing (its two references fit its small buffer), and a blind `SET` never calls it.

### W2: a write at `power_loss`, and RC1

W2 is 64 workers at 5,000/s, each blocking until its write is power-durable. The target is twice the run's calibrated single-file flush p99, plus 50 µs. Both cases ran 60 s; with a 1 h quiet window cold flushed nothing during the run. W2 is defined at no more than half of saturation, but saturation at `power_loss` was not measured here, so that 5,000/s is within it is assumed, not shown.

**Host load.** All four runs waited out the gate's 5-minute cap and started at a load of 5.0–7.4, ending at 4.9–8.0, with a compiler or tests running at the end of each.

| Cold during the run | Run | p50 ms | p99 ms | Target ms | WAL flushes | Flush mean ms | Lagging | Overloaded | Backlogged slots |
|---|---|---|---|---|---|---|---|---|---|
| flushing (5 s quiet window) | 1 | 5.97 | 10.1 | 8.6 | 15,000 | 3.91 | no | no | 0.08% |
| flushing | 2 | 6.06 | 11.6 | 10.2 | 14,700 | 3.98 | no | no | 0.6% |
| idle (1 h quiet window) | 1 | 5.87 | 9.2 | 8.3 | 15,300 | 3.84 | no | no | 0.26% |
| idle | 2 | 5.91 | 8.6 | 8.4 | 15,200 | 3.87 | no | no | 0.01% |

**W2 misses by 0.3–1.6 ms on macOS, with cold idle or flushing.** That is a measured FAIL, but an indicative one: on a host this loaded the miss is inside the noise. At equal durations cold's flushing adds 2–3% to the WAL's mean flush and 1–3 ms to the write p99, against a run-to-run spread of the same order, so it is not what makes W2 miss. The earlier comparison that had cold-idle W2 passing set an 8 s run against 60 s ones, and fewer slow flushes fall in 8 s. Each write waits for the flush in flight and then its own, so its p99 sits near twice the in-run flush p99, and the in-run flushes run longer than the calibration's, with 64 shards' appends behind them and other processes' syncs beside them: `F_FULLFSYNC` flushes the whole drive cache. On Linux `fdatasync` syncs one file's data, but on a drive with a volatile write cache it also issues a device-wide cache FLUSH, so other writers' syncs lengthen the WAL's there too; only a drive with power-loss protection, which can skip the FLUSH, avoids that.

The WAL flush histogram's buckets step from 5 to 10 ms, so its quantiles here are `≤5 ms` at p50 and `≤10 ms` at p99 in every run; the means are exact.

RC1 (`BM_RecoveryColdHot`, target: a 24 h queue of 1M entries recovered in under 60 s), two runs of each case. Cold has committed 16 entries a shard, so it absorbs the whole log; "dropped" cases drop the log's pages from the page cache between writing the log and `Open` (`msync(MS_INVALIDATE)` on macOS, `posix_fadvise(POSIX_FADV_DONTNEED)` on Linux, since macOS's `purge` needs root), and at most 0.03 MiB stayed resident. *Total* is `Open` plus the rebuild; it leaves out creating the RocksDB store and the teardown, which Google Benchmark's `real_time` includes. A benchmark, so it has no driver; `abyss_bench` from the build of tree `939d…`, at a load of 4.4–5.3, with no compiler or tests running at the end of either run:

| Entries | Keys | Value | Hot | Log | Page cache | Open ms | Rebuild ms | Total s | Hot after |
|---|---|---|---|---|---|---|---|---|---|
| 1M | 100k | 1 B | 1 GiB | 69 MiB | warm | 73–81 | 1,357–1,374 | 1.4 | 100k keys, 24 MiB |
| 1M | 100k | 1 B | 1 GiB | 69 MiB | dropped | 158–163 | 1,333–1,346 | 1.5 | 100k keys, 24 MiB |
| 1M | 1M | 1 KiB | 256 MiB | 1,045 MiB | warm | 296–302 | 3,121–3,122 | 3.4 | 209k keys, 254 MiB |
| 1M | 1M | 1 KiB | 256 MiB | 1,045 MiB | dropped | 1,591–1,774 | 2,992–3,259 | 4.6–5.0 | 209k keys, 254 MiB |

**RC1 holds with the log read from the device**: 1M entries of 1 KiB opened and rebuilt in 4.6–5.0 s (5.03 s at worst, and 5.26 s of `real_time` with the store's creation and teardown), about 5 µs an entry, so 60 s would cover about 11–12M such entries on this laptop. Dropping the cache moves the cost into `Open`, which walks the whole log to rebuild its index and brings it back into the page cache, so the rebuild after it reads from memory either way. The second log is about four times hot, so replay evicts as it goes.

**No forced drain, and none is possible.** The coordinator hands each scan batch to cold before hot, and cold advances its drained seq as it absorbs each entry, so hot's eviction horizon always covers everything hot has replayed. A cold commit that lags, or is withheld entirely as here, only means cold absorbs more of the log. Hot can fail to make room only behind an entry cold cannot parse, which pins the horizon, or with one entry larger than a shard's budget; a flush moves the horizon in neither case. So the forced-drain path was dead, and #160 removed it: replay instead fails loudly past twice the hot limit. Until now this benchmark also left hot's drained horizon unwired, so hot treated everything as drained; it is now wired as the server wires it.

### Shard-lock hold with readers

`abyss_sequencer_lock_hold_seconds`, 50/50 `SET`/`GET`, 20,000 operations/s in total, Zipf 0.99 unless stated, one run each (µs; samples in brackets where the tail is thin):

| Workers | Values | p50 | p99 | p99.9 | Mean | `lock_wait` p99 | Lagging | Overloaded | Backlogged slots |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 64 B | ≤1 | ≤10 | ≤500 (10) | 2.5 | ≤0.5 | no | no | 1.5% |
| 8 | 64 B | ≤2.5 | ≤50 | ≤1,500 (16) | 7.8 | ≤2.5 | no | no | 0.55% |
| 64 | 64 B | ≤2.5 | ≤100 | ≤1,500 (15) | 6.8 | ≤1 | yes | no | 0.01% |
| 64, uniform | 64 B | ≤2.5 | ≤250 | ≤1,500 (27) | 8.9 | ≤1 | yes | no | 0.05% |
| 1 | 5% at 16–64 KiB | ≤2.5 | ≤25 | ≤250 (23) | 2.4 | ≤1 | no | no | 18% |
| 8 | 5% at 16–64 KiB | ≤2.5 | ≤1,500 | ≤2,000 (37) | 54 | ≤5 | yes | no | 7.6% |
| 64 | 5% at 16–64 KiB | ≤2.5 | ≤1,500 | **≤3,000 (19)** | 93 | ≤1,000 | yes | yes | 0.12% |

- The p99.9 with 64-byte values is the same `reserve` stall at every worker count. With the large-value mix it is the stall made longer: the mix adds about 20 MB/s of log, so each flush, and each fault behind one, takes longer. The large frames themselves are copied before the lock and encoded after it.
- With 64 workers on 14 cores a holder can be descheduled, which shows from p99 up, and in `lock_wait`'s p99 of up to 1 ms with the large mix.
- One worker cannot carry 20,000/s of the large mix: 18% of its slots waited on the write before, and the cell's latencies are mostly that backlog.
- `GET`s took 0.8–1.1 µs at p50 and 12–13 µs at p99 with 8 or 64 workers and 64-byte values, and 170 µs at p99 with one worker, whose reads queue behind its own writes.
- **Host load.** The runs started at 5.1–6.5 and ended at 4.7–6.6, except the uniform row, which started at 11.1 and ended at 8.2 while another build ran. That row and the first (one worker, 64 B) both waited out the gate's 5-minute cap, and a compiler or tests were running at the end of four of the seven runs.

**Fill and publish waits, and park against yield.** Each is a wait past the 2 µs spin, in the same runs, whose driver flags and load are in the table and note above:

| Workers, values | Fill waits | Fill p50 / p99 / p99.9 µs | Publish waits | Publish p50 / p99 / p99.9 µs |
|---|---|---|---|---|
| 8, 64 B | 1,950 | ≤50 / ≤2,500 (42) / ≤10,000 (5) | 79 | ≤50 / ≤5,000 (4) / ≤5,000 (4) |
| 64, 64 B | 4,580 | ≤50 / ≤2,500 (75) / ≤5,000 (10) | 282 | ≤100 / ≤5,000 (4) / ≤5,000 (4) |
| 8, 5% at 16–64 KiB | 36,000 | ≤50 / ≤2,500 (561) / ≤5,000 (76) | 1,330 | ≤50 / ≤1,000 (18) / ≤10,000 (4) |
| 64, 5% at 16–64 KiB | 79,000 | ≤100 / ≤2,500 (2,026) / ≤5,000 (217) | 5,340 | ≤100 / ≤1,000 (73) / ≤10,000 (30) |

The median waiter yields for 50–100 µs; one in a hundred yields for milliseconds. The long waits are head-of-line blocking: appends on a log are published in reservation order, so a small write reserved behind a 16–64 KiB fill waits for that fill, and so does every shard's publish behind it on the same log; a fill stalled on a fault holds them all. The large mix multiplies both the fill waits and the publish waits about seventeen-fold. Only the blob lane (#162), which takes large values out of the log's order, removes this. Parking the waiters (`std::atomic::wait`) instead of yielding would save the CPU they burn, which is part of why the 64-worker large-mix run is overloaded, but it would not shorten a wait. And in an asynchronous front end, where one reactor thread serves many connections, parking and yielding would both stall the reactor and every connection on it; the wait has to leave the reactor's thread, which the blob lane does.

### The trace workload

`--shape trace` over 4 shards of 1M keys each (4M keys, 1.8 GB in hot after prefill), 8 workers, every key prefilled, one run per cell. On Linux the same should run over 64 shards. **TTL share:** 85.3% of the sampled keys carried a TTL after prefill, and 80.5–81.7% at the end of each trace run, once expired keys were gone and collections whose TTL had lapsed came back without one. The Zipf generators are not scrambled: key index 0 is the most popular, so at θ 1.2 over 4M keys it takes about 18.7% of operations, all on one of the four shards (see the collapse cells below for each shard's share).

Latencies in µs as p50 / p99 / p99.9; lock hold as bucket bounds; *Backlog* is the share of slots that waited on the worker's previous operation.

**Saturation**, closed loop, Zipf 0.99, so no schedule to lag or backlog; load 4.9–6.0 at the start and 6.2–7.5 at the end:

| Mix (reads/writes) | Operations/s | Writes/s | Read | Write | Lock hold | Overloaded (cores) | Errors |
|---|---|---|---|---|---|---|---|
| 95/5 | 638,000 | 31,900 | 1.2 / 143 / 1,550 | 14 / 1,410 / 2,120 | ≤5 / ≤500 / ≤2,000 | yes (2.5) | 1 |
| 50/50 | 133,000 | 66,700 | 1.7 / 892 / 2,000 | 15 / 1,680 / 3,170 | ≤5 / ≤1,000 / ≤2,000 | yes (1.9) | 2 |
| 20/80 | 93,800 | 75,100 | 2.6 / 1,330 / 2,220 | 15 / 1,750 / 3,500 | ≤5 / ≤1,500 / ≤2,000 | yes (1.8) | 0 |

The errors are reads whose cold probe timed out (`Deadline exceeded`) under saturation. Writes saturate at 67,000–75,000/s on one log. Two things hold them there:
- Every append waits for the reservations ahead of it in the log to be filled and published in order. Fill waits run at a median of 50–100 µs and a p99 of 1–2.5 ms here, behind 16–64 KiB values and page faults: the head-of-line wait #162's blob lane removes.
- A trace write holds its shard for 2.5–5 µs at p50, against about 1 µs for a 64-byte `SET` with no TTL: its TTL is filed in the TTL index, its values are larger, and four shards under Zipf contend.

**Open loop, 95/5 at 100,000/s** (16% of saturation). Load 5.8 and 6.0 at the start of the Zipf runs and 5.3–5.6 at their end; the uniform run waited out the gate's 5-minute cap, started at 3.6 and ended at 9.9:

| Keys | Read | Write | Lock hold | Hot hits | Lagging | Overloaded (cores) | Backlog |
|---|---|---|---|---|---|---|---|
| Zipf 0.99 | 1.3 / 1,380 / 3,420 | 7.2 / 1,620 / 3,660 | ≤5 / ≤1,000 / ≤2,000 | 96.4% | no | yes (2.2) | 13% |
| Zipf 1.2 | 0.83 / 1,240 / 2,610 | 6.5 / 1,510 / 2,870 | ≤2.5 / ≤1,000 / ≤2,000 | 98.8% | no | yes (3.0) | 8.8% |
| Uniform | 1.7 / 1,700 / 3,970 | 8.6 / 1,890 / 4,140 | ≤5 / ≤1,000 / ≤2,000 | 92.0% | yes | yes (2.2) | 17% |

A worker issues its reads and writes one after another, as a reactor does, so a stalled write delays the reads queued behind it on that worker: 9–17% of slots waited on the operation before them, and the read p99 of 1.2–1.7 ms is that wait. The workers' 2.2–3.0 cores are the engine's CPU, over the 1.4-core threshold.

**At a tenth of saturation**, 50/50 at 12,500/s and 20/80 at 7,500/s. Load 5.0–5.9 at the start and 4.8–7.2 at the end, with a compiler or tests running at the end of the Zipf 1.2 20/80 and uniform 50/50 runs:

| Keys, mix | Read | Write | Lock hold | Hot hits | Lagging | Overloaded | Backlog |
|---|---|---|---|---|---|---|---|
| Zipf 0.99, 50/50 | 1.7 / 1,350 / 7,920 | 8.3 / 1,650 / 6,330 | ≤5 / ≤1,500 / ≤2,000 | 96.6% | yes | no | 5.0% |
| Zipf 0.99, 20/80 | 2.0 / 1,260 / 2,660 | 8.6 / 1,590 / 3,380 | ≤5 / ≤1,500 / ≤2,000 | 96.4% | yes | no | 2.8% |
| Zipf 1.2, 50/50 | 1.2 / 1,180 / 10,600 | 6.0 / 1,600 / 11,800 | ≤2.5 / ≤1,500 / ≤3,000 | 98.8% | yes | no | 3.8% |
| Zipf 1.2, 20/80 | 1.3 / 960 / 2,260 | 5.8 / 1,420 / 2,820 | ≤2.5 / ≤1,000 / ≤2,000 | 98.7% | yes | no | 1.8% |
| Uniform, 50/50 | 4.7 / 1,440 / 2,990 | 12 / 1,660 / 3,310 | ≤10 / ≤1,500 / ≤2,000 | 92.3% | yes | no | 5.9% |
| Uniform, 20/80 | 5.6 / 1,390 / 2,920 | 12 / 1,620 / 3,390 | ≤10 / ≤1,500 / ≤3,000 | 92.5% | yes | no | 3.0% |

- Every cell lagged by its driver's own measure, with send-lag p99s of 3.1–3.3 µs against the 2 µs W1 threshold, which eight workers on a loaded host rarely meet; the latencies carry the backlog, not that lag.
- The lock hold's p99 is in the 1–1.5 ms bucket and its p99.9 in the 1.5–2 ms bucket in nearly every cell: the `reserve` stall again, longer here than in the W1 runs because the large values keep the device busier.
- Uniform keys over 4M take about twice Zipf's hold at p50: the hot set no longer fits in cache.

**At a third of saturation the write-heavy mixes collapse under Zipf 0.99 (#196).** 50/50 at 50,000/s and 20/80 at 30,000/s (38% and 32% of Zipf 0.99's saturation) reached p99s of 0.38–0.43 s, with 32–43% of slots backlogged; uniform keys reached 36 ms at 50/50 and 5 ms at 20/80, and Zipf 1.2 stayed at 2.3–2.6 ms. Every cell lagged and four were overloaded. *Load* is the one-minute load at the run's start and end, and *busy* marks a compiler or tests running at its end:

| Keys, mix, rate | Read p50 µs | Write p50 µs | Read p99; write p99 / p99.9 ms | Lagging | Overloaded (cores) | Backlog | Backlog p99 ms | Hottest shard | Load |
|---|---|---|---|---|---|---|---|---|---|
| Zipf 0.99, 50/50, 50,000/s | 11 | 74 | 383; 383 / 478 | yes | yes (1.8) | 43% | 406 | 26.9% | 4.8 → 7.0 |
| Zipf 0.99, 20/80, 30,000/s | 4.5 | 17 | 427; 429 / 509 | yes | yes (1.5) | 32% | 493 | 26.9% | 5.7 → 8.8 |
| Zipf 1.2, 50/50, 50,000/s | 1.3 | 7.3 | 2.4; 2.6 / 5.9 | yes | yes (2.0) | 26% | 4.1 | 33.0% | 6.0 → 6.2, busy |
| Zipf 1.2, 20/80, 30,000/s | 1.4 | 7.0 | 2.2; 2.3 / 5.0 | yes | no (1.2) | 22% | 3.8 | 33.0% | 5.9 → 5.4, busy |
| Uniform, 50/50, 50,000/s | 89 | 167 | 36; 36 / 77 | yes | yes (1.8) | 49% | 51 | 25.0% | 5.2 → 12.3, busy |
| Uniform, 20/80, 30,000/s | 6.1 | 19 | 5.3; 5.3 / 11 | yes | no (1.3) | 33% | 7.4 | 25.0% | 3.6 → 5.4, busy |

The Zipf 0.99 20/80 run had two read errors, cold probes past their deadline. No admission waited on the durability window. The slowest WAL flushes fell in the 0.1–0.25 s bucket, two in the Zipf 0.99 50/50 run and one in its 20/80 run; the one flush slower than that, in the 0.25–1 s bucket, was in the Zipf 1.2 50/50 run, which did not collapse.

**The hottest shard does not explain it.** *Hottest shard* is that shard's share of the offered operations, from the probe's own key distributions (`tests/perf/framework/key_dist.*`, the eight workers' seeds, 80M draws) over the trace shape's 4M keys, each placed by the server's `ShardForKey` over 4 shards; the analytic Zipf gives the same shares to within 0.4 points. Zipf 0.99's hottest key takes 5.9% of operations and its next hottest fall on the other shards, so its hottest shard takes 26.9%, barely above uniform's 25.0%. Zipf 1.2's key 0 alone takes 18.7%, and its shard 33.0%.
- At 50/50 and 50,000/s, Zipf 0.99 offered its hottest shard 13,500 operations/s (6,700 writes), and Zipf 1.2 offered its own 16,500 (8,200 writes) without collapsing.
- The saturation runs put 26.9% of 133,000 and of 93,800 operations/s on that same Zipf 0.99 shard: 35,900 and 25,300 (18,000 and 20,200 writes), 2.7 and 3.1 times what the collapse cells offered it. That bounds what the shard can carry from below; it is not the shard's own ceiling.
- The lock histograms do not single Zipf 0.99 out either. Its mean write hold of 41–44 µs, against a p50 of 5 µs, and its 1.4% of holds of 1 ms or more match uniform keys' 43–44 µs and 1.5% at the same rates. The mean `lock_wait` rose with the rate under every distribution, from 13–24 µs at a tenth of saturation to 21–41 µs here: 33–41 µs under Zipf 0.99, 30–38 µs for uniform keys and 21–30 µs under Zipf 1.2.

So the cause is not isolated: neither the hottest shard's offered load nor the lock holds explain why Zipf 0.99 queues for 0.4 s where Zipf 1.2, with a hotter shard, keeps up. What is missing:
- one shard's own saturation, from a closed-loop `--shards 1` run of each mix, which these runs do not include;
- lock waits per shard, since `abyss_sequencer_phase_seconds{phase="lock_wait"}` carries no shard label;
- the scrambled-Zipf re-run #196 lists, to separate key-to-shard concentration from per-key contention;
- the same cells over 64 shards on Linux.

For an operator, the signal for writers queueing on a shard's lock is `abyss_sequencer_phase_seconds{phase="lock_wait"}` (#196; #185 tracks a reader-scalable shard lock). Here it rose with the load under every distribution, so it did not mark this collapse ahead of the latency.

**Maintenance holds at 1M keys a shard** (`abyss_hot_maintenance_hold_seconds`, the 4 GiB runs). About 25% of keys carry a 60 s TTL, so 0.8–1.1M expired in each run's window; ranges over the 18 runs, the three saturation runs and the 15 open-loop cells above, whose driver flags and load are in their tables:

| Pass | Holds a run | p50 µs | p99 µs | p99.9 µs |
|---|---|---|---|---|
| `ttl` | 13,200–18,600 | ≤50–100 | ≤100–1,000 | ≤250–1,500 |
| `tombstones` | 228–266 | ≤0.25–25 | ≤10–250 | ≤25–500 |
| `deadline`, `memory`, `parked` | 224–236 | ≤0.25 | ≤0.25–2.5 | ≤0.5–25 |

A pass stops at 64 keys or about 1 ms, so a hold over 1 ms is the holder descheduled or faulting, with 8 workers and the eviction worker on a loaded host.

**The doorkeeper, with writes.** Hot at 512 MiB, about a quarter of the data, Zipf 0.99, one run each:

| Mix, rate | Doorkeeper | Hot hits | Cold reads/s | Fills installed | Read p50 µs | Read errors | Lagging | Overloaded (cores) | Backlog | Load |
|---|---|---|---|---|---|---|---|---|---|---|
| 95/5, 100,000/s | on | 76.6% | 22,200 | 409,000 | 1.4 | 4 | no | yes (2.4) | 15% | 6.0 → 5.8, busy |
| 95/5, 100,000/s | off | 80.9% | 18,100 | 1,026,000 | 1.4 | 4 | no | yes (2.4) | 15% | 3.9 → 6.5 |
| 50/50, 12,500/s | on | 67.9% | 2,000 | 12,300 | 2.2 | 1 | yes | no (0.9) | 7.6% | 2.8 → 3.9 |
| 50/50, 12,500/s | off | 70.6% | 1,830 | 106,000 | 1.9 | 1 | yes | no (0.9) | 7.9% | 5.9 → 4.4 |

At 95/5 both runs were overloaded, at 2.4 cores with 15% of slots backlogged; at 50/50 both lagged; and every run had read errors, cold probes past their deadline. On and off shared those conditions, so the comparison between them stands as indicative; the absolute rates do not. Cold reads are cold hits plus misses, over the 60 s window. With writes in the mix the doorkeeper cost 2.7–4.3 points of hot hit rate and 9–23% more cold reads, for 2.5–8.7 times fewer fills. Reads alone went the other way: in `BM_ReadMixFill` (200,000 keys, hot at a tenth of the data, one thread, 500,000 `GET`s), measured on an earlier tree when the doorkeeper landed and not re-run here, the doorkeeper raised Zipf 0.99's hit rate from 68.3% to 72.0%. Why it costs cold reads here is not shown: misses rose from 60,000 to 138,000 at 95/5 and from 3,700 to 15,000 at 50/50, but the second-miss rule bounds a key's probes before its fill, so repeated probes of expired keys cannot be the whole cause. Keep the default until Linux repeats this.

**Rehashes.** The server sizes each shard's map for `max_memory_bytes` at 256 bytes a key. No run rehashed, before or during the window: at 4 GiB each shard was sized for 4.2M keys and held 1M; at 1 GiB (one 50/50 run at 12,500/s: lagging, not overloaded, 7.3% backlog, load 4.4 → 4.1) and 512 MiB (the doorkeeper runs) the shards filled their memory at 594,000 and 288,000 trace keys, short of the 1.05M and 524,000 they were sized for, since a trace key costs about 450 bytes in hot. A shard of keys that hot counts at under 256 bytes each could still outgrow its map and rehash; 16-byte values, at 256, sit on that line (#186).

### Memory

Benchmarks, so no driver columns; each case ran in its own process, at a load of 4.1–5.3 with no compiler or tests running at the end of any.

**Per hot key** (`BM_HotMemoryPerKey`, on `abyss_bench` from the later build, tree `5b19…`; 1M string keys of 21 bytes, which `std::string` keeps inline; no stub share, so the figure is per key):

| Value | Process growth per key | Hot's own count per key |
|---|---|---|
| 16 B | 245 B | 256 B |
| 64 B | 325 B | 305 B |
| 230 B (the Twitter median) | 502 B | 465 B |

- `sizeof(Entry)` is 176 bytes. The map node adds the key's `std::string` and two words, and the bucket array about 8 bytes a key, which hot now counts.
- `Value` is a `std::variant` of 72 bytes, sized by `ZsetValue`. A string needs 24 of them.
- Hot's own count is within 11–37 bytes a key of the process's growth: over for the smallest values, under once the value is on the heap, from the string's capacity rounding and the allocator's overhead.
- The stub cache's index is reserved for its cap up front, a fixed cost of about 0.23% of `max_memory_bytes` (2% of the budget at 80 bytes a stub, plus an eighth, at 8 bytes a bucket), which hot counts: about 9 MiB at the 4 GiB default. With the benchmark's 64 GiB budget and the default 2% stub share it added about 155 bytes to each of its 1M keys, which is why the figure is taken with no stub share.
- What would win most back: boxing the collection alternatives so `Value` is 32 bytes (40 a key), 32-bit coarse ticks for `linked_at` and `accessed` (8), and moving the TTL list's three words into the TTL index for keys without a TTL (24).

**Per buffered key** (`BM_BufferMemoryPerEntry`, 1M distinct `SET`s). These rows are from the earlier build, tree `939d…`, unlike the hot rows above, and were not re-run on the later tree:

| Value | Process growth per key | The buffer's estimate |
|---|---|---|
| 16 B | 588 B | 544 B |
| 230 B | 845 B | 784 B |

`sizeof(BufferEntry)` is 416 bytes. The pending order is a `std::deque` of pointers, 8 bytes a key, not a multiset; a deque is the right structure, and the cost is the entry itself. The estimate now counts the entry, its map node and the key's two copies, and is within 7–8% of the process's growth, so `buffer_high_water_bytes` is reached close to the memory it names.

**Copies of a 1 MiB `SET`.** One copy, made before the hot lock for the log entry, then the encode into the log, which `Complete` does after the lock. Hot keeps the request's own buffer, and nothing large is copied under the lock (`SequencerAllocTest.ALargeSetCopiesItsValueOnceAndMovesIt`, `abyss_sequencer_locked_copy_bytes_total`). A refused `Reserve` keeps the copy for the retry, on one shard or several; a debug check confirms each copy still equals the request's argument it stands for.

### Cold's apply of collections that replace their key

A write flagged as replacing its key's state, which the sequencer logs when a `SADD` or `ZADD` creates its key, reaches cold as a `DEL` and then the write, so cold never keeps a stale or expired collection under a new key; the `DEL` first probes whether the key exists. Measured with `abyss_bench` from the later build, tree `5dc726e+dirty.5b1923ce76e3`, after that change landed; a benchmark, so no driver, at a load of 3.7–5.7 with no compiler or tests running at the end of either run:

| Benchmark | Writes | Flush to RocksDB s | Flushed writes/s |
|---|---|---|---|
| `BM_ColdReplayCollections`, each write creating its key, not flagged | 1M | 18.3–18.7 | 53,400–54,600 |
| the same, flagged as replacing | 1M | 22.4–23.2 | 43,100–44,600 |

Two runs each, Release; each write a `SADD` or `ZADD` of four members to its own new key, so every `DEL`'s probe misses. Absorbing the scan into the buffer took 0.16–0.20 s either way. **The `DEL` adds 22–24% to cold's flush time** for a log of creating writes, the worst case, against the rough +45% a Debug build suggested. In the write probe on the same build, a mix of only sets and sorted sets (`--collection-fraction 1`, 20/80 at 7,500/s, Zipf 0.99, about a million TTL expiries a run, so many writes recreate their key) against the same mix of strings, one run each:

| Keys | Cold flushed writes/s | Cold checkpoint mean ms | Write p50 / p99 µs | Lagging | Overloaded (cores) | Backlog | Load |
|---|---|---|---|---|---|---|---|
| sets and sorted sets (`--collection-fraction 1`) | 3,370 | 9.6 | 7.4 / 660 | yes | no (0.7) | 0.47% | 4.2 → 6.2, busy |
| strings (`--collection-fraction 0`) | 3,190 | 10.5 | 8.8 / 1,670 | yes | no (0.7) | 3.5% | 5.8 → 5.4 |

Both lagged by the driver's measure, as the trace cells at a tenth of saturation did; cold kept up at that rate with room to spare.

### To reproduce on Linux

The authoritative skew, read-scaling, W3 and W1-L figures need Linux on perf-grade hardware: performance governor, isolated cores, the WAL and cold directories on the NVMe device under test, and `ABYSS_PERF_AUTHORITATIVE=1`. Each block below is the set of runs behind one table above, with the laptop's arguments; the paths differ, and the laptop's quiet gate is left out. Before each run the laptop waited, up to a cap, for no compiler, linter or test binary to be running and for a low load (see **Host** above); a dedicated Linux host should not need that, but record its load before and after each run as `run` and `bench` do, and run nothing else beside them. Blocks marked as not run on the laptop are the follow-ups this section names. Every probe run writes `<name>.json`, plus `<name>.start.prom` and `<name>.end.prom`, and every benchmark run `<name>.json`; `build.source_tree` in a probe's JSON, and `context.source_tree` in a benchmark's, names the tree the binary was built from.

```bash
cmake --preset bench
cmake --build build/bench -j --target abyss_write_probe abyss_bench abyss_sequencer_alloc_tests
export ABYSS_PERF_AUTHORITATIVE=1
P=build/bench/tests/perf/probe/abyss_write_probe
B=build/bench/tests/perf/micro/abyss_bench
# Both directories must be empty or absent before each run.
run() {
  name=$1; shift
  rm -rf /mnt/nvme/wal /mnt/nvme/cold
  uptime > $name.load
  $P --wal-path /mnt/nvme/wal --cold-path /mnt/nvme/cold "$@" --output $name.json --metrics-out $name
  uptime >> $name.load
}
bench() {  # one process per case, since freed memory is reused
  name=$1; shift
  uptime > $name.load
  $B "$@" --benchmark_out=$name.json --benchmark_out_format=json
  uptime >> $name.load
}
summary() {  # latency, the two driver figures, and one histogram family
  jq '{w: .operations.write_ack.latency_us, r: .operations.read.latency_us,
       lag: .driver.lagging, ovl: .driver.overloaded, send: .driver.send_lag_us,
       backlog: .driver.backlog_us, measured,
       h: [.server_histograms[] | select(.series | test("'"$2"'"))]}' $1.json
}
```

**W1, its stages, and the stall A/B.**

```bash
W1="--durability process_crash --workers 1 --duration 60 --warmup 5 --quiet-threshold-s 5 --flush-samples 100"
for i in 1 2 3 4 5 6 7 8 9; do
  run w1-r5000-$i $W1 --target-rate-ops 5000
  case $i in 1|2|3)
    run w1-r2000-$i $W1 --target-rate-ops 2000
    run w1-r20000-$i $W1 --target-rate-ops 20000 ;;
  esac
done
run w1-r120000-1 $W1 --target-rate-ops 120000
run w1-closed-1 --durability process_crash --workers 1 --duration 20 --warmup 5 \
    --quiet-threshold-s 5 --flush-samples 100
summary w1-r5000-1 sequencer
A="--durability process_crash --workers 1 --duration 40 --warmup 5 --flush-samples 100 \
   --target-rate-ops 5000 --count-faults"
for i in 1 2 3; do
  run ab-on-$i $A --quiet-threshold-s 5
  run ab-held-$i $A --quiet-threshold-s 5 --hold-wal-flushes
done
summary ab-held-1 'phase="reserve"|lock_hold'
```

On Linux `--count-faults` counts each writer thread's own faults (`getrusage(RUSAGE_THREAD)`, `config.fault_scope` "thread" in the JSON); on macOS, which has no per-thread count, it counts the whole process's ("process"). Not run on the laptop: the arm that removes the A/B's first confound, with cold flushing in neither mode, so the WAL's flushes are the only difference. Check that `measured.cold_flushes_per_s` is 0 in all four.

```bash
for i in 1 2 3; do
  run ab-coldidle-on-$i $A --quiet-threshold-s 3600
  run ab-coldidle-held-$i $A --quiet-threshold-s 3600 --hold-wal-flushes
done
```

**Allocations under the hold.** Release, unlike the laptop's Debug figure:

```bash
build/bench/tests/unit/abyss_sequencer_alloc_tests --gtest_filter='HoldAllocTest.*'
```

**W2.** Equal durations, cold flushing and cold idle:

```bash
W2="--durability power_loss --workers 64 --target-rate-ops 5000 --duration 60 --warmup 5 --flush-samples 100"
for i in 1 2; do
  run w2-flushing-$i $W2 --quiet-threshold-s 5
  run w2-idle-$i $W2 --quiet-threshold-s 3600
done
summary w2-flushing-1 wal_flush
```

**RC1.** `BM_RecoveryColdHot` writes under `TMPDIR`, so point that at the device. Its `drop_cache:1` cases drop the log's pages before `Open` with `posix_fadvise(POSIX_FADV_DONTNEED)` and report what stayed resident as `cached_mib`. A global `echo 3 > /proc/sys/vm/drop_caches` before the run would not help: the log is written inside the run.

```bash
for i in 1 2; do
  TMPDIR=/mnt/nvme bench rc1-$i --benchmark_filter=BM_RecoveryColdHot
done
```

**Shard-lock hold, and fill and publish waits.**

```bash
H="--durability process_crash --target-rate-ops 20000 --mix write_ack=0.5,read=0.5 --zipf-theta 0.99 \
   --duration 60 --warmup 5 --quiet-threshold-s 5 --flush-samples 20"
run hold-w1-zipfian-l0 $H --workers 1 --distribution zipfian --large-fraction 0
run hold-w8-zipfian-l0 $H --workers 8 --distribution zipfian --large-fraction 0
run hold-w64-zipfian-l0 $H --workers 64 --distribution zipfian --large-fraction 0
run hold-w64-uniform-l0 $H --workers 64 --distribution uniform --large-fraction 0
run hold-w1-zipfian-l0.05 $H --workers 1 --distribution zipfian --large-fraction 0.05
run hold-w8-zipfian-l0.05 $H --workers 8 --distribution zipfian --large-fraction 0.05
run hold-w64-zipfian-l0.05 $H --workers 64 --distribution zipfian --large-fraction 0.05
summary hold-w64-zipfian-l0.05 'lock_hold|lock_wait|fill_wait|publish_wait'
```

**The trace workload: saturation, 95/5, the low-rate cells, the collapse cells, the doorkeeper, maintenance and rehash.** The laptop ran 4 shards of 1M keys. On Linux, run the same, then 64 shards with `--key-count 64000000 --prefill-entries 64000000` where memory allows (about 500 bytes a key in hot, plus the log and cold on disk); find each mix's saturation with the `sat` runs first, and keep the open-loop rates at or below half of it.

```bash
T="--shape trace --shards 4 --key-count 4000000 --prefill-entries 4000000 --workers 8 \
   --duration 60 --warmup 5 --quiet-threshold-s 5 --flush-samples 20"
Z99="--distribution zipfian --zipf-theta 0.99"
Z12="--distribution zipfian --zipf-theta 1.2"
UNI="--distribution uniform"
M05="--mix write_ack=0.05,read=0.95"
M50="--mix write_ack=0.5,read=0.5"
M80="--mix write_ack=0.8,read=0.2"
run trace-sat-z99-w05 $T $Z99 $M05 --target-rate-ops 0
run trace-sat-z99-w50 $T $Z99 $M50 --target-rate-ops 0
run trace-sat-z99-w80 $T $Z99 $M80 --target-rate-ops 0
for d in z99 z12 uni; do
  case $d in z99) D=$Z99;; z12) D=$Z12;; uni) D=$UNI;; esac
  run trace-$d-w05 $T $D $M05 --target-rate-ops 100000
  run trace-$d-w50-low $T $D $M50 --target-rate-ops 12500
  run trace-$d-w80-low $T $D $M80 --target-rate-ops 7500
  run trace-$d-w50 $T $D $M50 --target-rate-ops 50000
  run trace-$d-w80 $T $D $M80 --target-rate-ops 30000
done
for dk in true false; do
  run trace-dk-$dk-w05 $T $Z99 $M05 --target-rate-ops 100000 --hot-max-memory-bytes 536870912 \
      --fill-doorkeeper $dk
  run trace-dk-$dk-w50 $T $Z99 $M50 --target-rate-ops 12500 --hot-max-memory-bytes 536870912 \
      --fill-doorkeeper $dk
done
# Hot at 1 GiB: the memory pass, and whether a memory-full shard outgrows its map.
run trace-mem1g-w50 $T $Z99 $M50 --target-rate-ops 12500 --hot-max-memory-bytes 1073741824
summary trace-mem1g-w50 'maintenance|rehash'
# Doorkeeper: hot hits, cold hits and misses, fills, over the measured window.
for dk in true false; do for m in w05 w50; do
  paste <(grep -E '^abyss_(hits|misses)_total|^abyss_hot_fills_total' trace-dk-$dk-$m.start.prom) \
        <(grep -E '^abyss_(hits|misses)_total|^abyss_hot_fills_total' trace-dk-$dk-$m.end.prom) |
    awk '{print $1, $4 - $2}'
done; done
```

Prefill's rehashes, if any, are in the `start` scrape, taken after prefill and warmup: read `abyss_hot_rehash_seconds` from each run's `.start.prom`, and the run's own from the difference to `.end.prom`.

**Memory, and cold's apply of collections.** One process per memory case, since freed memory is reused, and two runs of the collections benchmark:

```bash
for v in 16 64 230; do
  bench mem-hot-$v --benchmark_filter="BM_HotMemoryPerKey/keys:1000000/value:$v/"
done
for v in 16 230; do
  bench mem-buf-$v --benchmark_filter="BM_BufferMemoryPerEntry/keys:1000000/value:$v/"
done
for i in 1 2; do
  TMPDIR=/mnt/nvme bench coll-bench-$i --benchmark_filter=BM_ColdReplayCollections
done
COLL="--shape trace --shards 4 --key-count 4000000 --prefill-entries 4000000 --workers 8 \
      --duration 60 --warmup 5 --quiet-threshold-s 5 --flush-samples 20 $Z99 \
      --mix write_ack=0.8,read=0.2 --target-rate-ops 7500"
run coll-w80 $COLL --collection-fraction 1
run coll-strings-w80 $COLL --collection-fraction 0
```

**Read scaling (#185).**

```bash
$B --benchmark_filter=BM_HotReadScaling
```

**W3 and W1-L** go over TCP against `abyss-server`, as under [Running the load generator](#running-the-load-generator): `write_throughput.yaml` with `--sweep` for W3, and `write_loopback_pipelined.yaml` beside Valkey in the same run for W1-L.

## Adding workloads

Drop a new YAML file under `tests/perf/workloads/`. The parser will reject invalid configs at load time; there is no need to register the workload in CMake. For repeatability, set `seed` explicitly rather than leaving it at the default 0.

## Adding command coverage to the load generator

`tests/perf/load/loadgen.cpp` dispatches on `op_name` against `kCmdGet` / `kCmdSet`. New RESP commands land here: add the constant, the dispatch branch, and any preload special-casing if the command requires structured input (e.g. hash field/value pairs). Mix weights and targets in the workload YAML automatically pick up the new op name.

## Adding metrics to the JSON output

The reporter emits a fixed schema at `schema_version: 1`. Additions are backwards-compatible at the schema level — downstream consumers should tolerate unknown fields. Removal or rename of an existing field is a breaking change and requires bumping the schema version (`RunReport::kSchemaVersion` in `tests/perf/framework/reporter.h`) and an ADP-013 amendment.
