# Performance Harness

The performance harness is the tooling Abyss uses to measure whether it meets the targets in [requirements.md](../design/requirements.md). The harness's design is described in [ADP-013](../design/proposals/013-performance-harness.md); this document is operational — how to build it, how to run it, how to read the numbers it produces, and what conditions make those numbers trustworthy.

## What the harness is

Three substrates that share infrastructure but answer different questions:

| Substrate | Question | Runs |
|---|---|---|
| `tests/perf/micro/` | What is the lower-bound cost of one operation in isolation? | Google Benchmark targets via `abyss_bench`. |
| `tests/perf/probe/` | What does the engine deliver to the layer above it? | In-process binaries (`abyss_hot_probe`, `abyss_buffer_probe`, `abyss_cold_probe`) that construct real components and exercise them directly. |
| `tests/perf/load/` | What does a real client see? | `abyss_loadgen` — multi-threaded TCP driver against the full `abyss-server` binary using hiredis. |

A shared library — `abyss::perf_framework` under `tests/perf/framework/` — wraps HdrHistogram, parses workload YAML, schedules requests under the wrk2 coordinated-omission discipline, scrapes `/metrics`, and emits the versioned JSON report + `.hgrm` histogram logs.

## Building

Performance binaries are gated by `ABYSS_BUILD_PERF`. The `default` and `bench` presets turn it on; `release` and `container` turn it off so production builds don't carry hdr-histogram or hiredis.

```bash
cmake --preset default          # debug build, framework tests run in default ctest
cmake --preset bench            # release build, perf binaries optimised

cmake --build build/bench --target abyss_hot_probe abyss_buffer_probe abyss_cold_probe abyss_loadgen
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
    --mix "hot_get=0.95,hot_set=0.05" \
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

- `abyss_hot_probe` — `hot_get`, `hot_set`. Default targets: GET p99 < 100µs, SET p99 < 50µs.
- `abyss_buffer_probe` — `buffer_read`. Default target: p99 < 50µs.
- `abyss_cold_probe` — `cold_get`, optionally `cold_set`. Default target: GET p99 < 5ms. Also accepts `--data-path` to reuse a pre-populated RocksDB directory; otherwise creates a temporary one.

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

The bundled workloads (`hot_read_heavy.yaml`, `write_throughput.yaml`, `mixed_50_50.yaml`, `cold_read_aged.yaml`) are reference shapes for authoritative runs. Treat them as templates: scale `workers × connections_per_worker`, `target_rate_ops`, and `duration` down before running on a developer machine.

### Loadgen flags

| Flag | Required | Meaning |
|---|---|---|
| `--workload` | yes | Path to a workload YAML file. |
| `--server` | no | `host:port` of the abyss-server RESP listener (default `127.0.0.1:6379`). |
| `--metrics-url` | no | Base URL for `/metrics` scraping. Pass empty to disable. Three snapshots are recorded — start, mid, end. |
| `--output` | no | JSON report path (defaults to stdout). |
| `--hgrm-dir` | no | Directory for per-operation `.hgrm` files. |
| `--gate` | no | Exit code 3 if any target fails. |
| `--skip-preload` | no | Skip the workload's preload phase (useful when re-running against a server that already has the keyspace populated). |
| `--run-id` | no | Override the auto-generated run id. |

Loadgen v1 supports `GET` and `SET` operations. Other RESP commands (`HGET`, `HSET`, `SADD`, …) are recognised in the mix but produce errors that count against the run; expanding command coverage is tracked separately.

## Workload YAML

A workload is a small YAML document. The schema is enforced by the parser in `tests/perf/framework/workload.cpp` — invalid configs are rejected at startup.

```yaml
name: example
description: free text
duration_seconds: 60          # required, > 0
warmup_seconds: 10            # optional, default 0
workers: 4                    # required, >= 1
connections_per_worker: 4     # loadgen only; probes ignore
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
    SET:
      p99_us: 50
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
- `host` — os, kernel, cpu_model, hostname
- `workload` — the parsed workload config inlined
- `operations.<name>` — `count`, `throughput_ops`, `latency_us.{p50,p99,p999,max}`, `noise_floor_cv`, `saturated`, `histogram_b64` (base64-encoded HdrHistogram log)
- `server_metrics` — start / mid / end snapshots from the abyss-server `/metrics` endpoint (loadgen only)
- `targets[]` — per-target `{metric, target, actual, pass}` evaluations
- `pass` — top-level boolean: true iff every target passed

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
| Hot read p99 < 100µs | hot probe (`hot_get`) | loadgen (`GET` after hot preload) |
| Hot write+ACK p99 < 50µs | hot probe (`hot_set`) | loadgen (`SET`) |
| Buffer read p99 < 50µs | buffer probe (`buffer_read`) | — |
| Cold read p99 < 5ms | cold probe (`cold_get`) | loadgen (`GET` after aging out hot) |
| Write throughput > 100 000 ops/s | loadgen (`write_throughput.yaml`) | — |
| Recovery < 60s for 24h / 1M | micro (`recovery_bench`) | — |

When the probe and loadgen numbers disagree for the same target, the gap is the cost of the RESP+TCP path plus client-side scheduling — both real and worth knowing.

## Adding workloads

Drop a new YAML file under `tests/perf/workloads/`. The parser will reject invalid configs at load time; there is no need to register the workload in CMake. For repeatability, set `seed` explicitly rather than leaving it at the default 0.

## Adding command coverage to the load generator

`tests/perf/load/loadgen.cpp` dispatches on `op_name` against `kCmdGet` / `kCmdSet`. New RESP commands land here: add the constant, the dispatch branch, and any preload special-casing if the command requires structured input (e.g. hash field/value pairs). Mix weights and targets in the workload YAML automatically pick up the new op name.

## Adding metrics to the JSON output

The reporter emits a fixed schema at `schema_version: 1`. Additions are backwards-compatible at the schema level — downstream consumers should tolerate unknown fields. Removal or rename of an existing field is a breaking change and requires bumping the schema version (`RunReport::kSchemaVersion` in `tests/perf/framework/reporter.h`) and an ADP-013 amendment.
