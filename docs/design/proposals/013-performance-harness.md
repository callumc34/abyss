# ADP-013: Performance Harness

**Status:** Proposed
**Created:** 2026-05-17

## Context

The PRD commits Abyss to specific latency and throughput numbers: hot read under 100 microseconds at p99, hot write under 50, buffer read under 50, cold read under 5 milliseconds, sustained write throughput above 100 000 ops/s on the embedded profile, and recovery of a 24-hour, one-million-entry queue under 60 seconds. These are not aspirations. They are the contract the system must meet before it is operationally interesting.

Today the only performance tooling in the tree is a small set of Google Benchmark microbenches under `tests/bench/` (WAL append, WAL entry codec, recovery, eviction). They cover the lowest-level questions and nothing else. There is no client-driven load generator, no end-to-end latency measurement against the server, no histogram with usable tail resolution, no machine-readable output, no mechanism for tracking a number across builds, and no discipline against the measurement traps that have embarrassed every previous KV store benchmark. Without that substrate we cannot validate the targets, we cannot tell whether a change has regressed the system, and we cannot use measurement to improve the system in any rigorous way.

This ADP defines the performance harness: how Abyss is measured, what tooling is admitted into the tree, what discipline that tooling enforces, and what is left out.

## Goals

- Validate every performance target in `requirements.md` against the embedded profile, with each target assigned to the substrate that can answer the question honestly.
- Measure latency with coordinated-omission correction. A run that omits this is not a measurement.
- Record latency at full tail resolution. p999 and max are first-class outputs, not afterthoughts.
- Produce machine-readable output that is independent of any specific CI, dashboarding, or regression-tracking vendor.
- Carry no production cost. The server binary and its release-mode behaviour are unchanged by the presence of the harness.
- Be honest about the conditions of measurement. A noisy macOS laptop run and a clean dedicated-runner run produce numbers labelled accordingly, and one is not silently substituted for the other.
- Be testable. The harness is itself a program; it has unit and integration tests that pin its measurement behaviour, including its coordinated-omission corrector.

## Non-goals

- External-profile benchmarking (Kafka queue backend, external Redis hot store). Phase 2.
- Chaos injection, fault simulation, network partition workloads. Phase 3.
- Long-duration soak runs (hours to days). Phase 3, separate scaffolding.
- Grafana dashboard templates. Phase 3.
- A CI workflow that automatically fails a pull request when a target slips. The harness reports honestly; PR gating on absolute targets requires hardware we do not yet have.
- Server-side performance instrumentation compiled into release. Latency is observed by the harness as a client; server-side correlation uses the existing observability facade ([ADP-012](012-observability-facade.md)) and nothing else.
- A unified one-tool-measures-everything benchmark. Combining substrates produces dishonest numbers; see below.

## Three substrates, one harness

A single substrate cannot answer all the questions in `requirements.md` without conflating them. The harness therefore comprises three substrates that share infrastructure and reporting but answer distinct questions.

| Substrate | Question | What it runs | What it excludes |
|---|---|---|---|
| micro | What is the lower bound of cost for one operation in isolation? | One Google Benchmark target per unit (WAL append, queue codec, eviction policy, recovery). | Threading, contention, system calls beyond the unit, network. |
| probe | What does the engine deliver to the layer immediately above it? | A real component constructed in-process: hot store, compaction buffer, cold store. The probe drives configured load against it through its own interface. | RESP encoding, TCP, client wakeup latency. |
| load | What does a real client see? | The full `abyss-server` binary as a subprocess. The driver opens hiredis connections over TCP and exercises the system through RESP. | Nothing. This is the whole stack including operating-system scheduling. |

Targets are assigned to the substrate that can answer them honestly. Where a target is answerable by more than one substrate, the harness reports both numbers under distinct names; it does not pick one and call it the target.

| Target | Authoritative substrate | Cross-check |
|---|---|---|
| H1 hot apply | probe (hot probe, `hot_apply`) | micro |
| R1 hot read | probe (hot probe, `hot_get`) | load |
| W1 write overhead (`process_crash`) | probe (write probe, `write_ack`) | — |
| W1-L write over loopback | load (pipelined SET workload, against Valkey in the same run) | memtier |
| R1-L read under writes | load (90/10 mixed workload, against Valkey in the same run) | memtier |
| W2 durable write (`power_loss`) | probe (write probe, `write_ack`, against the calibrated `device_flush` floor) | load |
| W3 durable write throughput | load | — |
| X1 comparative | load (same driver against every server) | memtier |
| B1 buffer read | probe | — |
| C1 cold read | probe | load |
| RC1 recovery | micro | — |

> **Amended by [ADP-015](015-write-path-and-durability.md) (2026-10-01).** §Coordinated omission now records each request from its intended time without added synthetic samples; the earlier implementation did both, which counted every stall twice. The original "hot write (queue append + hot apply + ACK) < 50µs" row was assigned to a hot-store-only probe, so it would have passed while measuring neither the queue, the flush nor the acknowledgement. Write targets are now per durability class. They are measured by the write probe, which drives the real in-process write path: the engine dispatch, the WAL on the volume under test, and all three consumer pools. Before measuring, the write probe records the device's flush floor with the same primitive the WAL uses, so the power-loss target is evaluated against the floor of the device it ran on. See §Comparative baselines for the drivers.

The reason for splitting hot read into both an authoritative probe and a cross-check load is that the probe-measured number is the engine-true latency and the load-measured number includes everything between the engine and the client. The PRD target is stated in engine-internal terms ("hot read"), so the probe is authoritative; but a wildly larger load number diagnoses real client-visible regression in RESP, the request pipeline, or TCP path, none of which the probe will catch. Reporting both keeps the harness honest in both directions.

## Comparative baselines

The X1 target compares Abyss with other servers on the same host, using the same load driver and workload for each durability class. The load generator speaks RESP, so it drives Valkey, Redis and Dragonfly unchanged. Every report records which server it measured: the load generator reads the server's identity from `HELLO` and `INFO`, and stores it in the report, so a baseline can never be mistaken for an Abyss run.

The load generator pipelines requests on each connection up to the workload's configured depth. Coordinated-omission correction still applies per request: each request has its own intended send time.

**Matrix.** Each comparison runs the full cross product of:
- workload: SET-only, GET-only, and 90/10 GET/SET;
- value size: 64 B and 1 KiB;
- pipeline depth: 1 and 16;
- connections: 64 and 256.

Each Abyss durability class is paired with its equivalent: `power_loss` with Valkey `appendfsync always`, and `process_crash` with Valkey `appendfsync everysec` and with Dragonfly (labelled no-AOF).

**What counts as better.**
- *Lower p99* compares p99 at the same offered load, at most 50% of the slower server's saturation. p99 at saturation is not comparable.
- *Higher throughput* compares the maximum sustainable rate within the same latency SLO.

**Tuning parity.** Valkey I/O threads and Dragonfly proactor threads are set for the host, CPU pinning is the same for every server, and the client runs on separate cores or a separate host.

`memtier_benchmark` is admitted as a cross-check driver only, never the authoritative one. It makes Abyss numbers comparable with published Redis, Valkey, Dragonfly and Garnet figures, which use it, and it sweeps pipeline depth. Its numbers are not corrected for coordinated omission and do not feed target evaluation.

## Coordinated omission

Coordinated omission is the most common reason that published benchmark numbers are dishonest. The trap is well-known: a closed-loop driver that issues a request, waits for the response, then issues the next, will systematically under-report tail latency when the system stalls. If the target rate is 100 000 ops/s (one request every 10µs) and the system stalls for 100 ms, a naive driver records one slow request and then races to catch up; the 9 999 requests that *should* have been issued during the stall are not recorded at all. p99 in that scenario looks fine while the user experience is a disaster.

The harness rejects this. The load driver runs an open-loop schedule under wrk2 discipline: per worker, the driver maintains a sequence of intended send times spaced by the inverse of the target rate. When a response returns at wall-clock time `t_actual`, the recorded latency is `t_actual - t_intended`, not `t_actual - t_sent`. Every intended slot is still issued. If a previous operation was still in flight, the slot is issued late, and its recorded latency carries the time it spent waiting to be sent. A stall therefore shows up in the latency of every request scheduled during it, which is the wrk2 algorithm. Synthetic samples are not added on top: measuring from the intended time already accounts for the missed slots, and adding samples as well would count the stall twice. With pipelining, each request in a connection's window has its own intended time, and the same rule applies per request.

For the closed-loop case — target rate of zero, meaning "drive the system as hard as it will go" — coordinated omission does not apply because there are no missed slots. The harness records per-request latency directly and reports throughput as the primary number. The JSON output flags the run as closed-loop so downstream tooling does not compare its latency numbers to open-loop runs.

The coordinated-omission corrector is a load-bearing piece of code that is invisible in normal operation. A test pins its behaviour by driving an open-loop schedule with a stall injected. It checks four things:
- exactly one sample is recorded per scheduled request, so nothing is omitted and nothing is double counted;
- the maximum covers the stall;
- requests scheduled during the stall show the backlog draining;
- p99 reflects the stall.

The same properties are pinned for the pipelined driver. Without this test the algorithm can regress silently.

## Latency recording

The harness records latency to HdrHistogram with sub-microsecond precision across a range of one microsecond to one minute, three significant digits. This is the standard Gil Tene HdrHistogram_c (BSD-2) library used by wrk2, jHiccup, and the broader latency-measurement ecosystem; the canonical log format (`.hgrm`) is readable by every downstream tool we care about and replayable by the histogram-log processor.

We do not roll our own histogram. We do not use Google Benchmark's built-in percentile reporter for anything other than micro: its range and precision are not appropriate for system-level p99/p999 work, and it cannot be merged across threads losslessly. We do not use a fixed-bucket scheme; the cost of getting bucket boundaries wrong silently is higher than the small per-sample cost of HDR.

Histograms are recorded per worker thread, per operation type, and merged at run end. Merging is lossless within HDR's precision. The per-thread instances eliminate cross-thread contention on the hot path of the load driver. Per-operation breakdown (GET vs SET, vs HSET, etc.) is non-negotiable: a mixed workload with a 10:1 read/write ratio whose write p99 is bad will look fine in a combined histogram.

## Workload model

Workloads are data, not code. A workload is a YAML document specifying: duration, warmup duration, connection count, target rate (zero for closed-loop), key count, key distribution, value size distribution, operation mix, optional preload phase, and a target block declaring the expected per-operation p50/p99/p999 and the expected throughput. The harness parses the workload, drives the system according to it, and emits a single JSON report and a set of `.hgrm` files.

Key distributions admitted in the initial scope: uniform, zipfian, and latest (read-recently-written, YCSB workload D). These cover the realistic shapes we expect — uniform for synthetic upper-bound throughput, zipfian for cache-like skew, latest for queue-following readers. More distributions can be added without ADP amendment because they are pluggable behind a small interface.

Workloads live under `tests/perf/workloads/`. They are inputs to test binaries, not runtime configuration; they belong next to the code that consumes them.

## Output

Every run emits two artefacts:

1. A set of `.hgrm` histogram log files, one per operation type. Industry-standard format. Replayable, plottable, mergeable across runs. This is the lossless ground truth.

2. A single JSON summary file. Schema-versioned. Includes run metadata (commit SHA, build preset, compiler, optimisation level, hostname, kernel, CPU model, sanitizer state), the workload document inlined, per-operation throughput and latency percentiles, server-side metric snapshots scraped at run start, mid, and end (cold consumer lag, oldest-unflushed buffer age, queue depth, hot evictions, fsync latency), the per-target pass/fail evaluation, the observed noise floor (coefficient of variation across the warmup-and-measure window), an indicative/authoritative classification (see below), and a top-level `pass` boolean that is true if every target passed.

The JSON schema is versioned (`schema_version: 1`). Downstream tooling reads the version field and can refuse incompatible payloads. Schema changes are ADP-amendable; the schema is not allowed to mutate quietly.

The harness output is deliberately tool-agnostic. It is not coupled to bencher.dev, Codspeed, or any other regression-tracking service. A future ADP-amendment selecting one of those vendors will consume the existing JSON; the harness will not need to change.

## Conditions of measurement

The first runs of this harness will happen on a developer's macOS laptop. That environment has thermal throttling under sustained load, no performance CPU governor, no `taskset` or `isolcpus` equivalent, no quiet-system guarantees, and competing processes. Numbers from such a run are real signal but they are not the authoritative answer to "does Abyss meet its targets". The PRD targets are the bar the system must clear on perf-grade hardware. The bar Abyss clears on a developer laptop is whatever it clears.

The harness handles this honestly. Every run is classified, in the JSON metadata, as either `indicative` or `authoritative` based on a small set of host checks (OS, CPU governor, machine identity overrides). macOS runs are always `indicative`. The classification is reported but not used to silence or hide results — a failing indicative run is still reported as a failure, it is simply known to be an unreliable failure. The harness reports the observed noise floor (coefficient of variation) alongside each percentile so the reader can judge how much weight to give it.

The first full pass against the targets is expected to find failures, possibly several. This is fine. The harness's value is its truthfulness as an instrument; the results are what they are. The first run becomes the baseline. Subsequent work improves the system against that baseline.

## CI integration

The harness produces an artefact. It does not, in this ADP, gate pull requests. Auto-gating on absolute targets requires hardware that produces stable numbers; GitHub Actions runners do not. Auto-gating on relative deltas requires a stable baseline plus a deliberate decision about acceptable noise; we do not yet have either.

The harness will be wired into a separate scheduled workflow (filed as a follow-up issue, not in this ADP) that produces a JSON artefact per run and archives it. Tracking, diffing, and gating decisions are downstream of that artefact and are not the harness's concern. The harness's contract is that the artefact is honest.

## Layout

```
tests/perf/
├── framework/          shared library for histogram, scheduling, workload
├── micro/              Google Benchmark targets (migrated from tests/bench/)
├── probe/              in-process component probes
├── load/               TCP load generator binary
└── workloads/          YAML workload definitions
```

The existing `tests/bench/` directory is moved into `tests/perf/micro/` as part of the framework PR. This is a single, called-out consolidation: every performance-related binary lives under one umbrella, the mental model is uniform, the CTest labels are uniform, and the build option that gates the harness gates the microbenches too. The microbench source files are not edited as part of the move; the target name `abyss_bench` is preserved.

## CTest labels

The default `ctest` run remains fast and excludes performance work. New labels:

| Label | Default | Contents |
|---|---|---|
| `perf-framework` | included | Unit and component tests of the harness itself. Sub-second. |
| `perf-micro` | excluded | The Google Benchmark suite. Minutes. |
| `perf-probe` | excluded | In-process component probes. Tens of seconds to minutes. |
| `perf-load` | excluded | TCP load runs. Minutes per workload. |

The performance test binaries are built only when `ABYSS_BUILD_PERF` is on. The `bench` CMake preset turns this on alongside its existing Google Benchmark wiring.

## Risks and mitigations

- **Coordinated-omission corrector regresses silently.** The unit test described above is the entire mitigation. If the test ever flakes or is disabled, the harness is unreliable.
- **HdrHistogram range or precision misconfigured for an operation.** Fixed precision of three significant digits across one microsecond to one minute covers everything in the PRD. If a future operation exceeds the range — for instance a deliberately slow administrative command — the histogram saturates rather than overflows; the harness flags saturation in the JSON output.
- **macOS-local noise misread as system regression.** Mitigated by indicative/authoritative classification and noise-floor reporting. The reader is expected to weight indicative runs accordingly; the harness does not silently downgrade them.
- **Single-process load driver bottlenecks before the server does.** The load driver uses multiple worker threads, each with multiple hiredis connections. Per-worker throughput is reported separately, so a driver bottleneck is visible as flat per-worker throughput at scale. If we observe this we will revisit the driver design (async hiredis, multi-process); the harness does not paper over it.
- **Refactor blast radius of the `tests/bench/` move.** Single called-out move in a single PR. The bench binary keeps its name. No source edits in the moved files. CI and build documentation are updated in the same PR.

## Alternatives considered

- **A single end-to-end load harness covering all targets.** Rejected. Engine-internal targets cannot be measured honestly through RESP and TCP; the loopback and parser overhead is not zero, and conflating them with the engine number paints a misleading picture of where time is spent. The cost of three substrates is small; the cost of dishonest numbers is high.
- **`redis-benchmark` as the load driver.** Rejected. No coordinated-omission correction, limited percentile resolution, awkward to integrate into CI artefacts, no per-operation breakdown in a useful form.
- **`memtier_benchmark` as the load driver.** Rejected for v1. Larger dependency surface than we need, no coordinated-omission correction, command-mix expressiveness exceeds our requirements. Worth revisiting if our needs grow.
- **Google Benchmark for everything including load.** Rejected. Google Benchmark's iteration model is closed-loop and not adaptable to a target-rate open-loop schedule with coordinated-omission correction. It is excellent for what it does (micro) and is retained for that.
- **Hand-rolled latency histogram.** Rejected. There is no version of this that is not worse than HdrHistogram. The only argument for rolling our own is dependency aversion; HdrHistogram_c is small, vendored cleanly via vcpkg, and BSD-2 licensed.
- **HdrHistogramCpp over HdrHistogram_c.** Marginal. `_c` is canonical, used by wrk2, and the API difference is irrelevant given the framework wraps it anyway. We pick `_c`. Worth revisiting only if the C++ binding diverges in capability.
- **Server-side perf instrumentation compiled in release.** Rejected. Violates the principle that release-mode behaviour is unaffected by the harness, and the existing observability facade already provides what the harness needs from the server side.
- **Coupling the JSON output to a specific tracking vendor.** Rejected. Vendor selection is a downstream concern. The harness's contract is the artefact; tracking is a separate decision behind a separate ADP.

## Acceptance

This ADP is accepted when:

- The three substrates and their target assignments are agreed.
- The coordinated-omission discipline is agreed and the unit-test contract is named.
- The output schema's existence and versioning are agreed (the schema itself may evolve under ADP amendment).
- The macOS-local / indicative-only stance and the no-auto-gate position are agreed.
- The `tests/bench/` move is agreed.

Implementation lands across four pull requests: the framework with the bench migration, the in-process probes, the TCP load generator with its workloads, and the developer documentation. The framework PR is the only one with externally observable cost — a new vcpkg dependency, a new CMake option, a new directory layout. Subsequent PRs add binaries and tests under the established structure.
