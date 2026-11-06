# A/B runs for performance gates

Paired A/B runs of two builds, each decided by a rule written down before the first counted round. Phase 3's gates were run this way on macOS ([performance.md](../../../docs/development/performance.md#asynchronous-execution-measurements-macos-indicative)), and those numbers are indicative only. **The authoritative Linux runs have not happened yet: they await the Linux hardware they will run on, which is still to be chosen.** This directory holds everything those runs need. No CI job runs it.

| File | What it does |
|---|---|
| `run_server_ab.sh` | Server-level A/B. It runs `SET` at depth 1 and 32 under `process_crash` and `power_loss`, `PING` at depth 32, a 90/10 `GET`/`SET` mix, and hot `GET`s beside cold scans on one reactor. Each workload starts a fresh server on an empty data directory. |
| `run_probe_ab.sh` | Write-probe A/B: the engine's write path driven without the front end (W1, W2, and the lock hold at `process_crash` and at `power_loss`). |
| `run_probe_aa.sh` | The A/A control: `run_probe_ab.sh` with one binary as both arms. |
| `lib.sh` | Shared by the run scripts: the `quiet()` gate, the DECISION and PROVENANCE checks, and the per-run load record. |
| `decide.py` | Applies the run's DECISION to its data and prints one `GATE` line per gate and a `VERDICT` line. It exits 0 only if every gate passes. |
| `hdr.py` | Decodes the HdrHistogram that loadgen and the probe embed in their JSON, for means and percentiles. |
| `resp_setup.py` | A small RESP client. It preloads keys by `MSET`, writes the cold scan hashes, and runs the `SET order v<i> GET` order check. |
| `templates/DECISION.*` | DECISION templates for the server A/B, the probe A/B and the A/A control. |
| `server/*.yaml`, `workloads/*` | The server configs, loadgen workloads and probe flags that a run directory starts from. |
| `slow-ack-ring.320b6e8.patch` | The W1 stall diagnostic. It is an artefact, not a tool (see the [last section](#the-w1-slow-ack-diagnostic)). |

The Python is standard library only, Python 3.8 or later. Run it as `python3 -I`, so nothing is imported from the run directory or the environment; `decide.py` loads `hdr.py` from its own directory by path. The shell scripts need bash, curl, `pgrep`, and `memtier_benchmark` for the head-of-line case.

## The sequence

Run the steps in this order. A step that comes after round 1 has started cannot inform that round.

### 1. The host

The host should be Linux on perf-grade hardware:
- the performance governor;
- isolated cores;
- the data directory (`DATA_DIR`) on the NVMe device under test;
- `export ABYSS_PERF_AUTHORITATIVE=1`, so the probe's and loadgen's JSON record the run as `authoritative`;
- nothing else running beside the runs.

Launch the runs at the session's normal priority and keep that priority for the whole experiment. zsh's `BG_NICE` adds 5 to a job started with `&`; the macOS runs lost a level that way. Use `setopt NO_BG_NICE`, or start the runs from bash. Each run's `.load` file records its nice value.

### 2. Build A and B against one dependency tree

Only Abyss's own code may differ between A and B. Build B, the tree under test, as usual. Then build A from a `git archive` of the comparison commit, configured against B's `vcpkg_installed`, so both link the same RocksDB and every other dependency:

```bash
cmake --preset bench
cmake --build build/bench -j --target abyss-server abyss_loadgen abyss_write_probe
B_VCPKG=$PWD/build/bench/vcpkg_installed
A=<commit>
git diff --quiet $A HEAD -- vcpkg.json CMakePresets.json CMakeLists.txt cmake/ || echo "build setup differs: record it"
mkdir -p /tmp/abyss-$A && git archive $A | tar -x -C /tmp/abyss-$A
(cd /tmp/abyss-$A &&
 cmake --preset bench -DVCPKG_INSTALLED_DIR=$B_VCPKG -DVCPKG_MANIFEST_INSTALL=OFF &&
 cmake --build build/bench -j --target abyss-server abyss_write_probe)
```

Check parity before trusting the pair:
- Compare `compile_commands.json` for a few hot translation units, such as `src/engine/sequencer.cpp`, `src/net/reactor.cpp` and `src/queue/log.cpp`.
- Compare each binary's `link.txt`.
- Only paths may differ.

One load generator, built from B, drives both servers. It needs the `PING` op and the in-order reply check.

### 3. Prepare the run directory

Run the script without rounds:

```bash
AB=tests/perf/ab
RUN=/srv/ab/server-<date>; mkdir -p $RUN
# One of, by experiment; each run directory holds one experiment.
$AB/run_server_ab.sh $RUN $SA $SB $LG > $RUN/sha256.txt   # server A/B
$AB/run_probe_ab.sh  $RUN $PA $PB     > $RUN/sha256.txt   # probe A/B
$AB/run_probe_aa.sh  $RUN $PA         > $RUN/sha256.txt   # A/A control
```

This writes the configs and workloads the rounds will use into the run directory: `srv-*.yaml` with `DATA_DIR` and the ports filled in, and `wl/*.yaml` or `wl/*.args`. It also prints each binary's sha256. The rounds read only the run directory's copies, so if the experiment needs a change, edit them now.

### 4. DECISION

Copy the matching template to `$RUN/DECISION`:
- `templates/DECISION.server` for a server A/B;
- `templates/DECISION.probe` for a probe A/B;
- `templates/DECISION.aa` for an A/A control.

Then replace every `<...>` before round 0 (or round 1 if there is no pilot).
- `decide.py` and the run scripts read only the `key = value` lines; the rest records what was decided and why. Both refuse a placeholder left in.
- The thresholds live only in DECISION. `decide.py` has no defaults, so the rule is in one place, written before the data.
- A value never changes after round 1 has started. Write any amendment into DECISION, with its time and reason, before the round it first applies to.

Each template states the estimator:
- per round, B/A of each arm's p50 or mean;
- over rounds, the geometric mean;
- rounds alternate, A first in odd rounds and B first in even ones.

### 5. PROVENANCE

Write `$RUN/PROVENANCE` before the first round. It holds:
- **Host:** CPU, cores, memory, kernel, governor, isolated cores, and the device and filesystem under `DATA_DIR`.
- **Binaries:** for each, the sha256 (from `sha256.txt`), the commit it was built from, and whether its tree was clean.
- **Build:** the preset, build type, compiler and version, `CMAKE_CXX_FLAGS_RELEASE`, LTO and PGO (off unless stated), the vcpkg baseline, and `VCPKG_INSTALLED_DIR`. Add the parity check from step 2.
- **Tools:** the commit of this directory, and memtier's version and sha256.

Before every arm, the run scripts refuse any binary whose sha256 PROVENANCE does not contain, so a rebuild mid-experiment stops the run.

### 6. The quiet() gate

Before each measured run, `quiet()` waits:
- up to `quiet.compiler_wait_s` for no compiler or clang-tidy to be running;
- then up to `quiet.load_wait_s` for a one-minute load under `quiet.load_max`.

After that it starts the run regardless. All three values come from DECISION. Each run's `<name>.load` keeps `uptime` at its start and end, its nice value, and whether a compiler was running. `decide.py` prints the start loads beside the per-round ratios, so a gate can be checked against them.

On a dedicated host the gate should rarely wait. A host whose idle load sits above `quiet.load_max` is not quiet, and a longer wait does not change that.

### 7. The A/A control

A mean is set by its rarest stalls, so two copies of one binary can differ by more than a 5% gate. Phase 3's W1 mean did, on macOS: A/A per-round mean ratios of 0.675–1.275, geomean 0.893.

The rule, from Phase 3's W1 ruling:
- Run an A/A control in the same session as the A/B: the same probe as both arms, the same flags, the same `quiet()` and the same nice.
- **Gate the A/B mean only if the A/A spread is well inside 5%.** Otherwise the gate is uninformative by construction.

In the probe template:
- `aa.calibrates` names the workloads whose mean gate this applies to (W1);
- `aa.spread_max` (1.025) is the limit;
- the spread is exp(|mean(log r)| + 2·SE), where r is each A/A round's mean ratio and SE is the standard error of the mean of the per-round log(r). That's bias plus a ~95% half-width, as a ratio: roughly what the A/B estimator reads when nothing differs. 1.025 is a 2.5% bound;
- the A/A runs in the same session as the A/B, interleaved with it, with at least half the A/B's round count. decide.py refuses a smaller control.

`decide.py <run> --aa <aa-run>` gates such a mean only if the A/A spread is within the limit, and otherwise prints `NOT DECIDED`. The macOS A/A's spread was 1.37.

```bash
DATA_DIR=/mnt/nvme/ab $AB/run_probe_aa.sh $AA $PA 1 2 3 4 5 6
```

### 8. Run

```bash
export DATA_DIR=/mnt/nvme/ab
$AB/run_server_ab.sh $RUN $SA $SB $LG 0                       # pilot: validates the scripts, never decided
$AB/run_server_ab.sh $RUN $SA $SB $LG 1 2 3 4 5 6 7 8 9 10
$AB/run_probe_ab.sh $RUN $PA $PB 1 2 3 4 5 6 7 8 9 10 11 12
```

`order.txt` records each arm as it finishes, and `DONE` marks the end of the rounds asked for. A run that stops partway leaves its round incomplete. Rerun that whole round, both arms, and say so in DECISION.

The scripts stop when the server ports are already taken (a stale server would be measured instead) or when a server never becomes ready. They use ports 16379, 16380 and 16390, or `PORT`, `ADMIN_PORT` and `METRICS_PORT` if those are set.

### 9. Decide

```bash
python3 -I $AB/decide.py $RUN                 # server A/B, or an A/A control
python3 -I $AB/decide.py $RUN --aa $AA        # probe A/B with A/A-calibrated means
python3 -I $AB/hdr.py $RUN/A-1-w1.json        # one run's n, p50, mean, p99
```

Each gate prints `PASS`, `FAIL` or `NOT DECIDED` with its numbers, then the per-round ratios and the diagnostics the template names. A `NOT DECIDED` gate is not a pass. Report a miss with its diagnostics, as Phase 3's W1 mean was reported; never waive it.

## The W1 slow-ack diagnostic

`slow-ack-ring.320b6e8.patch` is the diagnostic build behind Phase 3's W1 stall attribution ([performance.md](../../../docs/development/performance.md#engine-no-regression-g2)). It is kept so that attribution can be reproduced. **It applies only to 320b6e8, and it is not a maintained tool.**

```bash
git archive 320b6e8 | tar -x -C /tmp/abyss-diag
git -C /tmp/abyss-diag init -q && git -C /tmp/abyss-diag apply $PWD/tests/perf/ab/slow-ack-ring.320b6e8.patch
```

What it adds:
- `ABYSS_DIAG_UNSAMPLED=1` times the sequencer stages of every write, not one in 64, and keeps every write whose stages took 1 ms or more.
- `ABYSS_DIAG_SLOW_OUT=<csv>` makes the write probe record each write's wall time and thread CPU time, with the process's page faults and context switches meanwhile, keep the writes of 2 ms or more, and write both lists to the CSV at exit.
- Without either variable it costs one `getenv`.

The supported, built-in form of this capability is issue #207. Use that once it lands, not this patch.
