#!/usr/bin/env bash
# Server-level A/B: SET at depth 1 and 32 under process_crash and
# power_loss, PING at depth 32, a 90/10 GET/SET mix under both, and
# hot GETs beside cold scans on one reactor. Every workload starts a
# fresh server on an empty data directory.
#
#   run_server_ab.sh <run-dir> <server-A> <server-B> <loadgen> [round...]
#
# Without rounds it prepares <run-dir>: the server configs and the
# workloads the rounds will use, kept there (edit them before round 1
# if the experiment needs to), and the binaries' sha256 for PROVENANCE.
# With rounds it runs them, A first in odd rounds and B first in even
# ones; DECISION and PROVENANCE must exist by then (see README.md).
# Environment: DATA_DIR (default <run-dir>/data; put it on the device
# under test), MEMTIER (default memtier_benchmark), PORT, ADMIN_PORT and
# METRICS_PORT (default 16379, 16380 and 16390).
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/lib.sh"
if [ $# -lt 4 ]; then
  sed -n '2,17p' "$0"
  exit 2
fi
RUN=$(cd "$1" && pwd) || exit 2
SA=$2 SB=$3 LG=$4
shift 4
DATA=${DATA_DIR:-$RUN/data}
MEMTIER=${MEMTIER:-memtier_benchmark}
PORT=${PORT:-16379} ADMIN_PORT=${ADMIN_PORT:-16380} METRICS_PORT=${METRICS_PORT:-16390}
M=http://127.0.0.1:$METRICS_PORT/metrics
READY=http://127.0.0.1:$ADMIN_PORT/ready
SPID=
trap 'if [ -n "$SPID" ]; then kill -9 "$SPID" 2> /dev/null; fi' EXIT

prepare() {
  local c w b
  for c in pc pl hol; do
    [ -e "$RUN/srv-$c.yaml" ] && continue
    sed -e "s#AB_DATA_DIR#$DATA#g" -e "s#AB_RESP_PORT#$PORT#g" -e "s#AB_ADMIN_PORT#$ADMIN_PORT#g" \
        -e "s#AB_METRICS_PORT#$METRICS_PORT#g" "$HERE/server/$c.yaml" > "$RUN/srv-$c.yaml"
  done
  mkdir -p "$RUN/wl"
  for w in d1-set d32-set d32-ping mix hol-get; do
    [ -e "$RUN/wl/$w.yaml" ] || cp "$HERE/workloads/$w.yaml" "$RUN/wl/"
  done
  for b in "$SA" "$SB" "$LG"; do echo "$(sha256 "$b")  $b"; done
}

listening() { (exec 3<> "/dev/tcp/127.0.0.1/$1") 2> /dev/null; }
ports_taken() { listening "$PORT" || listening "$ADMIN_PORT" || listening "$METRICS_PORT"; }

start() {  # <server> <config>
  local _
  rm -rf "$DATA"
  mkdir -p "$DATA"
  for _ in $(seq 50); do ports_taken || break; sleep 0.2; done
  if ports_taken; then
    echo "port $PORT, $ADMIN_PORT or $METRICS_PORT is taken by another process" >&2
    exit 1
  fi
  "$1" -c "$RUN/srv-$2.yaml" --no-banner > "$RUN/cur-server.log" 2>&1 &
  SPID=$!
  for _ in $(seq 150); do
    curl -sf -o /dev/null "$READY" && return 0
    kill -0 "$SPID" 2> /dev/null || break
    sleep 0.2
  done
  echo "$1 with srv-$2.yaml never became ready; see $RUN/cur-server.log" >&2
  exit 1
}

stop() {  # <name>
  kill -9 "$SPID" 2> /dev/null
  wait "$SPID" 2> /dev/null
  SPID=
  mv "$RUN/cur-server.log" "$RUN/$1.server.log"
  rm -rf "$DATA"
}

lg() {  # <name> <workload> [loadgen args]; NOQUIET=1 skips the gate
  local name=$1 wl=$2
  shift 2
  [ "${NOQUIET:-0}" = 1 ] || quiet_for "$RUN"
  : > "$RUN/$name.load"
  record_load "$RUN/$name.load" start
  curl -s "$M" > "$RUN/$name.start.prom"
  ps -o time= -p "$SPID" > "$RUN/$name.cpu"
  "$LG" --workload "$RUN/wl/$wl.yaml" --server "127.0.0.1:$PORT" \
      --metrics-url "http://127.0.0.1:$METRICS_PORT" --output "$RUN/$name.json" "$@" \
      > "$RUN/$name.log" 2>&1
  echo "rc=$?" >> "$RUN/$name.log"
  ps -o time= -p "$SPID" >> "$RUN/$name.cpu"
  curl -s "$M" > "$RUN/$name.end.prom"
  record_load "$RUN/$name.load" end
}

setup() {  # <out> <resp_setup args...>
  local out=$1
  shift
  python3 -I "$HERE/resp_setup.py" "$PORT" "$@" >> "$out" 2>&1
}

one() {  # <arm> <round> <server>
  local a=$1 i=$2 s=$3 c mpid _
  start "$s" pc; lg "$a-$i-d1-pc" d1-set; stop "$a-$i-d1-pc"
  start "$s" pc; lg "$a-$i-d32-pc" d32-set; stop "$a-$i-d32-pc"
  start "$s" pl; lg "$a-$i-d1-pl" d1-set; stop "$a-$i-d1-pl"
  start "$s" pl; lg "$a-$i-d32-pl" d32-set
  : > "$RUN/$a-$i-order.txt"
  setup "$RUN/$a-$i-order.txt" setget-order 200 32
  stop "$a-$i-d32-pl"
  start "$s" pl; lg "$a-$i-ping32" d32-ping; stop "$a-$i-ping32"
  for c in pl pc; do
    start "$s" "$c"
    : > "$RUN/$a-$i-mix-$c.preload"
    setup "$RUN/$a-$i-mix-$c.preload" preload 100000 64
    lg "$a-$i-mix-$c" mix
    stop "$a-$i-mix-$c"
  done
  start "$s" hol
  : > "$RUN/$a-$i-hol.preload"
  setup "$RUN/$a-$i-hol.preload" preload 100 64
  setup "$RUN/$a-$i-hol.preload" hashes 200 2000 1
  # Every hash must be in cold, and out of hot, before the first scan.
  for _ in $(seq 100); do
    curl -s "$M" | grep -q '^abyss_evicted_total 200$' && break
    sleep 0.2
  done
  curl -s "$M" | grep -E '^abyss_(evicted_total|hot_keys) ' >> "$RUN/$a-$i-hol.preload"
  lg "$a-$i-hol-alone" hol-get
  "$MEMTIER" -s 127.0.0.1 -p "$PORT" --protocol=redis -c 1 -t 1 --test-time=27 \
      --command="HGETALL __key__" --key-prefix="scan:" --key-minimum=1 --key-maximum=199 \
      --command-key-pattern=R --hide-histogram > "$RUN/$a-$i-hol-scans.txt" 2>&1 &
  mpid=$!
  sleep 2
  # Gated with hol-alone just before: waiting here would outlast the scans.
  NOQUIET=1 lg "$a-$i-hol-scan" hol-get
  wait "$mpid"
  stop "$a-$i-hol"
}

if [ $# -eq 0 ]; then
  prepare
  exit 0
fi
[ -e "$RUN/srv-pc.yaml" ] || { echo "prepare $RUN first: run without rounds" >&2; exit 1; }
for i in "$@"; do
  for a in $(arms "$i"); do
    # Every arm checks again that its binaries are the ones PROVENANCE names.
    require_protocol "$RUN" "$SA" "$SB" "$LG" || exit 1
    if [ "$a" = A ]; then one A "$i" "$SA"; else one B "$i" "$SB"; fi
    echo "$i $a" >> "$RUN/order.txt"
  done
done
echo done > "$RUN/DONE"
