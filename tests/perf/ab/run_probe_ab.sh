#!/usr/bin/env bash
# Write-probe A/B: the engine's write path, driven without the front
# end. With the same binary as both arms it is the A/A control
# (run_probe_aa.sh).
#
#   run_probe_ab.sh <run-dir> <probe-A> <probe-B> [round...]
#
# Without rounds it prepares <run-dir>: one wl/<workload>.args file of
# probe flags per workload (w1, w2, hold, pl1), kept there (edit them
# before round 1 if the experiment needs to), and the binaries' sha256
# for PROVENANCE. With rounds it runs the workloads DECISION's
# `workloads` line names, A first in odd rounds and B first in even
# ones; DECISION and PROVENANCE must exist by then (see README.md).
# Environment: DATA_DIR (default <run-dir>/data; put it on the device
# under test).
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/lib.sh"
if [ $# -lt 3 ]; then
  sed -n '2,16p' "$0"
  exit 2
fi
RUN=$(cd "$1" && pwd) || exit 2
PA=$2 PB=$3
shift 3
DATA=${DATA_DIR:-$RUN/data}

prepare() {
  local w b
  mkdir -p "$RUN/wl"
  for w in w1 w2 hold pl1; do
    [ -e "$RUN/wl/$w.args" ] || cp "$HERE/workloads/probe-$w.args" "$RUN/wl/$w.args"
  done
  for b in "$PA" "$PB"; do echo "$(sha256 "$b")  $b"; done
}

probe() {  # <probe> <name> <workload>
  local flags
  quiet_for "$RUN"
  rm -rf "$DATA"
  mkdir -p "$DATA"
  read -r -a flags < "$RUN/wl/$3.args"
  : > "$RUN/$2.load"
  record_load "$RUN/$2.load" start
  "$1" --wal-path "$DATA/wal" --cold-path "$DATA/cold" "${flags[@]}" \
      --output "$RUN/$2.json" > "$RUN/$2.log" 2>&1
  echo "rc=$?" >> "$RUN/$2.log"
  record_load "$RUN/$2.load" end
}

if [ $# -eq 0 ]; then
  prepare
  exit 0
fi
require_protocol "$RUN" "$PA" "$PB" || exit 1
WORKLOADS=$(decision "$RUN" workloads) || exit 1
for w in $WORKLOADS; do
  [ -e "$RUN/wl/$w.args" ] || { echo "no $RUN/wl/$w.args: prepare $RUN first" >&2; exit 1; }
done
for i in "$@"; do
  for a in $(arms "$i"); do
    # Every arm checks again that its binaries are the ones PROVENANCE names.
    require_protocol "$RUN" "$PA" "$PB" || exit 1
    for w in $WORKLOADS; do
      if [ "$a" = A ]; then probe "$PA" "A-$i-$w" "$w"; else probe "$PB" "B-$i-$w" "$w"; fi
    done
    echo "$i $a" >> "$RUN/order.txt"
  done
done
rm -rf "$DATA"
echo done > "$RUN/DONE"
