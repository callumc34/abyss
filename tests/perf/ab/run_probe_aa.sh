#!/usr/bin/env bash
# The A/A control: run_probe_ab.sh with one probe binary as both arms,
# in the same session as the A/B it calibrates. Its DECISION has
# `experiment = aa` (templates/DECISION.aa).
#
#   run_probe_aa.sh <run-dir> <probe> [round...]
set -u
if [ $# -lt 2 ]; then
  sed -n '2,6p' "$0"
  exit 2
fi
run=$1 probe=$2
shift 2
exec "$(dirname "$0")/run_probe_ab.sh" "$run" "$probe" "$probe" "$@"
