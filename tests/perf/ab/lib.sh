# Sourced by the run scripts: the quiet() gate, the run directory's
# DECISION and PROVENANCE checks, and per-run load records.

# The value of `key = value` in the run's DECISION; fails if absent.
decision() {  # <run-dir> <key>
  local v
  [ -r "$1/DECISION" ] || { echo "no $1/DECISION: write it before the first round" >&2; return 1; }
  v=$(awk -v k="$2" '
    { line = $0; sub(/^[ \t]+/, "", line) }
    index(line, k) == 1 {
      rest = substr(line, length(k) + 1)
      if (rest ~ /^[ \t]*=/) { sub(/^[ \t]*=[ \t]*/, "", rest); sub(/[ \t]+$/, "", rest); print rest; exit }
    }' "$1/DECISION")
  if [ -z "$v" ]; then
    echo "DECISION in $1 has no '$2 = ...' line" >&2
    return 1
  fi
  case $v in
    *'<'*) echo "DECISION in $1: replace the template's $2 = $v" >&2; return 1 ;;
  esac
  echo "$v"
}

sha256() {  # <file>
  if command -v sha256sum > /dev/null; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

# Both files are written before the first counted round, and every
# binary that runs must be one PROVENANCE names by its sha256.
require_protocol() {  # <run-dir> <binary>...
  local run=$1 b sum
  shift
  for f in DECISION PROVENANCE; do
    [ -s "$run/$f" ] || { echo "write $run/$f before the first round" >&2; return 1; }
  done
  for b in "$@"; do
    [ -x "$b" ] || { echo "not an executable: $b" >&2; return 1; }
    sum=$(sha256 "$b")
    grep -q "$sum" "$run/PROVENANCE" || {
      echo "PROVENANCE does not name $b (sha256 $sum)" >&2
      return 1
    }
  done
}

load1() {
  if [ -r /proc/loadavg ]; then
    awk '{print $1}' /proc/loadavg
  else
    sysctl -n vm.loadavg | awk '{print $2}'
  fi
}

compiling() { pgrep -f 'clang -cc1|cc1plus|clang-tidy' > /dev/null; }

# Waits up to <compiler-wait-s> for no compiler, then up to
# <load-wait-s> for a one-minute load under <load-max>, and returns
# either way: what the host was doing is in each run's .load file.
quiet() {  # <load-max> <load-wait-s> <compiler-wait-s>
  local max=$1 load_wait=$2 compiler_wait=$3 waited=0
  while compiling && [ "$waited" -lt "$compiler_wait" ]; do sleep 10; waited=$((waited + 10)); done
  waited=0
  while [ "$waited" -lt "$load_wait" ]; do
    awk -v l="$(load1)" -v m="$max" 'BEGIN { exit !(l < m) }' && return 0
    sleep 10
    waited=$((waited + 10))
  done
  return 0
}

# The gate from the run's DECISION (quiet.load_max, quiet.load_wait_s,
# quiet.compiler_wait_s).
quiet_for() {  # <run-dir>
  quiet "$(decision "$1" quiet.load_max)" "$(decision "$1" quiet.load_wait_s)" \
        "$(decision "$1" quiet.compiler_wait_s)"
}

# A run's host state at its start or end, appended to <name>.load: the
# uptime line (decide.py reads its one-minute load), the shell's nice
# value, and whether a compiler ran.
record_load() {  # <file> <start|end>
  {
    uptime
    echo "$2 nice $(ps -o nice= -p $$ | tr -d ' ')"
    compiling && echo "compiler running at $2"
  } >> "$1"
  return 0
}

# Odd rounds run A first, even rounds B first.
arms() {  # <round>
  if [ $(($1 % 2)) -eq 1 ]; then echo "A B"; else echo "B A"; fi
}
