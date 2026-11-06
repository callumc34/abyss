"""Decides an A/B run directory by the rule in its DECISION file.

  python3 -I decide.py <run-dir> [--aa <aa-run-dir>]

DECISION is prose plus `key = value` lines (see templates/). Its
`experiment` names the decision: `server` (run_server_ab.sh), `probe`
(run_probe_ab.sh) or `aa` (an A/A control, run_probe_aa.sh). Every
threshold comes from DECISION; none is defaulted here. The estimator:
per round, B/A of each arm's statistic; over rounds, the geometric mean.
A probe DECISION's `aa.calibrates` names the workloads whose mean gate
is decided only against an A/A control (--aa) of that workload whose
spread is within `aa.spread_max`.

Exits 0 when every gate passes, 1 otherwise.
"""
import argparse
import importlib.util
import json
import math
import os
import re
import statistics
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
_SPEC = importlib.util.spec_from_file_location("abyss_ab_hdr", os.path.join(HERE, "hdr.py"))
hdr = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(hdr)

KEY = re.compile(r"^\s*(experiment|rounds|workloads|gate\.[\w.]+|aa\.[\w.]+|quiet\.[\w.]+)"
                 r"\s*=\s*(\S.*?)\s*$")
NAN = float("nan")


class Decision:
    def __init__(self, run):
        self.path = os.path.join(run, "DECISION")
        self.keys = {}
        with open(self.path) as f:
            for line in f:
                m = KEY.match(line)
                if not m:
                    continue
                if m.group(1) in self.keys:
                    sys.exit(f"{self.path}: {m.group(1)} is set twice")
                self.keys[m.group(1)] = m.group(2)

    def has(self, key):
        return key in self.keys

    def get(self, key):
        if key not in self.keys:
            sys.exit(f"{self.path} has no '{key} = ...' line")
        if "<" in self.keys[key]:
            sys.exit(f"{self.path}: replace the template's {key} = {self.keys[key]}")
        return self.keys[key]

    def num(self, key):
        return float(self.get(key))

    def rounds(self):
        out = []
        for part in self.get("rounds").replace(",", " ").split():
            lo, _, hi = part.partition("-")
            out.extend(range(int(lo), int(hi or lo) + 1))
        return out

    def workloads(self):
        return self.get("workloads").split()


class Verdicts:
    def __init__(self):
        self.items = []

    def gate(self, name, ok, detail):
        self.items.append((name, ok))
        print(f"GATE {name}: {'PASS' if ok else 'FAIL'} ({detail})")

    def undecided(self, name, why):
        self.items.append((name, None))
        print(f"GATE {name}: NOT DECIDED ({why})")

    def finish(self):
        failed = [n for n, ok in self.items if ok is False]
        open_ = [n for n, ok in self.items if ok is None]
        if not failed and not open_:
            print(f"VERDICT: PASS, all {len(self.items)} gates")
            return 0
        print(f"VERDICT: FAIL; failed: {', '.join(failed) or 'none'}; "
              f"not decided: {', '.join(open_) or 'none'}")
        return 1


def geo(xs):
    return math.exp(sum(math.log(x) for x in xs) / len(xs))


def listed(xs, fmt="{:.4f}"):
    return " ".join(fmt.format(x) for x in xs)


def median(xs):
    return statistics.median(list(xs))


def read(path):
    with open(path) as f:
        return f.read()


def start_load(path):
    m = re.search(r"load averages?:\s*([\d.]+)", read(path))
    return float(m.group(1)) if m else NAN


def prom(path):
    out = {}
    for line in read(path).splitlines():
        if line.startswith("#") or not line.strip():
            continue
        k, v = line.rsplit(" ", 1)
        out[k] = float(v)
    return out


def cpu_seconds(t):
    s = 0.0
    for part in t.strip().split(":"):
        s = s * 60 + float(part)
    return s


def sign_p(worse, n):
    return sum(math.comb(n, k) for k in range(worse, n + 1)) / 2 ** n


# --- server: run_server_ab.sh ---

SERVER_RUNS = (("d1-pc", "SET"), ("d32-pc", "SET"), ("d1-pl", "SET"), ("d32-pl", "SET"),
               ("ping32", "PING"), ("hol-alone", "GET"), ("hol-scan", "GET"),
               ("mix-pl", "GET"), ("mix-pc", "GET"))


def loadgen_run(run, name, op):
    base = os.path.join(run, name)
    with open(base + ".json") as f:
        r = json.load(f)
    o = r["operations"][op]
    log = read(base + ".log")
    m = re.search(r"(\d+) replies answered their requests in order, (\d+) did not", log)
    cpu = read(base + ".cpu").split()
    return dict(
        n=o["count"], tput=o["throughput_ops"], p50=o["latency_us"]["p50"],
        mean=hdr.op_stats(o)["mean"], p99=o["latency_us"]["p99"],
        errors=sum(x["errors"] for x in r["operations"].values()),
        mismatched=int(m.group(2)) if m else -1, rc=log.strip().splitlines()[-1],
        lg_cpu=r["driver"]["cpu_per_wall_second"],
        srv_cpu=(cpu_seconds(cpu[1]) - cpu_seconds(cpu[0])) if len(cpu) == 2 else NAN,
        load=start_load(base + ".load"), ops=r["operations"],
        prom=(prom(base + ".start.prom"), prom(base + ".end.prom")))


def delta(r, key):
    start, end = r["prom"]
    return end.get(key, 0.0) - start.get(key, 0.0)


def decide_server(run, d, v):
    rounds = d.rounds()
    res = {(a, i, w): loadgen_run(run, f"{a}-{i}-{w}", op)
           for a in "AB" for i in rounds for w, op in SERVER_RUNS}

    def per_round(w, key, num="B", den="A", wden=None):
        return [res[num, i, w][key] / res[den, i, wden or w][key] for i in rounds]

    print(f"rounds {rounds}")
    p50_max, mean_max = d.num("gate.d1.p50_max"), d.num("gate.d1.mean_max")
    for w in ("d1-pc", "d1-pl"):
        p50, mean = per_round(w, "p50"), per_round(w, "mean")
        v.gate(f"{w}: B/A p50 <= {p50_max:g}, mean <= {mean_max:g}",
               geo(p50) <= p50_max and geo(mean) <= mean_max,
               f"p50 {geo(p50):.4f}, mean {geo(mean):.4f}")
        print(f"   p50 B/A rounds {listed(p50)}")
        print(f"   mean B/A rounds {listed(mean)}")
        for a in "AB":
            print(f"   {a} median p50 {median(res[a, i, w]['p50'] for i in rounds):.2f} us, "
                  f"mean {median(res[a, i, w]['mean'] for i in rounds):.2f} us, "
                  f"p99 {median(res[a, i, w]['p99'] for i in rounds):.1f} us, "
                  f"tput {median(res[a, i, w]['tput'] for i in rounds):.0f}/s")

    low = d.num("gate.pipelining.min")
    xs = [res["B", i, "d32-pl"]["tput"] / res["B", i, "d1-pl"]["tput"] for i in rounds]
    v.gate(f"pipelining: B d32-pl / d1-pl writes/s geomean >= {low:g}", geo(xs) >= low,
           f"{geo(xs):.2f}")
    print(f"   rounds {listed(xs, '{:.1f}')}")
    xs = [res["A", i, "d32-pl"]["tput"] / res["A", i, "d1-pl"]["tput"] for i in rounds]
    print(f"   A for contrast: geomean {geo(xs):.2f}, rounds {listed(xs, '{:.2f}')}")

    high = d.num("gate.flushes_per_write.max")
    fpw = {(a, w): [delta(res[a, i, w], "abyss_wal_flush_duration_seconds_count") /
                    delta(res[a, i, w], "abyss_queue_appended_total") for i in rounds]
           for a in "AB" for w in ("d1-pl", "d32-pl")}
    v.gate(f"flushes per write: B d32-pl <= {high:g} every round",
           all(x <= high for x in fpw["B", "d32-pl"]), f"max {max(fpw['B', 'd32-pl']):.3f}")
    for (a, w), xs in fpw.items():
        print(f"   {a} {w}: median {median(xs):.3f} (range {min(xs):.3f}-{max(xs):.3f})")

    high = d.num("gate.client_share.max")
    xs = [res["B", i, "d32-pl"]["tput"] / res["B", i, "ping32"]["tput"] for i in rounds]
    v.gate(f"client headroom: B d32-pl / ping32 <= {high:g} every round",
           all(x <= high for x in xs), f"max {max(xs):.4f}")
    xs = [res["B", i, "d32-pc"]["tput"] / res["B", i, "ping32"]["tput"] for i in rounds]
    print(f"   d32-pc / ping32 (reported): median {median(xs):.3f} "
          f"(range {min(xs):.3f}-{max(xs):.3f})")

    bad = []
    for a in "AB":
        for i in rounds:
            for w in ("d32-pc", "d32-pl", "ping32"):
                r = res[a, i, w]
                if r["errors"] or r["mismatched"] != 0 or r["rc"] != "rc=0":
                    bad.append(f"{a}-{i}-{w}: errors {r['errors']} mismatched {r['mismatched']} "
                               f"{r['rc']}")
            if " 0 out of order" not in read(os.path.join(run, f"{a}-{i}-order.txt")):
                bad.append(f"{a}-{i}-order")
    n = sum(res[a, i, w]["n"] for a in "AB" for i in rounds for w in ("d32-pc", "d32-pl"))
    v.gate("order: no error or mismatched reply at depth 32, every order check clean", not bad,
           f"{n} measured depth-32 SETs; {'; '.join(bad[:4]) or 'none bad'}")

    low = d.num("gate.pc_ratio.min")
    xs = [res["B", i, "d32-pc"]["tput"] / res["B", i, "d1-pc"]["tput"] for i in rounds]
    v.gate(f"pipelining never hurts: B d32-pc / d1-pc geomean and every round >= {low:g}",
           geo(xs) >= low and min(xs) >= low, f"geomean {geo(xs):.2f}, min {min(xs):.2f}")

    high = d.num("gate.hol_ratio.max")
    for a in "AB":
        xs = [res[a, i, "hol-scan"]["p99"] / res[a, i, "hol-alone"]["p99"] for i in rounds]
        cold = [delta(res[a, i, "hol-scan"], 'abyss_hits_total{tier="cold"}') for i in rounds]
        scans = [delta(res[a, i, "hol-scan"], 'abyss_engine_io_run_seconds_count{kind="scan"}')
                 for i in rounds]
        if a == "B":
            if min(cold) <= 0 or min(scans) <= 0:
                v.undecided("head-of-line", "a hol-scan round read nothing cold")
            else:
                v.gate(f"head-of-line: B hot GET p99 with scans / alone <= {high:g} every round",
                       all(x <= high for x in xs), f"geomean {geo(xs):.2f}, max {max(xs):.2f}")
        else:
            print(f"   A (reported): geomean {geo(xs):.2f}")
        print(f"   {a} rounds {listed(xs, '{:.2f}')}; cold hits during scans min {min(cold):.0f}, "
              f"pool scans min {min(scans):.0f}")
        for w in ("hol-alone", "hol-scan"):
            p99 = [res[a, i, w]["p99"] for i in rounds]
            print(f"   {a} {w}: median p50 {median(res[a, i, w]['p50'] for i in rounds):.1f} "
                  f"p99 {median(p99):.1f} us (p99 range {min(p99):.1f}-{max(p99):.1f})")

    print("REPORTED depth-32 throughput B/A")
    for w in ("d32-pc", "d32-pl", "ping32"):
        xs = per_round(w, "tput")
        print(f"   {w}: geomean {geo(xs):.3f}, rounds {listed(xs, '{:.3f}')}")
        for a in "AB":
            ts = [res[a, i, w]["tput"] for i in rounds]
            print(f"   {a} median {median(ts):.0f}/s (range {min(ts):.0f}-{max(ts):.0f}), "
                  f"p50 {median(res[a, i, w]['p50'] for i in rounds):.0f} us")
    print("REPORTED mix: GET throughput, 64 connections, 90/10, closed loop")
    for w in ("mix-pl", "mix-pc"):
        for a in "AB":
            g = [res[a, i, w]["tput"] for i in rounds]
            s = [res[a, i, w]["ops"]["SET"]["throughput_ops"] for i in rounds]
            print(f"   {w} {a}: GET median {median(g):.0f}/s ({min(g):.0f}-{max(g):.0f}), "
                  f"SET median {median(s):.0f}/s, GET p99 "
                  f"{median(res[a, i, w]['p99'] for i in rounds):.0f} us, errors "
                  f"{sum(res[a, i, w]['errors'] for i in rounds)}, loadgen "
                  f"{median(res[a, i, w]['lg_cpu'] for i in rounds):.2f} cores")
    print("REPORTED server lock hold mean and CPU")
    for w in ("d1-pc", "d32-pc", "d1-pl", "d32-pl"):
        for a in "AB":
            hold = [delta(res[a, i, w], "abyss_sequencer_lock_hold_seconds_sum") /
                    max(delta(res[a, i, w], "abyss_sequencer_lock_hold_seconds_count"), 1) * 1e6
                    for i in rounds]
            print(f"   {w} {a}: hold mean median {median(hold):.2f} us; server CPU "
                  f"{median(res[a, i, w]['srv_cpu'] for i in rounds):.1f} s, loadgen "
                  f"{median(res[a, i, w]['lg_cpu'] for i in rounds):.2f} cores")
    for a in "AB":
        print(f"   start loads {a}: " + listed((res[a, i, "d1-pc"]["load"] for i in rounds),
                                            "{:.2f}"))


# --- probe: run_probe_ab.sh, and its A/A control ---

STAGES = ("lock_wait", "decide", "entries", "reserve", "apply", "complete", "admit", "durable")


def histogram(r, series):
    for h in r.get("server_histograms", []):
        if h["series"] == series:
            return h
    return None


def probe_run(run, name):
    base = os.path.join(run, name)
    with open(base + ".json") as f:
        r = json.load(f)
    op = r["operations"]["write_ack"]
    s = hdr.op_stats(op)
    med = s["med"]
    under = [(x, c) for x, c in s["buckets"] if x <= 1_000_000]
    under_n = sum(c for _, c in under)
    flush = histogram(r, "abyss_wal_flush_duration_seconds")
    hold = histogram(r, "abyss_sequencer_lock_hold_seconds")
    stages = {}
    for st in STAGES:
        h = histogram(r, f'abyss_sequencer_phase_seconds{{phase="{st}"}}')
        if h:
            stages[st] = h["mean_us"]
    return dict(
        p50=op["latency_us"]["p50"], mean=s["mean"], p99=op["latency_us"]["p99"],
        # A power_loss ack never lands under 1 ms.
        under=(sum(med(x) * c for x, c in under) / under_n / 1000) if under_n else NAN,
        stalls=sum(c for x, c in s["buckets"] if x > 10_000_000), errors=op["errors"],
        flush=flush["mean_us"] if flush else NAN,
        hold_mean=hold["mean_us"] if hold else NAN,
        hold_p50=hold["p50"]["le_us"] if hold else NAN,
        hold_p99=hold["p99"]["le_us"] if hold else NAN,
        hold_n=hold["count"] if hold else 0, stages=stages,
        lag=r["driver"]["lagging"], ovl=r["driver"]["overloaded"],
        load=start_load(base + ".load"), tree=r["build"].get("source_tree", "?"))


def probe_rows(run, rounds, w):
    return [(i, probe_run(run, f"A-{i}-{w}"), probe_run(run, f"B-{i}-{w}")) for i in rounds]


def aa_spread(ratios):
    """The A/A geomean's distance from 1 plus twice its standard error,
    as a ratio: what the A/B estimator reads with no difference."""
    logs = [math.log(x) for x in ratios]
    se = statistics.stdev(logs) / math.sqrt(len(logs)) if len(logs) > 1 else math.inf
    return math.exp(abs(sum(logs) / len(logs)) + 2 * se)


def aa_report(aa_run, w):
    d = Decision(aa_run)
    if d.get("experiment") != "aa":
        sys.exit(f"{aa_run} is not an A/A control (experiment = {d.get('experiment')})")
    if w not in d.workloads():
        return None
    rows = probe_rows(aa_run, d.rounds(), w)
    mean = [b["mean"] / a["mean"] for _, a, b in rows]
    p50 = [b["p50"] / a["p50"] for _, a, b in rows]
    return dict(mean=mean, p50=p50, spread=aa_spread(mean), p50_spread=aa_spread(p50), rows=rows)


def decide_probe(run, d, v, aa_run):
    rounds = d.rounds()
    print(f"rounds {rounds}")
    gated = False
    for w in d.workloads():
        rows = probe_rows(run, rounds, w)
        for stat in ("p50", "mean"):
            key = f"gate.{w}.{stat}_max"
            if not d.has(key):
                continue
            gated = True
            xs = [b[stat] / a[stat] for _, a, b in rows]
            name = f"{w} {stat}: B/A <= {d.num(key):g}"
            detail = f"{geo(xs):.4f}, rounds {min(xs):.3f}-{max(xs):.3f}"
            if stat == "mean" and d.has("aa.calibrates") and w in d.get("aa.calibrates").split():
                limit = d.num("aa.spread_max")
                aa = aa_report(aa_run, w) if aa_run else None
                if aa is None:
                    v.undecided(name, f"{detail}; the A/A-calibrated rule needs an A/A "
                                      f"control of {w} (--aa)")
                    continue
                spread = aa["spread"]
                if spread > limit:
                    v.undecided(name, f"{detail}; A/A spread {spread:.4f} > {limit:g}")
                    continue
                detail += f"; A/A spread {spread:.4f} <= {limit:g}"
            v.gate(name, geo(xs) <= d.num(key), detail)
        for stat in ("mean", "p50"):
            key = f"gate.{w}.lock_hold_{stat}_max"
            if not d.has(key):
                continue
            gated = True
            xs = [b[f"hold_{stat}"] / a[f"hold_{stat}"] for _, a, b in rows]
            v.gate(f"{w} lock hold {stat}: B/A <= {d.num(key):g}", geo(xs) <= d.num(key),
                   f"{geo(xs):.4f}, rounds {listed(xs, '{:.3f}')}")
        report_probe(rows)
    if not gated:
        sys.exit(f"{d.path} gates none of its workloads")


def report_probe(rows):
    under = [b["under"] / a["under"] for _, a, b in rows]
    p99 = [b["p99"] / a["p99"] for _, a, b in rows]
    worse = sum(1 for _, a, b in rows if b["p99"] > a["p99"])
    if not any(math.isnan(x) for x in under):
        print(f"   acks <= 1 ms mean B/A {geo(under):.4f}")
    print(f"   p99 B/A {geo(p99):.4f}, B worse in {worse}/{len(rows)} "
          f"(sign p={sign_p(worse, len(rows)):.3f})")
    for key in ("p50", "mean", "p99", "flush", "hold_mean", "hold_p50"):
        a, b = median(r[1][key] for r in rows), median(r[2][key] for r in rows)
        if not (math.isnan(a) or math.isnan(b)):
            print(f"   median {key}: A {a:.3f} B {b:.3f}")
    print("   acks over 10 ms: " + (" ".join(f"{i}:A{a['stalls']}/B{b['stalls']}"
                                             for i, a, b in rows if a["stalls"] or b["stalls"])
                                    or "none"))
    print("   errors A %d B %d; lagging A %d B %d; overloaded A %d B %d" % (
        sum(a["errors"] for _, a, _ in rows), sum(b["errors"] for _, _, b in rows),
        sum(a["lag"] for _, a, _ in rows), sum(b["lag"] for _, _, b in rows),
        sum(a["ovl"] for _, a, _ in rows), sum(b["ovl"] for _, _, b in rows)))
    if any(a["hold_n"] for _, a, _ in rows):
        print(f"   sampled holds per run: A median {median(a['hold_n'] for _, a, _ in rows):.0f}, "
              f"B median {median(b['hold_n'] for _, _, b in rows):.0f}")
        for st in STAGES:
            if all(st in a["stages"] and st in b["stages"] for _, a, b in rows):
                ratio = geo([max(b["stages"][st], 1e-3) / max(a["stages"][st], 1e-3)
                             for _, a, b in rows])
                print(f"   stage {st}: mean us median A "
                      f"{median(a['stages'][st] for _, a, _ in rows):.3f} B "
                      f"{median(b['stages'][st] for _, _, b in rows):.3f}, paired B/A {ratio:.3f}")
    for i, a, b in rows:
        print(f"   round {i}: A p50 {a['p50']:.3f} mean {a['mean']:.2f} p99 {a['p99']:.0f} "
              f"load {a['load']:.2f} | B p50 {b['p50']:.3f} mean {b['mean']:.2f} "
              f"p99 {b['p99']:.0f} load {b['load']:.2f}")
    trees = sorted({r[k]["tree"] for r in rows for k in (1, 2)})
    print(f"   source trees: {', '.join(trees)}")


def decide_aa(run, d):
    limit = d.num("aa.spread_max") if d.has("aa.spread_max") else None
    for w in d.workloads():
        aa = aa_report(run, w)
        print(f"{w}: A/A mean ratio geomean {geo(aa['mean']):.4f} "
              f"(rounds {listed(aa['mean'], '{:.3f}')}), spread {aa['spread']:.4f}; "
              f"p50 ratio geomean {geo(aa['p50']):.4f}, spread {aa['p50_spread']:.4f}")
        if limit is not None:
            ok = aa["spread"] <= limit
            print(f"CALIBRATION {w}: mean spread {aa['spread']:.4f} "
                  f"{'<=' if ok else '>'} {limit:g}: an A/B mean gate on {w} is "
                  f"{'informative' if ok else 'not informative on this host'}")
        for i, a, b in aa["rows"]:
            print(f"   round {i}: first arm mean {a['mean']:.2f} acks>10ms {a['stalls']} "
                  f"load {a['load']:.2f} | second arm mean {b['mean']:.2f} "
                  f"acks>10ms {b['stalls']} load {b['load']:.2f}")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("run")
    parser.add_argument("--aa", help="the A/A control's run directory, for a probe run")
    args = parser.parse_args()
    d = Decision(args.run)
    kind = d.get("experiment")
    v = Verdicts()
    if kind == "server":
        decide_server(args.run, d, v)
    elif kind == "probe":
        decide_probe(args.run, d, v, args.aa)
    elif kind == "aa":
        decide_aa(args.run, d)
        return 0
    else:
        sys.exit(f"{d.path}: unknown experiment {kind!r} (server, probe or aa)")
    return v.finish()


if __name__ == "__main__":
    sys.exit(main())
