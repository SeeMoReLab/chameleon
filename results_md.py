#!/usr/bin/env python3
"""Seed-averaged per-phase and per-sub-SLA tables for every finished run under results/, as Markdown.

Reads results/<family>/<config>_<mode>_<sMax>_<seed>/, the run folders a sweep leaves behind
(run.log, occupancy_<i>.csv.gz, client_metrics_global.csv.gz) sorted into one subfolder per
experiment family, and prints one Markdown document with a section per family:

  failure              follower and leader failure, chameleon only, seeds 7, 42, 100
  mongodb              local, regional, global, all five arms, seeds 7, 42, 100
  mongodb_scalability  27, 21, 15, 9, 3 nodes, all five arms, seeds 7, 42, 100
  raft                 local, regional, global, all five arms, seeds 7, 42, 100
  raft_scalability     27, 21, 15, 9, 3 servers, all five arms, seeds 7, 42, 100
  random               config_random_1..30 (each config's seed is its number), all five arms

Every (type, arm) the plan names gets a subsection, also before any of its seeds exist, headed by
the seeds found against the seeds planned. Only averages over the seeds found are printed. Each
subsection has the per-phase table (sub-SLAs pooled) closed by two whole-run rows, with and
without WarmUp and CoolDown; one sub-SLA table per phase, each closed by an all-sub-SLAs row; and
two whole-run sub-SLA tables, with and without WarmUp and CoolDown. Each type opens with a table
comparing its arms. The random family, whose seeds are distinct configs rather than repeats of one,
also prints that comparison table for every config on its own (the summary only, no per-phase or
sub-SLA breakdown) before the pooled arm subsections.

The per-phase rows come from the driver's 5 s progress lines in run.log, which are already
labelled with the phase, so their counts are exact per phase. The sub-SLA tables come from the
client ledger (one row per cell per ~1 s flush), mapped onto phases from the workload start,
taken as the ledger's first row, which is written at the first request: each flush counts into
the phase holding the midpoint of the interval it closes, and the drain tail after the last
phase counts into the last phase. The leader's utilization, price and in-flight come from the
occupancy stream (100 ms samples, role LEADER or PRIMARY), mapped onto phases by timestamp from
the same start. Both mappings are accurate to about half a sample.

Every family folder must exist under results/, even when empty; other entries under results/
(such as legacy/) are ignored. Inside a family folder, a run whose arm or seed is not in the
family's plan is an error, as are two runs of the same (type, arm, seed) and seeds of one
(type, arm) with different phase schedules.

Usage:
  python3 results_md.py [--results-dir results] > RESULTS.md
"""

import argparse
import collections
import os
import re
import sys

import numpy as np
import pandas as pd

TICK_RE = re.compile(
    r"\[(\d+)s\] Sent=(\d+) \| Skipped=(\d+) \| Served=(\d+) \| Rejected=(\d+) \| Lost=(\d+) "
    r"\| Violations=(\d+) \| PredictedProfit=(\d+) \| RealizedProfit=(\d+) \| Phase=(\w+)")
PHASE_RE = re.compile(r"^Starting Phase \d+/\d+: (\w+)")
DURATION_RE = re.compile(r"^\s*Duration: (\d+)s \| Total TPS: (\d+)")
DONE_RE = re.compile(r"^Experiment completed\. (.*)$")

ARMS = ["chameleon", "chameleonPileus", "pileus", "strongest", "weakest"]
RUN_NAME_RE = re.compile(r"^(?P<stem>config_.+)_(?P<arm>" + "|".join(ARMS) + r")_(?P<smax>\d+)_(?P<seed>\d+)$")
LEADER_ROLES = {"LEADER", "PRIMARY"}
CEILING = 1000.0  # lambda at or above this means the scorer refuses every level (CALIBRATION.md)
EDGE_PHASES = ("WarmUp", "CoolDown")
ALL, CORE = "whole run", "whole run without WarmUp and CoolDown"

# Ledger columns summed per (phase, sub-SLA). A request ends in exactly one of the outcome columns.
OUTCOMES = ["Count", "Rejected", "Lost", "DeadlineExceeded", "ClientDropped", "Failures"]
LEDGER_SUMS = OUTCOMES + ["Violations", "UpgradesFree", "UpgradesWaiting", "Fallbacks", "Redirects",
                          "PredictedProfitSum", "RealizedProfitSum"]
# The ledger's end-to-end latency histogram of served requests, bucketed as in ClientMetricsTracker: bucket i
# holds latencies up to LAT_BASE_MS * LAT_RATIO^(i+1), and the last bucket is the overflow above them all.
LAT_BUCKETS = 64
LAT_BASE_MS = 0.5
LAT_RATIO = 1.15
LAT_COLUMNS = [f"LatBucket{i}" for i in range(LAT_BUCKETS + 1)]

Family = collections.namedtuple("Family", "folder title types arms seeds per_seed")
# title None: the family has a single type and its arms sit directly under the family heading.
# per_seed: the seeds are distinct configs, so each one also gets its own arm comparison table.
Type = collections.namedtuple("Type", "key title stem_re")

SEEDS = [7, 42, 100]
SIZES = [27, 21, 15, 9, 3]
TOPOLOGIES = ["local", "regional", "global"]
FAMILIES = [
    Family("failure", "Failure",
           [Type("follower", "follower failure", re.compile(r"config_failure_follower_global")),
            Type("leader", "leader failure", re.compile(r"config_failure_leader_global"))],
           ["chameleon"], SEEDS, False),
    Family("mongodb", "MongoDB",
           [Type(t, t, re.compile(rf"config_{t}_mongodb")) for t in TOPOLOGIES], ARMS, SEEDS, False),
    Family("mongodb_scalability", "MongoDB scalability",
           [Type(str(n), f"{n} nodes", re.compile(rf"config_scale_mongodb_{n}")) for n in SIZES], ARMS, SEEDS, False),
    Family("raft", "Raft",
           [Type(t, t, re.compile(rf"config_{t}")) for t in TOPOLOGIES], ARMS, SEEDS, False),
    Family("raft_scalability", "Raft scalability",
           [Type(str(n), f"{n} servers", re.compile(rf"config_scale_{n}")) for n in SIZES], ARMS, SEEDS, False),
    Family("random", "Random",
           [Type("random", None, re.compile(r"config_random_(\d+)"))], ARMS, list(range(1, 31)), True),
]


class Run:
    def __init__(self, path, smax, seed):
        self.path = path
        self.name = os.path.basename(path)
        self.smax = smax
        self.seed = seed
        self.phases = []          # (name, duration_s) in order
        self.ticks = collections.defaultdict(collections.Counter)  # phase -> progress-line sums
        self.lines = 0
        self.summary = {}
        self._parse_run_log()
        names = [n for n, _ in self.phases]
        if names[0] != EDGE_PHASES[0] or names[-1] != EDGE_PHASES[1] or len(names) < 3:
            raise ValueError(f"{self.name}: the schedule must open with {EDGE_PHASES[0]}, close with "
                             f"{EDGE_PHASES[1]} and have a phase between them, got {names}")
        self.edges = np.cumsum([0] + [d for _, d in self.phases]).astype(float)
        t0 = self._parse_ledger()
        self._parse_occupancy(t0)

    def windows(self):
        """Each phase, then the two whole-run windows, as (label, phase indices)."""
        n = len(self.phases)
        return ([(name, [i]) for i, (name, _) in enumerate(self.phases)]
                + [(ALL, list(range(n))), (CORE, list(range(1, n - 1)))])

    def _parse_run_log(self):
        pending = None
        with open(os.path.join(self.path, "run.log"), errors="replace") as f:
            for line in f:
                m = PHASE_RE.match(line)
                if m:
                    pending = m.group(1)
                    continue
                m = DURATION_RE.match(line)
                if m and pending is not None:
                    self.phases.append((pending, int(m.group(1))))
                    pending = None
                    continue
                m = TICK_RE.match(line)
                if m:
                    c = self.ticks[m.group(10)]
                    for key, i in (("sent", 2), ("skip", 3), ("served", 4), ("rej", 5), ("lost", 6),
                                   ("viol", 7), ("pred", 8), ("real", 9)):
                        c[key] += int(m.group(i))
                    c["ticks"] += 1
                    self.lines += 1
                    continue
                m = DONE_RE.match(line)
                if m:
                    self.summary = {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", m.group(1))}
        if not self.phases:
            raise ValueError(f"{self.name}: no phase schedule in run.log")
        if not self.summary:
            raise ValueError(f"{self.name}: run.log has no 'Experiment completed' line")

    def _parse_ledger(self):
        path = os.path.join(self.path, "client_metrics_global.csv.gz")
        header = pd.read_csv(path, nrows=0).columns
        buckets = [c for c in header if c.startswith("LatBucket")]
        if buckets != LAT_COLUMNS:
            raise ValueError(f"{self.name}: client ledger has latency buckets {buckets[:1]}..{buckets[-1:]}, expected "
                             f"LatBucket0..LatBucket{LAT_BUCKETS}; update LAT_* to match ClientMetricsTracker")
        try:
            df = pd.read_csv(path, usecols=["Timestamp", "ChosenLevel"] + LEDGER_SUMS + LAT_COLUMNS)
        except ValueError as e:
            raise ValueError(f"{self.name}: client ledger lacks a needed column ({e})") from None
        if df.empty:
            raise ValueError(f"{self.name}: empty client ledger")
        t0 = int(df["Timestamp"].min())
        # A flush's rows share its stamp, taken when the interval closes, so the rows cover
        # (previous flush, own stamp]; place each flush by the midpoint of that interval. By the
        # stamp alone every phase would lose its last second to the next one.
        stamps = np.unique(df["Timestamp"].to_numpy())
        mid = (stamps + np.concatenate(([stamps[0]], stamps[:-1]))) / 2.0
        # Flushes past the last phase's end are its drain tail and count into it.
        flush_phase = np.minimum(np.searchsorted(self.edges, (mid - t0) / 1000.0, side="right") - 1,
                                 len(self.phases) - 1)
        df["phase"] = flush_phase[np.searchsorted(stamps, df["Timestamp"].to_numpy())]
        g = df.groupby(["phase", "ChosenLevel"])[LEDGER_SUMS + LAT_COLUMNS].sum()
        self.ledger = {key: collections.Counter(row._asdict()) for key, row in
                       zip(g.index, g.itertuples(index=False))}
        return t0

    def _parse_occupancy(self, t0):
        frames = []
        for name in sorted(os.listdir(self.path)):
            if re.fullmatch(r"occupancy_\d+\.csv\.gz", name):
                df = pd.read_csv(os.path.join(self.path, name), usecols=["Timestamp", "U", "Lambda", "AvgInFlight", "Role"])
                frames.append(df[df["Role"].isin(LEADER_ROLES)])
        rows = pd.concat(frames).sort_values("Timestamp") if frames else pd.DataFrame()
        if rows.empty:
            raise ValueError(f"{self.name}: no leader rows in the occupancy stream")
        ts = rows["Timestamp"].to_numpy()
        interval_s = (ts[-1] - ts[0]) / 1000.0 / max(1, len(ts) - 1)
        t = (ts - t0) / 1000.0
        idx = np.searchsorted(self.edges, t, side="right") - 1  # -1 before the start, n past the end
        u, lam, infl = (rows[c].to_numpy() for c in ("U", "Lambda", "AvgInFlight"))
        self.leader = {}
        for label, phases in self.windows():
            mask = np.isin(idx, phases)
            if not mask.any():
                self.leader[label] = None
                continue
            self.leader[label] = {
                "u": float(u[mask].mean()),
                "lambda_end": float(lam[mask][-1]),
                "inflight": float(infl[mask].mean()),
                "ceiling_s": float((lam[mask] >= CEILING).sum() * interval_s),
            }


class Group:
    """The runs of one (type, arm), pooled over their seeds."""

    def __init__(self, runs):
        self.runs = sorted(runs, key=lambda r: r.seed)
        self.n = len(self.runs)
        if not self.runs:
            return
        self.phases = self.runs[0].phases
        for r in self.runs[1:]:
            if r.phases != self.phases:
                raise ValueError(f"{r.name} and {self.runs[0].name} have different phase schedules")
        self.windows = self.runs[0].windows()
        names = [n for n, _ in self.phases]
        durations = [d for _, d in self.phases]
        self.ticks, self.ledger, self.seconds, self.leader = {}, {}, {}, {}
        for label, idx in self.windows:
            c = collections.Counter()
            for r in self.runs:
                for i in idx:
                    c.update(r.ticks.get(names[i], collections.Counter()))
            self.ticks[label] = c
            by_sla = collections.defaultdict(collections.Counter)
            for r in self.runs:
                for (i, sla), counts in r.ledger.items():
                    if i in idx:
                        by_sla[sla].update(counts)
            self.ledger[label] = by_sla
            self.seconds[label] = self.n * sum(durations[i] for i in idx)
            seen = [r.leader[label] for r in self.runs if r.leader[label] is not None]
            self.leader[label] = ({k: sum(s[k] for s in seen) / len(seen) for k in seen[0]} if seen else None)


def fmt_int(v):
    return f"{v:,.0f}"


def pct(num, den):
    if not den:
        return "n/a"
    v = 100.0 * num / den
    if abs(v) < 0.05:
        v = 0.0  # avoid printing -0.0% for a phase whose carried-over replies slightly exceed its offered load
    return f"{v:.1f}%"


def ratio(num, den):
    return f"{num / den:.3f}" if den else "n/a"


def sci(v):
    return f"{v:.3g}"


def latency_quantile(hist, q):
    """The q-quantile of a latency histogram, as ClientMetricsTracker reports it: the upper edge of the bucket holding
    it, so at most one bucket (15%) above the true value. A quantile in the overflow bucket has no upper edge and
    prints as above the last one."""
    total = sum(hist)
    if not total:
        return "n/a"
    target = np.ceil(q * total)
    i = int(np.searchsorted(np.cumsum(hist), target))
    if i >= LAT_BUCKETS:
        return f"> {LAT_BASE_MS * LAT_RATIO ** LAT_BUCKETS:,.0f}"
    edge = LAT_BASE_MS * LAT_RATIO ** (i + 1)
    return f"{edge:.1f}" if edge < 100 else fmt_int(edge)


def rw_cells(by_sla, seconds):
    """Served reads/s and writes/s, then read p50 and p99, then write p50 and p99 in ms, from the ledger cells of one
    window: the R: and W: sub-SLAs pooled."""
    cells = {}
    for kind in ("R:", "W:"):
        served = sum(c["Count"] for sla, c in by_sla.items() if sla.startswith(kind))
        hist = [sum(c[b] for sla, c in by_sla.items() if sla.startswith(kind)) for b in LAT_COLUMNS]
        # The tracker snapshots each counter on its own, so a request can land in a flush's count and the next flush's
        # histogram; across a phase boundary that leaves a few requests (at most 49 in 13M seen) on either side.
        if abs(sum(hist) - served) > 1e-3 * served:
            raise ValueError(f"{kind} latency histogram holds {sum(hist):,} requests against {served:,} served")
        cells[kind] = (fmt_int(served / seconds), latency_quantile(hist, 0.50), latency_quantile(hist, 0.99))
    (r_rate, r50, r99), (w_rate, w50, w99) = cells["R:"], cells["W:"]
    return [r_rate, w_rate, r50, r99, w50, w99]


RW_HEADER = "reads/s | writes/s | read p50 ms | read p99 ms | write p50 ms | write p99 ms"


def ranges(nums):
    """1, 2, 3, 5, 7, 8 -> '1-3, 5, 7, 8'."""
    runs = []
    for n in nums:
        if runs and n == runs[-1][-1] + 1:
            runs[-1].append(n)
        else:
            runs.append([n])
    return ", ".join(f"{r[0]}-{r[-1]}" if len(r) > 2 else ", ".join(str(n) for n in r) for r in runs)


def seeds_line(group, planned):
    have = [r.seed for r in group.runs]
    missing = [s for s in planned if s not in have]
    parts = [f"have {ranges(have)}" if have else "none yet"]
    if missing:
        parts.append(f"missing {ranges(missing)}")
    line = f"**Seeds {len(have)}/{len(planned)}** ({'; '.join(parts)})."
    if not have:
        return line + " Data currently missing."
    lines = sorted({r.lines for r in group.runs})
    lines_txt = f"{lines[0]}" if len(lines) == 1 else f"{lines[0]}-{lines[-1]}"
    lost = sum(r.summary.get("Lost", 0) for r in group.runs) / group.n
    dropped = sum(r.summary.get("ClientDropped", 0) for r in group.runs) / group.n
    return line + f" Per run: {lines_txt} progress lines, lost {fmt_int(lost)}, client dropped {fmt_int(dropped)}."


def phase_table(group):
    out = ["| phase | offered/s | served | rejected | other | violations | profit/s | pred/real | leader u | lambda end | leader in-flight | at ceiling | " + RW_HEADER + " |",
           "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for label, _ in group.windows:
        c = group.ticks[label]
        name = f"**{label}**" if label in (ALL, CORE) else label
        if not c["ticks"]:
            out.append(f"| {name} | (no progress lines) | | | | | | | | | | |" + " |" * 6)
            continue
        secs = 5 * c["ticks"]
        offered = c["sent"] + c["skip"]
        other = offered - c["served"] - c["rej"]
        lead = group.leader[label]
        if lead:
            ceiling = f"{lead['ceiling_s']:.0f} s" if lead["ceiling_s"] >= 0.5 else ""
            lead_cols = f"{lead['u']:.2f} | {sci(lead['lambda_end'])} | {fmt_int(lead['inflight'])} | {ceiling}"
        else:
            lead_cols = " | | | "
        out.append(f"| {name} | {fmt_int(offered / secs)} | {pct(c['served'], offered)} | {pct(c['rej'], offered)} | "
                   f"{pct(other, offered)} | {pct(c['viol'], c['served'])} | {fmt_int(c['real'] / secs)} | "
                   f"{ratio(c['pred'], c['real'])} | {lead_cols} | "
                   + " | ".join(rw_cells(group.ledger[label], group.seconds[label])) + " |")
    return out


def sla_table(by_sla, seconds):
    total = collections.Counter()
    for counts in by_sla.values():
        total.update(counts)
    out = ["| sub-SLA | attempts/s | served | rejected | other | violations | upgraded | free | fallbacks/s | redirects/s | profit/s | pred/real | served share | profit share |",
           "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    rows = [(sla, by_sla[sla]) for sla in sorted(by_sla)
            if sum(by_sla[sla][k] for k in OUTCOMES) > 0 or by_sla[sla]["RealizedProfitSum"] > 0]
    for sla, c in rows + [("**all sub-SLAs**", total)]:
        attempts = sum(c[k] for k in OUTCOMES)
        upgrades = c["UpgradesFree"] + c["UpgradesWaiting"]
        out.append(f"| {sla} | {fmt_int(attempts / seconds)} | {pct(c['Count'], attempts)} | {pct(c['Rejected'], attempts)} | "
                   f"{pct(attempts - c['Count'] - c['Rejected'], attempts)} | {pct(c['Violations'], c['Count'])} | "
                   f"{pct(upgrades, c['Count'])} | {pct(c['UpgradesFree'], upgrades)} | "
                   f"{fmt_int(c['Fallbacks'] / seconds)} | {fmt_int(c['Redirects'] / seconds)} | "
                   f"{fmt_int(c['RealizedProfitSum'] / seconds)} | {ratio(c['PredictedProfitSum'], c['RealizedProfitSum'])} | "
                   f"{pct(c['Count'], total['Count'])} | {pct(c['RealizedProfitSum'], total['RealizedProfitSum'])} |")
    return out


def comparison_table(groups, arms, planned=None):
    """One row per arm. With planned, the seeds found against the seeds planned are a column; without it (a single
    run per arm) there is no seeds column and an absent run is 'missing'."""
    seeds_col = planned is not None
    out = ["| arm | " + ("seeds | " if seeds_col else "") +
           "profit without WarmUp and CoolDown | vs chameleon | profit, whole run | vs chameleon | served | rejected | violations | "
           + RW_HEADER + " |",
           "|---|" + ("---|" if seeds_col else "") + "---|---|---|---|---|---|---|" + "---|" * 6]
    profit = {arm: {label: groups[arm].ticks[label]["real"] / groups[arm].n for label in (ALL, CORE)}
              for arm in arms if groups[arm].n}
    for arm in arms:
        g = groups[arm]
        seeds = f"{g.n}/{len(planned)} | " if seeds_col else ""
        if not g.n:
            out.append(f"| {arm} | {seeds}missing | | missing | | | | |" + " |" * 6)
            continue
        cells = []
        for label in (CORE, ALL):
            cells.append(f"{profit[arm][label] / 1e6:,.0f}M")
            base = profit.get("chameleon", {}).get(label)
            cells.append(f"{100.0 * (profit[arm][label] / base - 1):+.1f}%" if base else "n/a")
        c = g.ticks[ALL]
        offered = c["sent"] + c["skip"]
        out.append(f"| {arm} | {seeds}" + " | ".join(cells) +
                   f" | {pct(c['served'], offered)} | {pct(c['rej'], offered)} | {pct(c['viol'], c['served'])} | "
                   + " | ".join(rw_cells(g.ledger[CORE], g.seconds[CORE])) + " |")
    return out


def relative_tables(by_arm, arms, seeds, heading):
    """Profit relative to chameleon per seed (config), one table per phase (WarmUp and CoolDown excluded) and one for all
    those phases together, for the seeds where every arm has a run. Each table closes with the arithmetic and the geometric
    mean across those seeds and the count of seeds where the arm is above chameleon. The geometric mean is the one to read
    for robustness: it is the mean of the log ratios, so one seed where an arm scores 1.5 cannot cancel one where it scores 0.1,
    and it gives the same answer whichever arm is the base."""
    others = [a for a in arms if a != "chameleon"]
    complete = []  # (seed, {arm: Run})
    for seed in seeds:
        runs = {arm: [r for r in by_arm[arm].runs if r.seed == seed] for arm in arms}
        if all(runs[arm] for arm in arms):
            complete.append((seed, {arm: runs[arm][0] for arm in arms}))
    if not complete:
        return [f"{heading} Relative profit by config", "", "No config has every arm yet.", ""]
    phases = [name for name, _ in complete[0][1]["chameleon"].phases if name not in EDGE_PHASES]
    for _, runs in complete:
        for r in runs.values():
            if [n for n, _ in r.phases if n not in EDGE_PHASES] != phases:
                raise ValueError(f"{r.name}: phase schedule differs from {complete[0][1]['chameleon'].name}")
    out = []
    for window in phases + [CORE]:
        names = phases if window == CORE else [window]
        rows = []
        for seed, runs in complete:
            profit = {arm: sum(runs[arm].ticks.get(n, {}).get("real", 0) for n in names) for arm in arms}
            if profit["chameleon"] > 0:
                rows.append((seed, {arm: profit[arm] / profit["chameleon"] for arm in others}))
        title = "all phases without WarmUp and CoolDown" if window == CORE else f"phase {window}"
        out += [f"{heading} Relative profit by config, {title}", "",
                "| config | " + " | ".join(others) + " |", "|---|" + "---|" * len(others)]
        for seed, ratio in rows:
            out.append(f"| {seed} | " + " | ".join(f"{ratio[a]:.2f}" for a in others) + " |")
        if rows:
            am = {a: np.mean([r[a] for _, r in rows]) for a in others}
            gm = {a: np.exp(np.mean([np.log(r[a]) for _, r in rows])) for a in others}
            above = {a: sum(r[a] > 1.0 for _, r in rows) for a in others}
            out.append("| arithmetic mean | " + " | ".join(f"{am[a]:.2f}" for a in others) + " |")
            out.append("| **geometric mean** | " + " | ".join(f"**{gm[a]:.2f}**" for a in others) + " |")
            out.append("| configs above chameleon | " + " | ".join(f"{above[a]}/{len(rows)}" for a in others) + " |")
        out.append("")
    return out


def per_seed_tables(by_arm, arms, seeds, heading):
    """The comparison table of every planned seed on its own, from the single run each arm has for it."""
    out = []
    for seed in seeds:
        single = {arm: Group([r for r in by_arm[arm].runs if r.seed == seed]) for arm in arms}
        out.append(f"{heading} config_random_{seed}")
        out.append("")
        if not any(g.n for g in single.values()):
            out.append("Data currently missing.")
        else:
            out.extend(comparison_table(single, arms))
        out.append("")
    return out


def load(results_dir):
    """{(family folder, type key, arm): Group}; every family folder must exist, other entries under results_dir are ignored."""
    if not os.path.isdir(results_dir):
        sys.exit(f"no results directory {results_dir}")
    missing = [f.folder for f in FAMILIES if not os.path.isdir(os.path.join(results_dir, f.folder))]
    if missing:
        sys.exit(f"{results_dir} lacks the family folder(s): {', '.join(missing)}")
    found = collections.defaultdict(list)
    errors = []
    for fam in FAMILIES:
        folder = os.path.join(results_dir, fam.folder)
        seen = {}
        for name in sorted(os.listdir(folder)):
            if name.startswith("."):
                continue
            m = RUN_NAME_RE.match(name)
            typ = next((t for t in fam.types if m and t.stem_re.fullmatch(m.group("stem"))), None)
            if typ is None or m.group("arm") not in fam.arms or int(m.group("seed")) not in fam.seeds:
                errors.append(f"{fam.folder}/{name}: not a run of this family's plan")
                continue
            seed = int(m.group("seed"))
            stem_seed = typ.stem_re.fullmatch(m.group("stem")).groups()
            if stem_seed and int(stem_seed[0]) != seed:
                errors.append(f"{fam.folder}/{name}: seed {seed} differs from its config's number {stem_seed[0]}")
                continue
            key = (typ.key, m.group("arm"), seed)
            if key in seen:
                errors.append(f"{fam.folder}/{name}: same type, arm and seed as {seen[key]}")
                continue
            seen[key] = name
            found[(fam.folder, typ.key, m.group("arm"))].append(Run(os.path.join(folder, name), int(m.group("smax")), seed))
    if errors:
        sys.exit("unexpected run folders:\n  " + "\n  ".join(errors))
    return {(fam.folder, t.key, arm): Group(found.get((fam.folder, t.key, arm), []))
            for fam in FAMILIES for t in fam.types for arm in fam.arms}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results-dir", default="results",
                        help="directory holding one subfolder per experiment family (default: results)")
    args = parser.parse_args()
    groups = load(args.results_dir)

    planned = sum(len(f.types) * len(f.arms) * len(f.seeds) for f in FAMILIES)
    have = sum(g.n for g in groups.values())
    out = []
    out.append("# Results, seed-averaged per phase and per sub-SLA")
    out.append("")
    out.append(f"Generated by `results_md.py` from the run folders under `{args.results_dir}/`; do not edit by hand, regenerate "
               "with `python3 results_md.py > RESULTS.md`. Hardware, C, sMax and the other calibrated values are in "
               "CALIBRATION.md (section *Hardware*); the day sequence is in EXPERIMENT.md (*The day sequence*), and the "
               "failure and random schedules are in their configs.")
    out.append("")
    out.append(f"{have:,} of the {planned:,} planned runs are in (see *Completeness*). Every (type, arm) of the plan has a "
               "subsection, also before its first seed lands. Its first line gives the seeds found against the seeds "
               "planned, the 5 s progress lines per run (a range when seeds differ, which points at a run cut short), and "
               "the lost and client-dropped requests per run (the latter are the MongoDB transport's pool checkout "
               "timeouts). Only averages over the seeds found are printed: counts are pooled across the seeds, so every "
               "share is a ratio of pooled counts and every per-second rate is a mean per run; the leader columns are "
               "means of the per-seed values.")
    out.append("")
    out.append("Each type opens with a comparison of its arms: profit is the realized profit per run in millions, first "
               "without WarmUp and CoolDown (the reported window) and then over the whole run, each against chameleon; "
               "served, rejected and violations are over the whole run, and the read and write throughput and latency "
               "columns (described with the per-phase tables) are over the run without WarmUp and CoolDown. The random "
               "family, whose seeds are distinct configs, "
               "adds tables of profit relative to chameleon per config, one per phase and one overall, each with its arithmetic and geometric mean, and repeats the comparison "
               "for every config on its own under *Per config*, summary only. Each arm subsection then has:")
    out.append("")
    out.append("- **Per phase, sub-SLAs pooled**: one row per phase from the driver's phase-labelled progress lines, closed "
               "by the whole run and the whole run without WarmUp and CoolDown.")
    out.append("- **By sub-SLA**: one table per phase, then the whole run and the whole run without WarmUp and CoolDown, "
               "each closed by an all-sub-SLAs row. These come from the client ledger, which is mapped onto phases by "
               "timestamp, so an all-sub-SLAs row can differ from the same phase's row above by the requests within "
               "about a second of a phase boundary.")
    out.append("")
    out.append("Columns of the per-phase tables:")
    out.append("")
    out.append("- **offered/s**: requests the injector was scheduled to send per second (sent plus skipped).")
    out.append("- **served / rejected / other**: share of offered requests that were answered, refused by a server, or neither "
               "(lost, dropped by the client, expired, or skipped by the injector).")
    out.append("- **violations**: violations over served requests; a violation is a reply that missed every rung of its "
               "SLA or a request whose deadline expired.")
    out.append("- **profit/s**: realized profit per second; **pred/real** is the scorer's predicted profit over realized.")
    out.append("- **leader u / lambda end / leader in-flight**: the leader's mean utilization, its price at the end of the "
               "window and its mean in-flight, from the occupancy stream; **at ceiling** is the time in the window the "
               "price spent at or above 1,000, where the scorer refuses every level and the leader admits nothing until "
               "the price comes back down. The fixed arms (pileus, strongest, weakest) never read the price, so their "
               "lambda is diagnostic only.")
    out.append("- **reads/s / writes/s**: served reads and writes per second, from the client ledger (R: and W: sub-SLAs).")
    out.append("- **read p50 / p99, write p50 / p99**: end-to-end latency of served reads and writes in ms, client-observed "
               "from the first send to the reply, so including any redirect resend. Taken from the ledger's latency "
               "histogram pooled across seeds, whose buckets are 15% wide: each value is the upper edge of the bucket "
               "holding the percentile, as the client reports it, so it is at most 15% above the true value. A value "
               f"past the last bucket prints as > {LAT_BASE_MS * LAT_RATIO ** LAT_BUCKETS:,.0f}.")
    out.append("")
    out.append("Columns of the sub-SLA tables (a sub-SLA is the SLA a request was issued under, R: read and W: write, "
               "A application, S SLA):")
    out.append("")
    out.append("- **attempts/s**: requests that reached an outcome per second: served, rejected, lost, deadline expired, "
               "dropped by the client, or failed in transport.")
    out.append("- **served / rejected / other**: share of attempts; **violations** as above.")
    out.append("- **upgraded / free**: share of served requests delivered above their SLA's floor, and the share of those "
               "upgrades that did not wait.")
    out.append("- **fallbacks/s / redirects/s**: replies whose consistency wait timed out and fell back, and not-leader "
               "replies the client redirected, per second.")
    out.append("- **profit/s / pred/real**: as above; **served share / profit share**: the sub-SLA's share of the window's "
               "served requests and realized profit.")
    out.append("")

    out.append("## Completeness")
    out.append("")
    out.append("Seeds found over seeds planned for each (type, arm); a dash is an arm the family does not run.")
    out.append("")
    out.append("| family | type | " + " | ".join(ARMS) + " |")
    out.append("|---|---|" + "---|" * len(ARMS))
    for fam in FAMILIES:
        for t in fam.types:
            cells = [f"{groups[(fam.folder, t.key, arm)].n}/{len(fam.seeds)}" if arm in fam.arms else "-" for arm in ARMS]
            out.append(f"| {fam.title} | {t.title or 'all configs'} | " + " | ".join(cells) + " |")
    out.append("")

    for fam in FAMILIES:
        out.append(f"## {fam.title}")
        out.append("")
        for t in fam.types:
            by_arm = {arm: groups[(fam.folder, t.key, arm)] for arm in fam.arms}
            arm_level = "###" if t.title is None else "####"
            if t.title is not None:
                smax = sorted({r.smax for g in by_arm.values() for r in g.runs})
                suffix = f" (sMax {', '.join(f'{s:,}' for s in smax)})" if smax else ""
                out.append(f"### {t.title}{suffix}")
                out.append("")
            out.extend(comparison_table(by_arm, fam.arms, fam.seeds))
            out.append("")
            if fam.per_seed:
                out.append("Relative profit by config: each arm's profit divided by chameleon's on the same config, for the "
                           "configs where every arm has a run, one table per phase and one for all phases without WarmUp and "
                           "CoolDown, each closed by the arithmetic and the geometric mean across configs and the count of "
                           "configs where the arm is above chameleon. Robustness to the SLA reads off the geometric mean: an arm "
                           "whose mean is below 1 loses more on the configs it loses than it gains on the ones it wins, and a "
                           "collapse on one config is not cancelled by a windfall on another.")
                out.append("")
                out.extend(relative_tables(by_arm, fam.arms, fam.seeds, arm_level))
                out.append(f"{arm_level} Per config")
                out.append("")
                out.append("The same comparison for each config on its own; a config's single run per arm is its only seed, "
                           "so the pooled arm subsections below are the across-config averages of these rows.")
                out.append("")
                out.extend(per_seed_tables(by_arm, fam.arms, fam.seeds, arm_level + "#"))
            for arm in fam.arms:
                g = by_arm[arm]
                out.append(f"{arm_level} {arm}")
                out.append("")
                out.append(seeds_line(g, fam.seeds))
                out.append("")
                if not g.n:
                    continue
                out.append("Per phase, sub-SLAs pooled:")
                out.append("")
                out.extend(phase_table(g))
                out.append("")
                for label, _ in g.windows:
                    out.append(f"By sub-SLA, {label}:" if label in (ALL, CORE) else f"By sub-SLA, phase {label}:")
                    out.append("")
                    out.extend(sla_table(g.ledger[label], g.seconds[label]))
                    out.append("")
    sys.stdout.write("\n".join(out).rstrip() + "\n")


if __name__ == "__main__":
    main()
