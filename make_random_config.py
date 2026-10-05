#!/usr/bin/env python3
"""Generate a random-SLA experiment config from the local Java-store config.

config_random_<seed>.yaml is config_local.yaml (five nodes, local delay, C, sMax, the cap
and the budgets as calibrated) with three things replaced:

- the SLA table: one application with one read SLA and one write SLA, each of the
  requested number of rungs (1 to 4) drawn at random from the ladders below;
- the mixes: one per requested write percentage, reads being the rest;
- the phases: a 30 s warm-up at the first mix, one phase per write percentage at 1 C
  (read off the base's phase marked "# 1C"), then a 30 s cool-down at the last mix.

A rung is (level or concern, latency target, profit), each drawn uniformly from its
ladder. An SLA's rungs are drawn together, and the whole set is drawn again until it
makes sense: whenever one rung is at least as demanding as another on both dimensions
(consistency not weaker, latency target not longer) and strictly more demanding on at
least one, it must pay strictly more. Two rungs
with the same demands are a duplicate, and a stricter rung that pays the same or less is
dead, because RungScorer takes the suffix maximum of profit over the rungs a level
satisfies, so the weaker rung would always claim the pay. Incomparable rungs (stronger but
slower) may pay anything. The config's own seed is set to --seed too, so a run's request
stream follows the same number.

  python3 make_random_config.py --seed 42 [--read 3] [--write 3] [--write-rate "20;50;80"] [--duration 300]

Validate the result the way every config is validated:

  java -cp <jar> org.example.Utility.ExperimentConfig config_random_42.yaml
"""

import argparse
import random
import re
import sys
from collections import namedtuple

BASE = "config_local.yaml"
NUM_SERVERS = 5                  # the base's cluster; write concerns range over 1..majority
MAJORITY = NUM_SERVERS // 2 + 1
READ_LEVELS = ["EVENTUAL_LOCAL", "EVENTUAL_MAJORITY", "CAUSAL_LOCAL", "CAUSAL_MAJORITY", "LINEARIZABLE"]
LATENCY_MS = [20, 40, 60, 100, 150, 200, 300, 400, 600, 800, 1000, 1500, 2000]
WRITE_MIN_LATENCY_MS = 40        # the hand-written local SLAs give no write less than this
PROFIT = [0.5, 1, 1.5, 2, 3, 4, 5, 6, 8, 10, 12, 15, 20, 25, 30, 40, 50, 60, 80, 100, 120, 150]
MAX_RUNGS = 4                    # the client ledger's satisfied-rung columns are fixed
WARMUP_S = 30
COOLDOWN_S = 30
MAX_DRAWS = 10_000

# strength is the read level's index in READ_LEVELS or the write concern.
Rung = namedtuple("Rung", "strength latency profit")


def replace_once(text, pattern, replacement, what):
    matches = list(re.finditer(pattern, text, flags=re.MULTILINE))
    if len(matches) != 1:
        sys.exit(f"{what}: expected exactly one match in {BASE}, found {len(matches)}")
    m = matches[0]
    return text[:m.start()] + replacement + text[m.end():]


def at_least_as_demanding(a, b):
    return a.strength >= b.strength and a.latency <= b.latency


def may_share_an_sla(a, b):
    """The rule: the rung that is at least as demanding on both dimensions pays strictly more."""
    a_over_b = at_least_as_demanding(a, b)
    b_over_a = at_least_as_demanding(b, a)
    if a_over_b and b_over_a:
        return False  # the same demands twice
    if a_over_b:
        return a.profit > b.profit
    if b_over_a:
        return b.profit > a.profit
    return True  # incomparable: one is stronger, the other faster


def draw_sla(rng, count, strengths, latencies, what):
    """Draw the rungs together and keep the set only if every pair obeys the rule, else draw
    the whole set again, so every consistent SLA is equally likely. Redrawing one rung at a
    time cannot work: a first draw of the most demanding rung at the lowest profit leaves no
    second rung that could ever be consistent with it."""
    for _ in range(MAX_DRAWS):
        rungs = [Rung(rng.choice(strengths), rng.choice(latencies), rng.choice(PROFIT)) for _ in range(count)]
        if all(may_share_an_sla(a, b) for i, a in enumerate(rungs) for b in rungs[i + 1:]):
            rungs.sort(key=lambda r: (-r.strength, r.latency))  # strongest first, like the hand-written SLAs
            return rungs
    sys.exit(f"{what}: no consistent set of {count} rungs in {MAX_DRAWS} draws")


def rungs_yaml(rungs, is_read):
    out = ""
    for r in rungs:
        key = f"level: {READ_LEVELS[r.strength]}" if is_read else f"concern: {r.strength}"
        out += f"          - {key}\n            latencyMs: {r.latency}\n            profit: {r.profit:g}\n"
    return out


def slas_block(seed, read_rungs, write_rungs):
    return ("slas:\n"
            f"  # Random SLAs (make_random_config.py, seed {seed}): one application with one read SLA of\n"
            f"  # {len(read_rungs)} rungs and one write SLA of {len(write_rungs)} rungs. The rungs are drawn uniformly from the\n"
            "  # generator's ladders, together, and the set is redrawn until in every pair the rung\n"
            "  # at least as demanding on both dimensions (consistency, latency target) and stricter\n"
            "  # on one pays strictly more; equal demands are a duplicate. Written strongest first.\n"
            "  - applicationId: 1\n"
            "    read:\n      - slaId: 1\n        rungs:\n" + rungs_yaml(read_rungs, True)
            + "    write:\n      - slaId: 1\n        rungs:\n" + rungs_yaml(write_rungs, False))


def mixes_and_phases(write_rates, duration, tps):
    out = ("  mixes:\n"
           "    # One application with one SLA per side, so a mix is its write percentage and\n"
           "    # reads are the rest. The phases below walk through them in the order given.\n")
    for p in dict.fromkeys(write_rates):
        out += (f"    w{p}:\n"
                f"      - {{ applicationId: 1, type: read,  slaId: 1, weight: {100 - p} }}\n"
                f"      - {{ applicationId: 1, type: write, slaId: 1, weight: {p} }}\n")

    def phase(name, comment, seconds, mix):
        return (f"    - name: {name:16s}# {comment}\n      durationSeconds: {seconds}\n"
                f"      totalTPS: {tps}     # 1C\n      mix: {mix}\n")

    out += ("  # Every phase runs at 1C, the base's calibrated capacity; only the write share moves.\n"
            "  phases:\n")
    out += phase("WarmUp", "warm up the cluster at the first mix", WARMUP_S, f"w{write_rates[0]}")
    seen = {}
    for p in write_rates:
        seen[p] = seen.get(p, 0) + 1
        name = f"Writes{p}" if seen[p] == 1 else f"Writes{p}_{seen[p]}"
        out += phase(name, f"{p}% writes, {100 - p}% reads", duration, f"w{p}")
    out += phase("CoolDown", "cool down at the last mix", COOLDOWN_S, f"w{write_rates[-1]}")
    return out


def derive(base_text, args, read_rungs, write_rungs):
    m = re.search(r"^\s+totalTPS: (\d+)\s*# 1C\b", base_text, flags=re.MULTILINE)
    if not m:
        sys.exit(f"no phase marked '# 1C' in {BASE}, so C cannot be read off it")
    tps = int(m.group(1))

    text = replace_once(base_text, r"^seed: \d+\n", f"seed: {args.seed}\n", "seed")
    text = replace_once(text, r"^slas:\n(?:(?:  .*|)\n)+?(?=^# Failure injection)",
                        slas_block(args.seed, read_rungs, write_rungs) + "\n", "slas")
    text = replace_once(text, r"^  mixes:\n(?:(?:  .*|)\n)+?(?=^# Which store)",
                        mixes_and_phases(args.write_rate, args.duration, tps), "workload.mixes and workload.phases")

    rates = ";".join(str(p) for p in args.write_rate)
    percent_list = ", ".join(f"{p}%" for p in args.write_rate)
    header = [
        f"# GENERATED by make_random_config.py from {BASE}; do not edit by hand, rerun the generator:",
        f"#   python3 make_random_config.py --seed {args.seed} --read {args.read} --write {args.write}"
        f" --write-rate \"{rates}\" --duration {args.duration}",
        f"# Random SLAs: one application with a {args.read}-rung read SLA and a {args.write}-rung write SLA drawn with",
        f"# seed {args.seed} under the rule that a rung at least as demanding as another on both dimensions pays",
        f"# strictly more. The workload is a {WARMUP_S} s warm-up, then {args.duration} s each at {percent_list} writes,",
        f"# all at 1C ({tps:,}/s), then a {COOLDOWN_S} s cool-down. Topology, C, sMax, the cap and the budgets are",
        "# the five-node local base's. Java store.",
    ]
    return "\n".join(header) + "\n\n" + text


def rung_count(text):
    v = int(text)
    if not 1 <= v <= MAX_RUNGS:
        raise argparse.ArgumentTypeError(f"must be 1 to {MAX_RUNGS} (the client ledger's rung columns), got {v}")
    return v


def positive_int(text):
    v = int(text)
    if v <= 0:
        raise argparse.ArgumentTypeError(f"must be positive, got {v}")
    return v


def write_rates(text):
    rates = []
    for part in text.split(";"):
        part = part.strip()
        if not re.fullmatch(r"\d+", part):
            raise argparse.ArgumentTypeError(f"write percentages must be integers separated by ';', got '{text}'")
        v = int(part)
        if not 1 <= v <= 99:
            raise argparse.ArgumentTypeError(
                f"a write percentage must be 1 to 99 (0 or 100 leaves one side of the mix at weight 0), got {v}")
        rates.append(v)
    return rates


def parse_args():
    parser = argparse.ArgumentParser(description="Generate config_random_<seed>.yaml with random SLAs.")
    parser.add_argument("--seed", type=int, required=True, help="seed for the rung draws and the config's own seed")
    parser.add_argument("--read", type=rung_count, default=3, help="rungs in the read SLA (default 3)")
    parser.add_argument("--write", type=rung_count, default=3, help="rungs in the write SLA (default 3)")
    parser.add_argument("--write-rate", type=write_rates, default=[20, 50, 80],
                        help="write percentages, one phase each, in order (default \"20;50;80\")")
    parser.add_argument("--duration", type=positive_int, default=300, help="seconds per write-rate phase (default 300)")
    return parser.parse_args()


def main():
    args = parse_args()
    rng = random.Random(args.seed)
    read_rungs = draw_sla(rng, args.read, list(range(len(READ_LEVELS))), LATENCY_MS, "read SLA")
    write_rungs = draw_sla(rng, args.write, list(range(1, MAJORITY + 1)),
                           [ms for ms in LATENCY_MS if ms >= WRITE_MIN_LATENCY_MS], "write SLA")
    with open(BASE) as f:
        base_text = f.read()
    out_name = f"config_random_{args.seed}.yaml"
    with open(out_name, "w") as f:
        f.write(derive(base_text, args, read_rungs, write_rungs))
    print(f"wrote {out_name}")
    print("  read SLA:  " + " | ".join(f"{READ_LEVELS[r.strength]} {r.latency} ms {r.profit:g}" for r in read_rungs))
    print("  write SLA: " + " | ".join(f"wc{r.strength} {r.latency} ms {r.profit:g}" for r in write_rungs))
    print(f"  phases: WarmUp {WARMUP_S} s, " + ", ".join(f"Writes{p} {args.duration} s" for p in args.write_rate)
          + f", CoolDown {COOLDOWN_S} s, all at 1C")


if __name__ == "__main__":
    main()
