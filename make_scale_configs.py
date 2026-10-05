#!/usr/bin/env python3
"""Derive the server-count scaling configs from the two local configs.

config_scale_<N>.yaml and config_scale_mongodb_<N>.yaml are config_local.yaml and
config_local_mongodb.yaml with N servers instead of five, laid out in three zones. Server
i is in zone i mod 3, so the zones are as equal as N allows (exactly equal for every
default size), and there are three client sites, site k in zone k. One-way delay is
0.1 ms within a zone and 0.3 ms between zones, for server-to-server and client-to-server
paths alike. Only the replica count varies; the client population and its three-zone
geography are the same at every size. Everything else (C and the phase rates, sMax, the
hard cap, the SLAs, the mixes, the client settings) is the five-node local calibration,
carried over as is and not re-measured for N servers. The exception is the Java store's
replication budget, measured at every size (BUDGET below).

Round-robin zones rather than contiguous blocks because of mongod's voting set:
MongoClusterAdmin makes members 0 to 6 the voters, and in blocks they would all sit in
zone 0 from 15 servers up, so mongod's majority writes would never leave the leader's
zone while the Raft store's always do. Round-robin spreads the voters over the three
zones at every size, and both stores' majorities need a cross-zone acknowledgement.

The client is held near its five-node connection totals because the single client
machine is the bound on these nodes (CALIBRATION.md, "Hardware"), and two earlier
layouts at 27 servers showed what happens otherwise. With one site per server the client
installed one delay filter per (site, server) pair and opened connections per pair, and
the check runs measured the client machine instead of the stores. With the base's five
sites the mongod run still filled every driver pool in Flash until the client's pool
ceiling was held (below).

Six things depend on N and are rewritten:

- The majority write rung. On five nodes the SLAs write it as `concern: 3`. The Java
  store's majority is N/2 + 1. mongod allows at most seven voting members, and a
  majority write is a majority of the voters, so on mongod it is min(N, 7)/2 + 1: two
  on three nodes and four from nine nodes up. (A numeric w above that would be
  acknowledged by non-voting members too, which is not the commit point, and the
  server-side scorer only enumerates concerns up to the replica set's write majority.)
  `concern: 2` rungs stay at 2.
- workload.sessionsPerApplication must divide by the number of client sites, so it is
  the multiple of the site count nearest 60 (60 itself at three sites).
- The topology blocks themselves: cluster.numServers, cluster.serverHosts,
  geo.interServerLatencyMs and geo.clientSites.
- client.ingressConnectionsPerServer on the Java store. The client opens that many
  framed connections per (client site, server) pair, so the total grows with sites x N:
  16 per pair is 400 connections on five nodes and was 11,664 on 27 with one site per
  server. That 27-server check run spent 73% of the client machine's CPU spinning on its
  transmit-queue lock and dropped 15% of the packets in the delay queue, so the total
  is held at the five-node 400: 400 / (sites x N) per pair, clamped to 1..16. The
  mongod transport does not read this setting (its connections are the driver's pools),
  so the mongod configs keep the base's value.
- mongodb.maxPoolSize on mongod. The driver keeps one pool per (client site, server)
  pair, so the primary's connection ceiling is sites x maxPoolSize, 5,000 on five nodes,
  and the client's is sites x N x maxPoolSize, 25,000 on five nodes. With one site per
  server on 27 the pools grew toward the primary's 27,000 under load, the client ran out
  of file descriptors and the driver failed its checkouts; with five sites and the
  base's 1,000 they filled every pool in Flash, 136,594 connections, while the primary
  held 55 operations in flight, which is the calibration's finding that a larger cap
  lets in-flight, connections and threads grow together at the knee (CALIBRATION.md,
  the pool-cap probe). Both ceilings are held at their five-node values: maxPoolSize is
  the smaller of 5,000 / sites and 25,000 / (sites x N). Not clamped at the base's
  1,000, because a pool cap is a concurrency limit that can bind: three sites at 1,000
  would give the primary less client capacity than the five-node calibration had.

The generated files say so in their first line; edit this script or the base config,
then regenerate:

  python3 make_scale_configs.py            # 3 9 15 21 27
  python3 make_scale_configs.py 5 7        # any sizes

MongoClusterAdmin initiates the first seven members as voters and the rest with votes 0
and priority 0, and both stores send a majority write as w:"majority", so a non-voting
acknowledgement never stands in for the commit point.
"""

import re
import sys

BASES = [
    ("config_local.yaml", "config_scale_{n}.yaml", "java"),
    ("config_local_mongodb.yaml", "config_scale_mongodb_{n}.yaml", "mongod"),
]
MAX_VOTING_MEMBERS = 7          # MongoDB's limit; members beyond it must have votes: 0
ZONES = 3                       # server i is in zone i mod ZONES; client site k is zone k
INTRA_ZONE_MS = 0.1             # one-way, within a zone (server-server and client-server)
INTER_ZONE_MS = 0.3             # one-way, between zones
BASE_SESSIONS = 60              # config_local's workload.sessionsPerApplication
BASE_CONNECTIONS = 16           # config_local's client.ingressConnectionsPerServer
BASE_CONNECTION_TOTAL = 5 * 5 * BASE_CONNECTIONS  # five sites x five servers on the base
BASE_POOL = 1000                # config_local_mongodb's mongodb.maxPoolSize
BASE_POOL_TOTAL = 5 * BASE_POOL  # the primary's connection ceiling on the base: five sites x the cap
BASE_CLIENT_POOL_TOTAL = 5 * 5 * BASE_POOL  # the client's ceiling on the base: five sites x five servers x the cap
CAP_RATIO = 3                   # server.ingressHardCapInFlight = 3 x sMax, as on every topology

# sMax per store and size, calibrated at that size (CALIBRATION.md, "Scaling the server
# count"). None keeps the five-node local value, and the file's header says so. A size
# missing from its store's table is an error, not a default.
SMAX = {
    "java": {3: None, 9: None, 15: None, 21: None, 27: None},
    "mongod": {3: None, 9: None, 15: None, 21: None, 27: None},
}

# server.replicationBudgetPerSecond per store and size. Every admitted write is replicated
# to all N - 1 followers whatever its concern, so a budget measured against the five-node
# base's four followers does not carry over; at 27 servers it put the leader past its
# replication stream. The Java values are measured at each size by the method that set the
# five-node one (CALIBRATION.md, "Scaling the server count"): 0.78 x the leader's peak
# stream per follower in a write-only probe (strongest arm), rounded down to a multiple of
# 5,000, then checked on the compressed day with the weakest arm. mongod keeps its
# five-node value at every size: a majority write waits on at most seven voting members,
# and the 27-server check runs matched five servers. None keeps the five-node value and
# the header says so.
BUDGET = {
    "java": {3: 110000, 9: 90000, 15: 85000, 21: 70000, 27: 60000},
    "mongod": {3: None, 9: None, 15: None, 21: None, 27: None},
}


def replace_once(text, pattern, replacement, what):
    matches = list(re.finditer(pattern, text, flags=re.MULTILINE))
    if len(matches) != 1:
        sys.exit(f"{what}: expected exactly one match, found {len(matches)}")
    m = matches[0]
    return text[:m.start()] + replacement + text[m.end():]


def fmt(ms):
    return "0" if ms == 0 else f"{ms:g}"


def majority(n, store):
    voters = n if store == "java" else min(n, MAX_VOTING_MEMBERS)
    return voters // 2 + 1


def zone_of(server):
    return server % ZONES


def sites_for(n):
    return min(ZONES, n)


def zone_sizes(n):
    return [sum(1 for i in range(n) if zone_of(i) == z) for z in range(sites_for(n))]


def delay(zone_a, zone_b):
    return INTRA_ZONE_MS if zone_a == zone_b else INTER_ZONE_MS


def sessions_for(sites):
    candidates = [k * sites for k in range(1, BASE_SESSIONS + 1)]
    return min(candidates, key=lambda s: (abs(s - BASE_SESSIONS), -s))


def connections_for(n, sites):
    return max(1, min(BASE_CONNECTIONS, round(BASE_CONNECTION_TOTAL / (sites * n))))


def pool_for(n, sites):
    return round(min(BASE_POOL_TOTAL / sites, BASE_CLIENT_POOL_TOTAL / (sites * n)))


def budget_for(n, store, base_budget):
    if n not in BUDGET[store]:
        sys.exit(f"no budget entry for {store} at {n} servers in BUDGET; measure it or enter None for the base's value")
    return BUDGET[store][n] or base_budget


def smax_for(n, store, base_smax):
    if n not in SMAX[store]:
        sys.exit(f"no sMax entry for {store} at {n} servers in SMAX; calibrate it or enter None for the base's value")
    return SMAX[store][n] or base_smax


def derive(base_text, n, store, base_name, out_name):
    text = base_text
    base_smax = int(re.search(r"^  sMax: (\d+)$", text, re.M).group(1))
    base_cap = int(re.search(r"^  ingressHardCapInFlight: (\d+)$", text, re.M).group(1))
    if base_cap != CAP_RATIO * base_smax:
        sys.exit(f"{base_name}: cap {base_cap} is not {CAP_RATIO} x sMax {base_smax}; the scale rule assumes it is")
    smax = smax_for(n, store, base_smax)
    base_budget = int(re.search(r"^  replicationBudgetPerSecond: (\d+)$", text, re.M).group(1))
    budget = budget_for(n, store, base_budget)
    text = replace_once(text, rf"^  replicationBudgetPerSecond: {base_budget}\n",
                        f"  replicationBudgetPerSecond: {budget}\n", "server.replicationBudgetPerSecond")
    text = replace_once(text, rf"^  sMax: {base_smax}\n", f"  sMax: {smax}\n", "server.sMax")
    text = replace_once(text, rf"^  ingressHardCapInFlight: {base_cap}\n", f"  ingressHardCapInFlight: {CAP_RATIO * smax}\n",
                        "server.ingressHardCapInFlight")
    text = replace_once(text, r"^  numServers: 5\n", f"  numServers: {n}\n", "cluster.numServers")
    text = replace_once(text, r"^  serverHosts:\n(?:    - localhost\n){5}",
                        "  serverHosts:\n" + "    - localhost\n" * n, "cluster.serverHosts")
    rows = "".join("    - [" + ", ".join(fmt(0 if i == j else delay(zone_of(i), zone_of(j))) for j in range(n)) + "]\n"
                   for i in range(n))
    text = replace_once(text, r"^  interServerLatencyMs:\n(?:    - \[.*\]\n){5}",
                        "  interServerLatencyMs:\n" + rows, "geo.interServerLatencyMs")
    sites_n = sites_for(n)
    sites = "".join("    - { name: az" + str(s) + ", latencyMs: ["
                    + ", ".join(fmt(delay(s, zone_of(j))) for j in range(n)) + "] }\n"
                    for s in range(sites_n))
    text = replace_once(text, r"^  clientSites:\n(?:    - \{ name: az\d+, latencyMs: \[.*\] \}\n){5}",
                        "  clientSites:\n" + sites, "geo.clientSites")
    sessions = sessions_for(sites_n)
    text = replace_once(text, r"^  sessionsPerApplication: 60\n", f"  sessionsPerApplication: {sessions}\n",
                        "workload.sessionsPerApplication")
    if store == "java":
        conns = connections_for(n, sites_n)
        text = replace_once(text, rf"^  ingressConnectionsPerServer: {BASE_CONNECTIONS}\n",
                            f"  ingressConnectionsPerServer: {conns}\n", "client.ingressConnectionsPerServer")
    else:
        pool = pool_for(n, sites_n)
        text = replace_once(text, rf"^  maxPoolSize: {BASE_POOL}\n", f"  maxPoolSize: {pool}\n", "mongodb.maxPoolSize")
    m = majority(n, store)
    concern_lines = len(re.findall(r"^\s*- concern: 3\n", text, flags=re.MULTILINE))
    if concern_lines == 0:
        sys.exit(f"{base_name}: no 'concern: 3' write rungs to rewrite")
    text = re.sub(r"^(\s*- concern: )3\n", rf"\g<1>{m}\n", text, flags=re.MULTILINE)

    voters = n if store == "java" else min(n, MAX_VOTING_MEMBERS)
    sizes = zone_sizes(n)
    header = [
        f"# GENERATED by make_scale_configs.py from {base_name}; do not edit by hand, edit the base or the",
        f"# generator and regenerate. {n} servers in {sites_n} zones of {', '.join(map(str, sizes))} (server i in zone i mod {ZONES}),"
        f" {sites_n} client sites (site k in zone k);",
        f"# one-way delay {INTRA_ZONE_MS:g} ms within a zone and {INTER_ZONE_MS:g} ms between zones, servers and clients alike.",
        f"# sessionsPerApplication {sessions} ({sessions // sites_n} per site), and the majority write rung at concern {m}",
        f"# ({voters} voting members; it is 3 in the five-node base). C and the phase rates are the five-node local",
        "# calibration and have not been re-measured at this size.",
        (f"# replicationBudgetPerSecond {budget:,}, measured at this size (CALIBRATION.md, \"Scaling the server count\")."
         if BUDGET[store][n] else
         f"# replicationBudgetPerSecond {budget:,}: the five-node local value, not re-measured at this size."),
        (f"# sMax {smax} and cap {CAP_RATIO * smax} ({CAP_RATIO} x sMax), calibrated at this size (CALIBRATION.md)."
         if SMAX[store][n] else
         f"# sMax {smax} and cap {CAP_RATIO * smax}: the five-node local values, not re-calibrated at this size."),
    ]
    if store == "java":
        header.append(f"# ingressConnectionsPerServer {conns} per (site, server) pair, {conns * sites_n * n} connections in all,"
                      f" against the base's {BASE_CONNECTION_TOTAL}.")
    else:
        header.append(f"# maxPoolSize {pool} per (site, server) pair: connection ceilings of {pool * sites_n:,} on the primary"
                      f" and {pool * sites_n * n:,} on the client, against the base's {BASE_POOL_TOTAL:,} and {BASE_CLIENT_POOL_TOTAL:,}.")
    if store == "mongod" and n > MAX_VOTING_MEMBERS:
        header.append(f"# MongoDB allows at most {MAX_VOTING_MEMBERS} voting members: MongoClusterAdmin initiates members"
                      f" {MAX_VOTING_MEMBERS} to {n - 1} with votes 0 and priority 0, and a majority write is")
        header.append(f"# w:\"majority\", {majority(n, store)} of the {MAX_VOTING_MEMBERS} voters (the Raft store's is {n // 2 + 1} of {n}).")
    return "\n".join(header) + "\n\n" + text


def main():
    sizes = [int(a) for a in sys.argv[1:]] or [3, 9, 15, 21, 27]
    for n in sizes:
        if n < 1:
            sys.exit(f"size {n}: a cluster needs at least one server")
    for base_name, out_pattern, store in BASES:
        with open(base_name) as f:
            base_text = f.read()
        for n in sizes:
            out_name = out_pattern.format(n=n)
            with open(out_name, "w") as f:
                f.write(derive(base_text, n, store, base_name, out_name))
            print(f"wrote {out_name}: {n} servers in zones of {zone_sizes(n)}, {sites_for(n)} client sites,"
                  f" majority write concern {majority(n, store)}, {sessions_for(sites_for(n))} sessions per application")


if __name__ == "__main__":
    main()
