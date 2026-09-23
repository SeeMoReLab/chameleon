# Calibration: C and sMax

Final values and the measurements that establish them. Exploration that led here is
deliberately not included - only the runs a reader would need to check the numbers.

Both stores were calibrated after the harness fix described under *Harness corrections*
below. Measurements taken before it do not transfer, because the fix changed throughput
and latency on every topology.

## Final values

### Java store (`config_{local,regional,global}.yaml`)

| | C | sMax | ingressHardCapInFlight | lambdaMax | replicationLingerMs | injectorThreads |
|---|---|---|---|---|---|---|
| local | 1,000,000 | 3,200 | 9,600 (3x sMax) | 1.0e6 | 5.0 | 30 |
| regional | 1,000,000 | 7,200 | 21,600 (3x sMax) | 1.0e6 | 5.0 | 30 |
| global | 1,000,000 | 10,000 | 30,000 (3x sMax) | 1.0e6 | 5.0 | 30 |

### mongod (`config_{local,regional,global}_mongodb.yaml`)

| | C | sMax |
|---|---|---|
| local | 120,000 | 120 |
| regional | 120,000 | 1,200 |
| global | 120,000 | 2,000 |

`ingressHardCapInFlight` and `replicationLingerMs` are **not** pushed to mongod and have
no effect there: mongod uses its own replication, and its concurrency is bounded by the
driver connection pool and WiredTiger tickets. `MongoClusterControl.applyServerParameters`
pushes only `chameleonEnabled`, `chameleonServerDecision`, `chameleonSMax`,
`chameleonReplicationBudgetPerSecond`, `chameleonMaxWaitMs`, `chameleonControlIntervalMs`,
`chameleonUTarget`, `chameleonEta`, `chameleonLambdaMin`, `chameleonHistogramDecay` and
the SLA table.

## Method

**C is a workload parameter and is common across topologies; sMax is per-topology.**
C appears in the configs only as the phase schedule's multiplier base; sMax is the only
system setting that differs between the three topology configs outside the geo matrices.

**sMax is an operating point, not a capacity to discover.** The earlier derivation - the
primary's unregulated in-flight at C, measured with sMax pinned out of reach - no longer
measures anything. With the price silenced, in-flight has no equilibrium: it grows until
something external stops it, which on the Java store is `ingressHardCapInFlight` and on
mongod is the driver pool. Both give the cap's value back, not the machine's.

**Realized profit is the criterion.** Two alternatives were tried and rejected on
evidence:

- *Goodput* (served minus SLA violations) rises monotonically with sMax on the Java
  store's local and global topologies, so it always selects the largest value on the
  grid. The SLA deadlines (500-2,000 ms for writes) are loose against these latencies
  (11-86 ms), so queueing never registers as a violation and goodput sees only the cost
  of rejecting, never the cost of queueing.
- *Utilization* is tautological. u read **0.85 at 14 of 15 points** of the Java store
  sweep, because the controller drives u to uTarget for any reachable sMax. u is useful
  only as a ceiling test: it falls below target when sMax exceeds what the machine can
  fill, which is how global's overlarge values were caught.

Profit prices both sides of the trade-off. On the Java store's local topology, profit per
request falls 10.75 -> 10.27 as sMax rises from 400 to 12,800, while throughput rises
877,548 -> 991,501/s: queueing pushes responses down the SLA rungs, so total profit peaks
in the middle.

**A price that does not regulate disqualifies a value**, whatever its profit. If u never
reaches uTarget, lambda decays to `lambdaMin` and stays there for the whole run including
overload, which is the priceless ablation rather than the chameleon arm.

## Java store: sMax

Offered 1,000,000/s, linger 5 ms, chameleon arm, 240 s dwell. Two passes: the first
capped the ingress at 3x sMax, the second at 60,000 absolute. Repeats averaged.

| | sMax | n | profit/s | spread | goodput/s | lat ms | lambda | note |
|---|---|---|---|---|---|---|---|---|
| local | 400 | 2 | 9,684,536 | 5.3% | 909,020 | 11.8 | 2.9e0 | cap bound 4.2% |
| | 800 | 2 | 9,393,420 | 18.5% | 932,414 | 14.4 | 6.9e0 | cap bound 5.0% |
| | 1,600 | 2 | 10,269,586 | 2.3% | 973,468 | 18.0 | 5.3e-1 | cap bound 1.3% |
| | **3,200** | 2 | **10,631,720** | 1.7% | 987,044 | 19.6 | 3.0e-1 | clean |
| | 6,400 | 1 | 10,534,555 | - | 986,160 | 27.6 | 1.4e-1 | clean |
| | 12,800 | 1 | 10,186,693 | - | 985,823 | 38.8 | 5.6e-2 | clean |
| regional | 900 | 1 | 9,190,089 | - | 949,528 | 28.4 | 7.4e-1 | |
| | 1,800 | 2 | 9,675,932 | 0.6% | 977,455 | 34.0 | 5.7e-1 | |
| | 3,600 | 3 | 9,391,975 | - | 948,938 | 37.4 | 3.1e-1 | |
| | **7,200** | 2 | 9,462,036 | 5.0% | 971,532 | 41.0 | 1.6e-1 | |
| | 14,400 | 1 | 8,920,462 | - | 988,124 | 38.8 | 4.4e-2 | |
| global | 1,200 | 1 | 6,462,811 | - | 874,487 | 41.5 | 2.2e-1 | |
| | 2,500 | 1 | 6,598,016 | - | 878,125 | 45.1 | 1.4e-1 | |
| | 5,000 | 2 | 6,808,147 | 1.7% | 896,018 | 50.5 | 1.1e-1 | |
| | **10,000** | 2 | 6,776,762 | 3.6% | 906,328 | 58.5 | 3.7e-2 | |
| | 20,000 | 1 | 6,649,933 | - | 935,512 | 85.6 | **1.4e-4** | price inert, u 0.78 |

**Why these three and not the argmaxes.** The profit curves are flat near their optima -
about 3% across a 2-4x range of sMax - while run-to-run variance reaches 5-19%, so the
argmax is not resolvable. The chosen set is therefore monotonic in inter-node delay,
which the physics supports and which costs at most 2.2% profit (regional 7,200 against
1,800's 9.68M). A majority write holds its slot for the round trip to the second-nearest
follower - 0.6 ms on local, 20 ms on regional, 80 ms on global - so slot demand rises
with delay. Measured write latency at the chosen values is 28 / 107 / 191 ms.

Too small a budget does not merely reject writes, it drives the scorer off strong
consistency: at regional sMax 1,800, 56% of writes executed at the weakest level against
100% at majority at 7,200.

Global is 10,000 rather than 20,000 because 20,000 drives u to 0.78 with lambda at its
floor - the machine cannot fill the budget, so the price never rises.

**Re-measured under the shipping configuration** (cap 3x sMax, lambdaMax 1e6, on a fresh
allocation), same schedule, one run per point:

| | sMax | profit/s | goodput/s | lat ms | u | lambda | cap rejects | scorer rejects |
|---|---|---|---|---|---|---|---|---|
| local | 1,600 | 10,307,068 | 977,602 | 17.6 | 0.85 | 5.2e-1 | 0.84% | 0.70% |
| | **3,200** | 10,079,414 | 987,161 | 22.3 | 0.85 | 2.6e-1 | 0.21% | 0.84% |
| | 6,400 | 10,015,064 | 988,552 | 30.4 | 0.85 | 1.3e-1 | 0.05% | 0.44% |
| global | 5,000 | 6,519,325 | 875,313 | 55.5 | 0.85 | 1.1e-1 | 0.63% | 9.13% |
| | **10,000** | 6,489,265 | 873,815 | 67.1 | 0.85 | 4.4e-2 | 0.14% | 9.76% |
| | 20,000 | 6,767,169 | 935,601 | 85.1 | **0.78** | **3.9e-4** | 0.00% | 3.98% |

Local's curve spans 2.9% across the three points, inside the run-to-run spread of the
original sweep, so 3,200 stands. Global 20,000 again earns the most (4% over 10,000) and
again does it with u stuck at 0.78 and lambda within 4x of its floor, the same inert-price
condition as before, so 10,000 stands. At 1C on global the price, not the cap, does the
admission: the scorer rejects 9.8% against the cap's 0.1%. Regional was not repeated.

## Java store: C = 1,000,000

Price-silenced probes tracked 95.8% / 98.7% / 96.8% of 1,000,000 on local / regional /
global, and 93.5% / 96.4% / 97.0% at 1,200,000, so capacity is 1.1-1.2M on all three and
**barely varies with delay** - the framed transport multiplexes over 16 shared connections
per (site, server), so latency costs it no capacity.

At 1,000,000 with the calibrated sMax values all three track 93-98% with 100% of served
writes meeting their deadline, and the rung repeats within 0.3-0.9 points against 11.6
points at 1,200,000.

**2.5C = 2,500,000 is the binding constraint on C, and it is close to the edge.** A single
client machine at `injectorThreads: 30` emits 2,499,371/s (local), 2,491,616/s (regional)
and 2,497,747/s (global) against a 2.5M target, skipping 0.08% at worst; at 3,000,000 all
three fall short by 3-10%. At the previous `injectorThreads: 15` the client topped out at
~1.9M and produced only 75.5% of a 2.5M target, so the Flash phase would silently have run
at 1.9C. 45 threads is worse than 30 (2.74M against 2.92M at a 3.0M target).

## mongod: C = 120,000

Price-silenced probes, 90 s rungs:

| offered | local | regional | global |
|---|---|---|---|
| 90,000 | - | - | 97.1% |
| 105,000 | - | - | 97.0% |
| **120,000** | - | - | **96.6%** (1.0% pool drops) |
| 135,000 | - | - | 95.3% (2.4% pool drops) |
| 150,000 | 99.8% | 99.6% | 93.1% |
| 175,000 | 99.0% | - | - |
| 200,000 | 98.8% | - | - |
| 225,000 | 97.6% | - | - |
| 250,000 | 99.1% | 97.3% | - |
| 280,000 | - | 94.0% | - |
| 310,000 | - | 89.2% | - |
| 340,000 | - | 81.2% | - |

Knees at 95% tracking: global **135,000**, regional **250,000**, local **>=250,000**.
Global binds, and 120,000 leaves margin where 135,000 does not.

A repeat of the global fine probe on a fresh allocation gave 96.1% at 120,000 (0.9% pool
drops) and 95.3% at 135,000 (2.3% pool drops), against 96.6% / 1.0% and 95.3% / 2.4% the
first time, so the global knee reproduces within half a point.

**Why global binds, and why the Java store's C does not vary this way.** Every mongod
operation holds one pooled connection for its whole latency, against a fixed
`maxPoolSize: 1000` per (client site, server), so capacity falls as latency rises. At the
same offered rate of 150,000 the three differ almost entirely in write latency:

| | write lat | read lat | writes/s/site | primary-pool occupancy | of the 1,000 cap | pool drops |
|---|---|---|---|---|---|---|
| local | 13.3 ms | 8.2 | 3,898 | 52 | 5% | 0.0% |
| regional | 77.3 ms | 35.6 | 3,248 | 251 | 25% | 0.0% |
| global | 304.7 ms | 64.9 | 3,173 | 967 | **97%** | 4.3% |

The write rates are identical (same mix), so the 19x spread in pool occupancy is pure
write latency, set by the distance to the second-nearest follower: 0.3 ms, 10 ms, 40 ms
one way. Pool drops begin once occupancy passes roughly 60% of the cap and grow steeply
past 100%.

Local and regional stop for a different reason, which is why their knees nearly coincide
rather than scaling with their very different write latencies: both tip into positive
feedback around 250-280k, where checkout queueing inflates write latency, which raises
occupancy further. Local's write latency rises 13.3 -> 159.4 ms between 150k and 250k
while regional's rises only 77.3 -> 114.2 ms, so despite starting 5.8x lower local arrives
at the same place.

**Two limits worth knowing when reading any mongod result:**

1. Above roughly **270,000/s** all three plateau regardless of topology (peaks 275,822 /
   273,631 / 279,741) and the loss becomes driver pool refusal - 59-60% `ClientDropped` at
   700,000 offered, while mongod's own in-flight *falls* as offered rises (local 542 ->
   136). That is starvation upstream of mongod, so no larger C is measurable on one client
   machine, and absolute throughput comparisons between the two stores are partly a
   comparison of the client against itself.
2. At **2.5C = 300,000** every topology is past its own capacity, which is the intent, but
   on global most of the refusal is pool checkout failure rather than the price declining
   work. `ClientDropped` must be reported as its own channel and never folded into price
   rejection. Pool drops are not value-aware: at global's top regulated rung a class that
   was 32.0% of offered load took 51.8% of the drops while one that was 14.0% took 1.0%,
   the skew following which (site, server) pools saturate.

## mongod: sMax

Offered 120,000/s for 180 s, then 300,000/s (2.5C) for 90 s in the same run.

| | sMax | profit/s @120k | % best | profit/s @300k | % best | u @120k | lambda @300k | price share of Flash refusals |
|---|---|---|---|---|---|---|---|---|
| local | 60 | 1,186,509 | 93.1% | 1,802,541 | 85.8% | 0.62 | 7.6e0 | 66.9% |
| | **120** | 1,271,415 | 99.8% | **2,100,250** | **100%** | 0.38 | 3.0e0 | 63.2% |
| | 250 | 1,271,335 | 99.8% | 1,581,213 | 75.3% | 0.19 | 2.6e-1 | 49.2% |
| | 500 | **1,274,113** | **100%** | 1,835,404 | 87.4% | 0.09 | 1.0e-4 | 38.5% |
| regional | 300 | 1,112,825 | 90.7% | 1,578,016 | 87.6% | 0.85 | 5.7e-1 | 41.7% |
| | 600 | 1,181,294 | 96.2% | 1,370,865 | 76.1% | 0.86 | 4.3e-1 | 12.2% |
| | **1,200** | **1,227,451** | **100%** | **1,800,988** | **100%** | 0.56 | 2.4e-1 | 23.3% |
| | 2,400 | 1,220,770 | 99.5% | 1,684,642 | 93.5% | 0.27 | 1.6e-4 | 12.0% |
| global | 1,000 | 796,829 | 93.3% | **1,292,861** | **100%** | 0.85 | 1.0e-1 | 36.9% |
| | **2,000** | 853,133 | 99.9% | 1,196,841 | 92.6% | 0.85 | 4.8e-2 | 26.2% |
| | 4,000 | **854,137** | **100%** | 1,272,544 | 98.4% | 0.52 | **1.0e-4** | 9.5% |
| | 8,000 | 840,613 | 98.4% | 1,234,863 | 95.5% | 0.26 | **1.0e-4** | 8.8% |

local 120 and regional 1,200 are best or within 0.2% at both rates.

A repeat of local 120 on a fresh allocation gave 1,230,836/s at 120k (3.2% under the first
run) but 1,543,141/s at 300k, 27% under the first run's 2,100,250. The dwell reproduces;
the Flash rung does not, and on this evidence the 300k column above should be read as one
sample of a bimodal quantity, not as a value.

**Global is 2,000 rather than the more profitable 4,000** because at 4,000 u is 0.52 at C
and only 0.74 at 2.5C, so lambda sits at its 1e-4 floor for the whole run and the price
never regulates - the same condition that rules out 20,000 on the Java store's global
topology. At 2,000, u holds at 0.85 at both rates. The cost is 0.1% profit at C and 6.3%
at 2.5C. 1,000 regulates harder still and wins at 2.5C, but costs 6.7% at C and breaks the
monotonicity in delay that 120 / 1,200 / 2,000 has.

Profit per request falls from 120k to 300k on every topology - local 10.60 -> 8.11,
regional 10.26 -> 6.59, global 7.44 -> 5.31 - so overload does not merely serve fewer
requests, it serves them at lower value.

## The price controller: windup, and why lambdaMax exists

Admission keeps a level only while `expected > lambda * omegaMs` (RungScorer step 4f), and
the largest rung profit in the SLA table is 150, so any lambda above `150 / omega` already
refuses everything and more of it changes no decision. The controller does not stop there.

Under overload, in-flight is bounded by `ingressHardCapInFlight`, not by the price - a
request occupies a slot from admission until its rejection reply goes out, so at 2.5C the
pipeline stays full of about-to-be-rejected requests and u is pinned at `cap / sMax`
whatever lambda does. The error term `u - uTarget` therefore stays positive permanently
and the multiplicative update compounds unopposed. That is integrator windup, made
explosive by the multiplicative form.

Measured on local with the 60,000 cap (`u = 18.75`), during a 60 s phase at 2.5C:

| t into Flash | u | in-flight | lambda |
|---|---|---|---|
| 0 s | 0.42 | 1,329 | 0.98 |
| 10 s | 18.74 | 59,964 | 1.4e36 |
| 20 s | 18.74 | 59,974 | 3.6e113 |
| 40 s | 18.74 | 59,977 | 2.6e227 |
| 50 s | 18.74 | 59,964 | **Infinity** |

`exp(0.1 * (18.74 - 0.85))` is 5.98 per 100 ms tick, about 10^38 every five seconds.
In-flight never moves: the price rose by 300 orders of magnitude and changed nothing about
the quantity it controls. Infinity is absorbing under a multiplicative update exactly as
zero is, so that leader then refused every request for the remaining 41% of the run - 7,695
of 18,623 leader intervals at infinity, while all four followers stayed at the price floor
and kept serving. Raft never intervened and should not have: the node held leadership,
heartbeat and replicated throughout, and answered every request, with a rejection. The
fault was in the admission layer above Raft, which Raft cannot observe.

Regional and global reached 1e167 and 1e130 without overflowing, because their `cap / sMax`
was 8.33 and 6.00, and recovered only after ~460 s of refusing everything.

`chameleon.lambdaMax: 1.0e6` bounds it. It changes no admission decision - even an
implausibly fast omega of 0.01 ms puts saturation at 15,000, so 1e6 clears it by ~67x - and
it makes the saturation recoverable, since recovery is proportional to `log(lambdaMax)`.
Verified: no infinity in any phase of six runs across two caps and two arms, with lambda
returning to its 1e-4 floor during Collapse.

The clamp masks the windup rather than curing it. Under saturation the error stays positive
whatever the price does, so the integrator will always run to its bound; clamping makes
that harmless. Curing it would mean not integrating while the price is saturated - the
textbook anti-windup condition - which is a change to the mechanism under study and has not
been made.

## Harness corrections these numbers depend on

**Delay-class deduplication in `run_all_cloudlab.sh`.** The geo setup previously created
one `htb` class and one `netem` instance per (src, dst) pair. The cost is contention among
the per-class instances, and it hits the fastest topology hardest, because netem's
per-dequeue cost is amortised over resident packets (~5 on local against thousands on
global). Emitting one class per distinct delay fixed it: local's client went from 25
classes to 2, and the failure mode it caused - majority writes expiring against their
deadlines on the *local* topology - disappeared. Any measurement predating this is void;
on local it moved write latency from 602.3 ms to 27.7 ms.

**`NETEM_LIMIT` stays at netem's default of 1,000.** Raising it to 50,000 was measured and
is harmful: the queue stops signalling congestion, so TCP never backs off. Netem drops
fell 0.315% -> 0.045% but tracking at 700k fell 96.0% -> 79.6% and the age of an expired
frame rose from 2.6 s to 22.6 s mean / 89 s p99.

**`ingressHardCapInFlight = 3 * sMax`, and the ratio is the thing that matters.** In-flight
cannot exceed the cap, and `u = in-flight / sMax`, so `cap / sMax` bounds the largest
utilization the price controller can ever observe - which decides whether it can reach
uTarget at all. Measured on local (sMax 3,200) at three ratios, at 1C and 2.5C:

| cap | ratio | 1C: cap rejects | 1C: price rejects | lambda at 1C | lambda at 2.5C | u at 2.5C | profit/s W+P+F |
|---|---|---|---|---|---|---|---|
| 4,800 | 1.5x | 7.72% | **0.00%** | floor | 1.66 | 0.98 | 29,402,978 |
| **9,600** | **3x** | 1.04% | 0.41% | 3.0e-1 | **235** | **0.78** | **31,081,570** |
| 60,000 | 18.75x | 0.00% | 6.09% | 6.6e-1 | **saturated at lambdaMax** | 0.91 | 23,138,475 |

At 1.5x the cap does all the admission and the price never engages. At 18.75x the price
engages but u reaches 18.75, so the controller can never reach target, saturates, and
rejects 72% at the scorer - serving 557,086/s against 1,518,371/s at 3x. Only at 3x does
lambda stay four orders below its ceiling while holding u at target, and return to its
floor when the load drops.

3x also earns more while serving *less*: at 2.5C it serves 1.52M/s against 1.5x's 1.70M/s
and earns 12% more, because profit per request is 9.84 against 7.85 - the price selecting
what to keep rather than the cap shedding blindly.

An absolute 60,000 was used briefly and was a mistake, chosen from the chameleon arm at 1C
alone. **It cripples the arms that have no price**, where the cap is the only admission
control: strongest at 1C ran 92.5 ms with 17.2% violations and in-flight averaging 54,596,
against 22.8 ms and 0.0% at 3x. Comparing a regulated arm against baselines hobbled that
way would not be a fair experiment, and the defect is invisible unless the baseline arms
are inspected.

In-flight remains heavy-tailed - max/mean over 100 ms intervals reached 9.6x on local at
sMax 3,200 - so a 3x cap does clip spikes (1.04% at 1C). That clipping is the price of a
controller that can reach its target, and `lambdaMax` keeps the consequence bounded.

**`replicationLingerMs: 5.0`**, measured on local after the dedup fix: against 1 ms it
raised C from 800,000 to 900,000, cut leader-to-follower messages from 3,048/s to 744/s by
growing the batch from 135 to 580 entries, and *lowered* mean write latency (18.8 ms
against 21.1 ms at 800,000). 10 ms is worse, collapsing tracking to 71% at the top rungs.
Held at 5 ms on all three topologies; the regional and global halves of that comparison
were not measured separately against 1 ms.

**The injector now counts budget it never offered.** A worker that falls a full tick behind
re-anchors its schedule and drops the missed ticks, which is correct - injecting them late
would burst-load a struggling system - but it was previously unmeasured, so a phase the
client could not generate silently became a smaller one. `Skipped=` now appears in the
progress and completion lines, and a shortfall above 0.01% of configured load prints a
warning. This is what established the 2.5M emission ceiling.

**`transport` is required.** It previously defaulted to `framed` when absent. A generated
config that had lost the line ran a full mongod calibration ladder against the Java store,
with every other key valid and the numbers plausible enough to analyse. Those three runs
were discarded.

**Machine clocks are checked before a distributed run starts.** The servers compute a request's age and remaining deadline from the client's wall-clock stamps against their own clock, so the machines have to agree to well under a deadline. On a fresh allocation one server booted 228 ms fast, and chrony was slewing it back at roughly 30 ms per five minutes. That node expired 605,000 of the 627,000 requests/s it saw on arrival, and because an instant rejection looks like the fastest replica it drew most of the reads to itself; the retries reached the other servers with their deadlines already spent. Writes, which go to the leader, were unaffected, so the three local sMax runs that hit it completed with 83% violations and served throughput that scaled with sMax - a plausible-looking result. `run_all_cloudlab.sh` now refuses to start when any machine is more than 2 ms off NTP time, `setup_cloudlab.sh` steps the clock after provisioning, and those three runs were discarded and repeated.

**The Raft log is bounded.** It is in memory and was never compacted, so every admitted write stayed on the heap of every node for the whole run: a `LogEntry` (index, term, key, value, opId) per write, about 200 to 300 bytes. The servers run with the JVM's ergonomic maximum heap, 30 GiB on the 188 GB CloudLab machines, which is about 145 million entries. In the first day-sequence sweep on the shipping configuration every Java-store run reached that point between 1,060 s and 1,770 s, depending on how many writes the arm admitted: the 100 ms occupancy stream then shows stop-the-world pauses of about 1 s on all five nodes in the same minute (identical logs, identical heaps), growing to 2 to 20 s on the leader, against a 5 s election timeout. Leadership was lost for the first time after 143.5M to 145.5M served writes on local and regional and 119M to 123M on global, in every one of the nine runs that lost it; the other six ended below that count. No election happened in any run before the pauses began, so overload alone never destabilised Raft. The 15 Java-store runs are void, the 15 mongod runs (capped oplog) are not. `RaftLog` now compacts committed entries behind a retention window (`server.logRetentionEntries`, 1,000,000) and a follower whose next entry is compacted is restored from the leader's committed KV state over a new InstallSnapshot stream.

## Known gaps

- **mongod repeats are thin.** Global C now has two runs and they agree (see above). Of
  the sMax points only local 120 has a repeat, and its 300k Flash rung came back 27% lower,
  the same bistability seen at local 250,000 (91.6% with 5.2% pool drops in one run, 99.1%
  with 0.0% in another). Every other mongod sMax point is a single run.
- **The Java store's regional 7,200 is bimodal** - its two runs gave 9.69M and 9.23M
  profit with write admission 97.8% and 66.8%. The cause is a threshold effect: admission
  compares predicted profit against `lambda * cost`, and every request in a class carries
  the same profit and cost, so a class sits entirely above or below the line. A 26% lambda
  change (1.38e-1 to 1.74e-1) flipped three write classes wholesale, 8.6M requests in one
  dwell, while every read class was unchanged. Run-to-run variance near such a threshold
  is bimodal, not noise, so repeats there produce a distribution whose mean describes
  neither mode.
- **The Java store's day-sequence runs on the shipping configuration are void** (the log
  growth above) and must be repeated with the bounded log. Their first 1,000 s are sound and
  already show the arm ordering chameleon > pileus > strongest > weakest before any node
  was affected (local: -15%, -19%, -36% against chameleon before Payday). The 15 mongod runs
  stand.
- **The 3x cap is measured at 2.5C on local only.** Global at 1C shows the price doing the
  admission (scorer 9.8%, cap 0.1%), but neither regional nor global has been driven to 2.5C
  under the shipping cap, so whether it regulates or saturates there rests on the ratio
  argument. Every sweep run reports `HardCapRejected` and lambda per phase, so a problem
  will surface in the experiment data without a separate calibration run.
- **Log compaction has not been exercised on the cluster.** It is covered by unit tests and
  a 3-node integration test in which a partitioned follower is restored from a snapshot,
  but no CloudLab run has yet gone past the old 145M-entry point with the bound in place.
  The first repeated day sequence is that measurement.
