# MongoDB performance baselines

Reference numbers for the MongoDB port, all measured on 2026-09-16 on one CloudLab node (clnode355, Clemson: AMD EPYC 9354P, 32 cores / 64 hardware threads, 188 GB RAM, NVMe, Ubuntu 24.04, kernel 6.8), with every process on that node. Run directories are under `runs/` and the closed-loop benchmark artifacts under the node's `~/bench/`. MONGODB_PORT.md holds the narrative; this file holds the tables.

## Binaries

| name | source | build | size | location on the node |
|---|---|---|---|---|
| debug | mongodb/mongo 6d990ba + chameleon patch | bazel default profile `fastbuild` (opt off, dbg on, debug assertions) | 1.6 GB | superseded; cache id `6d990baca59c-652b98980c9d6e86` |
| ours | mongodb/mongo 6d990ba + chameleon patch | `--config=opt` | 1.49 GB | `~/tuneable/mongod`, build id `6d990baca59c-652b98980c9d6e86-opt` |
| control | mongodb/mongo 6d990ba, unpatched | `--config=opt` | 1.48 GB | `~/tuneable/control/mongod` |
| stock | MongoDB 8.2.12 release tarball (ubuntu2404 x86_64) | MongoDB's release build (includes profile-guided optimization) | 222 MB | `~/tuneable/stock/mongod` |

"Hooks on" means `chameleonEnabled=true` pushed by the WorkloadDriver and the envelope on every request; "hooks off" means `mongodb.serverHooks: false` (nothing pushed, parameter at its default false, hooks return at their first check, envelope still sent). Every run below was made before any of the pending harness fixes (write-timeout invalidation, driver logging) and with the open-file limit raised to 64000.

## 1. Standalone mongod, closed-loop client

One mongod, no replica set, port 27100, `MongoBench` (sync Java driver, one operation in flight per thread, `_id` point reads, `updateOne` with `$set` for writes, `w:1`, 100,000 documents, 10 s measured after 3 s warm-up). CPU per operation is mongod user+system time over the window divided by operations.

### 1a. Point reads only

| threads | debug ops/s | debug CPU/op | ours ops/s | ours CPU/op | control ops/s | control CPU/op | stock ops/s | stock CPU/op |
|---|---|---|---|---|---|---|---|---|
| 8 | 12,828 | 0.636 ms | 81,389 | 0.078 ms | 80,755 | 0.080 ms | 97,693 | 0.059 ms |
| 32 | 47,464 | 0.645 ms | 203,927 | 0.105 ms | 204,036 | 0.104 ms | 254,966 | 0.080 ms |
| 128 | 57,754 | 0.727 ms | 254,756 | 0.107 ms | 282,495 | 0.109 ms | 328,167 | 0.088 ms |
| 512 | 55,218 | 0.718 ms | 268,794 | 0.107 ms | 299,808 | 0.104 ms | 317,561 | 0.082 ms |

Mean latency at 8 threads: debug 623 µs, ours 98 µs, control 99 µs, stock 82 µs.

### 1b. 85% reads, 15% updates

| threads | debug ops/s | debug CPU/op | ours ops/s | ours CPU/op | control ops/s | control CPU/op | stock ops/s | stock CPU/op |
|---|---|---|---|---|---|---|---|---|
| 8 | 11,623 | 0.676 ms | 75,498 | 0.085 ms | 75,478 | 0.085 ms | 91,502 | 0.065 ms |
| 32 | 43,684 | 0.695 ms | 198,270 | 0.111 ms | 196,532 | 0.110 ms | 228,811 | 0.089 ms |
| 128 | (not run) | | 279,543 | 0.117 ms | 279,368 | 0.116 ms | 314,552 | 0.096 ms |
| 512 | 59,941 | 0.787 ms | 269,371 | 0.115 ms | 274,332 | 0.114 ms | 301,009 | 0.096 ms |

### 1c. Interleaved A/B, control against ours, three rounds each

Same benchmark, the two binaries alternated (control, ours, control, ours, control, ours). Mean ops/s with [min to max]; CPU per operation is the three-round mean.

| threads | mix | control ops/s | ours ops/s | control CPU/op | ours CPU/op |
|---|---|---|---|---|---|
| 8 | reads | 81,794 [81,160 to 82,143] | 80,537 [79,920 to 81,380] | 0.079 ms | 0.080 ms |
| 8 | 85% reads | 74,507 [73,507 to 75,039] | 74,356 [73,764 to 75,324] | 0.085 ms | 0.086 ms |
| 32 | reads | 205,207 [202,354 to 207,521] | 203,604 [202,863 to 204,213] | 0.104 ms | 0.104 ms |
| 32 | 85% reads | 196,474 [195,316 to 198,446] | 199,045 [195,408 to 202,983] | 0.111 ms | 0.111 ms |
| 128 | reads | 285,240 [257,296 to 309,071] | 269,885 [257,010 to 282,291] | 0.109 ms | 0.109 ms |
| 128 | 85% reads | 285,604 [281,686 to 289,441] | 292,382 [288,949 to 297,583] | 0.115 ms | 0.116 ms |
| 512 | reads | 269,817 [252,450 to 282,986] | 292,265 [290,305 to 295,647] | 0.104 ms | 0.105 ms |
| 512 | 85% reads | 266,507 [261,316 to 276,222] | 258,867 [209,126 to 288,365] | 0.115 ms | 0.114 ms |

Ours ahead in four cells, behind in four; every range overlaps; CPU per operation equal to the third decimal.

## 2. Five-node replica set, closed-loop client

Five mongods on 127.0.1.1 to 127.0.1.5:27017, replica set `rs0` initiated by `MongoClusterAdmin init` (tags, node 0 at priority 10), `MongoBench` with `w:majority` and `readPreference=nearest`, 10 s measured after 3 s warm-up. CPU per operation sums the five mongods.

| workload | threads | stock ops/s | stock CPU/op | control ops/s | control CPU/op | ours ops/s | ours CPU/op |
|---|---|---|---|---|---|---|---|
| reads | 32 | 205,505 | 0.106 ms | 186,455 | 0.127 ms | 183,018 | 0.128 ms |
| reads | 128 | 233,136 | 0.107 ms | 218,289 | 0.125 ms | 195,977 | 0.128 ms |
| reads | 512 | 210,569 | 0.106 ms | 200,628 | 0.125 ms | 215,482 | 0.126 ms |
| 85% reads | 32 | 69,204 | 0.329 ms | 82,305 | 0.351 ms | 64,902 | 0.371 ms |
| 85% reads | 128 | 129,020 | 0.257 ms | 123,563 | 0.305 ms | 111,882 | 0.310 ms |
| 85% reads | 512 | 128,710 | 0.257 ms | 124,447 | 0.295 ms | 120,658 | 0.295 ms |

Single runs; the 32-thread mixed cell is latency-bound by the majority acknowledgment and varies with which node is primary. Adding 15% majority writes roughly triples CPU per operation on every binary.

## 3. Harness capacity probes, weakest arm, geo off

WorkloadDriver, `mode: weakest`, normal mix (about 87% reads), six client sites without delay rules, `maxPoolSize` 2000 per site, open-loop injection in 100 ms ticks, 30 s per step. Served is the mean over the step's progress lines. Configs: `config_mongodb_capacity_probe*.yaml`.

### 3a. Debug binary (hooks on), `runs/probe_20260916_102119`

| offered req/s | served req/s | note |
|---|---|---|
| 5,000 | 5,000 | clean |
| 10,000 | 10,000 | clean |
| 20,000 | 20,000 | 4.5% violations, primary invalidations begin |
| 40,000 | 34,000 | mongods on about 48 of 64 hardware threads, client on 6 |
| 80,000 | 35,000 | client could inject only 42,000 to 47,000 |

### 3b. Ours, hooks on, coarse steps, `runs/probeopt_20260916_123008` and `runs/probeopthigh_20260916_123530`

| offered req/s | served req/s | mean latency |
|---|---|---|
| 5,000 to 80,000 | equal to offered | 5.8 ms overall, zero violations, 11 deadline-exceeded in 4.5 M |
| 120,000 | 120,000 | 5 ms |
| 160,000 | 160,000 | 5 ms, a few hundred violations per second |
| 240,000 | 161,000 to 174,000 | above 250 ms, deadlines dominate |
| 320,000 | 156,000 to 159,000 | |
| 400,000 | 155,000 to 157,000 | client at 17 to 30 hardware threads, mongods at about 42 |

### 3c. Fine steps, all binaries and hook states

Served requests per second at each offered step. "ours on" is the experiment configuration. Four-way = `runs/fourway-*_20260916_16*` (back to back, same day); the ours pair = `runs/probehooks_20260916_154336` and `runs/probenohooks_20260916_153702`.

| offered | stock (four-way) | control (four-way) | ours off (four-way) | ours on (four-way) | ours on (earlier fine) | ours off (earlier fine) |
|---|---|---|---|---|---|---|
| 148,000 | 147,700 | 147,000 | 147,400 | 147,500 | 147,000 | 147,100 |
| 169,500 | 169,400 | 169,500 | 169,300 | 169,400 | 169,600 | 169,300 |
| 189,300 | 189,000 | 189,100 | 188,800 | 185,300 | 184,700 | 187,400 |
| 209,300 | 208,600 | 186,600 | 180,700 | 173,000 | 176,800 | 182,800 |
| 228,000 | 209,300 | 187,300 | 181,700 | 174,000 | 175,400 | 184,100 |

Every run saturates the box past the knee at the same CPU: mongods about 40 to 44 hardware threads, client 17 to 20 of 64. The knee for the experiment binary with hooks on is 170,000 requests/s (served in full with 0.5 to 0.9% violations and 12 ms mean latency; 150,000 is the last step with about 0.1% violations). C = 170,000 for the MongoDB configs, against C = 400,000 for the Java store on the same node type (Java-store runs from the same CloudLab experiment served 196,000 of 200,000 offered).

Ours-on plateaus observed across separate runs: 170,000 to 175,000 (coarse), 171,000 to 174,000 (fine), 175,000 to 177,000 (pair), 173,000 to 174,000 (four-way). Ours-off plateaus: 183,000 to 184,000 (pair), 181,000 to 182,000 (four-way).

### 3d. Interleaved harness A/B, control against ours off, three rounds each

Full fine probe, alternated (control, ours off) three times. `runs/ab{1,2,3}-control_*`, `runs/ab{1,2,3}-oursoff_*`. Mean served with [min to max].

| offered | control | ours off | difference |
|---|---|---|---|
| 148,000 | 147,300 | 147,200 | none |
| 169,500 | 169,500 | 169,500 | none |
| 189,300 | 188,800 [188,200 to 189,200] | 188,400 [187,600 to 189,100] | 0.2%, ranges overlap |
| 209,300 | 185,100 [182,200 to 186,500] | 181,600 [180,000 to 182,500] | 1.9% |
| 228,000 | 186,600 [184,600 to 188,500] | 183,700 [182,100 to 186,300] | 1.6% |
| plateau (210k and 230k pooled) | 185,800 [183,300 to 187,400] | 182,500 [180,900 to 184,200] | 1.7% |

Per-run plateaus: control 186,651 / 187,398 / 183,282; ours off 180,933 / 184,192 / 182,496. Direction consistent in five of six pairwise comparisons.

### 3e. Hardware counters at saturation, control against ours off

Flat 230,000 offered for 100 s (`config_mongodb_perf_*.yaml`, `runs/perf-control_20260916_181418`, `runs/perf-oursoff_20260916_181834`); `perf stat` over 30 s of the saturated phase on the five mongods (events multiplexed, about 62% coverage each).

| counter | control | ours off | ours vs control |
|---|---|---|---|
| served req/s in the window | 170,300 | 172,100 | +1.1% |
| instructions | 1.466 T | 1.451 T | -1.0% |
| cycles | 4.268 T | 4.231 T | -0.9% |
| instructions per served request | 287,000 | 281,000 | -2% |
| L1 instruction-cache misses | 6.10 G | 5.53 G | -9% |
| iTLB misses | 6.49 G | 6.55 G | +1% |
| dTLB misses | 4.41 G | 4.40 G | 0% |
| cache misses | 122.4 G | 123.8 G | +1% |
| branch misses | 33.7 G | 34.6 G | +3% |
| context switches | 16.9 M | 17.5 M | +3% |

## Conclusions

1. The debug build was the ten-times factor: 0.65 ms of CPU per point read against 0.08 to 0.11 ms for the same tree built with `--config=opt`. `build_mongod.sh` now always builds opt and stamps the profile into the build id.
2. Stock 8.2.12 is 12 to 20% faster than the unpatched pinned commit built with opt. That is the release branch plus MongoDB's profile-guided optimization, which our build leaves off; nothing in the patch is involved.
3. The patch with its hooks idle costs nothing measurable in work: CPU per operation equal to the third decimal standalone, instructions per request equal within counter noise under the harness. The residual 1 to 2% throughput offset at saturation in the three-round A/B is consistent in direction but reverses in the perf pair, with 9% fewer instruction-cache misses on our side there; it is a code-layout effect of the build, not something the source does, and it cancels in the experiment because every arm runs the same binary.
4. The hooks doing their work, every arm's cost for the measurement plane, are 4 to 5% of saturated throughput (about 174,000 against 182,000 to 184,000) and nothing below the knee.
5. C = 170,000 requests/s for the MongoDB configs on this node, weakest arm, geo off, hooks on.
6. Under the harness the four configurations are indistinguishable below 170,000 offered; they separate only past the knee, which is where the experiment's overload phases operate.

## 3f. Capacity under the global geo topology, weakest arm, hooks on

Same harness and mix as 3c with the delay rules on (`config_mongodb_capacity_probe_fine_geo.yaml`, `runs/probegeo_20260916_210904`; `config_mongodb_capacity_probe_low_geo.yaml`, `runs/probelowgeo_20260916_211441`), build `6d990baca59c-21cdf984cb77ebbf-opt` with the `bindsrc.c` shim so replication is delayed too.

| offered req/s | served req/s | violations/s | mean latency | connections per mongod (peak) |
|---|---|---|---|---|
| 39,500 | 37,500 | 5,500 | 82 ms | |
| 59,300 | 58,400 | 6,100 | 82 ms | |
| 79,300 | 78,500 | 7,800 | 82 ms | |
| 99,100 | 98,000 | 10,600 | 82 ms | 1,700 to 4,200 |
| 118,600 | 115,700 | 17,700 | 97 ms | |
| 148,200 | 111,700 | 46,800 | 100 to 210 ms | about 5,000 |
| 169,800 | 110,100 | 99,300 | 300 ms | |
| 188,700 | 108,000 | 128,800 | 330 ms | |
| 208,200 | 104,200 | 156,100 | 380 ms | |
| 223,800 | 97,700 | 178,000 | 390 ms | 4,400 to 5,800 |

The 5,000 to 8,000 violations per second present at every rate are geo-inherent for weakest (far sites cannot meet the tighter thresholds for any level); the knee is where served stops tracking offered and violations rise above that floor: about 100,000 to 110,000 requests/s, against 170,000 without delays. At the knee the box is nearly full (mongods 33 to 36 hardware threads, client 20 to 22, load average 100 to 200) with about 10,000 connections open; past it the load average exceeds 600 with 25,000 connections. Per-request CPU is unchanged by distance; the loss is the connection-per-in-flight-request model holding 10 to 15 times more connections and threads at the same rate. The Java store under the same delay matrix on the same node type served 195,900 at 200,000 offered with a 101 ms mean latency (`runs/config_global_chameleon_9000_7`), its in-flight requests being queue entries over 16 multiplexed sockets per site and server. A steady 300 to 500 client-side failures per second at every rate are pool checkout timeouts. They are not connection churn (the mongod logs show no connection closed during the run) but throttled pool growth: the driver opens at most `maxConnecting` = 2 connections per pool at a time, each open costs two round trips under geo, so the Singapore-to-Virginia pool grew by exactly 42 connections per 10 s (460 ms per open) for the whole run while Little's law asked for 350 to 1,000 concurrent connections at its 1,500 to 4,500 requests/s; checkouts that find the pool busy wait behind those opens and fail at the 200 ms pool wait. Failures concentrate on the far pairs (Singapore to its four far nodes about 5%, Tokyo 1 to 2%, others near zero) and spike at each rate step. Fix (implemented after this probe, not yet remeasured): `maxConnecting` raised to the pool size and a separate one-second pool wait, `mongodb.poolWaitTimeoutMs`, decoupled from the server-selection timeout.

### 3g. Geo probe, hooks on against hooks off, 40,000 to 120,000

Same steps and delay rules, back to back (`runs/probelowgeo_20260916_211441`, `runs/probelowgeonohooks_20260916_21*`).

| offered req/s | served, hooks on | served, hooks off | violations/s on | violations/s off | mean ms on | mean ms off |
|---|---|---|---|---|---|---|
| 39,500 | 37,480 | 37,661 | 5,495 | 5,372 | 85 | 86 |
| 59,300 | 58,433 | 58,354 | 6,129 | 6,244 | 82 | 83 |
| 79,300 | 78,521 | 78,228 | 7,815 | 7,982 | 82 | 81 |
| 99,000 | 98,032 | 97,896 | 10,620 | 10,488 | 82 | 85 |
| 118,600 | 115,690 | 116,140 | 17,748 | 15,512 | 97 | 93 |

Totals 11.70 M against 11.72 M served of 11.94 M sent; peak connections per node identical to within 1% (4,150 / 2,170 / 1,715 / 2,990 / 1,875); mongod CPU 31 to 38 hardware threads and client 20 to 25 in both at the top steps. Under geo and below the knee the hooks cost nothing measurable, consistent with the local result that their cost appears only at saturation. The 1-minute load average read 460 to 870 during the hooks-off run's last two steps against 100 to 200 in the hooks-on run, with no difference in throughput, latency, CPU, or connections; unexplained and without effect on any reported metric.


## 3h. Pool growth fix and the failures past the knee (geo, weakest, hooks on)

Same low geo probe as 3f after the transport change (`maxConnecting` = pool size, `poolWaitTimeoutMs` 1000), two runs (`runs/probelowgeo2_20260916_224308`, `runs/probelowgeo3_20260916_225313`), then three 70 s diagnostics that jump to 120,000 requests/s (`runs/stormgeo*`), the last two with the new failure-cause table and driver-event listeners.

| step (requests/s) | 3f failures/s | after fix, run 1 | after fix, run 2 |
|---|---|---|---|
| 40,000 to 80,000 | 250 to 700 | 0 to 4 | 0 to 1 |
| 100,000 | 300 to 500 | 118 to 5,500 (pool-fill storm), then 1,500 | 0 to 3 |
| 120,000 | 400 | 5,700 to 6,700 | 1 to 8,400 (storm) |

Below the knee the fix removes the failures entirely and served equals offered. At the knee the picture changed shape: with growth unthrottled, the latency spike makes every site-to-node pool fill to its 2,000 cap within ten seconds (every mongod at 12,025 connections, six sites x 2,000 plus monitors), the box carries 60,000 client threads, and served at 120,000 offered is 90,000 to 110,000 against about 117,000 in 3f, where the two-at-a-time throttle had kept total connections near 25,000. Where exactly the storm starts moves between runs (100,000 in run 1, 120,000 in run 2), which is the knee itself.

Failure causes past the knee (from `runs/stormgeo2_20260916_230741`, every attempt including retries): 760,800 server-selection timeouts, 136,300 pool checkout timeouts, 131,200 server-side deadline expiries. The driver listeners attributed the first entirely to the primary being marked UNKNOWN by a code-50 write-concern error (one to five episodes per site in 70 s, each stalling every request bound for the primary from that site until the next heartbeat), the failure mode of MONGODB_PORT.md item 8 surviving the server-side clamp under a 60,000-thread box. After writes stopped carrying `maxTimeMS` (`runs/stormgeo3_20260916_231420`): no node marked UNKNOWN, failures zero in every window, redirects down from 2,000 to 11,000 per second to about 100, server-selection timeouts 1,601 (all in the first second, before the far sites had discovered the cluster), and the shortfall at 120,000 offered graded as deadline exceeded (13,000 to 28,000 per second, from 564,000 checkout timeouts at the cap plus server-side deadlines). Mean latency of served requests rose from 230 to 390 ms to 470 to 860 ms because writes now run to the envelope's wait bound instead of being cut at the deadline; this is the Java store's behaviour as well, and it is the past-the-knee regime.


## 3i. Calibration on the global topology, C = 100,000

Runs `runs/cal-*_20260916_23*`. All from `config_mongodb_global.yaml` (sMax 450, geo on) with the phases replaced; 10 s warm-up at 10,000 unless noted.

**Pool cap** (weakest arm, 20 s at 80,000 then 40 s at 120,000):

| maxPoolSize | served/s at 80,000 | served/s at 120,000 | rejected/s at 120,000 | mean latency at 120,000 | primary connections | driver incidents |
|---|---|---|---|---|---|---|
| 1,000 | 71,600 | 111,700 | 3,200 | 218 ms | 6,036 | none |
| 2,000 | 65,100 | 88,100 | 16,300 | 525 ms | 12,037 | none |
| 4,000 | 47,000 | 75,300 | 1,200 | 682 ms | 17,755 | 6 nodes marked UNKNOWN, 206,000 checkout timeouts |

Monotone: the smaller the cap, the more is served at every rate, at lower latency, with fewer threads on the mongods. A larger cap lets the latency excursion at the knee grow in-flight, connections and threads together until every pool sits at its cap (six sites x cap per mongod); at 4,000 the client also ran into its 64,000 descriptor limit (sites x nodes x cap = 120,000), which failed socket opens and made the driver mark nodes UNKNOWN and clear pools, so the runner's limit is now 262,144. The 80,000 step in these runs includes the pools' growth from the 10,000 warm-up, so it is below the settled figures of 3h. Decision: 1,000, in the experiment and smoke configs.

**Strongest arm at 0.5C** (60 s at 50,000 after a 20 s warm-up at the same rate): with the 4,000 cap 41,000 served, 8,900 rejected and 127 deadline expiries per second, mean latency 75 ms, 24,037 connections on the primary; with the 1,000 cap 41,700 served, 8,100 rejected, 156 expiries, 73 ms, and only warm-up checkout timeouts. The rejections are the primary's occupancy backstop (utilization 1.4 against S_max 450, in-flight pinned at about 640, the 1.5 x S_max cap): every write and linearizable read of every site lands on the primary, so the arm exceeds the primary's calibrated occupancy at half of C. The pool cap is not what bounds strongest. Also seen: with utilization held above target and nothing to downgrade, the price on the primary ran to 10^12 and beyond, since the multiplicative update has no ceiling; irrelevant to strongest, which ignores the price, but the chameleon arm under sustained overload would show the same growth.

**S_max** (chameleon arm, 1,000 cap, 20 s warm-up at 50,000, 60 s at 80,000, 60 s at 180,000; `runs/cal-smax1000_20260916_235715`): at 0.8C the primary's utilization settled at 0.81 to 0.88 against the 0.85 target with the price at about 0.05, just off its 0.0001 floor, 72,000 served, 7,100 rejected per second, mean latency 50 ms, and the served mix shifted to causal-local 40%, causal-majority 28%, eventual 22%, w:1 5%, w:3 3%. In the warm-up at 0.5C, with upgrades still free, utilization ran to 1.0 to 1.24 and the price rose to 0.04 before settling, so on mongod under geo the price is already active at half of C: the primary's occupancy, not the box, is the scarce resource once free upgrades send writes at w:3 and reads at majority or linearizable levels to it. S_max stays 450 (the primary's mean in-flight at the weakest arm's knee, 432): the controller regulates to target at 0.8C exactly as designed, and the 0.5C activity is a property of the system under test, not of the constant. At 1.8C the run is dominated by the pool cap: every one of the 30 pools sat at 1,000 (6,036 connections on the primary, 6,025 on a follower), 15,500 served against 104,000 rejected and 50,700 deadline expiries per second, and the price on the primary fell to 0.004 because requests that die in the client pools never reach the occupancy meter. The rejections came from the followers (92,000 per second at near-zero occupancy), so they are scorer refusals, not the backstop: the RTT estimate the client sends includes its pool waits of up to a second, and with it inflated no rung is attainable; admission-aware routing then moves traffic to other, farther nodes, whose pools fill at geo round trips. This interaction between a client-side connection cap and rejection-driven rerouting does not exist in the Java store, whose multiplexed sockets carry unbounded in-flight, and it is the open question for the 1.8C and 2.5C segments; see MONGODB_PORT.md.

**Replication budget** (weakest arm, A1 writes only so every write is w:1, isolation topology, 1,000 cap, 20 s per step; `runs/cal-writes1000_20260917_000214`): served 37,800 / 74,500 / 105,800 writes per second at 40,000 / 80,000 / 120,000 offered with mean latency 3.8 / 5.6 / 13.9 ms, then 74,800 at 160,000 and 65,300 at 200,000 as the occupancy backstop rejected 79,000 to 127,000 per second (utilization 1.0 to 1.1). Ceiling about 105,800; budget 0.8 x that, 85,000. The earlier geo run of this probe at the 4,000 cap (`runs/cal-writes_20260916_234243`) measured only the connection collapse and is not used.

## 4. Verification of the write-timeout fix, strongest arm at saturation

Build `6d990baca59c-21cdf984cb77ebbf-opt` (wtimeout clamped inside the operation deadline, shape key gated on the estimator), flat 230,000 offered for 100 s, geo off. `runs/verify-hooks_20260916_202001`, `runs/verify-nohooks_20260916_202321`.

| signal | hooks on | hooks off |
|---|---|---|
| run.log size | 5.3 KB | 5.3 KB |
| driver "waiting for server" lines | 0 | 0 |
| primary marked UNKNOWN | 0 | 0 |
| writeConcernError code 50 | 0 | 0 |
| writeConcernError code 64 (wtimeout) in slow-op logs | 0 | 983 |
| served req/s at saturation | 134,700 | 152,100 |
| fall-backs | 241,995 | 2,054 |
| failures (pool checkout timeouts at the primary) | 3.79 M | 2.24 M |
| redirects (failed writes retried round-robin) | 6.96 M | 4.29 M |
| slow operations logged on the primary | 0 | 21,217 |

Strongest at 230,000 is several times the primary's own capacity, since every write and linearizable read lands there; the numbers describe extreme overload dynamics, not the arm's operating range. Its C and pool size need a separate probe.

## Harness fixes found by these runs

- Open-file soft limit raised to 64000 in the remote runner (1024 capped mongod at 409 connections).
- Every mongod started with `--nounixsocket` (five on one port raced on `/tmp/mongodb-27017.sock`).
- The runner waits for each mongod to accept connections before `replSetInitiate` (stock starts listening later than our build).
- The runner's own output is kept in `runner.log`; the orchestrator prints its tail when a run dies before `run_info.txt`.
- `mongodb.serverHooks: false` and `mongodb.binaryPath` let the harness run against the control or stock binaries; `globalLock.activeClients.total` is int64 on master and int32 in 8.x.
- Fixed after these runs: the write-timeout invalidation (wtimeout now clamped inside the operation deadline with the hooks on, no maxTimeMS on writes with them off), the driver's cluster logger set to WARN, and the shape key computed only when the exec estimator is on. Still open: the day-sequence configs carry the Java store's C, S_max, and replication budget.
