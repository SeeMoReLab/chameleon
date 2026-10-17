# Tuneable-Consistency Setup and Experiment Guide

Repository: [Tuneable-Consistency GitHub Repository](https://github.com/MARTIANSID/Tuneable-Consistency?utm_source=chatgpt.com)

---

# Install Java 17

```bash
sudo apt update
sudo apt install openjdk-17-jdk -y
```

---

# Verify Java Installation

```bash
java -version
```

You should see Java 17.

---

# Configure Java 17

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

Persist it:

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

---

# Install Maven

```bash
sudo apt install maven -y
```

---

# Verify Maven Installation

```bash
mvn -v
```

---

# Install Redis

The project uses Redis for the distributed token bucket implementation.

Install Redis:

```bash
sudo apt install redis-server -y
```

---

# Start Redis Server

```bash
sudo systemctl start redis-server
```

Enable Redis on boot:

```bash
sudo systemctl enable redis-server
```

Verify Redis is running:

```bash
redis-cli ping
```

Expected output:

```text
PONG
```

Redis runs on:

| Service      | Port |
| ------------ | ---- |
| Redis Server | 6379 |

The Java token bucket implementation connects to:

```java
new Jedis(redisHost, redisPort)
```

using port:

```text
6379
```

---

# Build the Project

From the repository root:

```bash
mvn clean install
```

---

# Run the Distributed Experiment Framework

From the repository root:

```bash
./run_all.sh [label] [config_local.yaml]
```

This builds the project and launches one `ServerNode` process per Raft node plus a `WorkloadDriver` process inside a tmux session, with all results written to `runs/<label>_<timestamp>/`. To launch the processes by hand instead, start each node with `java -cp <classpath> org.example.Server.ServerNode config_local.yaml <serverId>` and then run `java -cp <classpath> org.example.Client.WorkloadDriver config_local.yaml` (the classpath is written to `target-script/classpath.txt` by `run_all.sh`).

---

# What the Experiment Runner Does

Each Raft node runs in its own OS process (`ServerNode`); the workload driver (`WorkloadDriver`) runs in another and controls the cluster over an admin gRPC service.

The Raft log is in memory and bounded: committed entries are compacted once they trail the commit index by `server.logRetentionEntries`, and a follower whose next entry is already compacted is brought forward with the leader's committed KV state (an InstallSnapshot stream) instead of the missing entries. Without the bound the log filled the JVM's default heap about 1,200 s into the day sequence and the GC pauses cost the leadership (CALIBRATION.md).

It automatically:

* Starts multiple gRPC servers
* Elects a Raft leader
* Starts workload injection
* Runs phased consistency experiments
* Collects CSV metrics
* Simulates failures
* Simulates geo latency
* Tests tunable consistency behavior

---

# Server Configuration

This controls the cluster size:

```java
public static final int NUM_OF_SERVERS = 3;
```

The servers run on:

| Server  | Port |
| ------- | ---- |
| Server0 | 8001 |
| Server1 | 8002 |
| Server2 | 8003 |

You will see logs like:

```text
Server0 started on port 8001
Server1 started on port 8002
Server2 started on port 8003
```

---

# Client Callback Server

The framework also starts a callback listener:

```text
Client callback server started on port 9000
```

This is used for:

* ACK collection
* latency measurement
* timestamp synchronization

---

# Workload Phases

The framework automatically changes workload phases during execution.

Configured in:

```java
PHASES.add(new Phase(...))
```

Current setup:

| Phase  | Duration | TPS     | Characteristics           |
| ------ | -------- | ------- | ------------------------- |
| Light  | 70s      | 30k TPS | Mostly eventual reads     |
| Medium | 60s      | 30k TPS | Balanced consistency      |
| Heavy  | 60s      | 70k TPS | Mostly linearizable reads |
| Light  | 60s      | 30k TPS | Recovery phase            |

---

# What Gets Tested

The framework evaluates:

* Tunable consistency
* Dynamic consistency upgrades
* Throughput under varying load
* Latency behavior
* Queue pressure
* Token bucket admission control
* Read/write consistency tradeoffs
* Failure handling
* Leader re-election

---

# Important Experiment Toggles

## Transaction Upgrading

```java
private static final boolean UPGRADE_TRANSACTIONS = true;
```

When enabled:

* transactions may dynamically upgrade consistency
* stronger guarantees may be applied during execution

---

## Pressure Mode

```java
private static final boolean PRESSURE_MODE_ENABLED = false;
```

Controls:

* pressure-aware admission
* transaction deferrals
* token bucket behavior

---

## Failure Injection (Java store only)

Two config blocks, both rejected by the loader with `transport: mongodb`:

- `nodeFailure` fails one node by id from the start of the run (its inter-server RPCs are dropped and it answers every client request with a failure).
- `failureSchedule` is a list of timed events, seconds from the workload start, strictly increasing: `{ atSeconds, action: fail, target: LEADER | FOLLOWER }` fails the leader the cluster has at that moment or the lowest-numbered healthy follower, and `{ atSeconds, action: recover }` brings back the node that has been failed longest. The loader refuses a schedule that would fail more nodes at once than a majority can spare, or that recovers when nothing is failed. The driver prints each event into run.log with the workload second.

`config_failure_leader_global.yaml` and `config_failure_follower_global.yaml` (generated by `make_failure_configs.py` from `config_global.yaml`) are the failure experiment: a 600 s normal day split into 120 s windows, two failures at 120 and 240 s into it, two recoveries at 360 and 480 s (EXPERIMENT.md, *Failure experiment*).

`config_random_<seed>.yaml` is a random-SLA config generated from `config_local.yaml` by `make_random_config.py --seed <seed> [--read 3] [--write 3] [--write-rate "20;50;80"] [--duration 300]`: one application with one read SLA and one write SLA of the requested rung counts (1 to 4), each rung's level or concern drawn uniformly and its latency target (20 to 2,000 ms, 40 for writes) and profit (0.5 to 150) log-uniformly, rounded to whole milliseconds and cents, with the set redrawn until every pair is consistent (a rung at least as demanding on both consistency and latency target, and stricter on one, must pay strictly more; equal demands are a duplicate). The workload is a 30 s warm-up, one phase per write percentage at 1 C, and a 30 s cool-down; the config's own `seed` is set to the same number. `config_random_42.yaml` is the default run of it.

---

# Geo Latency Simulation

Enable:

```java
private static final boolean ENABLE_GEO_SETTINGS = true;
```

The framework uses:

```bash
simulate_geo_latency.sh
```

to inject artificial WAN latency using Linux `tc/netem`.

Example:

```java
private static final int GEO_LATENCY_MS = 50;
```

Useful for:

* geo-distributed experiments
* WAN consistency evaluation
* latency amplification studies

---

# Generated CSV Metrics

The framework automatically generates CSV files such as:

| File                                   | Description           |
| -------------------------------------- | --------------------- |
| `tps_0.csv`                            | TPS metrics           |
| `system_latency_0.csv`                 | end-to-end latency    |
| `backlog_0.csv`                        | queue backlog         |
| `avg_latencies_0.csv`                  | average latency       |
| `read_latencies_0.csv`                 | read latency          |
| `process_batch_duration_0.csv`         | batch execution time  |
| `incoming_transaction_rate_global.csv` | global injection rate |

These files are later used for plotting and analysis.

---

# Transaction Types

The framework injects both reads and writes.

## Writes

Supports tunable write concerns such as:

* W:1
* majority

Configured through:

```java
Map<Integer, Double> writeDistribution
```

---

## Reads

Supports:

* EVENTUAL
* CAUSAL_LOCAL
* CAUSAL_MAJORITY
* LINEARIZABLE

Configured through:

```java
Map<ReadClass, Double> readDistribution
```

---

# Injection Model

Transactions are injected in batches:

```java
private static final int BATCH_SIZE = 1000;
```

The injector:

* distributes reads/writes
* routes linearizable reads to leader
* routes weaker reads to followers
* continuously measures throughput

---

# Typical Workflow

## 1. Configure the Experiment

Modify:

* TPS
* workload phases
* consistency distributions
* read/write ratios
* failure toggles
* geo latency settings

---

## 2. Start Redis

```bash
sudo systemctl start redis-server
```

---

## 3. Build the Project

```bash
mvn clean install
```

---

## 4. Run the Experiment

```bash
./run_all.sh
```

---

## 5. Wait for Completion

The framework automatically:

* cycles through phases
* injects workloads
* collects metrics
* shuts down at completion

---

## 6. Plot Results

Use the plotting scripts to visualize:

* TPS
* latency
* backlog
* consistency upgrades
* throughput
* token usage

---

# Useful Configuration Locations

## Change Number of Servers

```java
NUM_OF_SERVERS
```

---

## Change TPS

Inside:

```java
new Phase(...)
```

---

## Change Read/Write Mix

```java
0.90, 0.10
```

---

## Change Consistency Distribution

```java
Map<ReadClass, Double>
```

---

## Change Experiment Duration

```java
TOTAL_EXPERIMENT_DURATION_MS
```

---

# Example Experiment Ideas

## Tunable Consistency Under Load

* Light load → eventual reads dominate
* Heavy load → linearizable reads dominate
* Measure latency vs throughput

---

## Failure Recovery Experiment

Enable timed leader failure:

* observe leader re-election
* measure TPS degradation
* analyze recovery latency

---

## Geo-Distributed Experiment

Enable geo latency:

* compare eventual vs linearizable performance
* study WAN latency amplification
* analyze consistency costs under network delay


<!-- Done -->
bash run_all_cloudlab_sweep.sh kevindmw@clnode317.clemson.cloudlab.us ~/.ssh/cloudlab --modes "chameleon;chameleonPileus;pileus;strongest;weakest" --smax "1125" --seeds "42" "config_local.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode335.clemson.cloudlab.us ~/.ssh/cloudlab --modes "chameleon;chameleonPileus;pileus;strongest;weakest" --smax "2250" --seeds "42" "config_local.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode348.clemson.cloudlab.us ~/.ssh/cloudlab --modes "chameleon;chameleonPileus;pileus;strongest;weakest" --smax "4500" --seeds "42" "config_local.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode330.clemson.cloudlab.us ~/.ssh/cloudlab --modes "chameleon;chameleonPileus;pileus;strongest;weakest" --smax "9000" --seeds "42" "config_local.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode373.clemson.cloudlab.us ~/.ssh/cloudlab --modes "chameleon;chameleonPileus;pileus;strongest;weakest" --smax "18000" --seeds "42" "config_local.yaml"

<!-- Done -->
bash run_all_cloudlab_sweep.sh kevindmw@clnode317.clemson.cloudlab.us ~/.ssh/cloudlab --modes "chameleon;chameleonPileus;pileus;strongest;weakest" --smax "1125" --seeds "42" "config_regional.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode335.clemson.cloudlab.us ~/.ssh/cloudlab --modes "chameleon;chameleonPileus;pileus;strongest;weakest" --smax "2250" --seeds "42" "config_regional.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode348.clemson.cloudlab.us ~/.ssh/cloudlab --modes "chameleon;chameleonPileus;pileus;strongest;weakest" --smax "4500" --seeds "42" "config_regional.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode330.clemson.cloudlab.us ~/.ssh/cloudlab --modes "chameleon;chameleonPileus;pileus;strongest;weakest" --smax "9000" --seeds "42" "config_regional.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode373.clemson.cloudlab.us ~/.ssh/cloudlab --modes "chameleon;chameleonPileus;pileus;strongest;weakest" --smax "18000" --seeds "42" "config_regional.yaml"

<!-- Done -->
bash run_all_cloudlab_sweep.sh kevindmw@clnode317.clemson.cloudlab.us ~/.ssh/cloudlab --modes "chameleon;chameleonPileus;pileus;strongest;weakest" --smax "1125" --seeds "42" "config_global.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode335.clemson.cloudlab.us ~/.ssh/cloudlab --modes "chameleon;chameleonPileus;pileus;strongest;weakest" --smax "2250" --seeds "42" "config_global.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode348.clemson.cloudlab.us ~/.ssh/cloudlab --modes "chameleon;chameleonPileus;pileus;strongest;weakest" --smax "4500" --seeds "42" "config_global.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode330.clemson.cloudlab.us ~/.ssh/cloudlab --modes "chameleon;chameleonPileus;pileus;strongest;weakest" --smax "9000" --seeds "42" "config_global.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode373.clemson.cloudlab.us ~/.ssh/cloudlab --modes "chameleon;chameleonPileus;pileus;strongest;weakest" --smax "18000" --seeds "42" "config_global.yaml"

<!-- Done -->
bash run_all_cloudlab_sweep.sh kevindmw@clnode317.clemson.cloudlab.us ~/.ssh/cloudlab --modes "strongest;weakest;chameleon;chameleonPileus;pileus" --smax "9000" --seeds "7" "config_global.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode335.clemson.cloudlab.us ~/.ssh/cloudlab --modes "strongest;weakest;chameleon;chameleonPileus;pileus" --smax "18000" --seeds "7" "config_global.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode348.clemson.cloudlab.us ~/.ssh/cloudlab --modes "strongest;weakest;chameleon;chameleonPileus;pileus" --smax "1125" --seeds "7" "config_global.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode330.clemson.cloudlab.us ~/.ssh/cloudlab --modes "strongest;weakest;chameleon;chameleonPileus;pileus" --smax "2250" --seeds "7" "config_global.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode373.clemson.cloudlab.us ~/.ssh/cloudlab --modes "strongest;weakest;chameleon;chameleonPileus;pileus" --smax "4500" --seeds "7" "config_global.yaml"

bash run_all_cloudlab_sweep.sh kevindmw@clnode317.clemson.cloudlab.us ~/.ssh/cloudlab --modes "strongest;weakest;chameleon;chameleonPileus;pileus" --smax "9000" --seeds "7" "config_local.yaml;config_regional.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode335.clemson.cloudlab.us ~/.ssh/cloudlab --modes "strongest;weakest;chameleon;chameleonPileus;pileus" --smax "18000" --seeds "7" "config_local.yaml;config_regional.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode348.clemson.cloudlab.us ~/.ssh/cloudlab --modes "strongest;weakest;chameleon;chameleonPileus;pileus" --smax "1125" --seeds "7" "config_local.yaml;config_regional.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode330.clemson.cloudlab.us ~/.ssh/cloudlab --modes "strongest;weakest;chameleon;chameleonPileus;pileus" --smax "2250" --seeds "7" "config_local.yaml;config_regional.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode373.clemson.cloudlab.us ~/.ssh/cloudlab --modes "strongest;weakest;chameleon;chameleonPileus;pileus" --smax "4500" --seeds "7" "config_local.yaml;config_regional.yaml"

<!-- Done -->
bash run_all_cloudlab_sweep.sh kevindmw@clnode317.clemson.cloudlab.us ~/.ssh/cloudlab --modes "pileus;strongest;weakest;chameleon;chameleonPileus" --smax "2250" --seeds "100" "config_local.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode335.clemson.cloudlab.us ~/.ssh/cloudlab --modes "pileus;strongest;weakest;chameleon;chameleonPileus" --smax "4500" --seeds "100" "config_local.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode348.clemson.cloudlab.us ~/.ssh/cloudlab --modes "pileus;strongest;weakest;chameleon;chameleonPileus" --smax "9000" --seeds "100" "config_local.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode330.clemson.cloudlab.us ~/.ssh/cloudlab --modes "pileus;strongest;weakest;chameleon;chameleonPileus" --smax "18000" --seeds "100" "config_local.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode373.clemson.cloudlab.us ~/.ssh/cloudlab --modes "pileus;strongest;weakest;chameleon;chameleonPileus" --smax "1125" --seeds "100" "config_local.yaml"

<!-- Done -->

bash run_all_cloudlab_sweep.sh kevindmw@clnode317.clemson.cloudlab.us ~/.ssh/cloudlab --modes "pileus;strongest;weakest;chameleon;chameleonPileus" --smax "2250" --seeds "100" "config_regional.yaml;config_global.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode335.clemson.cloudlab.us ~/.ssh/cloudlab --modes "pileus;strongest;weakest;chameleon;chameleonPileus" --smax "4500" --seeds "100" "config_regional.yaml;config_global.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode348.clemson.cloudlab.us ~/.ssh/cloudlab --modes "pileus;strongest;weakest;chameleon;chameleonPileus" --smax "9000" --seeds "100" "config_regional.yaml;config_global.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode330.clemson.cloudlab.us ~/.ssh/cloudlab --modes "pileus;strongest;weakest;chameleon;chameleonPileus" --smax "18000" --seeds "100" "config_regional.yaml;config_global.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode373.clemson.cloudlab.us ~/.ssh/cloudlab --modes "pileus;strongest;weakest;chameleon;chameleonPileus" --smax "1125" --seeds "100" "config_regional.yaml;config_global.yaml"
<!-- MongoDB (MONGODB_PORT.md). Same sweep script; the node must carry the patched mongod
     (build_cloudlab.sh). C = 100,000 on the global topology, sMax 450, budget 85,000, pool cap 1,000
     (MONGODB_BASELINES.md 3i). Runs land in runs/config_<loadsweep|global>_mongodb_<mode>_<sMax>_<seed>/.
     The static load sweep is 10 minutes per arm, the day sequence 31; a four-arm pass of both is about 2 h 45 min.
     chameleonPileus ran against mongod in the Utah sweep below without incident. -->

bash run_all_cloudlab_sweep.sh kevindmw@clnode355.clemson.cloudlab.us ~/.ssh/cloudlab --modes "weakest;strongest;pileus;chameleon" --smax "450" --seeds "42" "config_loadsweep_mongodb.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode355.clemson.cloudlab.us ~/.ssh/cloudlab --modes "weakest;strongest;pileus;chameleon" --smax "450" --seeds "42" "config_global_mongodb.yaml"

<!-- MongoDB sMax sensitivity, the same 0.25x to 4x ladder as the Java store's (1125 to 18000 around 4500), around 450.
     One node per sMax value if several nodes are provisioned; on one node, run them in sequence. -->
bash run_all_cloudlab_sweep.sh kevindmw@clnode355.clemson.cloudlab.us ~/.ssh/cloudlab --modes "weakest;strongest;pileus;chameleon" --smax "112.5;225;900;1800" --seeds "42" "config_loadsweep_mongodb.yaml"
bash run_all_cloudlab_sweep.sh kevindmw@clnode355.clemson.cloudlab.us ~/.ssh/cloudlab --modes "weakest;strongest;pileus;chameleon" --smax "112.5;225;900;1800" --seeds "42" "config_global_mongodb.yaml"

<!-- MongoDB repeats under other seeds, as for the Java store -->
bash run_all_cloudlab_sweep.sh kevindmw@clnode355.clemson.cloudlab.us ~/.ssh/cloudlab --modes "weakest;strongest;pileus;chameleon" --smax "450" --seeds "7;100" "config_loadsweep_mongodb.yaml;config_global_mongodb.yaml"

<!-- Utah c6525-25g, one machine per process, 2026-09-24 to 2026-09-27: the sweep RESULTS.md reports.
     Two allocations (hosts.txt for the Java store, hosts_2.txt for mongod), each after setup_cloudlab.sh and,
     for mongod, build_cloudlab.sh. No --smax: the configs carry the calibrated value. 45 runs per store. -->
bash run_all_cloudlab_sweep.sh --hosts hosts.txt --key ~/.ssh/cloudlab --modes "chameleon;chameleonPileus;pileus;strongest;weakest" --seeds "42;7;100" "config_local.yaml;config_regional.yaml;config_global.yaml"
bash run_all_cloudlab_sweep.sh --hosts hosts_2.txt --key ~/.ssh/cloudlab --modes "chameleon;chameleonPileus;pileus;strongest;weakest" --seeds "42;7;100" "config_local_mongodb.yaml;config_regional_mongodb.yaml;config_global_mongodb.yaml"

---

# Distributed runs: one machine per server, plus one for the client

Every script that talks to CloudLab takes either a single host, which is the original layout with the servers and the driver sharing one machine, or `--hosts <file> --key <key>`, which spreads the cluster over one machine per Raft node plus one for the driver.

The single-host layout emulates the geo topology on loopback, so it is cheap to schedule but the machines' CPUs are shared. That is visible in the measurements: mongod's served ceiling is about the same on every topology (112,547 local, 119,512 regional, 121,251 global), which is the machine rather than the store, and on the local topology one mongod becomes a straggler under the contention and every pool checkout timeout in the run lands on it (MONGODB_BASELINES.md 3l). Capacity also does not fall as the topology spreads, because netem delays packets without consuming CPU. A distributed run removes all three confounds, at the cost of an allocation of `numServers + 1` machines.

## The hosts file

See `hosts.example.txt`. One line per machine: the role, the ssh target this laptop uses, and the address the machines use to reach each other on the experiment network. Exactly one `client` line and one `server` line per Raft node, in node order.

## Workflow

```bash
# once per allocation, and again after any reboot: it also sets the experiment
# interface to MTU 9000, which run_all_cloudlab.sh checks; the MongoDB build
# environment goes on the first server only
./setup_cloudlab.sh --hosts hosts.txt --key ~/.ssh/cloudlab --mongodb

# once per change to the chameleon patch; builds on the first server machine and
# copies the binary to the others through the CloudLab project directory
./build_cloudlab.sh --hosts hosts.txt --key ~/.ssh/cloudlab

# once per run
./run_all_cloudlab.sh --hosts hosts.txt --key ~/.ssh/cloudlab <label> config_global_mongodb.yaml

# sweeps take the same two flags in place of the positional host and key
./run_all_cloudlab_sweep.sh --hosts hosts.txt --key ~/.ssh/cloudlab \
    --modes "weakest;strongest;pileus;chameleon" --seeds "42" "config_global_mongodb.yaml"

# after an interrupted run
./clean_cloudlab.sh --hosts hosts.txt --key ~/.ssh/cloudlab
```

## What the runner does differently

`cluster.serverHosts` is rewritten to the machines' experiment-network addresses instead of `127.0.1.N`, and `geo.clientSiteBindHosts` is written into the config with one address per client site, because the loopback `127.0.2.x` convention is not reachable from the server machines. Those per-site addresses are aliases the runner adds to the client machine's experiment interface, from `--site-subnet` (default `10.99.0.0/24`, site k takes `.k+1`), and each server gets a route back to that subnet.

Delays are applied per machine on egress, so each hop's one-way latency is added once by the sender, where the single-host layout puts both directions of every hop on one loopback. The totals match: a five-node, six-site global topology emits 80 one-way rules either way. Only the experiment interface is touched, so ssh on the control interface is unaffected.

Before anything starts, the runner checks every machine's clock against NTP time and refuses to run if one is more than 2 ms off (the error names the machine and the `chronyc makestep` that fixes it). The servers judge each request's age and remaining deadline from the client's wall-clock stamps, so a freshly booted node that is still hundreds of milliseconds fast expires everything that reaches it, and a run on such a cluster completes and looks like a capacity result rather than failing.

The servers start first, each signalling `servers_ready` once its port accepts, and only then does the driver start. Afterwards each machine's artifacts are fetched into the one run directory: the Java store's `occupancy_<i>.csv` and `server_<i>.log` are written on server *i*, as is `mongod_<i>.log`, while under the MongoDB transport the driver polls `serverStatus`, so those occupancy files come from the client. Each machine's `runner.log` is kept as `runner_node<i>.log`, and `hosts.txt` in the run directory records which machine was which node.

## Before trusting a distributed number

Two things are worth measuring once, since neither is checked automatically:

- **The client machine's ceiling.** It now generates the whole offered load over a real NIC rather than loopback, and it is the obvious candidate for the new bottleneck. If it caps below the store, capacity is still measuring the harness, just a different part of it.
- **The baseline round trip.** Loopback adds about 0.02 ms; a CloudLab LAN adds 0.1 to 0.2 ms, against a configured 0.5 ms for a colocated site. Measure it and subtract, or the local topology's shortest hops are systematically long.

All six capacities and budgets in MONGODB_BASELINES.md 3l were measured on the single-host layout and do not carry over.

## Results

RESULTS.md holds every finished run under `results/`, one section per subfolder (failure, mongodb, mongodb_scalability, raft, raft_scalability, random) and one subsection per (type, arm) of the plan with its seed completeness, averaged over the seeds found: a per-phase table, a per-sub-SLA table per phase, and whole-run tables with and without WarmUp and CoolDown, after a table comparing the arms of each type. It is generated: after adding or removing run directories under `results/`, run `python3 results_md.py > RESULTS.md`. `analyze_runs.py` gives the pooled latency percentiles and counters for one config across modes and seeds, and `analyze_run.py` the detail of a single run.
