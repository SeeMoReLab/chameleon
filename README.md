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

## Simulated Node Failure

Enable:

```java
private static final boolean ENABLE_NODE_NETWORK_FAILURE = true;
```

Choose failed node:

```java
private static final int FAILED_NODE_ID = 0;
```

This simulates node failure by dropping inter-server RPCs.

---

## Timed Failure Injection

Enable:

```java
private static final boolean ENABLE_TIMED_NODE_FAILURE = true;
```

You can fail:

* leader
* follower

after:

```java
private static final int FAILURE_AFTER_SECONDS = 40;
```

Useful for:

* recovery testing
* liveness evaluation
* re-election experiments

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