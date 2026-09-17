#!/usr/bin/env bash
#
# cloudlab_remote_run.sh - runs ON the CloudLab node, inside the tmux session
# started by run_all_cloudlab.sh. Launches the servers the config asks for,
# plus the WorkloadDriver from the fat jar, waits for the driver to finish,
# then writes run_info.txt, which the local orchestrator polls for.
#
# transport: framed   one ServerNode JVM per Raft node (the Java store)
# transport: mongodb  one patched mongod per node (MONGODB_PORT.md), from the
#                     binary the build step left at $HOME/tuneable/mongod, then
#                     replica set initiation and the data load through the jar
#
# Usage: cloudlab_remote_run.sh <fat-jar> <config> <numServers> <label>
# Working directory: the remote run directory (set by tmux -c); all CSVs and
# logs land there and are fetched back by run_all_cloudlab.sh.
#
set -euo pipefail

JAR="$1"
CONFIG="$2"
NUM_SERVERS="$3"
LABEL="$4"

echo "=== Experiment starting on $(hostname) (label: $LABEL) ==="
echo "=== Working directory: $(pwd) ==="
echo

# Open-file limit for every process started below. The default soft limit of
# 1024 caps mongod at 409 concurrent connections (80% of half the limit, one
# connection per in-flight command), which the driver pools exceed under load.
# The WorkloadDriver JVM holds one descriptor per pooled socket to every node
# from every site (sites x nodes x maxPoolSize, 120,000 at six sites, five
# nodes and a 4,000 cap), and hitting its limit fails socket opens, which makes
# the driver mark nodes UNKNOWN and clear pools (MONGODB_BASELINES.md 3h).
# 262144 keeps the client out of that regime for any cap the configs use; the
# server-side thread count is bounded by the pool cap, not by this limit.
# Inherited by the children, so it is set once here.
FD_LIMIT=262144
ulimit -n "$FD_LIMIT" || { echo "ERROR: cannot raise the open-file soft limit to $FD_LIMIT (hard limit $(ulimit -Hn))" >&2; exit 2; }
echo "Open-file limit: $(ulimit -Sn)"

CONFIG_INFO="$(java -cp "$JAR" org.example.Utility.ExperimentConfig "$CONFIG")"
cfg() { printf '%s\n' "$CONFIG_INFO" | sed -n "s/^$1=//p"; }
TRANSPORT="$(cfg transport)"

SERVER_PIDS=()
if [[ "$TRANSPORT" == "mongodb" ]]; then
    # The experiment binary unless the config names another (mongodb.binaryPath: the
    # unpatched control or a stock release, with the server hooks off).
    MONGOD="$(cfg mongodbBinaryPath)"
    MONGOD="${MONGOD:-$HOME/tuneable/mongod}"
    MONGOD="${MONGOD/#\~/$HOME}"
    [[ -x "$MONGOD" ]] || { echo "ERROR: $MONGOD missing or not executable; run build_mongod.sh on this host first, or fix mongodb.binaryPath" >&2; exit 2; }
    # Outgoing connections (replication, heartbeats) leave from the node's own address so
    # the geo delay rules match them; mongod itself never binds its client sockets.
    BINDSRC="$HOME/tuneable/libbindsrc.so"
    [[ -f "$BINDSRC" ]] || { echo "ERROR: $BINDSRC missing; run build_cloudlab.sh (build_mongod.sh compiles bindsrc.c)" >&2; exit 2; }
    IFS=',' read -r -a HOSTS <<< "$(cfg serverHosts)"
    PORT="$(cfg mongodbPort)"
    RS="$(cfg mongodbReplicaSet)"
    [[ ${#HOSTS[@]} -eq "$NUM_SERVERS" ]] || { echo "ERROR: serverHosts count ${#HOSTS[@]} != numServers $NUM_SERVERS" >&2; exit 2; }
    for ((i = 0; i < NUM_SERVERS; i++)); do
        DBPATH="$(pwd)/mongo_data/node_$i"
        rm -rf "$DBPATH" && mkdir -p "$DBPATH"
        # --nounixsocket: five mongods on one port would all bind /tmp/mongodb-<port>.sock and
        # race on it (each unlinks a stale file before binding; one loses with EADDRINUSE).
        # --slowOpSampleRate: mongod logs every operation slower than --slowms (100 ms), and
        # under geo most operations on the primary cross that, so unsampled logs reach
        # gigabytes per run. One in a thousand keeps a uniform diagnostic sample. Both are
        # startup options (operationProfiling.*), not setParameter parameters.
        LD_PRELOAD="$BINDSRC" BINDSRC_ADDR="${HOSTS[$i]}" \
        "$MONGOD" --replSet "$RS" --bind_ip "${HOSTS[$i]}" --port "$PORT" --dbpath "$DBPATH" --nounixsocket \
            --slowOpSampleRate 0.001 \
            --logpath "$(pwd)/mongod_$i.log" --logappend --oplogSize 4096 > "mongod_${i}.stdout" 2>&1 &
        SERVER_PIDS+=($!)
    done
    # chameleonEnabled stays at its server default (false) until the WorkloadDriver pushes
    # the parameters; with mongodb.serverHooks: false it never does, and the same command
    # line also starts an unpatched mongod.
    echo "Started $NUM_SERVERS mongod processes (pids: ${SERVER_PIDS[*]}) on port $PORT, replica set $RS"
    # replSetInitiate needs every member listening; startup time differs between
    # binaries (a stock release takes longer than the patched build), so wait for
    # each port rather than racing it.
    for ((i = 0; i < NUM_SERVERS; i++)); do
        for ((t = 0; t < 300; t++)); do
            (exec 3<>"/dev/tcp/${HOSTS[$i]}/$PORT") 2>/dev/null && break
            kill -0 "${SERVER_PIDS[$i]}" 2>/dev/null || { echo "ERROR: mongod ${HOSTS[$i]} exited during startup; see mongod_$i.log" >&2; exit 2; }
            sleep 0.2
        done
        (exec 3<>"/dev/tcp/${HOSTS[$i]}/$PORT") 2>/dev/null \
            || { echo "ERROR: mongod ${HOSTS[$i]}:$PORT not accepting connections after 60 s; see mongod_$i.log" >&2; exit 2; }
    done
    echo "All $NUM_SERVERS mongods accepting connections"
    java -cp "$JAR" org.example.Client.MongoClusterAdmin init "$CONFIG"
    java -cp "$JAR" org.example.Client.MongoClusterAdmin load "$CONFIG"
else
    for ((i = 0; i < NUM_SERVERS; i++)); do
        java -cp "$JAR" org.example.Server.ServerNode "$CONFIG" "$i" > "server_$i.log" 2>&1 &
        SERVER_PIDS+=($!)
    done
    echo "Started $NUM_SERVERS server processes (pids: ${SERVER_PIDS[*]})"
fi

set +e
java -cp "$JAR" org.example.Client.WorkloadDriver "$CONFIG" 2>&1 | tee run.log
RUN_EXIT=${PIPESTATUS[0]}
set -e

# The driver already requested shutdown (Admin service or the shutdown
# command); give the server processes a moment to flush and exit, then escalate.
DEADLINE=$(( $(date +%s) + 20 ))
for pid in "${SERVER_PIDS[@]}"; do
    while kill -0 "$pid" 2>/dev/null && [[ $(date +%s) -lt $DEADLINE ]]; do
        sleep 0.5
    done
    if kill -0 "$pid" 2>/dev/null; then
        echo "Server process $pid still alive, sending SIGTERM"
        kill "$pid" 2>/dev/null || true
        sleep 2
        kill -9 "$pid" 2>/dev/null || true
    fi
done
if [[ "$TRANSPORT" == "mongodb" ]]; then
    # Data files are not results; the fetch step copies the run directory back.
    rm -rf "$(pwd)/mongo_data"
fi

# Written last: the local orchestrator polls for this file.
{
    echo "label:        $LABEL"
    echo "date:         $(date '+%Y-%m-%d %H:%M:%S')"
    echo "exit_code:    $RUN_EXIT"
    echo "host:         $(hostname)"
    echo "java:         $(java -version 2>&1 | head -1)"
    echo "transport:    $TRANSPORT"
    if [[ "$TRANSPORT" == "mongodb" ]]; then
        echo "processes:    $NUM_SERVERS mongod + 1 WorkloadDriver"
        echo "mongod_path:  $MONGOD"
        echo "mongod:       $("$MONGOD" --version 2>/dev/null | head -1)"
        echo "mongod_build: $(cat "$(dirname "$MONGOD")/mongod.build-id" 2>/dev/null || echo "unknown (no build id stamp beside the binary)")"
        echo "server_hooks: $(cfg mongodbServerHooks)"
    else
        echo "processes:    $NUM_SERVERS ServerNode + 1 WorkloadDriver"
    fi
    echo "config:       archived as $CONFIG in this directory"
} > run_info.txt

exit "$RUN_EXIT"
