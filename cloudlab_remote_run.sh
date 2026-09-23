#!/usr/bin/env bash
#
# cloudlab_remote_run.sh - runs ON a CloudLab node, inside the tmux session
# started by run_all_cloudlab.sh. One script, three roles, so the fd limit,
# the mongod command line and the shutdown escalation are written once:
#
#   --role all      everything on this machine: the servers plus the
#                   WorkloadDriver. The single-host layout, and the default.
#   --role servers  only the servers this machine owns (--server-index i,
#                   repeatable). Signals readiness with servers_ready, then
#                   waits for its children to exit or for the stop_servers
#                   marker the orchestrator drops after the driver finishes.
#   --role client   only the driver: replica set initiation and the data load
#                   for the MongoDB transport, then the WorkloadDriver, then
#                   run_info.txt, which the local orchestrator polls for.
#
# transport: framed   one ServerNode JVM per Raft node (the Java store)
# transport: mongodb  one patched mongod per node (MONGODB_PORT.md), from the
#                     binary the build step left at $HOME/tuneable/mongod
#
# Usage: cloudlab_remote_run.sh <fat-jar> <config> <numServers> <label>
#                               [--role all|servers|client] [--server-index i]
# Working directory: the remote run directory (set by tmux -c); all CSVs and
# logs land there and are fetched back by run_all_cloudlab.sh.
#
set -euo pipefail

JAR="$1"
CONFIG="$2"
NUM_SERVERS="$3"
LABEL="$4"
shift 4

ROLE="all"
SERVER_INDICES=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --role) ROLE="$2"; shift 2 ;;
        --server-index) SERVER_INDICES+=("$2"); shift 2 ;;
        *) echo "ERROR: unknown argument: $1" >&2; exit 2 ;;
    esac
done
case "$ROLE" in
    all)
        [[ ${#SERVER_INDICES[@]} -eq 0 ]] || { echo "ERROR: --server-index is meaningless with --role all" >&2; exit 2; }
        for ((i = 0; i < NUM_SERVERS; i++)); do SERVER_INDICES+=("$i"); done ;;
    servers)
        [[ ${#SERVER_INDICES[@]} -gt 0 ]] || { echo "ERROR: --role servers needs at least one --server-index" >&2; exit 2; } ;;
    client)
        [[ ${#SERVER_INDICES[@]} -eq 0 ]] || { echo "ERROR: --server-index is meaningless with --role client" >&2; exit 2; } ;;
    *) echo "ERROR: --role must be all, servers or client; got '$ROLE'" >&2; exit 2 ;;
esac

echo "=== Experiment $ROLE starting on $(hostname) (label: $LABEL) ==="
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
IFS=',' read -r -a HOSTS <<< "$(cfg serverHosts)"
[[ ${#HOSTS[@]} -eq "$NUM_SERVERS" ]] || { echo "ERROR: serverHosts count ${#HOSTS[@]} != numServers $NUM_SERVERS" >&2; exit 2; }


# wait_for_port <host> <port> <label> [pid]
# replSetInitiate needs every member listening, and startup time differs
# between binaries (a stock release takes longer than the patched build), so
# wait for each port rather than racing it. With a pid, a child that dies
# during startup is reported immediately instead of after the full timeout.
wait_for_port() {
    local host="$1" port="$2" what="$3" pid="${4:-}" t
    for ((t = 0; t < 300; t++)); do
        (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null && return 0
        if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
            echo "ERROR: $what exited during startup; see its log in $(pwd)" >&2
            return 1
        fi
        sleep 0.2
    done
    echo "ERROR: $what ($host:$port) not accepting connections after 60 s" >&2
    return 1
}

SERVER_PIDS=()
if [[ "$ROLE" != "client" ]]; then
    if [[ "$TRANSPORT" == "mongodb" ]]; then
        # The experiment binary unless the config names another (mongodb.binaryPath: the
        # unpatched control or a stock release, with the server hooks off).
        MONGOD="$(cfg mongodbBinaryPath)"
        MONGOD="${MONGOD:-$HOME/tuneable/mongod}"
        MONGOD="${MONGOD/#\~/$HOME}"
        [[ -x "$MONGOD" ]] || { echo "ERROR: $MONGOD missing or not executable; run build_mongod.sh on this host first, or fix mongodb.binaryPath" >&2; exit 2; }
        # Outgoing connections (replication, heartbeats) leave from the node's own address so
        # the geo delay rules match them; mongod itself never binds its client sockets. On a
        # distributed run the routing table would usually pick the same address, but the
        # machines have a second interface, so the source stays pinned rather than inferred.
        BINDSRC="$HOME/tuneable/libbindsrc.so"
        [[ -f "$BINDSRC" ]] || { echo "ERROR: $BINDSRC missing; run build_cloudlab.sh (build_mongod.sh compiles bindsrc.c)" >&2; exit 2; }
        PORT="$(cfg mongodbPort)"
        RS="$(cfg mongodbReplicaSet)"
        for i in "${SERVER_INDICES[@]}"; do
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
        echo "Started ${#SERVER_INDICES[@]} mongod process(es) for node(s) ${SERVER_INDICES[*]} (pids: ${SERVER_PIDS[*]}) on port $PORT, replica set $RS"
        for n in "${!SERVER_INDICES[@]}"; do
            i="${SERVER_INDICES[$n]}"
            wait_for_port "${HOSTS[$i]}" "$PORT" "mongod node $i" "${SERVER_PIDS[$n]}" || exit 2
        done
        echo "mongod node(s) ${SERVER_INDICES[*]} accepting connections"
    else
        for i in "${SERVER_INDICES[@]}"; do
            java -cp "$JAR" org.example.Server.ServerNode "$CONFIG" "$i" > "server_$i.log" 2>&1 &
            SERVER_PIDS+=($!)
        done
        echo "Started ${#SERVER_INDICES[@]} server process(es) for node(s) ${SERVER_INDICES[*]} (pids: ${SERVER_PIDS[*]})"
    fi
fi

# stop_children [grace] - give the server processes `grace` seconds to exit on
# their own, then escalate. With the driver on this machine it has already
# requested shutdown and they are on their way out, so the default grace is
# generous. On a server machine of a distributed run the request never lands:
# mongod refuses `shutdown` from a non-localhost client when auth is off, so
# waiting the full grace is dead time and SIGTERM, which mongod treats as a
# clean shutdown, is the actual stop signal.
stop_children() {
    local deadline=$(( $(date +%s) + ${1:-20} )) pid
    for pid in ${SERVER_PIDS[@]+"${SERVER_PIDS[@]}"}; do
        while kill -0 "$pid" 2>/dev/null && [[ $(date +%s) -lt $deadline ]]; do
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
}

if [[ "$ROLE" == "servers" ]]; then
    # The orchestrator polls for this before it starts the driver.
    echo "ready" > servers_ready
    # Hold the machine until the driver has shut these down, or until the
    # orchestrator drops stop_servers (a driver that died without shutting the
    # cluster down would otherwise leave these running).
    while true; do
        alive=0
        for pid in ${SERVER_PIDS[@]+"${SERVER_PIDS[@]}"}; do
            kill -0 "$pid" 2>/dev/null && alive=1
        done
        [[ "$alive" == 0 ]] && { echo "All local servers exited"; break; }
        [[ -f stop_servers ]] && { echo "stop_servers marker seen"; break; }
        sleep 1
    done
    stop_children 2
    echo "done" > servers_done
    # A server machine's work ends here; without this it would fall through into
    # the client path below and try to start a second WorkloadDriver.
    exit 0
fi

if [[ "$TRANSPORT" == "mongodb" ]]; then
    PORT="$(cfg mongodbPort)"
    # --role client: the orchestrator has already waited for servers_ready on
    # every server machine, but the replica set commands below fail unhelpfully
    # against a port that is not up, so check rather than assume.
    for ((i = 0; i < NUM_SERVERS; i++)); do
        wait_for_port "${HOSTS[$i]}" "$PORT" "mongod node $i" || exit 2
    done
    echo "All $NUM_SERVERS mongods accepting connections"
    java -cp "$JAR" org.example.Client.MongoClusterAdmin init "$CONFIG"
    java -cp "$JAR" org.example.Client.MongoClusterAdmin load "$CONFIG"
fi

set +e
java -cp "$JAR" org.example.Client.WorkloadDriver "$CONFIG" 2>&1 | tee run.log
RUN_EXIT=${PIPESTATUS[0]}
set -e

stop_children

# Written last: the local orchestrator polls for this file.
{
    echo "label:        $LABEL"
    echo "date:         $(date '+%Y-%m-%d %H:%M:%S')"
    echo "exit_code:    $RUN_EXIT"
    echo "host:         $(hostname)"
    echo "role:         $ROLE"
    echo "java:         $(java -version 2>&1 | head -1)"
    echo "transport:    $TRANSPORT"
    if [[ "$TRANSPORT" == "mongodb" ]]; then
        if [[ "$ROLE" == "all" ]]; then
            echo "processes:    $NUM_SERVERS mongod + 1 WorkloadDriver"
            echo "mongod_path:  $MONGOD"
            echo "mongod:       $("$MONGOD" --version 2>/dev/null | head -1)"
            echo "mongod_build: $(cat "$(dirname "$MONGOD")/mongod.build-id" 2>/dev/null || echo "unknown (no build id stamp beside the binary)")"
        else
            echo "processes:    1 WorkloadDriver (the $NUM_SERVERS mongods run on their own machines)"
            echo "mongod_build: recorded by the orchestrator in hosts.txt beside this file"
        fi
        echo "server_hooks: $(cfg mongodbServerHooks)"
    elif [[ "$ROLE" == "all" ]]; then
        echo "processes:    $NUM_SERVERS ServerNode + 1 WorkloadDriver"
    else
        echo "processes:    1 WorkloadDriver (the $NUM_SERVERS ServerNodes run on their own machines)"
    fi
    echo "config:       archived as $CONFIG in this directory"
} > run_info.txt

exit "$RUN_EXIT"
