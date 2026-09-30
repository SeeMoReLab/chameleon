#!/usr/bin/env bash
#
# run_all_cloudlab.sh - run one experiment on CloudLab and fetch the results
# back. Two layouts:
#
#   single host   ./run_all_cloudlab.sh <user@host> <key> [label] [config.yaml]
#                 Servers and driver share one machine; the geo topology is
#                 emulated on its loopback. Cheap to schedule, but the
#                 machine is shared, so measured capacity is the machine's
#                 rather than the store's (MONGODB_BASELINES.md 3l).
#
#   distributed   ./run_all_cloudlab.sh --hosts hosts.txt --key <key> [label] [config.yaml]
#                 One machine per server plus one for the driver, described by
#                 the hosts file (see cloudlab_hosts.sh). The geo topology is
#                 emulated on each machine's experiment interface.
#
# What it does:
#   1. Builds the fat jar locally and validates the config with the same
#      strict loader the experiment uses
#   2. Rewrites cluster.serverHosts for the layout (loopback 127.0.1.N, or the
#      machines' experiment-network addresses), and for a distributed run also
#      geo.clientSiteBindHosts, the per-site source aliases on the client
#   3. Ships the jar, the config, and this runner to every machine
#   4. If geo.enabled, applies the tc/netem delay rules; a distributed run also
#      adds the client's site aliases and the servers' routes back to them
#   5. Starts the servers, waits for them, then runs the driver, all inside
#      remote tmux sessions (they survive ssh drops) while this terminal
#      blocks, streaming run.log
#   6. Clears the network setup and copies every machine's artifacts into
#      local runs/<label>_<stamp>/ for analyze_run.py
#
# On Ctrl-C the experiment keeps running remotely; the printed instructions
# tell you how to attach, clear the setup, and fetch the results by hand.
#
# --jar <path>: use this prebuilt fat jar instead of building one (the
# caller vouches that it matches the working tree). The sweep passes the jar
# it built and validated with, so concurrent sweeps never share a build
# directory and every run executes the exact jar its config was validated
# against.
#
# --seed <n>: override the config's top-level `seed:` (the base PRNG seed
# for workload generation, routing exploration, and election jitter).
# Without the flag the config's own value stands (42 in the repo configs).
#
# --site-subnet <a.b.c.0/24>: distributed runs only. The addresses the client
# machine puts on its experiment interface, one per client site, so the delay
# rules can tell sites apart the way 127.0.2.x does on a single host. Site k
# takes a.b.c.(k+1). Must not overlap the experiment network. Default
# 10.99.0.0/24.
#
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SESSION="tuneable"
# Packets one netem class may hold; netem's own default is 1000. Raising it
# was measured and is harmful: at 50000 the queue stops signalling congestion,
# so TCP never backs off and frames accumulate instead of being dropped. On a
# local dwell that cut netem drops from 0.315% to 0.045% but took tracking at
# 700k from 96.0% to 79.6%, leader dead-on-arrival from 1% to 20%, spread
# dead-on-arrival to the followers (1% -> 60% at 900k), and pushed the age of
# an expired frame from 2.6 s to 22.6 s mean / 89 s p99
# (runs/diag-local_20260920_091141 against runs/diag-local-lim50k_20260920_103939).
# A small queue is doing useful work here. Change it only against a paired
# measurement of frame ages and netem drops.
NETEM_LIMIT=1000

die() { echo "ERROR: $*" >&2; exit 1; }

# How to re-invoke a sibling script for this layout, for the hints below.
target_args() { if [[ -n "${HOSTS_FILE:-}" ]]; then echo "--hosts $HOSTS_FILE --key $KEY"; else echo "$1 $KEY"; fi; }

# shellcheck source=cloudlab_hosts.sh
source "$REPO_DIR/cloudlab_hosts.sh"

JAR_ARG=""
SEED_ARG=""
HOSTS_FILE=""
KEY_ARG=""
SITE_SUBNET="10.99.0.0/24"
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --jar)
            [[ $# -ge 2 ]] || die "--jar requires a path to a prebuilt fat jar"
            JAR_ARG="$2"
            shift 2
            ;;
        --jar=*)
            JAR_ARG="${1#--jar=}"
            shift
            ;;
        --seed)
            [[ $# -ge 2 ]] || die "--seed requires an integer"
            SEED_ARG="$2"
            shift 2
            ;;
        --seed=*)
            SEED_ARG="${1#--seed=}"
            shift
            ;;
        --hosts)
            [[ $# -ge 2 ]] || die "--hosts requires a hosts file"
            HOSTS_FILE="$2"
            shift 2
            ;;
        --hosts=*)
            HOSTS_FILE="${1#--hosts=}"
            shift
            ;;
        --key)
            [[ $# -ge 2 ]] || die "--key requires an ssh key path"
            KEY_ARG="$2"
            shift 2
            ;;
        --key=*)
            KEY_ARG="${1#--key=}"
            shift
            ;;
        --site-subnet)
            [[ $# -ge 2 ]] || die "--site-subnet requires a.b.c.0/24"
            SITE_SUBNET="$2"
            shift 2
            ;;
        --site-subnet=*)
            SITE_SUBNET="${1#--site-subnet=}"
            shift
            ;;
        --*)
            die "unknown flag: $1"
            ;;
        *)
            POSITIONAL+=("$1")
            shift
            ;;
    esac
done

# --hosts takes the ssh key from --key and leaves both positional slots for
# the label and the config; the single-host form keeps its original signature.
if [[ -n "$HOSTS_FILE" ]]; then
    DISTRIBUTED=1
    KEY="$KEY_ARG"
    LABEL="${POSITIONAL[0]:-run}"
    CONFIG_PATH="${POSITIONAL[1]:-$REPO_DIR/config_local.yaml}"
    [[ -n "$KEY" ]] || die "usage: ./run_all_cloudlab.sh --hosts <hosts.txt> --key <ssh-key-path> [label] [config.yaml]"
    [[ ${#POSITIONAL[@]} -le 2 ]] || die "unexpected arguments: ${POSITIONAL[*]:2}"
else
    DISTRIBUTED=0
    HOST="${POSITIONAL[0]:-}"
    KEY="${KEY_ARG:-${POSITIONAL[1]:-}}"
    LABEL="${POSITIONAL[2]:-run}"
    CONFIG_PATH="${POSITIONAL[3]:-$REPO_DIR/config_local.yaml}"
    [[ -n "$HOST" && -n "$KEY" ]] || die "usage: ./run_all_cloudlab.sh <user@host> <ssh-key-path> [label] [config.yaml] [--jar prebuilt-all.jar] [--seed n]
       or: ./run_all_cloudlab.sh --hosts <hosts.txt> --key <ssh-key-path> [label] [config.yaml]"
fi
[[ -f "$KEY" ]] || die "ssh key not found: $KEY"
[[ "$LABEL" =~ ^[A-Za-z0-9._-]+$ ]] || die "label must match [A-Za-z0-9._-]+, got: $LABEL"
CONFIG_PATH="$(cd "$(dirname "$CONFIG_PATH")" 2>/dev/null && pwd)/$(basename "$CONFIG_PATH")" || die "config file not found"
[[ -f "$CONFIG_PATH" ]] || die "config file not found: $CONFIG_PATH"
case "$CONFIG_PATH" in
    *.yaml|*.yml) ;;
    *) die "run_all_cloudlab requires a YAML config (cluster.serverHosts is rewritten for the layout)" ;;
esac
cd "$REPO_DIR"

if [[ "$DISTRIBUTED" == 1 ]]; then
    hosts_load "$HOSTS_FILE"
    [[ "$SITE_SUBNET" =~ ^([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})\.0/24$ ]] \
        || die "--site-subnet must be a.b.c.0/24, got: $SITE_SUBNET"
    SITE_PREFIX="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}.${BASH_REMATCH[3]}"
    # Overlap would make the site aliases unreachable, or hijack a machine's
    # own address; either way it fails deep inside the run rather than here.
    for lan in "${ALL_LAN[@]}"; do
        [[ "${lan%.*}" == "$SITE_PREFIX" ]] \
            && die "--site-subnet $SITE_SUBNET overlaps the experiment network ($lan); pick an unused /24"
    done
    echo "Distributed layout from $HOSTS_FILE:"
    hosts_describe
    echo "  client site aliases: $SITE_PREFIX.1 .. (one per geo.clientSites entry, on the client's experiment interface)"
fi

# --- Rewrite the config for the layout ---
# Single host: cluster.serverHosts becomes distinct literal loopback IPs
# (127.0.1.1 .. 127.0.1.N), which Linux answers natively, and the client sites
# keep the built-in 127.0.2.x convention. Distributed: serverHosts becomes the
# machines' experiment-network addresses, and geo.clientSiteBindHosts is
# written with the client's per-site aliases, since loopback addresses cannot
# be reached from the server machines.
# Any geo.clientSiteBindHosts already in the file is dropped first: it is an
# output of this script, not an input, so a stale one must not survive.
# The .yaml suffix matters: the strict loader dispatches YAML vs JSON on the
# file extension.
if [[ "$DISTRIBUTED" == 1 ]]; then
    HOSTS_CSV="$(IFS=,; echo "${SERVER_LAN[*]}")"
    # One alias per client site; the count comes from the config, so read the
    # site count off the original file before rewriting it.
    SITE_COUNT="$(awk '/^  clientSites:/ { inblock = 1; next } inblock && /^    - / { n++; next } inblock && /^[^ ]/ { exit } END { print n + 0 }' "$CONFIG_PATH")"
    [[ "$SITE_COUNT" -gt 0 ]] || die "could not count geo.clientSites entries in $CONFIG_PATH"
    [[ "$SITE_COUNT" -le 254 ]] || die "geo.clientSites has $SITE_COUNT entries; a /24 of aliases holds 254"
    BIND_LIST=""
    for ((s = 0; s < SITE_COUNT; s++)); do
        BIND_LIST+="${BIND_LIST:+, }$SITE_PREFIX.$((s + 1))"
    done
else
    HOSTS_CSV=""
    BIND_LIST=""
fi
CLOUD_CONFIG="$(mktemp).yaml"
awk -v hosts="$HOSTS_CSV" -v bind="$BIND_LIST" '
    BEGIN { n = split(hosts, H, ",") }
    /^  clientSiteBindHosts:/ { next }
    /^  serverHosts:/ { inblock = 1; i = 0; print; next }
    inblock && /^    - / { i++; print "    - " (n > 0 ? H[i] : "127.0.1." i); next }
    { inblock = 0 }
    /^geo:/ && bind != "" { print; print "  clientSiteBindHosts: [" bind "]"; next }
    { print }
' "$CONFIG_PATH" > "$CLOUD_CONFIG"
trap 'rm -f "$CLOUD_CONFIG" "${CLOUD_CONFIG%.yaml}"' EXIT

# --seed overrides the config's top-level seed: line (without the flag the
# config's own value - 42 in the repo configs - stands).
if [[ -n "$SEED_ARG" ]]; then
    [[ "$SEED_ARG" =~ ^-?[0-9]+$ ]] || die "--seed must be an integer, got: $SEED_ARG"
    [[ "$(grep -Ec '^seed: ' "$CLOUD_CONFIG")" == 1 ]] \
        || die "expected exactly one top-level 'seed:' line in $CONFIG_PATH"
    sed -E -i.bak "s/^seed: .*/seed: $SEED_ARG/" "$CLOUD_CONFIG" && rm -f "$CLOUD_CONFIG.bak"
    grep -q "^seed: $SEED_ARG\$" "$CLOUD_CONFIG" || die "failed to rewrite seed: line"
fi

# One multiplexed ssh connection per machine (auth once, fast polling). %C is
# a hash of the connection parameters, so the machines never share a socket.
SSH_OPTS=(-i "$KEY" -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=30
          -o ControlMaster=auto -o "ControlPath=/tmp/tuneable-cloudlab-$$-%C" -o ControlPersist=120)
ssh_to() { local target="$1"; shift; ssh "${SSH_OPTS[@]}" "$target" "$@"; }
scp_to() { scp -q "${SSH_OPTS[@]}" "$@"; }
if [[ "$DISTRIBUTED" == 0 ]]; then
    ALL_SSH=("$HOST")
    CLIENT_SSH="$HOST"
    SERVER_SSH=()
fi
ssh_run() { ssh_to "$CLIENT_SSH" "$@"; }

# protobuf-java 3.x uses sun.misc.Unsafe, which JDK 24+ warns about; only the
# LOCAL helper invocation needs this (the machines run JDK 17).
JAVA_MAJOR="$(java -version 2>&1 | head -1 | sed -E 's/.*version "([0-9]+).*/\1/')"
JAVA_FLAGS=()
[[ "$JAVA_MAJOR" =~ ^[0-9]+$ && "$JAVA_MAJOR" -ge 24 ]] && JAVA_FLAGS+=(--sun-misc-unsafe-memory-access=allow)

# --- Build the fat jar (or take the caller's prebuilt one) ---
if [[ -n "$JAR_ARG" ]]; then
    [[ -f "$JAR_ARG" ]] || die "prebuilt jar not found: $JAR_ARG"
    JAR="$JAR_ARG"
    echo "Using prebuilt jar: $JAR"
else
    echo "Building fat jar (tests skipped)..."
    mvn -q clean package -Dbuild.dir=target-script -DskipTests
    JAR="$(ls target-script/*-all.jar 2>/dev/null | head -1)"
    [[ -n "$JAR" && -f "$JAR" ]] || die "fat jar not found in target-script/ (expected *-all.jar)"
fi

# --- Validate the rewritten config and read what the orchestration needs ---
CONFIG_INFO="$(java ${JAVA_FLAGS[@]+"${JAVA_FLAGS[@]}"} -cp "$JAR" org.example.Utility.ExperimentConfig "$CLOUD_CONFIG")" \
    || die "config validation failed: $CONFIG_PATH (with cluster.serverHosts rewritten for this layout)"
cfg() { printf '%s\n' "$CONFIG_INFO" | sed -n "s/^$1=//p"; }
NUM_SERVERS="$(cfg numServers)"
DURATION_S="$(cfg durationSeconds)"
GEO_ENABLED="$(cfg geoEnabled)"
TRANSPORT="$(cfg transport)"
[[ "$NUM_SERVERS" =~ ^[0-9]+$ ]] || die "could not read numServers from the config helper"
if [[ "$DISTRIBUTED" == 1 ]]; then
    [[ ${#SERVER_SSH[@]} -eq "$NUM_SERVERS" ]] \
        || die "$HOSTS_FILE lists ${#SERVER_SSH[@]} server machines but $(basename "$CONFIG_PATH") sets cluster.numServers=$NUM_SERVERS"
fi

# --- Remote preflight ---
for target in "${ALL_SSH[@]}"; do
    ssh_to "$target" "command -v java >/dev/null && command -v tmux >/dev/null" \
        || die "$target is missing java or tmux; provision it first: ./setup_cloudlab.sh $target $KEY"
    ssh_to "$target" "tmux has-session -t =$SESSION 2>/dev/null" \
        && die "tmux session '$SESSION' already exists on $target (previous run?). Inspect: ssh -i $KEY $target -t tmux attach -t $SESSION ; or abandon it: ./clean_cloudlab.sh $(target_args "$target")"
done
# Every machine must actually carry the address the hosts file claims, or the
# delay rules and the replica set would be built around an address nobody answers.
if [[ "$DISTRIBUTED" == 1 ]]; then
    for i in "${!ALL_SSH[@]}"; do
        ssh_to "${ALL_SSH[$i]}" "ip -o -4 addr show | grep -qw '${ALL_LAN[$i]}'" \
            || die "${ALL_SSH[$i]} has no interface carrying ${ALL_LAN[$i]} (from $HOSTS_FILE)"
    done
fi
# The experiment interface must carry jumbo frames (setup_cloudlab.sh sets it;
# a reboot resets it). netem queues packets, so at MTU 1500 the far links
# drop under load and the leader loses its followers' heartbeats.
EXPERIMENT_MTU=9000
if [[ "$DISTRIBUTED" == 1 ]]; then
    for i in "${!ALL_SSH[@]}"; do
        mtu="$(ssh_to "${ALL_SSH[$i]}" "cat /sys/class/net/\$(ip -o -4 addr show | awk -v a='${ALL_LAN[$i]}' '\$4 ~ \"^\"a\"/\" {print \$2; exit}')/mtu")"
        [[ "$mtu" == "$EXPERIMENT_MTU" ]] \
            || die "${ALL_SSH[$i]}: experiment interface is at MTU ${mtu:-unknown}, need $EXPERIMENT_MTU; run ./setup_cloudlab.sh --hosts $HOSTS_FILE --key $KEY again (the MTU does not survive a reboot)"
    done
fi
# The servers judge each request's age and remaining deadline from the client's
# wall-clock stamps against their own clock, so machine clocks have to agree to
# well under a deadline. A node that has just booted can be hundreds of ms off
# while chrony slews it back over hours; one such server expired 600k requests/s
# on arrival and, because an instant rejection looks like the fastest replica,
# drew most of the reads to itself - the run completed, with 83% violations, and
# looked like a capacity result. Refuse to start until every clock is within
# CLOCK_TOLERANCE_MS of NTP time; `sudo chronyc makestep` fixes an offender at once.
CLOCK_TOLERANCE_MS=2
if [[ "$DISTRIBUTED" == 1 ]]; then
    for target in "${ALL_SSH[@]}"; do
        offset_s="$(ssh_to "$target" "chronyc tracking 2>/dev/null | awk '/^System time/ {print (\$6 == \"fast\" ? \"\" : \"-\") \$4}'")"
        [[ "$offset_s" =~ ^-?[0-9.]+$ ]] \
            || die "$target: could not read the clock offset from 'chronyc tracking' (got '$offset_s'); is chrony running?"
        offset_ms="$(awk -v s="$offset_s" 'BEGIN { printf "%.3f", (s < 0 ? -s : s) * 1000 }')"
        awk -v o="$offset_ms" -v t="$CLOCK_TOLERANCE_MS" 'BEGIN { exit !(o <= t) }' \
            || die "$target clock is ${offset_ms} ms off NTP time (tolerance ${CLOCK_TOLERANCE_MS} ms); step it with: ssh -i $KEY $target sudo chronyc makestep"
    done
fi
# MongoDB transport (MONGODB_PORT.md): the build step is separate from the run
# step and leaves the patched binary at ~/tuneable/mongod on each server machine.
MONGOD_BUILD_IDS=()
if [[ "$TRANSPORT" == "mongodb" ]]; then
    MONGOD_PATH="$(cfg mongodbBinaryPath)"
    MONGOD_TARGETS=("$CLIENT_SSH")
    [[ "$DISTRIBUTED" == 1 ]] && MONGOD_TARGETS=("${SERVER_SSH[@]}")
    for target in "${MONGOD_TARGETS[@]}"; do
        if [[ -n "$MONGOD_PATH" ]]; then
            ssh_to "$target" "test -x ${MONGOD_PATH/#\~/\$HOME}" \
                || die "$target has no executable at mongodb.binaryPath $MONGOD_PATH"
            MONGOD_BUILD_IDS+=("$target: $(ssh_to "$target" "${MONGOD_PATH/#\~/\$HOME} --version | head -1")")
        else
            ssh_to "$target" "test -x \$HOME/tuneable/mongod" \
                || die "$target has no ~/tuneable/mongod; build it first: ./build_cloudlab.sh $(target_args "$target")"
            MONGOD_BUILD_IDS+=("$target: $(ssh_to "$target" 'cat $HOME/tuneable/mongod.build-id 2>/dev/null || echo "(no build id stamp)"')")
        fi
    done
    printf 'Patched mongod: %s\n' "${MONGOD_BUILD_IDS[@]}"
    # A split cluster of two different builds is a silent experiment error.
    if [[ "$DISTRIBUTED" == 1 ]]; then
        UNIQUE_IDS="$(printf '%s\n' "${MONGOD_BUILD_IDS[@]}" | sed 's/^[^:]*: //' | sort -u | wc -l | tr -d ' ')"
        [[ "$UNIQUE_IDS" == 1 ]] || die "the server machines do not all run the same mongod build; rebuild: ./build_cloudlab.sh --hosts $HOSTS_FILE --key $KEY"
    fi
fi

STAMP="$(date '+%Y%m%d_%H%M%S')"
RUN_NAME="${LABEL}_${STAMP}"
REMOTE_RUN="tuneable/runs/$RUN_NAME"

# --- Ship everything ---
echo "Shipping jar, config, and runner to ${#ALL_SSH[@]} machine(s)..."
for target in "${ALL_SSH[@]}"; do
    ssh_to "$target" "mkdir -p $REMOTE_RUN"
    scp_to "$JAR" "$target:tuneable/app-all.jar"
    scp_to "$REPO_DIR/cloudlab_remote_run.sh" "$target:tuneable/cloudlab_remote_run.sh"
    scp_to "$CLOUD_CONFIG" "$target:$REMOTE_RUN/config.yaml"
done
CONFIG_ARCHIVE="config.yaml"

# --- Network setup: simulated geo latency, and the client's site aliases ---
# Single host: every process shares the loopback, so one rule set on lo keyed
# on (source, destination) pairs covers both directions of every hop. This is
# the layout the calibration in MONGODB_BASELINES.md was measured on and is
# left byte-for-byte as it was.
# Distributed: each machine delays its own egress, so a hop's one-way latency
# is applied once, by the sender. The client's per-site source addresses are
# aliases on its experiment interface, and the servers need a route back to
# them.
NET_APPLIED=0
# Zero unless geo is on, so the loops below and net_clear are no-ops for a
# run with geo.enabled false (a distributed run with no emulated latency needs
# no aliases, no routes and no delay rules).
GEO_NUM=0
GEO_SITES=0
GEO_SITE_HOSTS=()
geo_nonzero() { awk "BEGIN{exit !($1 > 0)}"; }
if [[ "$GEO_ENABLED" == "true" ]]; then
    IFS=',' read -r -a GEO_HOSTS <<< "$(cfg serverHosts)"
    IFS=';' read -r -a GEO_ROWS <<< "$(cfg geoInterServerLatencyMs)"
    IFS=';' read -r -a GEO_SITE_ROWS <<< "$(cfg geoClientSiteLatencyMs)"
    IFS=',' read -r -a GEO_SITE_HOSTS <<< "$(cfg geoClientSiteHosts)"
    GEO_NUM=${#GEO_HOSTS[@]}
    GEO_SITES=${#GEO_SITE_HOSTS[@]}
fi

# Delay classes are shared by delay value rather than created per (source,
# destination) pair. Under one htb root every netem child contends for the same
# lock, and netem's cost per dequeue is amortised over the packets resident
# during one delay: about five at the local topology's 0.3 ms against thousands
# at the global topology's 160 ms. The local topology therefore paid that cost
# almost per packet, once per class, which burned ~10 points of system CPU on
# the client and left requests missing their deadlines in the send queue - 36%
# of writes served at 900k against 100% with a single class
# (runs/diag-local_20260920_091141 against runs/diag-local-1class_20260920_192045).
# The matrices are repetitive - 45 nonzero pairs but 2 distinct delays on local,
# 50 and 14 on global - so sharing by value keeps every delay exact while
# cutting the instance count severalfold.
geo_class_reset() { GEO_CLASS_DELAYS=(); GEO_CLASS_IDS=(); GEO_NEXT_CLASS=2; }

# geo_class_for <file> <device> <delayMs> - set GEO_CLASS_RESULT to the class id
# for this delay, emitting the class and its netem child the first time the
# delay is seen. Returns through a variable rather than stdout on purpose: a
# command substitution would run this in a subshell and discard the map, which
# silently reverts to one class per rule.
geo_class_for() {
    local file="$1" dev="$2" delay="$3" i
    for ((i = 0; i < ${#GEO_CLASS_DELAYS[@]}; i++)); do
        if [[ "${GEO_CLASS_DELAYS[$i]}" == "$delay" ]]; then
            GEO_CLASS_RESULT="${GEO_CLASS_IDS[$i]}"
            return
        fi
    done
    GEO_CLASS_RESULT=$GEO_NEXT_CLASS
    GEO_NEXT_CLASS=$((GEO_NEXT_CLASS + 1))
    GEO_CLASS_DELAYS+=("$delay")
    GEO_CLASS_IDS+=("$GEO_CLASS_RESULT")
    {
        echo "tc class add dev $dev parent 1: classid 1:$GEO_CLASS_RESULT htb rate 100gbit quantum 200000"
        echo "tc qdisc add dev $dev parent 1:$GEO_CLASS_RESULT netem limit $NETEM_LIMIT delay ${delay}ms"
    } >> "$file"
}

# geo_class_count - how many distinct delay classes the last host needed.
geo_class_count() { echo "${#GEO_CLASS_DELAYS[@]}"; }

if [[ "$GEO_ENABLED" == "true" && "$DISTRIBUTED" == 0 ]]; then
    GEO_SCRIPT="$(mktemp)"
    GEO_RULE_INDEX=0
    {
        echo "set -euo pipefail"
        echo "tc qdisc del dev lo root 2>/dev/null || true"
        echo "tc qdisc add dev lo root handle 1: htb default 1"
        echo "tc class add dev lo parent 1: classid 1:1 htb rate 100gbit quantum 200000"
    } > "$GEO_SCRIPT"
    geo_class_reset
    geo_emit() { # srcIp dstIp delayMs
        GEO_RULE_INDEX=$((GEO_RULE_INDEX + 1))
        geo_class_for "$GEO_SCRIPT" lo "$3"
        echo "tc filter add dev lo protocol ip parent 1: prio 1 u32 match ip src $1/32 match ip dst $2/32 flowid 1:$GEO_CLASS_RESULT" >> "$GEO_SCRIPT"
    }
    for ((i = 0; i < GEO_NUM; i++)); do
        IFS=',' read -r -a GEO_ROW <<< "${GEO_ROWS[$i]}"
        for ((j = 0; j < GEO_NUM; j++)); do
            [[ $i -eq $j ]] && continue
            geo_nonzero "${GEO_ROW[$j]}" && geo_emit "${GEO_HOSTS[$i]}" "${GEO_HOSTS[$j]}" "${GEO_ROW[$j]}"
        done
    done
    # Per-site client delays: each client site binds its own 127.0.2.x source
    # IP (a fixed convention printed by the config helper), so the rules can
    # give every (site, server) pair its own one-way delay. 127.0.0.1 stays
    # rule-free for the driver's admin RPCs.
    for ((s = 0; s < GEO_SITES; s++)); do
        IFS=',' read -r -a GEO_SITE_ROW <<< "${GEO_SITE_ROWS[$s]}"
        for ((j = 0; j < GEO_NUM; j++)); do
            if geo_nonzero "${GEO_SITE_ROW[$j]}"; then
                geo_emit "${GEO_SITE_HOSTS[$s]}" "${GEO_HOSTS[$j]}" "${GEO_SITE_ROW[$j]}"
                geo_emit "${GEO_HOSTS[$j]}" "${GEO_SITE_HOSTS[$s]}" "${GEO_SITE_ROW[$j]}"
            fi
        done
    done
    echo "Applying simulated geo latency on $HOST ($GEO_RULE_INDEX delay rules)..."
    scp_to "$GEO_SCRIPT" "$HOST:tuneable/net_apply.sh"
    rm -f "$GEO_SCRIPT"
    ssh_run "sudo bash tuneable/net_apply.sh"
    NET_APPLIED=1
fi

if [[ "$DISTRIBUTED" == 1 ]]; then
    # net_begin <lan-ip> <file>: discover the experiment interface from the
    # address the hosts file declares (the machines also have a control
    # interface, which must stay untouched so ssh survives), then reset it.
    net_begin() {
        cat > "$2" <<EOF
set -euo pipefail
IFACE="\$(ip -o -4 addr show | awk -v a="$1" '\$4 ~ "^"a"/" {print \$2; exit}')"
[[ -n "\$IFACE" ]] || { echo "ERROR: no interface carries $1 on \$(hostname)" >&2; exit 1; }
echo "experiment interface: \$IFACE ($1)"
tc qdisc del dev "\$IFACE" root 2>/dev/null || true
EOF
    }
    net_tc_root() {
        cat >> "$1" <<'EOF'
tc qdisc add dev "$IFACE" root handle 1: htb default 1
tc class add dev "$IFACE" parent 1: classid 1:1 htb rate 100gbit quantum 200000
EOF
    }
    # net_rule <file> <delayMs> <dstIp> [srcIp]
    net_rule() {
        local file="$1" delay="$2" dst="$3" src="${4:-}"
        geo_class_for "$file" '"$IFACE"' "$delay"
        echo "tc filter add dev \"\$IFACE\" protocol ip parent 1: prio 1 u32${src:+ match ip src $src/32} match ip dst $dst/32 flowid 1:$GEO_CLASS_RESULT" >> "$file"
    }

    echo "Applying network setup on ${#ALL_SSH[@]} machine(s)..."
    NET_CLEAR_CMDS=()
    for h in "${!ALL_SSH[@]}"; do
        target="${ALL_SSH[$h]}"
        lan="${ALL_LAN[$h]}"
        apply="$(mktemp)"
        net_begin "$lan" "$apply"
        geo_class_reset
        rules=0
        if [[ "$h" == 0 ]]; then
            # The client machine: one alias per site, then one egress rule per
            # (site, server) pair keyed on the site's source address.
            if [[ "$GEO_ENABLED" == "true" ]]; then
                for ((s = 0; s < GEO_SITES; s++)); do
                    echo "ip addr replace ${GEO_SITE_HOSTS[$s]}/32 dev \"\$IFACE\"" >> "$apply"
                done
                net_tc_root "$apply"
                for ((s = 0; s < GEO_SITES; s++)); do
                    IFS=',' read -r -a GEO_SITE_ROW <<< "${GEO_SITE_ROWS[$s]}"
                    for ((j = 0; j < GEO_NUM; j++)); do
                        if geo_nonzero "${GEO_SITE_ROW[$j]}"; then
                            rules=$((rules + 1))
                            net_rule "$apply" "${GEO_SITE_ROW[$j]}" "${GEO_HOSTS[$j]}" "${GEO_SITE_HOSTS[$s]}"
                        fi
                    done
                done
            fi
        else
            # A server machine: a route back to the site aliases, then egress
            # rules toward the other servers and toward each site.
            i=$((h - 1))
            if [[ "$GEO_ENABLED" == "true" ]]; then
                echo "ip route replace $SITE_SUBNET via $CLIENT_LAN dev \"\$IFACE\"" >> "$apply"
                # Replies whose source is a site alias arrive from the client's
                # address; loose reverse-path filtering accepts them whatever the
                # distribution's default happens to be.
                echo "sysctl -qw net.ipv4.conf.all.rp_filter=2" >> "$apply"
                echo "sysctl -qw net.ipv4.conf.\"\$IFACE\".rp_filter=2" >> "$apply"
                net_tc_root "$apply"
                IFS=',' read -r -a GEO_ROW <<< "${GEO_ROWS[$i]}"
                for ((j = 0; j < GEO_NUM; j++)); do
                    [[ $i -eq $j ]] && continue
                    if geo_nonzero "${GEO_ROW[$j]}"; then
                        rules=$((rules + 1))
                        net_rule "$apply" "${GEO_ROW[$j]}" "${GEO_HOSTS[$j]}"
                    fi
                done
                for ((s = 0; s < GEO_SITES; s++)); do
                    IFS=',' read -r -a GEO_SITE_ROW <<< "${GEO_SITE_ROWS[$s]}"
                    if geo_nonzero "${GEO_SITE_ROW[$i]}"; then
                        rules=$((rules + 1))
                        net_rule "$apply" "${GEO_SITE_ROW[$i]}" "${GEO_SITE_HOSTS[$s]}"
                    fi
                done
            fi
        fi
        scp_to "$apply" "$target:tuneable/net_apply.sh"
        rm -f "$apply"
        echo "  $target: $rules delay rule(s) in $(geo_class_count) class(es)"
        ssh_to "$target" "sudo bash tuneable/net_apply.sh"
    done
    NET_APPLIED=1
fi

# net_clear - undo everything the setup above added, on every machine.
net_clear() {
    local h target lan
    for h in "${!ALL_SSH[@]}"; do
        target="${ALL_SSH[$h]}"
        if [[ "$DISTRIBUTED" == 0 ]]; then
            ssh_to "$target" "sudo tc qdisc del dev lo root 2>/dev/null || true"
            continue
        fi
        lan="${ALL_LAN[$h]}"
        local aliases=""
        if [[ "$h" == 0 ]]; then
            for ((s = 0; s < GEO_SITES; s++)); do
                aliases+="ip addr del ${GEO_SITE_HOSTS[$s]}/32 dev \$IFACE 2>/dev/null || true; "
            done
        else
            aliases="ip route del $SITE_SUBNET via $CLIENT_LAN dev \$IFACE 2>/dev/null || true; "
        fi
        ssh_to "$target" "sudo bash -c 'IFACE=\$(ip -o -4 addr show | awk -v a=$lan '\"'\"'\$4 ~ \"^\"a\"/\" {print \$2; exit}'\"'\"'); tc qdisc del dev \$IFACE root 2>/dev/null || true; $aliases true'" || true
    done
}

# --- Launch the servers, then the driver ---
if [[ "$DISTRIBUTED" == 1 ]]; then
    echo "Starting $NUM_SERVERS server machine(s)..."
    for i in "${!SERVER_SSH[@]}"; do
        ssh_to "${SERVER_SSH[$i]}" "tmux new-session -d -s $SESSION -c \"\$HOME/$REMOTE_RUN\" \
            \"bash \$HOME/tuneable/cloudlab_remote_run.sh \$HOME/tuneable/app-all.jar $CONFIG_ARCHIVE $NUM_SERVERS $LABEL --role servers --server-index $i 2>&1 | tee runner.log\""
    done
    echo "Waiting for the servers to accept connections..."
    for i in "${!SERVER_SSH[@]}"; do
        for ((t = 0; t < 180; t++)); do
            ssh_to "${SERVER_SSH[$i]}" "test -f $REMOTE_RUN/servers_ready" 2>/dev/null && break
            ssh_to "${SERVER_SSH[$i]}" "tmux has-session -t =$SESSION 2>/dev/null" || {
                echo "Server machine ${SERVER_SSH[$i]} (node $i) exited during startup; last lines of its runner.log:" >&2
                ssh_to "${SERVER_SSH[$i]}" "tail -n 20 $REMOTE_RUN/runner.log 2>/dev/null" >&2 || true
                net_clear
                die "node $i failed to start"
            }
            sleep 2
        done
        ssh_to "${SERVER_SSH[$i]}" "test -f $REMOTE_RUN/servers_ready" \
            || { net_clear; die "node $i (${SERVER_SSH[$i]}) not ready after 360 s; see $REMOTE_RUN/runner.log there"; }
        echo "  node $i ready (${SERVER_SSH[$i]})"
    done
    CLIENT_ROLE=(--role client)
else
    CLIENT_ROLE=()
fi

echo "Starting remote tmux session '$SESSION' on $CLIENT_SSH..."
# The runner's own output (server start, replica set init, data load, errors before the
# driver starts) is kept in runner.log beside run.log; the pane alone would lose it.
ssh_run "tmux new-session -d -s $SESSION -c \"\$HOME/$REMOTE_RUN\" \
    \"bash \$HOME/tuneable/cloudlab_remote_run.sh \$HOME/tuneable/app-all.jar $CONFIG_ARCHIVE $NUM_SERVERS $LABEL ${CLIENT_ROLE[*]+"${CLIENT_ROLE[*]}"} 2>&1 | tee runner.log\""

echo
echo "Experiment running on $CLIENT_SSH (label: $LABEL). Attach with: ssh -i $KEY $CLIENT_SSH -t tmux attach -t $SESSION"
echo "Waiting for completion (~${DURATION_S}s)..."
echo

on_interrupt() {
    echo
    echo "Interrupted. The experiment keeps running in tmux session '$SESSION'."
    echo "  attach:        ssh -i $KEY $CLIENT_SSH -t tmux attach -t $SESSION"
    if [[ "$NET_APPLIED" == 1 ]]; then
        if [[ "$DISTRIBUTED" == 1 ]]; then
            echo "  clear network: ./clean_cloudlab.sh --hosts $HOSTS_FILE --key $KEY"
        else
            echo "  clear delays:  ssh -i $KEY $HOST 'sudo tc qdisc del dev lo root'"
        fi
    fi
    echo "  fetch results: scp -i $KEY -r $CLIENT_SSH:$REMOTE_RUN runs/"
    if [[ "$DISTRIBUTED" == 1 ]]; then
        echo "  abandon it:    ./clean_cloudlab.sh --hosts $HOSTS_FILE --key $KEY"
    else
        echo "  abandon it:    ./clean_cloudlab.sh $HOST $KEY   (stops it, clears delays, frees the node for the next run)"
    fi
    exit 130
}
trap on_interrupt INT

# Stream the driver's log while waiting; the poll below is authoritative.
ssh "${SSH_OPTS[@]}" "$CLIENT_SSH" "touch $REMOTE_RUN/run.log && tail -F $REMOTE_RUN/run.log" &
TAIL_PID=$!

while ! ssh_run "test -f $REMOTE_RUN/run_info.txt" 2>/dev/null; do
    ssh_run "tmux has-session -t =$SESSION 2>/dev/null" || break
    sleep 5
done

trap - INT
kill "$TAIL_PID" 2>/dev/null || true
wait "$TAIL_PID" 2>/dev/null || true

# --- Stop the server machines and clear the network setup ---
if [[ "$DISTRIBUTED" == 1 ]]; then
    echo "Stopping the server machines..."
    for i in "${!SERVER_SSH[@]}"; do
        # The driver shuts the cluster down itself; this marker releases a
        # server runner whose children outlived it.
        ssh_to "${SERVER_SSH[$i]}" "touch $REMOTE_RUN/stop_servers" || true
    done
    for i in "${!SERVER_SSH[@]}"; do
        for ((t = 0; t < 30; t++)); do
            ssh_to "${SERVER_SSH[$i]}" "test -f $REMOTE_RUN/servers_done" 2>/dev/null && break
            sleep 2
        done
        ssh_to "${SERVER_SSH[$i]}" "tmux kill-session -t =$SESSION 2>/dev/null || true"
    done
fi
if [[ "$NET_APPLIED" == 1 ]]; then
    echo "Clearing network setup..."
    net_clear
fi

# --- Fetch results ---
if ! ssh_run "test -f $REMOTE_RUN/run_info.txt"; then
    echo "Remote run ended without run_info.txt; last lines of its runner.log:" >&2
    ssh_run "tail -n 15 $REMOTE_RUN/runner.log 2>/dev/null" | grep -vE "INFO org.mongodb|^\s+at " >&2 || true
    die "remote run failed before completion; full logs at $CLIENT_SSH:$REMOTE_RUN"
fi
# The ledgers carry dense latency-histogram columns and compress ~10:1;
# gzip on the machine before the transfer (analyze_run.py reads .csv.gz
# directly, so the files stay compressed locally too).
echo "Compressing CSVs and fetching into runs/$RUN_NAME/..."
mkdir -p "$REPO_DIR/runs"
ssh_run "gzip -f $REMOTE_RUN/*.csv 2>/dev/null || true"
scp -q -r "${SSH_OPTS[@]}" "$CLIENT_SSH:$REMOTE_RUN" "$REPO_DIR/runs/"
if [[ "$DISTRIBUTED" == 1 ]]; then
    # Each server machine holds its own artifacts: the Java store's
    # occupancy_<i>.csv, server_drain_<i>.csv and server_<i>.log are written by
    # the server process, and mongod_<i>.log by mongod. The drain CSV carries
    # the leader-side admission accounting (queue waits, and what the hard cap,
    # the scorer and the replication budget rejected), which is the only record
    # of why a run behaved as it did. (Under the MongoDB transport the driver
    # polls serverStatus, so those occupancy files come from the client.)
    # Index-named, so they merge into one run directory without collision.
    # Streamed as a tar rather than an scp of a glob: the remote shell expands
    # the list, so this does not depend on scp's transfer backend doing it
    # (OpenSSH 9 moved to SFTP, which does not).
    for i in "${!SERVER_SSH[@]}"; do
        ssh_to "${SERVER_SSH[$i]}" "gzip -f $REMOTE_RUN/*.csv 2>/dev/null || true"
        NODE_TMP="$(mktemp -d)"
        ssh_to "${SERVER_SSH[$i]}" \
            "cd \$HOME/$REMOTE_RUN && ls occupancy_*.csv* server_drain_*.csv* server_*.log mongod_*.log mongod_*.stdout runner.log 2>/dev/null | tar czf - -T -" \
            | tar xzf - -C "$NODE_TMP" 2>/dev/null || true
        # runner.log would collide across machines; keep each under its node.
        [[ -f "$NODE_TMP/runner.log" ]] && mv "$NODE_TMP/runner.log" "$NODE_TMP/runner_node$i.log"
        # The client's own copies of any same-named file are not overwritten:
        # only this machine's node-indexed artifacts are moved in.
        cp -n "$NODE_TMP"/* "$REPO_DIR/runs/$RUN_NAME/" 2>/dev/null || true
        rm -rf "$NODE_TMP"
    done
    # The layout is part of the result: which machine was which node, and what
    # each was running. run_info.txt points here.
    {
        echo "layout:       distributed, $NUM_SERVERS server machines + 1 client"
        echo "hosts_file:   $HOSTS_FILE"
        echo "site_subnet:  $SITE_SUBNET"
        hosts_describe
        if [[ ${#MONGOD_BUILD_IDS[@]} -gt 0 ]]; then
            printf 'mongod %s\n' "${MONGOD_BUILD_IDS[@]}"
        fi
    } > "$REPO_DIR/runs/$RUN_NAME/hosts.txt"
fi
# The client's own runner.log is already in place; drop the marker files.
rm -f "$REPO_DIR/runs/$RUN_NAME/servers_ready" "$REPO_DIR/runs/$RUN_NAME/servers_done" \
      "$REPO_DIR/runs/$RUN_NAME/stop_servers"

RUN_EXIT="$(sed -nE 's/^exit_code: *//p' "$REPO_DIR/runs/$RUN_NAME/run_info.txt")"
CSV_COUNT="$(find "$REPO_DIR/runs/$RUN_NAME" -maxdepth 1 \( -name "*.csv" -o -name "*.csv.gz" \) | wc -l | tr -d ' ')"
echo
if [[ "$RUN_EXIT" == "0" ]]; then
    echo "Run complete: $CSV_COUNT CSV files in runs/$RUN_NAME"
    echo "Analyze with: python3 analyze_run.py runs/$RUN_NAME"
else
    echo "Run FAILED (exit code $RUN_EXIT). Partial data ($CSV_COUNT CSV files), run.log and server_<id>.log in runs/$RUN_NAME"
    exit 1
fi
