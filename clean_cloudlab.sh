#!/usr/bin/env bash
#
# clean_cloudlab.sh - return a CloudLab node to the state run_all_cloudlab.sh
# expects, after a run was stopped prematurely (Ctrl-C on the orchestrator,
# a dropped ssh, a driver that hung). Nothing here is needed after a run that
# finished on its own: the remote runner and the orchestrator clean up then.
#
# What a stopped run leaves behind on the node, and what this removes:
#   - the tmux session 'tuneable' (the orchestrator refuses to start while it exists)
#   - the server processes (ServerNode JVMs or patched mongods), the
#     WorkloadDriver, MongoClusterAdmin, and the remote runner shell
#   - the tc/netem delay rules (geo.enabled runs), and on a distributed run
#     also the client's per-site address aliases and the servers' routes to them
#   - mongo_data/ directories under ~/tuneable/runs/*/ (gigabytes, never results)
# It reports, but by default keeps, run directories without run_info.txt
# (partial CSVs and logs of the interrupted run); --purge-incomplete deletes them.
#
# It never touches: finished runs, ~/tuneable/mongod and its build id, the
# bazel cache, the ~/mongo checkout, or the 'mongod-build' tmux session.
#
# Locally it kills the orchestrator's leftover log-streaming ssh (the
# `tail -F run.log` started by run_all_cloudlab.sh survives its Ctrl-C).
#
# Usage: ./clean_cloudlab.sh <user@host> <ssh-key-path> [--dry-run] [--purge-incomplete]
#    or: ./clean_cloudlab.sh --hosts <hosts.txt> --key <key> [--dry-run] [--purge-incomplete]
#   --dry-run           report what would be removed and exit without changing anything
#   --purge-incomplete  also delete run directories that have no run_info.txt
#   --site-subnet       accepted for compatibility; the client site aliases and the servers'
#                       routes to them are found by shape (a /32 and a via-route on the
#                       experiment interface), so leftovers under any subnet are removed
#
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

die() { echo "ERROR: $*" >&2; exit 1; }

# shellcheck source=cloudlab_hosts.sh
source "$REPO_DIR/cloudlab_hosts.sh"

DRY_RUN=0
PURGE_INCOMPLETE=0
HOSTS_FILE=""
KEY=""
SITE_SUBNET="10.99.0.0/24"
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=1; shift ;;
        --purge-incomplete) PURGE_INCOMPLETE=1; shift ;;
        --hosts) [[ $# -ge 2 ]] || die "--hosts requires a hosts file"; HOSTS_FILE="$2"; shift 2 ;;
        --hosts=*) HOSTS_FILE="${1#--hosts=}"; shift ;;
        --key) [[ $# -ge 2 ]] || die "--key requires an ssh key path"; KEY="$2"; shift 2 ;;
        --key=*) KEY="${1#--key=}"; shift ;;
        --site-subnet) [[ $# -ge 2 ]] || die "--site-subnet requires a.b.c.0/24"; SITE_SUBNET="$2"; shift 2 ;;
        --site-subnet=*) SITE_SUBNET="${1#--site-subnet=}"; shift ;;
        --*) die "unknown flag: $1" ;;
        *) POSITIONAL+=("$1"); shift ;;
    esac
done

# TARGETS / TARGET_LAN run in lockstep; an empty LAN entry means "single host,
# the geo rules are on loopback and there are no aliases or routes to undo".
if [[ -n "$HOSTS_FILE" ]]; then
    [[ ${#POSITIONAL[@]} -eq 0 ]] || die "unexpected arguments with --hosts: ${POSITIONAL[*]}"
    [[ -n "$KEY" ]] || die "--hosts needs --key <ssh-key-path>"
    hosts_load "$HOSTS_FILE"
    TARGETS=("${ALL_SSH[@]}")
    TARGET_LAN=("${ALL_LAN[@]}")
    TAIL_HOST="$CLIENT_SSH"
else
    HOST="${POSITIONAL[0]:-}"
    KEY="${KEY:-${POSITIONAL[1]:-}}"
    [[ -n "$HOST" && -n "$KEY" ]] || die "usage: ./clean_cloudlab.sh <user@host> <ssh-key-path> [--dry-run] [--purge-incomplete]
   or: ./clean_cloudlab.sh --hosts <hosts.txt> --key <key> [--dry-run] [--purge-incomplete]"
    [[ ${#POSITIONAL[@]} -le 2 ]] || die "unexpected arguments: ${POSITIONAL[*]:2}"
    TARGETS=("$HOST")
    TARGET_LAN=("")
    TAIL_HOST="$HOST"
fi
[[ -f "$KEY" ]] || die "ssh key not found: $KEY"

SSH_OPTS=(-i "$KEY" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15)

# --- Local leftovers: the orchestrator's log stream for this host ---
# run_all_cloudlab.sh backgrounds `ssh ... tail -F <run>/run.log`; its Ctrl-C
# handler exits without killing it. Match on the host and the tail command.
LOCAL_TAILS="$(pgrep -f "ssh .*${TAIL_HOST}.*tail -F .*tuneable/runs/.*/run.log" || true)"
if [[ -n "$LOCAL_TAILS" ]]; then
    echo "local: leftover log-stream ssh process(es): $(echo "$LOCAL_TAILS" | tr '\n' ' ')"
    if [[ "$DRY_RUN" == 0 ]]; then
        echo "$LOCAL_TAILS" | xargs kill 2>/dev/null || true
        echo "local: killed"
    fi
else
    echo "local: no leftover log-stream ssh processes"
fi

# --- Remote cleanup ---
# The script travels on stdin (`bash -s`), so no remote command line contains
# the process patterns below and pgrep never matches the cleanup itself.
clean_one() {
local t="$1"
echo
echo "remote: connecting to ${TARGETS[$t]}"
ssh "${SSH_OPTS[@]}" "${TARGETS[$t]}" "bash -s -- $DRY_RUN $PURGE_INCOMPLETE '${TARGET_LAN[$t]}' '$SITE_SUBNET'" <<'REMOTE'
set -euo pipefail
DRY_RUN="$1"
PURGE_INCOMPLETE="$2"
LAN="$3"
SITE_SUBNET="$4"
SESSION="tuneable"
# Which interfaces a run may have installed delay rules on: loopback for a
# single-host run, and the experiment interface for a distributed one. The
# machine's control interface is never touched.
NET_IFACES=(lo)
if [[ -n "$LAN" ]]; then
    EXP_IFACE="$(ip -o -4 addr show | awk -v a="$LAN" '$4 ~ "^"a"/" {print $2; exit}')"
    [[ -n "$EXP_IFACE" ]] || { echo "remote: WARNING: no interface carries $LAN; skipping its network cleanup" >&2; }
    [[ -n "$EXP_IFACE" ]] && NET_IFACES+=("$EXP_IFACE")
fi
RUNS_DIR="$HOME/tuneable/runs"
# Exactly the processes the remote runner starts (cloudlab_remote_run.sh) plus
# the runner itself and the orchestrator's remote tail. The mongod pattern
# includes its first flag so a mongod started by hand from elsewhere does not match.
PATTERNS=(
    "tuneable/cloudlab_remote_run.sh"
    "org.example.Client.WorkloadDriver"
    "org.example.Client.MongoClusterAdmin"
    "org.example.Server.ServerNode"
    "tuneable/app-all.jar"
    "tuneable/mongod --replSet"
    "tail -F .*tuneable/runs/.*/run.log"
    # The per-node diagnostic sampler (diag_node.sh) polls top, ss and nstat
    # every 5 s; left running it perturbs the next run.
    "diag_node.sh"
)

list_pids() {
    # The tmux server keeps the launch command in its argv, so it matches the
    # runner pattern; it is excluded because killing it would take every tmux
    # session on the node with it (a running mongod-build included). The
    # session itself is closed with tmux kill-session below.
    local p pid pids=""
    for p in "${PATTERNS[@]}"; do
        pids+="$(pgrep -f -- "$p" || true) "
    done
    for pid in $(echo $pids | tr ' ' '\n' | grep -E '^[0-9]+$' | sort -un); do
        case "$(ps -o comm= -p "$pid" 2>/dev/null)" in
            tmux*) ;;
            *) echo "$pid" ;;
        esac
    done
    return 0
}
delay_ifaces() {
    # An untouched interface shows "qdisc noqueue 0: root" or the default
    # pfifo_fast/mq; net_apply.sh installs an htb root.
    local d
    for d in "${NET_IFACES[@]}"; do
        tc qdisc show dev "$d" 2>/dev/null | grep -qE 'htb|netem' && echo "$d"
    done
    return 0
}
site_aliases() {
    # The per-site source addresses the client machine carries during a
    # distributed run: every /32 on the experiment interface, whatever subnet
    # it is in. Finding them by the configured subnet alone missed the aliases
    # an aborted run left under another subnet, which then hijacked that subnet
    # when the machine served a later run (every connection from the new
    # client's aliases was answered locally and failed).
    [[ -n "$LAN" && -n "${EXP_IFACE:-}" ]] || return 0
    ip -o -4 addr show dev "$EXP_IFACE" | awk '$4 ~ /\/32$/ {print $4}'
    return 0
}
site_routes() {
    # The servers' routes back to a client's site aliases: the only routes
    # through a gateway on the experiment interface, whatever subnet.
    [[ -n "$LAN" && -n "${EXP_IFACE:-}" ]] || return 0
    ip -4 route show dev "$EXP_IFACE" 2>/dev/null | awk '$2 == "via" {print $1}'
    return 0
}
incomplete_runs() {
    local d
    for d in "$RUNS_DIR"/*/; do
        [[ -d "$d" && ! -f "$d/run_info.txt" ]] && echo "${d%/}"
    done
    return 0
}
mongo_data_dirs() {
    local d
    for d in "$RUNS_DIR"/*/mongo_data; do
        [[ -d "$d" ]] && echo "$d"
    done
    return 0
}

# --- Report ---
echo "remote: host $(hostname)"
if tmux has-session -t "=$SESSION" 2>/dev/null; then
    echo "remote: tmux session '$SESSION' exists"
    SESSION_LIVE=1
else
    echo "remote: no tmux session '$SESSION'"
    SESSION_LIVE=0
fi
PIDS="$(list_pids)"
if [[ -n "$PIDS" ]]; then
    echo "remote: run processes:"
    for pid in $PIDS; do
        echo "    $pid  $(ps -o args= -p "$pid" 2>/dev/null | cut -c1-110)"
    done
else
    echo "remote: no run processes"
fi
DELAY_IFACES="$(delay_ifaces)"
if [[ -n "$DELAY_IFACES" ]]; then
    for d in $DELAY_IFACES; do
        echo "remote: tc delay rules on $d: $(tc qdisc show dev "$d" | grep -c netem) netem qdiscs"
    done
else
    echo "remote: no tc delay rules on ${NET_IFACES[*]}"
fi
ALIASES="$(site_aliases)"
if [[ -n "$ALIASES" ]]; then
    echo "remote: client site aliases on ${EXP_IFACE:-}: $(echo $ALIASES)"
else
    echo "remote: no client site aliases"
fi
ROUTES="$(site_routes)"
if [[ -n "$ROUTES" ]]; then
    echo "remote: routes to client site aliases: $(echo $ROUTES)"
else
    echo "remote: no routes to client site aliases"
fi
if [[ -n "${EXP_IFACE:-}" ]]; then
    MTU="$(cat "/sys/class/net/$EXP_IFACE/mtu")"
    [[ "$MTU" == 9000 ]] || echo "remote: WARNING: $EXP_IFACE MTU is $MTU, not 9000; run_all_cloudlab.sh will refuse to start (setup_cloudlab.sh sets it)" >&2
fi
MONGO_DATA="$(mongo_data_dirs)"
if [[ -n "$MONGO_DATA" ]]; then
    echo "remote: mongo_data directories ($(du -shc $MONGO_DATA | tail -1 | cut -f1)):"
    printf '    %s\n' $MONGO_DATA
else
    echo "remote: no mongo_data directories"
fi
INCOMPLETE="$(incomplete_runs)"
if [[ -n "$INCOMPLETE" ]]; then
    echo "remote: run directories without run_info.txt (interrupted runs):"
    printf '    %s\n' $INCOMPLETE
else
    echo "remote: no incomplete run directories"
fi

if [[ "$DRY_RUN" == 1 ]]; then
    echo "remote: dry run, nothing changed"
    exit 0
fi

# --- Stop ---
if [[ "$SESSION_LIVE" == 1 ]]; then
    tmux kill-session -t "=$SESSION"
    echo "remote: killed tmux session '$SESSION'"
fi
if [[ -n "$PIDS" ]]; then
    kill -TERM $PIDS 2>/dev/null || true
    DEADLINE=$(( $(date +%s) + 15 ))
    while [[ -n "$(list_pids)" && $(date +%s) -lt $DEADLINE ]]; do
        sleep 0.5
    done
    LEFT="$(list_pids)"
    if [[ -n "$LEFT" ]]; then
        echo "remote: still alive after SIGTERM, sending SIGKILL: $(echo $LEFT)"
        kill -KILL $LEFT 2>/dev/null || true
        sleep 1
    fi
    echo "remote: stopped run processes"
fi

# --- Network ---
for d in $DELAY_IFACES; do
    sudo tc qdisc del dev "$d" root
    echo "remote: cleared tc delay rules on $d"
done
for a in $ALIASES; do
    sudo ip addr del "$a" dev "$EXP_IFACE"
    echo "remote: removed client site alias $a"
done
for r in $ROUTES; do
    sudo ip route del "$r" dev "$EXP_IFACE"
    echo "remote: removed the route to $r"
done
if [[ -n "${EXP_IFACE:-}" && ( -n "$ALIASES" || -n "$ROUTES" ) ]]; then
    # Neighbour entries learned while the aliases or routes were in place would
    # otherwise outlive them.
    sudo ip neigh flush dev "$EXP_IFACE"
    echo "remote: flushed the neighbour cache on $EXP_IFACE"
fi

# --- Disk ---
if [[ -n "$MONGO_DATA" ]]; then
    rm -rf $MONGO_DATA
    echo "remote: removed mongo_data directories"
fi
if [[ -n "$INCOMPLETE" ]]; then
    if [[ "$PURGE_INCOMPLETE" == 1 ]]; then
        rm -rf $INCOMPLETE
        echo "remote: removed incomplete run directories"
    else
        echo "remote: kept incomplete run directories (pass --purge-incomplete to delete, or scp them back first)"
    fi
fi

# --- Verify ---
FAIL=0
tmux has-session -t "=$SESSION" 2>/dev/null && { echo "remote: VERIFY FAILED: tmux session '$SESSION' still exists" >&2; FAIL=1; }
LEFT="$(list_pids)"
[[ -z "$LEFT" ]] || { echo "remote: VERIFY FAILED: processes still alive: $(echo $LEFT)" >&2; FAIL=1; }
[[ -z "$(delay_ifaces)" ]] || { echo "remote: VERIFY FAILED: tc delay rules still present on $(delay_ifaces)" >&2; FAIL=1; }
[[ -z "$(site_aliases)" ]] || { echo "remote: VERIFY FAILED: client site aliases remain" >&2; FAIL=1; }
[[ -z "$(site_routes)" ]] || { echo "remote: VERIFY FAILED: routes to client site aliases remain" >&2; FAIL=1; }
[[ -z "$(mongo_data_dirs)" ]] || { echo "remote: VERIFY FAILED: mongo_data directories remain" >&2; FAIL=1; }
[[ "$FAIL" == 0 ]] || exit 1
echo "remote: clean; ready for the next run_all_cloudlab.sh"
REMOTE
}

# Every machine at once (each may wait up to 15 s for its processes to exit),
# each into its own file, printed in host order once all have finished.
OUT_DIR="$(mktemp -d)"
trap 'rm -rf "$OUT_DIR"' EXIT
echo
echo "remote: cleaning ${#TARGETS[@]} machine(s) in parallel"
JOBS=()
for t in "${!TARGETS[@]}"; do
    clean_one "$t" > "$OUT_DIR/$t" 2>&1 &
    JOBS+=("$!")
done
FAILED=()
for t in "${!TARGETS[@]}"; do
    wait "${JOBS[$t]}" || FAILED+=("${TARGETS[$t]}")
done
for t in "${!TARGETS[@]}"; do
    cat "$OUT_DIR/$t"
done

[[ ${#FAILED[@]} -eq 0 ]] || die "cleanup did not verify on: ${FAILED[*]} (see above)"
