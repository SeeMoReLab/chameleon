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
#   - the tc/netem delay rules on loopback (geo.enabled runs)
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
#   --dry-run           report what would be removed and exit without changing anything
#   --purge-incomplete  also delete run directories that have no run_info.txt
#
set -euo pipefail

die() { echo "ERROR: $*" >&2; exit 1; }

DRY_RUN=0
PURGE_INCOMPLETE=0
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=1; shift ;;
        --purge-incomplete) PURGE_INCOMPLETE=1; shift ;;
        --*) die "unknown flag: $1" ;;
        *) POSITIONAL+=("$1"); shift ;;
    esac
done
HOST="${POSITIONAL[0]:-}"
KEY="${POSITIONAL[1]:-}"
[[ -n "$HOST" && -n "$KEY" ]] || die "usage: ./clean_cloudlab.sh <user@host> <ssh-key-path> [--dry-run] [--purge-incomplete]"
[[ -f "$KEY" ]] || die "ssh key not found: $KEY"
[[ ${#POSITIONAL[@]} -le 2 ]] || die "unexpected arguments: ${POSITIONAL[*]:2}"

SSH_OPTS=(-i "$KEY" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15)

# --- Local leftovers: the orchestrator's log stream for this host ---
# run_all_cloudlab.sh backgrounds `ssh ... tail -F <run>/run.log`; its Ctrl-C
# handler exits without killing it. Match on the host and the tail command.
LOCAL_TAILS="$(pgrep -f "ssh .*${HOST}.*tail -F .*tuneable/runs/.*/run.log" || true)"
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
echo "remote: connecting to $HOST"
ssh "${SSH_OPTS[@]}" "$HOST" "bash -s -- $DRY_RUN $PURGE_INCOMPLETE" <<'REMOTE'
set -euo pipefail
DRY_RUN="$1"
PURGE_INCOMPLETE="$2"
SESSION="tuneable"
RUNS_DIR="$HOME/tuneable/runs"
# Exactly the processes the remote runner starts (cloudlab_remote_run.sh) plus
# the runner itself and the orchestrator's remote tail. The mongod pattern
# includes its first flag so a mongod started by hand from elsewhere does not match.
PATTERNS=(
    "tuneable/cloudlab_remote_run.sh"
    "org.example.Client.WorkloadDriver"
    "org.example.Client.MongoClusterAdmin"
    "org.example.Server.ServerNode"
    "tuneable/mongod --replSet"
    "tail -F .*tuneable/runs/.*/run.log"
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
has_delay_rules() {
    # An untouched loopback shows "qdisc noqueue 0: root"; geo_apply.sh installs an htb root.
    tc qdisc show dev lo | grep -q 'htb'
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
if has_delay_rules; then
    echo "remote: tc delay rules on lo: $(tc qdisc show dev lo | grep -c netem) netem qdiscs"
    DELAYS=1
else
    echo "remote: no tc delay rules on lo"
    DELAYS=0
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
if [[ "$DELAYS" == 1 ]]; then
    sudo tc qdisc del dev lo root
    echo "remote: cleared tc delay rules on lo"
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
has_delay_rules && { echo "remote: VERIFY FAILED: tc delay rules still present on lo" >&2; FAIL=1; }
[[ -z "$(mongo_data_dirs)" ]] || { echo "remote: VERIFY FAILED: mongo_data directories remain" >&2; FAIL=1; }
[[ "$FAIL" == 0 ]] || exit 1
echo "remote: clean; ready for the next run_all_cloudlab.sh"
REMOTE
