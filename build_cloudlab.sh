#!/usr/bin/env bash
#
# build_cloudlab.sh - build the patched mongod on a CloudLab node, once per
# change to the chameleon patch (MONGODB_PORT.md). Exports the patch from the
# local mongo/ clone as a bundle, ships it with build_mongod.sh, resets the
# node's ~/mongo checkout (provisioned by setup_cloudlab.sh --mongodb) to a
# clean tree at the pinned commit, applies the bundle, and builds inside a
# remote tmux session that survives ssh drops. The binary lands where the run
# step expects it: ~/tuneable/mongod, stamped with its build id.
#
# build_mongod.sh is idempotent on the build id (pinned commit + patch hash + bazel profile, always opt),
# so re-running this with an unchanged patch costs one ssh round trip.
#
# Usage: ./build_cloudlab.sh <user@host> <ssh-key-path> [--cache DIR | --no-cache] [--force]
#    or: ./build_cloudlab.sh --hosts <hosts.txt> --key <key> [--cache DIR | --no-cache] [--force]
#   --cache DIR  persistent copy of the binary on the node. Without it build_mongod.sh
#                auto-detects the CloudLab project directory (/proj/<project>) and uses
#                <it>/tuneable/mongod-cache, so a reimaged node costs a copy, not a rebuild
#   --no-cache   never read or write a persistent copy
#   --force      rebuild even if a binary with the current build id exists
#
# With --hosts the build happens once, on the first server machine, and the
# binary is then placed on every other server machine: through the shared
# CloudLab project directory where one is mounted, otherwise relayed through
# this laptop. Every server machine ends up with the same build id, which
# run_all_cloudlab.sh checks before it starts a run.
#
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Not a prefix of the run script's session name: tmux has-session matches prefixes.
SESSION="mongod-build"

die() { echo "ERROR: $*" >&2; exit 1; }

# shellcheck source=cloudlab_hosts.sh
source "$REPO_DIR/cloudlab_hosts.sh"

HOST=""
KEY=""
HOSTS_FILE=""
CACHE_DIR=""
NO_CACHE=""
FORCE=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --cache) [[ $# -ge 2 ]] || die "--cache requires a directory"; CACHE_DIR="$2"; shift 2 ;;
        --no-cache) NO_CACHE="--no-cache"; shift ;;
        --force) FORCE="--force"; shift ;;
        --hosts) [[ $# -ge 2 ]] || die "--hosts requires a hosts file"; HOSTS_FILE="$2"; shift 2 ;;
        --hosts=*) HOSTS_FILE="${1#--hosts=}"; shift ;;
        --key) [[ $# -ge 2 ]] || die "--key requires an ssh key path"; KEY="$2"; shift 2 ;;
        --key=*) KEY="${1#--key=}"; shift ;;
        --*) die "unknown flag: $1" ;;
        *) if [[ -z "$HOST" ]]; then HOST="$1"; elif [[ -z "$KEY" ]]; then KEY="$1"; else die "unexpected argument: $1"; fi; shift ;;
    esac
done
[[ -f "${KEY:-}" ]] || die "ssh key not found: ${KEY:-<none given>}"

# The build runs on one machine; OTHER_SERVERS receive a copy of what it makes.
OTHER_SERVERS=()
if [[ -n "$HOSTS_FILE" ]]; then
    [[ -z "$HOST" ]] || die "pass either a positional host or --hosts, not both"
    hosts_load "$HOSTS_FILE"
    HOST="${SERVER_SSH[0]}"
    OTHER_SERVERS=("${SERVER_SSH[@]:1}")
    echo "Building on $HOST, then copying to ${#OTHER_SERVERS[@]} other server machine(s)."
else
    [[ -n "$HOST" ]] || die "usage: ./build_cloudlab.sh <user@host> <ssh-key-path> [--cache DIR] [--force]
   or: ./build_cloudlab.sh --hosts <hosts.txt> --key <key> [--cache DIR] [--force]"
fi
cd "$REPO_DIR"

# %C hashes the connection parameters, so each machine gets its own master
# socket; a fixed path would multiplex every later ssh onto the first
# machine's connection and run it there instead.
SSH_OPTS=(-i "$KEY" -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=30
          -o ControlMaster=auto -o "ControlPath=/tmp/tuneable-build-$$-%C" -o ControlPersist=120)
ssh_run() { ssh "${SSH_OPTS[@]}" "$HOST" "$@"; }

# --- Export the patch locally ---
BUNDLE="$(mktemp -d)/chameleon-mongod.tar.gz"
./mongo_patch_export.sh --out "$BUNDLE"
BASE_COMMIT="$(tar -xzOf "$BUNDLE" base-commit)"

# --- Remote preflight ---
ssh_run "test -d \$HOME/mongo/.git && command -v tmux >/dev/null && (test -x \$HOME/.local/bin/bazel || command -v bazel >/dev/null)" \
    || die "$HOST is not provisioned for MongoDB builds; run: ./setup_cloudlab.sh $HOST $KEY --mongodb"
REMOTE_HEAD="$(ssh_run 'git -C $HOME/mongo rev-parse HEAD')"
[[ "$REMOTE_HEAD" == "$BASE_COMMIT" ]] \
    || die "checkout on $HOST is at ${REMOTE_HEAD:0:12} but the patch is against ${BASE_COMMIT:0:12}; re-run setup_cloudlab.sh --mongodb"
ssh_run "tmux has-session -t =$SESSION 2>/dev/null" \
    && die "a build is already running on $HOST (tmux session '$SESSION'); attach: ssh -i $KEY $HOST -t tmux attach -t $SESSION"

# --- Ship the bundle and the build script ---
echo "Shipping patch bundle (against ${BASE_COMMIT:0:12}) and build_mongod.sh to $HOST..."
ssh_run "mkdir -p \$HOME/tuneable"
scp -q "${SSH_OPTS[@]}" "$BUNDLE" "$HOST:tuneable/chameleon-mongod.tar.gz"
scp -q "${SSH_OPTS[@]}" "$REPO_DIR/build_mongod.sh" "$HOST:tuneable/build_mongod.sh"
scp -q "${SSH_OPTS[@]}" "$REPO_DIR/bindsrc.c" "$HOST:tuneable/bindsrc.c"
rm -rf "$(dirname "$BUNDLE")"

# --- Build inside tmux through a shipped wrapper (no inline quoting) ---
# build_mongod.sh --reset-checkout discards a previous bundle's changes (the
# checkout is a build input, never edited by hand) and applies the new one.
# The wrapper captures everything into build.log and writes the exit code to
# build.done, which this script polls for.
CACHE_ARG="$NO_CACHE"
[[ -n "$CACHE_DIR" ]] && CACHE_ARG="--cache $CACHE_DIR"
WRAPPER="$(mktemp)"
cat > "$WRAPPER" <<EOF
#!/usr/bin/env bash
set -o pipefail
cd "\$HOME"
{
    echo "=== build started \$(date '+%Y-%m-%d %H:%M:%S') on \$(hostname) ==="
    bash "\$HOME/tuneable/build_mongod.sh" --mongo-dir "\$HOME/mongo" \\
        --patch-bundle "\$HOME/tuneable/chameleon-mongod.tar.gz" --out "\$HOME/tuneable" \\
        --reset-checkout $CACHE_ARG $FORCE
    echo "=== build exit \$? at \$(date '+%Y-%m-%d %H:%M:%S') ==="
} 2>&1 | tee "\$HOME/tuneable/build.log"
tail -1 "\$HOME/tuneable/build.log" | sed -E 's/.*build exit ([0-9]+).*/\\1/' > "\$HOME/tuneable/build.done"
EOF
scp -q "${SSH_OPTS[@]}" "$WRAPPER" "$HOST:tuneable/build_remote.sh"
rm -f "$WRAPPER"
ssh_run "rm -f \$HOME/tuneable/build.done \$HOME/tuneable/build.log && chmod +x \$HOME/tuneable/build_remote.sh \
    && tmux new-session -d -s $SESSION -c \"\$HOME\" \"bash \$HOME/tuneable/build_remote.sh\""

echo "Build running on $HOST in tmux session '$SESSION' (attach: ssh -i $KEY $HOST -t tmux attach -t $SESSION)."
echo "Streaming build.log; the first optimized build takes up to an hour, incremental rebuilds minutes."
echo
ssh "${SSH_OPTS[@]}" "$HOST" "touch \$HOME/tuneable/build.log && tail -F \$HOME/tuneable/build.log" &
TAIL_PID=$!
trap 'kill "$TAIL_PID" 2>/dev/null || true' EXIT

while ! ssh_run "test -f \$HOME/tuneable/build.done" 2>/dev/null; do
    ssh_run "tmux has-session -t =$SESSION 2>/dev/null" || break
    sleep 15
done
kill "$TAIL_PID" 2>/dev/null || true
wait "$TAIL_PID" 2>/dev/null || true

BUILD_EXIT="$(ssh_run 'cat $HOME/tuneable/build.done 2>/dev/null || echo missing')"
# The finished session would otherwise linger with its output on screen.
ssh_run "tmux kill-session -t =$SESSION 2>/dev/null || true"
[[ "$BUILD_EXIT" == "0" ]] || die "build failed on $HOST (exit $BUILD_EXIT); see ~/tuneable/build.log there"
BUILD_ID="$(ssh_run 'cat $HOME/tuneable/mongod.build-id')"
echo
echo "mongod ready on $HOST: ~/tuneable/mongod, build id $BUILD_ID"

# --- Place the same binary on the other server machines ---
# Preferred route is the shared CloudLab project directory, which
# build_mongod.sh has just written the binary into: no transfer at all. Where
# no project directory is mounted, the binary comes through this laptop.
# libbindsrc.so is a few KB and is not cached, so it always rides along.
if [[ ${#OTHER_SERVERS[@]} -gt 0 ]]; then
    STAGE="$(mktemp -d)"
    # The build's own EXIT trap has already done its work (the log stream is
    # stopped); keep killing it harmless and add the staging cleanup.
    trap 'kill "$TAIL_PID" 2>/dev/null || true; rm -rf "$STAGE"' EXIT
    scp -q "${SSH_OPTS[@]}" "$HOST:tuneable/libbindsrc.so" "$STAGE/libbindsrc.so"
    for target in "${OTHER_SERVERS[@]}"; do
        echo "Placing mongod $BUILD_ID on $target..."
        ssh "${SSH_OPTS[@]}" "$target" "mkdir -p \$HOME/tuneable"
        scp -q "${SSH_OPTS[@]}" "$STAGE/libbindsrc.so" "$target:tuneable/libbindsrc.so"
        if ssh "${SSH_OPTS[@]}" "$target" BUILD_ID="$BUILD_ID" bash -s <<'REMOTE'
set -euo pipefail
if [[ -x "$HOME/tuneable/mongod" && "$(cat "$HOME/tuneable/mongod.build-id" 2>/dev/null || true)" == "$BUILD_ID" ]]; then
    echo "  already at $BUILD_ID"
    exit 0
fi
# Same auto-detection build_mongod.sh uses: exactly one writable /proj/<project>.
PROJECTS=()
for d in /proj/*/; do [[ -w "$d" ]] && PROJECTS+=("${d%/}"); done
[[ ${#PROJECTS[@]} -eq 1 ]] || exit 3
SRC="${PROJECTS[0]}/tuneable/mongod-cache/$BUILD_ID/mongod"
[[ -x "$SRC" ]] || exit 3
install -m 0755 "$SRC" "$HOME/tuneable/mongod"
echo "$BUILD_ID" > "$HOME/tuneable/mongod.build-id"
echo "  copied from ${PROJECTS[0]}/tuneable/mongod-cache"
REMOTE
        then
            :
        else
            [[ $? -eq 3 ]] || die "failed to place mongod on $target"
            echo "  no shared project cache; relaying the binary through this machine"
            [[ -f "$STAGE/mongod" ]] || scp -q "${SSH_OPTS[@]}" "$HOST:tuneable/mongod" "$STAGE/mongod"
            scp -q "${SSH_OPTS[@]}" "$STAGE/mongod" "$target:tuneable/mongod"
            ssh "${SSH_OPTS[@]}" "$target" "chmod 0755 \$HOME/tuneable/mongod && echo '$BUILD_ID' > \$HOME/tuneable/mongod.build-id"
        fi
    done
    # A mismatch here means a machine kept an older binary, which would split
    # the replica set across two builds without any other symptom.
    for target in "${OTHER_SERVERS[@]}"; do
        got="$(ssh "${SSH_OPTS[@]}" "$target" 'cat $HOME/tuneable/mongod.build-id 2>/dev/null || echo missing')"
        [[ "$got" == "$BUILD_ID" ]] || die "$target ended up with build id '$got', expected '$BUILD_ID'"
    done
    echo "All ${#OTHER_SERVERS[@]} other server machine(s) at build id $BUILD_ID"
fi

if [[ -n "$HOSTS_FILE" ]]; then
    echo "Run experiments with: ./run_all_cloudlab.sh --hosts $HOSTS_FILE --key $KEY <label> config_global_mongodb.yaml"
else
    echo "Run experiments with: ./run_all_cloudlab.sh $HOST $KEY <label> config_global_mongodb.yaml"
fi
