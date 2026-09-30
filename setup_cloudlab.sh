#!/usr/bin/env bash
#
# setup_cloudlab.sh - one-time provisioning of a CloudLab node (Ubuntu 22.04
# or 24.04) so run_all_cloudlab.sh can run experiments on it. Idempotent:
# re-run it after CloudLab reimages the node.
#
# Usage: ./setup_cloudlab.sh <user@host> <ssh-key-path> [--mongodb]
#    or: ./setup_cloudlab.sh --hosts <hosts.txt> --key <ssh-key-path> [--mongodb]
#
# The second form provisions every machine in a distributed allocation (see
# cloudlab_hosts.sh for the file format), all machines at once; the MongoDB
# build environment below follows on one machine.
#
# --mongodb additionally provisions everything the MongoDB transport needs
# (MONGODB_PORT.md): the build dependencies, a checkout of mongodb/mongo at
# the pinned commit under ~/mongo (only that commit's tree, not the history),
# and bazel. The patched mongod itself is built by build_cloudlab.sh (once per
# code change) and started by the run script.
#
# That build environment costs about 60 GB and an hour, and only one machine
# needs it: build_cloudlab.sh builds there once and copies the binary to the
# other server machines through the CloudLab project directory. So in --hosts
# mode it is provisioned on the first server machine alone.
#
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

die() { echo "ERROR: $*" >&2; exit 1; }

# shellcheck source=cloudlab_hosts.sh
source "$REPO_DIR/cloudlab_hosts.sh"

# The upstream commit the chameleon patch is written against (branch chameleon
# in the local mongo/ clone). build_mongod.sh refuses a bundle whose base
# commit differs from the checkout, so change both together.
PINNED_MONGO_COMMIT="6d990baca59ce7cdddded93409e0266d1ac6a45a"

HOST=""
KEY=""
HOSTS_FILE=""
WITH_MONGODB=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mongodb) WITH_MONGODB=1; shift ;;
        --hosts) [[ $# -ge 2 ]] || die "--hosts requires a hosts file"; HOSTS_FILE="$2"; shift 2 ;;
        --hosts=*) HOSTS_FILE="${1#--hosts=}"; shift ;;
        --key) [[ $# -ge 2 ]] || die "--key requires an ssh key path"; KEY="$2"; shift 2 ;;
        --key=*) KEY="${1#--key=}"; shift ;;
        --*) die "unknown flag: $1" ;;
        *) if [[ -z "$HOST" ]]; then HOST="$1"; elif [[ -z "$KEY" ]]; then KEY="$1"; else die "unexpected argument: $1"; fi; shift ;;
    esac
done
[[ -f "$KEY" ]] || die "ssh key not found: ${KEY:-<none given>}"

# TARGETS: every machine to provision. BUILDER: the one that also gets the
# MongoDB build environment (node 0 in a distributed allocation).
if [[ -n "$HOSTS_FILE" ]]; then
    [[ -z "$HOST" ]] || die "pass either a positional host or --hosts, not both"
    hosts_load "$HOSTS_FILE"
    TARGETS=("${ALL_SSH[@]}")
    TARGET_LAN=("${ALL_LAN[@]}")
    BUILDER="${SERVER_SSH[0]}"
    echo "Provisioning ${#TARGETS[@]} machine(s) from $HOSTS_FILE:"
    hosts_describe
    echo
else
    [[ -n "$HOST" ]] || die "usage: ./setup_cloudlab.sh <user@host> <ssh-key-path> [--mongodb]
   or: ./setup_cloudlab.sh --hosts <hosts.txt> --key <ssh-key-path> [--mongodb]"
    TARGETS=("$HOST")
    TARGET_LAN=("")
    BUILDER="$HOST"
fi

SSH_OPTS=(-i "$KEY" -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=30)

# The base provisioning is the same on every machine and none of it depends on
# another machine, so the hosts are provisioned concurrently: one ssh per host
# in the background, its output prefixed with the host name, and the script
# fails at the end naming every host whose provisioning failed. apt's own lock
# is per machine, so concurrency across machines is safe.
read -r -d '' PROVISION_SCRIPT <<'REMOTE' || true
set -euo pipefail
sudo apt-get update -q
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q \
    openjdk-17-jdk-headless tmux iproute2
mkdir -p "$HOME/tuneable/runs"
# A node that has just booted can be hundreds of ms off NTP time, and chrony
# slews that back over hours. The servers compare client clock stamps against
# their own clock, so step it now; run_all_cloudlab.sh refuses to start on a
# machine that is still off.
sudo chronyc makestep >/dev/null
# Jumbo frames on the experiment interface. The delay emulation queues packets,
# not bytes (run_all_cloudlab.sh, NETEM_LIMIT), so the packet count per byte
# decides when a link drops: at MTU 1500 a 12 MB/s replication stream on a
# 115 ms link keeps about 920 packets queued against the 1,000 limit and drops
# under any burst, which stalled the far follower's stream and cost the leader
# its seat six times in one run. At 9000 the same stream queues about 160. The
# Clemson nodes came up at 9000; the Utah c6525-25g image comes up at 1500. The
# setting does not survive a reboot, and run_all_cloudlab.sh refuses to start
# on an interface that is not at 9000. Single-host runs have no LAN address and
# use loopback, so LAN_IP is empty and this is skipped.
if [[ -n "${LAN_IP:-}" ]]; then
    IFACE="$(ip -o -4 addr show | awk -v a="$LAN_IP" '$4 ~ "^"a"/" {print $2; exit}')"
    [[ -n "$IFACE" ]] || { echo "ERROR: no interface carries $LAN_IP on $(hostname)" >&2; exit 1; }
    sudo ip link set dev "$IFACE" mtu 9000
    MTU="$(cat "/sys/class/net/$IFACE/mtu")"
    [[ "$MTU" == 9000 ]] || { echo "ERROR: $IFACE on $(hostname) is at MTU $MTU after setting 9000" >&2; exit 1; }
    echo "experiment interface $IFACE ($LAN_IP): MTU $MTU"
fi
echo "Installed: $(java -version 2>&1 | head -1); $(tmux -V); $(tc -V | head -1)"
REMOTE

# provision_host <user@host> <lan-ip>: runs PROVISION_SCRIPT there with the
# host's experiment LAN address (empty for a single host), every output line
# prefixed with the host's short name; exits with ssh's status.
provision_host() {
    local target="$1" lan="$2"
    local short="${target#*@}"
    short="${short%%.*}"
    ssh "${SSH_OPTS[@]}" "$target" LAN_IP="$lan" bash -s <<<"$PROVISION_SCRIPT" 2>&1 | sed -u "s/^/[$short] /"
    return "${PIPESTATUS[0]}"
}

echo "Provisioning ${#TARGETS[@]} machine(s) concurrently..."
PIDS=()
for i in "${!TARGETS[@]}"; do
    provision_host "${TARGETS[$i]}" "${TARGET_LAN[$i]}" &
    PIDS+=("$!")
done
FAILED=()
for i in "${!TARGETS[@]}"; do
    wait "${PIDS[$i]}" || FAILED+=("${TARGETS[$i]}")
done
[[ ${#FAILED[@]} -eq 0 ]] || die "provisioning failed on ${#FAILED[@]} machine(s): ${FAILED[*]} (their output is prefixed above)"
echo "Base provisioning done on ${#TARGETS[@]} machine(s)."

if [[ "$WITH_MONGODB" == 1 ]]; then
    echo
    echo "Provisioning the MongoDB build environment on $BUILDER (pinned commit ${PINNED_MONGO_COMMIT:0:12})..."
    ssh "${SSH_OPTS[@]}" "$BUILDER" PINNED_MONGO_COMMIT="$PINNED_MONGO_COMMIT" bash -s <<'REMOTE'
set -euo pipefail
# The bazel build fetches MongoDB's own hermetic compiler toolchain, so only
# the headers it links against and python are needed on the host. gcc compiles
# the small LD_PRELOAD source-binding shim (bindsrc.c) beside mongod. libssl-dev
# is for the vendored s2n (openssl/aes.h); the Clemson image shipped it, the
# Utah image does not.
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q \
    git python3 libcurl4-openssl-dev libssl-dev liblzma-dev gcc

# The root partition of a CloudLab image is about 64 GB and a full mongod
# build with bazel's sandboxing does not fit beside the OS. The rest of the
# boot disk is unallocated; CloudLab's mkextrafs puts a filesystem on it at
# /mydata. The checkout and bazel's output root live there; ~/mongo stays a
# symlink so the build scripts are unaffected.
BUILD_ROOT="/mydata/$USER"
if ! mountpoint -q /mydata; then
    echo "creating /mydata on the unallocated disk space (mkextrafs)"
    sudo mkdir -p /mydata
    sudo /usr/local/etc/emulab/mkextrafs.pl -f /mydata
fi
sudo mkdir -p "$BUILD_ROOT" && sudo chown "$USER" "$BUILD_ROOT"
FREE_GB="$(df -BG --output=avail /mydata | tail -1 | tr -dc '0-9')"
[[ "$FREE_GB" -ge 60 ]] || { echo "ERROR: need about 60 GB free under /mydata for the checkout and build, have ${FREE_GB} GB" >&2; exit 1; }

# bazel output root (downloads, external repos, sandboxes, outputs) on /mydata.
BAZELRC_LINE="startup --output_user_root=$BUILD_ROOT/bazel"
if ! grep -qsF "$BAZELRC_LINE" "$HOME/.bazelrc"; then
    sed -i '/^startup --output_user_root=/d' "$HOME/.bazelrc" 2>/dev/null || true
    echo "$BAZELRC_LINE" >> "$HOME/.bazelrc"
fi
if [[ -d "$HOME/.cache/bazel" ]]; then
    echo "removing the old bazel output root under ~/.cache/bazel ($(du -sh "$HOME/.cache/bazel" | cut -f1)); the new one is $BUILD_ROOT/bazel"
    # bazel writes its output tree read-only; restore write permission before removing.
    chmod -R u+w "$HOME/.cache/bazel"
    rm -rf "$HOME/.cache/bazel"
fi

# The checkout: one commit's tree, no history, on /mydata with ~/mongo pointing at it.
if [[ -d "$HOME/mongo/.git" && ! -L "$HOME/mongo" ]]; then
    echo "moving the existing checkout from ~/mongo to $BUILD_ROOT/mongo"
    rm -rf "$BUILD_ROOT/mongo"
    mv "$HOME/mongo" "$BUILD_ROOT/mongo"
fi
mkdir -p "$BUILD_ROOT/mongo"
[[ -L "$HOME/mongo" ]] || { rm -rf "$HOME/mongo"; ln -s "$BUILD_ROOT/mongo" "$HOME/mongo"; }
cd "$BUILD_ROOT/mongo"
if [[ -d .git ]]; then
    if [[ "$(git rev-parse HEAD)" != "$PINNED_MONGO_COMMIT" ]]; then
        git fetch --depth 1 origin "$PINNED_MONGO_COMMIT"
        git checkout -q --detach FETCH_HEAD
    fi
    echo "mongo checkout present at $(git rev-parse --short HEAD) in $BUILD_ROOT/mongo"
else
    git init -q
    git remote add origin https://github.com/mongodb/mongo.git
    git fetch --depth 1 origin "$PINNED_MONGO_COMMIT"
    git checkout -q --detach FETCH_HEAD
    echo "mongo checked out at $(git rev-parse --short HEAD) in $BUILD_ROOT/mongo"
fi

if [[ ! -x "$HOME/.local/bin/bazel" ]] && ! command -v bazel >/dev/null; then
    python3 buildscripts/install_bazel.py
fi
export PATH="$HOME/.local/bin:$PATH"
echo "bazel: $(bazel --version 2>/dev/null | head -1)"
REMOTE
fi

echo
if [[ -n "$HOSTS_FILE" ]]; then
    echo "${#TARGETS[@]} machines ready."
    echo "  experiments:      ./run_all_cloudlab.sh --hosts $HOSTS_FILE --key $KEY [label] [config.yaml]"
    if [[ "$WITH_MONGODB" == 1 ]]; then
        echo "  build mongod:     ./build_cloudlab.sh --hosts $HOSTS_FILE --key $KEY   (once per change to the patch;"
        echo "                    builds on $BUILDER and copies to the other server machines)"
    fi
else
    echo "Node ready."
    echo "  experiments:      ./run_all_cloudlab.sh $HOST $KEY [label] [config.yaml]"
    if [[ "$WITH_MONGODB" == 1 ]]; then
        echo "  build mongod:     ./build_cloudlab.sh $HOST $KEY   (once per change to the patch)"
    fi
fi
