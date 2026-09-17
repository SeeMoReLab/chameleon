#!/usr/bin/env bash
#
# setup_cloudlab.sh - one-time provisioning of a CloudLab node (Ubuntu 22.04
# or 24.04) so run_all_cloudlab.sh can run experiments on it. Idempotent:
# re-run it after CloudLab reimages the node.
#
# Usage: ./setup_cloudlab.sh <user@host> <ssh-key-path> [--mongodb]
#
# --mongodb additionally provisions everything the MongoDB transport needs
# once per machine (MONGODB_PORT.md): the build dependencies, a checkout of
# mongodb/mongo at the pinned commit under ~/mongo (only that commit's tree,
# not the history), and bazel. The patched mongod itself is built by
# build_cloudlab.sh (once per code change) and started by the run script.
#
set -euo pipefail

die() { echo "ERROR: $*" >&2; exit 1; }

# The upstream commit the chameleon patch is written against (branch chameleon
# in the local mongo/ clone). build_mongod.sh refuses a bundle whose base
# commit differs from the checkout, so change both together.
PINNED_MONGO_COMMIT="6d990baca59ce7cdddded93409e0266d1ac6a45a"

HOST=""
KEY=""
WITH_MONGODB=0
for arg in "$@"; do
    case "$arg" in
        --mongodb) WITH_MONGODB=1 ;;
        --*) die "unknown flag: $arg" ;;
        *) if [[ -z "$HOST" ]]; then HOST="$arg"; elif [[ -z "$KEY" ]]; then KEY="$arg"; else die "unexpected argument: $arg"; fi ;;
    esac
done
[[ -n "$HOST" && -n "$KEY" ]] || die "usage: ./setup_cloudlab.sh <user@host> <ssh-key-path> [--mongodb]"
[[ -f "$KEY" ]] || die "ssh key not found: $KEY"

SSH_OPTS=(-i "$KEY" -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=30)

echo "Provisioning $HOST..."
ssh "${SSH_OPTS[@]}" "$HOST" bash -s <<'REMOTE'
set -euo pipefail
sudo apt-get update -q
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q \
    openjdk-17-jdk-headless tmux iproute2
mkdir -p "$HOME/tuneable/runs"
echo
echo "Installed:"
java -version 2>&1 | head -1
tmux -V
tc -V | head -1
REMOTE

if [[ "$WITH_MONGODB" == 1 ]]; then
    echo
    echo "Provisioning the MongoDB build environment on $HOST (pinned commit ${PINNED_MONGO_COMMIT:0:12})..."
    ssh "${SSH_OPTS[@]}" "$HOST" PINNED_MONGO_COMMIT="$PINNED_MONGO_COMMIT" bash -s <<'REMOTE'
set -euo pipefail
# The bazel build fetches MongoDB's own hermetic compiler toolchain, so only
# the headers it links against and python are needed on the host. gcc compiles
# the small LD_PRELOAD source-binding shim (bindsrc.c) beside mongod.
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q \
    git python3 libcurl4-openssl-dev liblzma-dev gcc

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
echo "Node ready."
echo "  experiments:      ./run_all_cloudlab.sh $HOST $KEY [label] [config.yaml]"
if [[ "$WITH_MONGODB" == 1 ]]; then
    echo "  build mongod:     ./build_cloudlab.sh $HOST $KEY   (once per change to the patch)"
fi
