#!/usr/bin/env bash
# Build the patched mongod (MONGODB_PORT.md, Milestone 0) idempotently.
#
# The build id is the pinned upstream commit, a hash of the chameleon patch
# (every tracked change on top of that commit, plus untracked files under
# src/mongo/db/chameleon), and the bazel profile. The script skips the build
# when a mongod stamped with the current id already exists, first locally,
# then in the persistent cache directory, so a reused CloudLab instance costs
# nothing and a fresh one costs a copy. Only a changed patch, commit, or
# profile rebuilds.
#
# The profile is always the release one, --config=opt. MongoDB's default bazel
# profile (fastbuild) is an unoptimized debug build: opt=off, dbg=True, debug
# assertions on, 1.6 GB binary. Measured on CloudLab it spends about 0.65 ms of
# CPU per point read, roughly ten times a release build, which makes every
# capacity number meaningless. Never build the experiment binary without it.
#
# Usage: ./build_mongod.sh [--mongo-dir DIR] [--out DIR] [--cache DIR] [--patch-bundle FILE] [--force]
#   --mongo-dir     the MongoDB source checkout (default: ./mongo)
#   --out           where the built binary and its stamp land (default: ./target-mongod;
#                   the CloudLab run step expects $HOME/tuneable)
#   --cache         persistent copy location. Default: $MONGOD_CACHE_DIR if set, else
#                   /proj/<project>/tuneable/mongod-cache when exactly one writable CloudLab
#                   project directory is mounted, else none (announced either way)
#   --no-cache      never read or write a persistent copy
#   --reset-checkout  with --patch-bundle, discard local changes in the checkout first (they
#                   are the previous bundle; build_cloudlab.sh always passes this)
#   --patch-bundle  apply a bundle from mongo_patch_export.sh to the checkout first: the
#                   checkout must be clean and at the bundle's base commit (clone
#                   mongodb/mongo and `git checkout <commit>` on the host)
#   --force         rebuild even when a matching binary exists
#
# Runs on the host. Provisioning (build dependencies, the ~/mongo checkout at
# the pinned commit, bazel) is setup_cloudlab.sh --mongodb, once per machine;
# this script only verifies it and fails explicitly otherwise. Driven remotely
# by build_cloudlab.sh, once per change to the patch. The first optimized
# build takes up to an hour on a many-core host; incremental rebuilds after a
# patch change take minutes as long as bazel's output root survives.
set -euo pipefail

die() { echo "build_mongod.sh: $*" >&2; exit 1; }

MONGO_DIR="$(pwd)/mongo"
OUT_DIR="$(pwd)/target-mongod"
CACHE_DIR="${MONGOD_CACHE_DIR:-}"
NO_CACHE=0
PATCH_BUNDLE=""
FORCE=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mongo-dir) MONGO_DIR="$2"; shift 2 ;;
        --out) OUT_DIR="$2"; shift 2 ;;
        --cache) CACHE_DIR="$2"; shift 2 ;;
        --no-cache) NO_CACHE=1; shift ;;
        --patch-bundle) PATCH_BUNDLE="$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"; shift 2 ;;
        --reset-checkout) RESET_CHECKOUT=1; shift ;;
        --force) FORCE=1; shift ;;
        *) die "unknown argument: $1" ;;
    esac
done
RESET_CHECKOUT="${RESET_CHECKOUT:-0}"

# --- Cache location: explicit, else the CloudLab project directory ---
# CloudLab mounts the persistent per-project NFS directory at /proj/<project>
# on every node of an experiment and it survives reimaging, which is exactly
# what the cache is for. Auto-detected when exactly one writable project
# directory is mounted; ambiguity or absence means no cache, said out loud.
if [[ "$NO_CACHE" == 1 ]]; then
    CACHE_DIR=""
    echo "cache: disabled (--no-cache)"
elif [[ -n "$CACHE_DIR" ]]; then
    echo "cache: $CACHE_DIR"
else
    PROJECTS=()
    shopt -s nullglob
    for candidate in /proj/*/; do
        [[ -d "$candidate" && -w "$candidate" ]] && PROJECTS+=("${candidate%/}")
    done
    shopt -u nullglob
    if [[ ${#PROJECTS[@]} -eq 1 ]]; then
        CACHE_DIR="${PROJECTS[0]}/tuneable/mongod-cache"
        echo "cache: $CACHE_DIR (auto-detected CloudLab project directory)"
    elif [[ ${#PROJECTS[@]} -eq 0 ]]; then
        echo "cache: none (no writable /proj/<project> mounted; pass --cache DIR to keep binaries across reimages)"
    else
        echo "cache: none (several writable project directories: ${PROJECTS[*]}; pass --cache DIR to choose)"
    fi
fi

[[ -d "$MONGO_DIR/.git" ]] || die "not a git checkout: $MONGO_DIR"
[[ "$(uname -s)" == "Linux" ]] || die "mongod for the experiment must be built on Linux (got $(uname -s))"

cd "$MONGO_DIR"

# --- Optional: apply the exported patch bundle to a clean checkout ---
if [[ -n "$PATCH_BUNDLE" ]]; then
    [[ -f "$PATCH_BUNDLE" ]] || die "patch bundle not found: $PATCH_BUNDLE"
    STAGE="$(mktemp -d)"
    tar -C "$STAGE" -xzf "$PATCH_BUNDLE"
    BUNDLE_BASE="$(cat "$STAGE/base-commit")"
    [[ "$(git rev-parse HEAD)" == "$BUNDLE_BASE" ]] \
        || die "checkout is at $(git rev-parse --short HEAD) but the bundle needs ${BUNDLE_BASE:0:12}: run git checkout $BUNDLE_BASE"
    if [[ -n "$(git status --porcelain)" ]]; then
        if [[ "$RESET_CHECKOUT" == 1 ]]; then
            # The checkout is a build input, never edited by hand: what is here is a
            # previous bundle, superseded by this one.
            git checkout -q -- .
            git clean -fdq src/mongo/db/chameleon
            echo "reset checkout to a clean tree at $(git rev-parse --short HEAD)"
        else
            die "checkout has local changes; pass --reset-checkout to discard them (they are a previous bundle) or clean the tree by hand"
        fi
    fi
    git apply --check "$STAGE/tracked.patch" || die "bundle does not apply cleanly to ${BUNDLE_BASE:0:12}"
    git apply "$STAGE/tracked.patch"
    cp -R "$STAGE/new/src/mongo/db/chameleon" src/mongo/db/
    rm -rf "$STAGE"
    echo "applied patch bundle $(basename "$PATCH_BUNDLE") on ${BUNDLE_BASE:0:12}"
fi

# --- Build id: upstream commit + patch hash ---
BASE_COMMIT="$(git rev-parse HEAD)"
PATCH_HASH="$( { git diff HEAD; git ls-files --others --exclude-standard -z src/mongo/db/chameleon \
    | sort -z | xargs -0 -r cat; } | sha256sum | cut -c1-16)"
BAZEL_PROFILE="opt"
BUILD_ID="${BASE_COMMIT:0:12}-${PATCH_HASH}-${BAZEL_PROFILE}"
STAMP_NAME="mongod.build-id"

have_binary() {
    local dir="$1"
    [[ -x "$dir/mongod" && -f "$dir/$STAMP_NAME" ]] && [[ "$(cat "$dir/$STAMP_NAME")" == "$BUILD_ID" ]]
}

mkdir -p "$OUT_DIR"

# --- Source-binding shim (bindsrc.c): outgoing mongod connections take the node's own
# address so the geo delay rules apply to replication and heartbeats. Compiled on every
# invocation (well under a second) so it is never stale relative to its source.
SHIM_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/bindsrc.c"
[[ -f "$SHIM_SRC" ]] || die "bindsrc.c not found beside build_mongod.sh"
command -v cc >/dev/null || die "cc not found; provision the host with setup_cloudlab.sh --mongodb (installs gcc)"
cc -O2 -shared -fPIC -o "$OUT_DIR/libbindsrc.so" "$SHIM_SRC" -ldl || die "failed to compile bindsrc.c"
echo "built $OUT_DIR/libbindsrc.so"

if [[ $FORCE -eq 0 ]]; then
    if have_binary "$OUT_DIR"; then
        echo "mongod $BUILD_ID already built at $OUT_DIR/mongod; nothing to do"
        exit 0
    fi
    if [[ -n "$CACHE_DIR" ]] && have_binary "$CACHE_DIR/$BUILD_ID"; then
        echo "mongod $BUILD_ID found in cache $CACHE_DIR; copying"
        install -m 0755 "$CACHE_DIR/$BUILD_ID/mongod" "$OUT_DIR/mongod"
        echo "$BUILD_ID" > "$OUT_DIR/$STAMP_NAME"
        exit 0
    fi
fi

# --- Host checks (verification only; installation is setup_cloudlab.sh --mongodb) ---
export PATH="$HOME/.local/bin:$PATH"
command -v bazel >/dev/null || die "bazel not found; provision the host with setup_cloudlab.sh --mongodb"
command -v python3 >/dev/null || die "python3 not found; provision the host with setup_cloudlab.sh --mongodb"
# Header files checked by path: the host has no system compiler (bazel brings its own).
for hdr in curl/curl.h lzma.h; do
    found=0
    for dir in /usr/include /usr/include/"$(uname -m)"-linux-gnu /usr/local/include; do
        [[ -f "$dir/$hdr" ]] && found=1
    done
    [[ $found -eq 1 ]] || die "missing development header $hdr; provision the host with setup_cloudlab.sh --mongodb"
done
# Space is consumed where bazel writes (downloads, external repos, sandboxes, outputs):
# the output root from ~/.bazelrc, else bazel's default under ~/.cache.
OUTPUT_ROOT="$(sed -nE 's/^startup --output_user_root=(.*)$/\1/p' "$HOME/.bazelrc" 2>/dev/null | tail -1)"
OUTPUT_ROOT="${OUTPUT_ROOT:-$HOME/.cache/bazel}"
mkdir -p "$OUTPUT_ROOT"
FREE_GB="$(df -BG --output=avail "$OUTPUT_ROOT" | tail -1 | tr -dc '0-9')"
[[ "$FREE_GB" -ge 40 ]] || die "need about 40 GB free on the filesystem holding bazel's output root $OUTPUT_ROOT, have ${FREE_GB} GB (setup_cloudlab.sh --mongodb puts it on /mydata)"

# --- Build ---
echo "building mongod $BUILD_ID (commit ${BASE_COMMIT:0:12}, patch ${PATCH_HASH}, profile ${BAZEL_PROFILE})"
START=$(date +%s)
bazel build --config="$BAZEL_PROFILE" install-mongod --disable_warnings_as_errors=True
echo "build finished in $(( $(date +%s) - START )) s"

BIN="$MONGO_DIR/bazel-bin/install/bin/mongod"
[[ -x "$BIN" ]] || die "build reported success but $BIN is missing"
# install replaces the previous binary, which bazel left read-only, where cp would refuse.
install -m 0755 "$BIN" "$OUT_DIR/mongod"
echo "$BUILD_ID" > "$OUT_DIR/$STAMP_NAME"
"$OUT_DIR/mongod" --version | head -3

if [[ -n "$CACHE_DIR" ]]; then
    mkdir -p "$CACHE_DIR/$BUILD_ID"
    install -m 0755 "$OUT_DIR/mongod" "$CACHE_DIR/$BUILD_ID/mongod.tmp" && mv -f "$CACHE_DIR/$BUILD_ID/mongod.tmp" "$CACHE_DIR/$BUILD_ID/mongod"
    echo "$BUILD_ID" > "$CACHE_DIR/$BUILD_ID/$STAMP_NAME"
    echo "cached at $CACHE_DIR/$BUILD_ID"
fi
