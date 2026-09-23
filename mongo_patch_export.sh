#!/usr/bin/env bash
# Export the Chameleon mongod patch as a bundle the CloudLab host can apply to
# its own checkout of mongodb/mongo at the pinned commit (MONGODB_PORT.md,
# Milestone 0). The bundle holds the pinned commit id, a git diff of every
# tracked file changed on top of it, and the new src/mongo/db/chameleon/ tree.
#
# Usage: ./mongo_patch_export.sh [--mongo-dir DIR] [--out FILE] [--base COMMIT]
#   --mongo-dir  the patched checkout (default: ./mongo)
#   --out        bundle path (default: ./chameleon-mongod-<commit>.tar.gz)
#   --base       the upstream commit the patch sits on (default: the merge base
#                with upstream/master, then origin/master, then HEAD)
#
# On the host: ./build_mongod.sh --patch-bundle chameleon-mongod-<commit>.tar.gz
set -euo pipefail

die() { echo "mongo_patch_export.sh: $*" >&2; exit 1; }

MONGO_DIR="$(pwd)/mongo"
OUT=""
BASE_COMMIT=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mongo-dir) MONGO_DIR="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        --base) BASE_COMMIT="$2"; shift 2 ;;
        *) die "unknown argument: $1" ;;
    esac
done
[[ -d "$MONGO_DIR/.git" ]] || die "not a git checkout: $MONGO_DIR"

cd "$MONGO_DIR"
# The pinned upstream commit is the merge base with whichever remote tracks
# mongodb/mongo. Both names are tried because the clone may call it either;
# with the patch as uncommitted working-tree changes and no such remote, HEAD
# is already the pinned commit. --base states it outright.
if [[ -z "$BASE_COMMIT" ]]; then
    for ref in upstream/master origin/master; do
        if git rev-parse --verify -q "$ref" >/dev/null; then
            BASE_COMMIT="$(git merge-base HEAD "$ref")"
            break
        fi
    done
    BASE_COMMIT="${BASE_COMMIT:-$(git rev-parse HEAD)}"
fi
git rev-parse --verify -q "$BASE_COMMIT^{commit}" >/dev/null || die "not a commit: $BASE_COMMIT"
[[ -n "$OUT" ]] || OUT="$OLDPWD/chameleon-mongod-${BASE_COMMIT:0:12}.tar.gz"

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
echo "$BASE_COMMIT" > "$STAGE/base-commit"
git diff "$BASE_COMMIT" -- . ':(exclude)src/mongo/db/chameleon' > "$STAGE/tracked.patch"
mkdir -p "$STAGE/new"
git ls-files --others --exclude-standard -z src/mongo/db/chameleon | tar --null -T - -cf - | tar -C "$STAGE/new" -xf -
git ls-files -z src/mongo/db/chameleon | tar --null -T - -cf - | tar -C "$STAGE/new" -xf - 2>/dev/null || true

# An empty diff usually means the base was resolved to the patch commit itself
# rather than to the upstream commit underneath it, so say which base was used.
[[ -s "$STAGE/tracked.patch" ]] || die "no tracked changes on top of ${BASE_COMMIT:0:12} in $MONGO_DIR.
  Either the patch is not applied, or that base is the patch commit itself rather than the
  upstream commit it sits on. HEAD is $(git rev-parse --short HEAD) on $(git rev-parse --abbrev-ref HEAD).
  Pass --base <upstream-commit> to state it outright."
[[ -d "$STAGE/new/src/mongo/db/chameleon" ]] || die "src/mongo/db/chameleon not found in $MONGO_DIR"

# COPYFILE_DISABLE keeps macOS tar from adding extended-attribute headers that
# GNU tar on the host warns about.
TAR_XATTR_FLAGS=()
tar --version 2>/dev/null | grep -q bsdtar && TAR_XATTR_FLAGS=(--no-xattrs --no-mac-metadata)
COPYFILE_DISABLE=1 tar "${TAR_XATTR_FLAGS[@]}" -C "$STAGE" -czf "$OUT" base-commit tracked.patch new
echo "wrote $OUT"
echo "  base commit:   ${BASE_COMMIT:0:12}"
echo "  tracked diff:  $(grep -c '^diff --git' "$STAGE/tracked.patch") files"
echo "  new files:     $(find "$STAGE/new" -type f | wc -l | tr -d ' ')"
