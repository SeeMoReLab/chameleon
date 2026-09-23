#!/usr/bin/env bash
#
# run_all_cloudlab_sweep.sh - run one experiment per parameter combination by
# invoking run_all_cloudlab.sh once per combination, on either layout: a single
# CloudLab node, or a distributed allocation described by a hosts file. The
# swept axes are the base config file itself (';'-separated list) and the
# `mode:`, `sMax:`, and `seed:` lines, rewritten textually in a copy of each
# base config. Runs are sequential (the runner blocks until the remote
# experiment finishes and fetches its results), and every generated config
# is validated with the strict loader BEFORE the first run starts, so a typo
# in --modes, --smax, or --seeds fails in seconds instead of hours into the
# sweep.
#
# The sweep runs the full cross product, configs major, then modes, then
# sMax, then seeds (--seeds = repeats of the same setup under different
# PRNG seeds). Each finished run is renamed from the runner's timestamped
# runs/<name>_<stamp>/ to runs/<config>_<mode>_<sMax>_<seed>/ - <config> is
# the config file name without its extension, and an axis that is not swept
# uses that config's own value (seed: 42 in the repo configs) - ready for
# analyze_run.py. Every target directory must not exist yet; that is checked
# up front.
#
# On a failed or interrupted run the sweep stops and prints how to resume
# with narrowed lists (the failed run's partial results, if any, keep their
# timestamped name).
#
# Usage, one machine (servers and driver share it, geo on loopback):
#   ./run_all_cloudlab_sweep.sh <user@host> <ssh-key-path> \
#       --modes "chameleon;chameleonPileus;pileus;strongest;weakest" \
#       --seeds "42;43;44" \
#       "config_local.yaml;config_regional.yaml;config_global.yaml"
#
# Usage, one machine per server plus one for the driver:
#   ./run_all_cloudlab_sweep.sh --hosts hosts.txt --key ~/.ssh/cloudlab \
#       --modes "chameleon;chameleonPileus;pileus;strongest;weakest" \
#       --seeds "42;43;44" \
#       "config_local.yaml;config_regional.yaml;config_global.yaml"
#
# The hosts form also accepts --site-subnet and passes it through. Everything
# else is identical between the two: the sweep does not care how many machines
# there are, it just forwards the target arguments to run_all_cloudlab.sh.
#
# All flags take a ';'-separated list, may appear anywhere among the
# arguments, and at least one of them is required. With <user@host> the
# remaining positionals are host, key, and the ';'-separated config list, in
# that order; with --hosts/--key the only positional is the config list.
#
# --smax exists but is rarely what you want now that sMax is calibrated per
# topology (see CALIBRATION.md): one swept value applies to every config in
# the run, so sweeping it across config_local/regional/global overrides three
# different calibrated values with one number. Leave it out and each config
# uses its own.
#
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

die() { echo "ERROR: $*" >&2; exit 1; }

# --- Arguments ---
MODES_RAW=""
SMAX_RAW=""
SEEDS_RAW=""
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --modes)
            [[ $# -ge 2 ]] || die "--modes requires a ';'-separated list of modes"
            MODES_RAW="$2"
            shift 2
            ;;
        --modes=*)
            MODES_RAW="${1#--modes=}"
            shift
            ;;
        --smax)
            [[ $# -ge 2 ]] || die "--smax requires a ';'-separated list of sMax values"
            SMAX_RAW="$2"
            shift 2
            ;;
        --smax=*)
            SMAX_RAW="${1#--smax=}"
            shift
            ;;
        --seeds)
            [[ $# -ge 2 ]] || die "--seeds requires a ';'-separated list of integer seeds"
            SEEDS_RAW="$2"
            shift 2
            ;;
        --seeds=*)
            SEEDS_RAW="${1#--seeds=}"
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
            SITE_SUBNET_ARG="$2"
            shift 2
            ;;
        --site-subnet=*)
            SITE_SUBNET_ARG="${1#--site-subnet=}"
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

USAGE="usage: ./run_all_cloudlab_sweep.sh <user@host> <ssh-key-path> --modes \"m1;m2;...\" --smax \"s1;s2;...\" --seeds \"k1;k2;...\" \"config1.yaml;config2.yaml;...\" (at least one flag)
   or: ./run_all_cloudlab_sweep.sh --hosts <hosts.txt> --key <key> [same flags] \"config1.yaml;...\""
# Whichever layout, the invocation below passes it straight through to
# run_all_cloudlab.sh; the sweep itself does not care how many machines there are.
if [[ -n "${HOSTS_FILE:-}" ]]; then
    [[ ${#POSITIONAL[@]} -eq 1 ]] || die "$USAGE"
    [[ -n "${KEY_ARG:-}" ]] || die "--hosts needs --key <ssh-key-path>"
    KEY="$KEY_ARG"
    CONFIGS_RAW="${POSITIONAL[0]}"
    TARGET_ARGS=(--hosts "$HOSTS_FILE" --key "$KEY")
    [[ -n "${SITE_SUBNET_ARG:-}" ]] && TARGET_ARGS+=(--site-subnet "$SITE_SUBNET_ARG")
    TARGET_DESC="--hosts $HOSTS_FILE --key $KEY"
else
    [[ ${#POSITIONAL[@]} -eq 3 ]] || die "$USAGE"
    HOST="${POSITIONAL[0]}"
    KEY="${POSITIONAL[1]}"
    CONFIGS_RAW="${POSITIONAL[2]}"
    TARGET_ARGS=("$HOST" "$KEY")
    TARGET_DESC="$HOST $KEY"
fi

[[ -n "$MODES_RAW" || -n "$SMAX_RAW" || -n "$SEEDS_RAW" ]] || die "nothing to sweep: pass --modes, --smax, and/or --seeds. $USAGE"
[[ -f "$KEY" ]] || die "ssh key not found: $KEY"

# --- Config list: resolve, check, and read each file's base mode/sMax ---
# Both key lines must exist and be unique in every config regardless of
# which axes are swept: the unswept axis's base value names the output
# directory. Comments mentioning sMax never start a line with "sMax: ", so
# exactly one match means exactly one key line.
IFS=';' read -r -a CONFIG_INPUTS <<< "$CONFIGS_RAW"
declare -a CONFIG_PATHS=() CONFIG_STEMS=() CONFIG_BASE_MODES=() CONFIG_BASE_SMAXES=() CONFIG_BASE_SEEDS=()
for c in "${CONFIG_INPUTS[@]}"; do
    c="$(echo "$c" | sed -E 's/^[[:space:]]+|[[:space:]]+$//g')"
    [[ -z "$c" ]] && continue
    case "$c" in
        *.yaml|*.yml) ;;
        *) die "the sweep requires YAML configs (the mode:/sMax: lines are rewritten textually), got: $c" ;;
    esac
    path="$(cd "$(dirname "$c")" 2>/dev/null && pwd)/$(basename "$c")" || die "config file not found: $c"
    [[ -f "$path" ]] || die "config file not found: $c"
    stem="$(basename "$c")"
    stem="${stem%.*}"
    [[ "$stem" =~ ^[A-Za-z0-9._-]+$ ]] || die "config file name must match [A-Za-z0-9._-]+, got: $stem"
    grep -Ecq '^mode: ' "$path" || die "no top-level 'mode:' line found in $c"
    [[ "$(grep -Ec '^mode: ' "$path")" == 1 ]] || die "multiple top-level 'mode:' lines in $c"
    [[ "$(grep -Ec '^[[:space:]]*sMax: ' "$path")" == 1 ]] \
        || die "expected exactly one 'sMax:' line in $c, found $(grep -Ec '^[[:space:]]*sMax: ' "$path")"
    [[ "$(grep -Ec '^seed: ' "$path")" == 1 ]] \
        || die "expected exactly one top-level 'seed:' line in $c, found $(grep -Ec '^seed: ' "$path")"
    CONFIG_PATHS+=("$path")
    CONFIG_STEMS+=("$stem")
    CONFIG_BASE_MODES+=("$(sed -nE 's/^mode: *//p' "$path" | tr -d '[:space:]')")
    CONFIG_BASE_SMAXES+=("$(sed -nE 's/^[[:space:]]*sMax: *//p' "$path" | tr -d '[:space:]')")
    CONFIG_BASE_SEEDS+=("$(sed -nE 's/^seed: *//p' "$path" | tr -d '[:space:]')")
done
[[ ${#CONFIG_PATHS[@]} -ge 1 ]] || die "config list contained no configs: '$CONFIGS_RAW'"

# An empty element in either flag axis means "keep the base config's value";
# it stands in for a flag that wasn't given.
MODES=("")
if [[ -n "$MODES_RAW" ]]; then
    IFS=';' read -r -a MODES <<< "$MODES_RAW"
    CLEANED=()
    for m in "${MODES[@]}"; do
        m="${m//[[:space:]]/}"
        [[ -z "$m" ]] && continue
        [[ "$m" =~ ^[A-Za-z]+$ ]] || die "mode name must be alphabetic, got: '$m'"
        CLEANED+=("$m")
    done
    [[ ${#CLEANED[@]} -ge 1 ]] || die "--modes contained no modes: '$MODES_RAW'"
    MODES=("${CLEANED[@]}")
fi

SMAXES=("")
if [[ -n "$SMAX_RAW" ]]; then
    IFS=';' read -r -a SMAXES <<< "$SMAX_RAW"
    CLEANED=()
    for s in "${SMAXES[@]}"; do
        s="${s//[[:space:]]/}"
        [[ -z "$s" ]] && continue
        [[ "$s" =~ ^[0-9]+(\.[0-9]+)?$ ]] || die "sMax must be a positive number, got: '$s'"
        CLEANED+=("$s")
    done
    [[ ${#CLEANED[@]} -ge 1 ]] || die "--smax contained no values: '$SMAX_RAW'"
    SMAXES=("${CLEANED[@]}")
fi

SEEDS=("")
if [[ -n "$SEEDS_RAW" ]]; then
    IFS=';' read -r -a SEEDS <<< "$SEEDS_RAW"
    CLEANED=()
    for k in "${SEEDS[@]}"; do
        k="${k//[[:space:]]/}"
        [[ -z "$k" ]] && continue
        [[ "$k" =~ ^-?[0-9]+$ ]] || die "seed must be an integer, got: '$k'"
        CLEANED+=("$k")
    done
    [[ ${#CLEANED[@]} -ge 1 ]] || die "--seeds contained no values: '$SEEDS_RAW'"
    SEEDS=("${CLEANED[@]}")
fi

cd "$REPO_DIR"

# --- Generate one config per combination (kept for the whole sweep) ---
# Combinations are configs-major, then modes, then sMax. COMBO_* arrays are
# parallel; COMBO_NAMES holds the final runs/<config>_<mode>_<sMax>
# directory name of each run.
# Per-invocation build directory: concurrent sweeps must never share one
# (mvn clean would race), and the jar built here is both the validator and,
# via --jar, the exact binary every run executes.
BUILD_DIR="target-sweep-$$"
SWEEP_TMP="$(mktemp -d)"
trap 'rm -rf "$SWEEP_TMP" "$REPO_DIR/$BUILD_DIR"' EXIT
declare -a COMBO_CFG_IDX=() COMBO_MODES=() COMBO_SMAXES=() COMBO_SEEDS=() COMBO_NAMES=() COMBO_CONFIGS=()
for ci in "${!CONFIG_PATHS[@]}"; do
    for m in "${MODES[@]}"; do
        for s in "${SMAXES[@]}"; do
            for k in "${SEEDS[@]}"; do
                name="${CONFIG_STEMS[$ci]}_${m:-${CONFIG_BASE_MODES[$ci]}}_${s:-${CONFIG_BASE_SMAXES[$ci]}}_${k:-${CONFIG_BASE_SEEDS[$ci]}}"
                for existing in "${COMBO_NAMES[@]+"${COMBO_NAMES[@]}"}"; do
                    [[ "$existing" == "$name" ]] && die "duplicate sweep combination: $name"
                done
                [[ -e "runs/$name" ]] && die "runs/$name already exists; move or delete it before sweeping"
                combo_config="$SWEEP_TMP/${name}.yaml"
                cp "${CONFIG_PATHS[$ci]}" "$combo_config"
                if [[ -n "$m" ]]; then
                    sed -E -i.bak "s/^mode: .*/mode: $m/" "$combo_config" && rm -f "$combo_config.bak"
                    grep -q "^mode: $m\$" "$combo_config" || die "failed to rewrite mode: line for '$m'"
                fi
                if [[ -n "$s" ]]; then
                    sed -E -i.bak "s/^([[:space:]]*)sMax: .*/\\1sMax: $s/" "$combo_config" && rm -f "$combo_config.bak"
                    grep -Eq "^[[:space:]]*sMax: ${s//./\\.}\$" "$combo_config" || die "failed to rewrite sMax: line for '$s'"
                fi
                if [[ -n "$k" ]]; then
                    sed -E -i.bak "s/^seed: .*/seed: $k/" "$combo_config" && rm -f "$combo_config.bak"
                    grep -q "^seed: $k\$" "$combo_config" || die "failed to rewrite seed: line for '$k'"
                fi
                COMBO_CFG_IDX+=("$ci")
                COMBO_MODES+=("$m")
                COMBO_SMAXES+=("$s")
                COMBO_SEEDS+=("$k")
                COMBO_NAMES+=("$name")
                COMBO_CONFIGS+=("$combo_config")
            done
        done
    done
done

# --- Build once, validate every generated config before the first run ---
# The jar is built into this invocation's own directory and handed to every
# run via --jar, so concurrent sweeps never share a build and each run
# executes the exact jar its config was validated against. An unknown mode,
# an out-of-range sMax, or any other config problem dies here, before
# anything touches the node.
JAVA_MAJOR="$(java -version 2>&1 | head -1 | sed -E 's/.*version "([0-9]+).*/\1/')"
JAVA_FLAGS=()
[[ "$JAVA_MAJOR" =~ ^[0-9]+$ && "$JAVA_MAJOR" -ge 24 ]] && JAVA_FLAGS+=(--sun-misc-unsafe-memory-access=allow)
echo "Building fat jar (into $BUILD_DIR) to pre-validate ${#COMBO_CONFIGS[@]} sweep configs..."
mvn -q clean package -Dbuild.dir="$BUILD_DIR" -DskipTests
JAR="$(ls "$BUILD_DIR"/*-all.jar 2>/dev/null | head -1)"
[[ -n "$JAR" && -f "$JAR" ]] || die "fat jar not found in $BUILD_DIR/ (expected *-all.jar)"
for i in "${!COMBO_CONFIGS[@]}"; do
    java ${JAVA_FLAGS[@]+"${JAVA_FLAGS[@]}"} -cp "$JAR" org.example.Utility.ExperimentConfig \
        "${COMBO_CONFIGS[$i]}" > /dev/null \
        || die "config validation failed for '${COMBO_NAMES[$i]}' (base: ${CONFIG_PATHS[${COMBO_CFG_IDX[$i]}]})"
done
echo "All ${#COMBO_CONFIGS[@]} sweep configs validate:"
for n in "${COMBO_NAMES[@]}"; do echo "  $n"; done

# --- Run the sweep sequentially ---
for i in "${!COMBO_CONFIGS[@]}"; do
    name="${COMBO_NAMES[$i]}"
    echo
    echo "================================================================"
    echo "Sweep run $((i + 1))/${#COMBO_CONFIGS[@]}: $name"
    echo "================================================================"
    if ! "$REPO_DIR/run_all_cloudlab.sh" "${TARGET_ARGS[@]}" "$name" "${COMBO_CONFIGS[$i]}" --jar "$JAR"; then
        echo
        echo "Sweep stopped: '$name' failed (its partial results, if any, keep their timestamped name under runs/${name}_*)." >&2
        # Resume hints, innermost axis out: finish the failed (config, mode,
        # sMax)'s remaining seeds, then the (config, mode)'s remaining sMax
        # values with all seeds, then the config's remaining modes with
        # everything, then the remaining configs with everything.
        ci="${COMBO_CFG_IDX[$i]}"
        m="${COMBO_MODES[$i]}"
        s="${COMBO_SMAXES[$i]}"
        cfg="${CONFIG_PATHS[$ci]}"
        rest_seeds=()
        for ((j = i; j < ${#COMBO_CONFIGS[@]}; j++)); do
            [[ "${COMBO_CFG_IDX[$j]}" == "$ci" && "${COMBO_MODES[$j]}" == "$m" \
                && "${COMBO_SMAXES[$j]}" == "$s" ]] && rest_seeds+=("${COMBO_SEEDS[$j]}")
        done
        rest_smax=()
        for ((j = i + 1; j < ${#COMBO_CONFIGS[@]}; j++)); do
            [[ "${COMBO_CFG_IDX[$j]}" != "$ci" || "${COMBO_MODES[$j]}" != "$m" \
                || "${COMBO_SMAXES[$j]}" == "$s" ]] && continue
            already=0
            for r in "${rest_smax[@]+"${rest_smax[@]}"}"; do [[ "$r" == "${COMBO_SMAXES[$j]}" ]] && already=1; done
            [[ $already -eq 0 ]] && rest_smax+=("${COMBO_SMAXES[$j]}")
        done
        rest_modes=()
        for ((j = i + 1; j < ${#COMBO_CONFIGS[@]}; j++)); do
            [[ "${COMBO_CFG_IDX[$j]}" != "$ci" || "${COMBO_MODES[$j]}" == "$m" ]] && continue
            already=0
            for r in "${rest_modes[@]+"${rest_modes[@]}"}"; do [[ "$r" == "${COMBO_MODES[$j]}" ]] && already=1; done
            [[ $already -eq 0 ]] && rest_modes+=("${COMBO_MODES[$j]}")
        done
        rest_cfgs=()
        for ((cj = ci + 1; cj < ${#CONFIG_PATHS[@]}; cj++)); do
            rest_cfgs+=("${CONFIG_PATHS[$cj]}")
        done
        mode_flag=""
        [[ -n "$m" ]] && mode_flag=" --modes \"$m\""
        smax_flag=""
        [[ -n "$s" ]] && smax_flag=" --smax \"$s\""
        seeds_flag=""
        [[ -n "${rest_seeds[0]}" ]] && seeds_flag=" --seeds \"$(IFS=';'; echo "${rest_seeds[*]}")\""
        all_modes_flag=""
        [[ -n "$MODES_RAW" ]] && all_modes_flag=" --modes \"$(IFS=';'; echo "${MODES[*]}")\""
        all_smax_flag=""
        [[ -n "$SMAX_RAW" ]] && all_smax_flag=" --smax \"$(IFS=';'; echo "${SMAXES[*]}")\""
        all_seeds_flag=""
        [[ -n "$SEEDS_RAW" ]] && all_seeds_flag=" --seeds \"$(IFS=';'; echo "${SEEDS[*]}")\""
        echo "Resume with:" >&2
        echo "  ./run_all_cloudlab_sweep.sh $TARGET_DESC$mode_flag$smax_flag$seeds_flag $cfg" >&2
        if [[ ${#rest_smax[@]} -gt 0 ]]; then
            echo "  ./run_all_cloudlab_sweep.sh $TARGET_DESC$mode_flag --smax \"$(IFS=';'; echo "${rest_smax[*]}")\"$all_seeds_flag $cfg" >&2
        fi
        if [[ ${#rest_modes[@]} -gt 0 ]]; then
            echo "  ./run_all_cloudlab_sweep.sh $TARGET_DESC --modes \"$(IFS=';'; echo "${rest_modes[*]}")\"$all_smax_flag$all_seeds_flag $cfg" >&2
        fi
        if [[ ${#rest_cfgs[@]} -gt 0 ]]; then
            echo "  ./run_all_cloudlab_sweep.sh $TARGET_DESC$all_modes_flag$all_smax_flag$all_seeds_flag \"$(IFS=';'; echo "${rest_cfgs[*]}")\"" >&2
        fi
        exit 1
    fi
    # Rename the runner's timestamped output to the plain combination name.
    fetched="$(ls -dt runs/${name}_* 2>/dev/null | head -1 || true)"
    [[ -n "$fetched" && -d "$fetched" ]] || die "runner reported success but no runs/${name}_* directory was fetched"
    [[ -e "runs/$name" ]] && die "runs/$name appeared mid-sweep; leaving results at $fetched"
    mv "$fetched" "runs/$name"
    echo "Results: runs/$name"
done

echo
echo "Sweep complete: ${#COMBO_CONFIGS[@]} runs."
for n in "${COMBO_NAMES[@]}"; do
    echo "  runs/$n"
done
echo "Analyze each with: python3 analyze_run.py <run-dir>"
