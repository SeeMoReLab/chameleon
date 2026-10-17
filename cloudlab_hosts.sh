#!/usr/bin/env bash
#
# cloudlab_hosts.sh - shared parser for the hosts file that describes a
# distributed CloudLab allocation. Sourced by setup_cloudlab.sh,
# build_cloudlab.sh, run_all_cloudlab.sh and clean_cloudlab.sh; it is not
# executable on its own.
#
# Format: one role per line, blank lines and '#' comments ignored.
#
#     # role    ssh target                                lan ip
#     client    user@host1.cloudlab.us     10.10.1.1
#     server    user@host2.cloudlab.us     10.10.1.2
#     server    user@host3.cloudlab.us     10.10.1.3
#
# Exactly one client line and at least one server line. The ssh target is how
# this laptop reaches the machine (a public CloudLab name); the LAN IP is how
# the machines reach each other, on the experiment network the profile
# declares. They differ, both are needed, and neither can be derived from the
# other, so both are written out. The server order is the cluster's node
# order: the first server line is node 0, which is where the preferred leader
# lives.
#
# After hosts_load:
#   CLIENT_SSH   ssh target of the client machine (the WorkloadDriver)
#   CLIENT_LAN   its experiment-network address
#   SERVER_SSH[] ssh targets, node 0 first
#   SERVER_LAN[] their experiment-network addresses, same order
#   ALL_SSH[]    every machine, client first
#   ALL_LAN[]    their addresses, same order

# hosts_load <path>
hosts_load() {
    local file="$1" line role target lan lineno=0
    [[ -f "$file" ]] || die "hosts file not found: $file"

    CLIENT_SSH=""
    CLIENT_LAN=""
    SERVER_SSH=()
    SERVER_LAN=()

    local extra
    while IFS= read -r line || [[ -n "$line" ]]; do
        lineno=$((lineno + 1))
        line="${line%%#*}"
        # read splits on IFS whatever the caller's shell options are, and
        # `extra` catches a fourth field instead of silently dropping it.
        read -r role target lan extra <<< "$line"
        [[ -z "$role" ]] && continue
        [[ -n "$lan" && -z "$extra" ]] \
            || die "$file:$lineno: expected '<client|server> <ssh-target> <lan-ip>', got: $line"
        [[ "$lan" =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]] \
            || die "$file:$lineno: '$lan' is not a literal IPv4 address (the delay rules match on literals)"
        case "$role" in
            client)
                [[ -z "$CLIENT_SSH" ]] || die "$file:$lineno: a second client line; exactly one machine runs the driver"
                CLIENT_SSH="$target"; CLIENT_LAN="$lan" ;;
            server)
                SERVER_SSH+=("$target"); SERVER_LAN+=("$lan") ;;
            *)
                die "$file:$lineno: role must be 'client' or 'server', got '$role'" ;;
        esac
    done < "$file"

    [[ -n "$CLIENT_SSH" ]] || die "$file: no client line; one machine must run the WorkloadDriver"
    [[ ${#SERVER_SSH[@]} -gt 0 ]] || die "$file: no server lines"

    # A repeated address silently sends two roles to one machine, which looks
    # like a working run with half the cluster missing.
    local seen=() a b
    for a in "$CLIENT_LAN" "${SERVER_LAN[@]}"; do
        for b in ${seen[@]+"${seen[@]}"}; do
            [[ "$a" == "$b" ]] && die "$file: LAN address $a appears twice; each machine needs its own"
        done
        seen+=("$a")
    done
    seen=()
    for a in "$CLIENT_SSH" "${SERVER_SSH[@]}"; do
        for b in ${seen[@]+"${seen[@]}"}; do
            [[ "$a" == "$b" ]] && die "$file: ssh target $a appears twice; each machine needs its own"
        done
        seen+=("$a")
    done

    ALL_SSH=("$CLIENT_SSH" "${SERVER_SSH[@]}")
    ALL_LAN=("$CLIENT_LAN" "${SERVER_LAN[@]}")
}

# hosts_describe - one line per machine, for the scripts' preflight output.
hosts_describe() {
    echo "  client  $CLIENT_SSH  ($CLIENT_LAN)"
    local i
    for ((i = 0; i < ${#SERVER_SSH[@]}; i++)); do
        printf '  node %-2d %s  (%s)\n' "$i" "${SERVER_SSH[$i]}" "${SERVER_LAN[$i]}"
    done
}
