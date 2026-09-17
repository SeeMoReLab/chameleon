/*
 * bindsrc.c - LD_PRELOAD shim that binds every outgoing IPv4 TCP connection of the
 * process to the address in BINDSRC_ADDR before connecting.
 *
 * Why: the CloudLab geo emulation keys its tc/netem delay rules on (source IP,
 * destination IP) pairs on loopback. The Java servers bind their outgoing
 * connections to their own 127.0.1.k address, so inter-server traffic is delayed;
 * mongod does not, so its replication and heartbeat connections leave from
 * 127.0.0.1 and match no rule, and a majority write acknowledges in microseconds
 * regardless of the configured inter-region latency. Loading this shim into each
 * mongod with BINDSRC_ADDR=127.0.1.k gives its outgoing sockets the same source
 * address as its listener, and the existing rules apply.
 *
 * Only unbound AF_INET stream sockets are touched; anything else falls through to
 * the real connect(). Failure to bind is reported once and the connect proceeds
 * unbound, so a misconfigured address degrades to today's behaviour loudly rather
 * than silently.
 *
 * Built by build_mongod.sh into libbindsrc.so beside mongod; loaded by
 * cloudlab_remote_run.sh.
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <dlfcn.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

typedef int (*connect_fn)(int, const struct sockaddr *, socklen_t);

static connect_fn real_connect;
static struct in_addr bind_addr;
static int have_addr = -1; /* -1 unknown, 0 no, 1 yes */
static int reported;

static void init(void) {
    real_connect = (connect_fn)dlsym(RTLD_NEXT, "connect");
    const char *env = getenv("BINDSRC_ADDR");
    if (env != NULL && inet_pton(AF_INET, env, &bind_addr) == 1) {
        have_addr = 1;
    } else {
        have_addr = 0;
        if (env != NULL) {
            fprintf(stderr, "bindsrc: BINDSRC_ADDR '%s' is not an IPv4 address; connections left unbound\n", env);
        }
    }
}

int connect(int fd, const struct sockaddr *addr, socklen_t len) {
    if (have_addr == -1) {
        init();
    }
    if (have_addr == 1 && addr != NULL && addr->sa_family == AF_INET) {
        int type = 0;
        socklen_t tlen = sizeof type;
        struct sockaddr_in cur;
        socklen_t clen = sizeof cur;
        if (getsockopt(fd, SOL_SOCKET, SO_TYPE, &type, &tlen) == 0 && type == SOCK_STREAM &&
            getsockname(fd, (struct sockaddr *)&cur, &clen) == 0 && cur.sin_family == AF_INET &&
            cur.sin_port == 0 && cur.sin_addr.s_addr == htonl(INADDR_ANY)) {
            struct sockaddr_in src;
            memset(&src, 0, sizeof src);
            src.sin_family = AF_INET;
            src.sin_addr = bind_addr;
            src.sin_port = 0;
            if (bind(fd, (const struct sockaddr *)&src, sizeof src) != 0 && !reported) {
                reported = 1;
                fprintf(stderr, "bindsrc: bind to BINDSRC_ADDR failed: %s; connections left unbound\n",
                        strerror(errno));
            }
        }
    }
    return real_connect(fd, addr, len);
}
