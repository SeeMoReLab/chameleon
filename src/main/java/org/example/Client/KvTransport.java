package org.example.Client;

import java.util.function.Consumer;

import org.example.raft.KvRequest;
import org.example.raft.KvResponse;

import io.grpc.Status;

/**
 * The client's transport boundary: one request to one node from one client
 * site, answered by exactly one of the two callbacks. The framed KV protocol
 * and the MongoDB wire protocol both implement it, so the session client,
 * grader, selector and ledger are transport-agnostic.
 */
public interface KvTransport extends AutoCloseable {

    /** What produced a transport-level failure, which decides how it is counted. */
    enum Cause {
        /** The server answered, or the network or the server is at fault. */
        SERVER,
        /**
         * The transport declined the request itself, because its own queue for that node
         * could not deliver within the deadline. Not a broken promise by the cluster and
         * not a value-aware refusal by admission control, so it is counted on its own.
         * Only a transport that queues can raise it.
         */
        TRANSPORT_QUEUE
    }

    /** Transport-level failure; the status code drives the session client's retry decision. */
    record RpcFailure(Status status, long serverReplyEpochMs, Cause cause) {
        public RpcFailure(Status status, long serverReplyEpochMs) {
            this(status, serverReplyEpochMs, Cause.SERVER);
        }
    }

    interface RequestHandle {
        void cancel();
    }

    int numServers();

    int numSites();

    /**
     * Acknowledgments a committed write needs on this transport's store, which is also the
     * largest write concern a client may ask for: numServers/2 + 1 on the Raft store. The
     * MongoDB transport overrides it with a majority of the voting members.
     */
    default int writeMajority() {
        return numServers() / 2 + 1;
    }

    /**
     * Recent time a request waits inside this transport before it reaches the network,
     * in ms, for the given path. Queueing here is a property of the client, not of the
     * node, so the session client keeps it out of the round trip estimate it feeds the
     * scorer. Zero for a transport that never queues: the framed protocol multiplexes
     * over pre-opened sockets and accepts a write without waiting.
     */
    default double queueWaitMs(int siteId, int nodeId) {
        return 0.0;
    }

    /**
     * Send {@code request} to {@code nodeId} from {@code siteId}. Exactly one
     * of {@code onResponse} or {@code onError} fires, on a transport thread.
     * {@code remainingDeadlineNanos} bounds the whole exchange.
     */
    RequestHandle execute(int siteId, int nodeId, KvRequest request, long remainingDeadlineNanos,
            Consumer<KvResponse> onResponse, Consumer<RpcFailure> onError);

    @Override
    void close();
}
