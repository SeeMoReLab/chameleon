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

    /** Transport-level failure; the status code drives the session client's retry decision. */
    record RpcFailure(Status status, long serverReplyEpochMs) {
    }

    interface RequestHandle {
        void cancel();
    }

    int numServers();

    int numSites();

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
