package org.example.Server;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.example.Client.KvFramedTransport;
import org.example.raft.KvRequest;
import org.example.raft.KvResponse;

/**
 * Minimal test session over the real client protocol: blocking single calls
 * plus an optional async listener for load tests. Requests are SLA-driven
 * like the real client; helpers fill the session-index field.
 */
final class TestSession implements AutoCloseable {

    private final KvFramedTransport transport;
    private final AtomicLong ids = new AtomicLong();
    private volatile Consumer<KvResponse> asyncListener;

    TestSession(int port) {
        transport = new KvFramedTransport(java.util.List.of("localhost"), port - 1, 1);
    }

    private void onResponse(KvResponse response, CompletableFuture<KvResponse> future) {
                Consumer<KvResponse> listener = asyncListener;
                if (listener != null) {
                    listener.accept(response);
                }
                if (future != null) {
                    future.complete(response);
                }
    }

    void setAsyncListener(Consumer<KvResponse> listener) {
        this.asyncListener = listener;
    }

    KvResponse call(KvRequest.Builder request) throws Exception {
        long id = ids.incrementAndGet();
        CompletableFuture<KvResponse> future = new CompletableFuture<>();
        transport.execute(0, request.setRequestId(id).build(), TimeUnit.SECONDS.toNanos(10),
                response -> onResponse(response, future),
                failure -> future.completeExceptionally(failure.status().asRuntimeException()));
        return future.get(10, TimeUnit.SECONDS);
    }

    /** Fire and forget; responses reach the async listener only. */
    void send(KvRequest.Builder request) {
        transport.execute(0, request.setRequestId(ids.incrementAndGet()).build(), TimeUnit.SECONDS.toNanos(10),
                response -> onResponse(response, null),
                failure -> { });
    }

    KvResponse write(String key, String value, int applicationId, int slaId) throws Exception {
        return call(writeRequest(key, value, applicationId, slaId));
    }

    KvResponse read(String key, int applicationId, int slaId, int sessionIndex)
            throws Exception {
        return call(readRequest(key, applicationId, slaId, sessionIndex));
    }

    static KvRequest.Builder writeRequest(String key, String value, int applicationId, int slaId) {
        return KvRequest.newBuilder().setIsRead(false).setKey(key).setValue(value)
                .setApplicationId(applicationId).setSlaId(slaId)
                .setSessionIndex(-1);
    }

    static KvRequest.Builder readRequest(String key, int applicationId, int slaId, int sessionIndex) {
        return KvRequest.newBuilder().setIsRead(true).setKey(key)
                .setApplicationId(applicationId).setSlaId(slaId)
                .setSessionIndex(sessionIndex);
    }

    @Override
    public void close() {
        transport.close();
    }
}
