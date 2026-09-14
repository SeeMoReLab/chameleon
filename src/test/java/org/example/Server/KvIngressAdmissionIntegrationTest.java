package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.example.Client.KvFramedTransport;
import org.example.Transport.KvWireProtocol;
import org.example.raft.KvRequest;
import org.example.raft.KvResponse;
import org.junit.jupiter.api.Test;

class KvIngressAdmissionIntegrationTest {

    @Test
    void rejectsAtTheFixedHeaderBeforeParsingOrInvokingTheService() throws Exception {
        MeasurementPlane plane = new MeasurementPlane(97, 3);
        AtomicInteger invocations = new AtomicInteger();
        AtomicReference<KvResponseSink> held = new AtomicReference<>();
        CountDownLatch firstEntered = new CountDownLatch(1);
        KvRequestHandler handler = (request, receivedNanos, response) -> {
            invocations.incrementAndGet();
            held.set(response);
            firstEntered.countDown();
        };
        KvIngressServer server = new KvIngressServer(0, handler, 1, plane);
        server.start();
        try (server;
                KvFramedTransport transport = new KvFramedTransport(
                        List.of("localhost"), server.port() - 1, 1)) {
            CompletableFuture<KvResponse> first = new CompletableFuture<>();
            transport.execute(0, request(1), TimeUnit.SECONDS.toNanos(10),
                    first::complete,
                    failure -> first.completeExceptionally(failure.status().asRuntimeException()));
            assertTrue(firstEntered.await(5, TimeUnit.SECONDS));
            assertEquals(1, server.inFlight());

            try (Socket socket = new Socket("localhost", server.port());
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    DataInputStream in = new DataInputStream(socket.getInputStream())) {
                int length = KvWireProtocol.REQUEST_HEADER_BYTES + 1;
                out.writeInt(length);
                out.writeByte(KvWireProtocol.VERSION);
                out.writeByte(KvWireProtocol.REQUEST);
                out.writeLong(2);
                out.writeLong(System.currentTimeMillis());
                out.writeLong(System.currentTimeMillis() + 10_000);
                out.writeByte(0xff); // deliberately invalid protobuf, which hard rejection must never parse
                out.flush();

                assertEquals(KvWireProtocol.TERMINAL_HEADER_BYTES, in.readInt());
                assertEquals(KvWireProtocol.VERSION, in.readByte());
                assertEquals(KvWireProtocol.REJECTED, in.readByte());
                assertEquals(2, in.readLong());
                assertTrue(in.readLong() > 0);
            }
            assertEquals(1, invocations.get(), "hard rejection must not parse and invoke another request");
            assertEquals(1, server.inFlight());

            held.get().respond(KvResponse.newBuilder().setRequestId(1).setOk(true));
            assertTrue(first.get(5, TimeUnit.SECONDS).getOk());
            assertEquals(0, server.inFlight());
        } finally {
            plane.close();
        }
    }

    private static KvRequest request(long id) {
        return KvRequest.newBuilder()
                .setRequestId(id)
                .setApplicationId(1)
                .setSlaId(1)
                .setIsRead(true)
                .setKey("k")
                .setSessionIndex(-1)
                .build();
    }
}
