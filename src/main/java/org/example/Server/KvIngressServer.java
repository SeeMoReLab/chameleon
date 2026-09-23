package org.example.Server;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.example.Transport.KvWireProtocol;
import org.example.raft.KvRequest;
import org.example.raft.KvResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.flush.FlushConsolidationHandler;

/**
 * Persistent client ingress transport. Its fixed header is admitted directly
 * on a Netty event loop before the protobuf payload is parsed or work is
 * submitted to an application executor.
 */
public final class KvIngressServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KvIngressServer.class);
    private static final int WRITE_LOW_WATERMARK = 1 << 20;
    private static final int WRITE_HIGH_WATERMARK = 4 << 20;

    private final int port;
    private final KvRequestHandler service;
    private final MeasurementPlane plane;
    private final ServerDrainMetrics metrics;
    private final int capacity;
    private final AtomicInteger inFlight = new AtomicInteger();
    private final EventLoopGroup acceptGroup;
    private final EventLoopGroup workerGroup;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Channel listener;

    public KvIngressServer(int port, KvClientService service, MeasurementPlane plane) {
        this(port, service, service.admissionCapacity(), plane);
    }

    KvIngressServer(int port, KvRequestHandler service, int capacity, MeasurementPlane plane) {
        if (port < 0 || port > 65_535) {
            throw new IllegalArgumentException("client ingress port must be in [0, 65535], got " + port);
        }
        if (capacity <= 0) {
            throw new IllegalArgumentException("client ingress capacity must be positive");
        }
        this.port = port;
        this.service = service;
        this.plane = plane;
        this.metrics = plane.drainMetrics();
        this.capacity = capacity;
        this.acceptGroup = new NioEventLoopGroup(1, runnable -> {
            Thread thread = new Thread(runnable, "kv-ingress-accept-" + port);
            thread.setDaemon(true);
            return thread;
        });
        this.workerGroup = new NioEventLoopGroup(0, runnable -> {
            Thread thread = new Thread(runnable, "kv-ingress-io-" + port);
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start() throws IOException {
        if (listener != null) {
            throw new IllegalStateException("client ingress server is already started");
        }
        try {
            listener = new ServerBootstrap()
                    .group(acceptGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,
                            new WriteBufferWaterMark(WRITE_LOW_WATERMARK, WRITE_HIGH_WATERMARK))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) {
                            channel.pipeline()
                                    .addLast(new LengthFieldBasedFrameDecoder(
                                            KvWireProtocol.MAX_FRAME_BYTES,
                                            0,
                                            KvWireProtocol.LENGTH_FIELD_BYTES,
                                            0,
                                            KvWireProtocol.LENGTH_FIELD_BYTES))
                                    .addLast(new FlushConsolidationHandler(256, true))
                                    .addLast(new IngressHandler());
                        }
                    })
                    .bind(port).syncUninterruptibly().channel();
        } catch (RuntimeException e) {
            close();
            throw new IOException("failed to bind client ingress port " + port, e);
        }
    }

    public int port() {
        Channel channel = listener;
        if (channel == null) {
            return port;
        }
        return ((java.net.InetSocketAddress) channel.localAddress()).getPort();
    }

    public int inFlight() {
        return inFlight.get();
    }

    private boolean tryAcquire() {
        while (true) {
            int current = inFlight.get();
            if (current >= capacity) {
                return false;
            }
            if (inFlight.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    private void release() {
        int remaining = inFlight.decrementAndGet();
        if (remaining < 0) {
            inFlight.incrementAndGet();
            throw new IllegalStateException("client ingress in-flight count became negative");
        }
    }

    private final class IngressHandler extends SimpleChannelInboundHandler<ByteBuf> {
        private final ConcurrentHashMap<Long, FramedResponse> accepted = new ConcurrentHashMap<>();

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf frame) throws Exception {
            long receivedNanos = System.nanoTime();
            long receivedEpochMs = System.currentTimeMillis();
            metrics.headerSeen();
            if (frame.readableBytes() < KvWireProtocol.REQUEST_HEADER_BYTES) {
                throw new CorruptedFrameException("client request frame is shorter than the fixed header");
            }
            byte version = frame.readByte();
            byte type = frame.readByte();
            if (version != KvWireProtocol.VERSION || type != KvWireProtocol.REQUEST) {
                throw new CorruptedFrameException("unsupported client frame version/type " + version + "/" + type);
            }
            long correlationId = frame.readLong();
            long clientSendEpochMs = frame.readLong();
            long deadlineEpochMs = frame.readLong();
            if (correlationId <= 0) {
                throw new CorruptedFrameException("correlation id must be positive");
            }
            // How long this frame waited between the client sending it and this
            // event loop reading it. Everything before this point - the client's
            // outbound path, the wire, the receive buffer - is otherwise
            // unmeasured, and a frame that has already burned its whole deadline
            // out there is answered below without ever entering the server.
            long ingressAgeNanos = TimeUnit.MILLISECONDS.toNanos(
                    Math.max(0, receivedEpochMs - clientSendEpochMs));
            if (deadlineEpochMs <= receivedEpochMs) {
                metrics.ingressAgeExpired(ingressAgeNanos);
                writeTerminal(ctx.channel(), KvWireProtocol.DEADLINE_EXCEEDED,
                        correlationId, receivedEpochMs, false);
                return;
            }
            if (!tryAcquire()) {
                metrics.rejected(ServerDrainMetrics.RejectionReason.HARD_CAP);
                writeTerminal(ctx.channel(), KvWireProtocol.REJECTED,
                        correlationId, receivedEpochMs, true);
                return;
            }

            metrics.headerAccepted();
            // After headerAccepted, so this shares its count exactly and the
            // mean stays a mean over the frames it claims to cover.
            metrics.ingressAgeAccepted(ingressAgeNanos);
            metrics.rpcOpened();
            plane.requestAdmitted();
            FramedResponse response = new FramedResponse(ctx.channel(), accepted, correlationId,
                    clientSendEpochMs, deadlineEpochMs, receivedNanos);
            if (accepted.putIfAbsent(correlationId, response) != null) {
                response.cancel();
                throw new CorruptedFrameException("duplicate active correlation id " + correlationId);
            }

            try {
                KvRequest request = KvRequest.parseFrom(frame.nioBuffer(frame.readerIndex(), frame.readableBytes()));
                metrics.messageDeframed();
                long callbackStartedNanos = System.nanoTime();
                metrics.callbackStarted();
                try {
                    service.execute(request, receivedNanos, response);
                } finally {
                    metrics.callbackReturned(System.nanoTime() - callbackStartedNanos);
                }
            } catch (InvalidProtocolBufferException | RuntimeException e) {
                response.cancel();
                throw e;
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            accepted.values().forEach(FramedResponse::cancel);
            accepted.clear();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOG.warn("Closing malformed or failed client ingress connection on port {}: {}",
                    port, cause.toString());
            ctx.close();
        }
    }

    private final class FramedResponse implements KvResponseSink {
        private final Channel channel;
        private final ConcurrentHashMap<Long, FramedResponse> owner;
        private final long correlationId;
        private final long clientSendEpochMs;
        private final long deadlineEpochMs;
        private final long receivedNanos;
        private final AtomicBoolean finished = new AtomicBoolean();

        private FramedResponse(Channel channel, ConcurrentHashMap<Long, FramedResponse> owner,
                long correlationId, long clientSendEpochMs, long deadlineEpochMs, long receivedNanos) {
            this.channel = channel;
            this.owner = owner;
            this.correlationId = correlationId;
            this.clientSendEpochMs = clientSendEpochMs;
            this.deadlineEpochMs = deadlineEpochMs;
            this.receivedNanos = receivedNanos;
        }

        @Override
        public boolean isFinished() {
            return finished.get();
        }

        @Override
        public double remainingDeadlineMs() {
            return Math.max(0, deadlineEpochMs - System.currentTimeMillis());
        }

        @Override
        public double ingressElapsedMs() {
            return Math.max(0, System.currentTimeMillis() - clientSendEpochMs);
        }

        @Override
        public void respond(KvResponse.Builder reply) {
            if (!finish()) {
                return;
            }
            long writeStartedNanos = System.nanoTime();
            double serviceTimeMs = (writeStartedNanos - receivedNanos) / 1_000_000.0;
            KvResponse response = reply
                    .setServiceTimeMs(serviceTimeMs)
                    .setServerReplyEpochMs(System.currentTimeMillis())
                    .build();
            byte[] payload = response.toByteArray();
            ByteBuf frame = channel.alloc().buffer(
                    KvWireProtocol.LENGTH_FIELD_BYTES + KvWireProtocol.TERMINAL_HEADER_BYTES + payload.length);
            frame.writeInt(KvWireProtocol.TERMINAL_HEADER_BYTES + payload.length);
            frame.writeByte(KvWireProtocol.VERSION);
            frame.writeByte(KvWireProtocol.RESPONSE);
            frame.writeLong(correlationId);
            frame.writeLong(response.getServerReplyEpochMs());
            frame.writeBytes(payload);
            writeFrame(channel, frame, response.getRejected(), writeStartedNanos);
        }

        private void cancel() {
            if (!finish()) {
                return;
            }
            metrics.rpcCancelled();
        }

        private boolean finish() {
            if (!finished.compareAndSet(false, true)) {
                return false;
            }
            owner.remove(correlationId, this);
            double serviceTimeMs = (System.nanoTime() - receivedNanos) / 1_000_000.0;
            plane.requestCompleted(serviceTimeMs);
            release();
            metrics.headerReleased();
            metrics.rpcClosed();
            return true;
        }
    }

    private void writeTerminal(Channel channel, byte type, long correlationId,
            long serverReplyEpochMs, boolean rejected) {
        long writeStartedNanos = System.nanoTime();
        ByteBuf frame = channel.alloc().buffer(
                KvWireProtocol.LENGTH_FIELD_BYTES + KvWireProtocol.TERMINAL_HEADER_BYTES);
        frame.writeInt(KvWireProtocol.TERMINAL_HEADER_BYTES);
        frame.writeByte(KvWireProtocol.VERSION);
        frame.writeByte(type);
        frame.writeLong(correlationId);
        frame.writeLong(serverReplyEpochMs);
        writeFrame(channel, frame, rejected, writeStartedNanos);
    }

    private void writeFrame(Channel channel, ByteBuf frame, boolean rejected, long writeStartedNanos) {
        if (!channel.isActive() || !channel.isWritable()) {
            frame.release();
            metrics.replyOnNextFinished(rejected, System.nanoTime() - writeStartedNanos, false);
            channel.close();
            return;
        }
        metrics.outboundMessageStarted();
        ChannelFuture write = channel.writeAndFlush(frame);
        metrics.replyOnNextFinished(rejected, System.nanoTime() - writeStartedNanos, true);
        write.addListener(future -> {
            if (future.isSuccess()) {
                metrics.outboundMessageSent();
            } else {
                metrics.outboundMessagesAbandoned(1);
            }
        });
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        Channel channel = listener;
        if (channel != null) {
            channel.close().syncUninterruptibly();
        }
        acceptGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
        int remaining = inFlight.get();
        if (remaining != 0) {
            throw new IllegalStateException("client ingress closed with " + remaining + " admitted requests");
        }
    }
}
