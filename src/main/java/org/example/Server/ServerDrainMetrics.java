package org.example.Server;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.DoubleAdder;

/**
 * Per-node flow ledger for the framed ingress path. Counters are reset on
 * every flush; gauges are sampled at the close of the interval. This keeps
 * the hot path lock-free and makes arrival, callback, admission, and reply
 * drain rates directly comparable.
 */
final class ServerDrainMetrics {

    enum RejectionReason {
        HARD_CAP,
        ADMISSION_QUEUE_FULL,
        SCORER,
        REPLICATION_BUDGET
    }

    private static final int LATENCY_BUCKETS = 64;
    private static final double LATENCY_BASE_MS = 0.001;
    private static final double LATENCY_RATIO = 1.35;
    private static final double LOG_RATIO = Math.log(LATENCY_RATIO);

    private final int nodeId;
    private final String csvPath;
    private long lastFlushNanos = System.nanoTime();

    private final AtomicLong rpcsOpened = new AtomicLong();
    private final AtomicLong rpcsClosed = new AtomicLong();
    private final AtomicLong rpcsCancelled = new AtomicLong();
    private final AtomicLong headersSeen = new AtomicLong();
    private final AtomicLong headersAccepted = new AtomicLong();
    private final AtomicLong deframed = new AtomicLong();
    private final AtomicLong callbackEntered = new AtomicLong();
    private final AtomicLong callbackReturned = new AtomicLong();
    private final AtomicLong executionAdmitted = new AtomicLong();
    private final AtomicLong hardCapRejected = new AtomicLong();
    private final AtomicLong admissionQueueRejected = new AtomicLong();
    private final AtomicLong scorerRejected = new AtomicLong();
    private final AtomicLong replicationRejected = new AtomicLong();
    private final AtomicLong admissionQueueAccepted = new AtomicLong();
    private final AtomicLong admissionWorkStarted = new AtomicLong();
    private final AtomicLong admissionWorkCompleted = new AtomicLong();
    private final AtomicLong replyOnNextSucceeded = new AtomicLong();
    private final AtomicLong replyOnNextFailed = new AtomicLong();
    private final AtomicLong rejectedReplyOnNextSucceeded = new AtomicLong();
    private final AtomicLong outboundMessagesStarted = new AtomicLong();
    private final AtomicLong outboundMessagesSent = new AtomicLong();
    private final AtomicLong outboundMessagesAbandoned = new AtomicLong();

    private final AtomicInteger deframedAwaitingCallback = new AtomicInteger();
    private final AtomicInteger callbacksExecuting = new AtomicInteger();
    private final AtomicInteger headerPermitsInFlight = new AtomicInteger();
    private final AtomicInteger outboundMessagesPending = new AtomicInteger();
    private final AtomicInteger admissionQueueDepth = new AtomicInteger();
    private final AtomicInteger admissionWorkersExecuting = new AtomicInteger();

    private final DoubleAdder callbackNanos = new DoubleAdder();
    private final AtomicLongArray callbackLatencyBuckets = new AtomicLongArray(LATENCY_BUCKETS + 1);
    private final DoubleAdder replyOnNextNanos = new DoubleAdder();
    private final AtomicLongArray replyOnNextLatencyBuckets = new AtomicLongArray(LATENCY_BUCKETS + 1);
    private final DoubleAdder admissionQueueWaitNanos = new DoubleAdder();
    private final AtomicLongArray admissionQueueWaitBuckets = new AtomicLongArray(LATENCY_BUCKETS + 1);
    private final DoubleAdder admissionWorkNanos = new DoubleAdder();
    private final AtomicLongArray admissionWorkBuckets = new AtomicLongArray(LATENCY_BUCKETS + 1);
    // How old a request frame already is when the event loop first reads it:
    // the client's send stamp against this machine's clock. Split by what
    // happened to it, because a frame that arrives already past its deadline
    // is turned away at the door and never reaches any other counter here -
    // it is invisible everywhere else, and it is the whole story when a node
    // cannot drain its socket. Cross-machine clocks, so treat small values as
    // noise and read the distribution, not any single sample.
    private final DoubleAdder ingressAgeAcceptedNanos = new DoubleAdder();
    private final AtomicLongArray ingressAgeAcceptedBuckets = new AtomicLongArray(LATENCY_BUCKETS + 1);
    private final DoubleAdder ingressAgeExpiredNanos = new DoubleAdder();
    private final AtomicLongArray ingressAgeExpiredBuckets = new AtomicLongArray(LATENCY_BUCKETS + 1);
    private final AtomicLong ingressExpiredOnArrival = new AtomicLong();

    ServerDrainMetrics(int nodeId) {
        this.nodeId = nodeId;
        this.csvPath = "server_drain_" + nodeId + ".csv";
    }

    void rpcOpened() {
        rpcsOpened.incrementAndGet();
    }

    void rpcClosed() {
        rpcsClosed.incrementAndGet();
    }

    void rpcCancelled() {
        rpcsCancelled.incrementAndGet();
    }

    void headerSeen() {
        headersSeen.incrementAndGet();
    }

    void headerAccepted() {
        headersAccepted.incrementAndGet();
        headerPermitsInFlight.incrementAndGet();
    }

    /** Age of an accepted frame when the event loop first read it. */
    void ingressAgeAccepted(long ageNanos) {
        ingressAgeAcceptedNanos.add(ageNanos);
        ingressAgeAcceptedBuckets.incrementAndGet(latencyBucketOf(ageNanos));
    }

    /** Age of a frame already past its deadline on arrival, and so turned away. */
    void ingressAgeExpired(long ageNanos) {
        ingressExpiredOnArrival.incrementAndGet();
        ingressAgeExpiredNanos.add(ageNanos);
        ingressAgeExpiredBuckets.incrementAndGet(latencyBucketOf(ageNanos));
    }

    void headerReleased() {
        subtractGauge(headerPermitsInFlight, 1, "accepted header permits");
    }

    void messageDeframed() {
        deframed.incrementAndGet();
        deframedAwaitingCallback.incrementAndGet();
    }

    void messagesAbandonedBeforeCallback(int count) {
        subtractGauge(deframedAwaitingCallback, count, "deframed messages awaiting callbacks");
    }

    void callbackStarted() {
        subtractGauge(deframedAwaitingCallback, 1, "deframed messages awaiting callbacks");
        callbacksExecuting.incrementAndGet();
        callbackEntered.incrementAndGet();
    }

    void callbackReturned(long durationNanos) {
        subtractGauge(callbacksExecuting, 1, "executing callbacks");
        callbackReturned.incrementAndGet();
        callbackNanos.add(durationNanos);
        callbackLatencyBuckets.incrementAndGet(latencyBucketOf(durationNanos));
    }

    void executionAdmitted() {
        executionAdmitted.incrementAndGet();
    }

    void admissionQueued() {
        admissionQueueDepth.incrementAndGet();
    }

    void admissionQueueAccepted() {
        admissionQueueAccepted.incrementAndGet();
    }

    void admissionQueueSubmissionCancelled() {
        subtractGauge(admissionQueueDepth, 1, "requests in the admission queue");
    }

    void admissionWorkStarted(long queueWaitNanos) {
        subtractGauge(admissionQueueDepth, 1, "requests in the admission queue");
        admissionWorkersExecuting.incrementAndGet();
        admissionWorkStarted.incrementAndGet();
        admissionQueueWaitNanos.add(queueWaitNanos);
        admissionQueueWaitBuckets.incrementAndGet(latencyBucketOf(queueWaitNanos));
    }

    void admissionWorkCompleted(long workNanos) {
        subtractGauge(admissionWorkersExecuting, 1, "executing admission workers");
        admissionWorkCompleted.incrementAndGet();
        admissionWorkNanos.add(workNanos);
        admissionWorkBuckets.incrementAndGet(latencyBucketOf(workNanos));
    }

    void rejected(RejectionReason reason) {
        switch (reason) {
            case HARD_CAP -> hardCapRejected.incrementAndGet();
            case ADMISSION_QUEUE_FULL -> admissionQueueRejected.incrementAndGet();
            case SCORER -> scorerRejected.incrementAndGet();
            case REPLICATION_BUDGET -> replicationRejected.incrementAndGet();
        }
    }

    void replyOnNextFinished(boolean rejected, long durationNanos, boolean succeeded) {
        replyOnNextNanos.add(durationNanos);
        replyOnNextLatencyBuckets.incrementAndGet(latencyBucketOf(durationNanos));
        if (succeeded) {
            replyOnNextSucceeded.incrementAndGet();
            if (rejected) {
                rejectedReplyOnNextSucceeded.incrementAndGet();
            }
        } else {
            replyOnNextFailed.incrementAndGet();
        }
    }

    void outboundMessageStarted() {
        outboundMessagesStarted.incrementAndGet();
        outboundMessagesPending.incrementAndGet();
    }

    void outboundMessageSent() {
        subtractGauge(outboundMessagesPending, 1, "outbound messages awaiting sent callback");
        outboundMessagesSent.incrementAndGet();
    }

    void outboundMessagesAbandoned(int count) {
        subtractGauge(outboundMessagesPending, count, "outbound messages awaiting sent callback");
        outboundMessagesAbandoned.addAndGet(count);
    }

    private static void subtractGauge(AtomicInteger gauge, int count, String label) {
        int remaining = gauge.addAndGet(-count);
        if (remaining < 0) {
            gauge.addAndGet(count);
            throw new IllegalStateException("cannot remove " + count + " from " + label
                    + " when only " + (remaining + count) + " remain");
        }
    }

    private static int latencyBucketOf(long durationNanos) {
        double durationMs = durationNanos / 1_000_000.0;
        if (durationMs <= LATENCY_BASE_MS) {
            return 0;
        }
        int index = (int) Math.floor(Math.log(durationMs / LATENCY_BASE_MS) / LOG_RATIO);
        return Math.min(index, LATENCY_BUCKETS);
    }

    private static double latencyBucketUpperMs(int bucket) {
        return LATENCY_BASE_MS * Math.pow(LATENCY_RATIO, bucket + 1.0);
    }

    private static double quantileMs(long[] buckets, long total, double quantile) {
        if (total == 0) {
            return 0.0;
        }
        long target = (long) Math.ceil(total * quantile);
        long cumulative = 0;
        for (int i = 0; i <= LATENCY_BUCKETS; i++) {
            cumulative += buckets[i];
            if (cumulative >= target) {
                return latencyBucketUpperMs(i);
            }
        }
        return latencyBucketUpperMs(LATENCY_BUCKETS);
    }

    synchronized void flush() {
        long nowNanos = System.nanoTime();
        long intervalNanos = nowNanos - lastFlushNanos;
        if (intervalNanos <= 0) {
            return;
        }
        lastFlushNanos = nowNanos;

        long opened = rpcsOpened.getAndSet(0);
        long closed = rpcsClosed.getAndSet(0);
        long cancelled = rpcsCancelled.getAndSet(0);
        long headerCount = headersSeen.getAndSet(0);
        long headerAcceptedCount = headersAccepted.getAndSet(0);
        long deframedCount = deframed.getAndSet(0);
        long entered = callbackEntered.getAndSet(0);
        long returned = callbackReturned.getAndSet(0);
        long admitted = executionAdmitted.getAndSet(0);
        long hardRejected = hardCapRejected.getAndSet(0);
        long queueRejected = admissionQueueRejected.getAndSet(0);
        long valueRejected = scorerRejected.getAndSet(0);
        long replicationBudgetRejected = replicationRejected.getAndSet(0);
        long queueAccepted = admissionQueueAccepted.getAndSet(0);
        long workStarted = admissionWorkStarted.getAndSet(0);
        long workCompleted = admissionWorkCompleted.getAndSet(0);
        long replySucceeded = replyOnNextSucceeded.getAndSet(0);
        long replyFailed = replyOnNextFailed.getAndSet(0);
        long rejectedReplies = rejectedReplyOnNextSucceeded.getAndSet(0);
        long outboundStarted = outboundMessagesStarted.getAndSet(0);
        long outboundSent = outboundMessagesSent.getAndSet(0);
        long outboundAbandoned = outboundMessagesAbandoned.getAndSet(0);

        long[] callbackBuckets = snapshotBuckets(callbackLatencyBuckets);
        long[] replyBuckets = snapshotBuckets(replyOnNextLatencyBuckets);
        long[] queueWaitBuckets = snapshotBuckets(admissionQueueWaitBuckets);
        long[] workBuckets = snapshotBuckets(admissionWorkBuckets);
        long expiredOnArrival = ingressExpiredOnArrival.getAndSet(0);
        long[] ageAcceptedBuckets = snapshotBuckets(ingressAgeAcceptedBuckets);
        long[] ageExpiredBuckets = snapshotBuckets(ingressAgeExpiredBuckets);
        double ageAcceptedTotalNanos = ingressAgeAcceptedNanos.sumThenReset();
        double ageExpiredTotalNanos = ingressAgeExpiredNanos.sumThenReset();
        double callbackTotalNanos = callbackNanos.sumThenReset();
        double replyTotalNanos = replyOnNextNanos.sumThenReset();
        double queueWaitTotalNanos = admissionQueueWaitNanos.sumThenReset();
        double workTotalNanos = admissionWorkNanos.sumThenReset();

        double intervalMs = intervalNanos / 1_000_000.0;
        File file = new File(csvPath);
        boolean writeHeader = !file.exists() || file.length() == 0;
        try (FileWriter fw = new FileWriter(file, true); PrintWriter out = new PrintWriter(fw)) {
            if (writeHeader) {
                out.println("Timestamp,NodeId,IntervalMs,FramesSeen,FramesAdmitted,"
                        + "TransportPermitsInFlightAtClose,RequestsOpened,RequestsClosed,RequestsCancelled,"
                        + "FramesDecoded,IngressDispatchEntered,IngressDispatchReturned,ExecutionAdmitted,HardCapRejected,"
                        + "AdmissionQueueRejected,ScorerRejected,ReplicationBudgetRejected,"
                        + "ResponseEnqueueSucceeded,ResponseEnqueueFailed,"
                        + "RejectedResponseEnqueueSucceeded,ResponseWritesStarted,ResponseWritesCompleted,"
                        + "ResponseWritesAbandoned,DecodedAwaitingDispatchAtClose,IngressDispatchesExecutingAtClose,"
                        + "ResponsesPendingWriteAtClose,AvgIngressDispatchMs,P50IngressDispatchMs,"
                        + "P90IngressDispatchMs,P99IngressDispatchMs,AvgResponseEnqueueMs,P50ResponseEnqueueMs,"
                        + "P90ResponseEnqueueMs,P99ResponseEnqueueMs,"
                        + "AdmissionQueueAccepted,AdmissionWorkStarted,AdmissionWorkCompleted,"
                        + "AdmissionQueueDepthAtClose,AdmissionWorkersExecutingAtClose,"
                        + "AvgAdmissionQueueWaitMs,P50AdmissionQueueWaitMs,P90AdmissionQueueWaitMs,"
                        + "P99AdmissionQueueWaitMs,AvgAdmissionWorkMs,P50AdmissionWorkMs,P90AdmissionWorkMs,"
                        + "P99AdmissionWorkMs,ExpiredOnArrival,"
                        + "AvgIngressAgeAcceptedMs,P50IngressAgeAcceptedMs,P90IngressAgeAcceptedMs,"
                        + "P99IngressAgeAcceptedMs,AvgIngressAgeExpiredMs,P50IngressAgeExpiredMs,"
                        + "P90IngressAgeExpiredMs,P99IngressAgeExpiredMs");
            }
            out.printf("%d,%d,%.3f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,"
                            + "%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%d,%d,%d,%d,%d,"
                            + "%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%d,"
                            + "%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f%n",
                    System.currentTimeMillis(), nodeId, intervalMs, headerCount, headerAcceptedCount,
                    headerPermitsInFlight.get(), opened, closed, cancelled, deframedCount,
                    entered, returned, admitted, hardRejected, queueRejected, valueRejected, replicationBudgetRejected,
                    replySucceeded, replyFailed, rejectedReplies, outboundStarted, outboundSent,
                    outboundAbandoned, deframedAwaitingCallback.get(), callbacksExecuting.get(),
                    outboundMessagesPending.get(),
                    averageMs(callbackTotalNanos, returned),
                    quantileMs(callbackBuckets, returned, 0.50),
                    quantileMs(callbackBuckets, returned, 0.90),
                    quantileMs(callbackBuckets, returned, 0.99),
                    averageMs(replyTotalNanos, replySucceeded + replyFailed),
                    quantileMs(replyBuckets, replySucceeded + replyFailed, 0.50),
                    quantileMs(replyBuckets, replySucceeded + replyFailed, 0.90),
                    quantileMs(replyBuckets, replySucceeded + replyFailed, 0.99),
                    queueAccepted, workStarted, workCompleted, admissionQueueDepth.get(),
                    admissionWorkersExecuting.get(),
                    averageMs(queueWaitTotalNanos, workStarted),
                    quantileMs(queueWaitBuckets, workStarted, 0.50),
                    quantileMs(queueWaitBuckets, workStarted, 0.90),
                    quantileMs(queueWaitBuckets, workStarted, 0.99),
                    averageMs(workTotalNanos, workCompleted),
                    quantileMs(workBuckets, workCompleted, 0.50),
                    quantileMs(workBuckets, workCompleted, 0.90),
                    quantileMs(workBuckets, workCompleted, 0.99),
                    expiredOnArrival,
                    averageMs(ageAcceptedTotalNanos, headerAcceptedCount),
                    quantileMs(ageAcceptedBuckets, headerAcceptedCount, 0.50),
                    quantileMs(ageAcceptedBuckets, headerAcceptedCount, 0.90),
                    quantileMs(ageAcceptedBuckets, headerAcceptedCount, 0.99),
                    averageMs(ageExpiredTotalNanos, expiredOnArrival),
                    quantileMs(ageExpiredBuckets, expiredOnArrival, 0.50),
                    quantileMs(ageExpiredBuckets, expiredOnArrival, 0.90),
                    quantileMs(ageExpiredBuckets, expiredOnArrival, 0.99));
        } catch (IOException e) {
            System.err.println("Failed to write " + csvPath + ": " + e.getMessage());
        }
    }

    private static long[] snapshotBuckets(AtomicLongArray source) {
        long[] snapshot = new long[source.length()];
        for (int i = 0; i < source.length(); i++) {
            snapshot[i] = source.getAndSet(i, 0);
        }
        return snapshot;
    }

    private static double averageMs(double totalNanos, long count) {
        return count == 0 ? 0.0 : totalNanos / count / 1_000_000.0;
    }
}
