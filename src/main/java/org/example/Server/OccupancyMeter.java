package org.example.Server;

import java.util.function.LongSupplier;

/**
 * Slot-time occupancy accumulator (step 5 of the request path): every request
 * holds a slot from admission to reply, and the code that changes the
 * in-flight count is the code that records how long the previous count
 * lasted. No sampling is involved, so the interval average is exactly
 * time-weighted: a count of 10 held for 90 ms and 50 held for 10 ms averages
 * to 14, not 30.
 *
 * The clock is injectable for tests; production uses System.nanoTime.
 */
public final class OccupancyMeter {

    /** One closed control interval. */
    public record Interval(long slotNanos, long intervalNanos, int inFlightAtClose) {
        /** Time-weighted average number of requests in flight. */
        public double averageInFlight() {
            return intervalNanos <= 0 ? 0.0 : (double) slotNanos / intervalNanos;
        }
    }

    private final LongSupplier clock;

    private long accumulatedSlotNanos;
    private int inFlight;
    private long lastEventNanos;
    private long intervalStartNanos;

    public OccupancyMeter() {
        this(System::nanoTime);
    }

    public OccupancyMeter(LongSupplier clock) {
        this.clock = clock;
        long now = clock.getAsLong();
        this.lastEventNanos = now;
        this.intervalStartNanos = now;
    }

    /** +1 on admission, -1 on reply. */
    /**
     * Step 5 admission of a request that has already waited {@code waitedNanos} since it came
     * off the socket (parsing and the admission queue): the wait is credited to the slot time
     * as if the slot had been open throughout, then the slot opens. An admitted request thus
     * holds its slot from t_recv to reply, as the calibration of S_max assumes, while a frame
     * refused before admission never calls this and holds nothing.
     */
    public synchronized void onAdmitted(long waitedNanos) {
        if (waitedNanos < 0) {
            throw new IllegalArgumentException("waited time must not be negative, got " + waitedNanos);
        }
        accumulatedSlotNanos += waitedNanos;
        onEvent(1);
    }

    public synchronized void onEvent(int delta) {
        long t = clock.getAsLong();
        accumulatedSlotNanos += (long) inFlight * (t - lastEventNanos);
        inFlight += delta;
        lastEventNanos = t;
    }

    /** Close out the running stretch and start a new interval (control boundary). */
    public synchronized Interval closeInterval() {
        long t = clock.getAsLong();
        accumulatedSlotNanos += (long) inFlight * (t - lastEventNanos);
        lastEventNanos = t;
        Interval interval = new Interval(accumulatedSlotNanos, t - intervalStartNanos, inFlight);
        accumulatedSlotNanos = 0;
        intervalStartNanos = t;
        return interval;
    }

    public synchronized int inFlight() {
        return inFlight;
    }
}
