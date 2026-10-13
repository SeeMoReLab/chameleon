package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

class OccupancyMeterTest {

    @Test
    void timeWeightedAverageMatchesTheWorkedExample() {
        // The plan's example: a count of 10 held for 90 ms and 50 held for
        // 10 ms averages to 14, not 30; a boundary sample would have read 50.
        AtomicLong clock = new AtomicLong(0);
        OccupancyMeter meter = new OccupancyMeter(clock::get);

        for (int i = 0; i < 10; i++) {
            meter.onEvent(1);
        }
        clock.set(90_000_000L); // 90 ms
        for (int i = 0; i < 40; i++) {
            meter.onEvent(1);
        }
        clock.set(100_000_000L); // 100 ms

        OccupancyMeter.Interval interval = meter.closeInterval();
        assertEquals(100_000_000L, interval.intervalNanos());
        assertEquals(10L * 90_000_000L + 50L * 10_000_000L, interval.slotNanos());
        assertEquals(14.0, interval.averageInFlight(), 1e-9);
        assertEquals(50, interval.inFlightAtClose());
    }

    @Test
    void closingResetsTheAccumulatorButCarriesInFlight() {
        AtomicLong clock = new AtomicLong(0);
        OccupancyMeter meter = new OccupancyMeter(clock::get);

        meter.onEvent(1);
        clock.set(50_000_000L);
        OccupancyMeter.Interval first = meter.closeInterval();
        assertEquals(1.0, first.averageInFlight(), 1e-9);

        // The request is still in flight: the next interval keeps charging it.
        clock.set(150_000_000L);
        meter.onEvent(-1);
        clock.set(200_000_000L);
        OccupancyMeter.Interval second = meter.closeInterval();
        assertEquals(150_000_000L, second.intervalNanos());
        assertEquals(100_000_000L, second.slotNanos(), "1 in flight for the first 100 ms of the interval");
        assertEquals(0, second.inFlightAtClose());
    }

    @Test
    void admissionCreditsTheWaitAndRefusedFramesHoldNothing() {
        // A frame received at 0 and admitted at 20 ms holds its slot from 0: the 20 ms wait
        // is credited at admission, then the slot runs to the reply at 50 ms. A frame
        // refused before admission never touches the meter.
        AtomicLong clock = new AtomicLong(0);
        OccupancyMeter meter = new OccupancyMeter(clock::get);

        clock.set(20_000_000L);
        meter.onAdmitted(20_000_000L);
        assertEquals(1, meter.inFlight());
        clock.set(50_000_000L);
        meter.onEvent(-1);
        clock.set(100_000_000L);

        OccupancyMeter.Interval interval = meter.closeInterval();
        assertEquals(50_000_000L, interval.slotNanos(), "20 ms credited wait + 30 ms in flight");
        assertEquals(0, interval.inFlightAtClose());
    }

    @Test
    void sumOfPerRequestSlotTimesEqualsTheIntegral() {
        // Interleaved requests: the accumulator must equal the sum of each
        // request's individual residence time.
        AtomicLong clock = new AtomicLong(0);
        OccupancyMeter meter = new OccupancyMeter(clock::get);

        meter.onEvent(1);            // A in
        clock.set(10_000_000L);
        meter.onEvent(1);            // B in
        clock.set(25_000_000L);
        meter.onEvent(-1);           // A out (25 ms)
        clock.set(40_000_000L);
        meter.onEvent(-1);           // B out (30 ms)
        clock.set(60_000_000L);

        OccupancyMeter.Interval interval = meter.closeInterval();
        assertEquals(55_000_000L, interval.slotNanos(), "25 ms + 30 ms of residence");
    }
}
