package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ServiceTimeHistogramsTest {

    @Test
    void latencyBucketIndexFormulaEdges() {
        assertEquals(0, ServiceTimeHistograms.latencyBucketOf(0.0));
        assertEquals(0, ServiceTimeHistograms.latencyBucketOf(0.4));
        assertEquals(0, ServiceTimeHistograms.latencyBucketOf(0.5));
        // Bucket 0 covers [0, 0.5 * 1.15); just below the edge stays in 0.
        assertEquals(0, ServiceTimeHistograms.latencyBucketOf(0.5 * 1.15 - 1e-9));
        assertEquals(1, ServiceTimeHistograms.latencyBucketOf(0.5 * 1.15 + 1e-9));
        // Monotone across all edges.
        for (int b = 1; b < ServiceTimeHistograms.LATENCY_BUCKETS; b++) {
            double edge = 0.5 * Math.pow(1.15, b);
            assertEquals(b, ServiceTimeHistograms.latencyBucketOf(edge * 1.0001), "just above edge " + b);
            assertEquals(b - 1, ServiceTimeHistograms.latencyBucketOf(edge * 0.9999), "just below edge " + b);
        }
        // Values beyond the last edge clamp to the last bucket.
        assertEquals(ServiceTimeHistograms.LATENCY_BUCKETS - 1, ServiceTimeHistograms.latencyBucketOf(1e9));
    }

    @Test
    void gapBucketEdges() {
        assertEquals(0, ServiceTimeHistograms.gapBucketOf(-100));
        assertEquals(0, ServiceTimeHistograms.gapBucketOf(0));
        assertEquals(1, ServiceTimeHistograms.gapBucketOf(1));
        assertEquals(1, ServiceTimeHistograms.gapBucketOf(2000));
        assertEquals(2, ServiceTimeHistograms.gapBucketOf(2001));
        assertEquals(2, ServiceTimeHistograms.gapBucketOf(20000));
        assertEquals(3, ServiceTimeHistograms.gapBucketOf(20001));
    }

    @Test
    void samplesBecomeVisibleOnTickAndDecayGeometrically() {
        ServiceTimeHistograms h = new ServiceTimeHistograms(1, 0.95);
        for (int i = 0; i < 100; i++) {
            h.file(0, 0, 10.0);
        }
        // Not yet published: requests read the previous snapshot.
        assertEquals(0.0, h.snapshot(0, 0).totalCount);

        h.refreshTick();
        ServiceTimeHistograms.Snapshot s1 = h.snapshot(0, 0);
        assertEquals(100 * 0.95, s1.totalCount, 1e-9, "new samples are folded in and decayed once");
        assertEquals(10.0, s1.meanMs, 1e-9, "decay cancels in sum/count, the mean is unbiased");

        h.refreshTick();
        ServiceTimeHistograms.Snapshot s2 = h.snapshot(0, 0);
        assertEquals(100 * 0.95 * 0.95, s2.totalCount, 1e-9, "each tick decays the state again");
        assertEquals(10.0, s2.meanMs, 1e-9);
    }

    @Test
    void cdfAndQuantilesInterpolate() {
        ServiceTimeHistograms h = new ServiceTimeHistograms(1, 0.95);
        for (int i = 0; i < 50; i++) {
            h.file(0, 0, 1.0);
        }
        for (int i = 0; i < 50; i++) {
            h.file(0, 0, 100.0);
        }
        h.refreshTick();
        ServiceTimeHistograms.Snapshot s = h.snapshot(0, 0);

        assertEquals(0.5, s.fractionAtMost(10.0), 0.02, "half the samples are fast");
        assertEquals(1.0, s.fractionAtMost(500.0), 1e-9);
        assertEquals(0.0, s.fractionAtMost(0.0), 1e-9);
        assertEquals(50.5, s.meanMs, 1e-9);

        assertTrue(s.quantileMs(0.25) < 10.0, "p25 must sit near the fast mode, got " + s.quantileMs(0.25));
        assertTrue(s.quantileMs(0.75) > 50.0, "p75 must sit near the slow mode, got " + s.quantileMs(0.75));
    }

    @Test
    void aCellNobodySamplesIsForgottenOnceItsNewestSampleWeighsOnePercent() {
        // A level that looked bad once and then stopped being chosen: decay alone
        // would keep the bad shape forever, since the scorer only samples what it picks.
        ServiceTimeHistograms h = new ServiceTimeHistograms(1, 0.95);
        assertEquals(90, h.staleTicks, "0.95^90 is the first power below 1%");
        for (int i = 0; i < 100; i++) {
            h.file(0, 0, 400.0);
        }
        h.refreshTick();
        assertEquals(0.0, h.snapshot(0, 0).fractionAtMost(200.0), 1e-9, "the burst reads as slow");

        for (int idle = 1; idle < h.staleTicks; idle++) {
            h.refreshTick();
            assertTrue(h.snapshot(0, 0).totalCount > 0, "still remembered after " + idle + " idle ticks");
            assertEquals(0.0, h.snapshot(0, 0).fractionAtMost(200.0), 1e-9, "decay keeps the shape");
        }
        h.refreshTick();
        ServiceTimeHistograms.Snapshot s = h.snapshot(0, 0);
        assertEquals(0.0, s.totalCount, 1e-9, "forgotten after staleTicks idle ticks");
        assertEquals(1.0, s.fractionAtMost(200.0), 1e-9, "forgotten, the cell is uncalibrated again");
        assertEquals(0.0, s.meanMs, 1e-9);
    }

    @Test
    void aCellThatKeepsBeingSampledIsNeverForgotten() {
        // One sample every 60 ticks: the decayed count drops well below one
        // sample between them, but the evidence is fresh, so it is kept.
        ServiceTimeHistograms h = new ServiceTimeHistograms(1, 0.95);
        for (int tick = 0; tick < 600; tick++) {
            if (tick % 60 == 0) {
                h.file(0, 0, 10.0);
            }
            h.refreshTick();
            assertTrue(h.snapshot(0, 0).totalCount > 0, "a sampled cell must keep its evidence, tick " + tick);
        }
        assertEquals(10.0, h.snapshot(0, 0).meanMs, 1e-9);
    }

    @Test
    void emptyCellIsFreeAndCertain() {
        ServiceTimeHistograms h = new ServiceTimeHistograms(1, 0.95);
        h.refreshTick();
        ServiceTimeHistograms.Snapshot s = h.snapshot(0, 0);
        assertEquals(1.0, s.fractionAtMost(5.0), 1e-9, "cold-start rule: empty cell is certain");
        assertEquals(0.0, s.meanMs, 1e-9, "cold-start rule: empty cell is free");
    }
}
