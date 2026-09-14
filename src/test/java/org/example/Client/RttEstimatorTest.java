package org.example.Client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class RttEstimatorTest {

    @Test
    void estimatesTheMedianOfLatencyMinusServiceTime() {
        RttEstimator estimator = new RttEstimator(2, 8);
        estimator.observe(0, 10.0, 4.0); // rtt 6
        estimator.observe(0, 12.0, 4.0); // rtt 8
        estimator.observe(0, 9.0, 5.0);  // rtt 4
        assertEquals(6.0, estimator.estimateMs(0), 1e-9);
        // The other node's window is independent.
        assertEquals(0.0, estimator.estimateMs(1), 1e-9);
    }

    @Test
    void longServiceTimesSubtractOutCompletely() {
        RttEstimator estimator = new RttEstimator(1, 8);
        estimator.observe(0, 10.0, 4.0);   // rtt 6
        // A reply that waited 480 ms server-side carries that wait inside its
        // service time, so it is an ordinary sample of the same RTT.
        estimator.observe(0, 486.0, 480.0); // rtt 6
        estimator.observe(0, 508.0, 500.0); // rtt 8
        assertEquals(3, estimator.sampleCount(0));
        assertEquals(6.0, estimator.estimateMs(0), 1e-9);
    }

    @Test
    void negativeDifferencesAreDiscarded() {
        RttEstimator estimator = new RttEstimator(1, 8);
        estimator.observe(0, 4.0, 10.0); // timer jitter: latency < service time
        assertEquals(0, estimator.sampleCount(0));
        assertEquals(0.0, estimator.estimateMs(0), 1e-9);
    }

    @Test
    void reportsZeroUntilASampleArrives() {
        RttEstimator estimator = new RttEstimator(1, 8);
        assertEquals(0.0, estimator.estimateMs(0), 1e-9);
    }
}
