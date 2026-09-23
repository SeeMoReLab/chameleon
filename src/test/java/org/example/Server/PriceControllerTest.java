package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PriceControllerTest {

    @Test
    void startsAtZeroAndTheFloorBootstrapsIt() {
        PriceController controller = new PriceController(0.85, 1.0, 0.0001, 1.0e6);
        assertEquals(0.0, controller.lambda(), 1e-12, "initialized to zero");
        controller.update(0.1);
        assertEquals(0.0001, controller.lambda(), 1e-12,
                "the multiplicative update leaves zero unchanged; the floor bootstraps");
    }

    @Test
    void zeroFloorPinsThePriceAtZeroForever() {
        // The priceless-chameleon ablation: with lambdaMin = 0, zero is an
        // absorbing fixed point, so even sustained extreme overload never
        // raises the price.
        PriceController controller = new PriceController(0.85, 1.0, 0.0, 1.0e6);
        for (int i = 0; i < 1000; i++) {
            controller.update(1.5);
        }
        assertEquals(0.0, controller.lambda(), 0.0, "zero must be absorbing when the floor is 0");
    }

    @Test
    void risesMultiplicativelyAboveTargetAndDecaysBelow() {
        PriceController controller = new PriceController(0.85, 1.0, 0.0001, 1.0e6);
        controller.update(0.85); // bootstrap to the floor
        double previous = controller.lambda();

        for (int i = 0; i < 5; i++) {
            controller.update(1.85); // u - target = 1
            assertEquals(previous * Math.E, controller.lambda(), previous * 1e-9,
                    "each interval above target multiplies lambda by exp(eta * (u - target))");
            previous = controller.lambda();
        }

        for (int i = 0; i < 100; i++) {
            controller.update(0.0);
        }
        assertEquals(0.0001, controller.lambda(), 1e-12,
                "idle periods decay lambda to the floor, not to zero");
    }

    @Test
    void gainScalesTheResponse() {
        PriceController gentle = new PriceController(0.85, 0.1, 0.0001, 1.0e6);
        PriceController aggressive = new PriceController(0.85, 2.0, 0.0001, 1.0e6);
        gentle.update(1.85);
        aggressive.update(1.85);
        gentle.update(1.85);
        aggressive.update(1.85);
        assertTrue(aggressive.lambda() > gentle.lambda());
        assertEquals(0.0001 * Math.exp(0.1), gentle.lambda(), 1e-12);
        assertEquals(0.0001 * Math.exp(2.0), aggressive.lambda(), 1e-12);
    }

    /**
     * Under sustained overload the price has no authority over u - in-flight is
     * bounded by the ingress cap, not by the price - so the error term stays
     * positive and the multiplicative update compounds without opposition. That
     * is integrator windup, and unbounded it reaches infinity, which is
     * absorbing: the node then refuses everything forever. Measured on the local
     * topology, u sat at 18.74 through a 60 s phase at 2.5C and lambda overflowed
     * after 45 s.
     */
    @Test
    void ceilingStopsWindupAndKeepsTheControllerRecoverable() {
        PriceController controller = new PriceController(0.85, 0.1, 0.0001, 1.0e6);
        controller.forceLambda(1.0);
        // 600 intervals at the utilization actually observed during Flash.
        for (int i = 0; i < 600; i++) {
            controller.update(18.74);
        }
        assertTrue(Double.isFinite(controller.lambda()), "price must stay finite under windup");
        assertEquals(1.0e6, controller.lambda(), 1e-6);

        // And it must come back down once the load goes away, which infinity
        // never does: inf * exp(negative) is still inf.
        for (int i = 0; i < 400; i++) {
            controller.update(0.0);
        }
        assertEquals(0.0001, controller.lambda(), 1e-9);
    }

    @Test
    void ceilingMustExceedTheFloor() {
        assertThrows(IllegalArgumentException.class,
                () -> new PriceController(0.85, 0.1, 0.01, 0.01));
        assertThrows(IllegalArgumentException.class,
                () -> new PriceController(0.85, 0.1, 0.0001, Double.POSITIVE_INFINITY));
    }
}
