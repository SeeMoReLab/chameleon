package org.example.Server;

/**
 * The shadow price controller: once per control interval,
 *
 *   lambda <- min(lambda_max, max(lambda_min, lambda * exp(eta * (u - u_target))))
 *
 * Multiplicative because the right price spans orders of magnitude and can
 * never go negative; a controller rather than a closed form because profit
 * units are whatever the applications say they are. The floor does two jobs:
 * it lets lambda start at zero (the multiplicative update alone would leave
 * zero unchanged forever) and it stops lambda decaying so far during idle
 * periods that recovery takes many intervals.
 *
 * lambda_min = 0 is therefore a deliberate ablation switch: zero is an
 * absorbing fixed point, so the price stays exactly 0 for the whole run -
 * the scorer still picks levels, but nothing is ever priced out ("priceless
 * chameleon": upgrades free, admission reduced to the hard occupancy cap).
 *
 * The ceiling exists because infinity is absorbing in the same way, and is
 * reachable. A level is admitted only while its expected profit exceeds
 * lambda * omega (RungScorer step 4f), so once lambda passes
 * max_rung_profit / omega the price already refuses everything and further
 * growth changes no decision. But growth does not stop there: in-flight is
 * bounded by the ingress hard cap, not by the price, so under heavy overload
 * u stays pinned above target no matter how high lambda goes and the update
 * compounds unopposed - classic integrator windup, made explosive by the
 * multiplicative form. Measured on the local topology at 2.5C: u sat at 18.74
 * for the whole Flash phase while lambda rose by 10^38 every five seconds and
 * reached Double.POSITIVE_INFINITY after 45 s, from which no decay can return
 * (inf * exp(negative) is still inf). That node then refused every request for
 * the remaining 41% of the run. The ceiling makes the saturation recoverable:
 * decay is exp(eta * (0 - u_target)) per interval, so the climb back down is
 * proportional to log(lambda_max), which is seconds rather than never.
 *
 * Requests never read u; they read the published lambda, which is up to one
 * interval stale by design.
 */
public final class PriceController {

    private final double uTarget;
    private final double eta;
    private final double lambdaMin;
    private final double lambdaMax;

    private volatile double lambda = 0.0;

    public PriceController(double uTarget, double eta, double lambdaMin, double lambdaMax) {
        if (!(lambdaMax > lambdaMin) || !Double.isFinite(lambdaMax)) {
            throw new IllegalArgumentException("lambdaMax must be finite and greater than lambdaMin, got "
                    + lambdaMax + " against " + lambdaMin);
        }
        this.uTarget = uTarget;
        this.eta = eta;
        this.lambdaMin = lambdaMin;
        this.lambdaMax = lambdaMax;
    }

    /** Called once per control interval with that interval's utilization. */
    public void update(double utilization) {
        lambda = Math.min(lambdaMax, Math.max(lambdaMin, lambda * Math.exp(eta * (utilization - uTarget))));
    }

    /** The published price; up to one control interval stale. */
    public double lambda() {
        return lambda;
    }

    /** Test-only: set the price directly to probe admission at a known lambda. */
    void forceLambda(double value) {
        lambda = value;
    }
}
