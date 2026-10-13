package org.example.Server;

import org.example.raft.KvResponse;

/** One ingress-accepted framed request and its exactly-once terminal response. */
interface KvResponseSink {

    boolean isFinished();

    /**
     * Step 5 admission: the request holds an occupancy slot from its socket receipt (the wait
     * in the admission queue is credited at this point) until its terminal reply. Called once,
     * by the path that decided to execute it; a request the scorer, the replication bucket or
     * the admission queue refuses never holds a slot, so the price controller prices the
     * queueing of the work the node took on and nothing of the work it turned away.
     */
    void admitted();

    double remainingDeadlineMs();

    double ingressElapsedMs();

    void respond(KvResponse.Builder response);
}
