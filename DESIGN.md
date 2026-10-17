# Chameleon redesign

## What to build

A replicated key-value store on Raft with single-key reads and writes. Clients do not choose a consistency level per request. Each application registers SLAs, and the server decides what level to actually deliver, upgrading above the application's floor when the value of doing so exceeds the capacity it consumes. All decisions are server side.

There are no operating modes. Nothing switches between normal and pressure behavior. There is no global latency threshold and no batch-average latency check. Degradation under load is produced by a price that rises, not by a mode that flips.

## Consistency levels

MongoDB style.

Reads: eventual-local, eventual-majority, causal-local, causal-majority, linearizable.

Local means the replica's log, which may contain uncommitted entries that a leader change can roll back. Majority means committed state. That distinction is real and must be stated in any writeup, because a reader assuming stock Raft will assume followers only apply committed entries.

Each session carries one causal frontier: the highest log index of anything it has written or read, advanced on every acknowledged write and on every delivered read value. Causal-local waits until the local log index reaches it; causal-majority waits until the commit index reaches it. Because reads extend the frontier, the causal levels provide the full causal order and not just read-your-writes: read-your-writes and monotonic reads come from the frontier, monotonic writes and writes-follow-writes come for free from the single log's total order. Linearizable is anchored at the leader, but followers may serve it too (below).

Implement linearizable with ReadIndex, not with a no-op append and not as an entry. The leader records its commit index, confirms leadership via a heartbeat round with a majority, waits for its applied index to reach the recorded value, then serves locally. No log entry is created, and reads batch onto whatever AppendEntries round is already in flight. Follower linearizable reads (the follower obtains a confirmed read index from the leader, then waits for its own commit index to catch up) relieve leader concentration at the cost of an extra hop; they are gated by `followerLinearizableReads` and the reference configs enable them.

Writes: wc:1 through wc:majority.

## SLA format

An application registers one or more SLAs. An SLA can be either for read or write and is a set of rungs, each a triple (consistency requirement, end-to-end latency threshold in milliseconds, profit). Rungs are not ordered by anything; the structure below handles any shape. A request will have an application ID and which SLA applies. This is what lets one application have a cheap path and an expensive path.

## Protocol

Request carries: application id, SLA id, operation (read or write, key, value), the client's session index (its causal frontier), and the client's current RTT estimate to this server. It does not carry a consistency level, a deadline, or any profit values.

Send the RTT rather than a pre-subtracted budget, because the SLA has several thresholds and the server subtracts from each.

Send one session index, the frontier of everything the session has observed, and anchor both causal levels on it. One log position suffices because everything causally before an entry has a smaller index in the single Raft log, so a view that covers the frontier covers the whole transitive causal past. A read advances the frontier to the reply's per-key value index, not to the node's log head, so a session is never bound to unrelated writes the node happened to hold. Anchoring causal-majority on the same frontier as causal-local is what makes it strictly stronger: a committed view that covers the frontier implies a local view that does. The alternative, a separate majority-acknowledged index for causal-majority, would let a wc:1-acknowledged write stay invisible to the session's own committed reads and would break the strength ordering every ranking component assumes. The residual cost is a wait on the position of a wc:1 write that a leader change later rolls back; since the anchor is a position, that wait ends as soon as any entry commits there, and the wait bound caps it regardless.

Response carries: the status (ok, redirect with leader hint, rejected, or deadline exceeded), the level actually delivered, the node's current log index and commit index, the server-side service time, and for reads the write index of the local view's value and of the committed view's value, or for writes the entry's index. The client grades from these fields alone (step 7) and credits whichever view pays more. The reply also carries the Chameleon scorer's predicted profit, so the ledger can record prediction against realization, plus ledger-only diagnostics: whether the request waited, whether the wait expired and fell back, and the write index of the value the delivered level served.

The service time is required. Clients estimate RTT by subtracting it from observed end-to-end latency. It spans frame receipt to reply write on the server's clock, so it contains any queueing or waiting and every reply, waited or not, is a valid sample. Without it the RTT estimate absorbs server-side work and inflates under load, which is exactly backwards.

## Client routing

The server decides the level; the client decides the server. Every arm routes with the same rule except where noted, so the cross-arm comparison measures the decision policy and not the placement policy.

Each session keeps, per server, a sliding window of the last `rttWindowSize` network RTT samples (64 in the reference configs): end-to-end latency minus the service time the reply reported. Every reply is a sample, served, rejected, or redirected, because the service time already contains any server-side waiting or queueing, and rejections matter most: they are the only samples still flowing when a node sheds everything, and without them an estimate poisoned by an overload burst could never recover. The estimate for a server is the window's median; a server with no samples estimates 0. This is the same number the request carries as the scorer's `rho`.

The RTT is deliberately blind to load. Subtracting service time removes queueing, index waits and CPU saturation from the sample, so a saturated server at the same distance looks exactly as attractive as an idle one. The load signal is kept separately: per session, per registered SLA (reads and writes tracked apart), per server, exponentially decayed counts of admits and rejects with a prior of 1, giving `pAdmit = (admits + 1) / (admits + rejects + 1)`. The prior makes the estimate 1 with no evidence and keeps it strictly above 0 under any number of rejections, so a rejecting node always retains a re-probe incentive. The counts decay by `admitRateGamma` once per second (0.9 in the reference configs, so a rejection loses half its weight in about seven seconds), which lets a recovered node earn its traffic back. Tracking per SLA matters because the scorer rejects low-profit SLAs first: a node shedding browses may still be accepting checkouts.

Reads score every server as `rtt / pAdmit` and go to the minimum, ties broken uniformly at random. With no rejections the score is the raw RTT, so the admit factor changes nothing until a node starts shedding, and then a node rejecting half of this SLA's requests looks twice as far away. The random tie-break matters at cold start, when every estimate is 0 and the first probes spread across the cluster. A fraction `explorationFraction` of reads (0.15) bypasses the score and goes to a uniformly random server, so the windows of servers that are not currently winning keep receiving samples and a recovered or newly fast server can win the routing back. This is the only mechanism that bounds how stale a rarely contacted server's window can become: a window holds its last 64 samples however long ago they arrived, unlike the server's histograms, which forget geometrically.

Writes go to the last known leader, learned from redirects, and round-robin across servers until the first redirect fills the hint in. Writes are never explored and never RTT-routed: they must reach the leader, and any other target costs a redirect round trip.

The client-decided arms (strongest and weakest) route reads identically, with one exception: when the fixed rung is linearizable and follower linearizable reads are disabled, the read goes straight to the leader hint, since no follower can serve it. The reference configs enable follower linearizable reads, so this branch does not fire in the experiments.

The Pileus arms (pileus and chameleonPileus) replace the read rule with the Pileus selector, which picks the server maximising expected profit over its own per-server windows of full response time, including service time and waits, and its tracked per-server high-water log and commit indices, multiplied by the same `pAdmit`. Pileus takes both the server and the rung from that choice; chameleonPileus takes only the server and lets the scorer choose the level. In both arms the selector learns from delivery: a served read is graded against the rung the selector routed for, and whether the delivered level reached it updates that server's delivery record for that rung. In chameleonPileus the rung is never sent to the server and exists client-side for this grading only; without it the selector rated the primary as a certain linearizable server for every read and herded 60% of all reads onto it. When the selector returns no candidate, both fall back to the `rtt / pAdmit` rule. Exploration applies to the selector at the same fraction. The RTT sent on the request is still the network-only estimate in these arms, because the scorer's `rho` must not contain the server's own service time.

Redirects update the leader hint and resend to it (or to the next server round-robin when the redirect carried no hint), up to `retryLimit` attempts (3), after which the request is a failure. Rejections are terminal by design and never retried: a rejected request is recorded as shed and its only lasting effect is on the RTT window and the admit counts.

## Server state

Per node: Raft log index and commit index; a histogram of service times per (level, gap bucket); an occupancy accumulator; the shadow price lambda; the registered SLA tables.

Leader only: the replication rate bucket.

There is no pool and no leader-side control state beyond that bucket. Leaders and followers run the identical algorithm and differ only in which levels are legal and which resources they hold.

## Resource model

Two resources, priced separately.

Replication is a rate. Every write costs one log entry. Reads cost zero, including linearizable reads, since ReadIndex appends nothing. Budget it at the measured maximum commit rate in entries per second, refill continuously, charge at admission, never return. This lives at the leader only, since all entries originate there, and in practice it gates writes only.

Raft replication uses one persistent ordered bidirectional gRPC stream from the leader to each follower. The leader pipelines disjoint batches with separate confirmed and speculative cursors, advances `matchIndex` only on an ordered acknowledgement, and bounds each follower by `maxInflightReplicationBatchesPerFollower`. A rejection, stream failure, term change, or leadership change invalidates the speculative suffix and restarts from the last confirmed prefix. `maxEntriesPerReplicationBatch` bounds each stream message.

Occupancy is slot-time. Every admitted request holds a slot from `t_recv` to reply: the slot opens at admission (step 5) and is backdated to receipt, so the wait in the admission queue counts, as does time spent waiting on an index, a confirmation round, or replication acknowledgments. A request refused before admission holds nothing, so the budget prices the queueing of the work the node takes on and nothing of the work it turns away. The cost of a level is its expected service time in milliseconds. Budget S_max at the concurrency where latency starts bending upward on a load sweep, not at a hard resource ceiling, so that utilization can exceed 1 and give the controller signal. Keep a genuine hard cap at three times S_max as an immediate backstop, applied at ingress in every arm (step 1); at 1.5 times, the cap does all the admission and the price never engages. Occupancy is per node; followers have this and not the replication bucket.

## Request path

### Step 1: timestamp

Take `t_recv` the moment the request comes off the socket, before parsing and before it enters any queue. Use a monotonic clock. Everything downstream measures from here, and taking it later attributes queueing delay to the network, which makes clients conclude the network slowed down every time the server got busy.

Three refusals happen here, before the decision and identically in every arm. The occupancy hard cap is checked on the raw frame, before parsing: a frame arriving while the node holds `ingressHardCapInFlight` requests in flight is refused at once and never reaches the scorer. A parsed request whose deadline has already passed is answered deadline-exceeded. The rest enter a bounded admission queue, which refuses a request when it is full. The fixed arms (pileus, strongest, weakest) then skip steps 2 to 5 and go straight to execution at the level the client put on the wire, their writes still passing the replication bucket of step 5.

### Step 2: determine the legal level set

Filter the ladder by this node's role and the operation type. In other words, followers can't take write requests, and a follower excludes linearizable only when follower linearizable reads are disabled. If the request is illegal here, redirect rather than reject.

Look up the SLA by (application id, SLA id). Reads use a read SLA, writes use a write SLA.

### Step 3: compute the index gap for every legal level

For each legal level `c`, compute the gap it must close before it can be served:

- eventual-local, eventual-majority: gap is zero, both values are already local
- causal-local: `gap = session_index - local_log_index`
- causal-majority: `gap = session_index - commit_index`
- linearizable and write concerns: no gap dimension, they wait on rounds and acks rather than indices, so they always use the `gap <= 0` bucket. A linearizable read does also wait for the session frontier, but that wait overlaps the leadership round and is dominated by it, and splitting the level's samples across rarely fed gap buckets would leave the scorer pricing it from cold cells exactly when the gap is large.

Bucket the gap coarsely based on the maximum batch size, for example into buckets for `gap <= 0`, `1..2000`, `2001..20000`, `> 20000`. Everything at or below zero lands in the same bucket, whose observed waits are near zero, so the case where an upgrade is free emerges from measured data rather than from a special rule.

The histogram cell `H[c][gap_bucket]` is now the price of level `c` for this request. Its CDF gives expected profit in step 4, its running mean gives occupancy cost.

### Step 4: score every legal level

Do this per level, independently. Symbols: the SLA is a set of rungs `r_j = (kappa_j, delta_j, pi_j)`, meaning consistency requirement, end-to-end latency threshold in milliseconds, and profit. `rho` is the client's RTT estimate from the request. `F_c` is the CDF of `H[c][gap_bucket]`, so `F_c(x)` is the fraction of recent executions of level `c` at this gap that finished within `x` milliseconds. `omega_c` is that cell's running mean, the occupancy cost in milliseconds. `lambda` is the current shadow price in profit per millisecond, initialized to zero. When a cell has no samples, treat the level as free and certain: omega_c = 0 and F_c(x) = 1 everywhere. Cap the number of requests concurrently riding an uncalibrated cell at a small number, since samples arrive only on completion.

**4a. Restrict to satisfiable rungs.** `R(c) = { j : c is at least as strong as kappa_j }`. A level satisfies its own rungs and every weaker one, which is why stronger levels can earn more.

**4b. Convert thresholds to the server's clock.** For each `j` in `R(c)`, set `d_j = delta_j - rho`. Discard any rung with `d_j <= 0`; the network alone already consumed that budget.

**4c. Sort and deduplicate.** Sort survivors by threshold ascending to get `d_(1) <= d_(2) <= ... <= d_(k)` with profits `pi_(1) ... pi_(k)`. Set `d_(0) = 0`.

**4d. Suffix maximum.** `M_i = max{ pi_(i), ..., pi_(k) }`, with `M_(k+1) = 0`.

Use the maximum rather than `pi_(i)` because beating a tight threshold means beating every looser one, so the request claims the best-paying rung among all it satisfied. This also keeps the formula correct for SLAs where a looser rung happens to pay more. Collapse identical thresholds to one entry holding the suffix max; duplicates otherwise produce empty intervals that contribute nothing.

**4e. Expected profit.**

```
E_c = sum over i of  M_i * ( F_c(d_(i)) - F_c(d_(i-1)) )
```

Implement the equivalent by-parts form, which needs only `k` CDF lookups and no differencing:

```
E_c = sum over i of  ( M_i - M_(i+1) ) * F_c(d_(i))
```

Read `F_c` from the cached prefix-sum array, interpolating linearly within the bucket that contains `d_(i)`.

**4f. Score.**

```
V_c = E_c - lambda * omega_c
```

`E_c` is expected profit, `omega_c` is milliseconds of slot time, `lambda` converts one into the other, so `V_c` is net profit after paying for the capacity the level consumes.

**4g. Choose.** `c* = argmax over legal c of V_c`.

Worked example, for testing. SLA rungs (linearizable, 150, 10), (causal-majority, 150, 6), (causal-majority, 400, 4), (eventual, 400, 1), (eventual, 150, 5), with `rho = 20`, so server-side thresholds are 130 and 380. After 4a through 4d: eventual collapses to 130 to 5 and 380 to 1; causal-majority to 130 to 6 and 380 to 4; linearizable to 130 to 10 and 380 to 4. With `F_eventual(130) = 0.99, F(380) = 1.00, omega = 2`; `F_cm(130) = 0.90, F(380) = 0.99, omega = 40`; `F_lin(130) = 0.70, F(380) = 0.97, omega = 80`, the expected profits are 4.96, 5.76, and 8.08. At `lambda = 0.02` the scores are 4.92, 4.96, and 6.48, so linearizable wins. At `lambda = 0.08` they are 4.80, 2.56, and 1.68, so eventual wins. Nothing switched modes; the price moved.

### Step 5: admit or reject, and charge

Reject if `max over c of V_c <= 0`. Nothing is worth what it would consume at the current price. This is the admission control: under light load `lambda` is near zero and nothing is refused, under overload `lambda` rises and the least valuable requests are shed first, in value order rather than arrival order.

Two hard backstops exist to cover model error and react within a single request rather than within a control interval. The occupancy hard cap is the first and was already applied at ingress (step 1). The replication bucket is the second and is checked here, for writes only, after the scorer has chosen the level: a write whose bucket is empty is rejected, and any cold-start rider the choice took is released. A write the scorer admits can therefore still be refused by the bucket, and the server's drain stream counts the two causes apart.

On admission, the bucket has already been charged one entry for a write; open the occupancy slot by crediting the wait since `t_recv` and updating the accumulator:

```c
// per worker thread; sum A across workers at the interval boundary
long A     = 0;        // accumulated request-microseconds
int  n     = 0;        // requests currently in flight
long tLast = now();

void on_event(int delta) {      // +1 on admit, -1 on reply
    long t = now();
    A += (long)n * (t - tLast); // close out the stretch that just ended
    n += delta;
    tLast = t;
}

void on_admitted(long waited) { // waited = now() - t_recv
    A += waited;                // the slot is backdated to receipt
    on_event(+1);
}
```

Call `on_admitted(now() - t_recv)` here. An admitted request's slot time thus runs from `t_recv` to reply, which is what the calibration of S_max measures, while a request refused at ingress, in the queue, by the scorer or by the bucket adds nothing to `A`. No sampling is involved: the code that changes `n` is the code that records how long the previous value lasted.

### Step 6: execute

Wait for whatever the chosen level requires, then read or apply. Eventual levels wait for nothing. Causal levels wait for the relevant index to advance. Linearizable waits for a leadership confirmation round, and on a follower additionally for its own applied index to reach the read index. Write concerns wait for the required number of acknowledgments.

These are execution mechanics, not decision points. The decision was made in step 4 and is not revisited.

Bound the wait by the loosest server-side threshold computed in step 4b, clamped to the configured `maxWaitMs` backstop (500 ms in the reference configs). The clamp caps how long any request can hold a slot waiting; a rung whose threshold exceeds it falls back earlier than its threshold alone would allow. If the bound expires, abandon the wait, fall back to the strongest level that needs no waiting, and serve immediately. A request that waits past every threshold will deliver zero profit whatever it does, so continuing to hold a slot for it is pure loss.

### Step 7: grade after the fact

Determine what was actually delivered, not what was targeted. Reads frequently satisfy a stronger level than the one chosen, and that profit is free. In the implementation this grading, and the profit ledger built from it, run in the client, computed from the delivered level and the log and commit indices carried on the reply; the server's own sample filing in step 8 is unaffected.

For a read, take the returned value's index. If it is at or below the commit index, the majority rungs are satisfied as well as the local ones. If the replica turned out to be at or ahead of the client's session index, causal is satisfied even if eventual was executed.

Realized profit is the highest-profit rung whose consistency requirement the delivered result meets and whose threshold the total, service time plus `rho`, came in under.

Writes cannot be graded this way. Once acknowledged at wc:1 the decision is spent.

### Step 8: reply, release, record

Reply with the fields listed in the protocol section, including the measured service time `T = now() - t_recv`.

Call `on_event(-1)` to close the occupancy slot.

File the sample: in cell `H[c*][gap_bucket]`, where `c*` is the level actually executed and `gap_bucket` is the one it ran under, increment the bucket containing `T`, and add `T` to that cell's running sum and 1 to its running count. Three operations, no rebuild. Compute `omega` from the running sum over the running count, never from bucket midpoints.

Do not pre-aggregate samples per interval before filing them. Profit depends on which latency band the request lands in, so the distribution is the thing being used, and averaging first destroys it.

If the request abandoned its wait in step 6, file the sample into the cell of the level originally chosen, at the timeout value d_max, and file nothing under the fallback level, whose measured time is dominated by the abandoned wait and says nothing about what it costs.

## Background tasks

### Histogram refresh

Histogram cells use fixed geometric bucket edges, for example 64 buckets from 0.5 ms with ratio 1.15, so the bucket index for `x` is `floor(log(x/0.5)/log(1.15))` and no search is needed.

On each tick, multiply every bucket count, running sum, and running count by a decay factor, then rebuild the prefix-sum array and publish it. Old observations fade geometrically, giving an effective memory of roughly `interval / (1 - decay)`, so 100 ms and 0.95 gives about 2000 ms; tune the decay for the memory you want. The reference configs use 0.9, about 1000 ms.

That memory holds only while samples arrive. Decay rescales every bucket alike, so a cell nobody samples keeps its shape indefinitely, and the scorer samples only the level it chooses: a level that once looked slow would never be chosen again to show it has recovered. So a cell is emptied once even its newest sample has decayed to 1% of its weight, after the first `n` idle ticks with `decay^n <= 0.01` (44 ticks, about 4.4 s, at 0.9). It is then uncalibrated, free and certain under the cold-start rule, and the rider cap bounds the requests that probe it. A cell that keeps receiving samples, however rarely, keeps its evidence. Both stores apply the rule to every cell, including mongod's per-shape execution cells.

Requests read the published snapshot and never block on the refresh. The snapshot is up to one tick stale, which is far below the estimate's own variance. The grid is 5 read levels plus majority write concerns, times 4 gap buckets, but only the two causal levels use more than the first gap bucket, so 14 of the 32 cells ever hold samples (13 in a 3-node cluster). The refresh sweeps every allocated cell regardless, so at 64 buckets a tick is 2048 bucket decays plus as many prefix-sum entries, a few thousand operations.

### Price controller, once per control interval

At the boundary, close out the final stretch of each worker's accumulator, sum them, and compute utilization:

```c
// tEnd is the interval boundary, T its length
A += (long)n * (tEnd - tLast);
tLast = tEnd;
double u = (double)A_summed_across_workers / ((double)S_max * T);
A = 0;
```

`u` is the time-weighted average number of requests in flight divided by `S_max`. Time weighting matters: a count of 10 held for 90 ms and 50 held for 10 ms averages to 14, not 30, and a boundary sample would have read 50.

Then update the price:

```
lambda <- max( lambda_min, lambda * exp( eta * ( u - u_target ) ) )
```

with `u_target` around 0.85, `eta` setting the reaction speed (the reference configs use 0.1; 1 reacts within a couple of intervals but overshoots more), and `lambda_min` a small positive constant; 0.0001 works for rung profits on the order of 1 to 100. The floor does two jobs: it lets `lambda` start at zero, since the multiplicative update on its own would leave zero unchanged forever, and it stops `lambda` decaying so far during idle periods that recovery takes many intervals.

Multiplicative rather than additive because the right price spans orders of magnitude and because it can never go negative. A controller rather than a closed form because profit units are whatever the applications say they are, so a closed form would need a scale constant that is actually accurate. `lambda_min` is the one constant that remains, and it only has to be the right order of magnitude, since the controller finds the operating price from there.

Requests never read `u`. They read `lambda`, which is up to one interval stale by design, since a price recomputed per request would jitter and turn the ranking into noise. The hard occupancy cap at ingress (step 1) and the replication bucket in step 5 cover the fast timescale.

Compute `u` and `lambda` per node. Occupancy is local.

As a cross-check during development, the sum of service times of requests completing in the interval, divided by `S_max * T`, approximates the same quantity, with boundary bias when service times are comparable to the interval length.

## Write semantics

In Raft every accepted write takes the same replication path, so wc:1 and wc:majority are the same write imposing the same load. Only the moment of acknowledgment differs. A write upgrade therefore costs zero replication and pure occupancy, which is why a flat per-write cost would make write upgrades unconditional.

Holding the acknowledgment until majority commit closes the loss window, costs latency, and is gated by the SLA thresholds exactly like a read upgrade. That is the write upgrade.

An optional lower rung is acknowledging at wc:1 and letting the client learn later that the entry committed, from the commit index piggybacked on subsequent replies. This does not close the loss window, it only reports the outcome afterwards, so it is worth less and is semantically different. Note that a commit index alone confirms success but cannot detect failure, since a leader change can replace the entry at that index: detecting loss requires the acknowledgment to carry term and index together, and a check that the committed entry at that index still has the expected term. If that turns out to need per-session server state, drop this rung.

## The MongoDB instantiation

The same design runs inside a patched mongod (MongoDB master at commit 6d990ba, September 2026; the patch is one module, `db/chameleon/`, plus hooks in the command entry point). The decision algorithm, the SLA format, the price controller, the histograms, the occupancy meter, the replication bucket, the client routing rules, the grading rules and the phase schedule are unchanged. The arms differ only in who decides: the client-decided arms put the level and write concern on the wire themselves, and chameleon sends the request envelope (application, SLA, RTT estimate, frontier, wait bound) in the command's generic `comment` field and reads the server's choice back from a `$chameleon` field appended to the reply. The measurement plane (timers, histograms, occupancy, price) runs in every arm, so the five arms share one binary and one cost; MongoDB's own overload controls are left at their defaults.

Three premises of the design above do not hold in MongoDB, and the port states them once:

- Linearizable reads are primary-only and are confirmed by appending a no-op oplog entry after the read and waiting for a majority acknowledgement of it. There is no ReadIndex and no follower-served linearizable read. The replication bucket therefore charges one entry per linearizable read on mongod, where on the Raft store it charges none.
- A write produces one oplog entry per modified document, so its replication charge is not known at admission. The bucket charges one entry at admission and trues up at completion from the count of documents modified; for the point-update workload the two are equal.
- Service time is not a function of the level alone. Latency under level c is exec(shape) + wait(c, gap): the execution cost depends on the query shape and not on the level, and the waits (read concern, linearizable confirmation, write concern) are single, per command and independent of the query. The histogram is factored accordingly, one execution cell per shape and one wait cell per (level, gap bucket), combined per request. With point operations only, the execution cell is one and the combination is a shift by its mean.

The levels map onto MongoDB's read and write concerns as follows; the "wait" column is what the level costs beyond execution, which is what the wait histogram measures.

| design level | MongoDB request | wait |
|---|---|---|
| eventual-local | readConcern local, no afterClusterTime | none (reads the secondary's lastApplied snapshot, the primary's untimestamped state) |
| eventual-majority | readConcern majority, no afterClusterTime | until a majority-committed snapshot exists, usually none |
| causal-local | readConcern local + afterClusterTime T | lastApplied >= T |
| causal-majority | readConcern majority + afterClusterTime T | commit point >= T |
| linearizable | readConcern linearizable, primary only | the no-op write and its majority acknowledgement, after the read |
| wc:k | writeConcern w:k | k nodes at lastApplied >= the write's optime |
| wc:majority | writeConcern w:majority | committed snapshot >= the write's optime |

Two further differences are behavioural rather than structural and are reported as such. The session frontier on mongod is the hybrid logical timestamp the driver returns as operationTime on every reply, which is the serving node's lastApplied rather than the read value's own position, so causal gaps are larger than in the Raft store, where a read advances the frontier only to the value's index. And lastApplied on a secondary advances at oplog batch boundaries, so the causal wait histogram has a floor of one batch. The frontier is carried by the harness and not by the driver's causal sessions: the transport writes it into afterClusterTime for the causal levels and advances it from the reply's timestamps, exactly as the KV protocol does. Gaps are afterClusterTime against the node's lastApplied (causal-local) or its commit point (causal-majority), converted to milliseconds of lag from the wall time stored beside each optime and bucketed with the same gap edges rescaled to time.

Where the decision sits: in the command entry point, after the command's read and write concern have been extracted and after MongoDB's own ingress admission, and only for commands carrying the envelope. A secondary excludes linearizable from the legal set; a write arriving at a secondary fails with NotWritablePrimary, which the client treats as the redirect. The scorer is a C++ port of the rung scorer over the combined CDF, verified against the step 4 worked example. A request is rejected when the occupancy hard cap is reached (checked on arrival at the hook, before the scorer, in every arm), when no level has positive value, or when a write finds the bucket empty after its level is chosen; the rejection is an error carrying MongoDB's SystemOverloadedError label, so the driver treats it as terminal rather than retrying, which is the Raft store's rule that rejections are never retried. The chosen level overwrites the command's read concern (keeping afterClusterTime for the causal levels and dropping it for the eventual ones) or its write concern (w:k with a wtimeout equal to the wait bound). A read concern wait that expires downgrades the read to local without afterClusterTime and the command proceeds; a linearizable confirmation that expires returns the value already read, marked local; a write concern that expires reports the number of nodes that had reached the write's optime as the delivered concern. In every case the reply is graded at what was delivered and the sample is filed to the chosen level's cell at the bound, as in step 8.

Write majority on a large set. MongoDB allows at most seven voting members, so on a replica set larger than seven the harness initiates the first seven as voters and the rest with votes 0 and priority 0: they replicate and serve reads but neither elect nor count toward the commit point. A write at the majority rung is therefore sent as w:"majority" on both the client-decided and the server-decided paths, never as a count, because a count is also satisfied by non-voting acknowledgements; the majority is a majority of the voters, four of seven, where the Raft store's is N/2 + 1. The reply reports the count, and the SLA tables of the scaling configs write the rung at each store's own majority.

The client side differs in one structural way from the Raft transport: the MongoDB driver holds one pooled connection, and the server one thread, for every in-flight request, for its whole latency, where the framed transport multiplexes. Each client site is four driver clients bound to the site's source address so that the per-site delay rules apply, a session pinned to one client for its whole life and the four sharing one cluster clock, because the driver checks out a pool's connections through one thread per pool and one client per site capped a (site, server) pair at about 40,000 to 50,000 checkouts per second; reads name their node through a read preference plus a per-node tag so the driver's own selection collapses to the node the routing rule chose, and writes target the primary. What the connection-per-request model does to capacity, to sMax, and to what the price can see is in CALIBRATION.md (*mongod: build provenance, overhead and the client transport*).

## Instrumentation to build in

Predicted versus realized profit per request, since profit now depends on a latency prediction and a misprediction is an accounting error, not just a missed deadline.

Fraction of upgrades that were free (gap at or below zero) versus waiting, swept against write load and key skew.

Per-level tail latency, not averages.

Which rung was satisfied, per request. This is the metric that makes a head-to-head against Pileus direct, since both systems consume the same declarative input.
