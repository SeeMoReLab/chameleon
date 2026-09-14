# Experiment workload: topology, applications, SLAs, phases

Reference configuration for the evaluation.

Every application uses 60 sessions, each belonging to one application and one client site, and holding one counter, its causal frontier: the highest log index of anything it has written or read, advanced on every acknowledged write and every delivered read value. The key space is shared across applications, so all of them contend on the same keys. Sessions are long-lived and never reset. An arriving request picks one session of its application uniformly, which also determines where it is issued from. Injection is open loop at a fixed rate: a deterministic per-tick budget every 100 ms, not Poisson arrivals, and a stalled injector drops its missed budget rather than bursting to catch up. Rejections are terminal and never retried; redirects and transport failures are resent up to the configured retry limit.

## Topologies

Three configurations, run with identical SLAs and mixes so that the only thing changing is where the nodes are. All delays are **one-way**, which is what netem takes; apply the value at each end of a pair, not double at one end. They are representative of real inter-region latencies rather than a uniform injected value, which matters because a reviewer already objected that uniform netem cannot reproduce asymmetric paths.

Five nodes in every config, leader always at A. A majority is the leader plus two, so a quorum operation waits for the second acknowledgement to return.

### Local: five AZs in one region

Every server pair at **0.3 ms**. Five client sites, one per AZ, each 0.1 ms to its own node and 0.3 ms to the others. No remote client site: inside one region every node is equidistant, so there is no placement choice to defend and no routing question to ask, which is what makes this the isolation config for capacity.

Quorum cost about **1.2 ms** round trip.

### Regional: five North American regions

Nodes: A Virginia (leader), B Ohio, C Montreal, D N. California, E Oregon.

| server to server | A | B | C | D | E |
|---|---|---|---|---|---|
| A Virginia | 0 | 6 | 10 | 30 | 35 |
| B Ohio | 6 | 0 | 12 | 25 | 25 |
| C Montreal | 10 | 12 | 0 | 38 | 35 |
| D N. California | 30 | 25 | 38 | 0 | 10 |
| E Oregon | 35 | 25 | 35 | 10 | 0 |

Six client sites: one colocated with each node, plus Phoenix with no local replica.

| client to server | A | B | C | D | E |
|---|---|---|---|---|---|
| Virginia | 0.5 | 6 | 10 | 30 | 35 |
| Ohio | 6 | 0.5 | 12 | 25 | 25 |
| Montreal | 10 | 12 | 0.5 | 38 | 35 |
| N. California | 30 | 25 | 38 | 0.5 | 10 |
| Oregon | 35 | 25 | 35 | 10 | 0.5 |
| Phoenix | 25 | 20 | 28 | 10 | 15 |

Quorum is the leader plus Ohio and Montreal, about **20 ms** round trip. Replica lag runs 6 ms at Ohio to 35 ms at Oregon.

### Global: five regions worldwide

Nodes: A Virginia (leader), B Oregon, C Ireland, D Singapore, E São Paulo.

| server to server | A | B | C | D | E |
|---|---|---|---|---|---|
| A Virginia | 0 | 30 | 40 | 115 | 60 |
| B Oregon | 30 | 0 | 65 | 85 | 90 |
| C Ireland | 40 | 65 | 0 | 90 | 95 |
| D Singapore | 115 | 85 | 90 | 0 | 160 |
| E São Paulo | 60 | 90 | 95 | 160 | 0 |

Six client sites: one colocated with each node, plus Tokyo with no local replica.

| client to server | A | B | C | D | E |
|---|---|---|---|---|---|
| Virginia | 0.5 | 30 | 40 | 115 | 60 |
| Oregon | 30 | 0.5 | 65 | 85 | 90 |
| Ireland | 40 | 65 | 0.5 | 90 | 95 |
| Singapore | 115 | 85 | 90 | 0.5 | 160 |
| São Paulo | 60 | 90 | 95 | 160 | 0.5 |
| Tokyo | 85 | 50 | 110 | 35 | 130 |

Quorum is the leader plus Oregon and Ireland, about **80 ms** round trip. Replica lag runs 30 ms at Oregon to 115 ms at Singapore.

## Expected level costs

| Operation | Local | Regional | Global | Why |
|---|---|---|---|---|
| eventual-local, eventual-majority | 1 to 3 ms | same | same | Local read, no coordination |
| causal-local, causal-majority | plus wait | plus wait | plus wait | Local read unless the session's index is ahead of the replica |
| linearizable | 1.2 ms + | 20 ms + | 80 ms + | ReadIndex confirmation round to a majority |
| wc:1 | 1 to 3 ms | same | same | Local append |
| wc:2 | 1.2 ms + | 12 ms + | 60 ms + | One acknowledgement, from the nearest follower |
| wc:majority | 1.2 ms + | 20 ms + | 80 ms + | Two acknowledgements |

Add the client's own round trip on top. A colocated client pays 1 ms; a Singapore client pays 230 ms just to reach the leader.

The linearizable row prices the leader-served path. With follower linearizable reads enabled (the reference configs enable them), a follower serves the same guarantee by fetching a confirmed read index from the leader and waiting for its own commit index to catch up, so a client near a follower pays that follower's round trip to the leader, plus any catch-up wait, instead of its own round trip to the leader.

Quorum cost across the three configs is roughly 1.2, 20, and 80 ms, about two orders of magnitude. With the SLA thresholds held fixed, that is what makes rungs progressively unreachable and turns the choice of delay into a swept parameter rather than something to defend.

Phoenix and Tokyo are the same probe at two scales: in both, the nearest node is also the stalest, so least-RTT routing takes the close-and-lagging option while a freshness-aware policy pays a little more distance for much fresher data. That is the cleanest single test of routing quality in the setup, and it mirrors Pileus's China client, the only one of their four with no local replica.

Leader placement is a cheap extra sweep: moving the leader to Singapore in the global config inverts the cost landscape and is the natural configuration for the flash sale.

## Client sites and sessions

Sessions carry a site as well as an application. All four applications spread their 60 sessions evenly across every client site: twelve each across five sites locally, ten each across six sites in the other two configs.

A4 is spread like the others rather than concentrated near the leader. Concentrating it would make its requests both the most valuable and the cheapest to serve, so surviving under load would no longer distinguish value from cost, and its tail rungs already keep distant sites servable.

Without distributed clients every client would see identical distances and the routing comparison would have nothing to measure, so this is load-bearing rather than decorative.

## A1: online shopping

A shopper browsing a catalogue and adding items to a cart. Almost everything is browsing, worth little and stale-tolerant, but occasionally the same person checks out, and at that moment the cart must reflect what they just added and the request is worth far more. Terry's canonical latency-favouring class.

A1 carries three read SLAs, which is what makes value-ordered shedding observable inside one application, on the same node, from the same session pool, over the same key range. The third is active only during flash sale segments.

Write fraction 10 percent.

**slaId 1, browsing.** Prefers session-consistent catalogue data, accepts anything committed, accepts anything at all rather than nothing. This is the only place eventual-majority appears, so without it that level is never rewarded and never chosen.

| Level | Threshold | Profit |
|---|---|---|
| causal-local | 40 | 5 |
| eventual-majority | 40 | 4 |
| eventual-local | 200 | 1.5 |

**slaId 2, checkout.** The cart must reflect what the shopper just added, and it must not reflect state that later rolls back, because money is about to move.

| Level | Threshold | Profit |
|---|---|---|
| causal-majority | 60 | 50 |
| causal-local | 60 | 38 |
| eventual-local | 60 | 15 |

**slaId 3, flash checkout.** Active only during a flash sale. What changes during the event is not value but the correctness requirement: stock is a handful of units and thousands of people are racing for it, so a stale inventory read means selling something you do not have. The floor rises accordingly, from causal-local on an ordinary day to majority-committed here. Patience rises too, since a shopper will wait a second and a half rather than lose a one-dollar car. Profit is only modestly above ordinary checkout, on customer-acquisition grounds, because per unit of revenue a discounted item is worth less, not more.

| Level | Threshold | Profit |
|---|---|---|
| linearizable | 300 | 70 |
| causal-majority | 300 | 52 |
| causal-majority | 1500 | 30 |

There is deliberately no eventual rung. The application would rather be refused than oversell, which is what real flash sales do when they put people in a queue. That makes this the one segment where the price cannot economise by downgrading, because the floor forbids it, and admission is the only lever left. Every other segment gives the price the option of serving something cheaper.

**Writes.** Adding to a cart should be acknowledged instantly; the order record itself must be durable. The last rung exists so that distant clients are servable at all: a Singapore client is 230 ms round trip from the leader, so without it every A1 write from that site would miss all three thresholds, earn zero, and be rejected regardless of load. A shopper adding to a US-hosted cart from Singapore really does wait a quarter second, and it really is worth very little.

| Level | Threshold | Profit |
|---|---|---|
| wc:majority | 200 | 12 |
| wc:1 | 40 | 8 |
| wc:1 | 200 | 2 |
| wc:1 | 500 | 1 |

## A2: advertising-funded search or news feed

Results may be a little stale without anyone noticing, but the reader must not see their own feed go backwards, so the requirement is session ordering rather than durability: causal-local, not causal-majority. A story that later rolls back is not a problem for a feed. Revenue falls as latency rises, so the ladder varies only in the deadline. Terry's consistency-favouring class.

Its writes are billing events, rare but never allowed to be lost, so every write rung demands full durability. That is where A2's greed lives: it never contributes a write upgrade decision, only expensive load, and the price has to keep it from crowding out everyone else.

Write fraction 5 percent.

**Reads.**

| Level | Threshold | Profit |
|---|---|---|
| causal-local | 20 | 5 |
| causal-local | 50 | 4 |
| causal-local | 120 | 2.5 |
| causal-local | 400 | 0.25 |

**Writes.**

| Level | Threshold | Profit |
|---|---|---|
| wc:majority | 120 | 18 |
| wc:majority | 300 | 12 |
| wc:majority | 800 | 5 |
| wc:majority | 2000 | 0.6 |

## A3: login and credential checking

A password check reads credentials, and the standard pattern is a fast weak read first with an authoritative fallback only if the check fails. So the ladder prefers linearizable quickly, takes eventual quickly over linearizable slowly, and puts slow linearizable last. Two rungs share the 150 ms threshold, so a linearizable result must claim the higher-paying one; A3 is the only SLA exercising that deduplication.

Its writes are password changes and session records, where the durability tradeoff is real and graded, and A3 is the only place wc:2 appears.

Write fraction 30 percent.

**Reads.** One ladder, unchanged across scenarios. A login is a login; what a flash sale changes is how many of them arrive, not what each is worth.

| Level | Threshold | Profit |
|---|---|---|
| linearizable | 150 | 9 |
| eventual-local | 150 | 4.5 |
| linearizable | 600 | 2 |

That produces an effect worth a paragraph in the discussion. During the flash segment, logins stay cheap at 9 while flash checkouts carry an expensive floor, so the price sheds logins to protect checkouts, and a customer who cannot log in never reaches the checkout the system was busy protecting. Per-request value allocation is blind to cross-request dependencies, so shedding a cheap prerequisite destroys the valuable thing downstream. It falls out of the workload rather than being arranged, which makes it a finding rather than a demonstration.

**Writes.**

| Level | Threshold | Profit |
|---|---|---|
| wc:majority | 150 | 15 |
| wc:2 | 120 | 10 |
| wc:1 | 40 | 6 |
| wc:majority | 600 | 4 |

## A4: payments and account balances

Low volume, high stakes. A4's distinctiveness lives in its writes and its consistency floor rather than in a blanket multiplier: a balance read is worth about the same as a checkout read, since both gate the same transaction, but a transfer acknowledged and then lost is in a class of its own. A4 is the application that should still be served, and still upgraded, at prices that have shed everyone else.

Write fraction 20 percent.

**Reads.**

| Level | Threshold | Profit |
|---|---|---|
| linearizable | 200 | 50 |
| causal-majority | 100 | 30 |
| causal-majority | 400 | 12 |

**Writes.**

| Level | Threshold | Profit |
|---|---|---|
| wc:majority | 250 | 150 |
| wc:majority | 1000 | 60 |

## SLA reachability

A well-formedness condition worth checking at configuration time rather than discovering at runtime: for every SLA and every client site, at least one rung must be reachable using a level that site can actually serve, given the measured level costs above.

State it in terms of cost rather than ladder position. A3's lowest-profit rung is linearizable within 600 ms, which is the most expensive level in the system, so a check that simply asked whether the bottom rung was reachable would pass A3 for the wrong reason and would fail the moment the leader became unavailable to that client even though a perfectly good local option existed.

Two deliberate exemptions. No write SLA can have a leader-free rung, since writes go to the leader by definition, so for writes the condition reduces to whether the loosest threshold clears the client's round trip plus the concern's cost. And A1's flash checkout has no cheap rung on purpose, because the application would rather be refused than oversell, so it is exempt explicitly.

Checked against the global config from Singapore, the worst site, the only violation was A1 writes, which is why that ladder has a fourth rung at 500 ms.

Because this is checkable statically from the topology and the level cost table, it could run at startup and tell an operator which SLAs are unservable from which sites, instead of the operator finding out when those requests start being rejected. The implementation does not perform this check; it was done by hand against the tables above, and automating it remains an open item.

## Scenario mixes

Weights are relative and normalised to 100. Each mix is a day in the life of the platform rather than a synthetic knob setting, which is why the composition and the rate move together in most of them.

**normal.** An ordinary day. Overwhelmingly browsing, a trickle of checkouts, steady logins, payments the thinnest stream in the system.

```yaml
- { applicationId: 1, type: read,  slaId: 1, weight: 32 }   # browse
- { applicationId: 1, type: read,  slaId: 2, weight: 4 }    # checkout
- { applicationId: 1, type: write, slaId: 1, weight: 4 }
- { applicationId: 2, type: read,  slaId: 1, weight: 33 }
- { applicationId: 2, type: write, slaId: 1, weight: 2 }
- { applicationId: 3, type: read,  slaId: 1, weight: 14 }
- { applicationId: 3, type: write, slaId: 1, weight: 6 }
- { applicationId: 4, type: read,  slaId: 1, weight: 4 }
- { applicationId: 4, type: write, slaId: 1, weight: 1 }
```

**holiday.** The weeks before Christmas. Shopping displaces feed reading, conversion creeps up, everything else follows proportionally. Nothing dramatic; the system simply runs hot for a long time.

```yaml
- { applicationId: 1, type: read,  slaId: 1, weight: 38 }
- { applicationId: 1, type: read,  slaId: 2, weight: 6 }
- { applicationId: 1, type: write, slaId: 1, weight: 5 }
- { applicationId: 2, type: read,  slaId: 1, weight: 26 }
- { applicationId: 2, type: write, slaId: 1, weight: 2 }
- { applicationId: 3, type: read,  slaId: 1, weight: 12 }
- { applicationId: 3, type: write, slaId: 1, weight: 5 }
- { applicationId: 4, type: read,  slaId: 1, weight: 5 }
- { applicationId: 4, type: write, slaId: 1, weight: 1 }
```

**saleDay.** Prime Day or 11.11. Far more browsing converts, cart writes multiply, payments follow, and the feed is ignored. Volume and value rise together, which is the realistic case the earlier synthetic mixes were artificially separating.

```yaml
- { applicationId: 1, type: read,  slaId: 1, weight: 26 }
- { applicationId: 1, type: read,  slaId: 2, weight: 14 }
- { applicationId: 1, type: write, slaId: 1, weight: 12 }
- { applicationId: 2, type: read,  slaId: 1, weight: 16 }
- { applicationId: 2, type: write, slaId: 1, weight: 2 }
- { applicationId: 3, type: read,  slaId: 1, weight: 12 }
- { applicationId: 3, type: write, slaId: 1, weight: 6 }
- { applicationId: 4, type: read,  slaId: 1, weight: 9 }
- { applicationId: 4, type: write, slaId: 1, weight: 3 }
```

**preFlash.** The minutes before a flash sale. Everyone signs in to position themselves, so logins dominate and, because A3 is write-heavy, replication lag climbs right before the burst arrives. Roughly 25 percent writes.

```yaml
- { applicationId: 1, type: read,  slaId: 1, weight: 18 }
- { applicationId: 1, type: read,  slaId: 2, weight: 3 }
- { applicationId: 1, type: write, slaId: 1, weight: 2 }
- { applicationId: 2, type: read,  slaId: 1, weight: 8 }
- { applicationId: 2, type: write, slaId: 1, weight: 1 }
- { applicationId: 3, type: read,  slaId: 1, weight: 40 }
- { applicationId: 3, type: write, slaId: 1, weight: 20 }
- { applicationId: 4, type: read,  slaId: 1, weight: 6 }
- { applicationId: 4, type: write, slaId: 1, weight: 2 }
```

**flash.** A one-dollar car at noon. Browsing collapses, the feed is abandoned, and almost everything is a checkout attempt on a single item under the flash SLA, with logins still arriving heavily under their ordinary ladder. Payments are a trickle because only the winners pay. Roughly 27 percent writes.

```yaml
- { applicationId: 1, type: read,  slaId: 1, weight: 5 }
- { applicationId: 1, type: read,  slaId: 3, weight: 45 }   # flash checkout
- { applicationId: 1, type: write, slaId: 1, weight: 20 }
- { applicationId: 2, type: read,  slaId: 1, weight: 2 }
- { applicationId: 2, type: write, slaId: 1, weight: 1 }
- { applicationId: 3, type: read,  slaId: 1, weight: 18 }
- { applicationId: 3, type: write, slaId: 1, weight: 5 }
- { applicationId: 4, type: read,  slaId: 1, weight: 3 }
- { applicationId: 4, type: write, slaId: 1, weight: 1 }
```

**collapse.** Immediately after. Load falls off a cliff, the losers drift back to browsing, and the winners pay, so payments are briefly elevated.

```yaml
- { applicationId: 1, type: read,  slaId: 1, weight: 25 }
- { applicationId: 1, type: read,  slaId: 2, weight: 6 }
- { applicationId: 1, type: write, slaId: 1, weight: 5 }
- { applicationId: 2, type: read,  slaId: 1, weight: 20 }
- { applicationId: 2, type: write, slaId: 1, weight: 1 }
- { applicationId: 3, type: read,  slaId: 1, weight: 15 }
- { applicationId: 3, type: write, slaId: 1, weight: 6 }
- { applicationId: 4, type: read,  slaId: 1, weight: 15 }
- { applicationId: 4, type: write, slaId: 1, weight: 7 }
```

**payday.** End of month. Balances and transfers surge while browsing stays flat. The mirror image of the greedy-neighbour test: the surging tenant is the most valuable one, so the price should admit it and shed cheap browsing to make room.

```yaml
- { applicationId: 1, type: read,  slaId: 1, weight: 22 }
- { applicationId: 1, type: read,  slaId: 2, weight: 3 }
- { applicationId: 1, type: write, slaId: 1, weight: 3 }
- { applicationId: 2, type: read,  slaId: 1, weight: 22 }
- { applicationId: 2, type: write, slaId: 1, weight: 1 }
- { applicationId: 3, type: read,  slaId: 1, weight: 10 }
- { applicationId: 3, type: write, slaId: 1, weight: 4 }
- { applicationId: 4, type: read,  slaId: 1, weight: 25 }
- { applicationId: 4, type: write, slaId: 1, weight: 10 }
```

## The day sequence

One continuous run of about thirty-one minutes per arm: a 30 s warm-up at normal load, the eight segments below (thirty minutes), and a 30 s cool-down, the bookends excluded from the story. Rates are fractions of measured capacity C so the run is portable. Sessions persist throughout, so history and lag carry across segments, which is realistic and is also why this is the dynamics figure rather than the quantitative one.

The cluster is provisioned the way a cost-conscious operator would provision it: for ordinary peak plus headroom, not for the flash sale. Nobody buys twenty times capacity to sit idle all year, so normal load sits at half of C and the sale day exceeds it.

| Segment | Duration | Rate | vs normal | Mix | What it shows |
|---|---|---|---|---|---|
| Normal day | 300 s | 0.5 C | 1x | normal | Baseline. The price rests on its floor and nearly everything upgrades free |
| Holiday plateau | 300 s | 0.8 C | 1.6x | holiday | Sustained seasonal lift. Tests equilibrium rather than reaction, and is where an oscillation would appear |
| Sale day | 300 s | 1.8 C | 3.6x | saleDay | Prime Day scale. Rate and value rise together, which is the realistic combined case |
| Pre-flash login surge | 120 s | 1.0 C | 2x | preFlash | People gathering and signing in. Write-heavy logins drive lag up just before the burst |
| Flash sale | 60 s | 2.5 C | 5x | flash | Roughly sixty percent of arrivals must be refused, and the floor is expensive, so downgrading is unavailable and admission is the only lever |
| Collapse | 120 s | 0.7 C | 1.4x | collapse | Load vanishes. Does the price come down promptly or strand capacity |
| Payday | 300 s | 0.6 C | 1.2x | payday | Composition shifts while volume barely does. The valuable tenant is the one surging |
| Return to normal | 300 s | 0.5 C | 1x | normal | Recovery, compared directly against the opening segment |

Two deliberate compressions to state in the methodology. A holiday plateau lasts weeks in reality and five minutes here, which is honest because the system's slowest adaptation timescale is a few seconds, set by the histogram and acceptance-rate decay; every segment is at least an order of magnitude longer than that, so each shows its settled state, and the flash segment shows the entry transient and the settled overload behaviour both. And real flash sales reach ten to fifty times normal traffic rather than five, which was capped for load-generation reasons. The binding quantity there is the rejection fraction rather than the multiple of capacity, and at 2.5 C it is already high enough to separate value-ordered from arrival-ordered shedding; a higher peak changes the size of the gap, not its existence or its ordering.

This sequence does not replace a static load sweep, which is the quantitative backbone and is immune to the carryover objection: hold the mix at normal and step the rate from 0.2 C to 2.5 C, 45 seconds per step, reporting delivered profit, rejection rate, and per-level tail latency. The arms are indistinguishable below saturation, so a sweep that never crosses C will show four flat lines.

If experiment time is short, the priority order is the load sweep on the geo topology for all four arms, then the day sequence on geo for all four arms, then the load sweep on the single-region control, then failure injection for the write concern result. The day sequence alone is about thirty-one minutes per arm per topology, so one full replication of the geo config is a matter of hours, not under an hour.

## Notes

Replication lag rises with total write load, and the fraction of causal reads that must wait is roughly `min(1, (w * T / N) * lag)` per application, where `w` is its write fraction, `T` the total rate, and `N` its session count. That is why the write surge changes behaviour without changing the rate.

With 60 sessions for every application regardless of its share of load, per-session activity differs: A2 at 35 percent of traffic has busy sessions with high floors, while A4 at 5 percent has quiet sessions whose counters fall behind, so more of A4's causal upgrades will be free. Its linearizable rung stays expensive regardless, so the A4 story survives, but it will show in the free-versus-waiting split.

One modelling detail pinned down in the implementation: both causal levels anchor on the same session frontier, so causal-majority implies causal-local (a committed view that covers the frontier implies the log does) and the read ladder is a total order. For scoring, count only the rungs a level definitely satisfies, which is conservative. Post-hoc grading will award anything extra that was actually delivered.
