# ADR-0005: Group commit, and acknowledge only after the journal commit

* Status: accepted
* Date: 2026-11-25

## Decision

A shard drains up to `maxBatchSize` commands, processes them all in memory, appends all their events,
calls `journal.commit()` once, and only then completes the futures. Durability strength is a policy:
`OS_CACHE` (write to the page cache) or `FSYNC` (`FileChannel.force` per commit).

## Why

* **Correctness**: a client must never be told "filled" for something a restart would forget. Acking
  after commit makes "acknowledged ⊆ journaled" hold by construction.
* **Throughput**: `fsync` costs tens of microseconds to milliseconds. One `fsync` per batch instead of
  per command amortises it. Batching is adaptive: under light load batches are size 1 (lowest latency),
  under load they grow (highest throughput). No timers, no tuning knob that trades latency for
  throughput at idle.

## Alternatives considered

* **Ack before write** (async journal): lowest latency, but acknowledged trades can vanish on crash.
  Unacceptable for a matching engine.
* **fsync per command**: simplest and safest, but throughput collapses to the device's fsync rate.
* **Linger timer (Kafka `linger.ms` style)**: adds latency at low load to gain bigger batches; the
  drain-what-is-there approach gets most of the benefit without the penalty.

## Consequences

* If a batch fails part-way, in-memory books are ahead of the journal. The shard therefore halts
  (fail-stop) rather than continue from a state nobody can recover. See failure-scenarios.md.
* Measured in performance.md: `FSYNC` vs `OS_CACHE` vs no journal.
