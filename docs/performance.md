# Performance

## Environment

| | |
|---|---|
| Machine | Cloud VM, 4 vCPU Intel Xeon @ 2.10 GHz, 15 GB RAM, shared tenancy |
| JVM | OpenJDK 21.0.13, default G1 GC, `-Xms2g -Xmx2g` |
| Harness | JMH 1.37, 1 fork, 3 × 2 s warm-up, 5 × 2-3 s measurement |
| Raw data | [`benchmarks/jmh-2026-11-25.json`](benchmarks/jmh-2026-11-25.json) |

A shared 4-vCPU VM is noisy (see the error bars) and small: the end-to-end benchmark runs 8 client
threads and 4 shard threads on 4 vCPUs, so it is CPU-oversubscribed. Treat these as a **lower bound**
and as relative comparisons. Reproduce with `java -jar engine-benchmarks/target/benchmarks.jar`.

## 1. Matching core (single thread, no queue, no I/O)

`OrderBookBenchmark` calls `OrderBook.process` directly. This is the ceiling for one shard.

| Benchmark | What one op is | Throughput | p50 | p99 | p99.9 |
|---|---|---|---|---|---|
| `placeAndCancelPassive` | 1 command (avg of place + cancel) on a 20×5-deep book | **3.65 M ops/s** (± 1.8) | 0.12 µs | 0.66 µs | 16 µs |
| `restThenFullyFill` | 1 command (avg of rest + market take with ≥ 1 trade) | **1.69 M ops/s** (± 1.0) | 0.23 µs | 1.15 µs | 20 µs |
| `mixedWorkload` | 1 command from `WorkloadGenerator` (incl. generation) | **1.18 M ops/s** (± 0.6) | 0.46 µs | 3.3 µs | 49 µs |
| `mixedWorkloadGeneratorOnly` | generation only (baseline) | 21.5 M ops/s | 0.08 µs | 0.21 µs | 11 µs |

Notes:

* Sample-mode latency is per invocation; for the two-command benchmarks one sample is a pair.
* The mixed workload keeps adding passive GTC orders faster than it cancels them, so the book deepens
  through each iteration. That is pessimistic compared to a real book in equilibrium.
* The p99.9 tail (10-50 µs) is dominated by GC and allocation (records, boxed `Long` keys,
  `LinkedHashMap` entries), not by the algorithm. See the roadmap below.

## 2. End-to-end engine (closed loop, 8 client threads, 4 shards, 16 symbols)

`EngineBenchmark` measures `engine.submit(cmd).join()`: queue hand-off, matching, journal append and
commit, future completion and wake-up of the waiting client. Each client waits for its ack before
sending the next order (closed loop), so throughput = clients / latency.

| Journal | Throughput | mean | p50 | p95 | p99 | p99.9 |
|---|---|---|---|---|---|---|
| none (`Journal.noop()`) | **131 k cmd/s** | 62 µs | 50 µs | 125 µs | 187 µs | 762 µs |
| file, page cache (`OS_CACHE`) | **116 k cmd/s** | 70 µs | 56 µs | 141 µs | 214 µs | 810 µs |
| file, `fsync` per commit (`FSYNC`) | **16 k cmd/s** | 556 µs | 495 µs | 942 µs | 1.5 ms | 4.1 ms |

The demo (64 virtual-thread clients, see README) reaches **~283 k cmd/s** with the page-cache journal on
the same machine, because more concurrent clients produce bigger batches.

## Analysis

**The book is not the bottleneck.** One shard can match over a million mixed commands per second, but
end-to-end throughput is about a tenth of that. The difference is thread hand-off: every command crosses
from a client thread to a shard thread and back (lock in `ArrayBlockingQueue`, park/unpark of the
shard, `CompletableFuture` completion, unpark of the client). With 12 busy threads on 4 vCPUs, most of
the 50 µs median is waiting to be scheduled.

**The journal is cheap when it is just a write.** Adding the file journal in page-cache mode costs
~12% throughput and ~6 µs at the median: encoding plus one `write` syscall per *batch*.

**`fsync` dominates when enabled**, as expected: ~0.5 ms per commit on this VM's disk.
Group commit is what keeps this usable. Without it, throughput would be capped at roughly
1 / fsync latency ≈ 2 k cmd/s per shard; with it, 4 shards reach 16 k cmd/s with only 8 clients, and
throughput rises with concurrency because each `fsync` covers more commands (the demo with 64 clients
and `--fsync` reached ~61 k cmd/s). On a local NVMe drive with power-loss protection, `fsync` is
typically tens of microseconds and the gap narrows sharply.

**Recovery** replays ~0.9 M events/s (demo: 2.66 M events / 197 MB in 2.9 s), so a day of heavy trading
on one shard rebuilds in seconds. Snapshots would make it independent of history length
(scalability.md).

## Optimisation roadmap (in expected order of payoff)

| Change | Targets | Expected effect |
|---|---|---|
| Busy-spin / `LockSupport.parkNanos` back-off in the shard loop, ring buffer (Disruptor/JCTools) instead of `ArrayBlockingQueue` | hand-off latency | Large drop in end-to-end median; this is where most time goes today |
| Pin shard threads to isolated cores | scheduler jitter, p99.9 | Removes most of the tail on a dedicated box |
| Pipeline journaling on its own thread (match batch *n+1* while `fsync`-ing batch *n*) | `FSYNC` throughput | Overlaps disk and CPU |
| Array price ladder + intrusive order lists + primitive maps | allocation, GC tail | Allocation-free hot path; flatter p99.9 in the core benchmarks |
| Pooled/flyweight events, off-heap encoding | allocation | Same |
| ZGC (generational) | GC pauses | Sub-millisecond pauses with no code change; worth measuring first |

Each of these is localised: the shard loop, the `PriceLevel`/`OrderBook` internals, or the journal.
None changes the public API or the determinism guarantees.
