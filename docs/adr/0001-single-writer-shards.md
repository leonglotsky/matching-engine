# ADR-0001: Single-writer shards partitioned by symbol

* Status: accepted
* Date: 2026-11-25

## Context

Order books are hot, mutable, and need strict per-symbol ordering. Many clients submit concurrently.
We need (a) deterministic results for a given input order, (b) high throughput on commodity hardware,
(c) no lost updates or torn reads, and (d) a design that can grow beyond one machine.

## Decision

Partition symbols across N shards with `floorMod(symbol.hashCode(), N)`. Each shard is owned by exactly
**one platform thread** that is the only reader and writer of its books and journal. Clients hand
commands over through a bounded `ArrayBlockingQueue` and get a `CompletableFuture` back.

## Alternatives considered

| Option | Why not |
|---|---|
| One global lock around all books | Serialises unrelated symbols; throughput capped at one core; lock convoying under load. |
| Lock per book (`synchronized`/`ReentrantLock`) | Works, but the arrival order at the lock is the execution order, and that order is not recorded anywhere, so replay cannot reproduce it unless we add a sequencer anyway. Also pins virtual threads when `synchronized` (Java 21). |
| Lock-free book (CAS on levels) | Very hard to get right for multi-step operations (sweep several levels atomically), hard to reason about, and still nondeterministic. |
| LMAX Disruptor ring buffer | Lower hand-off latency than `ArrayBlockingQueue`, but adds a dependency and a lot of ceremony for a portfolio project. The `Shard` boundary is where it would slot in (see performance.md). |
| Actor framework (Akka/Pekko) | Same model, heavy dependency, licence churn. |

## Consequences

* No locks or atomics on the matching path. `OrderBook` is plain single-threaded Java.
* The queue gives a total order per shard, which *is* the sequencing. Determinism follows directly.
* Scales with symbols, not with one hot symbol: one symbol can use at most one core. That is the
  standard trade-off in exchange design (a single book is inherently sequential).
* Hash routing is stable across JVMs (JLS specifies `String.hashCode`), but **changing N moves symbols**.
  The journal manifest pins N; resharding is a migration (see scalability.md).
* Shard threads are platform threads on purpose. They are few, long-lived and CPU-bound; virtual threads
  bring nothing there and would share carriers with the client virtual threads.
