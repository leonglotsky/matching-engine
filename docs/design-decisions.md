# Design decisions

Each decision has a full ADR in [`adr/`](adr/) with context, alternatives and consequences.
This page is the one-screen summary.

| # | Decision | In one line |
|---|---|---|
| [0001](adr/0001-single-writer-shards.md) | Single-writer shards partitioned by symbol | No locks on the matching path; the queue order *is* the sequence, so execution is deterministic. |
| [0002](adr/0002-event-sourcing-with-decide-apply.md) | Event sourcing with pure `decide` and single `apply` | One mutation path for live and replay, so recovered books cannot drift. |
| [0003](adr/0003-fixed-point-prices.md) | `long` ticks and lots | Exact, allocation-free arithmetic; no `double` rounding or `BigDecimal` scale bugs. |
| [0004](adr/0004-price-level-data-structure.md) | `TreeMap` of levels, `SequencedMap` per level | O(1) best price/fill/cancel with readable code; array ladder is the upgrade path. |
| [0005](adr/0005-group-commit-and-ack-after-durable.md) | Group commit, ack after commit | Never acknowledge what a restart would forget, and amortise `fsync` across a batch. |
| [0006](adr/0006-idempotent-submission.md) | Idempotency on (account, clientOrderId) | Retries are always safe, including across restarts and after a shard halt. |
| [0007](adr/0007-plain-java-core.md) | Plain Java 21 core, virtual threads at the edges | Sealed types + exhaustive switches make missing cases compile errors. |

## Smaller decisions

* **Rejections are events.** Invalid orders produce `OrderRejected` in the journal instead of an
  exception, so the audit trail shows what was refused and why.
* **Market orders are IOC limit orders with an unreachable price.** One matching loop for both types.
* **FOK is checked before matching.** `decide` is pure, so it can scan available liquidity without
  side effects and emit `OrderCancelled(FOK_UNFILLABLE)` with no trades.
* **Operational outcomes are values, not exceptions.** `CommandResult` is sealed (`Processed`,
  `Duplicate`, `Overloaded`, `Unavailable`, `UnknownSymbol`), so callers must handle backpressure and
  outages in their `switch`.
* **Bounded queues reject instead of block.** A blocking `put` would push the stall into every client
  thread; an unbounded queue would turn overload into an OOM. `Overloaded` lets the gateway shed load.
* **Hand-written binary codec.** Java serialization is slow and a security risk; JSON is several times larger and slower to parse.
  Enum wire codes are pinned explicitly so reordering an enum cannot corrupt old journals.
* **CRC32C**, not CRC32: hardware-accelerated on modern CPUs and the same choice as Kafka, ext4 and
  iSCSI.
