# ADR-0002: Event sourcing with a pure `decide` and a single `apply`

* Status: accepted
* Date: 2026-11-25

## Context

We need recovery after a crash, an audit trail of everything that happened, and confidence that the
recovered state is *exactly* what clients were told.

## Decision

The journal stores **events** (`OrderAccepted`, `TradeExecuted`, `OrderRested`, `OrderCancelled`,
`OrderRejected`, `CancelRejected`), not snapshots and not raw commands. The order book is written as a
decider:

* `decide(state, command, timestamp) -> events` is pure and does not mutate.
* `apply(state, event)` is the only mutation path and is used by both live processing and replay.

Events carry a per-symbol gap-free sequence number, and `apply` validates every event against the
current state (sequence continuity, contiguous ids, maker exists, quantities fit).

## Alternatives considered

| Option | Trade-off |
|---|---|
| **Command sourcing** (journal inputs, re-execute on replay, LMAX style) | Smaller journal and simpler to write. But replay re-runs the matching code, so a bug fix or behaviour change in matching silently changes history on replay. Events are facts and survive code changes. |
| Periodic state snapshots only | Loses everything between snapshots; no audit trail. Snapshots are still worth adding *on top* of the log for restart speed (scalability.md). |
| Write-through to a database (MySQL row per order) | One synchronous round trip per change; orders of magnitude slower; the DB becomes the bottleneck and the concurrency problem. Good for downstream reporting, fed from the event log. |
| Mutate during matching, emit events as a side effect | Common and faster (one pass), but replay then needs a *second* code path that mutates from events, and the two paths can drift. Here there is only one. |

## Consequences

* Recovery is a fold over the journal; tests prove recovered books equal live books.
* Decide-then-apply walks the touched levels twice. The second pass hits data the first pass just
  loaded, and the whole book step is a fraction of the end-to-end cost (compare `OrderBookBenchmark`
  with `EngineBenchmark` in performance.md). Worth it for a single mutation path.
* The event stream doubles as the market data / drop-copy feed for downstream consumers.
* Event schemas are now a compatibility contract. `EventCodec` pins tags and enum codes explicitly and
  the file header carries a format version.
