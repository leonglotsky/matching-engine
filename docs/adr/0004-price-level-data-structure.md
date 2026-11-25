# ADR-0004: `TreeMap` of price levels, each a `LinkedHashMap` in time order

* Status: accepted
* Date: 2026-11-25

## Decision

Each side is a `TreeMap<Long, PriceLevel>` (bids in reverse order). Each `PriceLevel` holds its orders in
a `LinkedHashMap<Long, RestingOrder>` used through Java 21's `SequencedMap` API (`putLast`,
`sequencedValues`). A `HashMap<orderId, RestingOrder>` indexes live orders.

| Operation | Cost |
|---|---|
| Best price | O(1) amortised (`firstKey`) |
| Add at existing level | O(log L) lookup + O(1) append |
| Fill at the touch | O(1) per maker touched |
| Cancel by id | O(1) + O(log L) if the level empties |

## Alternatives considered

| Structure | Why not (here) |
|---|---|
| `PriorityQueue<Order>` per side | Natural for "best first", but cancel is O(n) and iteration order is not sorted, so sweeping levels and producing depth snapshots is awkward. |
| Array-indexed price ladder (`PriceLevel[]` around a reference price) | Fastest option (O(1) everything, cache friendly) and what production engines use for instruments with a bounded tick range. Needs re-centering and a bound on the price band. Good next step if profiling demands it. |
| `ConcurrentSkipListMap` | Pays for concurrency we do not need (single writer). |
| Intrusive doubly-linked list per level | Removes the `LinkedHashMap` entry allocation; worth it for the last 20-30%. `SequencedMap` keeps the code obvious for now. |

## Consequences

Clear, idiomatic code that is fast enough for hundreds of thousands to millions of operations per second
per shard (see performance.md), with an identified upgrade path.
