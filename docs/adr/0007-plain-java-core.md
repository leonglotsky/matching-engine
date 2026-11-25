# ADR-0007: Plain Java 21 core, no framework; virtual threads at the edges

* Status: accepted
* Date: 2026-11-25

## Decision

`engine-core` depends on nothing but the JDK. No Spring, no Kafka, no database. Java 21 features are
used where they make the design safer, not for show:

| Feature | Where | Why it matters |
|---|---|---|
| Sealed interfaces | `Command`, `EngineEvent`, `Outcome`, `CommandResult`, `Shard.Envelope` | The compiler knows every case. Add an event type and `apply`, `EventCodec` and every `switch` stop compiling until handled. |
| Records | All commands, events, snapshots | Immutable facts with structural `equals`, which the determinism tests compare directly. |
| Pattern-matching `switch` + record deconstruction | `OrderBook.decide/apply`, `EventCodec`, `Shard.process` | Exhaustive, no default branch to hide a missing case. |
| Sequenced collections | `PriceLevel` (`SequencedMap.putLast`, `sequencedValues`), `List.getFirst/getLast` | Time priority is expressed in the type, not implied by the implementation class. |
| Virtual threads | Demo clients, concurrency tests | One cheap thread per client connection with plain blocking code (`future.join()`). |

## Why no Spring Boot (yet)

The interesting problems here (ordering, determinism, durability) live below any web framework, and a
library with no dependencies is easier to embed, benchmark and reason about. A REST/WebSocket gateway is
a thin adapter over `submit()` and `snapshot()` and would be its own module, so the core stays clean.

## Why platform threads for shards

Covered in ADR-0001. Short version: virtual threads are for many blocking tasks; shards are a few hot
loops.
