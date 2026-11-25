# ADR-0006: Idempotent submission keyed on (account, clientOrderId)

* Status: accepted
* Date: 2026-11-25

## Context

Networks time out. A client that does not get an answer must be able to resend without risking a
double fill. This is also how clients reconcile after a shard halts with an unknown outcome.

## Decision

Every new order carries a client-assigned `clientOrderId`. The book remembers every accepted
`(accountId, clientOrderId)` (per symbol). A repeat returns `Duplicate(originalOrderId)`, produces no
events and changes nothing. Cancels address orders by the same key, so a client never needs the engine's
id to cancel, and repeating a cancel is harmless (`CancelRejected`).

The set of accepted keys is part of the book state and is rebuilt from `OrderAccepted` events, so
idempotency **survives restarts** (tested in `MatchingEngineTest.Recovery`).

Rejected orders (invalid price/quantity) do not reserve their key: the retry is rejected again for the
same reason, which is still idempotent in effect.

## Alternatives considered

* **Engine-assigned ids only**: the client cannot tell whether a timed-out request landed.
* **Request-id cache with TTL**: bounded memory, but the TTL becomes a correctness parameter, and replay
  would need the wall clock to evict identically.
* **FIX-style session sequence numbers**: the right answer for a FIX gateway (gap fill, resend). Lives
  at the session layer, above this engine; compatible with this design.

## Consequences

Key memory grows with the number of orders ever accepted per symbol. Production fix: scope keys to a
trading day (books are typically reset at session end) or evict keys of terminal orders older than the
client retry window. Both are deterministic if eviction is driven by sequenced events, not wall time.
