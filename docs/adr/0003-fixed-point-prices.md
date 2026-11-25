# ADR-0003: Prices and quantities as `long` ticks and lots

* Status: accepted
* Date: 2026-11-25

## Decision

Prices are `long` numbers of ticks and quantities are `long` numbers of lots. Conversion to display
units (e.g. tick size 0.01) is the gateway's job.

## Why

* `double` cannot represent 0.1 exactly; equality on price levels would be wrong and two "equal" prices
  could land on different levels.
* `BigDecimal` is exact but allocates on every arithmetic operation, is far slower to compare and hash, and
  its `equals` considers scale (`1.0` != `1.00`), a classic bug source for map keys.
* `long` is exact, allocation-free in arithmetic, and the natural key for a price ladder.

## Consequences

* The engine is unaware of tick size; validation of tick multiples belongs in a symbol reference-data
  layer (not implemented).
* `TreeMap<Long, PriceLevel>` boxes keys. Acceptable here; a primitive map or an array ladder removes
  it (see ADR-0004 and performance.md).
