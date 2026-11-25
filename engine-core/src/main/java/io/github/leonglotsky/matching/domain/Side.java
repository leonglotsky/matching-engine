package io.github.leonglotsky.matching.domain;

/** Side of an order. */
public enum Side {
    BUY,
    SELL;

    public Side opposite() {
        return this == BUY ? SELL : BUY;
    }
}
