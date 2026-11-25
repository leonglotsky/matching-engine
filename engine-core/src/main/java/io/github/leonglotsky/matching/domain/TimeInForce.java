package io.github.leonglotsky.matching.domain;

/** How long a limit order stays eligible for matching. */
public enum TimeInForce {
    /** Good-till-cancelled: any unfilled remainder rests on the book. */
    GTC,
    /** Immediate-or-cancel: fill what is possible now, cancel the remainder. */
    IOC,
    /** Fill-or-kill: fill the whole quantity immediately or do nothing at all. */
    FOK
}
