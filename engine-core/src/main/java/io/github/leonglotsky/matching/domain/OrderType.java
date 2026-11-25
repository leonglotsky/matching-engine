package io.github.leonglotsky.matching.domain;

/** Order type as recorded in {@link EngineEvent.OrderAccepted}. */
public enum OrderType {
    LIMIT,
    MARKET
}
