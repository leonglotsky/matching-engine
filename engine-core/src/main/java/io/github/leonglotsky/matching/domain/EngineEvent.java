package io.github.leonglotsky.matching.domain;

/**
 * Everything the engine has ever decided, as immutable facts. The order book state is a pure
 * left fold over these events (see {@code OrderBook#apply}), which is what makes the journal
 * the source of truth and recovery a replay.
 *
 * <p>Every event carries the symbol and a per-symbol, gap-free {@link #sequence()}. Replay checks
 * the sequence, so a missing, duplicated or re-ordered event is detected instead of silently
 * producing a different book.
 */
public sealed interface EngineEvent {

    long sequence();

    String symbol();

    /** An order passed validation and was assigned an engine order id. Always the first event for an order. */
    record OrderAccepted(long sequence, String symbol, long orderId, String accountId, String clientOrderId,
                         Side side, OrderType orderType, long price, long quantity, TimeInForce timeInForce,
                         long timestampMicros) implements EngineEvent {
    }

    /** An order failed validation. It never received an order id and changed no state. */
    record OrderRejected(long sequence, String symbol, String accountId, String clientOrderId,
                         RejectReason reason) implements EngineEvent {
    }

    /** A match between a resting (maker) order and an incoming (taker) order, at the maker's price. */
    record TradeExecuted(long sequence, String symbol, long tradeId, long price, long quantity,
                         long makerOrderId, long takerOrderId, Side takerSide) implements EngineEvent {
    }

    /** The unfilled remainder of a limit order was added to the book. */
    record OrderRested(long sequence, String symbol, long orderId, String accountId, String clientOrderId,
                       Side side, long price, long remainingQuantity) implements EngineEvent {
    }

    /** Quantity was removed from the market without trading. */
    record OrderCancelled(long sequence, String symbol, long orderId, String accountId, String clientOrderId,
                          long cancelledQuantity, CancelReason reason) implements EngineEvent {
    }

    /** A cancel request could not be honoured (unknown order, already filled or already cancelled). */
    record CancelRejected(long sequence, String symbol, String accountId, String clientOrderId,
                          RejectReason reason) implements EngineEvent {
    }

    enum RejectReason {
        INVALID_QUANTITY,
        INVALID_PRICE,
        UNKNOWN_ORDER
    }

    enum CancelReason {
        /** The owner asked for it. The order was resting on the book. */
        USER_REQUESTED,
        /** IOC remainder that could not be filled immediately. Never rested. */
        IOC_REMAINDER,
        /** FOK order that could not be filled in full. Never traded, never rested. */
        FOK_UNFILLABLE,
        /** Market order remainder after the opposite side ran out of liquidity. Never rested. */
        NO_LIQUIDITY
    }
}
