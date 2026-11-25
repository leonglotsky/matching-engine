package io.github.leonglotsky.matching.domain;

import java.util.Objects;

/**
 * Every input the engine accepts. The hierarchy is sealed so that the matching logic can
 * use exhaustive pattern matching: adding a new command type is a compile error everywhere
 * it is not handled, instead of a silent fall-through at runtime.
 *
 * <p>Prices and quantities are {@code long} ticks/lots, never {@code double} or {@code BigDecimal}
 * (see ADR-0003). Business validation (positive quantity, positive price) is deliberately
 * <em>not</em> done here: invalid orders are turned into {@link EngineEvent.OrderRejected}
 * events so that rejections are journaled and auditable like everything else.
 */
public sealed interface Command permits Command.PlaceOrder, Command.CancelOrder {

    String symbol();

    String accountId();

    /**
     * Client-assigned id. For new orders it is the idempotency key (scoped to symbol and account);
     * for cancels it identifies the order to cancel.
     */
    String clientOrderId();

    /** A request to add liquidity or take it. */
    sealed interface PlaceOrder extends Command permits LimitOrder, MarketOrder {
        Side side();

        long quantity();
    }

    /** Buy/sell up to {@code quantity} at {@code price} or better. */
    record LimitOrder(String symbol, String accountId, String clientOrderId,
                      Side side, long price, long quantity, TimeInForce timeInForce) implements PlaceOrder {
        public LimitOrder {
            Objects.requireNonNull(symbol, "symbol");
            Objects.requireNonNull(accountId, "accountId");
            Objects.requireNonNull(clientOrderId, "clientOrderId");
            Objects.requireNonNull(side, "side");
            Objects.requireNonNull(timeInForce, "timeInForce");
        }

        public static LimitOrder gtc(String symbol, String accountId, String clientOrderId,
                                     Side side, long price, long quantity) {
            return new LimitOrder(symbol, accountId, clientOrderId, side, price, quantity, TimeInForce.GTC);
        }
    }

    /** Take {@code quantity} at any price; whatever cannot be filled immediately is cancelled. */
    record MarketOrder(String symbol, String accountId, String clientOrderId,
                       Side side, long quantity) implements PlaceOrder {
        public MarketOrder {
            Objects.requireNonNull(symbol, "symbol");
            Objects.requireNonNull(accountId, "accountId");
            Objects.requireNonNull(clientOrderId, "clientOrderId");
            Objects.requireNonNull(side, "side");
        }
    }

    /** Cancel the resting order this account placed with {@code clientOrderId}. */
    record CancelOrder(String symbol, String accountId, String clientOrderId) implements Command {
        public CancelOrder {
            Objects.requireNonNull(symbol, "symbol");
            Objects.requireNonNull(accountId, "accountId");
            Objects.requireNonNull(clientOrderId, "clientOrderId");
        }
    }
}
