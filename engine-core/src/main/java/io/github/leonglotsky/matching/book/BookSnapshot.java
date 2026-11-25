package io.github.leonglotsky.matching.book;

import java.util.List;

/**
 * Immutable, fully structural copy of a book. Two snapshots are {@code equals} iff the books hold
 * exactly the same orders in exactly the same priority, which is what the determinism and
 * recovery tests compare.
 */
public record BookSnapshot(String symbol, long lastSequence, long lastOrderId, long lastTradeId,
                           List<Level> bids, List<Level> asks) {

    public BookSnapshot {
        bids = List.copyOf(bids);
        asks = List.copyOf(asks);
    }

    public record Level(long price, long totalQuantity, List<Order> orders) {
        public Level {
            orders = List.copyOf(orders);
        }
    }

    public record Order(long orderId, String accountId, String clientOrderId, long remainingQuantity) {
    }
}
