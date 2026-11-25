package io.github.leonglotsky.matching.book;

import io.github.leonglotsky.matching.domain.Side;

/**
 * Mutable, book-private view of an order sitting on the book. It never escapes the owning
 * shard thread; callers only ever see immutable {@link BookSnapshot}s.
 */
final class RestingOrder {
    final long orderId;
    final String accountId;
    final String clientOrderId;
    final Side side;
    final long price;
    long remaining;

    RestingOrder(long orderId, String accountId, String clientOrderId, Side side, long price, long remaining) {
        this.orderId = orderId;
        this.accountId = accountId;
        this.clientOrderId = clientOrderId;
        this.side = side;
        this.price = price;
        this.remaining = remaining;
    }
}
