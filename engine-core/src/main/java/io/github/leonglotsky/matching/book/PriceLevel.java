package io.github.leonglotsky.matching.book;

import java.util.LinkedHashMap;
import java.util.SequencedMap;

/**
 * All resting orders at one price, in time priority.
 *
 * <p>A {@link SequencedMap} (Java 21) gives both properties we need from one structure:
 * insertion order for FIFO time priority, and O(1) removal by order id for cancels and fills.
 * {@link #oldest()} uses the new {@code firstEntry()} instead of the old iterator dance.
 */
final class PriceLevel {
    final long price;
    private final SequencedMap<Long, RestingOrder> orders = new LinkedHashMap<>();
    private long totalQuantity;

    PriceLevel(long price) {
        this.price = price;
    }

    void add(RestingOrder order) {
        orders.putLast(order.orderId, order);
        totalQuantity += order.remaining;
    }

    void reduce(RestingOrder order, long quantity) {
        order.remaining -= quantity;
        totalQuantity -= quantity;
        if (order.remaining == 0) {
            orders.remove(order.orderId);
        }
    }

    void remove(RestingOrder order) {
        if (orders.remove(order.orderId) != null) {
            totalQuantity -= order.remaining;
        }
    }

    Iterable<RestingOrder> inTimePriority() {
        return orders.sequencedValues();
    }

    boolean isEmpty() {
        return orders.isEmpty();
    }

    long totalQuantity() {
        return totalQuantity;
    }

    int orderCount() {
        return orders.size();
    }
}
