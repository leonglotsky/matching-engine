package io.github.leonglotsky.matching.engine;

import io.github.leonglotsky.matching.book.BookSnapshot;
import io.github.leonglotsky.matching.domain.Command;
import io.github.leonglotsky.matching.domain.EngineEvent;
import io.github.leonglotsky.matching.domain.EngineEvent.OrderAccepted;
import io.github.leonglotsky.matching.domain.EngineEvent.OrderCancelled;
import io.github.leonglotsky.matching.domain.EngineEvent.TradeExecuted;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;

final class EngineTestSupport {

    private EngineTestSupport() {
    }

    /** One entry of a shard's input tape. */
    record Sequenced(Command command, long timestampMicros) {
    }

    /** Deterministic, strictly increasing clock so tests never depend on wall time. */
    static LongSupplier fakeClock() {
        var t = new AtomicLong(1_700_000_000_000_000L);
        return t::incrementAndGet;
    }

    static CommandResult await(java.util.concurrent.CompletableFuture<CommandResult> f) {
        return f.join();
    }

    /**
     * Quantity conservation, the invariant a matching engine must never break: for every accepted
     * order, original quantity == traded + cancelled + still resting.
     */
    static void assertQuantityConserved(List<EngineEvent> events, List<BookSnapshot> finalBooks) {
        var original = new HashMap<String, Long>();
        var accounted = new HashMap<String, Long>();
        for (EngineEvent e : events) {
            switch (e) {
                case OrderAccepted a -> original.put(key(a.symbol(), a.orderId()), a.quantity());
                case TradeExecuted t -> {
                    accounted.merge(key(t.symbol(), t.makerOrderId()), t.quantity(), Long::sum);
                    accounted.merge(key(t.symbol(), t.takerOrderId()), t.quantity(), Long::sum);
                }
                case OrderCancelled c -> accounted.merge(key(c.symbol(), c.orderId()), c.cancelledQuantity(), Long::sum);
                default -> {
                }
            }
        }
        for (BookSnapshot book : finalBooks) {
            for (var level : concat(book.bids(), book.asks())) {
                for (var o : level.orders()) {
                    accounted.merge(key(book.symbol(), o.orderId()), o.remainingQuantity(), Long::sum);
                }
            }
        }
        assertThat(accounted).isEqualTo(original);
    }

    static void assertNotCrossed(BookSnapshot book) {
        if (!book.bids().isEmpty() && !book.asks().isEmpty()) {
            assertThat(book.bids().getFirst().price())
                    .as("best bid must be below best ask in %s", book.symbol())
                    .isLessThan(book.asks().getFirst().price());
        }
    }

    private static <T> List<T> concat(List<T> a, List<T> b) {
        return java.util.stream.Stream.concat(a.stream(), b.stream()).toList();
    }

    private static String key(String symbol, long orderId) {
        return symbol + "#" + orderId;
    }

    static Map<String, BookSnapshot> snapshots(MatchingEngine engine) {
        var out = new HashMap<String, BookSnapshot>();
        for (String s : engine.symbols()) {
            out.put(s, engine.snapshot(s).join());
        }
        return out;
    }
}
