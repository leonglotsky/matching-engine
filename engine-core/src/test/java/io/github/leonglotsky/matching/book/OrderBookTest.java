package io.github.leonglotsky.matching.book;

import io.github.leonglotsky.matching.domain.Command.CancelOrder;
import io.github.leonglotsky.matching.domain.Command.LimitOrder;
import io.github.leonglotsky.matching.domain.Command.MarketOrder;
import io.github.leonglotsky.matching.domain.EngineEvent;
import io.github.leonglotsky.matching.domain.EngineEvent.CancelReason;
import io.github.leonglotsky.matching.domain.EngineEvent.CancelRejected;
import io.github.leonglotsky.matching.domain.EngineEvent.OrderAccepted;
import io.github.leonglotsky.matching.domain.EngineEvent.OrderCancelled;
import io.github.leonglotsky.matching.domain.EngineEvent.OrderRejected;
import io.github.leonglotsky.matching.domain.EngineEvent.OrderRested;
import io.github.leonglotsky.matching.domain.EngineEvent.RejectReason;
import io.github.leonglotsky.matching.domain.EngineEvent.TradeExecuted;
import io.github.leonglotsky.matching.domain.Side;
import io.github.leonglotsky.matching.domain.TimeInForce;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.github.leonglotsky.matching.domain.Side.BUY;
import static io.github.leonglotsky.matching.domain.Side.SELL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderBookTest {

    private static final String SYM = "ACME";
    private final OrderBook book = new OrderBook(SYM);
    private long clock = 1_000;

    private List<EngineEvent> limit(String account, String clOrdId, Side side, long price, long qty) {
        return limit(account, clOrdId, side, price, qty, TimeInForce.GTC);
    }

    private List<EngineEvent> limit(String account, String clOrdId, Side side, long price, long qty, TimeInForce tif) {
        return applied(book.process(new LimitOrder(SYM, account, clOrdId, side, price, qty, tif), clock++));
    }

    private List<EngineEvent> market(String account, String clOrdId, Side side, long qty) {
        return applied(book.process(new MarketOrder(SYM, account, clOrdId, side, qty), clock++));
    }

    private List<EngineEvent> cancel(String account, String clOrdId) {
        return applied(book.process(new CancelOrder(SYM, account, clOrdId), clock++));
    }

    private static List<EngineEvent> applied(Outcome outcome) {
        assertThat(outcome).isInstanceOf(Outcome.Applied.class);
        return ((Outcome.Applied) outcome).events();
    }

    private static List<TradeExecuted> trades(List<EngineEvent> events) {
        return events.stream().filter(TradeExecuted.class::isInstance).map(TradeExecuted.class::cast).toList();
    }

    @Nested
    @DisplayName("price-time priority")
    class Priority {

        @Test
        void nonCrossingOrdersRestOnTheirSide() {
            limit("a", "1", BUY, 99, 10);
            limit("b", "2", SELL, 101, 5);

            assertThat(book.bestBid()).hasValue(99);
            assertThat(book.bestAsk()).hasValue(101);
            assertThat(book.liveOrderCount()).isEqualTo(2);
        }

        @Test
        void betterPriceMatchesFirstRegardlessOfArrival() {
            limit("m1", "1", SELL, 102, 10);
            limit("m2", "2", SELL, 100, 10);   // arrives later but better price
            limit("m3", "3", SELL, 101, 10);

            var fills = trades(limit("t", "4", BUY, 102, 25));

            assertThat(fills).extracting(TradeExecuted::price, TradeExecuted::quantity)
                    .containsExactly(
                            org.assertj.core.groups.Tuple.tuple(100L, 10L),
                            org.assertj.core.groups.Tuple.tuple(101L, 10L),
                            org.assertj.core.groups.Tuple.tuple(102L, 5L));
        }

        @Test
        void samePriceFillsInArrivalOrder() {
            limit("m1", "1", SELL, 100, 5);
            limit("m2", "2", SELL, 100, 5);
            limit("m3", "3", SELL, 100, 5);

            var fills = trades(limit("t", "4", BUY, 100, 7));

            assertThat(fills).extracting(TradeExecuted::makerOrderId).containsExactly(1L, 2L);
            assertThat(fills).extracting(TradeExecuted::quantity).containsExactly(5L, 2L);
            // order 2 keeps its place at the head of the queue with the leftover
            var level = book.snapshot().asks().getFirst();
            assertThat(level.orders()).extracting(BookSnapshot.Order::orderId).containsExactly(2L, 3L);
            assertThat(level.orders().getFirst().remainingQuantity()).isEqualTo(3);
            assertThat(level.totalQuantity()).isEqualTo(8);
        }

        @Test
        void tradesExecuteAtTheMakersPrice() {
            limit("m", "1", SELL, 100, 5);
            var fills = trades(limit("t", "2", BUY, 105, 5));
            assertThat(fills.getFirst().price()).isEqualTo(100);
            assertThat(fills.getFirst().takerSide()).isEqualTo(BUY);
        }

        @Test
        void limitPriceIsNeverTraded() {
            limit("m", "1", SELL, 101, 5);
            var events = limit("t", "2", BUY, 100, 5);
            assertThat(trades(events)).isEmpty();
            assertThat(events.getLast()).isInstanceOf(OrderRested.class);
        }

        @Test
        void partiallyFilledTakerRestsTheRemainder() {
            limit("m", "1", SELL, 100, 4);
            var events = limit("t", "2", BUY, 100, 10);

            assertThat(events).hasExactlyElementsOfTypes(OrderAccepted.class, TradeExecuted.class, OrderRested.class);
            assertThat(((OrderRested) events.getLast()).remainingQuantity()).isEqualTo(6);
            assertThat(book.bestBid()).hasValue(100);
            assertThat(book.bestAsk()).isEmpty();
        }

        @Test
        void sequencesAndIdsAreGapFree() {
            limit("m", "1", SELL, 100, 4);
            var events = limit("t", "2", BUY, 100, 10);
            assertThat(events).extracting(EngineEvent::sequence).containsExactly(3L, 4L, 5L);
            assertThat(((OrderAccepted) events.getFirst()).orderId()).isEqualTo(2);
            assertThat(((TradeExecuted) events.get(1)).tradeId()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("order types")
    class OrderTypes {

        @Test
        void marketOrderSweepsLevelsAndCancelsUnfilledRemainder() {
            limit("m", "1", SELL, 100, 3);
            limit("m", "2", SELL, 110, 3);

            var events = market("t", "3", BUY, 10);

            assertThat(trades(events)).extracting(TradeExecuted::price).containsExactly(100L, 110L);
            var cancelled = (OrderCancelled) events.getLast();
            assertThat(cancelled.reason()).isEqualTo(CancelReason.NO_LIQUIDITY);
            assertThat(cancelled.cancelledQuantity()).isEqualTo(4);
            assertThat(book.liveOrderCount()).isZero();
        }

        @Test
        void marketOrderNeverRests() {
            var events = market("t", "1", SELL, 10);
            assertThat(events).hasExactlyElementsOfTypes(OrderAccepted.class, OrderCancelled.class);
            assertThat(book.bestAsk()).isEmpty();
        }

        @Test
        void iocFillsWhatItCanAndCancelsTheRest() {
            limit("m", "1", SELL, 100, 3);
            var events = limit("t", "2", BUY, 100, 10, TimeInForce.IOC);

            assertThat(trades(events)).hasSize(1);
            assertThat(((OrderCancelled) events.getLast()).reason()).isEqualTo(CancelReason.IOC_REMAINDER);
            assertThat(book.bestBid()).isEmpty();
        }

        @Test
        void fokThatCannotFillCompletelyDoesNothing() {
            limit("m", "1", SELL, 100, 3);
            limit("m", "2", SELL, 105, 3);
            var before = book.snapshot();

            var events = limit("t", "3", BUY, 104, 5, TimeInForce.FOK); // only 3 available at <= 104

            assertThat(events).hasExactlyElementsOfTypes(OrderAccepted.class, OrderCancelled.class);
            assertThat(((OrderCancelled) events.getLast()).reason()).isEqualTo(CancelReason.FOK_UNFILLABLE);
            assertThat(book.snapshot().asks()).isEqualTo(before.asks());
        }

        @Test
        void fokThatCanFillCompletelyFillsAcrossLevels() {
            limit("m", "1", SELL, 100, 3);
            limit("m", "2", SELL, 105, 3);

            var events = limit("t", "3", BUY, 105, 5, TimeInForce.FOK);

            assertThat(trades(events)).extracting(TradeExecuted::quantity).containsExactly(3L, 2L);
            assertThat(events.getLast()).isInstanceOf(TradeExecuted.class);
        }
    }

    @Nested
    @DisplayName("cancels")
    class Cancels {

        @Test
        void cancelRemovesRestingOrderAndEmptyLevel() {
            limit("a", "1", BUY, 99, 10);
            var events = cancel("a", "1");

            var cancelled = (OrderCancelled) events.getFirst();
            assertThat(cancelled.reason()).isEqualTo(CancelReason.USER_REQUESTED);
            assertThat(cancelled.cancelledQuantity()).isEqualTo(10);
            assertThat(book.bestBid()).isEmpty();
            assertThat(book.snapshot().bids()).isEmpty();
        }

        @Test
        void cancelAfterPartialFillCancelsOnlyTheRemainder() {
            limit("a", "1", SELL, 100, 10);
            limit("b", "2", BUY, 100, 4);
            var cancelled = (OrderCancelled) cancel("a", "1").getFirst();
            assertThat(cancelled.cancelledQuantity()).isEqualTo(6);
        }

        @Test
        void cancelOfFilledOrUnknownOrderIsRejected() {
            limit("a", "1", SELL, 100, 5);
            limit("b", "2", BUY, 100, 5);

            assertThat(cancel("a", "1").getFirst()).isInstanceOf(CancelRejected.class);
            assertThat(cancel("a", "nope").getFirst()).isInstanceOf(CancelRejected.class);
        }

        @Test
        void accountsCannotCancelEachOthersOrders() {
            limit("a", "1", SELL, 100, 5);
            assertThat(cancel("mallory", "1").getFirst()).isInstanceOf(CancelRejected.class);
            assertThat(book.liveOrderCount()).isEqualTo(1);
        }

        @Test
        void repeatedCancelIsHarmless() {
            limit("a", "1", SELL, 100, 5);
            cancel("a", "1");
            assertThat(cancel("a", "1").getFirst()).isInstanceOf(CancelRejected.class);
        }
    }

    @Nested
    @DisplayName("validation and idempotency")
    class Validation {

        @Test
        void invalidOrdersAreRejectedAsEventsWithoutConsumingAnOrderId() {
            var e1 = limit("a", "1", BUY, 0, 10);
            var e2 = limit("a", "2", BUY, 100, -1);
            var e3 = market("a", "3", BUY, 0);

            assertThat(((OrderRejected) e1.getFirst()).reason()).isEqualTo(RejectReason.INVALID_PRICE);
            assertThat(((OrderRejected) e2.getFirst()).reason()).isEqualTo(RejectReason.INVALID_QUANTITY);
            assertThat(((OrderRejected) e3.getFirst()).reason()).isEqualTo(RejectReason.INVALID_QUANTITY);

            var accepted = (OrderAccepted) limit("a", "4", BUY, 100, 1).getFirst();
            assertThat(accepted.orderId()).isEqualTo(1);
        }

        @Test
        void resubmittingTheSameClientOrderIdIsANoOp() {
            limit("a", "1", BUY, 100, 10);
            var before = book.snapshot();

            var outcome = book.process(LimitOrder.gtc(SYM, "a", "1", BUY, 100, 10), clock++);

            assertThat(outcome).isEqualTo(new Outcome.Duplicate(1));
            assertThat(book.snapshot()).isEqualTo(before);
        }

        @Test
        void clientOrderIdsAreScopedPerAccount() {
            limit("a", "1", BUY, 100, 10);
            var events = limit("b", "1", BUY, 100, 10);
            assertThat(events.getFirst()).isInstanceOf(OrderAccepted.class);
        }

        @Test
        void duplicateIsDetectedEvenAfterTheOrderIsGone() {
            limit("a", "1", SELL, 100, 5);
            limit("b", "2", BUY, 100, 5); // fills order 1 completely
            assertThat(book.process(LimitOrder.gtc(SYM, "a", "1", SELL, 100, 5), clock++))
                    .isInstanceOf(Outcome.Duplicate.class);
        }

        @Test
        void commandForAnotherSymbolIsAProgrammingError() {
            assertThatThrownBy(() -> book.process(LimitOrder.gtc("OTHER", "a", "1", BUY, 1, 1), 0))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("decide is pure, apply validates")
    class DecideEvolve {

        @Test
        void decideDoesNotMutateTheBook() {
            limit("m", "1", SELL, 100, 5);
            var before = book.snapshot();

            book.decide(LimitOrder.gtc(SYM, "t", "2", BUY, 100, 5), clock++);
            book.decide(new CancelOrder(SYM, "m", "1"), clock++);

            assertThat(book.snapshot()).isEqualTo(before);
        }

        @Test
        void applyRejectsSequenceGaps() {
            var gap = new OrderRejected(5, SYM, "a", "1", RejectReason.INVALID_PRICE);
            assertThatThrownBy(() -> book.apply(gap)).hasMessageContaining("Sequence gap");
        }

        @Test
        void applyRejectsTradesAgainstUnknownOrders() {
            var bogus = new TradeExecuted(1, SYM, 1, 100, 5, 42, 43, BUY);
            assertThatThrownBy(() -> book.apply(bogus)).hasMessageContaining("unknown maker");
        }

        @Test
        void applyRejectsOverfill() {
            limit("m", "1", SELL, 100, 5);
            var overfill = new TradeExecuted(book.lastSequence() + 1, SYM, 1, 100, 6, 1, 2, BUY);
            assertThatThrownBy(() -> book.apply(overfill)).hasMessageContaining("overfills");
        }
    }
}
