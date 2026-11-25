package io.github.leonglotsky.matching.book;

import io.github.leonglotsky.matching.domain.Command;
import io.github.leonglotsky.matching.domain.Command.CancelOrder;
import io.github.leonglotsky.matching.domain.Command.LimitOrder;
import io.github.leonglotsky.matching.domain.Command.MarketOrder;
import io.github.leonglotsky.matching.domain.Command.PlaceOrder;
import io.github.leonglotsky.matching.domain.EngineEvent;
import io.github.leonglotsky.matching.domain.EngineEvent.CancelReason;
import io.github.leonglotsky.matching.domain.EngineEvent.CancelRejected;
import io.github.leonglotsky.matching.domain.EngineEvent.OrderAccepted;
import io.github.leonglotsky.matching.domain.EngineEvent.OrderCancelled;
import io.github.leonglotsky.matching.domain.EngineEvent.OrderRejected;
import io.github.leonglotsky.matching.domain.EngineEvent.OrderRested;
import io.github.leonglotsky.matching.domain.EngineEvent.RejectReason;
import io.github.leonglotsky.matching.domain.EngineEvent.TradeExecuted;
import io.github.leonglotsky.matching.domain.OrderType;
import io.github.leonglotsky.matching.domain.Side;
import io.github.leonglotsky.matching.domain.TimeInForce;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.TreeMap;

/**
 * A single-symbol limit order book with price-time priority.
 *
 * <h2>Decide / evolve</h2>
 * The book is written as an event-sourced "decider" (see ADR-0002):
 * <ul>
 *   <li>{@link #decide(Command, long)} is a pure function of (current state, command, timestamp)
 *       that returns the events the command produces. It does not mutate anything.</li>
 *   <li>{@link #apply(EngineEvent)} is the only method that mutates state, and it does so purely
 *       from an event. Live processing and journal replay call the exact same code, so a
 *       recovered book cannot drift from the original.</li>
 * </ul>
 *
 * <h2>Threading</h2>
 * Not thread-safe by design. Each book is owned by exactly one shard thread (single writer),
 * which removes all locking from the matching path. See ADR-0001.
 */
public final class OrderBook {

    private final String symbol;

    /** Best bid first (highest price). */
    private final NavigableMap<Long, PriceLevel> bids = new TreeMap<>(Comparator.reverseOrder());
    /** Best ask first (lowest price). */
    private final NavigableMap<Long, PriceLevel> asks = new TreeMap<>();

    /** Resting orders by engine order id, for O(1) cancel and fill lookup. */
    private final Map<Long, RestingOrder> live = new HashMap<>();
    /** Every accepted (account, clientOrderId), for idempotent submission and cancel-by-client-id. */
    private final Map<ClientKey, Long> acceptedKeys = new HashMap<>();

    private long lastSequence;
    private long lastOrderId;
    private long lastTradeId;

    public OrderBook(String symbol) {
        this.symbol = Objects.requireNonNull(symbol, "symbol");
    }

    /** Decide and immediately apply. This is what the live engine calls. */
    public Outcome process(Command command, long timestampMicros) {
        Outcome outcome = decide(command, timestampMicros);
        if (outcome instanceof Outcome.Applied(var events)) {
            for (EngineEvent event : events) {
                apply(event);
            }
        }
        return outcome;
    }

    // ------------------------------------------------------------------------------------------
    // decide: pure
    // ------------------------------------------------------------------------------------------

    public Outcome decide(Command command, long timestampMicros) {
        if (!symbol.equals(command.symbol())) {
            throw new IllegalArgumentException("Command for " + command.symbol() + " routed to book " + symbol);
        }
        return switch (command) {
            case PlaceOrder place -> decidePlace(place, timestampMicros);
            case CancelOrder cancel -> decideCancel(cancel);
        };
    }

    private Outcome decidePlace(PlaceOrder order, long timestampMicros) {
        Long existing = acceptedKeys.get(new ClientKey(order.accountId(), order.clientOrderId()));
        if (existing != null) {
            return new Outcome.Duplicate(existing);
        }

        var events = new ArrayList<EngineEvent>(4);
        long seq = lastSequence;

        RejectReason invalid = validate(order);
        if (invalid != null) {
            events.add(new OrderRejected(++seq, symbol, order.accountId(), order.clientOrderId(), invalid));
            return new Outcome.Applied(events);
        }

        // Normalise both order types into (limit price, type, tif). A market order is a limit order
        // with an unreachable price that is never allowed to rest.
        record Terms(long limitPrice, OrderType type, TimeInForce tif, long recordedPrice) {
        }
        Terms terms = switch (order) {
            case LimitOrder(var s, var a, var c, var side, var price, var qty, var tif) ->
                    new Terms(price, OrderType.LIMIT, tif, price);
            case MarketOrder(var s, var a, var c, var side, var qty) ->
                    new Terms(side == Side.BUY ? Long.MAX_VALUE : Long.MIN_VALUE, OrderType.MARKET, TimeInForce.IOC, 0L);
        };

        Side side = order.side();
        long quantity = order.quantity();
        long orderId = lastOrderId + 1;

        events.add(new OrderAccepted(++seq, symbol, orderId, order.accountId(), order.clientOrderId(),
                side, terms.type(), terms.recordedPrice(), quantity, terms.tif(), timestampMicros));

        if (terms.tif() == TimeInForce.FOK && !canFillCompletely(side, terms.limitPrice(), quantity)) {
            events.add(new OrderCancelled(++seq, symbol, orderId, order.accountId(), order.clientOrderId(),
                    quantity, CancelReason.FOK_UNFILLABLE));
            return new Outcome.Applied(events);
        }

        long remaining = quantity;
        long tradeId = lastTradeId;
        NavigableMap<Long, PriceLevel> opposite = side == Side.BUY ? asks : bids;

        sweep:
        for (PriceLevel level : opposite.values()) {
            if (!crosses(side, terms.limitPrice(), level.price)) {
                break;
            }
            for (RestingOrder maker : level.inTimePriority()) {
                long fill = Math.min(remaining, maker.remaining);
                events.add(new TradeExecuted(++seq, symbol, ++tradeId, level.price, fill,
                        maker.orderId, orderId, side));
                remaining -= fill;
                if (remaining == 0) {
                    break sweep;
                }
            }
        }

        if (remaining > 0) {
            CancelReason reason = switch (terms.type()) {
                case MARKET -> CancelReason.NO_LIQUIDITY;
                case LIMIT -> switch (terms.tif()) {
                    case GTC -> null;
                    case IOC -> CancelReason.IOC_REMAINDER;
                    case FOK -> throw new IllegalStateException("FOK pre-check passed but order not filled");
                };
            };
            events.add(reason == null
                    ? new OrderRested(++seq, symbol, orderId, order.accountId(), order.clientOrderId(),
                            side, terms.limitPrice(), remaining)
                    : new OrderCancelled(++seq, symbol, orderId, order.accountId(), order.clientOrderId(),
                            remaining, reason));
        }
        return new Outcome.Applied(events);
    }

    private static RejectReason validate(PlaceOrder order) {
        if (order.quantity() <= 0) {
            return RejectReason.INVALID_QUANTITY;
        }
        if (order instanceof LimitOrder limit && limit.price() <= 0) {
            return RejectReason.INVALID_PRICE;
        }
        return null;
    }

    private boolean canFillCompletely(Side side, long limitPrice, long quantity) {
        long available = 0;
        for (PriceLevel level : (side == Side.BUY ? asks : bids).values()) {
            if (!crosses(side, limitPrice, level.price)) {
                return false;
            }
            available += level.totalQuantity();
            if (available >= quantity) {
                return true;
            }
        }
        return false;
    }

    private static boolean crosses(Side takerSide, long limitPrice, long makerPrice) {
        return takerSide == Side.BUY ? makerPrice <= limitPrice : makerPrice >= limitPrice;
    }

    private Outcome decideCancel(CancelOrder cancel) {
        Long orderId = acceptedKeys.get(new ClientKey(cancel.accountId(), cancel.clientOrderId()));
        RestingOrder resting = orderId == null ? null : live.get(orderId);
        long seq = lastSequence + 1;
        EngineEvent event = resting == null
                ? new CancelRejected(seq, symbol, cancel.accountId(), cancel.clientOrderId(), RejectReason.UNKNOWN_ORDER)
                : new OrderCancelled(seq, symbol, resting.orderId, resting.accountId, resting.clientOrderId,
                        resting.remaining, CancelReason.USER_REQUESTED);
        return new Outcome.Applied(List.of(event));
    }

    // ------------------------------------------------------------------------------------------
    // evolve: the only mutation path, used by both live processing and replay
    // ------------------------------------------------------------------------------------------

    /**
     * Apply one event to the book. Fails fast if the event does not fit the current state
     * (sequence gap, unknown maker order, overfill...), which during replay means the journal is
     * corrupt or belongs to a different book.
     */
    public void apply(EngineEvent event) {
        if (!symbol.equals(event.symbol())) {
            throw new IllegalStateException("Event for " + event.symbol() + " applied to book " + symbol);
        }
        if (event.sequence() != lastSequence + 1) {
            throw new IllegalStateException("Sequence gap in " + symbol + ": expected " + (lastSequence + 1)
                    + " but got " + event.sequence());
        }
        switch (event) {
            case OrderAccepted accepted -> {
                expect(accepted.orderId() == lastOrderId + 1, "non-contiguous order id", event);
                lastOrderId = accepted.orderId();
                acceptedKeys.put(new ClientKey(accepted.accountId(), accepted.clientOrderId()), accepted.orderId());
            }
            case OrderRejected ignored -> {
                // Rejections are journaled for audit; they do not change the book.
            }
            case TradeExecuted(var seq, var sym, var tradeId, var price, var qty, var makerId, var takerId, var takerSide) -> {
                expect(tradeId == lastTradeId + 1, "non-contiguous trade id", event);
                RestingOrder maker = live.get(makerId);
                expect(maker != null, "trade against unknown maker order", event);
                expect(maker.price == price && maker.side != takerSide, "trade does not match maker order", event);
                expect(qty > 0 && qty <= maker.remaining, "trade overfills maker", event);
                NavigableMap<Long, PriceLevel> side = sideOf(maker.side);
                PriceLevel level = side.get(price);
                level.reduce(maker, qty);
                if (maker.remaining == 0) {
                    live.remove(makerId);
                    if (level.isEmpty()) {
                        side.remove(price);
                    }
                }
                lastTradeId = tradeId;
            }
            case OrderRested rested -> {
                expect(rested.remainingQuantity() > 0, "resting non-positive quantity", event);
                expect(!live.containsKey(rested.orderId()), "order rested twice", event);
                var order = new RestingOrder(rested.orderId(), rested.accountId(), rested.clientOrderId(),
                        rested.side(), rested.price(), rested.remainingQuantity());
                sideOf(rested.side()).computeIfAbsent(rested.price(), PriceLevel::new).add(order);
                live.put(order.orderId, order);
            }
            case OrderCancelled cancelled when cancelled.reason() == CancelReason.USER_REQUESTED -> {
                RestingOrder order = live.remove(cancelled.orderId());
                expect(order != null, "cancel of order that is not resting", event);
                expect(order.remaining == cancelled.cancelledQuantity(), "cancelled quantity mismatch", event);
                NavigableMap<Long, PriceLevel> side = sideOf(order.side);
                PriceLevel level = side.get(order.price);
                level.remove(order);
                if (level.isEmpty()) {
                    side.remove(order.price);
                }
            }
            case OrderCancelled ignored -> {
                // IOC / FOK / market remainders never rested, so there is nothing to remove.
            }
            case CancelRejected ignored -> {
            }
        }
        lastSequence = event.sequence();
    }

    private NavigableMap<Long, PriceLevel> sideOf(Side side) {
        return side == Side.BUY ? bids : asks;
    }

    private static void expect(boolean condition, String message, EngineEvent event) {
        if (!condition) {
            throw new IllegalStateException(message + ": " + event);
        }
    }

    // ------------------------------------------------------------------------------------------
    // queries
    // ------------------------------------------------------------------------------------------

    public String symbol() {
        return symbol;
    }

    public long lastSequence() {
        return lastSequence;
    }

    public OptionalLong bestBid() {
        return bids.isEmpty() ? OptionalLong.empty() : OptionalLong.of(bids.firstKey());
    }

    public OptionalLong bestAsk() {
        return asks.isEmpty() ? OptionalLong.empty() : OptionalLong.of(asks.firstKey());
    }

    public int liveOrderCount() {
        return live.size();
    }

    public BookSnapshot snapshot() {
        return new BookSnapshot(symbol, lastSequence, lastOrderId, lastTradeId, levels(bids), levels(asks));
    }

    private static List<BookSnapshot.Level> levels(NavigableMap<Long, PriceLevel> side) {
        var out = new ArrayList<BookSnapshot.Level>(side.size());
        for (PriceLevel level : side.values()) {
            var orders = new ArrayList<BookSnapshot.Order>(level.orderCount());
            for (RestingOrder o : level.inTimePriority()) {
                orders.add(new BookSnapshot.Order(o.orderId, o.accountId, o.clientOrderId, o.remaining));
            }
            out.add(new BookSnapshot.Level(level.price, level.totalQuantity(), orders));
        }
        return out;
    }

    private record ClientKey(String accountId, String clientOrderId) {
    }
}
