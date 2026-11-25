package io.github.leonglotsky.matching.sim;

import io.github.leonglotsky.matching.domain.Command;
import io.github.leonglotsky.matching.domain.Command.CancelOrder;
import io.github.leonglotsky.matching.domain.Command.LimitOrder;
import io.github.leonglotsky.matching.domain.Command.MarketOrder;
import io.github.leonglotsky.matching.domain.Side;
import io.github.leonglotsky.matching.domain.TimeInForce;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

/**
 * Seeded, reproducible order flow for one symbol and one client: mostly passive limit orders
 * around a mid price, plus aggressive limits, IOC/FOK, market orders, cancels and a sprinkle of
 * invalid orders. Shared by tests, the demo and the benchmarks so they all exercise the same mix.
 */
public final class WorkloadGenerator {

    private final String symbol;
    private final String accountId;
    private final SplittableRandom random;
    private final long midPrice;
    private final List<String> placed = new ArrayList<>();
    private long next;

    public WorkloadGenerator(String symbol, String accountId, long seed, long midPrice) {
        this.symbol = symbol;
        this.accountId = accountId;
        this.random = new SplittableRandom(seed);
        this.midPrice = midPrice;
    }

    public Command next() {
        int roll = random.nextInt(100);
        Side side = random.nextBoolean() ? Side.BUY : Side.SELL;
        long qty = 1 + random.nextInt(100);
        if (roll < 15 && !placed.isEmpty()) {
            return new CancelOrder(symbol, accountId, placed.get(random.nextInt(placed.size())));
        }
        String id = accountId + "-" + (++next);
        if (placed.size() < 1024) {
            placed.add(id);
        } else {
            placed.set(random.nextInt(placed.size()), id); // bounded memory for long benchmark runs
        }
        if (roll < 25) {
            return new MarketOrder(symbol, accountId, id, side, qty);
        }
        if (roll < 27) {
            return LimitOrder.gtc(symbol, accountId, id, side, 0, qty); // invalid price, rejected
        }
        // Passive orders sit a few ticks away from mid; aggressive ones cross it.
        boolean aggressive = roll < 45;
        long offset = random.nextInt(1, 10);
        long price = switch (side) {
            case BUY -> aggressive ? midPrice + offset : midPrice - offset;
            case SELL -> aggressive ? midPrice - offset : midPrice + offset;
        };
        TimeInForce tif = roll < 35 ? TimeInForce.IOC : roll < 40 ? TimeInForce.FOK : TimeInForce.GTC;
        return new LimitOrder(symbol, accountId, id, side, price, qty, tif);
    }

    public List<Command> next(int count) {
        var out = new ArrayList<Command>(count);
        for (int i = 0; i < count; i++) {
            out.add(next());
        }
        return out;
    }
}
