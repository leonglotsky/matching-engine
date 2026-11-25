package io.github.leonglotsky.matching.journal;

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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Compact binary encoding of events. Hand-written instead of Java serialization (slow, fragile,
 * a security liability) or JSON (several times larger, slower to parse).
 *
 * <p>Encoding is an exhaustive switch with record deconstruction patterns over the sealed event
 * hierarchy: a new event type will not compile until it has a wire format. Tags and enum ordinals
 * are part of the on-disk format, so they are pinned explicitly rather than derived from
 * declaration order.
 */
public final class EventCodec {

    private static final byte ACCEPTED = 1;
    private static final byte REJECTED = 2;
    private static final byte TRADE = 3;
    private static final byte RESTED = 4;
    private static final byte CANCELLED = 5;
    private static final byte CANCEL_REJECTED = 6;

    private EventCodec() {
    }

    public static void encode(EngineEvent event, DataOutput out) throws IOException {
        switch (event) {
            case OrderAccepted(var seq, var sym, var orderId, var account, var clOrdId,
                               var side, var type, var price, var qty, var tif, var ts) -> {
                header(out, ACCEPTED, seq, sym);
                out.writeLong(orderId);
                out.writeUTF(account);
                out.writeUTF(clOrdId);
                out.writeByte(side(side));
                out.writeByte(type == OrderType.LIMIT ? 0 : 1);
                out.writeLong(price);
                out.writeLong(qty);
                out.writeByte(tif(tif));
                out.writeLong(ts);
            }
            case OrderRejected(var seq, var sym, var account, var clOrdId, var reason) -> {
                header(out, REJECTED, seq, sym);
                out.writeUTF(account);
                out.writeUTF(clOrdId);
                out.writeByte(reject(reason));
            }
            case TradeExecuted(var seq, var sym, var tradeId, var price, var qty, var maker, var taker, var takerSide) -> {
                header(out, TRADE, seq, sym);
                out.writeLong(tradeId);
                out.writeLong(price);
                out.writeLong(qty);
                out.writeLong(maker);
                out.writeLong(taker);
                out.writeByte(side(takerSide));
            }
            case OrderRested(var seq, var sym, var orderId, var account, var clOrdId, var side, var price, var qty) -> {
                header(out, RESTED, seq, sym);
                out.writeLong(orderId);
                out.writeUTF(account);
                out.writeUTF(clOrdId);
                out.writeByte(side(side));
                out.writeLong(price);
                out.writeLong(qty);
            }
            case OrderCancelled(var seq, var sym, var orderId, var account, var clOrdId, var qty, var reason) -> {
                header(out, CANCELLED, seq, sym);
                out.writeLong(orderId);
                out.writeUTF(account);
                out.writeUTF(clOrdId);
                out.writeLong(qty);
                out.writeByte(cancel(reason));
            }
            case CancelRejected(var seq, var sym, var account, var clOrdId, var reason) -> {
                header(out, CANCEL_REJECTED, seq, sym);
                out.writeUTF(account);
                out.writeUTF(clOrdId);
                out.writeByte(reject(reason));
            }
        }
    }

    public static EngineEvent decode(DataInput in) throws IOException {
        byte tag = in.readByte();
        long seq = in.readLong();
        String sym = in.readUTF();
        return switch (tag) {
            case ACCEPTED -> new OrderAccepted(seq, sym, in.readLong(), in.readUTF(), in.readUTF(),
                    side(in.readByte()), in.readByte() == 0 ? OrderType.LIMIT : OrderType.MARKET,
                    in.readLong(), in.readLong(), tif(in.readByte()), in.readLong());
            case REJECTED -> new OrderRejected(seq, sym, in.readUTF(), in.readUTF(), reject(in.readByte()));
            case TRADE -> new TradeExecuted(seq, sym, in.readLong(), in.readLong(), in.readLong(),
                    in.readLong(), in.readLong(), side(in.readByte()));
            case RESTED -> new OrderRested(seq, sym, in.readLong(), in.readUTF(), in.readUTF(),
                    side(in.readByte()), in.readLong(), in.readLong());
            case CANCELLED -> new OrderCancelled(seq, sym, in.readLong(), in.readUTF(), in.readUTF(),
                    in.readLong(), cancel(in.readByte()));
            case CANCEL_REJECTED -> new CancelRejected(seq, sym, in.readUTF(), in.readUTF(), reject(in.readByte()));
            default -> throw new IOException("Unknown event tag " + tag);
        };
    }

    private static void header(DataOutput out, byte tag, long seq, String symbol) throws IOException {
        out.writeByte(tag);
        out.writeLong(seq);
        out.writeUTF(symbol);
    }

    // Explicit wire codes: reordering an enum must never change the meaning of an old journal.

    private static int side(Side side) {
        return switch (side) {
            case BUY -> 0;
            case SELL -> 1;
        };
    }

    private static Side side(byte code) throws IOException {
        return switch (code) {
            case 0 -> Side.BUY;
            case 1 -> Side.SELL;
            default -> throw new IOException("Unknown side " + code);
        };
    }

    private static int tif(TimeInForce tif) {
        return switch (tif) {
            case GTC -> 0;
            case IOC -> 1;
            case FOK -> 2;
        };
    }

    private static TimeInForce tif(byte code) throws IOException {
        return switch (code) {
            case 0 -> TimeInForce.GTC;
            case 1 -> TimeInForce.IOC;
            case 2 -> TimeInForce.FOK;
            default -> throw new IOException("Unknown time in force " + code);
        };
    }

    private static int reject(RejectReason reason) {
        return switch (reason) {
            case INVALID_QUANTITY -> 0;
            case INVALID_PRICE -> 1;
            case UNKNOWN_ORDER -> 2;
        };
    }

    private static RejectReason reject(byte code) throws IOException {
        return switch (code) {
            case 0 -> RejectReason.INVALID_QUANTITY;
            case 1 -> RejectReason.INVALID_PRICE;
            case 2 -> RejectReason.UNKNOWN_ORDER;
            default -> throw new IOException("Unknown reject reason " + code);
        };
    }

    private static int cancel(CancelReason reason) {
        return switch (reason) {
            case USER_REQUESTED -> 0;
            case IOC_REMAINDER -> 1;
            case FOK_UNFILLABLE -> 2;
            case NO_LIQUIDITY -> 3;
        };
    }

    private static CancelReason cancel(byte code) throws IOException {
        return switch (code) {
            case 0 -> CancelReason.USER_REQUESTED;
            case 1 -> CancelReason.IOC_REMAINDER;
            case 2 -> CancelReason.FOK_UNFILLABLE;
            case 3 -> CancelReason.NO_LIQUIDITY;
            default -> throw new IOException("Unknown cancel reason " + code);
        };
    }
}
