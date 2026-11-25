package io.github.leonglotsky.matching.engine;

import io.github.leonglotsky.matching.book.BookSnapshot;
import io.github.leonglotsky.matching.book.OrderBook;
import io.github.leonglotsky.matching.domain.Command;
import io.github.leonglotsky.matching.journal.Journal;
import io.github.leonglotsky.matching.journal.JournalFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SequencedSet;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

/**
 * Thread-safe entry point. Any number of threads (typically virtual threads, one per client
 * connection) may call {@link #submit}; each symbol is owned by exactly one {@link Shard} whose
 * single thread does all matching for it.
 *
 * <p>Ordering guarantee: commands for the same symbol are executed in the order they enter that
 * shard's queue, and the resulting events are a deterministic function of that order. There is no
 * ordering across symbols, and none is needed: books are independent.
 */
public final class MatchingEngine implements AutoCloseable {

    private final List<Shard> shards;
    private final Map<String, Shard> shardBySymbol;
    private final long recoveredEvents;
    private volatile boolean closed;

    private MatchingEngine(Builder b) throws IOException {
        var shardSymbols = new ArrayList<Map<String, OrderBook>>();
        for (int i = 0; i < b.shards; i++) {
            shardSymbols.add(new HashMap<>());
        }
        for (String symbol : b.symbols) {
            shardSymbols.get(shardIndex(symbol, b.shards)).put(symbol, new OrderBook(symbol));
        }

        var built = new ArrayList<Shard>(b.shards);
        long replayed = 0;
        try {
            for (int i = 0; i < b.shards; i++) {
                Map<String, OrderBook> books = shardSymbols.get(i);
                Journal journal = b.journals.open(i);
                built.add(new Shard(i, books, journal, b.queueCapacity, b.maxBatchSize, b.clockMicros, b.listener));
                replayed += recover(i, journal, books);
            }
        } catch (IOException | RuntimeException e) {
            // Journals of shards that were already opened are closed by stopping them.
            built.forEach(Shard::start);
            for (Shard s : built) {
                try {
                    s.stop();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
            throw e;
        }
        this.recoveredEvents = replayed;
        this.shards = List.copyOf(built);
        var bySymbol = new HashMap<String, Shard>();
        for (int i = 0; i < b.shards; i++) {
            for (String symbol : shardSymbols.get(i).keySet()) {
                bySymbol.put(symbol, shards.get(i));
            }
        }
        this.shardBySymbol = Map.copyOf(bySymbol);
        // Thread.start() happens-before everything the shard thread does, so the books rebuilt
        // above on this thread are safely published to their owners.
        shards.forEach(Shard::start);
    }

    /** Rebuild the books of one shard by folding its journal through {@link OrderBook#apply}. */
    private static long recover(int shard, Journal journal, Map<String, OrderBook> books) throws IOException {
        long[] count = {0};
        journal.replay(event -> {
            OrderBook book = books.get(event.symbol());
            if (book == null) {
                throw new IllegalStateException("Journal of shard " + shard + " contains events for "
                        + event.symbol() + ", which is not configured on this shard");
            }
            book.apply(event);
            count[0]++;
        });
        return count[0];
    }

    /**
     * Stable symbol-to-shard routing. {@link String#hashCode()} is specified by the JLS, so this is
     * the same on every JVM and every run, which the journal layout depends on.
     */
    public static int shardIndex(String symbol, int shardCount) {
        return Math.floorMod(symbol.hashCode(), shardCount);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Never blocks and never throws for operational reasons: overload and failure are results. */
    public CompletableFuture<CommandResult> submit(Command command) {
        Objects.requireNonNull(command, "command");
        if (closed) {
            return CompletableFuture.completedFuture(new CommandResult.Unavailable("engine closed"));
        }
        Shard shard = shardBySymbol.get(command.symbol());
        if (shard == null) {
            return CompletableFuture.completedFuture(new CommandResult.UnknownSymbol(command.symbol()));
        }
        return shard.submit(command);
    }

    /** A consistent snapshot of one book, taken on its owning thread between two commands. */
    public CompletableFuture<BookSnapshot> snapshot(String symbol) {
        Shard shard = shardBySymbol.get(symbol);
        if (shard == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown symbol " + symbol));
        }
        return shard.query(symbol);
    }

    public SequencedSet<String> symbols() {
        return new LinkedHashSet<>(shardBySymbol.keySet().stream().sorted().toList());
    }

    /** Number of events replayed from the journals at startup. */
    public long recoveredEvents() {
        return recoveredEvents;
    }

    /** Reason the shard owning {@code symbol} halted, if it did. */
    public Optional<String> haltReason(String symbol) {
        Shard shard = shardBySymbol.get(symbol);
        return shard == null ? Optional.empty() : Optional.ofNullable(shard.haltReason());
    }

    /** Graceful shutdown: commands already queued are processed and committed, later ones are refused. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (Shard shard : shards) {
            try {
                shard.stop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    public static final class Builder {
        private final SequencedSet<String> symbols = new LinkedHashSet<>();
        private int shards = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
        private int queueCapacity = 64 * 1024;
        private int maxBatchSize = 512;
        private JournalFactory journals = JournalFactory.noop();
        private LongSupplier clockMicros = Builder::systemMicros;
        private SequencingListener listener = SequencingListener.NONE;

        private Builder() {
        }

        public Builder symbols(String... symbols) {
            this.symbols.addAll(List.of(symbols));
            return this;
        }

        public Builder symbols(Iterable<String> symbols) {
            symbols.forEach(this.symbols::add);
            return this;
        }

        public Builder shards(int shards) {
            if (shards < 1) {
                throw new IllegalArgumentException("shards must be >= 1");
            }
            this.shards = shards;
            return this;
        }

        public Builder queueCapacity(int queueCapacity) {
            if (queueCapacity < 1) {
                throw new IllegalArgumentException("queueCapacity must be >= 1");
            }
            this.queueCapacity = queueCapacity;
            return this;
        }

        public Builder maxBatchSize(int maxBatchSize) {
            if (maxBatchSize < 1) {
                throw new IllegalArgumentException("maxBatchSize must be >= 1");
            }
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Builder journals(JournalFactory journals) {
            this.journals = Objects.requireNonNull(journals);
            return this;
        }

        /** Source of event timestamps. Only read on shard threads, and only for new commands, never on replay. */
        public Builder clockMicros(LongSupplier clockMicros) {
            this.clockMicros = Objects.requireNonNull(clockMicros);
            return this;
        }

        public Builder sequencingListener(SequencingListener listener) {
            this.listener = Objects.requireNonNull(listener);
            return this;
        }

        /** Opens the journals, replays them into fresh books and starts the shard threads. */
        public MatchingEngine start() {
            if (symbols.isEmpty()) {
                throw new IllegalStateException("At least one symbol must be configured");
            }
            try {
                return new MatchingEngine(this);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to recover engine from journal", e);
            }
        }

        private static long systemMicros() {
            Instant now = Instant.now();
            return now.getEpochSecond() * 1_000_000L + now.getNano() / 1_000;
        }
    }
}
