package io.github.leonglotsky.matching.engine;

import io.github.leonglotsky.matching.book.BookSnapshot;
import io.github.leonglotsky.matching.book.OrderBook;
import io.github.leonglotsky.matching.book.Outcome;
import io.github.leonglotsky.matching.domain.Command;
import io.github.leonglotsky.matching.domain.EngineEvent;
import io.github.leonglotsky.matching.journal.InMemoryJournal;
import io.github.leonglotsky.matching.journal.JournalFactory;
import io.github.leonglotsky.matching.sim.WorkloadGenerator;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.leonglotsky.matching.engine.EngineTestSupport.Sequenced;
import static io.github.leonglotsky.matching.engine.EngineTestSupport.assertNotCrossed;
import static io.github.leonglotsky.matching.engine.EngineTestSupport.assertQuantityConserved;
import static io.github.leonglotsky.matching.engine.EngineTestSupport.fakeClock;
import static io.github.leonglotsky.matching.engine.EngineTestSupport.snapshots;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The central claim of the design: concurrent submission only decides the <em>order</em> in which a
 * shard sees commands. Once sequenced, the outcome is a pure function of that order, and the journal
 * alone is enough to rebuild the exact same books.
 */
class DeterminismTest {

    private static final List<String> SYMBOLS = List.of("AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "META", "TSLA", "NFLX");
    private static final int CLIENTS = 32;
    private static final int ORDERS_PER_CLIENT = 2_000;

    @RepeatedTest(3)
    void concurrentRunIsReproducibleFromItsInputTapeAndFromItsJournal() throws Exception {
        // --- run: many virtual-thread clients hammer 8 symbols on 4 shards -------------------------
        Map<String, List<Sequenced>> tapes = new ConcurrentHashMap<>();
        Map<Integer, InMemoryJournal> journals = new ConcurrentHashMap<>();
        JournalFactory journalFactory = shard -> journals.computeIfAbsent(shard, s -> new InMemoryJournal());

        MatchingEngine engine = MatchingEngine.builder()
                .symbols(SYMBOLS)
                .shards(4)
                .maxBatchSize(64)
                .journals(journalFactory)
                .clockMicros(fakeClock())
                .sequencingListener((command, ts) ->
                        // Each symbol is only ever touched by its own shard thread, so a plain list is safe.
                        tapes.computeIfAbsent(command.symbol(), s -> new ArrayList<>()).add(new Sequenced(command, ts)))
                .start();

        var start = new CountDownLatch(1);
        var nonProcessed = new AtomicInteger();
        try (var clients = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int c = 0; c < CLIENTS; c++) {
                int client = c;
                clients.submit(() -> {
                    var generators = new HashMap<String, WorkloadGenerator>();
                    var rnd = new java.util.SplittableRandom(client);
                    start.await();
                    for (int i = 0; i < ORDERS_PER_CLIENT; i++) {
                        String symbol = SYMBOLS.get(rnd.nextInt(SYMBOLS.size()));
                        Command command = generators
                                .computeIfAbsent(symbol, s -> new WorkloadGenerator(s, "client-" + client, client * 31L + s.hashCode(), 10_000))
                                .next();
                        if (!(engine.submit(command).join() instanceof CommandResult.Processed)) {
                            nonProcessed.incrementAndGet();
                        }
                    }
                    return null;
                });
            }
            start.countDown();
        }
        Map<String, BookSnapshot> live = snapshots(engine);
        engine.close();

        assertThat(nonProcessed).hasValue(0);
        var allEvents = new ArrayList<EngineEvent>();
        journals.values().forEach(j -> allEvents.addAll(j.committed()));
        assertThat(allEvents).hasSizeGreaterThan(CLIENTS * ORDERS_PER_CLIENT);

        // --- invariants ------------------------------------------------------------------------
        live.values().forEach(EngineTestSupport::assertNotCrossed);
        assertQuantityConserved(allEvents, List.copyOf(live.values()));

        // --- 1. same input tape => same events, same book (single-threaded re-execution) ----------
        for (String symbol : SYMBOLS) {
            var book = new OrderBook(symbol);
            var reproduced = new ArrayList<EngineEvent>();
            for (Sequenced s : tapes.get(symbol)) {
                if (book.process(s.command(), s.timestampMicros()) instanceof Outcome.Applied(var events)) {
                    reproduced.addAll(events);
                }
            }
            var journaled = allEvents.stream().filter(e -> e.symbol().equals(symbol)).toList();
            assertThat(reproduced).as("events for %s", symbol).isEqualTo(journaled);
            assertThat(book.snapshot()).as("book for %s", symbol).isEqualTo(live.get(symbol));
        }

        // --- 2. journal replay => same books (recovery) ------------------------------------------
        try (MatchingEngine recovered = MatchingEngine.builder()
                .symbols(SYMBOLS).shards(4).journals(journalFactory).clockMicros(fakeClock()).start()) {
            assertThat(recovered.recoveredEvents()).isEqualTo(allEvents.size());
            assertThat(snapshots(recovered)).isEqualTo(live);
        }
    }

    @Test
    void identicalInputProducesIdenticalOutputAcrossRuns() {
        List<Command> tape = new WorkloadGenerator("ACME", "a", 42, 10_000).next(20_000);
        assertThat(run(tape)).isEqualTo(run(tape));
    }

    @Test
    void differentInterleavingsAcrossSymbolsDoNotAffectEachBook() {
        // Interleave two symbols' flows in two different global orders. Per-symbol order is the same,
        // so each book must end up identical: symbols are independent.
        List<Command> a = new WorkloadGenerator("AAA", "x", 1, 10_000).next(5_000);
        List<Command> b = new WorkloadGenerator("BBB", "y", 2, 10_000).next(5_000);

        var first = new ArrayList<Command>();
        first.addAll(a);
        first.addAll(b);
        var second = interleave(a, b);

        assertThat(runPerSymbol(first)).isEqualTo(runPerSymbol(second));
    }

    private static List<EngineEvent> run(List<Command> tape) {
        var book = new OrderBook("ACME");
        var out = new ArrayList<EngineEvent>();
        long ts = 0;
        for (Command c : tape) {
            if (book.process(c, ++ts) instanceof Outcome.Applied(var events)) {
                out.addAll(events);
            }
        }
        return out;
    }

    private static Map<String, BookSnapshot> runPerSymbol(List<Command> tape) {
        // Run through the real engine with one shard so both symbols compete for the same thread.
        try (MatchingEngine engine = MatchingEngine.builder().symbols("AAA", "BBB").shards(1)
                .clockMicros(() -> 0L).start()) {
            for (Command c : tape) {
                engine.submit(c).join();
            }
            return snapshots(engine);
        }
    }

    private static List<Command> interleave(List<Command> a, List<Command> b) {
        var out = new ArrayList<Command>(a.size() + b.size());
        var rnd = new java.util.Random(99);
        int i = 0;
        int j = 0;
        while (i < a.size() || j < b.size()) {
            if (j >= b.size() || (i < a.size() && rnd.nextBoolean())) {
                out.add(a.get(i++));
            } else {
                out.add(b.get(j++));
            }
        }
        return Collections.unmodifiableList(out);
    }
}
