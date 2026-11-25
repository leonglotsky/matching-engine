package io.github.leonglotsky.matching.demo;

import io.github.leonglotsky.matching.book.BookSnapshot;
import io.github.leonglotsky.matching.domain.Command;
import io.github.leonglotsky.matching.domain.EngineEvent;
import io.github.leonglotsky.matching.engine.CommandResult;
import io.github.leonglotsky.matching.engine.MatchingEngine;
import io.github.leonglotsky.matching.journal.FileJournal;
import io.github.leonglotsky.matching.journal.JournalFactory;
import io.github.leonglotsky.matching.journal.SyncPolicy;
import io.github.leonglotsky.matching.sim.WorkloadGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;

/**
 * End-to-end demo: many virtual-thread clients trade against a file-journaled engine, then the
 * engine is restarted from the journal and every book is checked to be identical.
 *
 * <pre>
 * java -jar engine-core/target/engine-core-*.jar [--clients 64] [--orders 20000] [--symbols 16]
 *                                                [--shards 4] [--journal ./journal] [--fsync]
 * </pre>
 */
public final class DemoMain {

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = parse(args);
        int clients = Integer.parseInt(opts.getOrDefault("clients", "64"));
        int ordersPerClient = Integer.parseInt(opts.getOrDefault("orders", "20000"));
        int symbolCount = Integer.parseInt(opts.getOrDefault("symbols", "16"));
        int shards = Integer.parseInt(opts.getOrDefault("shards", "4"));
        Path dir = Path.of(opts.getOrDefault("journal", "journal"));
        SyncPolicy sync = opts.containsKey("fsync") ? SyncPolicy.FSYNC : SyncPolicy.OS_CACHE;

        List<String> symbols = IntStream.range(0, symbolCount).mapToObj(i -> "SYM" + i).toList();
        deleteRecursively(dir);

        System.out.printf("Matching engine demo: %d clients x %,d orders, %d symbols on %d shards, journal=%s (%s)%n",
                clients, ordersPerClient, symbolCount, shards, dir.toAbsolutePath(), sync);

        // ---- phase 1: trade ------------------------------------------------------------------
        Map<String, BookSnapshot> before;
        long[] latencies;
        var processed = new LongAdder();
        var trades = new LongAdder();
        long elapsedNanos;
        JournalFactory journals = FileJournal.directory(dir, shards, sync);
        try (var engine = MatchingEngine.builder().symbols(symbols).shards(shards).journals(journals).start();
             var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var perClient = new ArrayList<Future<long[]>>();
            long start = System.nanoTime();
            for (int c = 0; c < clients; c++) {
                int client = c;
                perClient.add(pool.submit(() -> runClient(engine, symbols, client, ordersPerClient, processed, trades)));
            }
            var all = new ArrayList<long[]>();
            for (var f : perClient) {
                all.add(f.get());
            }
            elapsedNanos = System.nanoTime() - start;
            latencies = all.stream().flatMapToLong(Arrays::stream).sorted().toArray();
            before = snapshots(engine, symbols);
        }

        long total = (long) clients * ordersPerClient;
        System.out.printf("%nProcessed %,d commands in %.2f s -> %,.0f commands/s, %,d trades%n",
                total, elapsedNanos / 1e9, total / (elapsedNanos / 1e9), trades.sum());
        System.out.printf("Round-trip latency (submit -> journaled ack), closed loop: p50 %s  p99 %s  p99.9 %s  max %s%n",
                micros(latencies, 0.50), micros(latencies, 0.99), micros(latencies, 0.999), micros(latencies, 1.0));

        // ---- phase 2: crash-free restart and verify ----------------------------------------------
        long recoveryStart = System.nanoTime();
        try (var engine = MatchingEngine.builder().symbols(symbols).shards(shards)
                .journals(FileJournal.directory(dir, shards, sync)).start()) {
            long recoveryNanos = System.nanoTime() - recoveryStart;
            Map<String, BookSnapshot> after = snapshots(engine, symbols);
            long journalBytes = Files.walk(dir).filter(Files::isRegularFile).mapToLong(DemoMain::size).sum();
            System.out.printf("%nRecovered %,d events (%,.1f MB) in %.2f s -> %,.0f events/s%n",
                    engine.recoveredEvents(), journalBytes / 1e6, recoveryNanos / 1e9,
                    engine.recoveredEvents() / (recoveryNanos / 1e9));
            boolean identical = after.equals(before);
            System.out.printf("Replay check: %d/%d books identical to pre-restart state -> %s%n",
                    symbols.stream().filter(s -> after.get(s).equals(before.get(s))).count(), symbols.size(),
                    identical ? "OK" : "MISMATCH");
            BookSnapshot sample = after.get(symbols.getFirst());
            System.out.printf("%s: best bid %s, best ask %s, %d bid levels, %d ask levels, last seq %,d%n",
                    sample.symbol(),
                    sample.bids().isEmpty() ? "-" : sample.bids().getFirst().price(),
                    sample.asks().isEmpty() ? "-" : sample.asks().getFirst().price(),
                    sample.bids().size(), sample.asks().size(), sample.lastSequence());
            if (!identical) {
                System.exit(1);
            }
        }
    }

    private static long[] runClient(MatchingEngine engine, List<String> symbols, int client, int orders,
                                    LongAdder processed, LongAdder trades) {
        var rnd = new SplittableRandom(client);
        var generators = new HashMap<String, WorkloadGenerator>();
        long[] latencies = new long[orders];
        for (int i = 0; i < orders; i++) {
            String symbol = symbols.get(rnd.nextInt(symbols.size()));
            Command command = generators.computeIfAbsent(symbol,
                    s -> new WorkloadGenerator(s, "client-" + client, rnd.nextLong(), 10_000)).next();
            long t0 = System.nanoTime();
            CommandResult result = engine.submit(command).join();
            latencies[i] = System.nanoTime() - t0;
            switch (result) {
                case CommandResult.Processed(var events) -> {
                    processed.increment();
                    trades.add(events.stream().filter(EngineEvent.TradeExecuted.class::isInstance).count());
                }
                case CommandResult.Duplicate d -> { }
                case CommandResult.Overloaded o -> Thread.onSpinWait();
                case CommandResult.Unavailable u -> throw new IllegalStateException(u.reason());
                case CommandResult.UnknownSymbol u -> throw new IllegalStateException(u.symbol());
            }
        }
        return latencies;
    }

    private static Map<String, BookSnapshot> snapshots(MatchingEngine engine, List<String> symbols) {
        var out = new HashMap<String, BookSnapshot>();
        for (String s : symbols) {
            out.put(s, engine.snapshot(s).join());
        }
        return out;
    }

    private static String micros(long[] sorted, double q) {
        int idx = (int) Math.min(sorted.length - 1, Math.ceil(q * sorted.length) - 1);
        return "%.1f us".formatted(sorted[Math.max(0, idx)] / 1_000.0);
    }

    private static Map<String, String> parse(String[] args) {
        var out = new HashMap<String, String>();
        for (int i = 0; i < args.length; i++) {
            String key = args[i].replaceFirst("^--", "");
            if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                out.put(key, args[++i]);
            } else {
                out.put(key, "true");
            }
        }
        return out;
    }

    private static long size(Path p) {
        try {
            return Files.size(p);
        } catch (IOException e) {
            return 0;
        }
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (var paths = Files.walk(dir)) {
            for (Path p : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                Files.delete(p);
            }
        }
    }
}
