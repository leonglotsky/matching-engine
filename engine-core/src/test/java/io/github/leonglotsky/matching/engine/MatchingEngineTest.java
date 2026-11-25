package io.github.leonglotsky.matching.engine;

import io.github.leonglotsky.matching.book.BookSnapshot;
import io.github.leonglotsky.matching.domain.Command;
import io.github.leonglotsky.matching.domain.Command.CancelOrder;
import io.github.leonglotsky.matching.domain.Command.LimitOrder;
import io.github.leonglotsky.matching.domain.EngineEvent;
import io.github.leonglotsky.matching.domain.EngineEvent.OrderAccepted;
import io.github.leonglotsky.matching.domain.EngineEvent.TradeExecuted;
import io.github.leonglotsky.matching.domain.Side;
import io.github.leonglotsky.matching.journal.FileJournal;
import io.github.leonglotsky.matching.journal.InMemoryJournal;
import io.github.leonglotsky.matching.journal.Journal;
import io.github.leonglotsky.matching.journal.JournalFactory;
import io.github.leonglotsky.matching.journal.SyncPolicy;
import io.github.leonglotsky.matching.sim.WorkloadGenerator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static io.github.leonglotsky.matching.engine.EngineTestSupport.fakeClock;
import static io.github.leonglotsky.matching.engine.EngineTestSupport.snapshots;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(30)
class MatchingEngineTest {

    @Nested
    class Basics {

        @Test
        void matchesAndReportsCommittedEvents() {
            try (var engine = MatchingEngine.builder().symbols("ACME").shards(2).clockMicros(fakeClock()).start()) {
                engine.submit(LimitOrder.gtc("ACME", "maker", "m1", Side.SELL, 100, 10)).join();
                var result = engine.submit(LimitOrder.gtc("ACME", "taker", "t1", Side.BUY, 100, 4)).join();

                assertThat(result).isInstanceOfSatisfying(CommandResult.Processed.class, p ->
                        assertThat(p.events()).hasExactlyElementsOfTypes(OrderAccepted.class, TradeExecuted.class));
                BookSnapshot book = engine.snapshot("ACME").join();
                assertThat(book.asks().getFirst().totalQuantity()).isEqualTo(6);
            }
        }

        @Test
        void unknownSymbolIsReportedNotThrown() {
            try (var engine = MatchingEngine.builder().symbols("ACME").start()) {
                assertThat(engine.submit(LimitOrder.gtc("NOPE", "a", "1", Side.BUY, 1, 1)).join())
                        .isEqualTo(new CommandResult.UnknownSymbol("NOPE"));
            }
        }

        @Test
        void symbolRoutingIsStable() {
            assertThat(MatchingEngine.shardIndex("AAPL", 4)).isEqualTo(Math.floorMod("AAPL".hashCode(), 4));
            assertThat(MatchingEngine.shardIndex("AAPL", 1)).isZero();
        }

        @Test
        void requiresAtLeastOneSymbol() {
            assertThatThrownBy(() -> MatchingEngine.builder().start()).isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    class Idempotency {

        @Test
        void concurrentRetriesOfTheSameOrderExecuteExactlyOnce() throws Exception {
            try (var engine = MatchingEngine.builder().symbols("ACME").clockMicros(fakeClock()).start()) {
                var order = LimitOrder.gtc("ACME", "a", "retry-me", Side.BUY, 100, 10);
                var results = new ArrayList<CompletableFuture<CommandResult>>();
                var go = new CountDownLatch(1);
                try (var clients = Executors.newVirtualThreadPerTaskExecutor()) {
                    for (int i = 0; i < 200; i++) {
                        var f = new CompletableFuture<CommandResult>();
                        results.add(f);
                        clients.submit(() -> {
                            go.await();
                            engine.submit(order).thenAccept(f::complete);
                            return null;
                        });
                    }
                    go.countDown();
                }
                var outcomes = results.stream().map(CompletableFuture::join).toList();
                assertThat(outcomes).filteredOn(CommandResult.Processed.class::isInstance).hasSize(1);
                assertThat(outcomes).filteredOn(CommandResult.Duplicate.class::isInstance).hasSize(199)
                        .allMatch(r -> ((CommandResult.Duplicate) r).originalOrderId() == 1);
                assertThat(engine.snapshot("ACME").join().bids()).hasSize(1);
            }
        }
    }

    @Nested
    class Recovery {

        @TempDir
        Path dir;

        @Test
        void restartFromFileJournalRestoresBooksIdempotencyAndSequencing() throws IOException {
            List<String> symbols = List.of("AAPL", "MSFT", "GOOG");
            Map<String, BookSnapshot> before;
            try (var engine = startOn(dir, symbols)) {
                for (String s : symbols) {
                    var gen = new WorkloadGenerator(s, "acct", s.hashCode(), 10_000);
                    for (int i = 0; i < 3_000; i++) {
                        engine.submit(gen.next()).join();
                    }
                }
                engine.submit(LimitOrder.gtc("AAPL", "acct", "keep-me", Side.BUY, 1, 5)).join();
                before = snapshots(engine);
            }

            try (var engine = startOn(dir, symbols)) {
                assertThat(engine.recoveredEvents()).isPositive();
                assertThat(snapshots(engine)).isEqualTo(before);

                // A client retrying after the restart is told the order already exists.
                assertThat(engine.submit(LimitOrder.gtc("AAPL", "acct", "keep-me", Side.BUY, 1, 5)).join())
                        .isInstanceOf(CommandResult.Duplicate.class);

                // New work continues the sequence without gaps.
                var next = (CommandResult.Processed) engine.submit(LimitOrder.gtc("AAPL", "acct", "after-restart", Side.BUY, 1, 1)).join();
                assertThat(next.events().getFirst().sequence()).isEqualTo(before.get("AAPL").lastSequence() + 1);

                // And cancels of orders placed before the restart still work.
                var cancel = (CommandResult.Processed) engine.submit(new CancelOrder("AAPL", "acct", "keep-me")).join();
                assertThat(cancel.events().getFirst()).isInstanceOf(EngineEvent.OrderCancelled.class);
            }
        }

        @Test
        void startingWithADifferentShardCountIsRefused() throws IOException {
            try (var ignored = startOn(dir, List.of("AAPL"))) {
                // creates the manifest with 2 shards
            }
            assertThatThrownBy(() -> FileJournal.directory(dir, 3, SyncPolicy.OS_CACHE))
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        void journalWithEventsForAnUnconfiguredSymbolIsRefused() throws IOException {
            try (var engine = startOn(dir, List.of("AAPL", "MSFT"))) {
                engine.submit(LimitOrder.gtc("MSFT", "a", "1", Side.BUY, 1, 1)).join();
            }
            // Same shard count, but MSFT was dropped from the configuration: its history would be orphaned.
            assertThatThrownBy(() -> startOn(dir, List.of("AAPL")))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not configured");
        }

        private static MatchingEngine startOn(Path dir, List<String> symbols) throws IOException {
            return MatchingEngine.builder()
                    .symbols(symbols)
                    .shards(2)
                    .journals(FileJournal.directory(dir, 2, SyncPolicy.FSYNC))
                    .clockMicros(fakeClock())
                    .start();
        }
    }

    @Nested
    class Failures {

        /** Journal whose commit fails on demand, to simulate a full disk or I/O error. */
        static final class FlakyJournal implements Journal {
            final InMemoryJournal delegate = new InMemoryJournal();
            volatile boolean failCommits;

            @Override
            public void append(EngineEvent event) {
                delegate.append(event);
            }

            @Override
            public void commit() throws IOException {
                if (failCommits) {
                    throw new IOException("No space left on device");
                }
                delegate.commit();
            }

            @Override
            public void replay(Consumer<EngineEvent> sink) {
                delegate.replay(sink);
            }

            @Override
            public void close() {
            }
        }

        @Test
        void journalFailureHaltsOnlyTheAffectedShardAndNothingUndurableIsAcknowledged() {
            // Two symbols on different shards.
            String a = "AAPL";
            String b = java.util.stream.Stream.of("MSFT", "GOOG", "AMZN", "NVDA")
                    .filter(s -> MatchingEngine.shardIndex(s, 2) != MatchingEngine.shardIndex(a, 2))
                    .findFirst().orElseThrow();
            var journals = new FlakyJournal[]{new FlakyJournal(), new FlakyJournal()};
            int shardA = MatchingEngine.shardIndex(a, 2);

            try (var engine = MatchingEngine.builder().symbols(a, b).shards(2)
                    .journals(shard -> journals[shard]).clockMicros(fakeClock()).start()) {
                assertThat(engine.submit(LimitOrder.gtc(a, "x", "1", Side.BUY, 100, 1)).join())
                        .isInstanceOf(CommandResult.Processed.class);

                journals[shardA].failCommits = true;

                assertThat(engine.submit(LimitOrder.gtc(a, "x", "2", Side.BUY, 100, 1)).join())
                        .isInstanceOfSatisfying(CommandResult.Unavailable.class, u ->
                                assertThat(u.reason()).contains("No space left on device"));
                assertThat(engine.haltReason(a)).isPresent();

                // Fail-stop: later commands are refused even if the disk recovers...
                journals[shardA].failCommits = false;
                assertThat(engine.submit(LimitOrder.gtc(a, "x", "3", Side.BUY, 100, 1)).join())
                        .isInstanceOf(CommandResult.Unavailable.class);
                // ...while the other shard is unaffected.
                assertThat(engine.submit(LimitOrder.gtc(b, "x", "1", Side.BUY, 100, 1)).join())
                        .isInstanceOf(CommandResult.Processed.class);
                assertThat(engine.haltReason(b)).isEmpty();
            }
            // The journal holds exactly what was acknowledged.
            assertThat(journals[shardA].delegate.committed()).hasSize(2);
        }

        @Test
        void bugInMatchingHaltsTheShardInsteadOfCorruptingIt() {
            // A listener that throws stands in for any unexpected exception on the shard thread.
            try (var engine = MatchingEngine.builder().symbols("ACME").shards(1)
                    .sequencingListener((c, ts) -> {
                        if (c.clientOrderId().equals("boom")) {
                            throw new IllegalStateException("bug");
                        }
                    }).start()) {
                assertThat(engine.submit(LimitOrder.gtc("ACME", "a", "boom", Side.BUY, 1, 1)).join())
                        .isInstanceOf(CommandResult.Unavailable.class);
                assertThat(engine.submit(LimitOrder.gtc("ACME", "a", "fine", Side.BUY, 1, 1)).join())
                        .isInstanceOf(CommandResult.Unavailable.class);
            }
        }
    }

    @Nested
    class Backpressure {

        /** Journal whose commit blocks until released, to stall the shard deterministically. */
        static final class GatedJournal implements Journal {
            final CountDownLatch entered = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);

            @Override
            public void append(EngineEvent event) {
            }

            @Override
            public void commit() {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void replay(Consumer<EngineEvent> sink) {
            }

            @Override
            public void close() {
            }
        }

        @Test
        void fullQueueRejectsInsteadOfBlockingOrGrowingWithoutBound() throws Exception {
            var journal = new GatedJournal();
            JournalFactory factory = shard -> journal;
            try (var engine = MatchingEngine.builder().symbols("ACME").shards(1)
                    .queueCapacity(4).maxBatchSize(1).journals(factory).start()) {
                var first = engine.submit(order(0));
                journal.entered.await(); // shard is now stuck committing order 0

                var queued = new ArrayList<CompletableFuture<CommandResult>>();
                for (int i = 1; i <= 4; i++) {
                    queued.add(engine.submit(order(i)));
                }
                assertThat(engine.submit(order(5)).join()).isEqualTo(new CommandResult.Overloaded("ACME"));

                journal.release.countDown();
                assertThat(first.join()).isInstanceOf(CommandResult.Processed.class);
                for (var f : queued) {
                    assertThat(f.join()).isInstanceOf(CommandResult.Processed.class);
                }
            }
        }

        private static Command order(int i) {
            return LimitOrder.gtc("ACME", "a", "o" + i, Side.BUY, 100, 1);
        }
    }

    @Nested
    class Shutdown {

        @Test
        void closeDrainsQueuedWorkAndRefusesNewWork() {
            var journal = new InMemoryJournal();
            var engine = MatchingEngine.builder().symbols("ACME").shards(1).journals(s -> journal).start();
            var futures = new ArrayList<CompletableFuture<CommandResult>>();
            for (int i = 0; i < 1_000; i++) {
                futures.add(engine.submit(LimitOrder.gtc("ACME", "a", "o" + i, Side.BUY, 100, 1)));
            }
            engine.close();

            assertThat(futures).allSatisfy(f -> assertThat(f.join()).isInstanceOf(CommandResult.Processed.class));
            assertThat(journal.committed()).hasSize(2_000);
            assertThat(engine.submit(LimitOrder.gtc("ACME", "a", "late", Side.BUY, 100, 1)).join())
                    .isInstanceOf(CommandResult.Unavailable.class);
        }
    }
}
