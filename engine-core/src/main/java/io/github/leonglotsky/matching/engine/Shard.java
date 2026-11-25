package io.github.leonglotsky.matching.engine;

import io.github.leonglotsky.matching.book.BookSnapshot;
import io.github.leonglotsky.matching.book.OrderBook;
import io.github.leonglotsky.matching.book.Outcome;
import io.github.leonglotsky.matching.domain.Command;
import io.github.leonglotsky.matching.domain.EngineEvent;
import io.github.leonglotsky.matching.journal.Journal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

/**
 * One single-writer partition of the engine: a bounded inbox, a set of order books, a journal and
 * exactly one platform thread that owns all three.
 *
 * <h2>Processing loop</h2>
 * <ol>
 *   <li>Block for one envelope, then drain up to {@code maxBatchSize - 1} more without blocking.</li>
 *   <li>For each command: stamp time, decide and apply against the book, append events to the journal.</li>
 *   <li>Commit the journal <em>once</em> for the whole batch (group commit).</li>
 *   <li>Only then complete the callers' futures.</li>
 * </ol>
 * Batching adapts to load: under light load batches are size 1 (lowest latency), under heavy load
 * they grow and the per-commit cost is shared (highest throughput).
 *
 * <h2>Failure</h2>
 * If anything in steps 2-3 throws, the in-memory books may be ahead of the journal. The shard
 * halts (fail-stop): every caller in the batch and every later caller gets
 * {@link CommandResult.Unavailable}, and no futures were completed with results that are not durable.
 * A restart rebuilds the books from the journal, which is consistent with what was acknowledged.
 */
final class Shard implements Runnable {

    sealed interface Envelope {
        record Submit(Command command, CompletableFuture<CommandResult> result) implements Envelope {
        }

        record Query(String symbol, CompletableFuture<BookSnapshot> result) implements Envelope {
        }

        record Stop() implements Envelope {
        }
    }

    private static final System.Logger LOG = System.getLogger(Shard.class.getName());

    private final int index;
    private final Map<String, OrderBook> books;
    private final BlockingQueue<Envelope> inbox;
    private final Journal journal;
    private final LongSupplier clockMicros;
    private final SequencingListener listener;
    private final int maxBatchSize;
    private final Thread thread;

    private volatile String haltReason;
    private volatile boolean terminated;

    Shard(int index, Map<String, OrderBook> books, Journal journal, int queueCapacity, int maxBatchSize,
          LongSupplier clockMicros, SequencingListener listener) {
        this.index = index;
        this.books = Map.copyOf(books);
        this.inbox = new ArrayBlockingQueue<>(queueCapacity);
        this.journal = journal;
        this.maxBatchSize = maxBatchSize;
        this.clockMicros = clockMicros;
        this.listener = listener;
        // A platform thread, deliberately: this is a long-lived, CPU-bound hot loop. Virtual threads
        // pay off for many blocking tasks (the clients), not for a handful of spinning owners.
        this.thread = Thread.ofPlatform().name("matching-shard-" + index).daemon(true).unstarted(this);
    }

    void start() {
        thread.start();
    }

    CompletableFuture<CommandResult> submit(Command command) {
        var future = new CompletableFuture<CommandResult>();
        String halted = haltReason;
        if (halted != null) {
            future.complete(new CommandResult.Unavailable(halted));
            return future;
        }
        if (!inbox.offer(new Envelope.Submit(command, future))) {
            future.complete(new CommandResult.Overloaded(command.symbol()));
            return future;
        }
        if (terminated) {
            // Raced with shutdown after the final drain; no one else will complete it.
            future.complete(new CommandResult.Unavailable("engine closed"));
        }
        return future;
    }

    CompletableFuture<BookSnapshot> query(String symbol) {
        var future = new CompletableFuture<BookSnapshot>();
        if (!inbox.offer(new Envelope.Query(symbol, future))) {
            future.completeExceptionally(new IllegalStateException("shard " + index + " overloaded"));
        } else if (terminated) {
            future.completeExceptionally(new IllegalStateException("engine closed"));
        }
        return future;
    }

    void stop() throws InterruptedException {
        inbox.put(new Envelope.Stop());
        thread.join();
    }

    String haltReason() {
        return haltReason;
    }

    @Override
    public void run() {
        var batch = new ArrayList<Envelope>(maxBatchSize);
        try {
            boolean stop = false;
            while (!stop) {
                batch.add(inbox.take());
                inbox.drainTo(batch, maxBatchSize - 1);
                stop = haltReason == null ? process(batch) : reject(batch);
                batch.clear();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            terminated = true;
            batch.clear();
            inbox.drainTo(batch);
            reject(batch);
            try {
                journal.close();
            } catch (Exception e) {
                LOG.log(System.Logger.Level.WARNING, "Failed to close journal of shard " + index, e);
            }
        }
    }

    /** Returns true if a Stop was seen. */
    private boolean process(List<Envelope> batch) {
        var results = new Object[batch.size()];
        boolean stop = false;
        try {
            for (int i = 0; i < batch.size(); i++) {
                switch (batch.get(i)) {
                    case Envelope.Submit(var command, var ignored) -> {
                        long now = clockMicros.getAsLong();
                        listener.onSequenced(command, now);
                        results[i] = switch (books.get(command.symbol()).process(command, now)) {
                            case Outcome.Applied(var events) -> {
                                for (EngineEvent event : events) {
                                    journal.append(event);
                                }
                                yield new CommandResult.Processed(events);
                            }
                            case Outcome.Duplicate(var orderId) -> new CommandResult.Duplicate(orderId);
                        };
                    }
                    case Envelope.Query(var symbol, var ignored) -> results[i] = books.get(symbol).snapshot();
                    case Envelope.Stop() -> stop = true;
                }
            }
            journal.commit();
        } catch (Throwable t) {
            halt(t);
            reject(batch);
            return stop;
        }
        // Durable: now, and only now, tell the callers.
        for (int i = 0; i < batch.size(); i++) {
            switch (batch.get(i)) {
                case Envelope.Submit(var c, var future) -> future.complete((CommandResult) results[i]);
                case Envelope.Query(var s, var future) -> future.complete((BookSnapshot) results[i]);
                case Envelope.Stop() -> {
                }
            }
        }
        return stop;
    }

    private boolean reject(List<Envelope> batch) {
        String reason = haltReason != null ? haltReason : "engine closed";
        boolean stop = false;
        for (Envelope envelope : batch) {
            switch (envelope) {
                case Envelope.Submit(var c, var future) -> future.complete(new CommandResult.Unavailable(reason));
                case Envelope.Query(var s, var future) -> future.completeExceptionally(new IllegalStateException(reason));
                case Envelope.Stop() -> stop = true;
            }
        }
        return stop;
    }

    private void halt(Throwable cause) {
        // For the batch in flight the outcome is unknown: the failure may have happened after some bytes
        // reached the disk. Clients reconcile by resubmitting with the same clientOrderId once the
        // engine is back: idempotency turns that into either a Duplicate or a first execution.
        haltReason = "shard " + index + " halted, outcome of in-flight commands unknown: " + cause;
        LOG.log(System.Logger.Level.ERROR, haltReason, cause);
    }
}
