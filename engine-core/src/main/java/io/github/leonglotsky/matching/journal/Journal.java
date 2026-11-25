package io.github.leonglotsky.matching.journal;

import io.github.leonglotsky.matching.domain.EngineEvent;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Append-only, ordered log of {@link EngineEvent}s for one shard. It is the source of truth:
 * the in-memory order books are a cache that can always be rebuilt by {@link #replay}.
 *
 * <p>Writes are two-phase to allow group commit: {@link #append} buffers, {@link #commit} makes
 * everything appended so far durable according to the journal's policy. The engine only
 * acknowledges a command after the commit that covers its events has returned.
 *
 * <p>Implementations are used by a single writer thread and need not be thread-safe for writes.
 */
public interface Journal extends AutoCloseable {

    void append(EngineEvent event) throws IOException;

    void commit() throws IOException;

    /** Stream every committed event, oldest first. Called once at startup, before any append. */
    void replay(Consumer<EngineEvent> sink) throws IOException;

    @Override
    void close() throws IOException;

    /** A journal that remembers nothing. Useful for benchmarks that isolate matching cost. */
    static Journal noop() {
        return new Journal() {
            @Override
            public void append(EngineEvent event) {
            }

            @Override
            public void commit() {
            }

            @Override
            public void replay(Consumer<EngineEvent> sink) {
            }

            @Override
            public void close() {
            }
        };
    }
}
