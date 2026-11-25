package io.github.leonglotsky.matching.engine;

import io.github.leonglotsky.matching.domain.Command;

/**
 * Observes commands in the exact order a shard sequences them, together with the timestamp it
 * stamped. Invoked on the shard thread, so it must be fast and must not block.
 *
 * <p>This is the "input tape". Feeding the same tape to a fresh {@code OrderBook} must reproduce the
 * same events bit for bit; the determinism tests rely on that.
 */
@FunctionalInterface
public interface SequencingListener {

    void onSequenced(Command command, long timestampMicros);

    SequencingListener NONE = (command, timestampMicros) -> {
    };
}
