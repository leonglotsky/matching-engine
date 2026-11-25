package io.github.leonglotsky.matching.book;

import io.github.leonglotsky.matching.domain.EngineEvent;

import java.util.List;

/** What the book decided to do with a command. */
public sealed interface Outcome {

    /** The command produced these events (possibly just a rejection). They must be journaled. */
    record Applied(List<EngineEvent> events) implements Outcome {
        public Applied {
            events = List.copyOf(events);
        }
    }

    /**
     * The same (account, clientOrderId) was already accepted. Nothing happened and nothing needs
     * to be journaled: re-sending an order is safe.
     */
    record Duplicate(long originalOrderId) implements Outcome {
    }
}
