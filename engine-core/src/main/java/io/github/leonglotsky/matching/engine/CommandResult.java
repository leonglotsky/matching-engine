package io.github.leonglotsky.matching.engine;

import io.github.leonglotsky.matching.domain.EngineEvent;

import java.util.List;

/**
 * Answer to {@link MatchingEngine#submit}. Sealed so callers handle every case with a switch,
 * including the operational ones (backpressure, failed shard) that an exception would let them forget.
 */
public sealed interface CommandResult {

    /** The command was sequenced, matched and its events are committed to the journal. */
    record Processed(List<EngineEvent> events) implements CommandResult {
        public Processed {
            events = List.copyOf(events);
        }
    }

    /** This (account, clientOrderId) was already accepted; nothing happened. Safe outcome of a retry. */
    record Duplicate(long originalOrderId) implements CommandResult {
    }

    /** The shard's inbound queue is full. Nothing happened; the client should back off and retry. */
    record Overloaded(String symbol) implements CommandResult {
    }

    /** The shard is halted (journal failure, invariant violation) or the engine is closed. Nothing happened. */
    record Unavailable(String reason) implements CommandResult {
    }

    /** The symbol is not configured on this engine. */
    record UnknownSymbol(String symbol) implements CommandResult {
    }
}
