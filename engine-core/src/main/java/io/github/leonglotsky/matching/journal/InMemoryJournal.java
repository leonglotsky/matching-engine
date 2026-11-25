package io.github.leonglotsky.matching.journal;

import io.github.leonglotsky.matching.domain.EngineEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Journal kept on the heap. Honours the append/commit contract (uncommitted events are not visible
 * to {@link #replay} or {@link #committed}), which makes it a faithful stand-in for tests.
 */
public final class InMemoryJournal implements Journal {

    private final List<EngineEvent> pending = new ArrayList<>();
    private final List<EngineEvent> committed = new ArrayList<>();

    @Override
    public void append(EngineEvent event) {
        pending.add(event);
    }

    @Override
    public synchronized void commit() {
        committed.addAll(pending);
        pending.clear();
    }

    @Override
    public void replay(Consumer<EngineEvent> sink) {
        committed().forEach(sink);
    }

    /** Thread-safe copy of everything committed so far. */
    public synchronized List<EngineEvent> committed() {
        return List.copyOf(committed);
    }

    @Override
    public void close() {
    }
}
