package io.github.leonglotsky.matching.journal;

import java.io.IOException;

/** Opens the journal that belongs to one shard. */
@FunctionalInterface
public interface JournalFactory {

    Journal open(int shardIndex) throws IOException;

    static JournalFactory noop() {
        return shard -> Journal.noop();
    }
}
