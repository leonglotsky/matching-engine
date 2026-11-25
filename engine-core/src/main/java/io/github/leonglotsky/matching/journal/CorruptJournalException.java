package io.github.leonglotsky.matching.journal;

import java.io.IOException;

/**
 * A record in the middle of a journal failed its checksum or framing check. Unlike a torn tail
 * (an interrupted last write, which is safely truncated), this means previously acknowledged data
 * is damaged, so the engine refuses to start rather than silently serving a different book.
 */
public final class CorruptJournalException extends IOException {
    public CorruptJournalException(String message) {
        super(message);
    }
}
