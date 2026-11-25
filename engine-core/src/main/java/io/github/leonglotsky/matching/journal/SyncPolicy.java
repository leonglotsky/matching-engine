package io.github.leonglotsky.matching.journal;

/** What "committed" means for a {@link FileJournal}. See docs/failure-scenarios.md. */
public enum SyncPolicy {
    /**
     * Write to the OS page cache only. Survives a JVM crash or kill -9, but acknowledged events
     * can be lost on power failure or kernel panic. Fastest.
     */
    OS_CACHE,
    /**
     * {@code fsync} (FileChannel.force) on every commit. Survives power loss. The cost is amortised
     * by group commit: one fsync covers a whole batch of commands.
     */
    FSYNC
}
