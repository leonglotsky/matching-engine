package io.github.leonglotsky.matching.journal;

import io.github.leonglotsky.matching.book.OrderBook;
import io.github.leonglotsky.matching.book.Outcome;
import io.github.leonglotsky.matching.domain.Command;
import io.github.leonglotsky.matching.domain.EngineEvent;
import io.github.leonglotsky.matching.sim.WorkloadGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileJournalTest {

    @TempDir
    Path dir;

    /** A realistic event stream covering every event type. */
    private static List<EngineEvent> sampleEvents(int commands) {
        var book = new OrderBook("ACME");
        var gen = new WorkloadGenerator("ACME", "acct", 7, 10_000);
        var events = new ArrayList<EngineEvent>();
        for (int i = 0; i < commands; i++) {
            Command c = gen.next();
            if (book.process(c, i) instanceof Outcome.Applied(var e)) {
                events.addAll(e);
            }
        }
        return events;
    }

    private static List<EngineEvent> readAll(FileJournal journal) throws IOException {
        var out = new ArrayList<EngineEvent>();
        journal.replay(out::add);
        return out;
    }

    @Test
    void codecRoundTripsEveryEventType() throws IOException {
        var events = sampleEvents(2_000);
        assertThat(events).extracting(e -> e.getClass().getSimpleName()).contains(
                "OrderAccepted", "OrderRejected", "TradeExecuted", "OrderRested", "OrderCancelled", "CancelRejected");
        for (EngineEvent event : events) {
            var bytes = new ByteArrayOutputStream();
            EventCodec.encode(event, new DataOutputStream(bytes));
            var decoded = EventCodec.decode(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
            assertThat(decoded).isEqualTo(event);
        }
    }

    @Test
    void committedEventsSurviveReopen() throws IOException {
        var events = sampleEvents(500);
        Path file = dir.resolve("j");
        try (var journal = FileJournal.open(file, SyncPolicy.FSYNC)) {
            for (EngineEvent e : events) {
                journal.append(e);
            }
            journal.commit();
        }
        try (var reopened = FileJournal.open(file, SyncPolicy.FSYNC)) {
            assertThat(reopened.truncatedBytesOnOpen()).isZero();
            assertThat(readAll(reopened)).isEqualTo(events);
        }
    }

    @Test
    void uncommittedEventsAreNotPersisted() throws IOException {
        var events = sampleEvents(50);
        Path file = dir.resolve("j");
        try (var journal = FileJournal.open(file, SyncPolicy.OS_CACHE)) {
            journal.append(events.get(0));
            journal.commit();
            journal.append(events.get(1)); // never committed, e.g. the batch failed
        }
        try (var reopened = FileJournal.open(file, SyncPolicy.OS_CACHE)) {
            assertThat(readAll(reopened)).containsExactly(events.get(0));
        }
    }

    @Test
    void appendAfterReopenContinuesTheLog() throws IOException {
        var events = sampleEvents(100);
        Path file = dir.resolve("j");
        int half = events.size() / 2;
        try (var journal = FileJournal.open(file, SyncPolicy.OS_CACHE)) {
            for (EngineEvent e : events.subList(0, half)) {
                journal.append(e);
            }
            journal.commit();
        }
        try (var journal = FileJournal.open(file, SyncPolicy.OS_CACHE)) {
            for (EngineEvent e : events.subList(half, events.size())) {
                journal.append(e);
            }
            journal.commit();
        }
        try (var journal = FileJournal.open(file, SyncPolicy.OS_CACHE)) {
            assertThat(readAll(journal)).isEqualTo(events);
        }
    }

    @Test
    void tornTailIsTruncatedAtEveryPossibleCutPoint() throws IOException {
        var events = sampleEvents(30);
        Path full = dir.resolve("full");
        try (var journal = FileJournal.open(full, SyncPolicy.OS_CACHE)) {
            for (EngineEvent e : events) {
                journal.append(e);
            }
            journal.commit();
        }
        byte[] bytes = Files.readAllBytes(full);

        // Simulate a crash at every byte offset inside the log. Recovery must always yield a clean
        // prefix of the original events, never an exception and never a garbled event.
        for (int cut = FileJournal.FILE_HEADER_BYTES; cut <= bytes.length; cut++) {
            Path torn = dir.resolve("torn-" + cut);
            Files.write(torn, java.util.Arrays.copyOf(bytes, cut));
            try (var journal = FileJournal.open(torn, SyncPolicy.OS_CACHE)) {
                var recovered = readAll(journal);
                assertThat(recovered).isEqualTo(events.subList(0, recovered.size()));
                assertThat(Files.size(torn)).isEqualTo(cut - journal.truncatedBytesOnOpen());
            }
            Files.delete(torn);
        }
    }

    @Test
    void halfWrittenLastRecordWithFullLengthIsTreatedAsTorn() throws IOException {
        var events = sampleEvents(10);
        Path file = dir.resolve("j");
        try (var journal = FileJournal.open(file, SyncPolicy.OS_CACHE)) {
            for (EngineEvent e : events) {
                journal.append(e);
            }
            journal.commit();
        }
        flipLastByte(file);
        try (var journal = FileJournal.open(file, SyncPolicy.OS_CACHE)) {
            assertThat(readAll(journal)).isEqualTo(events.subList(0, events.size() - 1));
        }
    }

    @Test
    void zeroFilledTailIsTreatedAsTorn() throws IOException {
        var events = sampleEvents(10);
        Path file = dir.resolve("j");
        try (var journal = FileJournal.open(file, SyncPolicy.OS_CACHE)) {
            for (EngineEvent e : events) {
                journal.append(e);
            }
            journal.commit();
        }
        Files.write(file, new byte[4096], StandardOpenOption.APPEND);
        try (var journal = FileJournal.open(file, SyncPolicy.OS_CACHE)) {
            assertThat(journal.truncatedBytesOnOpen()).isEqualTo(4096);
            assertThat(readAll(journal)).isEqualTo(events);
        }
    }

    @Test
    void corruptionInTheMiddleRefusesToOpen() throws IOException {
        var events = sampleEvents(50);
        Path file = dir.resolve("j");
        try (var journal = FileJournal.open(file, SyncPolicy.OS_CACHE)) {
            for (EngineEvent e : events) {
                journal.append(e);
            }
            journal.commit();
        }
        try (var raf = new RandomAccessFile(file.toFile(), "rw")) {
            long offset = FileJournal.FILE_HEADER_BYTES + FileJournal.RECORD_HEADER_BYTES + 3; // inside record #1
            raf.seek(offset);
            int b = raf.read();
            raf.seek(offset);
            raf.write(b ^ 0xFF);
        }
        assertThatThrownBy(() -> FileJournal.open(file, SyncPolicy.OS_CACHE))
                .isInstanceOf(CorruptJournalException.class)
                .hasMessageContaining("Checksum mismatch");
    }

    @Test
    void foreignFileIsRejected() throws IOException {
        Path file = dir.resolve("j");
        Files.writeString(file, "definitely not a journal");
        assertThatThrownBy(() -> FileJournal.open(file, SyncPolicy.OS_CACHE))
                .isInstanceOf(CorruptJournalException.class);
    }

    @Test
    void manifestPinsTheShardCount() throws IOException {
        Path journals = dir.resolve("journals");
        FileJournal.directory(journals, 4, SyncPolicy.OS_CACHE);
        FileJournal.directory(journals, 4, SyncPolicy.OS_CACHE); // same count: fine
        assertThatThrownBy(() -> FileJournal.directory(journals, 8, SyncPolicy.OS_CACHE))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("resharding");
    }

    private static void flipLastByte(Path file) throws IOException {
        try (var raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(raf.length() - 1);
            int b = raf.read();
            raf.seek(raf.length() - 1);
            raf.write(b ^ 0xFF);
        }
    }
}
