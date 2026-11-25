package io.github.leonglotsky.matching.journal;

import io.github.leonglotsky.matching.domain.EngineEvent;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.zip.CRC32C;

/**
 * Append-only journal file.
 *
 * <pre>
 * file   := header record*
 * header := magic:int32 version:int32
 * record := length:int32 crc32c:int32 payload:byte[length]     (payload = {@link EventCodec})
 * </pre>
 *
 * <h2>Crash safety</h2>
 * On open the file is scanned and every record's length and CRC32C are checked:
 * <ul>
 *   <li>A record that is cut short, or the <em>last</em> record failing its checksum, or a
 *       zero-filled tail, is an interrupted write. Nobody was acknowledged for it (acks happen
 *       after commit), so it is truncated and the engine starts normally.</li>
 *   <li>A bad record <em>followed by more data</em> means acknowledged history is damaged.
 *       That is a {@link CorruptJournalException}: we refuse to start instead of guessing.</li>
 * </ul>
 */
public final class FileJournal implements Journal {

    private static final System.Logger LOG = System.getLogger(FileJournal.class.getName());

    static final int MAGIC = 0x4D454A4E; // "MEJN"
    static final int VERSION = 1;
    static final int FILE_HEADER_BYTES = 8;
    static final int RECORD_HEADER_BYTES = 8;
    static final int MAX_RECORD_BYTES = 64 * 1024;

    private final Path path;
    private final SyncPolicy syncPolicy;
    private final FileChannel channel;
    private final long recoveredEnd;
    private final long truncatedBytes;

    private final PayloadBuffer payload = new PayloadBuffer();
    private final DataOutputStream payloadOut = new DataOutputStream(payload);
    private final CRC32C crc = new CRC32C();
    private ByteBuffer batch = ByteBuffer.allocate(64 * 1024);

    private FileJournal(Path path, SyncPolicy syncPolicy) throws IOException {
        this.path = path;
        this.syncPolicy = syncPolicy;
        this.channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            if (channel.size() < FILE_HEADER_BYTES) {
                // New file, or a crash while creating it: nothing was ever committed.
                channel.truncate(0);
                ByteBuffer header = ByteBuffer.allocate(FILE_HEADER_BYTES).putInt(MAGIC).putInt(VERSION).flip();
                while (header.hasRemaining()) {
                    channel.write(header, channel.size());
                }
                channel.force(true);
            }
            long size = channel.size();
            this.recoveredEnd = scan(null, size);
            this.truncatedBytes = size - recoveredEnd;
            if (truncatedBytes > 0) {
                LOG.log(System.Logger.Level.WARNING, "Truncating {0} bytes of torn tail from {1}", truncatedBytes, path);
                channel.truncate(recoveredEnd);
                channel.force(true);
            }
            channel.position(recoveredEnd);
        } catch (IOException | RuntimeException e) {
            channel.close();
            throw e;
        }
    }

    public static FileJournal open(Path path, SyncPolicy syncPolicy) throws IOException {
        return new FileJournal(path, syncPolicy);
    }

    /**
     * A factory for one journal file per shard in {@code directory}. The shard count is pinned in a
     * manifest: symbols are routed by hash, so reopening with a different count would put a symbol's
     * history in one file and its future in another. Resharding is a migration, not a restart.
     */
    public static JournalFactory directory(Path directory, int shards, SyncPolicy syncPolicy) throws IOException {
        Files.createDirectories(directory);
        Path manifest = directory.resolve("journal.properties");
        var props = new Properties();
        if (Files.exists(manifest)) {
            try (Reader r = Files.newBufferedReader(manifest)) {
                props.load(r);
            }
            int recorded = Integer.parseInt(props.getProperty("shards"));
            if (recorded != shards) {
                throw new IllegalStateException("Journal in " + directory + " was written with " + recorded
                        + " shards but engine is configured with " + shards + "; resharding requires a migration");
            }
        } else {
            props.setProperty("shards", Integer.toString(shards));
            props.setProperty("formatVersion", Integer.toString(VERSION));
            try (Writer w = Files.newBufferedWriter(manifest)) {
                props.store(w, "matching-engine journal manifest");
            }
        }
        return shard -> open(directory.resolve("shard-%03d.journal".formatted(shard)), syncPolicy);
    }

    @Override
    public void append(EngineEvent event) throws IOException {
        payload.reset();
        EventCodec.encode(event, payloadOut);
        int length = payload.size();
        if (length > MAX_RECORD_BYTES) {
            throw new IOException("Event too large for journal: " + length + " bytes");
        }
        crc.reset();
        crc.update(payload.buffer(), 0, length);
        ensureCapacity(RECORD_HEADER_BYTES + length);
        batch.putInt(length).putInt((int) crc.getValue()).put(payload.buffer(), 0, length);
    }

    @Override
    public void commit() throws IOException {
        if (batch.position() == 0) {
            return;
        }
        batch.flip();
        while (batch.hasRemaining()) {
            channel.write(batch);
        }
        batch.clear();
        if (syncPolicy == SyncPolicy.FSYNC) {
            channel.force(false);
        }
    }

    @Override
    public void replay(Consumer<EngineEvent> sink) throws IOException {
        scan(sink, recoveredEnd);
    }

    /** Bytes removed from the tail when this journal was opened. Exposed for monitoring and tests. */
    public long truncatedBytesOnOpen() {
        return truncatedBytes;
    }

    public Path path() {
        return path;
    }

    /**
     * Closes the file. Anything appended but not committed is discarded on purpose: the engine only
     * reaches close without a commit when a batch failed, and those callers were not acknowledged.
     */
    @Override
    public void close() throws IOException {
        channel.close();
    }

    /** Validates records from the start of the file and returns the offset just past the last good one. */
    private long scan(Consumer<EngineEvent> sink, long size) throws IOException {
        try (FileChannel reader = FileChannel.open(path, StandardOpenOption.READ);
             var in = new DataInputStream(new BufferedInputStream(Channels.newInputStream(reader), 1 << 16))) {
            int magic = in.readInt();
            int version = in.readInt();
            if (magic != MAGIC) {
                throw new CorruptJournalException(path + " is not a journal file");
            }
            if (version != VERSION) {
                throw new CorruptJournalException(path + " has unsupported format version " + version);
            }
            long pos = FILE_HEADER_BYTES;
            var check = new CRC32C();
            while (true) {
                long remaining = size - pos;
                if (remaining == 0) {
                    return pos;
                }
                if (remaining < RECORD_HEADER_BYTES) {
                    return pos; // torn header
                }
                int length = in.readInt();
                int expectedCrc = in.readInt();
                if (length == 0 && restIsZero(in, remaining - RECORD_HEADER_BYTES)) {
                    return pos; // pre-extended, zero-filled tail
                }
                if (length <= 0 || length > MAX_RECORD_BYTES) {
                    throw new CorruptJournalException("Invalid record length " + length + " at offset " + pos + " in " + path);
                }
                if (remaining - RECORD_HEADER_BYTES < length) {
                    return pos; // torn payload
                }
                byte[] record = in.readNBytes(length);
                check.reset();
                check.update(record);
                long end = pos + RECORD_HEADER_BYTES + length;
                if ((int) check.getValue() != expectedCrc) {
                    if (end == size) {
                        return pos; // last record half-written
                    }
                    throw new CorruptJournalException("Checksum mismatch at offset " + pos + " in " + path
                            + " with " + (size - end) + " bytes of later data");
                }
                if (sink != null) {
                    sink.accept(EventCodec.decode(new DataInputStream(new ByteArrayInputStream(record))));
                }
                pos = end;
            }
        }
    }

    private static boolean restIsZero(InputStream in, long count) throws IOException {
        for (long i = 0; i < count; i++) {
            if (in.read() != 0) {
                return false;
            }
        }
        return true;
    }

    private void ensureCapacity(int extra) {
        if (batch.remaining() < extra) {
            int capacity = Math.max(batch.capacity() * 2, batch.position() + extra);
            batch = ByteBuffer.allocate(capacity).put(batch.flip());
        }
    }

    /** ByteArrayOutputStream that exposes its buffer, to avoid a copy per event. */
    private static final class PayloadBuffer extends ByteArrayOutputStream {
        PayloadBuffer() {
            super(256);
        }

        byte[] buffer() {
            return buf;
        }
    }
}
