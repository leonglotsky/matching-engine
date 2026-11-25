package io.github.leonglotsky.matching.bench;

import io.github.leonglotsky.matching.engine.CommandResult;
import io.github.leonglotsky.matching.engine.MatchingEngine;
import io.github.leonglotsky.matching.journal.FileJournal;
import io.github.leonglotsky.matching.journal.JournalFactory;
import io.github.leonglotsky.matching.journal.SyncPolicy;
import io.github.leonglotsky.matching.sim.WorkloadGenerator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * End-to-end round trip through {@link MatchingEngine#submit}: queue hand-off, matching, journal,
 * future completion. Each JMH thread is a closed-loop client that waits for its acknowledgement.
 */
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(value = 1, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@Threads(8)
public class EngineBenchmark {

    private static final List<String> SYMBOLS = IntStream.range(0, 16).mapToObj(i -> "SYM" + i).toList();

    @State(Scope.Benchmark)
    public static class Engine {
        @Param({"NONE", "FILE_OS_CACHE", "FILE_FSYNC"})
        public String journal;

        MatchingEngine engine;
        Path dir;
        final AtomicInteger clients = new AtomicInteger();

        @Setup(Level.Trial)
        public void start() throws IOException {
            JournalFactory factory = JournalFactory.noop();
            if (!journal.equals("NONE")) {
                dir = Files.createTempDirectory("engine-bench");
                factory = FileJournal.directory(dir, 4, journal.equals("FILE_FSYNC") ? SyncPolicy.FSYNC : SyncPolicy.OS_CACHE);
            }
            engine = MatchingEngine.builder().symbols(SYMBOLS).shards(4).journals(factory).start();
        }

        @TearDown(Level.Trial)
        public void stop() throws IOException {
            engine.close();
            if (dir != null) {
                try (var paths = Files.walk(dir)) {
                    for (Path p : paths.sorted(Comparator.reverseOrder()).toList()) {
                        Files.delete(p);
                    }
                }
            }
        }
    }

    @State(Scope.Thread)
    public static class Client {
        WorkloadGenerator[] generators;
        int next;

        @Setup(Level.Trial)
        public void setUp(Engine engine) {
            int id = engine.clients.incrementAndGet();
            generators = SYMBOLS.stream()
                    .map(s -> new WorkloadGenerator(s, "client-" + id, id * 1_000L + s.hashCode(), 10_000))
                    .toArray(WorkloadGenerator[]::new);
        }
    }

    @Benchmark
    public CommandResult submitAndAwait(Engine engine, Client client) {
        var generator = client.generators[client.next++ & (SYMBOLS.size() - 1)];
        return engine.engine.submit(generator.next()).join();
    }
}
