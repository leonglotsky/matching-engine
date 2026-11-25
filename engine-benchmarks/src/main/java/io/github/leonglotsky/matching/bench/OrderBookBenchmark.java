package io.github.leonglotsky.matching.bench;

import io.github.leonglotsky.matching.book.OrderBook;
import io.github.leonglotsky.matching.book.Outcome;
import io.github.leonglotsky.matching.domain.Command;
import io.github.leonglotsky.matching.domain.Command.CancelOrder;
import io.github.leonglotsky.matching.domain.Command.LimitOrder;
import io.github.leonglotsky.matching.domain.Side;
import io.github.leonglotsky.matching.sim.WorkloadGenerator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

/**
 * Single-threaded cost of the matching core ({@link OrderBook#process}) with no queueing and no I/O.
 * This is the ceiling for one shard.
 */
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
public class OrderBookBenchmark {

    /** A book with 20 levels a side, 5 orders per level, around a mid of 10,000. */
    @State(Scope.Thread)
    public static class DeepBook {
        OrderBook book;
        long ts;
        long n;

        @Setup(Level.Iteration)
        public void setUp() {
            book = new OrderBook("BENCH");
            for (int level = 1; level <= 20; level++) {
                for (int i = 0; i < 5; i++) {
                    book.process(LimitOrder.gtc("BENCH", "mm", "b" + level + "-" + i, Side.BUY, 10_000 - level, 100), ts++);
                    book.process(LimitOrder.gtc("BENCH", "mm", "a" + level + "-" + i, Side.SELL, 10_000 + level, 100), ts++);
                }
            }
        }
    }

    /** Add a passive order inside the existing book, then cancel it: the most common real-world message pair. */
    @Benchmark
    @OperationsPerInvocation(2)
    public void placeAndCancelPassive(DeepBook s, Blackhole bh) {
        String id = Long.toString(++s.n);
        bh.consume(s.book.process(LimitOrder.gtc("BENCH", "p", id, Side.BUY, 9_995, 10), ++s.ts));
        bh.consume(s.book.process(new CancelOrder("BENCH", "p", id), ++s.ts));
    }

    /**
     * Add 10 lots at the best ask, then take 10 lots with a market order: two commands, at least one
     * trade. Price-time priority means the taker consumes the oldest orders at that level, so total
     * depth stays constant across invocations while the queue at the touch keeps turning over.
     */
    @Benchmark
    @OperationsPerInvocation(2)
    public void restThenFullyFill(DeepBook s, Blackhole bh) {
        long n = ++s.n;
        bh.consume(s.book.process(LimitOrder.gtc("BENCH", "m", "m" + n, Side.SELL, 10_001, 10), ++s.ts));
        bh.consume(s.book.process(new Command.MarketOrder("BENCH", "t", "t" + n, Side.BUY, 10), ++s.ts));
    }

    /** Realistic mix from {@link WorkloadGenerator} (passive, aggressive, IOC/FOK, market, cancels, rejects). */
    @State(Scope.Thread)
    public static class Mixed {
        OrderBook book;
        WorkloadGenerator generator;
        long ts;

        @Setup(Level.Iteration)
        public void setUp() {
            book = new OrderBook("BENCH");
            generator = new WorkloadGenerator("BENCH", "acct", 42, 10_000);
        }
    }

    @Benchmark
    public Outcome mixedWorkload(Mixed s) {
        return s.book.process(s.generator.next(), ++s.ts);
    }

    /** Cost of generating the mixed workload alone, to subtract from {@link #mixedWorkload}. */
    @Benchmark
    public Command mixedWorkloadGeneratorOnly(Mixed s) {
        return s.generator.next();
    }
}
