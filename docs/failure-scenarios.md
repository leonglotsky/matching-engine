# Failure scenarios

The guiding rule: **a client is only told "done" for something the journal will bring back after a
restart, and the engine never continues from a state it cannot reproduce.** Everything below follows
from that.

Notation: *acked* = the caller's future completed with `Processed`.

## Summary

| # | Failure | Detected by | What the engine does | What the client sees | Data loss? |
|---|---|---|---|---|---|
| 1 | JVM crash / `kill -9` | Restart | Replays journals; truncates any torn tail | In-flight requests time out; retry → `Duplicate` or executes once | None for acked commands |
| 2 | Power loss / kernel panic | Restart | Same as 1 | Same as 1 | `FSYNC`: none. `OS_CACHE`: last unsynced writes, possibly acked |
| 3 | Torn write at end of journal | CRC / length check on open | Truncates the partial record, logs a warning | Nothing; that batch was never acked | None |
| 4 | Corruption in the middle of a journal | CRC mismatch with data after it | Refuses to start (`CorruptJournalException`) | Engine down | Operator decision; nothing is silently dropped |
| 5 | Disk full / I/O error on commit | `IOException` from `commit()` | Halts that shard (fail-stop) | `Unavailable` for the batch and all later commands on that shard | None acked; in-flight outcome unknown → reconcile by retry |
| 6 | Bug: exception while matching | Any `Throwable` on the shard thread | Halts that shard | Same as 5 | Same as 5 |
| 7 | Journal contradicts the book (gap, unknown maker, overfill) | `OrderBook.apply` invariant checks | Refuses to start | Engine down | Operator decision |
| 8 | Overload (queue full) | `offer` fails | Rejects the command, nothing queued | `Overloaded` immediately | None; client backs off |
| 9 | Client timeout / duplicate send | Same `(account, clientOrderId)` | No-op | `Duplicate(originalOrderId)` | None; no double fill |
| 10 | Restart with different shard count | Journal manifest | Refuses to start | Engine down | None |
| 11 | Restart with a symbol removed | Replay finds events for unknown symbol | Refuses to start | Engine down | None |
| 12 | Graceful shutdown | `close()` | Drains queued commands, commits, stops | Queued commands complete normally; later ones `Unavailable` | None |

## Details

### 1-2. Process crash vs power loss

`commit()` writes the batch with one `FileChannel.write`. With `SyncPolicy.OS_CACHE` the bytes are in
the kernel page cache once `write` returns: they survive the JVM dying, but not the machine losing
power. With `SyncPolicy.FSYNC` the shard also calls `force()` before acking, so acked data survives
power loss. Group commit (ADR-0005) makes `FSYNC` affordable: one `fsync` covers the whole batch.

Choose per deployment: `FSYNC` for a single node of record; `OS_CACHE` is reasonable when durability
comes from replication to another machine (not implemented, see scalability.md).

### 3. Torn tail

A crash mid-`write` can leave a partial record. On open, `FileJournal` scans every record:

* fewer than 8 bytes of header left → torn
* declared length runs past end of file → torn
* last record's CRC32C does not match → torn (partially overwritten page)
* the rest of the file is zeros → torn (pre-extended file)

The tail is truncated and fsynced before the engine starts. This is safe because commits happen before
acks: nobody was told about those events. `FileJournalTest.tornTailIsTruncatedAtEveryPossibleCutPoint`
cuts a real journal at **every byte offset** and checks recovery always yields a clean prefix.

### 4. Mid-file corruption

A bad CRC followed by more valid-looking data is not an interrupted write: acknowledged history is
damaged (bit rot, bad sector, someone edited the file). Truncating would silently delete acked trades,
and skipping the record would produce a book that never existed. The engine refuses to start and the
operator restores from a replica or backup. This is deliberate: *wrong* is worse than *down* for a
system of record.

### 5-6. Fail-stop shard

If `commit()` throws, or anything on the shard thread throws, the in-memory books may already contain
the effects of the batch while the journal may contain none, some, or all of it. Continuing would mean
acking from a state that a restart cannot reproduce. So the shard:

1. completes every future in the batch with `Unavailable("... outcome of in-flight commands unknown ...")`,
2. refuses every later command on that shard with `Unavailable`,
3. keeps other shards running (blast radius = the symbols on that shard).

Uncommitted bytes in the journal buffer are discarded on close, never flushed. Recovery is a restart:
the books are rebuilt from whatever the journal really holds.

**Reconciling an unknown outcome**: because submission is idempotent (ADR-0006), the client simply
resends the same `clientOrderId` once the engine is back. If it had landed, the answer is
`Duplicate(orderId)`; if not, it executes now. Either way it executes exactly once.

Tested in `MatchingEngineTest.Failures`.

### 7. Invariant violation on replay

`OrderBook.apply` validates each event: per-symbol sequence must be `last + 1`, order and trade ids
contiguous, a trade's maker must be resting at that price on the opposite side with enough quantity, a
user cancel must match the resting quantity. A journal from another engine, a reordered file or a codec
bug fails here with a precise message instead of producing a plausible but wrong book.

### 8. Backpressure

Each shard inbox is a bounded `ArrayBlockingQueue` (default 65,536). `submit` uses `offer`, never
`put`: when full the caller immediately gets `Overloaded(symbol)`. The gateway can turn that into a
`429`/FIX business reject and the client backs off. Alternatives were worse: blocking pushes the stall
into every client thread, and an unbounded queue turns a traffic spike into an `OutOfMemoryError` and
unbounded latency. Tested in `MatchingEngineTest.Backpressure` with a deliberately stalled journal.

### 9. Duplicate submission

See ADR-0006. Tested with 200 virtual threads submitting the same order simultaneously: exactly one
`Processed`, 199 `Duplicate`.

### 10-11. Configuration drift

Routing is `hash(symbol) mod shards`. Restarting with a different shard count would put a symbol's
history in one file and its future in another. The journal directory's `journal.properties` records the
count and the engine refuses a mismatch. Likewise, a journal containing events for a symbol that is no
longer configured refuses to load rather than silently orphaning open orders.

## Not handled (and how it would be)

| Gap | Approach |
|---|---|
| Machine loss | Replicate the journal (or the input stream) to a standby that applies the same events; promote on failure. Determinism makes the standby's books identical. |
| Slow disk causing latency spikes | Separate journal thread with its own ring buffer, still acking after commit (Disruptor-style pipeline). |
| Poison command that always crashes matching | Halt is per shard; add a quarantine list so the gateway refuses that exact command on restart. |
| Clock jumps | Timestamps are informational only (priority is sequence order), so a jump cannot reorder the book. Production would use a monotonic, NTP/PTP-disciplined source. |
