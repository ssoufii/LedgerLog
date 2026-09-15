# Roadmap

Working backwards from the finished engine to a build order. Each milestone should end with its own passing tests before starting the next one. Don't skip ahead, later milestones assume earlier ones actually work, not just exist.

## Milestone 0: project scaffold
- [x] Repo structure, `pyproject.toml`, CI, docs
- [ ] Empty `LedgerLog` class with `put`/`get`/`delete`/`close` stubs that raise `NotImplementedError`

## Milestone 1: WAL
- [ ] Append-only writer with length-prefixed, checksummed records
- [ ] Configurable fsync policy (`always` / `interval` / `never`)
- [ ] Sequential reader/replay
- [ ] Torn-write detection: truncate corrupt tail, replay everything before it
- [ ] Tests: normal replay, empty file, truncated record at various byte offsets, corrupted checksum on an otherwise well-formed record

## Milestone 2: memtable
- [ ] Skip list with insert/search/delete, sorted iteration
- [ ] Thread-safety: single writer, concurrent readers
- [ ] Tombstone support (delete marks, doesn't remove)
- [ ] Tests: single-threaded correctness against a reference (e.g. compare against a sorted dict), concurrent stress test (many reader threads against one writer thread), tombstone visibility

## Milestone 3: engine v1 (WAL + memtable only, no disk flush)
- [ ] Wire WAL + memtable together: every `put`/`delete` appends to WAL then updates memtable
- [ ] Crash recovery on startup: replay WAL into a fresh memtable
- [ ] Tests: kill the process (simulate by not calling a clean shutdown) mid-write-sequence, restart, verify recovered state matches what should have been durable

This milestone is a complete, durable, in-memory-only KV store. It's a good checkpoint: everything above this line is "does durability work", everything below is "does it work at a scale that doesn't fit in memory."

## Milestone 4: SSTable format
- [ ] Writer: take a sorted iterator (from a frozen memtable), write data block + sparse index + bloom filter placeholder + footer
- [ ] Reader: parse footer, binary search sparse index, scan for key
- [ ] Format version byte in the footer
- [ ] Tests: write then read back every key, read a key that doesn't exist, corrupt footer detection

## Milestone 5: bloom filter
- [ ] Bit array + k hash functions, tunable false-positive rate
- [ ] Build from a key set at SSTable write time, serialize/deserialize
- [ ] Tests: no false negatives (property test over random key sets), measured false-positive rate roughly matches the configured target

## Milestone 6: memtable flush
- [ ] Freeze memtable at size threshold, flush to a new SSTable, swap in a fresh memtable
- [ ] Writes are never blocked on flush completing
- [ ] Tests: flush under concurrent writes, verify no writes are lost or duplicated across the swap

## Milestone 7: multi-SSTable reads
- [ ] `get` checks memtable, then SSTables newest-to-oldest, using bloom filters to skip
- [ ] Tests: key shadowed by a newer write in a different table, key deleted by a tombstone in a newer table, key absent from all tables

## Milestone 8: size-tiered compaction
- [ ] Group SSTables into size tiers, merge a tier once it has enough tables
- [ ] Drop tombstones once safe (no older table can reference the key), drop TTL-expired records
- [ ] Atomic swap: merged table's footer must be fully written before source tables are deleted
- [ ] Tests: merge correctness (output matches newest-wins semantics), tombstone gets dropped only when safe, crash mid-compaction leaves source tables intact

## Milestone 9: recovery across the full stack
- [ ] Startup sequence: discover SSTables on disk, discard any without a valid footer, replay WAL on top
- [ ] Tests: kill mid-flush, kill mid-compaction, restart and verify no data loss and no resurrection of deleted keys

## Milestone 10: benchmarking and polish
- [ ] `ledgerlog.bench`: sequential write throughput, random read latency, read amplification (how many SSTables touched per read) before/after compaction
- [ ] README numbers reflect actual measured results, not estimates
- [ ] Tune default sparse index interval, bloom filter false-positive rate, and size-tier thresholds based on bench results

## Stretch goals (not required for a complete v1)
- TTL/retention as a first-class `put(key, value, ttl=...)` parameter, not just "compaction drops old stuff"
- Leveled compaction as an alternative strategy, benchmarked against size-tiered
- Range scans (`scan(start_key, end_key)`) across memtable + SSTables
- Snapshot isolation for reads during compaction
