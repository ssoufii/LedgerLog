# Roadmap

Working backwards from the finished engine to a build order. Each milestone should end with its own passing tests before starting the next one. Don't skip ahead, later milestones assume earlier ones actually work, not just exist.

## Milestone 0: project scaffold
- [x] Repo structure, `pyproject.toml`, CI (`.github/workflows/ci.yml`), docs
- [x] ~~Empty `LedgerLog` class with `put`/`get`/`delete`/`close` stubs that raise `NotImplementedError`~~ superseded: the real `LedgerLog` landed in Milestone 3 and is re-exported from `ledgerlog/__init__.py`

## Milestone 1: WAL
- [x] Append-only writer with length-prefixed, checksummed records
- [x] Configurable fsync policy (`always` / `interval` / `never`)
- [x] Sequential reader/replay
- [x] Torn-write detection: truncate corrupt tail, replay everything before it
- [x] Tests: normal replay, empty file, truncated record at various byte offsets, corrupted checksum on an otherwise well-formed record

## Milestone 2: memtable
- [x] Skip list with insert/search/delete, sorted iteration
- [x] Thread-safety: single writer, concurrent readers
- [x] Tombstone support (delete marks, doesn't remove)
- [x] Tests: single-threaded correctness against a reference (e.g. compare against a sorted dict), concurrent stress test (many reader threads against one writer thread), tombstone visibility

## Milestone 3: engine v1 (WAL + memtable only, no disk flush)
- [x] Wire WAL + memtable together: every `put`/`delete` appends to WAL then updates memtable
- [x] Crash recovery on startup: replay WAL into a fresh memtable
- [x] Tests: kill the process (simulate by not calling a clean shutdown) mid-write-sequence, restart, verify recovered state matches what should have been durable

This milestone is a complete, durable, in-memory-only KV store. It's a good checkpoint: everything above this line is "does durability work", everything below is "does it work at a scale that doesn't fit in memory."

## Milestone 4: SSTable format
- [x] Writer: take a sorted iterator (from a frozen memtable), write data block + sparse index + bloom filter placeholder + footer
- [x] Reader: parse footer, binary search sparse index, scan for key
- [x] Format version byte in the footer
- [x] Tests: write then read back every key, read a key that doesn't exist, corrupt footer detection

## Milestone 5: bloom filter
- [x] Bit array + k hash functions, tunable false-positive rate
- [x] Build from a key set at SSTable write time, serialize/deserialize
- [x] Tests: no false negatives (property test over random key sets), measured false-positive rate roughly matches the configured target

## Milestone 6: memtable flush
- [x] Freeze memtable at size threshold, flush to a new SSTable, swap in a fresh memtable
- [x] Writes are never blocked on flush completing
- [x] Tests: flush under concurrent writes, verify no writes are lost or duplicated across the swap

## Milestone 7: multi-SSTable reads
- [x] `get` checks memtable, then SSTables newest-to-oldest, using bloom filters to skip
- [x] Tests: key shadowed by a newer write in a different table, key deleted by a tombstone in a newer table, key absent from all tables

## Milestone 8: size-tiered compaction
- [x] Group SSTables into size tiers, merge a tier once it has enough tables
- [x] Drop tombstones once safe (no older table can reference the key), drop TTL-expired records
- [x] Atomic swap: merged table's footer must be fully written before source tables are deleted
- [x] Per-record expiry in the WAL and SSTable record formats, each with its own format version bump
- [x] TTL as a first-class `put(key, value, ttl=...)` parameter, promoted here from the stretch goals below because the expiry drop above has nothing to drop without it
- [x] Tests: merge correctness (output matches newest-wins semantics), tombstone gets dropped only when safe, crash mid-compaction leaves source tables intact
- [x] Tests: an expiry survives a WAL replay and a flush, an expired record reads as absent before any compaction runs, an expired record is dropped from a merge

## Milestone 9: recovery across the full stack
- [x] Startup sequence: discover SSTables on disk, discard any without a valid footer, replay WAL on top
- [x] Tests: kill mid-flush, kill mid-compaction, restart and verify no data loss and no resurrection of deleted keys

## Milestone 10: benchmarking and polish
- [ ] `ledgerlog.bench`: sequential write throughput, random read latency, read amplification (how many SSTables touched per read) before/after compaction
- [ ] Tune default sparse index interval, bloom filter false-positive rate, and size-tier thresholds based on bench results
- [ ] README numbers reflect actual measured results, not estimates (measured after tuning, so they describe the final defaults)

## Stretch goals (not required for a complete v1)
- TTL/retention as a first-class `put(key, value, ttl=...)` parameter: promoted into Milestone 8 (stories M8.6 to M8.8), since "compaction drops old stuff" needs a per-record expiry before it can drop anything
- Leveled compaction as an alternative strategy, benchmarked against size-tiered
- Range scans (`scan(start_key, end_key)`) across memtable + SSTables
- Snapshot isolation for reads during compaction
