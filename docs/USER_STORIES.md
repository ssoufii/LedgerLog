# User Stories

Written before implementation, one set per milestone in `docs/ROADMAP.md`. A milestone shouldn't start until its stories are written, and a story shouldn't be marked done until its acceptance criteria are actually demonstrated by a passing test.

Format for each story:

```
### <short title>

As a <role>, I want <capability>, so that <reason>.

**Acceptance criteria**
- [ ] ...
- [ ] ...

**Size**: S / M / L / Spike (not sized)
**Dependencies**: Depends on #<issue> (<story id>), or None

**Notes**
(edge cases, non-goals, links to the relevant section of ARCHITECTURE.md)
```

Story IDs (e.g. `M1.1`) appear in headings so dependencies between stories are unambiguous; they map 1:1 to GitHub issues.

Roles to write from: a caller of the `LedgerLog` API (the "user" of the engine), or the engine itself/an operator running it (for stories about recovery, compaction, and durability, where the beneficiary is the system's own correctness rather than an external caller).

---

## Milestone 0: project scaffold

*(stories not usually needed for pure scaffolding, skip to Milestone 1 unless there's a specific setup story worth tracking)*


## M1: WAL

### WAL record framing and append writer (M1.1)

As a caller of the LedgerLog API, I want every put/delete to be appended to an on-disk log as a length-prefixed, checksummed record, so that writes can be made durable independently of when the memtable or SSTables are synced.

**Acceptance criteria**
- [ ] Given a PUT(key, value), appending it writes a record framed as `[4B length][4B CRC32][1B op][key][value]`
- [ ] Given a DELETE(key), appending it writes a record with the DELETE op code and a documented empty-value convention
- [ ] CRC32 checksum is computed over op + key + value and stored in the record header
- [ ] Multiple appended records land sequentially in call order with no gaps or overlaps
- [ ] Appending is safe to call repeatedly without corrupting previously written records (append-only, no backward seeks)

**Size**: M
**Dependencies**: None

**Notes**
Architecture section 1. Fsync behavior is out of scope for this story (see M1.3).


### WAL file header with format version byte (M1.2)

As the engine/operator, I want the WAL file to start with a header carrying a format version byte, so that future changes to the record layout can be detected and rejected instead of silently misread.

**Acceptance criteria**
- [ ] A newly created WAL file writes a header containing a format version byte before any records
- [ ] Opening an existing WAL file reads and validates the version byte before parsing records
- [ ] Opening a WAL file with an unrecognized version raises a clear error rather than attempting to parse it as the current format

**Size**: S
**Dependencies**: Depends on #1 (M1.1)

**Notes**
Required by CLAUDE.md's ground rule that every on-disk format change needs a version byte. Not its own roadmap bullet, but implied by the ground rules and needed before the reader (M1.4) can trust record layout.


### Configurable fsync policy (M1.3)

As a caller of the LedgerLog API, I want to choose an fsync policy (always / interval / never) for the WAL, so that I can trade durability guarantees for write throughput based on my application's needs.

**Acceptance criteria**
- [ ] WAL writer accepts a policy of `always`, `interval`, or `never`
- [ ] Given policy `always`, each append call returns only after fsync completes
- [ ] Given policy `interval`, fsync is called on the configured cadence, not on every append (verified via call count)
- [ ] Given policy `never`, no fsync calls occur during normal operation

**Size**: S
**Dependencies**: Depends on #1 (M1.1)


### Sequential WAL reader (M1.4)

As the engine/operator, I want to read back a WAL file's records in the order they were written, so that recovery has a way to reconstruct writes without needing random access.

**Acceptance criteria**
- [ ] Given a WAL file with N well-formed records, reading yields exactly those N records in original order with correct op/key/value
- [ ] Given an empty WAL file (header only, no records), reading yields zero records without error
- [ ] Reader validates each record's length is within file bounds before trusting it

**Size**: M
**Dependencies**: Depends on #2 (M1.2)


### Torn-write and corruption detection with truncation (M1.5)

As the engine/operator, I want WAL replay to stop and truncate at the first invalid record, whether from a torn write or a corrupted checksum, so that a crash mid-append does not block recovery of everything written before it.

**Acceptance criteria**
- [ ] Given a WAL file where the last record is truncated mid-write, replay returns all prior valid records and reports the byte offset where it stopped
- [ ] Given a WAL file where a record's checksum does not match its op+key+value, replay treats it the same as a torn write: stop and report the offset, do not replay it or anything after it
- [ ] After detection, the WAL file is truncated at the offset of the first invalid record
- [ ] Test covers truncation at multiple distinct byte offsets within a record (mid-length-field, mid-checksum, mid-payload)

**Size**: M
**Dependencies**: Depends on #4 (M1.4)

**Notes**
Architecture sections 1 and 6 ("halt when torn"). Per CLAUDE.md this durability claim needs a real test that truncates/corrupts a file, not a docstring.



## M2: Memtable

### Skip list core: insert/search/delete/sorted iteration (M2.1)

As a caller of the LedgerLog API, I want an in-memory sorted structure supporting insert, search, and delete, so that recent writes can be looked up quickly and iterated in key order.

**Acceptance criteria**
- [ ] insert(key, value) then search(key) returns the inserted value
- [ ] delete(key) physically removes a node (generic data-structure operation; KV-level tombstone semantics are covered in M2.2)
- [ ] Sorted iteration over all inserted keys yields them in ascending key order
- [ ] Single-threaded correctness verified against a reference sorted structure (e.g. a sorted dict) across randomized insert/search/delete sequences

**Size**: L
**Dependencies**: None

**Notes**
Architecture section 2. Single-threaded only; concurrency is M2.3/M2.4. Must be hand-built per CLAUDE.md (no `sortedcontainers`).


### Tombstone support (M2.2)

As a caller of the LedgerLog API, I want deleting a key to record a tombstone rather than physically removing it, so that a delete correctly shadows older values in the memtable and later SSTables without needing immediate cleanup.

**Acceptance criteria**
- [ ] Given a key with an existing value, calling delete(key) inserts a tombstone marker for that key rather than removing the node
- [ ] Given a tombstoned key, a lookup returns not-found rather than an old value
- [ ] Given a tombstoned key with no prior value, delete still records a tombstone (idempotent, does not error)
- [ ] Sorted iteration includes tombstoned keys, since physical removal only happens later during compaction

**Size**: S
**Dependencies**: Depends on #6 (M2.1)


### Spike: choose concurrency strategy for the memtable (M2.3)

As the engine/operator, I want a decided and documented concurrency approach (e.g. per-node locking vs lock-free linking) for the skip list before implementing it, so that the trickiest and most bug-prone part of the memtable (per CLAUDE.md) is designed deliberately rather than discovered mid-implementation.

**Acceptance criteria**
- [ ] Deliverable: a short writeup (can live in memtable.py's module docstring per ARCHITECTURE.md) naming the chosen approach and why
- [ ] Deliverable: a rough sketch of the locking granularity for insert vs search

**Size**: Spike (not sized)
**Dependencies**: Depends on #6 (M2.1)

**Notes**
Architecture section 2 explicitly calls out that whichever approach is used must be documented since this is the part most likely to have a subtle bug. This spike produces the design that M2.4 implements.


### Concurrent memtable: single writer, concurrent readers (M2.4)

As a caller of the LedgerLog API, I want to read from the memtable concurrently while a single writer thread inserts, so that reads are not serialized behind writes they do not contend with.

**Acceptance criteria**
- [ ] A single writer thread performing inserts does not block reader threads completing lookups on keys the writer is not currently touching
- [ ] Concurrent stress test: many reader threads plus one writer thread running simultaneously for N operations completes with no crashes, deadlocks, or torn reads
- [ ] All values inserted by the writer before the stress test ends are eventually visible to readers (no lost writes)
- [ ] The chosen concurrency approach (M2.3) is documented in memtable.py's module docstring

**Size**: L
**Dependencies**: Depends on #6 (M2.1), #7 (M2.2), #8 (M2.3)

**Notes**
Per CLAUDE.md, concurrency claims must be tested with real concurrent readers/writer, not single-threaded calls.



## M3: Engine v1 (WAL + memtable only)

### Engine write path: WAL then memtable (M3.1)

As a caller of the LedgerLog API, I want put/delete calls to append to the WAL and then update the memtable, so that every acknowledged write is durable before it is visible for reads.

**Acceptance criteria**
- [ ] Given put(key, value), the engine appends a PUT record to the WAL before inserting into the memtable
- [ ] Given delete(key), the engine appends a DELETE record to the WAL before recording a tombstone in the memtable
- [ ] get(key) after put/delete reflects the memtable state (value, tombstone-as-not-found, or not-found)
- [ ] If the WAL append fails, the memtable is not updated

**Size**: M
**Dependencies**: Depends on #1 (M1.1), #3 (M1.3), #7 (M2.2)


### Crash recovery: replay WAL into a fresh memtable on startup (M3.2)

As the engine/operator, I want startup to replay the WAL into a fresh memtable, so that a process restart after a crash recovers every write that was durably logged.

**Acceptance criteria**
- [ ] Given a WAL with N valid records from a prior run, starting a new engine instance replays all N into a fresh memtable before accepting new operations
- [ ] Given a WAL whose tail record is torn (simulated by not performing a clean shutdown mid-write-sequence), restarting recovers every record before the torn tail and discards the torn one, matching WAL truncation behavior (M1.5)
- [ ] Test: perform a sequence of puts/deletes, kill the process without a clean shutdown at a specific point, restart, and assert recovered state exactly matches what was durably written up to that point
- [ ] get() calls issued immediately after startup, before any new writes, return values consistent with the replayed WAL

**Size**: M
**Dependencies**: Depends on #10 (M3.1), #4 (M1.4), #5 (M1.5)

**Notes**
This is the milestone's durability checkpoint per ROADMAP ("everything above this line is does durability work"). Test must actually simulate a kill, not just call replay on a well-formed file.



## M4: SSTable format

### SSTable writer: data block plus sparse index (M4.1)

As the engine/operator, I want a frozen memtable's contents written to disk as a sorted data block with a sparse index (every Nth key mapped to a byte offset), so that flushed data survives in a form that can be searched without loading the whole file into memory.

**Acceptance criteria**
- [ ] Given a sorted iterator of key-value (and tombstone) records, the writer produces a data block with records in the same sorted order
- [ ] The sparse index records the byte offset of every Nth key (N configurable), not every key
- [ ] Given a table with fewer than N keys, the sparse index still contains at least the first key's offset
- [ ] Round-trip check: every key written can be located by combining a binary search on the sparse index with a forward scan

**Size**: M
**Dependencies**: None

**Notes**
Architecture section 3. Data block and index are combined into one story because the index is built inline while streaming write offsets; splitting them would leave an untestable intermediate state. The bloom filter section is a placeholder here; real content lands in M5.3.


### SSTable footer with format version and section offsets (M4.2)

As the engine/operator, I want the footer to record the format version and the byte offsets of the index and bloom filter sections, so that a reader can locate every section without scanning the file and future format changes can be detected.

**Acceptance criteria**
- [ ] Footer is written last, after the data block, sparse index, and bloom filter placeholder
- [ ] Footer contains a format version byte and the byte offsets of the sparse index and bloom filter sections
- [ ] A file is only considered a valid, complete SSTable once its footer has been fully written

**Size**: S
**Dependencies**: Depends on #12 (M4.1)

**Notes**
This is the commit point per ARCHITECTURE.md section 6.


### SSTable reader: footer parse, index binary search, key scan (M4.3)

As a caller of the LedgerLog API, I want to look up a key in an SSTable file, so that reads can find values that have been flushed to disk.

**Acceptance criteria**
- [ ] Given an SSTable written by M4.1/M4.2, reading a key that exists returns its value (or tombstone marker)
- [ ] Given a key that does not exist in the table, the reader returns not-found without scanning the entire file
- [ ] Reader parses the footer first, then binary searches the sparse index to find the nearest offset <= the target key, then scans forward
- [ ] Reader rejects a footer whose format version it does not recognize with a clear error

**Size**: M
**Dependencies**: Depends on #13 (M4.2)


### Corrupt footer detection on read (M4.4)

As the engine/operator, I want opening an SSTable with a missing or corrupt footer to fail cleanly and mark the file invalid, so that a partially-written table from a crashed flush is never mistaken for valid data.

**Acceptance criteria**
- [ ] Given a file truncated before the footer was written, opening it raises a clear invalid/incomplete SSTable error rather than crashing on an out-of-bounds read
- [ ] Given a file with a corrupted footer (e.g. bad offsets pointing outside the file), opening it is detected and rejected rather than silently returning wrong data
- [ ] Detection distinguishes "never validly written" from "a real but different format version" (the latter is M4.3's job)

**Size**: S
**Dependencies**: Depends on #14 (M4.3)

**Notes**
Architecture section 6: a partial SSTable without a valid footer must be discarded on startup and its data recovered from the WAL instead. That reconciliation is engine-level (M9); this story keeps detection independently testable per CLAUDE.md.



## M5: Bloom filter

### Bloom filter core: bit array plus k hash functions (M5.1)

As the engine/operator, I want a bloom filter that can be sized for a target false-positive rate and queried for possibly-present vs definitely-absent, so that reads can skip SSTables that cannot contain a key.

**Acceptance criteria**
- [ ] Given a target false-positive rate and expected key count, the filter computes an appropriately sized bit array and number of hash functions k
- [ ] add(key) followed by might_contain(key) returns True for every added key, verified as a property test over randomized key sets (no false negatives)
- [ ] For keys never added, might_contain(key) returns False at a rate close to the configured false-positive target, measured over a large randomized sample
- [ ] Uses only standard library hashing, no external bloom filter dependency

**Size**: M
**Dependencies**: None


### Bloom filter serialize/deserialize (M5.2)

As the engine/operator, I want a bloom filter to be serialized to and deserialized from bytes with a format version, so that it can be stored as an SSTable section and read back correctly.

**Acceptance criteria**
- [ ] Serializing a filter and deserializing the result produces a filter with identical query behavior for every key tested before serialization
- [ ] Serialized format includes a version byte or field
- [ ] Deserializing a truncated or corrupted blob raises a clear error rather than returning a filter with wrong behavior

**Size**: S
**Dependencies**: Depends on #16 (M5.1)


### Integrate real bloom filter into SSTable writer (M5.3)

As the engine/operator, I want the SSTable writer to build a real bloom filter from every key in the table and store it, replacing the placeholder, so that readers can use it to skip tables that cannot contain a key.

**Acceptance criteria**
- [ ] Given a sorted iterator written to an SSTable, the resulting file's bloom filter section, once deserialized, returns possibly-present for every key actually in the table
- [ ] The SSTable footer's bloom filter offset points to a valid serialized filter
- [ ] Existing SSTable writer/reader round-trip tests from M4 still pass with the real filter in place of the placeholder

**Size**: S
**Dependencies**: Depends on #17 (M5.2), #13 (M4.2)



## M6: Memtable flush

### Freeze-and-swap at size threshold (M6.1)

As the engine/operator, I want the active memtable to be frozen and replaced with a fresh empty memtable once it exceeds a configured size threshold, so that a flush can proceed against a stable snapshot while new writes continue immediately.

**Acceptance criteria**
- [ ] Given a configured size threshold, once the active memtable's size meets or exceeds it, it is marked frozen and a new empty memtable becomes the active one
- [ ] A write that arrives immediately after the threshold is crossed goes to the new active memtable, not the frozen one
- [ ] The frozen memtable remains fully readable until it has been flushed and can be dropped
- [ ] The freeze-and-swap is atomic with respect to concurrent writers: no write is applied to the frozen memtable after it is marked frozen, and none is lost during the swap

**Size**: M
**Dependencies**: Depends on #9 (M2.4)


### Flush frozen memtable to SSTable without blocking writes (M6.2)

As a caller of the LedgerLog API, I want a memtable flush to happen in the background against the frozen snapshot, so that my writes to the new active memtable are never blocked waiting for the flush to finish.

**Acceptance criteria**
- [ ] Given a frozen memtable, flushing it produces a valid SSTable containing exactly its contents, including tombstones
- [ ] Writes issued to the active memtable while a flush is in progress succeed without waiting for the flush to complete
- [ ] Concurrency stress test: sustained writes on the active memtable running concurrently with a flush of the frozen memtable complete with no writes lost and no writes duplicated between the frozen table's SSTable and the active memtable
- [ ] Once the flush's SSTable footer is fully written, the frozen memtable can be dropped from memory

**Size**: L
**Dependencies**: Depends on #19 (M6.1), #13 (M4.2), #14 (M4.3)

**Notes**
Per CLAUDE.md, "writes never blocked on flush" and "no lost/duplicated writes across the swap" are concurrency claims that need the stress test above, not just sequential calls.



## M7: Multi-SSTable reads

### Track SSTables newest-first in engine metadata (M7.1)

As the engine/operator, I want the engine to maintain an ordered list of on-disk SSTables from newest to oldest, so that the read path can check them in the order needed for correct shadowing semantics.

**Acceptance criteria**
- [ ] After a flush produces a new SSTable, it is added to the front of the engine's SSTable list
- [ ] The list correctly reflects on-disk SSTables after multiple flushes, in the order they were created
- [ ] Each entry provides access to its bloom filter and reader without re-parsing the footer on every access

**Size**: S
**Dependencies**: Depends on #20 (M6.2)


### Multi-SSTable get(): newest-write-wins (M7.2)

As a caller of the LedgerLog API, I want get(key) to check the active memtable, then the frozen memtable if any, then SSTables from newest to oldest, skipping tables via bloom filter, so that I always see the most recent value or deletion for a key regardless of which layer holds it.

**Acceptance criteria**
- [ ] Given a key present with an older value in one SSTable and a newer value in a more recent SSTable, get() returns the newer value
- [ ] Given a key with an older value on disk and a tombstone in a newer SSTable, get() returns not-found
- [ ] Given a key present in the active or frozen memtable, get() returns that value without consulting any SSTable
- [ ] Given a key absent from the memtable and every SSTable, get() returns not-found without opening every table's data block (bloom filter negatives are skipped, verified by call-count assertions)

**Size**: M
**Dependencies**: Depends on #21 (M7.1)



## M8: Size-tiered compaction

### Size-tier grouping and compaction trigger (M8.1)

As the engine/operator, I want SSTables grouped into tiers by similar size, with a tier flagged for compaction once it accumulates enough tables, so that the number of tables a read must check stays bounded over time.

**Acceptance criteria**
- [ ] Given a set of SSTables of varying sizes, they are grouped into tiers such that tables within a tier are of comparable size, per a configurable ratio/threshold
- [ ] A tier is flagged as ready to compact once it holds at least the configured number of tables
- [ ] Adding a new SSTable (from a flush or a prior compaction) re-evaluates tier membership and trigger state

**Size**: M
**Dependencies**: Depends on #21 (M7.1)


### Merge a compaction tier into one SSTable (M8.2)

As the engine/operator, I want a triggered tier's SSTables merged into a single new SSTable with newest-write-wins semantics, so that a record is rewritten roughly once per tier instead of on every compaction pass.

**Acceptance criteria**
- [ ] Given a tier of SSTables with overlapping keys, the merged output contains, for each key, the value from the newest source table
- [ ] The merged output's keys are in sorted order and the output is itself a valid SSTable (data block, sparse index, bloom filter, footer)
- [ ] Merge correctness test: construct tables with known overlapping/shadowing keys and assert the merged table's contents exactly match manually computed newest-wins expectations

**Size**: L
**Dependencies**: Depends on #23 (M8.1), #13 (M4.2), #14 (M4.3), #18 (M5.3)


### Drop tombstones once safe during merge (M8.3)

As the engine/operator, I want a tombstone dropped from the merge output only once no older SSTable outside the merge can still reference that key, so that a deleted key can never be resurrected by an older, unmerged copy resurfacing later.

**Acceptance criteria**
- [ ] Given a tombstone whose key has no older value in any SSTable not part of the current merge, the tombstone is dropped from the merged output
- [ ] Given a tombstone whose key does have an older value in an SSTable outside the current merge, the tombstone is retained in the merged output
- [ ] Test simulates both cases explicitly and asserts drop vs retain behavior matches

**Size**: M
**Dependencies**: Depends on #24 (M8.2)


### Drop TTL-expired records during merge (M8.4)

As the engine/operator, I want records past their retention/TTL cutoff dropped during a merge, so that expired data does not consume space indefinitely.

**Acceptance criteria**
- [ ] Given a record with an expiry timestamp in the past relative to compaction time, the merge output does not include it
- [ ] Given a record with no expiry or a future expiry, the merge output retains it

**Size**: S
**Dependencies**: Depends on #24 (M8.2)

**Notes**
ROADMAP's stretch goals list a first-class put(key, value, ttl=...) API as not required for v1. This story's drop logic is a no-op until that API exists to set an expiry; consider deferring it to land alongside the TTL stretch goal, since it currently has nothing to test end-to-end.


### Atomic swap: footer-complete before source deletion (M8.5)

As the engine/operator, I want the merged table's footer fully written before any source table is deleted, so that a crash mid-compaction leaves the original data intact instead of losing it.

**Acceptance criteria**
- [ ] Given a successful merge, source tables are only deleted after the merged table's footer has been fully written and confirmed valid
- [ ] Simulated crash mid-merge, before the merged table's footer is written: on restart, all original source tables are still present and valid, and any partial merge output file is discarded
- [ ] After a crash-free compaction, the source tables are gone and only the merged table remains covering their key ranges

**Size**: M
**Dependencies**: Depends on #24 (M8.2), #25 (M8.3)

**Notes**
Durability claim per CLAUDE.md: test must actually kill the process/truncate the output mid-merge, not just assert ordering in code.



## M9: Recovery across the full stack

### Startup SSTable discovery with footer validation (M9.1)

As the engine/operator, I want startup to scan the data directory for SSTables and discard any without a valid footer, so that partially-written files from a crashed flush or compaction never get treated as valid data.

**Acceptance criteria**
- [ ] Given a data directory with a mix of valid SSTables and one with a missing/corrupt footer, discovery returns only the valid ones
- [ ] Discarded (invalid) files are excluded from the loaded table set (deletion/cleanup policy, if any, is a separate concern)
- [ ] Valid tables are loaded in an order that lets the engine reconstruct newest-first ordering (e.g. by embedded sequence number or file naming, not filesystem mtime)

**Size**: M
**Dependencies**: Depends on #15 (M4.4)


### Full startup sequence: load SSTables then replay WAL (M9.2)

As the engine/operator, I want startup to load valid SSTables and then replay the WAL on top of them, so that the engine's recovered state includes every durably-logged write, whether or not it was ever flushed.

**Acceptance criteria**
- [ ] Given valid SSTables plus a WAL containing writes made after the last flush, starting the engine produces a state where get() reflects both, with WAL writes taking precedence for shadowed keys
- [ ] Given no SSTables (fresh install) and a non-empty WAL, startup recovers purely from WAL replay into an empty memtable
- [ ] Given SSTables and an empty/absent WAL, startup serves reads purely from the SSTables

**Size**: M
**Dependencies**: Depends on #28 (M9.1), #11 (M3.2), #22 (M7.2)


### Crash-mid-flush recovery correctness (M9.3)

As the engine/operator, I want a crash during a memtable flush to leave the engine recoverable with no data loss and no duplication, so that flush failures never corrupt the durability guarantee the WAL provides.

**Acceptance criteria**
- [ ] Simulate a crash after the WAL records for a batch of writes exist but before the resulting SSTable's footer is written; restart and assert every one of those writes is present exactly once, recovered via WAL replay
- [ ] Simulate a crash after the SSTable footer is fully written (flush succeeded) but before WAL cleanup; restart and assert writes are not duplicated

**Size**: M
**Dependencies**: Depends on #29 (M9.2), #20 (M6.2)

**Notes**
Exercises the M6/M9.2 interaction end to end; kept as its own story since CLAUDE.md requires a real test for this specific crash scenario.


### Crash-mid-compaction recovery correctness (M9.4)

As the engine/operator, I want a crash during compaction to leave the engine recoverable with no data loss and no resurrection of deleted keys, so that compaction failures never violate the same durability and correctness guarantees as normal operation.

**Acceptance criteria**
- [ ] Simulate a crash mid-merge before the merged table's footer is written; restart and assert the original source tables are loaded and all their data is still reachable via get()
- [ ] Simulate a crash mid-merge where a tombstone had been dropped in the discarded partial output; restart and assert the tombstoned key still correctly returns not-found

**Size**: M
**Dependencies**: Depends on #29 (M9.2), #27 (M8.5)



## M10: Benchmarking and polish

### Bench harness: write throughput and read latency (M10.1)

As the engine/operator, I want a ledgerlog.bench command that measures sequential write throughput and random read latency over a configurable record count, so that performance claims are backed by actual measurements.

**Acceptance criteria**
- [ ] Running `python -m ledgerlog.bench --records N` performs N sequential writes and reports throughput
- [ ] The same run (or a follow-up phase) performs randomized reads over the written keys and reports latency distribution (e.g. p50/p99)
- [ ] Record count and other basic parameters (value size, fsync policy) are configurable via CLI arguments

**Size**: M
**Dependencies**: Depends on #29 (M9.2)


### Bench harness: read amplification before/after compaction (M10.2)

As the engine/operator, I want the bench harness to report how many SSTables a read touches on average, before and after a compaction pass, so that compaction's effect on read amplification is measurable rather than assumed.

**Acceptance criteria**
- [ ] Bench harness instruments the read path to count SSTables opened/consulted per get() call
- [ ] Running the bench with compaction disabled/deferred reports a read-amplification figure over a dataset with many small SSTables
- [ ] Running the same dataset through compaction and re-measuring reports a lower (or equal) read-amplification figure, and the harness prints both for comparison

**Size**: M
**Dependencies**: Depends on #32 (M10.1), #24 (M8.2)


### README numbers reflect measured bench results (M10.3)

As the engine/operator, I want the README's performance section to state numbers actually produced by the bench harness, so that documented performance claims are not estimates.

**Acceptance criteria**
- [ ] README's performance/benchmarks section shows throughput, latency, and read-amplification figures
- [ ] Each figure is traceable to a specific bench invocation (parameters shown alongside the numbers)
- [ ] No numeric performance claim in the README lacks a corresponding bench run backing it

**Size**: S
**Dependencies**: Depends on #32 (M10.1), #33 (M10.2)


### Spike: tune default sparse index interval, bloom FP rate, and size-tier thresholds (M10.4)

As the engine/operator, I want the default sparse index interval, bloom filter false-positive target, and size-tier thresholds chosen based on bench results rather than arbitrary starting values, so that out-of-the-box performance reflects a deliberate trade-off instead of a guess.

**Acceptance criteria**
- [ ] Deliverable: a short writeup of the trade-offs observed across a matrix of candidate values for each parameter
- [ ] Deliverable: the code change updating the defaults to match the chosen values

**Size**: Spike (not sized)
**Dependencies**: Depends on #32 (M10.1), #33 (M10.2), #16 (M5.1), #12 (M4.1), #23 (M8.1)

**Notes**
Genuinely exploratory: the right defaults cannot be known until measured across configs, so this is a spike rather than an estimated story.

