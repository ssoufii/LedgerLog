# Architecture

This is the design reference. If a component's behavior is ambiguous anywhere else (README, code comments, CLAUDE.md), this doc wins.

## 1. Write-ahead log (WAL)

**Purpose**: make writes durable before they're acknowledged, without needing the memtable or SSTables to be fsync'd on every write.

**Format**: append-only file. Each record is framed as:

```
[ 4 bytes: record length ][ 4 bytes: CRC32 checksum ][ 1 byte: op (PUT/DELETE) ][ key ][ value ]
```

- Length and checksum are written first so a reader can validate a record before trusting its contents.
- Checksum covers `op + key + value`. A mismatch means the write was torn (process died mid-append) or the file is corrupted.
- Writes are appended sequentially, fsync'd according to a configurable policy (`always`, `every N ms`, `never` — trade durability for throughput).

**Recovery**: on startup, read records sequentially from offset 0. For each record, validate length is in bounds and checksum matches. On the first invalid record, stop replaying and truncate the file at that offset. This is the "halt when torn" behavior: a partial record at the tail (the crash-in-progress write) is discarded, everything before it is replayed into the memtable.

**Why not just re-read random file positions**: WAL is only ever read sequentially, on recovery. No random access is needed, which keeps the format simple and avoids needing an index for a structure that's only ever consumed once, start to finish.

## 2. Memtable (concurrent skip list)

**Purpose**: an in-memory sorted structure that supports concurrent reads while a single writer is inserting.

**Why a skip list and not a balanced tree**: skip lists give O(log n) expected search/insert without rebalancing, which means an insert only needs to lock the nodes it's touching, not rebalance a subtree. That makes fine-grained concurrent access (readers proceeding while a write is in-flight) much simpler to reason about than a red-black or AVL tree.

**Concurrency model**: single writer at a time (guarded by a lock around the mutating insert path), multiple concurrent readers. Readers should not block behind a writer for keys they're not contending on; a real design does this with per-node locks or lock-free node linking. Document whichever approach is implemented in `memtable.py`'s module docstring, since this is the part most likely to have a subtle bug.

**Flush trigger**: when the memtable exceeds a configured size threshold, it is frozen (made read-only) and flushed to disk as a new SSTable. A new empty memtable takes over writes immediately, so writes are never blocked on a flush completing (the flush runs against the frozen table).

## 3. SSTables

**Purpose**: on-disk, immutable, sorted representation of a memtable snapshot.

**Layout**:

```
[ data block: sorted key-value records ]
[ sparse index: every Nth key -> byte offset into data block ]
[ bloom filter: bit array + hash function metadata ]
[ footer: offsets to index block and bloom filter, format version ]
```

**Sparse index**: indexing every Nth key (not every key) trades a small amount of read cost (binary search the sparse index, then scan forward from the nearest indexed offset) for a large reduction in memory footprint. N is configurable; a good default keeps the index small enough to comfortably fit in memory even for a large table.

**Bloom filter**: one per SSTable, built from every key in the table at write time. On read, a table is only opened and searched if the bloom filter says the key might be present. This turns a query that would otherwise touch every SSTable on disk into one that only touches tables that can actually contain the key, at the cost of a small, tunable false-positive rate (never false negatives).

**Immutability**: once written, an SSTable file is never modified. Deletes are tombstone records (a special value marking "this key was deleted at this point"), not in-place removal, so old files stay valid until compaction rewrites them.

## 4. Compaction (size-tiered)

**Purpose**: bound the number of SSTables a read has to check, reclaim space from tombstones and expired (TTL/retention-cleared) records, and control write amplification.

**Why size-tiered specifically**: tables are grouped by similar size, and a tier is compacted (merged into one larger table) once it accumulates enough tables at that size. This means a given record gets rewritten roughly once per tier it passes through, not on every compaction pass, which bounds write amplification to O(log(total data / memtable size)) rewrites over the record's lifetime.

**What gets dropped during a merge**:
- Tombstones, once no older SSTable can still reference the deleted key (otherwise a delete could "resurrect" if the tombstone is dropped too early).
- Records past their retention/TTL cutoff, if the engine supports expiry.

**Space amplification**: bounded by how aggressively tiers compact. More frequent compaction means less space wasted on stale/duplicate/expired data, at the cost of more I/O and CPU spent merging. This is the core LSM-tree trade-off and should be a configurable parameter, not a hardcoded constant.

## 5. Read path, precisely

```
get(key):
  1. check active memtable            -> if found, return (tombstone = not found)
  2. check frozen memtable (if any)   -> if found, return
  3. for each SSTable, newest first:
       a. check bloom filter          -> skip table if definitely absent
       b. binary search sparse index  -> find nearest offset <= key
       c. scan forward from offset    -> if found, return (tombstone = not found)
  4. not found
```

Newest-first ordering matters: a key can exist in multiple SSTables (old value, updated value, tombstone) and the newest write must win.

## 6. Failure modes this design should handle

- Process killed mid-WAL-append: recovery discards the torn tail record, keeps everything before it.
- Process killed mid-SSTable-flush: the engine should only consider an SSTable valid once its footer is written (footer write is the commit point); a partial SSTable file without a valid footer is discarded on startup and its data is recovered from the WAL instead, since the WAL record for those writes hasn't been truncated yet.
- Process killed mid-compaction: the engine should only swap in the merged SSTable and delete the source tables after the merged table's footer is fully written; a crash mid-merge leaves the original source tables intact and the partial merge output is discarded.
