# LedgerLog

A log-structured key-value storage engine built in Python, designed for sequential-write workloads like audit logs and event streams.

Writes never touch a random-access data structure on disk. They land in a write-ahead log, get indexed in an in-memory memtable, and eventually flush to disk as immutable, sorted SSTables. Reads are recovered from the resulting fragmentation with sparse indexes, bloom filters, and background compaction.

This project exists to actually build the pieces that make an LSM-tree work, not just describe them. Everything below is a real component with real trade-offs, not a toy wrapper around a dict.

## Why an LSM-tree

Audit and event logs are append-heavy and rarely updated in place. A B-tree pays a random I/O cost on every write to keep pages sorted. An LSM-tree instead makes writes cheap (sequential append) and pays the sorting cost later, in the background, on its own schedule. That trade only makes sense if you also solve the problems it creates: unbounded number of files to check on read, and unbounded disk growth. That's what the sparse index, bloom filters, and compaction below are for.

## Architecture

```
        writes
          │
          ▼
   ┌─────────────┐        ┌───────────────────┐
   │     WAL     │──────▶ │  Memtable (skip    │
   │ (append-only│        │  list, thread-safe)│
   │   segment)  │        └─────────┬──────────┘
   └─────────────┘                  │ flush at threshold
                                     ▼
                          ┌─────────────────────┐
                          │  SSTable (L0, L1...) │
                          │  sparse index +      │
                          │  bloom filter        │
                          └─────────┬────────────┘
                                     │ size-tiered compaction
                                     ▼
                          ┌─────────────────────┐
                          │  Merged SSTable      │
                          │  (tombstones/expired │
                          │   records dropped)   │
                          └─────────────────────┘
```

**Write path**: `put(key, value)` → append to WAL → insert into memtable → return. The WAL entry is fsync'd (configurable) before the write is acknowledged, so an acknowledged write survives a crash.

**Read path**: `get(key)` → check memtable → check SSTables newest-to-oldest, using each table's bloom filter to skip tables that provably don't have the key → binary search the sparse index of remaining candidates → seek and read.

**Crash recovery**: on startup, replay the WAL from the last known-good offset. Each record is checksummed; a bad checksum means a torn write (the process died mid-append) and recovery stops there rather than trusting a corrupted tail.

**Compaction**: size-tiered — tables of similar size get merged together, which bounds write amplification (each record is rewritten O(log N) times as it ages through size tiers, not every compaction cycle) and lets tombstones and TTL-expired records finally get dropped, bounding space amplification.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full design writeup, and [`docs/ROADMAP.md`](docs/ROADMAP.md) for the build order.

## Status

Pre-build. Writing user stories for each milestone in [`docs/USER_STORIES.md`](docs/USER_STORIES.md) before touching code. Milestones tracked in [`docs/ROADMAP.md`](docs/ROADMAP.md).

## Install

```bash
git clone https://github.com/<your-username>/ledgerlog.git
cd ledgerlog
pip install -e ".[dev]"
```

## Usage

```python
from ledgerlog import LedgerLog

db = LedgerLog("./data")
db.put("user:1042:login", b"2026-09-14T08:00:00Z")
db.get("user:1042:login")
db.delete("user:1042:login")
db.close()
```

## Development

```bash
pytest                      # run the test suite
pytest --cov=ledgerlog       # with coverage
python -m ledgerlog.bench    # write/read throughput benchmark
```

## License

MIT, see [LICENSE](LICENSE).
