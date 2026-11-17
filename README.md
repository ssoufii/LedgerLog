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

Milestones 0 through 11 of [`docs/ROADMAP.md`](docs/ROADMAP.md) are built and tested: WAL, concurrent skip-list memtable, SSTables with sparse index and bloom filter, memtable flush, multi-SSTable reads, size-tiered compaction with tombstone and TTL cleanup, full-stack crash recovery, the bench harness, and an in-browser demo that runs the real engine (see [Live demo](#live-demo)). Stretch goals (leveled compaction, range scans, snapshot reads) are not started. User stories live in [`docs/USER_STORIES.md`](docs/USER_STORIES.md).

## Performance

Every number in this section was produced by `python -m ledgerlog.bench` against the default tuning chosen in [`docs/TUNING.md`](docs/TUNING.md) (index interval 32, bloom target 0.01, size ratio 2.0, min tier tables 4). The full invocation for each run is shown with its figures, so any of them can be reproduced.

**Machine**: Apple M1 (8 cores), macOS 26.1, CPython 3.13.0, APFS on the internal SSD. The bench reports the filesystem as `unknown` on macOS because its detection reads Linux mount tables; it is a real disk, not a RAM-backed filesystem.

All three runs share these parameters and differ only in `--fsync-policy`:

```
python -m ledgerlog.bench --records 100000 --value-size 100 --reads 20000 \
  --fsync-policy {never|interval|always} --memtable-threshold-bytes 131072 \
  --index-interval 32 --bloom-fp-rate 0.01 --size-ratio 2.0 --min-tier-tables 4 \
  --compaction-schedule incremental --seed 0 --read-amplification
```

The 128 KiB memtable threshold is deliberately small: it produces 96 SSTables from 100k records, so reads are served from SSTables rather than from the memtable, and compaction has something to do.

### Sequential write throughput

| `--fsync-policy` | Throughput | Payload rate | Elapsed (incl. flush drain) |
|---|---|---|---|
| `never` | 64,466 records/s | 7.75 MiB/s | 1.551 s |
| `interval` (`--fsync-interval 0.1`) | 67,053 records/s | 8.06 MiB/s | 1.491 s |
| `always` | 23,419 records/s | 2.81 MiB/s | 4.270 s |

`never` and `interval` are within run-to-run noise of each other; fsyncing every 100 ms costs nothing measurable at this rate. On macOS, `os.fsync` hands data to the drive but does not force the drive's own write cache to stable media (that takes `F_FULLFSYNC`, which the WAL does not issue), so the `always` figure is an upper bound on fully durable throughput on this platform.

### Random read latency

20,000 reads of randomly chosen written keys, every value checked against what was written (0 wrong values in every run). Before compaction, 96 SSTables on disk.

| `--fsync-policy` | Reads/s | p50 | p99 |
|---|---|---|---|
| `never` | 9,755 | 0.0992 ms | 0.2185 ms |
| `interval` | 10,204 | 0.0973 ms | 0.1797 ms |
| `always` | 9,543 | 0.0958 ms | 0.2987 ms |

Full distribution for the `never` run: min 0.0033 ms, mean 0.1021 ms, p90 0.1536 ms, p99.9 0.7665 ms, max 2.7992 ms. The fsync policy only affects the write path, so the read figures differ by noise only.

### Read amplification (SSTables searched per `get`)

Counted inside the read path, not inferred from the table count. The incremental schedule compacts flush by flush, as a running engine would. These figures were identical in all three runs.

| | SSTables on disk | Mean searched per read | p99 | Max |
|---|---|---|---|---|
| Before compaction | 96 | 1.487 | 4 | 43 |
| After compaction | 3 | 1.015 | 2 | 2 |

Compaction ran 31 tier merges (1.804 s in the `never` run), took 96 tables down to 3, and rewrote 36.95 MiB, 2.66x the bytes flushed (write amplification). The bloom filters skipped 942,147 table probes before compaction at a measured 1.02% false-positive rate, which matches the 0.01 target.

## Live demo

`web/` is a one-page site that runs this repository's own `src/ledgerlog` in the browser, with Pyodide in a Web Worker. There is no server: you can put, get, delete, flush and compact keys, follow one record through the write path, and crash a copy of your database in six ways to see what recovery keeps. Every number on the page comes from the live engine or from the Performance tables above.

**Deploy on Render.** The site is a static build, and [`render.yaml`](render.yaml) describes it. In the Render dashboard, choose *New* then *Blueprint*, and connect this repository. Render reads `render.yaml` and builds `web/` with `npm ci && npm run build`. It publishes `web/dist`, with long-lived caching for hashed assets and `no-cache` for `index.html` and the engine source. A push that changes `web/`, `src/ledgerlog/` or this README redeploys it.

**Run it locally.** You need Node 22 or newer:

```bash
cd web
npm ci
npm run dev                  # development server
npm run build && npm run preview   # the production build, as Render serves it
npx playwright install chromium && npx playwright test   # the browser test suite
```

The design rules the page follows are in [`docs/FRONTEND_DESIGN.md`](docs/FRONTEND_DESIGN.md).

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
