# CLAUDE.md

Instructions for Claude Code when working in this repo. Read this before making changes.

## What this project is

LedgerLog is a log-structured key-value storage engine in Python (WAL, skip-list memtable, SSTables with sparse index + bloom filter, size-tiered compaction). It's a from-scratch systems project, not a wrapper around an existing embedded DB. The point is correctness of the storage internals, not API surface area. Don't reach for `shelve`, `sqlite3`, `lmdb`, or similar to shortcut any component described below.

Full design rationale lives in `docs/ARCHITECTURE.md`. Build order lives in `docs/ROADMAP.md`. User stories and acceptance criteria for the milestone you're on live in `docs/USER_STORIES.md`. Read all three before implementing a new component. If a milestone's stories aren't written yet in `docs/USER_STORIES.md`, stop and ask for them rather than inferring acceptance criteria from the roadmap alone.

There is no `src/` or `tests/` tree yet. It gets created milestone by milestone, starting from an empty repo, once each milestone's user stories exist.

## Ground rules

- **No external storage engines as dependencies.** Standard library plus small, well-scoped deps (e.g. `sortedcontainers` is not allowed as a memtable substitute — the skip list must be hand-built, since concurrent insert/lookup behavior is the point). Test tooling (`pytest`, `pytest-cov`, `hypothesis`) is fine.
- **Every on-disk format change needs a version byte.** SSTable and WAL file headers carry a format version. Don't silently change binary layout.
- **Durability claims must be testable.** If a component claims "survives a crash mid-write," there needs to be a test that kills the write partway (truncate the file, corrupt a byte) and asserts recovery behaves correctly, not just a docstring saying so.
- **Concurrency claims must be testable.** The memtable is used from multiple threads. Any change to it needs a test that exercises concurrent readers and a writer, not just single-threaded calls.
- **Keep components independently testable.** WAL, memtable, SSTable, bloom filter, and compaction should each have a test module that doesn't require booting the full engine.

## Repo layout (target, build incrementally)

```
src/ledgerlog/
  __init__.py       # public LedgerLog class
  wal.py             # write-ahead log: append, fsync policy, replay, torn-write detection
  memtable.py        # concurrent skip list
  sstable.py         # immutable sorted file, sparse index, read/write
  bloom.py           # bloom filter (bit array + k hash functions)
  compaction.py      # size-tiered compaction, tombstone/TTL cleanup
  engine.py          # wires the above into put/get/delete/flush
  bench.py           # throughput/latency benchmark harness
tests/
  test_wal.py
  test_memtable.py
  test_sstable.py
  test_bloom.py
  test_compaction.py
  test_engine.py     # integration tests across the full write/read/recovery path
docs/
  ARCHITECTURE.md
  ROADMAP.md
```

## Build order

Follow `docs/ROADMAP.md`. Don't build compaction before SSTables exist, don't build the bloom filter before there's a real false-positive rate to measure it against, and don't wire `engine.py` together until each component has its own passing tests. Each milestone in the roadmap should end with a green test suite before moving to the next.

## Commands

```bash
pytest                        # full test suite
pytest tests/test_wal.py -v   # single module, verbose
pytest --cov=ledgerlog --cov-report=term-missing
python -m ledgerlog.bench --records 100000
ruff check src tests          # lint
ruff format src tests         # format
```

Run `pytest` and `ruff check` before considering any change finished.

## Style

- Type hints on all public functions and methods.
- Docstrings explain *why* a design choice was made where it's non-obvious (e.g. why the sparse index samples every Nth key instead of indexing every key), not just what the function does.
- Prefer explicit byte-layout structs (e.g. `struct.pack`/`struct.unpack` with a documented format string) over pickling for anything written to disk. Disk formats should be readable by something other than this exact codebase in principle.
- No em dashes in comments, docstrings, or commit messages.
- Commit messages: imperative mood, one line summary, e.g. `Add checksum validation to WAL replay`.

## Known non-goals

- No SQL layer, no query planner, no secondary indexes. This is a KV engine.
- No networking/RPC layer. It's an embedded library, not a server, unless a later milestone in the roadmap explicitly adds one.
- No multi-process concurrency (single process, multiple threads only) unless the roadmap says otherwise.
