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
  introspect.py      # read-only JSON snapshot of engine internals for tools and the web demo (M11)
web/                 # M11: Vite + React + TypeScript demo running the engine in the browser via Pyodide
  src/engine/        # Web Worker that hosts Pyodide, and the typed bridge to it
  tests/             # Playwright specs, including house-rules.spec.ts
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
  FRONTEND_DESIGN.md # authoritative art direction, tokens, house rules and motion rules for web/
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

# web demo (from web/, once M11.2 exists)
npm ci
npm run lint && npm run typecheck && npm run build
npx playwright install --with-deps chromium
npx playwright test           # runs against vite preview of the production build
```

Run `pytest` and `ruff check` before considering any change finished, plus the web commands above when `web/` changed.

## Style

- Type hints on all public functions and methods.
- Docstrings explain *why* a design choice was made where it's non-obvious (e.g. why the sparse index samples every Nth key instead of indexing every key), not just what the function does.
- Prefer explicit byte-layout structs (e.g. `struct.pack`/`struct.unpack` with a documented format string) over pickling for anything written to disk. Disk formats should be readable by something other than this exact codebase in principle.
- No em dashes in comments, docstrings, or commit messages.
- Commit messages: imperative mood, one line summary, e.g. `Add checksum validation to WAL replay`.

## Web demo (Milestone 11)

- `docs/FRONTEND_DESIGN.md` is authoritative for every visual, copy and motion decision under `web/`. Read it in full before any frontend story.
- The demo runs the repo's own `src/ledgerlog` source in Pyodide. Never reimplement engine behaviour in TypeScript; everything shown comes from the engine through the bridge.
- Frontend claims must be testable the same way engine claims are: each acceptance criterion gets a Playwright test that runs against the production build.
- No em dashes in UI copy either.

## Known non-goals

- No SQL layer, no query planner, no secondary indexes. This is a KV engine.
- No networking/RPC layer. It's an embedded library, not a server, unless a later milestone in the roadmap explicitly adds one. The M11 web demo is not an exception: it embeds the engine in the browser and has no backend.
- No multi-process concurrency (single process, multiple threads only) unless the roadmap says otherwise.
