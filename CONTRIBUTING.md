# Contributing

This is primarily a solo learning/portfolio project, but it's built to be readable and extendable by others.

## Setup

```bash
git clone https://github.com/<your-username>/ledgerlog.git
cd ledgerlog
pip install -e ".[dev]"
```

## Before opening a PR

```bash
ruff check src tests
ruff format src tests
pytest --cov=ledgerlog --cov-report=term-missing
```

## Guidelines

- Follow the build order in `docs/ROADMAP.md`. Don't implement a later milestone's component using a shortcut that skips an earlier one (e.g. don't fake compaction results without real SSTable merging).
- Any durability or concurrency claim needs a test that actually exercises the failure mode, not just a docstring. See `CLAUDE.md` for specifics.
- Keep commit messages in imperative mood, one line: `Add sparse index binary search to SSTableReader`.
- Open an issue before a large structural change (e.g. switching compaction strategy) so the design tradeoff can be discussed first.
