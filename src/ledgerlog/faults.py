"""Deliberate damage, and what recovery makes of it.

Why this is a module and not a pile of helpers in the test suite: this engine's
durability claims are only worth what can be demonstrated, and the demonstration
has to happen in two places. ``tests/`` has to assert that a torn log replays to
the last whole record, and the web demo has to let a visitor do the same thing to
their own database and watch it happen. Two implementations of "cut the log in
half" would be two different claims, and the one on the page would be the one
nobody checks. There is one implementation here, the tests use it, and the demo
runs the same code in the browser.

What is in here is damage and nothing else: each function takes a directory that
holds a database and leaves it in the state a particular accident leaves it in.
None of it is reachable from the engine's read or write paths, nothing in
``engine.py`` imports it, and no caller gets it by accident.

:func:`run_scenario` is the whole demonstration: it copies a database, damages
the copy, opens an engine on it, and reports what survived. It copies first
because a visitor driving this from a web page is pointing it at the database
they have been typing into, and "show me what a crash would do" must not be a
way to lose it.

Every report comes from the engine's own accounts of what it found, its
:attr:`~ledgerlog.engine.LedgerLog.recovery` report for the log and its
:attr:`~ledgerlog.engine.LedgerLog.discovery` report for the tables, rather than
from this module comparing files. The engine is the thing being demonstrated, so
the engine is what speaks.
"""

from __future__ import annotations

import shutil
import tempfile
import time
from pathlib import Path
from typing import Any

from ledgerlog.engine import WAL_FILENAME, LedgerLog, SSTableHandle, sstable_filename
from ledgerlog.sstable import FOOTER_SIZE, iter_records, temp_table_path
from ledgerlog.wal import replay

SCENARIOS: tuple[str, ...] = (
    "torn-write",
    "flipped-byte",
    "crash-mid-flush",
    "crash-mid-compaction",
    "missing-footer",
    "expired-record",
)
"""The accidents :func:`run_scenario` can stage, in the order the write path meets them."""

TTL_SECONDS = 0.05
"""How far ahead the expiry scenario sets its deadline, and how long it then waits.

Short because the wait is real: an expiry is set from the engine's clock when
the record is written, so the only honest way to reach it is to let it arrive.
"""


class ScenarioNotPossible(RuntimeError):
    """Raised when a database has nothing for a scenario to damage.

    A distinct error rather than a silent no-op, because a tile that said
    "nothing dropped" after damaging nothing would be the most misleading
    possible result: it would read as the engine surviving something that never
    happened.
    """


def _engine(directory: Path) -> LedgerLog:
    """Open a database the way the demo opens one: no threads, no fsync."""
    return LedgerLog(directory, fsync_policy="never", flush_in_background=False)


def _table_keys(handle: SSTableHandle) -> list[bytes]:
    """Every key in one table's data block, read the way the table is written to be read."""
    with handle.borrow_reader() as reader:
        entries = reader.index.entries
    if not entries:
        return []
    with open(handle.path, "rb") as stream:
        return [
            record.key
            for record in iter_records(
                stream, start_offset=entries[0].offset, end_offset=handle.footer.data_block_end
            )
        ]


def live_keys(engine: LedgerLog) -> list[str]:
    """The keys ``engine`` would answer for, as text, in order.

    The candidates come from the structures the engine is holding, and the
    answer for each one comes from :meth:`~ledgerlog.engine.LedgerLog.get`. That
    split matters: a key is in the memtable or in a table whether it is live, a
    tombstone or expired, and only the read path knows which. Deciding it here
    would be a second read path to keep in step with the first.
    """
    candidates: set[bytes] = set()
    for memtable in (engine.active_memtable, *engine.frozen_memtables):
        candidates.update(memtable.keys())
    for handle in engine.sstables:
        candidates.update(_table_keys(handle))
    return sorted(
        key.decode("utf-8", errors="backslashreplace")
        for key in candidates
        if engine.get(key) is not None
    )


def _newest_table(directory: Path) -> Path:
    paths = sorted(directory.glob("sstable-*.sst"))
    if not paths:
        raise ScenarioNotPossible("there is no table on disk to damage: flush one first")
    return paths[-1]


def truncate_log_tail(directory: Path) -> str:
    """Cut the log's last record in half, as a process killed mid-append would.

    The cut lands inside the final record rather than between two of them, which
    is the case ARCHITECTURE.md section 1 calls a torn tail: the length and the
    checksum are there, and the bytes they describe are not all there.
    """
    path = Path(directory) / WAL_FILENAME
    result = replay(path, truncate=False)
    if not result.records:
        raise ScenarioNotPossible("the log holds no records to tear")
    last = result.records[-1]
    # Half of the final record's payload, so the length header survives to be
    # believed and the bytes behind it do not.
    cut = last.offset + (last.end_offset - last.offset) // 2
    with open(path, "r+b") as handle:
        handle.truncate(cut)
    return f"the log was cut at byte {cut}, halfway through its last record"


def flip_log_byte(directory: Path) -> str:
    """Invert one byte inside the last log record's payload.

    Damage after the write rather than a write that never finished: the record
    is whole and its checksum no longer matches, which is the other half of
    what the log's CRC32 is for.
    """
    path = Path(directory) / WAL_FILENAME
    result = replay(path, truncate=False)
    if not result.records:
        raise ScenarioNotPossible("the log holds no records to corrupt")
    last = result.records[-1]
    # Inside the payload, not the header, so the record still claims a length
    # the file can satisfy and only the checksum gives it away.
    target = last.offset + (last.end_offset - last.offset) // 2
    raw = bytearray(path.read_bytes())
    raw[target] ^= 0xFF
    path.write_bytes(bytes(raw))
    return f"byte {target} of the last log record was inverted"


def _truncate_at_footer(path: Path) -> None:
    size = path.stat().st_size
    if size <= FOOTER_SIZE:
        raise ScenarioNotPossible(f"{path.name} is shorter than a footer")
    with open(path, "r+b") as handle:
        handle.truncate(size - FOOTER_SIZE)


def crash_mid_flush(directory: Path) -> str:
    """Leave the newest table without the footer that would have committed it.

    What a process killed mid-flush leaves behind: a data block, an index and a
    filter that all landed, and no footer. The footer is the commit point
    (ARCHITECTURE.md section 6), so the right recovery is to pass the file over
    and replay its records from the log, which still holds them.
    """
    path = _newest_table(Path(directory))
    _truncate_at_footer(path)
    return f"{path.name} lost the footer that would have committed it"


def corrupt_footer(directory: Path) -> str:
    """Leave a committed table with a footer that no longer passes its own checksum.

    The other footer failure, and a different one from a flush that never
    finished: this file was committed, and then its last bytes were damaged. A
    footer the engine cannot verify is a footer the engine does not have, so the
    file is left out of the tables it serves and reported as rejected rather
    than read and believed.
    """
    path = _newest_table(Path(directory))
    raw = bytearray(path.read_bytes())
    if len(raw) <= FOOTER_SIZE:
        raise ScenarioNotPossible(f"{path.name} is shorter than a footer")
    # The first byte of the footer's fields, which is where the engine's own
    # test for this case damages one: inside the footer, ahead of its checksum
    # and its magic, so the file's length is untouched and only the
    # verification fails.
    target = len(raw) - FOOTER_SIZE
    raw[target] ^= 0xFF
    path.write_bytes(bytes(raw))
    return f"byte {target} inside {path.name}'s footer was inverted"


def crash_mid_compaction(directory: Path) -> str:
    """Leave a real merge's output half written, with both its sources still there.

    A merge writes to a temporary name and only swaps it in once its footer is
    down, so a crash in the middle leaves the sources intact and a partial file
    beside them. That is staged here with a real merge of the real tables,
    truncated before its footer, rather than with a plausible looking file: what
    recovery has to do with it depends on the file being the shape a merge
    leaves.
    """
    from ledgerlog.compaction import merge_sstables

    folder = Path(directory)
    sources = sorted(folder.glob("sstable-*.sst"), reverse=True)
    if len(sources) < 2:
        raise ScenarioNotPossible("a merge needs at least two tables: flush a few more")
    newest = max(int(path.stem.split("-")[1]) for path in sources)
    destination = temp_table_path(folder / sstable_filename(newest + 1))
    merge_sstables(sources, destination)
    _truncate_at_footer(destination)
    return (
        f"a merge of {len(sources)} tables was killed before its footer, leaving "
        f"{destination.name} behind and every source in place"
    )


def expire_a_record(directory: Path) -> str:
    """Write one record whose deadline has already passed.

    Not an accident, and on the board because it is the other way a key stops
    answering: a record can be whole, checksummed and committed, and still be
    gone, because its time ran out. Recovery keeps it (an expiry is a fact about
    the record, not damage to it) and the read path declines to return it.
    """
    with _engine(Path(directory)) as engine:
        keys = live_keys(engine)
        if not keys:
            raise ScenarioNotPossible("there is no record to expire: write one first")
        doomed = keys[0].encode("utf-8")
        engine.put(doomed, engine.get(doomed) or b"", ttl=TTL_SECONDS)
    # Waited out rather than written in the past, because a deadline is set from
    # the engine's own clock at the moment of the write and this has to be the
    # expiry the engine would really see. The wait is the deadline itself, which
    # is why the deadline is small.
    time.sleep(TTL_SECONDS * 3)
    return f"{keys[0]} was rewritten with a deadline {TTL_SECONDS} seconds away, and it passed"


DAMAGE = {
    "torn-write": truncate_log_tail,
    "flipped-byte": flip_log_byte,
    "crash-mid-flush": crash_mid_flush,
    "crash-mid-compaction": crash_mid_compaction,
    "missing-footer": corrupt_footer,
    "expired-record": expire_a_record,
}
"""What each scenario does. Keys match :data:`SCENARIOS`."""


def _needs_tables(scenario: str) -> int:
    """How many tables on disk the scenario needs before it can be staged."""
    if scenario == "crash-mid-compaction":
        return 2
    if scenario in ("crash-mid-flush", "missing-footer"):
        return 1
    return 0


def _report(engine: LedgerLog) -> dict[str, Any]:
    """The engine's own account of what it found when it opened."""
    recovery = engine.recovery
    discovery = engine.discovery
    return {
        "recovery": {
            "records_replayed": recovery.records_replayed,
            "end_offset": recovery.end_offset,
            "stopped_at": recovery.stopped_at,
            "reason": recovery.reason,
        },
        "tables_loaded": [table.path.name for table in discovery.tables],
        "tables_rejected": [
            {
                "name": rejection.path.name,
                "status": rejection.status.name,
                "reason": rejection.reason,
            }
            for rejection in discovery.rejected
        ],
    }


def run_scenario(directory: str | Path, scenario: str) -> dict[str, Any]:
    """Stage one accident on a copy of ``directory`` and report what survived.

    The copy is the point. A visitor pulls the plug on a database they have been
    typing into, and their own files are not touched: the damage, the reopen and
    the reading all happen in a temporary directory that is deleted before this
    returns.

    The sequence is the one a real crash has. The copy is opened and the keys it
    answers for are recorded, which is the "before". Some scenarios need a flush
    first, because a table cannot be damaged until one exists, and that flush
    happens before the "before" is taken so that the two sides describe the same
    database. Then the damage lands, the copy is opened again, and the engine
    says what it recovered and what it refused.

    Returns plain JSON types throughout, for the same reason
    :mod:`ledgerlog.introspect` does: this crosses into a browser.
    """
    if scenario not in DAMAGE:
        raise ValueError(f"unknown scenario {scenario!r}, expected one of {', '.join(SCENARIOS)}")

    source = Path(directory)
    workspace = Path(tempfile.mkdtemp(prefix="ledgerlog-crash-"))
    copy = workspace / "data"
    try:
        shutil.copytree(source, copy)

        needed = _needs_tables(scenario)
        with _engine(copy) as engine:
            while len(engine.sstables) < needed:
                before_flush = len(engine.sstables)
                engine.put(f"filler-{len(engine.sstables)}".encode(), b"filler")
                engine.flush()
                if len(engine.sstables) == before_flush:
                    raise ScenarioNotPossible(
                        "the database could not be flushed into the tables this scenario needs"
                    )
            staged = len(engine.sstables)
            keys_before = live_keys(engine)

        damage = DAMAGE[scenario](copy)

        with _engine(copy) as engine:
            keys_after = live_keys(engine)
            report = _report(engine)

        survivors = set(keys_after)
        return {
            "scenario": scenario,
            "damage": damage,
            "tables_staged": staged,
            "keys_before": keys_before,
            "keys_after": keys_after,
            "dropped": [key for key in keys_before if key not in survivors],
            **report,
        }
    finally:
        shutil.rmtree(workspace, ignore_errors=True)
