"""Tests for the engine's write path, story M3.1.

The story's one real claim is an ordering claim: a put or a delete reaches the
write-ahead log before it reaches the memtable, so a value a reader can see is
always a value the log already holds. Most of what follows is built around the
ways that claim could pass inspection while being false.

Checking that the log holds the record and the memtable holds the value, after
the call returns, would pass just as happily with the two steps the other way
around, so the ordering tests here observe the engine from *inside* the log
append: they wrap the WAL writer's append method and ask the engine, through its
own public ``get``, what a reader would see at that moment. The answer has to be
the state from before the write.

The other half is the failure case, which is the same claim seen from behind: if
the append raises, the memtable must be untouched. Those tests assert on what did
not happen (the value is unchanged, the log file did not grow), because an engine
that applied the write anyway would still return the exception to the caller and
look correct from the outside.

The concurrency tests at the bottom back the guarantee ``engine.py`` states, that
writes serialize and reads do not wait, using real threads. The interesting one
is not that nothing crashes; it is that the log's record order and the memtable's
final values agree afterwards. Writers contending on the same keys are the case
where an engine that locked only the log, and let the two memtable updates land
in either order, would leave a key whose in-memory value disagrees with the last
record written for it, so that a restart would silently change it.

A few tests reach for ``engine._memtable``. That is deliberate and it is marked
where it happens: a tombstone and a removed key are indistinguishable through
``get``, so checking that a delete tombstones rather than removes cannot be done
from outside until there are older layers for the tombstone to shadow.

The recovery tests for story M3.2 come in two kinds, because the story's claim
has two halves that different tests can reach. The kill tests at the bottom run
a real child process, let it acknowledge writes, and then SIGKILL it: that is
the only way to get an unclean shutdown that no cleanup path could have tidied
up, and it is what makes "survives a crash" a measurement rather than a
docstring. What a kill cannot do is land on a chosen byte, so the torn-tail
tests truncate and corrupt the log at specific offsets (mid length field, mid
checksum, mid payload, mid record in the middle of the file) to pin down exactly
which records come back. Neither kind stands in for the other.
"""

from __future__ import annotations

import os
import select
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

import ledgerlog
from ledgerlog import LedgerLog
from ledgerlog import engine as engine_module
from ledgerlog import wal as wal_module
from ledgerlog.engine import WAL_FILENAME
from ledgerlog.wal import (
    FILE_HEADER_SIZE,
    RECORD_HEADER_SIZE,
    WAL_FORMAT_VERSION,
    FsyncPolicy,
    WalFormatError,
    WalOp,
    WalReader,
    WalRecord,
    WalUnsupportedVersionError,
    encode_file_header,
    read_file_header_from_path,
)


def wal_records(engine: LedgerLog) -> list[WalRecord]:
    """Read back every record in an engine's log, in write order.

    Through a separate read-only handle, which is what makes this usable while
    the engine is still open: the writer flushes each record to the operating
    system as it appends it, so a reader opening the file afterwards sees every
    record that has been acknowledged.
    """
    with WalReader(engine.wal_path) as reader:
        return list(reader)


def logged(engine: LedgerLog) -> list[tuple[WalOp, bytes, bytes]]:
    """Return the engine's log as plain op/key/value triples, in write order."""
    return [(record.op, record.key, record.value) for record in wal_records(engine)]


class _AppendSpy:
    """Stand-in for a WAL append method that records what a reader could see.

    The point of the indirection is to observe the engine mid-write. ``observe``
    is called with the arguments the engine passed to the append, at the moment
    the append would run, and before the real one is invoked.
    """

    def __init__(self, real: Callable[..., int], observe: Callable[..., None]) -> None:
        self._real = real
        self._observe = observe
        self.calls = 0

    def __call__(self, *args: bytes) -> int:
        self.calls += 1
        self._observe(*args)
        return self._real(*args)


class _FailingAppend:
    """Stand-in for a WAL append method that raises instead of writing.

    Models the disk failing under an append: the engine's next step, updating
    the memtable, must not run. ``OSError`` specifically, because that is what a
    real write error surfaces as, and because it is not one of the exceptions the
    engine or the WAL raise for themselves, so a test asserting on it cannot pass
    by accident.
    """

    def __init__(self, message: str = "simulated disk failure") -> None:
        self.message = message
        self.calls = 0

    def __call__(self, *args: bytes) -> int:
        self.calls += 1
        raise OSError(self.message)


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[LedgerLog]:
    """An open engine on a fresh directory, closed when the test finishes."""
    with LedgerLog(tmp_path / "data") as open_engine:
        yield open_engine


# ---------------------------------------------------------------------------
# Story M3.1: the log is written before the memtable
# ---------------------------------------------------------------------------


def test_put_appends_a_put_record_carrying_the_key_and_value(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")

    assert logged(engine) == [(WalOp.PUT, b"alpha", b"one")]


def test_delete_appends_a_delete_record_with_an_empty_value(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")
    engine.delete(b"alpha")

    assert logged(engine) == [
        (WalOp.PUT, b"alpha", b"one"),
        (WalOp.DELETE, b"alpha", b""),
    ]


def test_put_reaches_the_log_before_a_reader_can_see_the_value(engine: LedgerLog) -> None:
    """Inside the append, the engine must still be answering with the old state."""
    seen: list[bytes | None] = []
    spy = _AppendSpy(engine._wal.append_put, lambda key, _value: seen.append(engine.get(key)))
    engine._wal.append_put = spy

    engine.put(b"alpha", b"one")

    assert spy.calls == 1
    assert seen == [None], "the memtable was updated before, or during, the log append"
    assert engine.get(b"alpha") == b"one"


def test_overwriting_reaches_the_log_before_the_new_value_is_visible(engine: LedgerLog) -> None:
    """The same ordering on an update, where the old state is a value and not absence."""
    engine.put(b"alpha", b"one")

    seen: list[bytes | None] = []
    spy = _AppendSpy(engine._wal.append_put, lambda key, _value: seen.append(engine.get(key)))
    engine._wal.append_put = spy

    engine.put(b"alpha", b"two")

    assert seen == [b"one"], "the memtable held the new value before the log did"
    assert engine.get(b"alpha") == b"two"


def test_delete_reaches_the_log_before_the_value_stops_being_visible(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")

    seen: list[bytes | None] = []
    spy = _AppendSpy(engine._wal.append_delete, lambda key: seen.append(engine.get(key)))
    engine._wal.append_delete = spy

    engine.delete(b"alpha")

    assert seen == [b"one"], "the tombstone was recorded before the log append"
    assert engine.get(b"alpha") is None


def test_records_land_in_the_log_in_call_order(engine: LedgerLog) -> None:
    engine.put(b"b", b"1")
    engine.put(b"a", b"2")
    engine.delete(b"b")
    engine.put(b"c", b"3")
    engine.put(b"a", b"4")
    engine.delete(b"absent")

    assert logged(engine) == [
        (WalOp.PUT, b"b", b"1"),
        (WalOp.PUT, b"a", b"2"),
        (WalOp.DELETE, b"b", b""),
        (WalOp.PUT, b"c", b"3"),
        (WalOp.PUT, b"a", b"4"),
        (WalOp.DELETE, b"absent", b""),
    ]


def test_every_write_is_logged_including_ones_the_memtable_collapses(engine: LedgerLog) -> None:
    """Three puts of one key are one memtable record but must be three log records.

    The memtable keeps one node per key, so an engine that logged only what
    changed in memory would lose the intermediate writes. The log is a history,
    not a snapshot.
    """
    engine.put(b"alpha", b"one")
    engine.put(b"alpha", b"two")
    engine.put(b"alpha", b"three")

    assert logged(engine) == [
        (WalOp.PUT, b"alpha", b"one"),
        (WalOp.PUT, b"alpha", b"two"),
        (WalOp.PUT, b"alpha", b"three"),
    ]
    assert engine.get(b"alpha") == b"three"


def test_delete_records_a_tombstone_in_the_memtable_rather_than_removing_the_key(
    engine: LedgerLog,
) -> None:
    """Reaches into the memtable on purpose, because this is invisible from outside.

    ``get`` returns ``None`` for a deleted key and for a key that was never
    written, so an engine that removed the node would satisfy every public
    assertion here. The difference only becomes observable once there are older
    layers for a tombstone to shadow (story M7.2), which is too late to find out
    that the delete has been dropping records all along.
    """
    engine.put(b"alpha", b"one")
    engine.delete(b"alpha")

    entry = engine._memtable.lookup(b"alpha")
    assert entry is not None, "the delete removed the record instead of tombstoning it"
    assert entry.is_tombstone
    assert list(engine._memtable.keys()) == [b"alpha"]


def test_deleting_a_key_the_engine_never_held_still_logs_and_tombstones(
    engine: LedgerLog,
) -> None:
    engine.delete(b"never-written")

    assert logged(engine) == [(WalOp.DELETE, b"never-written", b"")]
    entry = engine._memtable.lookup(b"never-written")
    assert entry is not None and entry.is_tombstone
    assert engine.get(b"never-written") is None


# ---------------------------------------------------------------------------
# Story M3.1: get reflects the memtable's state
# ---------------------------------------------------------------------------


def test_get_returns_the_value_that_was_put(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")

    assert engine.get(b"alpha") == b"one"


def test_get_of_a_key_that_was_never_written_is_none(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")

    assert engine.get(b"beta") is None


def test_get_after_delete_is_none(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")
    engine.delete(b"alpha")

    assert engine.get(b"alpha") is None


def test_get_returns_the_newest_value_after_an_overwrite(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")
    engine.put(b"alpha", b"two")

    assert engine.get(b"alpha") == b"two"


def test_a_put_over_a_tombstone_brings_the_key_back(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")
    engine.delete(b"alpha")
    engine.put(b"alpha", b"two")

    assert engine.get(b"alpha") == b"two"
    assert logged(engine) == [
        (WalOp.PUT, b"alpha", b"one"),
        (WalOp.DELETE, b"alpha", b""),
        (WalOp.PUT, b"alpha", b"two"),
    ]


def test_an_empty_value_is_stored_and_is_not_the_same_as_a_delete(engine: LedgerLog) -> None:
    """``b""`` and ``None`` must stay distinct at every layer, log included."""
    engine.put(b"empty", b"")
    engine.put(b"deleted", b"something")
    engine.delete(b"deleted")

    assert engine.get(b"empty") == b""
    assert engine.get(b"deleted") is None
    assert logged(engine) == [
        (WalOp.PUT, b"empty", b""),
        (WalOp.PUT, b"deleted", b"something"),
        (WalOp.DELETE, b"deleted", b""),
    ]


def test_the_empty_key_is_a_usable_key(engine: LedgerLog) -> None:
    engine.put(b"", b"value")

    assert engine.get(b"") == b"value"
    assert logged(engine) == [(WalOp.PUT, b"", b"value")]


def test_keys_are_compared_by_bytes_and_do_not_collide(engine: LedgerLog) -> None:
    engine.put(b"a", b"1")
    engine.put(b"a\x00", b"2")
    engine.put(b"A", b"3")

    assert engine.get(b"a") == b"1"
    assert engine.get(b"a\x00") == b"2"
    assert engine.get(b"A") == b"3"


def test_many_writes_are_all_readable_and_all_logged(engine: LedgerLog) -> None:
    for index in range(500):
        engine.put(f"key-{index:04d}".encode(), f"value-{index}".encode())
    for index in range(0, 500, 5):
        engine.delete(f"key-{index:04d}".encode())

    for index in range(500):
        key = f"key-{index:04d}".encode()
        expected = None if index % 5 == 0 else f"value-{index}".encode()
        assert engine.get(key) == expected

    assert len(wal_records(engine)) == 500 + 100


# ---------------------------------------------------------------------------
# Story M3.1: a failed log append leaves the memtable untouched
# ---------------------------------------------------------------------------


def test_a_failing_put_append_leaves_the_memtable_untouched(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")
    size_before = os.path.getsize(engine.wal_path)

    failing = _FailingAppend()
    engine._wal.append_put = failing

    with pytest.raises(OSError, match="simulated disk failure"):
        engine.put(b"alpha", b"two")

    assert failing.calls == 1
    assert engine.get(b"alpha") == b"one", "the memtable took a write the log rejected"
    assert os.path.getsize(engine.wal_path) == size_before


def test_a_failing_put_append_does_not_create_the_key(engine: LedgerLog) -> None:
    engine._wal.append_put = _FailingAppend()

    with pytest.raises(OSError):
        engine.put(b"brand-new", b"one")

    assert engine.get(b"brand-new") is None
    assert engine._memtable.lookup(b"brand-new") is None, "a failed write left a record behind"


def test_a_failing_delete_append_leaves_the_value_in_place(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")
    engine._wal.append_delete = _FailingAppend()

    with pytest.raises(OSError):
        engine.delete(b"alpha")

    assert engine.get(b"alpha") == b"one", "the tombstone was recorded despite the failed append"
    entry = engine._memtable.lookup(b"alpha")
    assert entry is not None and not entry.is_tombstone


def test_a_record_the_format_rejects_never_reaches_the_memtable(
    engine: LedgerLog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The same guarantee through a real rejection rather than an injected failure.

    The size limit is lowered instead of building a 64 MiB value, but nothing
    else is stubbed: this is ``encode_record`` refusing an oversized record and
    raising before a byte is written, which is one of the ways a real append
    fails.
    """
    monkeypatch.setattr(wal_module, "MAX_PAYLOAD_SIZE", 32)
    size_before = os.path.getsize(engine.wal_path)

    with pytest.raises(WalFormatError, match="exceeds"):
        engine.put(b"alpha", b"x" * 64)

    assert engine.get(b"alpha") is None
    assert os.path.getsize(engine.wal_path) == size_before


@pytest.mark.parametrize("bad_key", ["a string", 42, None, bytearray(b"mutable")])
def test_a_non_bytes_key_is_rejected_without_touching_either_layer(
    engine: LedgerLog, bad_key: object
) -> None:
    size_before = os.path.getsize(engine.wal_path)

    with pytest.raises(TypeError, match="key must be bytes"):
        engine.put(bad_key, b"value")
    with pytest.raises(TypeError, match="key must be bytes"):
        engine.delete(bad_key)

    assert os.path.getsize(engine.wal_path) == size_before
    assert len(engine._memtable) == 0


@pytest.mark.parametrize("bad_value", ["a string", 42, None, bytearray(b"mutable")])
def test_a_non_bytes_value_is_rejected_without_touching_either_layer(
    engine: LedgerLog, bad_value: object
) -> None:
    size_before = os.path.getsize(engine.wal_path)

    with pytest.raises(TypeError, match="value must be bytes"):
        engine.put(b"alpha", bad_value)

    assert os.path.getsize(engine.wal_path) == size_before
    assert len(engine._memtable) == 0


def test_the_engine_keeps_working_after_a_rejected_write(engine: LedgerLog) -> None:
    """A rejected write must not leave the engine wedged or the log misaligned."""
    with pytest.raises(TypeError):
        engine.put(b"alpha", "not bytes")

    engine.put(b"alpha", b"one")
    engine.delete(b"beta")

    assert engine.get(b"alpha") == b"one"
    assert logged(engine) == [
        (WalOp.PUT, b"alpha", b"one"),
        (WalOp.DELETE, b"beta", b""),
    ]


# ---------------------------------------------------------------------------
# Story M3.1: opening, closing and the files on disk
# ---------------------------------------------------------------------------


def test_opening_creates_the_directory_and_the_log(tmp_path: Path) -> None:
    directory = tmp_path / "missing" / "nested"

    with LedgerLog(directory) as engine:
        assert directory.is_dir()
        assert engine.directory == directory
        assert engine.wal_path == directory / WAL_FILENAME
        assert engine.wal_path.is_file()
        assert os.path.getsize(engine.wal_path) == FILE_HEADER_SIZE


def test_a_new_log_is_stamped_with_the_current_format_version(tmp_path: Path) -> None:
    """The engine's log carries a version byte like every other file this engine writes."""
    with LedgerLog(tmp_path / "data") as engine:
        assert read_file_header_from_path(engine.wal_path) == WAL_FORMAT_VERSION


def test_a_log_in_an_unrecognized_format_stops_the_engine_from_opening(tmp_path: Path) -> None:
    """A log this build cannot parse must be refused at open, not appended to.

    Refusing while the engine is still empty of this run's writes is the whole
    value of the check: an engine that opened anyway would append records in one
    layout behind records in another, and the caller would only find out at the
    restart where those records were the only copy.
    """
    directory = tmp_path / "data"
    directory.mkdir()
    (directory / WAL_FILENAME).write_bytes(encode_file_header(version=WAL_FORMAT_VERSION + 1))

    with pytest.raises(WalUnsupportedVersionError):
        LedgerLog(directory)


def test_an_existing_directory_is_reused_rather_than_refused(tmp_path: Path) -> None:
    directory = tmp_path / "data"
    directory.mkdir()

    with LedgerLog(directory) as engine:
        engine.put(b"alpha", b"one")
        assert engine.get(b"alpha") == b"one"


def test_reopening_appends_to_the_existing_log_instead_of_replacing_it(tmp_path: Path) -> None:
    """Records from an earlier run survive a reopen rather than being replaced.

    This is the part recovery rests on, kept as its own test because it is about
    the file rather than the recovered state: the earlier run's records are
    still on disk, in order, with the new run's records appended after them. The
    replay that turns those records back into state is asserted further down.
    """
    directory = tmp_path / "data"
    with LedgerLog(directory) as first:
        first.put(b"alpha", b"one")
        first.delete(b"beta")

    with LedgerLog(directory) as second:
        second.put(b"gamma", b"three")

        assert logged(second) == [
            (WalOp.PUT, b"alpha", b"one"),
            (WalOp.DELETE, b"beta", b""),
            (WalOp.PUT, b"gamma", b"three"),
        ]


def test_the_default_fsync_policy_is_always(engine: LedgerLog) -> None:
    """The durable default is the engine's too, not just the WAL writer's."""
    assert engine.fsync_policy is FsyncPolicy.ALWAYS


@pytest.mark.parametrize("policy", ["always", "interval", "never"])
def test_the_fsync_policy_is_passed_through_to_the_log(tmp_path: Path, policy: str) -> None:
    with LedgerLog(tmp_path / f"data-{policy}", fsync_policy=policy) as engine:
        assert engine.fsync_policy is FsyncPolicy(policy)
        engine.put(b"alpha", b"one")
        assert engine.get(b"alpha") == b"one"


def test_an_unknown_fsync_policy_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unknown fsync policy"):
        LedgerLog(tmp_path / "data", fsync_policy="sometimes")


def test_an_unusable_fsync_interval_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="fsync interval"):
        LedgerLog(tmp_path / "data", fsync_policy="interval", fsync_interval_seconds=0)


def test_close_is_idempotent(tmp_path: Path) -> None:
    engine = LedgerLog(tmp_path / "data")
    engine.put(b"alpha", b"one")

    engine.close()
    engine.close()

    assert engine.closed


def test_operations_after_close_are_refused(tmp_path: Path) -> None:
    engine = LedgerLog(tmp_path / "data")
    engine.put(b"alpha", b"one")
    engine.close()

    with pytest.raises(ValueError, match="closed LedgerLog"):
        engine.put(b"beta", b"two")
    with pytest.raises(ValueError, match="closed LedgerLog"):
        engine.delete(b"alpha")
    with pytest.raises(ValueError, match="closed LedgerLog"):
        engine.get(b"alpha")


def test_a_refused_write_after_close_does_not_reach_the_log(tmp_path: Path) -> None:
    engine = LedgerLog(tmp_path / "data")
    engine.put(b"alpha", b"one")
    engine.close()
    size_after_close = os.path.getsize(engine.wal_path)

    with pytest.raises(ValueError):
        engine.put(b"beta", b"two")

    assert os.path.getsize(engine.wal_path) == size_after_close
    assert [record.key for record in wal_records(engine)] == [b"alpha"]


def test_the_context_manager_closes_the_engine_on_the_way_out(tmp_path: Path) -> None:
    with LedgerLog(tmp_path / "data") as engine:
        engine.put(b"alpha", b"one")
        assert not engine.closed

    assert engine.closed


def test_the_context_manager_closes_the_engine_when_the_body_raises(tmp_path: Path) -> None:
    engine = LedgerLog(tmp_path / "data")

    with pytest.raises(RuntimeError, match="boom"), engine:
        engine.put(b"alpha", b"one")
        raise RuntimeError("boom")

    assert engine.closed
    assert [record.key for record in wal_records(engine)] == [b"alpha"]


# ---------------------------------------------------------------------------
# Story M3.1: the concurrency the module docstring promises
#
# engine.py claims that writers serialize on one lock covering both steps, and
# that readers take nothing and never wait. Both are claims about threads, so
# per CLAUDE.md they are tested with threads. The first test below is the one
# that would catch the tempting shortcut of relying on the WAL writer's own lock
# instead of holding one across both steps.
# ---------------------------------------------------------------------------


def test_the_log_order_and_the_memtable_agree_under_contending_writers(tmp_path: Path) -> None:
    """Replaying the log must reproduce exactly what the engine holds in memory.

    Eight threads write to the same small set of keys, so every key is contended
    and the last writer for it is decided by the lock. If the log append and the
    memtable update were not taken together, two writes to one key could be
    logged in one order and applied in the other, and this assertion would find
    a key whose in-memory value is not the one its last log record carries. That
    engine would change that key's value on the next restart.
    """
    keys = [f"key-{index}".encode() for index in range(8)]
    writers = 8
    per_writer = 200
    errors: list[BaseException] = []

    with LedgerLog(tmp_path / "data", fsync_policy=FsyncPolicy.NEVER) as engine:

        def write(thread_id: int) -> None:
            try:
                for step in range(per_writer):
                    key = keys[(thread_id + step) % len(keys)]
                    engine.put(key, f"t{thread_id}-s{step}".encode())
            except BaseException as error:
                errors.append(error)

        threads = [
            threading.Thread(target=write, args=(thread_id,), daemon=True)
            for thread_id in range(writers)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)

        assert not errors, f"a writer thread raised: {errors[0]!r}"
        assert all(not thread.is_alive() for thread in threads), "a writer thread did not finish"

        records = wal_records(engine)
        assert len(records) == writers * per_writer, "records were lost or duplicated in the log"

        replayed: dict[bytes, bytes] = {}
        for record in records:
            replayed[record.key] = record.value

        for key in keys:
            assert engine.get(key) == replayed[key], (
                f"key {key!r} holds a different value in memory than its last log record"
            )


def test_readers_run_against_a_writer_without_waiting_or_seeing_torn_values(
    tmp_path: Path,
) -> None:
    """Readers must keep running while a writer works, and never invent a value.

    The values a reader may legitimately see are exactly the ones the writer has
    written, plus ``None`` before the first write for that key. Anything else
    would mean a reader observed a half-applied update.

    The readers hand the interpreter back every so often (``YIELD_EVERY`` below)
    rather than spinning flat out. That is not a weakening of the test: four
    threads looping on a pure-Python read starve the writer through the global
    interpreter lock alone, with no engine lock involved at all, which turns two
    thousand writes into half a minute and measures CPython's scheduler instead
    of this module. With the yield in place the readers still complete tens of
    thousands of lookups against those writes, which is the thing being asserted.
    """
    key = b"contended"
    total_writes = 2000
    yield_every = 20
    allowed = {f"v{index}".encode() for index in range(total_writes)}
    stop = threading.Event()
    errors: list[BaseException] = []
    reads = [0] * 4

    with LedgerLog(tmp_path / "data", fsync_policy=FsyncPolicy.NEVER) as engine:

        def read(slot: int) -> None:
            try:
                while not stop.is_set():
                    value = engine.get(key)
                    assert value is None or value in allowed, (
                        f"reader saw a value nobody wrote: {value!r}"
                    )
                    reads[slot] += 1
                    if reads[slot] % yield_every == 0:
                        time.sleep(0)
            except BaseException as error:
                errors.append(error)

        readers = [
            threading.Thread(target=read, args=(slot,), daemon=True) for slot in range(len(reads))
        ]
        for thread in readers:
            thread.start()
        try:
            for index in range(total_writes):
                engine.put(key, f"v{index}".encode())
        finally:
            stop.set()
            for thread in readers:
                thread.join(timeout=60)

        assert not errors, f"a reader thread raised: {errors[0]!r}"
        assert all(not thread.is_alive() for thread in readers), "a reader thread did not finish"
        assert all(count > 0 for count in reads), "a reader made no progress while the writer ran"
        assert sum(reads) > total_writes, (
            "the readers completed fewer lookups than the writer did writes, which is what "
            "it would look like if reads were queueing behind the write path"
        )
        assert engine.get(key) == f"v{total_writes - 1}".encode()


def test_no_write_is_lost_when_many_threads_write_distinct_keys(tmp_path: Path) -> None:
    """Every key each thread wrote must be readable, and logged exactly once."""
    writers = 8
    per_writer = 250
    errors: list[BaseException] = []

    with LedgerLog(tmp_path / "data", fsync_policy=FsyncPolicy.NEVER) as engine:

        def write(thread_id: int) -> None:
            try:
                for step in range(per_writer):
                    engine.put(f"t{thread_id}-k{step:04d}".encode(), f"v{step}".encode())
            except BaseException as error:
                errors.append(error)

        threads = [
            threading.Thread(target=write, args=(thread_id,), daemon=True)
            for thread_id in range(writers)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)

        assert not errors, f"a writer thread raised: {errors[0]!r}"

        for thread_id in range(writers):
            for step in range(per_writer):
                key = f"t{thread_id}-k{step:04d}".encode()
                assert engine.get(key) == f"v{step}".encode(), f"lost the write for {key!r}"

        keys_logged = [record.key for record in wal_records(engine)]
        assert len(keys_logged) == writers * per_writer
        assert len(set(keys_logged)) == writers * per_writer, "a key was logged more than once"


def _race_a_write_against_close(directory: Path, attempt: int) -> None:
    """Start a write in another thread, close the engine, and check the two layers agree.

    Extracted from the loop below so the thread's closure captures parameters
    rather than a loop variable, which is the classic way a test like this ends
    up racing a value that has already moved on.
    """
    engine = LedgerLog(directory, fsync_policy=FsyncPolicy.NEVER)
    engine.put(b"settled", b"value")
    outcome: list[str] = []

    def write() -> None:
        try:
            engine.put(b"racing", f"attempt-{attempt}".encode())
            outcome.append("written")
        except ValueError:
            outcome.append("refused")

    writer = threading.Thread(target=write, daemon=True)
    writer.start()
    engine.close()
    writer.join(timeout=60)

    assert outcome, "the writer thread did not finish"
    with WalReader(engine.wal_path) as reader:
        logged_keys = [record.key for record in reader]

    if outcome[0] == "written":
        assert b"racing" in logged_keys, "an acknowledged write is missing from the log"
        assert engine._memtable.get(b"racing") == f"attempt-{attempt}".encode()
    else:
        assert b"racing" not in logged_keys, "a refused write still reached the log"
        assert engine._memtable.lookup(b"racing") is None


def test_a_concurrent_close_does_not_tear_a_write_in_progress(tmp_path: Path) -> None:
    """Whatever a racing close does to a write, the two layers must still agree.

    A close that landed between a write's log append and its memtable update
    would leave a record on disk with no value in memory, or the reverse, which
    is the disagreement the write lock exists to prevent. The write itself is
    allowed to be refused, since the engine may have closed first; what it may
    not do is half happen.
    """
    for attempt in range(25):
        _race_a_write_against_close(tmp_path / f"data-{attempt}", attempt)


# ---------------------------------------------------------------------------
# Story M3.2: startup replays the log into a fresh memtable
# ---------------------------------------------------------------------------


def wal_records_at(path: Path) -> list[WalRecord]:
    """Read back every record in the log at ``path``, in write order.

    Separate from :func:`wal_records` above, which takes an open engine. The
    recovery tests need to look at a log belonging to an engine that is closed,
    or to a process that was killed, so there is nothing to take the path from.
    """
    with WalReader(path) as reader:
        return list(reader)


def test_a_reopened_engine_serves_every_value_the_previous_run_wrote(tmp_path: Path) -> None:
    directory = tmp_path / "data"
    with LedgerLog(directory) as first:
        for index in range(50):
            first.put(b"key-%04d" % index, b"value-%04d" % index)

    with LedgerLog(directory) as second:
        assert second.recovery.records_replayed == 50
        assert second.recovery.is_intact
        for index in range(50):
            assert second.get(b"key-%04d" % index) == b"value-%04d" % index


def test_the_replay_finishes_before_the_constructor_returns(tmp_path: Path) -> None:
    """The first call a caller makes already sees the recovered state.

    Worth its own test because the alternative design (recover lazily, on the
    first read) would pass every other test in this section while leaving a
    window where a get answers from an empty memtable. The only way to observe
    that window from outside is to read before doing anything else at all, which
    is what this does.
    """
    directory = tmp_path / "data"
    with LedgerLog(directory) as first:
        first.put(b"alpha", b"one")
        first.delete(b"beta")

    second = LedgerLog(directory)
    try:
        assert second.get(b"alpha") == b"one"
        assert second.get(b"beta") is None
        assert second.recovery.records_replayed == 2
    finally:
        second.close()


def test_replay_applies_records_in_order_so_the_newest_write_wins(tmp_path: Path) -> None:
    """Recovery has to reconstruct state, not just the set of keys the log mentions."""
    directory = tmp_path / "data"
    with LedgerLog(directory) as first:
        first.put(b"alpha", b"one")
        first.put(b"alpha", b"two")
        first.put(b"alpha", b"three")

    with LedgerLog(directory) as second:
        assert second.recovery.records_replayed == 3
        assert second.get(b"alpha") == b"three"


def test_a_delete_still_shadows_an_earlier_put_after_a_restart(tmp_path: Path) -> None:
    directory = tmp_path / "data"
    with LedgerLog(directory) as first:
        first.put(b"alpha", b"one")
        first.delete(b"alpha")

    with LedgerLog(directory) as second:
        assert second.get(b"alpha") is None


def test_a_put_after_a_delete_comes_back_as_the_value(tmp_path: Path) -> None:
    """The reverse order of the test above, which a replay that sorted rather than
    sequenced its records would get wrong in exactly one of the two."""
    directory = tmp_path / "data"
    with LedgerLog(directory) as first:
        first.put(b"alpha", b"one")
        first.delete(b"alpha")
        first.put(b"alpha", b"two")

    with LedgerLog(directory) as second:
        assert second.get(b"alpha") == b"two"


def test_a_replayed_delete_is_a_tombstone_and_not_a_missing_key(tmp_path: Path) -> None:
    """Reaches into the memtable for the reason the M3.1 tombstone test does.

    ``get`` cannot tell a recovered tombstone from a key replay simply never
    inserted, and the difference is the whole durability value of replaying a
    delete: once there are SSTables below (story M7.2), a delete that came back
    as an absent key would let an older on-disk value resurface.
    """
    directory = tmp_path / "data"
    with LedgerLog(directory) as first:
        first.put(b"alpha", b"one")
        first.delete(b"alpha")
        first.delete(b"never-written")

    with LedgerLog(directory) as second:
        for key in (b"alpha", b"never-written"):
            entry = second._memtable.lookup(key)
            assert entry is not None, f"the replayed delete of {key!r} left no record"
            assert entry.is_tombstone


def test_an_empty_value_survives_a_restart_and_is_not_a_deletion(tmp_path: Path) -> None:
    """The distinction the record format carries has to survive the round trip."""
    directory = tmp_path / "data"
    with LedgerLog(directory) as first:
        first.put(b"empty", b"")
        first.delete(b"deleted")

    with LedgerLog(directory) as second:
        assert second.get(b"empty") == b""
        assert second.get(b"deleted") is None


def test_the_empty_key_survives_a_restart(tmp_path: Path) -> None:
    directory = tmp_path / "data"
    with LedgerLog(directory) as first:
        first.put(b"", b"empty-key")

    with LedgerLog(directory) as second:
        assert second.get(b"") == b"empty-key"


def test_a_fresh_directory_recovers_nothing_and_reports_an_intact_log(tmp_path: Path) -> None:
    with LedgerLog(tmp_path / "data") as engine:
        assert engine.recovery.records_replayed == 0
        assert engine.recovery.is_intact
        assert engine.recovery.stopped_at is None
        assert engine.recovery.reason is None


def test_a_log_holding_only_a_header_replays_zero_records(tmp_path: Path) -> None:
    """An engine opened and closed without a write leaves exactly this file."""
    directory = tmp_path / "data"
    with LedgerLog(directory):
        pass
    assert os.path.getsize(directory / WAL_FILENAME) == FILE_HEADER_SIZE

    with LedgerLog(directory) as second:
        assert second.recovery.records_replayed == 0
        assert second.recovery.is_intact


def test_writes_accumulate_across_several_restarts(tmp_path: Path) -> None:
    directory = tmp_path / "data"
    for run in range(5):
        with LedgerLog(directory) as engine:
            assert engine.recovery.records_replayed == run
            for earlier in range(run):
                assert engine.get(b"run-%d" % earlier) == b"value-%d" % earlier
            engine.put(b"run-%d" % run, b"value-%d" % run)

    with LedgerLog(directory) as final:
        assert final.recovery.records_replayed == 5
        recovered = [final.get(b"run-%d" % run) for run in range(5)]
        assert recovered == [b"value-%d" % run for run in range(5)]


def test_replay_reads_the_log_before_the_writer_opens_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Order matters, not just that both happen.

    Replay truncates a damaged log, so it has to be the file's only user while
    it runs. If the writer opened first, an engine recovering a torn log would
    have an appender sitting behind a truncation that never saw its records.
    """
    directory = tmp_path / "data"
    with LedgerLog(directory) as first:
        first.put(b"alpha", b"one")

    order: list[str] = []
    real_replay = engine_module.replay
    real_writer = engine_module.WalWriter

    def spy_replay(*args: object, **kwargs: object) -> object:
        order.append("replay")
        return real_replay(*args, **kwargs)

    def spy_writer(*args: object, **kwargs: object) -> object:
        order.append("writer")
        return real_writer(*args, **kwargs)

    monkeypatch.setattr(engine_module, "replay", spy_replay)
    monkeypatch.setattr(engine_module, "WalWriter", spy_writer)

    with LedgerLog(directory) as second:
        assert second.get(b"alpha") == b"one"

    assert order == ["replay", "writer"]


# ---------------------------------------------------------------------------
# Story M3.2: a torn or corrupt tail is discarded, the prefix is recovered
# ---------------------------------------------------------------------------


def _write_three_records(directory: Path) -> list[WalRecord]:
    """Write a known three-record log, close cleanly, and return its records.

    The records are returned so a test can truncate or corrupt at an offset
    inside a chosen one, which is the only way to aim the damage at a specific
    field rather than wherever a buffer boundary happened to fall.
    """
    with LedgerLog(directory) as engine:
        engine.put(b"alpha", b"one")
        engine.put(b"beta", b"two")
        engine.put(b"gamma", b"three")
    return wal_records_at(directory / WAL_FILENAME)


@pytest.mark.parametrize(
    ("description", "cut_into_last_record"),
    [
        ("mid length field", 2),
        ("mid checksum", 6),
        ("header only, no payload", RECORD_HEADER_SIZE),
        ("mid payload", RECORD_HEADER_SIZE + 2),
    ],
)
def test_a_torn_tail_is_discarded_and_every_earlier_record_is_recovered(
    tmp_path: Path, description: str, cut_into_last_record: int
) -> None:
    """A crash mid-append can stop at any byte, so every field gets its own case.

    The three cases before the payload are the ones a reader has to survive
    without trusting a number it has not finished reading: a half-written length
    field can claim any size at all, which is why the WAL bounds it against the
    bytes actually present rather than allocating first and checking later.
    """
    directory = tmp_path / "data"
    records = _write_three_records(directory)
    last = records[-1]
    path = directory / WAL_FILENAME

    with open(path, "r+b") as handle:
        handle.truncate(last.offset + cut_into_last_record)

    with LedgerLog(directory) as recovered:
        assert recovered.recovery.records_replayed == 2, description
        assert not recovered.recovery.is_intact
        assert recovered.recovery.stopped_at == last.offset
        assert recovered.recovery.reason is not None

        assert recovered.get(b"alpha") == b"one"
        assert recovered.get(b"beta") == b"two"
        assert recovered.get(b"gamma") is None, "the torn record was replayed anyway"

    assert os.path.getsize(path) == last.offset, "the torn tail was left on disk"


def test_a_corrupt_record_in_the_middle_discards_it_and_everything_after(
    tmp_path: Path,
) -> None:
    """Corruption is treated as a stopping point, not as one bad record to skip.

    Resuming after a damaged record would hand the memtable a state that never
    existed, with a later write present while the one it superseded is missing,
    so replay stops. The consequence is deliberate and is asserted here: the
    third record is lost even though its own bytes are fine.
    """
    directory = tmp_path / "data"
    records = _write_three_records(directory)
    middle = records[1]
    path = directory / WAL_FILENAME

    with open(path, "r+b") as handle:
        # Into the payload, past the record header, so the length and checksum
        # fields still look plausible and only the CRC32 comparison catches it.
        handle.seek(middle.offset + RECORD_HEADER_SIZE + 1)
        original = handle.read(1)
        handle.seek(middle.offset + RECORD_HEADER_SIZE + 1)
        handle.write(bytes([original[0] ^ 0xFF]))

    with LedgerLog(directory) as recovered:
        assert recovered.recovery.records_replayed == 1
        assert recovered.recovery.stopped_at == middle.offset
        assert recovered.get(b"alpha") == b"one"
        assert recovered.get(b"beta") is None
        assert recovered.get(b"gamma") is None

    assert os.path.getsize(path) == middle.offset


def test_writes_after_a_recovered_tear_append_at_the_truncation_point(tmp_path: Path) -> None:
    """The recovered log has to be appendable, and contiguous afterwards.

    An engine that recovered the state but left the torn bytes in place would
    bury the damage in the middle of the log, and the next replay would stop at
    it and silently drop everything this run wrote.
    """
    directory = tmp_path / "data"
    records = _write_three_records(directory)
    path = directory / WAL_FILENAME

    with open(path, "r+b") as handle:
        handle.truncate(records[-1].offset + RECORD_HEADER_SIZE + 1)

    with LedgerLog(directory) as recovered:
        recovered.put(b"delta", b"four")

    assert [(record.op, record.key, record.value) for record in wal_records_at(path)] == [
        (WalOp.PUT, b"alpha", b"one"),
        (WalOp.PUT, b"beta", b"two"),
        (WalOp.PUT, b"delta", b"four"),
    ]

    with LedgerLog(directory) as reopened:
        assert reopened.recovery.is_intact
        assert reopened.recovery.records_replayed == 3
        assert reopened.get(b"alpha") == b"one"
        assert reopened.get(b"beta") == b"two"
        assert reopened.get(b"gamma") is None
        assert reopened.get(b"delta") == b"four"


def test_a_log_truncated_inside_its_header_stops_the_engine_from_opening(tmp_path: Path) -> None:
    """A file that is not a readable WAL is refused, and is not truncated further.

    Recovery declines to guess at a file it cannot parse. Deleting or re-stamping
    it would be the one response worse than refusing, since a half-written header
    could equally be an unrelated file the caller pointed at by mistake.
    """
    directory = tmp_path / "data"
    with LedgerLog(directory) as engine:
        engine.put(b"alpha", b"one")
    path = directory / WAL_FILENAME

    with open(path, "r+b") as handle:
        handle.truncate(FILE_HEADER_SIZE - 1)

    with pytest.raises(WalFormatError):
        LedgerLog(directory)
    assert os.path.getsize(path) == FILE_HEADER_SIZE - 1


# ---------------------------------------------------------------------------
# Story M3.2: a killed process, restarted, recovers what it acknowledged
# ---------------------------------------------------------------------------


_CHILD_SOURCE = """
import sys

from ledgerlog import LedgerLog

engine = LedgerLog(sys.argv[1], fsync_policy="always")

for line in sys.stdin:
    command = line.strip()
    if not command:
        continue
    kind, _, argument = command.partition(" ")
    if kind == "put":
        key, _, value = argument.partition("=")
        engine.put(key.encode(), value.encode())
    elif kind == "delete":
        engine.delete(argument.encode())
    else:
        raise SystemExit("unknown command: " + command)
    print("done " + command, flush=True)
"""
"""A child engine driven one operation at a time over its standard input.

Driven rather than looping on its own so the parent knows exactly how many
writes were acknowledged when it pulls the trigger: the child does not start an
operation until it is told to, and does not get told until the previous one has
been acknowledged. That is what lets the kill tests assert the recovered state
*exactly* rather than approximately, with no write left in flight to argue about.

The policy is ``always`` for the same reason: with an fsync behind every append,
an acknowledgement is a statement that the record is on the disk, which is the
promise the restart is being asked to keep.
"""


class _KilledEngine:
    """A LedgerLog running in a child process that the test can SIGKILL.

    A real process and a real signal, because the point of the story is what
    survives a shutdown that ran no cleanup at all. Closing an engine in-process,
    or raising out of a context manager, both leave Python's own teardown to run,
    which is exactly the code path a crash does not take. SIGKILL cannot be
    caught, blocked, or handled, so there is no clean shutdown to accidentally
    depend on.
    """

    def __init__(self, directory: Path, script: Path) -> None:
        script.write_text(_CHILD_SOURCE)
        source_root = str(Path(ledgerlog.__file__).resolve().parent.parent)
        environment = dict(os.environ)
        existing = environment.get("PYTHONPATH")
        environment["PYTHONPATH"] = (
            source_root if not existing else os.pathsep.join([source_root, existing])
        )
        self._process = subprocess.Popen(
            [sys.executable, str(script), str(directory)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            env=environment,
        )

    def run(self, command: str, *, timeout: float = 60.0) -> None:
        """Send one operation and block until the child acknowledges it.

        The wait has a deadline because these tests run unattended. A child that
        neither answers nor exits (an engine that deadlocked on its own write
        lock would look exactly like that) should fail the suite rather than
        hang it, and a blocking read would hang it.
        """
        assert self._process.stdin is not None
        assert self._process.stdout is not None
        self._process.stdin.write(command + "\n")
        self._process.stdin.flush()

        ready, _, _ = select.select([self._process.stdout], [], [], timeout)
        assert ready, f"child did not answer {command!r} within {timeout} seconds"

        acknowledgement = self._process.stdout.readline()
        assert acknowledgement.strip() == f"done {command}", (
            f"child did not acknowledge {command!r}, got {acknowledgement!r}"
        )

    def kill(self) -> None:
        """Kill the child outright and wait for the operating system to reap it."""
        os.kill(self._process.pid, signal.SIGKILL)
        assert self._process.wait(timeout=30) != 0, "the child exited cleanly instead of dying"

    def close(self) -> None:
        """Make sure the child is gone, whatever the test did or did not do."""
        if self._process.poll() is None:
            self._process.kill()
            self._process.wait(timeout=30)
        for stream in (self._process.stdin, self._process.stdout):
            if stream is not None:
                stream.close()


@pytest.fixture
def killable_engine(tmp_path: Path) -> Iterator[Callable[[Path], _KilledEngine]]:
    """Factory for child engines, each cleaned up when the test finishes."""
    children: list[_KilledEngine] = []

    def start(directory: Path) -> _KilledEngine:
        child = _KilledEngine(directory, tmp_path / f"child_{len(children)}.py")
        children.append(child)
        return child

    try:
        yield start
    finally:
        for child in children:
            child.close()


def test_a_killed_process_recovers_every_write_it_acknowledged(
    tmp_path: Path, killable_engine: Callable[[Path], _KilledEngine]
) -> None:
    """The story's headline claim, measured against a process that really died."""
    directory = tmp_path / "data"
    child = killable_engine(directory)
    for index in range(12):
        child.run(f"put key-{index:04d}=value-{index:04d}")
    child.kill()

    with LedgerLog(directory) as restarted:
        assert restarted.recovery.records_replayed == 12
        assert restarted.recovery.is_intact, restarted.recovery.reason
        for index in range(12):
            expected = f"value-{index:04d}".encode()
            assert restarted.get(f"key-{index:04d}".encode()) == expected


def test_a_kill_partway_through_a_sequence_recovers_exactly_that_prefix(
    tmp_path: Path, killable_engine: Callable[[Path], _KilledEngine]
) -> None:
    """Writes the child never started must not come back, and nothing else may be missing.

    The child is killed while blocked waiting for its next instruction, so the
    boundary is exact: six operations were acknowledged and the seventh had not
    begun. An engine that recovered fewer has lost an acknowledged write, and one
    that recovered more has invented a write the caller never made.
    """
    directory = tmp_path / "data"
    child = killable_engine(directory)
    acknowledged = [
        "put alpha=one",
        "put beta=two",
        "put gamma=three",
        "put alpha=one-updated",
        "delete beta",
        "put delta=",
    ]
    for command in acknowledged:
        child.run(command)
    child.kill()

    with LedgerLog(directory) as restarted:
        assert restarted.recovery.records_replayed == len(acknowledged)
        assert restarted.recovery.is_intact, restarted.recovery.reason

        assert restarted.get(b"alpha") == b"one-updated"
        assert restarted.get(b"beta") is None, "a killed process resurrected a deleted key"
        assert restarted.get(b"gamma") == b"three"
        assert restarted.get(b"delta") == b"", "an empty value came back as a deletion"
        assert restarted.get(b"epsilon") is None, "a write the child never made came back"

        entry = restarted._memtable.lookup(b"beta")
        assert entry is not None and entry.is_tombstone


def test_a_killed_process_leaves_a_log_a_restarted_engine_can_keep_using(
    tmp_path: Path, killable_engine: Callable[[Path], _KilledEngine]
) -> None:
    """Recovery is not a one-shot read: the restarted engine owns the log afterwards."""
    directory = tmp_path / "data"
    first = killable_engine(directory)
    first.run("put alpha=one")
    first.run("put beta=two")
    first.kill()

    with LedgerLog(directory) as restarted:
        restarted.put(b"gamma", b"three")
        restarted.delete(b"alpha")

    second = killable_engine(directory)
    second.run("put epsilon=five")
    second.kill()

    with LedgerLog(directory) as final:
        assert final.recovery.records_replayed == 5
        assert final.get(b"alpha") is None
        assert final.get(b"beta") == b"two"
        assert final.get(b"gamma") == b"three"
        assert final.get(b"epsilon") == b"five"
