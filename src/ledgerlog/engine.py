"""Engine: the write-ahead log and the memtable wired into one key-value store.

Scope of this module today (stories M3.1, M3.2 and M6.1): the write path,
startup recovery, and the freeze-and-swap that retires a memtable once it has
grown past a configured size. A put or a delete is appended to the WAL first and
applied to the memtable second, a get answers from the memtable and then from any
frozen memtables behind it, and opening an engine over an existing log replays
that log into a fresh memtable before the caller can issue anything. Every
acknowledged write is in the log before any reader can see it, and a restart
brings every one of them back.

Why the WAL comes first, since this ordering is the only reason the module
exists: the memtable is memory and the log is disk, so the moment a write
becomes visible to a reader is the moment the engine has promised it. Applying
to the memtable first would let a reader observe a value that a crash one
instruction later would erase, and the engine would have handed out an answer
the disk never agreed to. Logging first can only fail the other way, by leaving
a record on disk for a write no reader ever saw, and that costs nothing: replay
applies it at startup and the caller, who never got an acknowledgement, cannot
tell the difference. The asymmetry is the point. One direction can lose an
acknowledged write, the other cannot.

What "the WAL append failed, so the memtable was not updated" rests on: the
memtable call is simply not reached. There is no compensating undo here, and
there deliberately is not one. An undo would have to remove a key from the
memtable, and removal at that layer is exactly what ARCHITECTURE.md section 3
forbids (a removed key falls through to an older value on disk), so a failed
write that tried to tidy up after itself could resurrect an older value. Not
starting is the only clean way to not have happened.

Why validation is left to the WAL rather than repeated here: :func:`encode_record`
rejects a non-``bytes`` key, a non-``bytes`` value and an oversized record
before it writes a single byte, so a rejected write fails in the WAL step and
the memtable is never touched. Checking the same things again in this module
would add a second place for the two layers' ideas of a valid write to drift
apart, and would make a type error fail in a different step than a size error
for no reason a caller benefits from.

Startup recovery
----------------

Opening an engine replays the log at ``wal.log`` into the memtable before the
constructor returns, so there is no window in which a caller holds an engine
whose memtable is emptier than its log. That is the whole of the ordering
argument above, seen from the other end: the write path promises that an
acknowledged write reached the disk, and replay is what makes that promise worth
something after the process that made it is gone.

Replay runs before the WAL writer opens, and that order is required rather than
tidy. :func:`~ledgerlog.wal.replay` truncates the log at the first record it
cannot trust, so it has to be the log's only user while it runs (see its
docstring). Opening the writer first would put an appender behind a truncation
that never saw its records.

A torn tail is discarded rather than repaired, which is :func:`replay`'s
behavior and ARCHITECTURE.md section 1's rule, and it costs the engine nothing
it promised: a record left half-written is a record whose write never returned,
so no caller was ever told it happened. What the engine adds is that the
discarding is reported instead of silent. :attr:`LedgerLog.recovery` carries how
many records came back and, if the log was damaged, where replay stopped and
why, because an operator restarting after a crash has a real question about
whether the machine lost the tail of a log or is losing writes generally, and an
engine that quietly dropped the evidence could not answer it.

A log whose header is missing, foreign, or of an unknown version stops the open
with an exception and nothing is truncated. Recovery declines to guess at a file
it cannot parse, for the reason ``wal.py`` gives: the one response worse than
refusing to recover from such a file is destroying it.

Freeze and swap
---------------

A memtable is not written to forever. Once the active one's payload size (see
``memtable.py`` for exactly what that counts) reaches
``memtable_threshold_bytes``, the writer that pushed it over freezes it, hands it
to :attr:`LedgerLog.frozen_memtables`, and installs a fresh empty memtable for
the writes that follow. ARCHITECTURE.md section 2 asks for precisely this: the
flush runs against a table nothing can still be writing to, and the caller's next
write does not wait for the flush, because it goes somewhere else.

The check runs on the write path, inside the same lock as the write, rather than
on a timer or a background thread. That is what makes the threshold mean
something: a writer that has just applied a record is the one thread that knows
the size is now over, and checking there means no write is ever applied to a
table already over the line by more than the one record that crossed it.

Publication order during the swap is the only subtle part, and it is written the
way it is because :meth:`LedgerLog.get` takes no lock. The table is frozen, then
appended to the frozen tuple, and only then replaced as the active one. A reader
that loads the old active table finds the records there; a reader that loads the
new one finds nothing and goes on to the frozen tuple, which was published
before the table it is looking for stopped being active. There is no ordering of
those loads that lets a reader see neither, which is what "no write is lost
during the swap" comes to for a reader. Doing it the other way round, swapping
first and publishing after, opens exactly that window.

Frozen tables are kept newest-first and are never dropped by this module.
Dropping one is :meth:`LedgerLog.drop_frozen`, which the flush calls once the
SSTable's footer is on disk (story M6.2), because until that moment the records
exist only in that table and in the log. So until M6.2 lands, a long-running
engine accumulates frozen tables rather than releasing memory, which is the
honest state of a half-built flush path and not an oversight: the alternative,
dropping a frozen table with nowhere to have written it, is data loss.

Concurrency
-----------

One lock guards the whole write path, and reads take nothing.

* :meth:`LedgerLog.put`, :meth:`LedgerLog.delete` and :meth:`LedgerLog.close`
  hold a per-engine lock across both steps, the WAL append and the memtable
  update together. Holding it across both is what makes the log and the memtable
  agree about order: the WAL writer has a lock of its own, so two concurrent
  puts of the same key would each land in the log intact, but without this outer
  lock they could still reach the memtable in the opposite order, leaving the
  in-memory value disagreeing with the last record in the log. A restart would
  then silently change the value of that key. The lock also supplies the single
  writer the memtable is specified for (see ``memtable.py``'s concurrency
  section), so several caller threads may write through one engine.
* :meth:`LedgerLog.get` takes no lock, because the memtable it reads is safe for
  any number of readers running against one writer. A read therefore never waits
  on a write, including one that is mid-fsync under the ``always`` policy, which
  is the case that would otherwise dominate read latency. The one qualification
  belongs to the memtable rather than to this module: on a free-threaded build it
  takes its own lock on the way through (see ``memtable.py``), so a read there
  can wait on the memtable step of a write, though never on the log step.

* The freeze-and-swap and :meth:`LedgerLog.drop_frozen` take the same engine
  lock, which is what gives story M6.1 the atomicity it asks for. A write cannot
  be in flight while a table is being frozen, because a write holds the lock
  across its whole self and the freeze happens inside that same critical
  section, so the record that crossed the threshold is in the table being frozen
  and the next record is in the table that replaced it, with nothing in between.

Lock ordering is fixed and one way, the engine lock then the WAL writer's or the
memtable's, so there is no cycle for two threads to deadlock around.

What is deliberately not here yet:

* Writing a frozen memtable out as an SSTable (story M6.2), and everything
  downstream of it. Freezing retires a memtable from writes but nothing yet
  moves its records to disk, so memory still grows with the data written, and
  the log is still replayed in full at startup because nothing yet trims it.
* Reading from SSTables (milestone 7). Every read is still answered from memory,
  from the active memtable and then the frozen ones.
"""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType

from ledgerlog.memtable import Memtable, MemtableEntry
from ledgerlog.wal import (
    DEFAULT_FSYNC_INTERVAL_SECONDS,
    FILE_HEADER_SIZE,
    FsyncPolicy,
    WalFormatError,
    WalOp,
    WalWriter,
    replay,
)

WAL_FILENAME = "wal.log"
"""Name of the write-ahead log inside an engine's directory.

A fixed name rather than a caller-supplied path: the directory is the unit an
engine owns, and from milestone 4 on it holds SSTables that have to be
discovered by name alongside this file. Letting the log live anywhere would mean
a data directory that is only complete if the caller remembers to point at the
matching log.
"""

DEFAULT_MEMTABLE_THRESHOLD_BYTES = 4 * 1024 * 1024
"""Payload bytes an active memtable may hold before it is frozen and replaced.

Four mebibytes, chosen as a starting point rather than a measured optimum, and
listed in ROADMAP.md milestone 10 as something the benchmarks are meant to tune.
The trade it sits in the middle of: a smaller threshold means more SSTables,
each cheap to flush but one more thing a read may have to check and one more
thing compaction has to merge, while a larger one means fewer, bigger files and
more memory held (several times this number once Python object overhead is
counted, per ``memtable.py``) and a longer log to replay after a crash, since
nothing trims the log until the data it describes is on disk.
"""


@dataclass(frozen=True)
class RecoveryReport:
    """What the replay at startup found in the log, and what it did about it.

    A summary rather than the :class:`~ledgerlog.wal.ReplayResult` it is built
    from, for one reason: that result holds every record it read, and an engine
    that kept it would pin a second copy of the whole replayed log in memory for
    as long as it stayed open. The memtable already holds the state those records
    add up to, so what is worth keeping is the count and the damage, not the
    bytes.

    Frozen because it describes an event that is over by the time any caller can
    look at it.
    """

    records_replayed: int
    end_offset: int
    stopped_at: int | None
    reason: str | None

    @property
    def is_intact(self) -> bool:
        """True if the whole log parsed, with no tail discarded."""
        return self.stopped_at is None


class LedgerLog:
    """A durable key-value store over a write-ahead log and an in-memory table.

    Keys and values are ``bytes``. Every :meth:`put` and :meth:`delete` is
    appended to the log before it is applied to the memtable, so a write is on
    disk before any reader can observe it, and opening an engine over an
    existing directory replays that log so a restart recovers every write the
    log holds. See this module's docstring for why that ordering is the
    guarantee the class exists to provide.

    Safe to use from several threads: writes serialize on one lock and reads
    take none.
    """

    def __init__(
        self,
        directory: str | os.PathLike[str],
        *,
        fsync_policy: FsyncPolicy | str = FsyncPolicy.ALWAYS,
        fsync_interval_seconds: float = DEFAULT_FSYNC_INTERVAL_SECONDS,
        memtable_threshold_bytes: int = DEFAULT_MEMTABLE_THRESHOLD_BYTES,
    ) -> None:
        """Open, or create, the engine whose data lives in ``directory``.

        The directory is created if it does not exist, including any missing
        parents, because an engine's directory is its own to manage: asking a
        caller to pre-create it would only turn a first run into an error with
        nothing to teach them.

        The fsync settings are handed to the WAL writer unchanged, and the
        default is :attr:`~ledgerlog.wal.FsyncPolicy.ALWAYS` for the reason the
        writer defaults to it: an acknowledged write should be on the disk unless
        the caller has explicitly asked to trade that away.

        If the directory already holds a log, it is replayed into the memtable
        here, before the writer opens and before the constructor returns. The
        constructor is the right place for it precisely because it is the one
        moment no caller has a reference to the engine yet: recovery that ran
        lazily, on the first get, would have to define what a get racing it
        should see, and there is no answer to that which is both simple and
        correct.

        ``memtable_threshold_bytes`` is the payload size at which the active
        memtable is frozen and replaced. It has to be at least one byte: a
        threshold of zero would be met by an empty memtable, so every write would
        freeze the table it had just landed in and the engine would make one
        table per record forever.
        """
        if memtable_threshold_bytes < 1:
            raise ValueError(
                f"memtable_threshold_bytes must be at least 1, got {memtable_threshold_bytes}"
            )

        self._directory = Path(directory)
        self._directory.mkdir(parents=True, exist_ok=True)

        self._write_lock = threading.Lock()
        self._closed = False
        self._memtable_threshold_bytes = memtable_threshold_bytes
        self._memtable = Memtable()
        self._frozen_memtables: tuple[Memtable, ...] = ()
        self._recovery = self._replay_existing_log()
        self._wal = WalWriter(
            self._directory / WAL_FILENAME,
            fsync_policy=fsync_policy,
            fsync_interval_seconds=fsync_interval_seconds,
        )

    @property
    def directory(self) -> Path:
        """Directory holding this engine's files."""
        return self._directory

    @property
    def wal_path(self) -> Path:
        """Path of the write-ahead log this engine appends to."""
        return self._wal.path

    @property
    def fsync_policy(self) -> FsyncPolicy:
        """Policy deciding when appended log bytes are forced onto the disk."""
        return self._wal.fsync_policy

    @property
    def memtable_threshold_bytes(self) -> int:
        """Payload size at which the active memtable is frozen and replaced."""
        return self._memtable_threshold_bytes

    @property
    def memtable_nbytes(self) -> int:
        """Payload bytes the active memtable currently holds.

        The frozen tables are not counted. This number is what the threshold is
        compared against, so a caller watching it is watching the same thing the
        write path is, and adding in tables that are no longer being written to
        would make it stop meaning that.
        """
        return self._memtable.nbytes

    @property
    def frozen_memtables(self) -> tuple[Memtable, ...]:
        """Memtables retired from writing but still holding records, newest first.

        Newest first because that is the order a read has to consult them in: a
        key written, frozen, written again and frozen again exists in two of
        these, and the newer record is the one that is true (ARCHITECTURE.md
        section 5).

        A tuple rather than a list, so that a caller holding this cannot change
        what the engine will read next, and so that the engine can publish a new
        set with one store that a lock-free reader sees all of or none of.
        """
        return self._frozen_memtables

    @property
    def recovery(self) -> RecoveryReport:
        """What the replay at startup found in this engine's log.

        Reports zero records and an intact log for a directory that had no log
        to replay, which is the honest description of that case: a new engine
        recovered everything there was.
        """
        return self._recovery

    @property
    def closed(self) -> bool:
        """True once :meth:`close` has run."""
        return self._closed

    def put(self, key: bytes, value: bytes) -> None:
        """Store ``value`` under ``key``, logging it before it becomes visible.

        The log append happens first and the memtable update second, so a
        :meth:`get` can only ever return a value whose record is already in the
        log. If the append raises, for a key or value that is not ``bytes``, a
        record over the format's size limit, or a failing disk, the memtable is
        left exactly as it was and the exception reaches the caller unchanged:
        the write did not happen, at either layer.

        Once the write is applied, this call also freezes the memtable and swaps
        in a fresh one if the threshold has been reached. That happens before the
        put returns, and under the same lock, so the caller's next write is
        already aimed at the new table.
        """
        with self._write_lock:
            self._check_open()
            self._wal.append_put(key, value)
            self._memtable.put(key, value)
            self._freeze_if_full()

    def delete(self, key: bytes) -> None:
        """Record that ``key`` was deleted, logging it before it becomes visible.

        Ordering and failure behavior match :meth:`put`. The memtable records a
        tombstone rather than removing anything, so the delete will shadow an
        older value on disk once there are SSTables to shadow (ARCHITECTURE.md
        section 3).

        Deleting a key this engine has no value for is not an error and still
        writes a record, because the engine cannot yet know whether an older
        layer holds that key, and a delete that quietly did nothing would be the
        one write that fails to shadow anything.
        """
        with self._write_lock:
            self._check_open()
            self._wal.append_delete(key)
            self._memtable.delete(key)
            self._freeze_if_full()

    def get(self, key: bytes) -> bytes | None:
        """Return the value stored under ``key``, or ``None`` if there is none.

        The active memtable is consulted first and the frozen ones after it,
        newest first, which is ARCHITECTURE.md section 5's ordering minus the
        SSTables that do not exist yet (story M7.2). The search stops at the
        first layer holding any record for the key, including a tombstone: a
        tombstone means the key was deleted after whatever an older layer still
        remembers, so falling through it would resurrect that older value.

        A deleted key therefore reads as ``None``, the same as a key that was
        never written. The two are different facts internally and the difference
        is what the paragraph above turns on, but to a caller asking for a value
        they come to the same thing.

        A key put with an empty value reads back as ``b""``, which is not
        ``None``. The distinction between an empty value and a deletion is
        carried all the way down into the log's record format, and it survives
        here.

        Takes no lock, and reads the active table before the frozen ones, which
        is the order the swap publishes in reverse. See this module's docstring
        for why that is what keeps a concurrent freeze from hiding a record from
        this call.
        """
        self._check_open()

        entry = self._lookup(key)
        if entry is None or entry.is_tombstone:
            return None
        return entry.value

    def _lookup(self, key: bytes) -> MemtableEntry | None:
        """Return the newest record held for ``key``, or ``None`` if there is none.

        Separate from :meth:`get` because the layers have to be walked with the
        distinction between "deleted here" and "not here" intact, and
        :meth:`Memtable.get` has already thrown it away. Collapsing the two into
        one loop would mean a tombstone in the active table reading the same as
        no record at all, and the search moving on to a frozen table that still
        holds the deleted value.
        """
        entry = self._memtable.lookup(key)
        if entry is not None:
            return entry

        for table in self._frozen_memtables:
            entry = table.lookup(key)
            if entry is not None:
                return entry

        return None

    def drop_frozen(self, memtable: Memtable) -> None:
        """Forget a frozen memtable, releasing its records from memory.

        The caller is asserting that these records are safe somewhere else. That
        will be an SSTable whose footer is fully written (story M6.2), since the
        footer is what makes a table count as existing at all (ARCHITECTURE.md
        section 6); dropping before that point loses every record in it that the
        log no longer covers.

        Identity, not equality, decides which table goes: two frozen tables can
        hold identical records and still be different snapshots, and the caller
        is dropping the one it flushed rather than one that looks like it.

        Raises :class:`ValueError` if the table is not one this engine is
        holding, rather than returning quietly. A caller dropping a table twice,
        or dropping one belonging to another engine, has lost track of which
        snapshot it flushed, and that is worth an exception on the spot rather
        than at the point where the records turn out to be missing.
        """
        with self._write_lock:
            remaining = tuple(table for table in self._frozen_memtables if table is not memtable)
            if len(remaining) == len(self._frozen_memtables):
                raise ValueError("memtable is not one of this engine's frozen memtables")
            self._frozen_memtables = remaining

    def close(self) -> None:
        """Close the log and stop accepting operations. Safe to call more than once.

        Takes the write lock, so a close cannot land between a write's log
        append and its memtable update and leave the two disagreeing.

        The engine is marked closed even if closing the log raises, since the
        handle is gone either way, and the exception is then re-raised rather
        than swallowed: a failure here is a failed final fsync, which means the
        log may be missing its last records, and a caller told the close
        succeeded would assume a durability the disk never confirmed.
        """
        with self._write_lock:
            if self._closed:
                return
            try:
                self._wal.close()
            finally:
                self._closed = True

    def __enter__(self) -> LedgerLog:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def _replay_existing_log(self) -> RecoveryReport:
        """Replay the directory's log into the memtable, and report what came back.

        Called once, from the constructor, with the memtable freshly built and
        the WAL writer not yet open. Both matter: replaying into a memtable that
        already held entries would let a previous run's records overwrite a
        newer state, and replaying while a writer is open would truncate a log
        underneath an appender.

        Records are applied in the order they were written, and a later record
        for a key simply overwrites an earlier one, which is what makes replay
        reconstruct the state the log describes rather than merely its contents.
        A DELETE replays as a tombstone rather than as a removal, exactly as the
        original delete did, so a recovered delete keeps the shadowing behavior
        that ARCHITECTURE.md section 3 depends on once there are older layers
        below.

        A missing log is not an error and not a special case worth much: a first
        run has nothing to recover, and the writer will create the file a moment
        later.
        """
        path = self._directory / WAL_FILENAME
        if not path.exists():
            return RecoveryReport(
                records_replayed=0,
                end_offset=FILE_HEADER_SIZE,
                stopped_at=None,
                reason=None,
            )

        result = replay(path)
        for record in result.records:
            if record.op is WalOp.PUT:
                self._memtable.put(record.key, record.value)
            elif record.op is WalOp.DELETE:
                self._memtable.delete(record.key)
            else:
                # Unreachable today: the reader rejects any op byte that is not
                # a WalOp before a record gets this far. It raises rather than
                # falling through to the delete branch so that a future op added
                # to the enum has to be given a replay rule here, instead of
                # quietly recovering as a deletion of its key.
                raise WalFormatError(
                    f"WAL record at byte offset {record.offset} carries op {record.op!r}, "
                    "which startup replay has no rule for"
                )

        # A log big enough to recover a memtable past the threshold is a log
        # whose records belong on disk, so the same rule is applied to the
        # replayed table as to a written one, once, after the last record. Not
        # per record: replay is not the write path, and freezing every threshold
        # worth of a large log would build a stack of tables at startup that
        # nothing can flush until the engine is open anyway.
        self._freeze_if_full()

        return RecoveryReport(
            records_replayed=len(result.records),
            end_offset=result.end_offset,
            stopped_at=result.stopped_at,
            reason=result.reason,
        )

    def _freeze_if_full(self) -> None:
        """Freeze and replace the active memtable if it has reached the threshold.

        Call holding the write lock, or from the constructor before any other
        thread can reach this engine. The lock is what story M6.1's atomicity
        criterion comes down to: the write that crossed the threshold and the
        freeze that follows it are one critical section, so no other writer can
        slip a record into the table between the two, and no writer is left
        holding the old table after the swap, since a writer reads
        ``self._memtable`` afresh under the lock every time.

        The last three steps below are ordered for the benefit of :meth:`get`,
        which takes no lock: freeze, then publish the frozen tuple, then swap the
        active table. This module's docstring works through why the reverse
        order would let a reader miss a record that was never lost.

        A threshold met exactly counts as full, matching the story's "meets or
        exceeds". Records are not split across tables, so the size lands on the
        threshold only by coincidence, and treating that coincidence as "not yet"
        would be a rule with no reason behind it.
        """
        if self._memtable.nbytes < self._memtable_threshold_bytes:
            return

        full = self._memtable
        full.freeze()
        self._frozen_memtables = (full, *self._frozen_memtables)
        self._memtable = Memtable()

    def _check_open(self) -> None:
        """Raise if the engine has been closed.

        Writers call this holding the lock, so a write that passes the check
        cannot have the log closed underneath it. :meth:`get` calls it without
        the lock, where it is a courtesy rather than a guarantee: a read racing
        a concurrent close may pass the check and then answer from the memtable,
        which is harmless, because the memtable is memory and stays readable
        whatever the log handle is doing.
        """
        if self._closed:
            raise ValueError("cannot operate on a closed LedgerLog")
