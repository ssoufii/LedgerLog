"""Engine: the write-ahead log and the memtable wired into one key-value store.

Scope of this module today (stories M3.1, M3.2, M6.1, M6.2, M7.1 and M7.2): the
write path, startup recovery, the freeze-and-swap that retires a memtable once it
has grown past a configured size, the background flush that writes a retired
memtable out as an SSTable and releases it from memory, and the read path that
walks all of those layers. A put or a delete is appended to the WAL first and
applied to the memtable second, a get answers from the active memtable, then from
any frozen memtables behind it, then from the SSTables on disk newest first, and
opening an engine over an existing log replays that log into a fresh memtable
before the caller can issue anything. Every acknowledged write is in the log
before any reader can see it, and a restart brings every one of them back.

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

Frozen tables are kept newest-first, and they are released by the flush below and
by nothing else.

Flushing to disk
----------------

A frozen memtable is written out as one SSTable, oldest frozen table first, and
is dropped from memory once that table's footer is on disk. Oldest first because
the tables on disk have to end up in age order for the read path that reads them
(ARCHITECTURE.md section 5, story M7.2): a flush that took the newest frozen
table first would give a younger snapshot a lower sequence number than an older
one, and every layer above would then have to carry an ordering the filenames
contradict.

The drop happens after :meth:`~ledgerlog.sstable.SSTableWriter.finish` returns
and not a moment earlier, because the footer is the commit point
(ARCHITECTURE.md section 6): before it lands, the file at the destination does
not exist yet (the writer builds the table under a temporary name and renames it),
so the only copies of those records are the frozen table and the log. A flush
that dropped first and wrote second would turn any failure in between into lost
reads, and a flush that fails partway leaves the frozen table exactly where it
was, still readable, for the next flush to try again.

By default the flush runs on one background thread, which is what the story asks
for and what keeps a write off it: a writer freezing a table only sets an event,
so the cost a caller pays for a flush is one ``Event.set``, whatever the flush
is doing at the time. The thread takes the flush lock and the write lock but
never holds either while writing the file, so a put issued during a flush waits
for neither the bytes nor the fsync. One thread rather than several, because the
tables have to be produced in the order they were frozen, and two flushers would
race to name them.

``flush_in_background=False`` turns the thread off and leaves the flush to
:meth:`LedgerLog.flush_frozen` and :meth:`LedgerLog.flush_pending`, which do the
same work on the calling thread. That is for a caller that wants to decide when
the I/O happens (a test asserting on a table that is still frozen, a batch load
that would rather flush at the end), and the two paths share one implementation
so the synchronous one is not a second flush with its own rules.

A failing flush does not stop the engine. The frozen table stays, the exception
is recorded in :attr:`LedgerLog.flush_error`, and the background thread goes back
to waiting rather than spinning on a table that just failed; the next freeze
wakes it and it tries again. Writes keep working throughout, because a write does
not depend on a flush having succeeded: its record is already in the log.

A table a flush commits is readable the moment it is registered: the flush
publishes it into the table list before it drops the frozen memtable it was
written from, and the read path below walks that list. So a key does not stop
being readable by being flushed, which is what lets the flush release the frozen
table from memory rather than holding every one of them until the process ends.

What a flush still does not do is trim the log. The records it wrote to disk stay
in the log as well, and replay reads all of them at startup, so a flushed key is
currently recoverable twice over. Trimming is a later milestone, and until then
the cost is a log that only grows.

Tracking the tables on disk
---------------------------

Every table a flush commits is recorded in :attr:`LedgerLog.sstables`, newest
first, as an :class:`SSTableHandle`. Newest first for the same reason the frozen
memtables are: that is the order the read path has to consult them in, since one
key can have an old value in one table and a newer value or a tombstone in
another and the newest record is the one that is true (ARCHITECTURE.md section
5). Keeping the order in the list rather than deriving it at read time means the
ordering is decided once, by the flush that knows which table it just wrote, and
not recomputed from filenames by every caller that walks the list.

A table is added to the list before its frozen memtable is dropped, which is the
same publish-before-retire rule the freeze-and-swap follows and for the same
reason: the frozen table and the SSTable hold the same snapshot, so a reader that
sees both sees one answer twice, while a reader that caught the drop before the
table appeared would see a key in neither. The list is stored under the write
lock and read without one, as one tuple store that a lock-free reader sees whole
or not at all.

What a handle exists to avoid is re-parsing. A flush has the table's footer and
its bloom filter in hand already, so the handle keeps both, and the one
:class:`~ledgerlog.sstable.SSTableReader` it opens for the data block is opened
once and kept, which is what decodes the sparse index once rather than per
lookup. A read path that took a path and opened it per get would pay a footer
parse and an index decode for every one of them, which is exactly the cost the
sparse index exists to amortize.

What the list does not yet hold is tables from a previous run: nothing discovers
those until story M9.1, so a reopened engine starts with an empty list and reads
the old filenames only to avoid reusing one. Nor is the number of readers held
open bounded, which is a question for the point where compaction (M8) starts
producing and retiring tables in bulk.

Reading across the layers
-------------------------

:meth:`LedgerLog.get` walks the layers newest to oldest, exactly as
ARCHITECTURE.md section 5 sets them out: the active memtable, then the frozen
memtables, then the SSTables. It stops at the first layer that holds any record
for the key, a tombstone included, because a tombstone is a record saying the key
was deleted after whatever an older layer still remembers. Falling through one
would resurrect that older value, which is the single failure this ordering
exists to prevent, and it is why the walk is written in terms of records rather
than of values: "deleted here" and "not here" have to stay distinguishable all
the way down (see :meth:`LedgerLog._lookup`).

Each SSTable's bloom filter is consulted before the table is opened. A negative
there is conclusive, so a table that does not hold the key costs a few hash
probes and no I/O, which is what keeps a read whose key is absent from every
table from turning into one data block scan per table. A positive may be wrong,
and the scan that follows simply finds nothing and moves on to the next table.

One ordering detail in the walk is not cosmetic: the table list is read after the
frozen memtables have been walked, never before. A flush publishes its table into
the list first and drops the frozen memtable it came from second, so a list read
at that point holds every table whose memtable the walk may have just missed.
Reading the list first would leave a window in which a flush that ran in between
had already dropped the memtable and published a table that the earlier snapshot
predates, and a key in neither of the two snapshots would read as absent while
being on disk the whole time.

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
* Each :class:`SSTableHandle` carries a lock of its own, guarding the one reader
  it lends out, because a reader owns its file position and two lookups on one
  reader would pull each other's cursor (see ``sstable.py``). It is a leaf lock,
  taken by nothing else and taking nothing, so it cannot be part of a cycle.
* A flush takes a second lock, the flush lock, for as long as it takes to pick a
  frozen table, write it and drop it. That lock is what makes "one flusher at a
  time" true rather than hoped for: the background thread and a caller invoking
  :meth:`LedgerLog.flush_frozen` by hand would otherwise both pick the oldest
  frozen table, write it to two files under two sequence numbers, and race to
  drop it, with one of them finding it already gone. No writer and no reader ever
  takes it, so a flush holding it across a whole file write blocks neither.
* :meth:`LedgerLog.close` takes a third lock of its own, so that a second thread
  closing the same engine waits for the first to finish rather than returning as
  soon as it sees the closed flag.

Lock ordering is fixed and one way, the close lock then the flush lock then the
engine lock then the WAL writer's or the memtable's, so there is no cycle for two
threads to deadlock around. The one place that has to be deliberate about it is
:meth:`LedgerLog.close`, which marks the engine closed under the engine lock and
then releases it before joining the flush thread: joining while holding it would
wait for a thread whose last act is to take it.

What is deliberately not here yet:

* Discovering the tables a previous run flushed (story M9.1). A reopened engine
  reads the existing filenames only to avoid reusing one, and nothing yet opens
  those files or judges whether they are complete.
* Trimming the log once the records it holds are on disk. Replay still reads it
  in full at startup, which is why a flush cannot lose data but does leave the
  same records in two places.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType

from ledgerlog.bloom import BloomFilter
from ledgerlog.memtable import Memtable, MemtableEntry
from ledgerlog.sstable import (
    SSTableFooter,
    SSTableLayout,
    SSTableReader,
    SSTableRecord,
    write_sstable,
)
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

SSTABLE_FILENAME_PREFIX = "sstable-"
"""Fixed start of the name of every SSTable an engine flushes."""

SSTABLE_FILENAME_SUFFIX = ".sst"
"""Fixed end of the name of every SSTable an engine flushes."""

SSTABLE_SEQUENCE_DIGITS = 10
"""Width of the zero-padded sequence number inside an SSTable's filename.

Padded to a fixed width, rather than written as the shortest form of the number,
so that sorting the names as text sorts the tables by age. Startup discovery
(story M9.1) and any operator listing the directory both see files in whatever
order the name comparison gives them, and an unpadded ``sstable-10`` sorting
before ``sstable-9`` would put that order at odds with the order the tables were
written in, which is the order the read path has to consult them in.

Ten digits, which a flush cannot exhaust: at the default threshold that is more
tables than four exabytes of flushed data would produce. The name is refused
rather than widened past it, since an eleven digit name sorts before every ten
digit one and would break the property the padding exists for.
"""


def sstable_filename(sequence: int) -> str:
    """Return the name of the SSTable carrying this sequence number."""
    if sequence < 0:
        raise ValueError(f"SSTable sequence number must not be negative, got {sequence}")
    if sequence >= 10**SSTABLE_SEQUENCE_DIGITS:
        raise ValueError(
            f"SSTable sequence number {sequence} does not fit in {SSTABLE_SEQUENCE_DIGITS} digits"
        )
    return (
        f"{SSTABLE_FILENAME_PREFIX}{sequence:0{SSTABLE_SEQUENCE_DIGITS}d}{SSTABLE_FILENAME_SUFFIX}"
    )


def parse_sstable_sequence(name: str) -> int | None:
    """Return the sequence number in an SSTable filename, or ``None`` if it is not one.

    ``None`` rather than an exception, because the caller is sifting a directory
    that legitimately holds files this function does not name: the log, the
    temporary file a flush in progress is writing under a leading dot, and
    whatever an operator has left there. Only a name this module could have
    produced is given a number.

    The digits are checked for being ASCII as well as for being digits, since
    :meth:`str.isdigit` is true of numerals from other scripts that ``int`` also
    accepts, and a file named with those would come back with a sequence number
    its own name does not sort by.
    """
    if not name.startswith(SSTABLE_FILENAME_PREFIX) or not name.endswith(SSTABLE_FILENAME_SUFFIX):
        return None
    digits = name[len(SSTABLE_FILENAME_PREFIX) : len(name) - len(SSTABLE_FILENAME_SUFFIX)]
    if len(digits) != SSTABLE_SEQUENCE_DIGITS:
        return None
    if not (digits.isascii() and digits.isdigit()):
        return None
    return int(digits)


class SSTableHandle:
    """One committed SSTable, with what the read path needs about it kept resident.

    An entry in :attr:`LedgerLog.sstables`. It exists so that consulting a table
    costs what reading it costs and nothing more: the footer and the bloom filter
    are held from the moment the table was written, and the reader behind them is
    opened once and reused, which decodes the sparse index once. A read path built
    on paths instead would parse a footer and decode an index on every lookup,
    which for a get that the bloom filter ends up rejecting is all of the work and
    none of the answer.

    The footer and the filter come from the flush that produced the table rather
    than from the file, because the writer had already built both on the way past
    (see :class:`~ledgerlog.sstable.SSTableLayout`). Reading them back off the disk
    to populate a handle would be decoding bytes this process encoded moments
    earlier. Discovering a table a previous run left behind is story M9.1, and that
    is the path where a handle will have to be built by parsing, which is why the
    footer is kept as a footer rather than as the layout it came from: a discovered
    table can supply a footer and has no layout.

    The reader, unlike the other two, is opened lazily on the first borrow.
    Nothing reads a table's data block until story M7.2, and a flush that opened
    one eagerly would hold a descriptor per table for tables nothing has asked
    about. The index is left to the reader rather than kept here as well, so that
    a table being read does not hold two copies of it.

    Lending the reader through :meth:`borrow_reader` rather than exposing it is
    what keeps that sharing safe. A reader owns its stream's position, so two
    lookups running at once on one reader would each move the other's cursor and
    both could return a record from the wrong offset (see ``sstable.py``). The
    borrow serializes them on this handle's lock, which is per table: a lookup in
    one table never waits for a lookup in another, which is what keeps the
    newest-first walk of many tables from turning into one queue.
    """

    def __init__(self, sequence: int, layout: SSTableLayout) -> None:
        """Build the handle for the table ``layout`` describes.

        ``sequence`` is the number in the table's filename, kept separately
        because it is what orders tables by age and a layout carries the path but
        not the number inside it. It is taken as given rather than parsed back out
        of the name: the flush allocated it, and re-deriving it here would mean
        two places deciding what a table's age is.
        """
        if sequence < 0:
            raise ValueError(f"SSTable sequence number must not be negative, got {sequence}")
        self._sequence = sequence
        self._path = layout.path
        self._footer = layout.footer
        self._bloom_filter = layout.bloom_filter
        self._lock = threading.Lock()
        self._reader: SSTableReader | None = None
        self._closed = False

    @property
    def sequence(self) -> int:
        """Sequence number from this table's filename, which is its age."""
        return self._sequence

    @property
    def path(self) -> Path:
        """Path of the table on disk."""
        return self._path

    @property
    def footer(self) -> SSTableFooter:
        """The footer the table was committed with, held rather than re-read."""
        return self._footer

    @property
    def bloom_filter(self) -> BloomFilter:
        """The table's bloom filter, as the writer built it.

        This is the first thing a read consults and the one that decides whether
        the table is opened at all (ARCHITECTURE.md section 5), so it is the piece
        that most has to be free of a parse: a get for a key no table holds should
        cost a few hash probes per table and no I/O.
        """
        return self._bloom_filter

    @property
    def record_count(self) -> int:
        """Number of records in the table, as its footer reports it."""
        return self._footer.record_count

    @property
    def closed(self) -> bool:
        """True once :meth:`close` has released this handle's reader."""
        return self._closed

    @contextmanager
    def borrow_reader(self) -> Iterator[SSTableReader]:
        """Yield this table's reader, exclusively, for the duration of the block.

        The reader is opened on the first borrow and kept for every borrow after
        it, so the footer is parsed and the index decoded once per handle rather
        than once per lookup, which is the point of the class.

        Exclusive because a reader is a cursor: two callers using one at the same
        time would interleave seeks. The lock is released on every exit, including
        an exception thrown by the caller's block, which is what the context
        manager form is for.

        A reader that fails to open leaves the handle without one rather than
        holding a broken object, so a later borrow tries again. A file that has
        been deleted or damaged under a running engine is the case that reaches
        here, and it is the caller's to report, not this class's to paper over.

        The lock is not reentrant, so a block must not close this handle, or the
        engine holding it, from inside the borrow. Nothing in the read path does:
        a borrow is one lookup long.
        """
        with self._lock:
            if self._closed:
                raise ValueError(f"cannot read from a closed SSTableHandle for {self._path}")
            if self._reader is None:
                self._reader = SSTableReader.open(self._path)
            yield self._reader

    def close(self) -> None:
        """Release the reader this handle holds. Safe to call more than once.

        Called when the engine closes, so that an engine that flushed many tables
        does not leave a descriptor per table behind it. The handle stays in the
        engine's list, closed, because the list says which tables this engine
        wrote and that stays true after the engine is shut.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            reader = self._reader
            self._reader = None
        if reader is not None:
            # Outside the lock, because closing a file can block and nothing else
            # can reach this reader once it has been unhooked above.
            reader.close()

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(sequence={self._sequence}, path={str(self._path)!r}, "
            f"record_count={self.record_count})"
        )


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
        flush_in_background: bool = True,
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

        ``flush_in_background`` starts the thread that writes frozen memtables out
        as SSTables, which is the default because a caller should not have to run
        a flush loop of their own to keep memory bounded. Turning it off leaves
        every flush to :meth:`flush_frozen` and :meth:`flush_pending` on the
        caller's thread, and an engine that neither runs the thread nor calls
        those accumulates frozen memtables for as long as it stays open.

        The thread is started last, after the log is open, so it cannot find a
        half-built engine: replay may already have frozen a table, and a flusher
        running before :attr:`_wal` exists would be a flush racing the
        constructor that created its work.
        """
        if memtable_threshold_bytes < 1:
            raise ValueError(
                f"memtable_threshold_bytes must be at least 1, got {memtable_threshold_bytes}"
            )

        self._directory = Path(directory)
        self._directory.mkdir(parents=True, exist_ok=True)

        self._write_lock = threading.Lock()
        # Shares the write lock rather than carrying one of its own, because what
        # it reports is a change to the frozen tuple and that tuple is only ever
        # stored under the write lock. A second lock would mean a window between
        # the store and the notification for a waiter to miss.
        self._frozen_changed = threading.Condition(self._write_lock)
        self._closed = False
        self._memtable_threshold_bytes = memtable_threshold_bytes
        self._memtable = Memtable()
        self._frozen_memtables: tuple[Memtable, ...] = ()
        # Newest first, and empty for a reopened engine until story M9.1 teaches
        # startup to discover the tables previous runs committed.
        self._sstables: tuple[SSTableHandle, ...] = ()

        # Ordered above the write lock and taken by nothing else, so a close can
        # hold it across the whole shutdown, including the join of a thread whose
        # last act is to take the write lock.
        self._close_lock = threading.Lock()
        self._flush_lock = threading.Lock()
        self._flush_wakeup = threading.Event()
        self._flush_stopping = False
        self._flush_count = 0
        self._flush_error: BaseException | None = None
        self._next_sstable_sequence = self._next_free_sstable_sequence()
        self._flush_in_background = flush_in_background
        self._flusher: threading.Thread | None = None

        self._recovery = self._replay_existing_log()
        self._wal = WalWriter(
            self._directory / WAL_FILENAME,
            fsync_policy=fsync_policy,
            fsync_interval_seconds=fsync_interval_seconds,
        )
        if flush_in_background:
            # A daemon thread so that a caller who forgets to close an engine
            # does not leave a process that will not exit. Nothing is lost by it:
            # a flush killed at interpreter exit leaves its temporary file
            # behind and its records in the log, which is the same state a crash
            # mid-flush leaves, and the one recovery already has to handle.
            self._flusher = threading.Thread(
                target=self._flush_loop,
                name=f"ledgerlog-flush-{self._directory.name}",
                daemon=True,
            )
            self._flusher.start()

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
    def sstables(self) -> tuple[SSTableHandle, ...]:
        """Tables this engine has flushed, newest first.

        The order a read has to consult them in (ARCHITECTURE.md section 5): a key
        can appear in several tables, as an old value, a newer value or a
        tombstone, and the first table in this list that holds it holds the record
        that is true. Sequence numbers therefore descend along it.

        Only tables this engine flushed are here. Ones an earlier run committed
        are on disk and not in this list until startup discovery (story M9.1).

        A tuple rather than a list, for the reason :attr:`frozen_memtables` is
        one: a caller cannot change what the engine will read next, and the engine
        publishes a new set with one store that a lock-free reader sees whole or
        not at all.
        """
        return self._sstables

    @property
    def flush_in_background(self) -> bool:
        """True if this engine runs its own thread to flush frozen memtables."""
        return self._flush_in_background

    @property
    def flush_count(self) -> int:
        """Number of SSTables this engine has flushed since it was opened.

        Counted per completed flush rather than per file found in the directory,
        so it says what this engine has done and not what previous runs left
        behind. A flush that failed is not counted, since no table was committed.
        """
        return self._flush_count

    @property
    def flush_error(self) -> BaseException | None:
        """The exception from the most recent failed flush, or ``None``.

        Kept because a background flush has nowhere to raise: the thread that
        would have received the exception is the engine's own. Reporting it here
        means a caller watching an engine whose memory is not coming down can find
        out why, rather than seeing only that frozen tables are accumulating.

        Not cleared by a later success, and not an engine-level failure: a flush
        that fails has committed nothing and lost nothing, because the records are
        still in the frozen table and in the log.
        """
        return self._flush_error

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

        The active memtable is consulted first, the frozen ones after it and the
        SSTables on disk after those, each newest first, which is
        ARCHITECTURE.md section 5's ordering. The search stops at the first layer
        holding any record for the key, including a tombstone: a tombstone means
        the key was deleted after whatever an older layer still remembers, so
        falling through it would resurrect that older value.

        A table whose bloom filter rejects the key is not opened at all, so a key
        no table holds costs hash probes rather than a scan per table.

        A deleted key therefore reads as ``None``, the same as a key that was
        never written. The two are different facts internally and the difference
        is what the paragraph above turns on, but to a caller asking for a value
        they come to the same thing.

        A key put with an empty value reads back as ``b""``, which is not
        ``None``. The distinction between an empty value and a deletion is
        carried all the way down into the log's record format, and it survives
        here.

        Takes no lock, and reads each layer in the reverse of the order a freeze
        or a flush publishes to it: the active table before the frozen ones, and
        the frozen ones before the table list. See this module's docstring for
        why that is what keeps a concurrent freeze or flush from hiding a record
        from this call.

        Raises :class:`ValueError` if the engine has been closed, and can raise
        the same from a table handle if another thread closes the engine while
        this call is mid-walk. That is a caller racing its own :meth:`close`,
        and it is reported rather than swallowed: the alternative, treating a
        closed table as one that does not hold the key, would answer "not found"
        for a key that is on disk.
        """
        self._check_open()

        entry = self._lookup(key)
        if entry is None or entry.is_tombstone:
            return None
        return entry.value

    def _lookup(self, key: bytes) -> MemtableEntry | SSTableRecord | None:
        """Return the newest record held for ``key``, or ``None`` if there is none.

        Separate from :meth:`get` because the layers have to be walked with the
        distinction between "deleted here" and "not here" intact, and
        :meth:`Memtable.get` has already thrown it away. Collapsing the two into
        one loop would mean a tombstone in one layer reading the same as no
        record at all, and the search moving on to an older layer that still
        holds the deleted value.

        The two record types are handed back as they come, a
        :class:`~ledgerlog.memtable.MemtableEntry` from memory and an
        :class:`~ledgerlog.sstable.SSTableRecord` from disk, rather than being
        converted into one type. They already agree about the two things this
        engine asks a record, its ``value`` and whether it ``is_tombstone``, so
        converting would allocate a second object per read to restate what the
        first one said.
        """
        entry = self._memtable.lookup(key)
        if entry is not None:
            return entry

        for table in self._frozen_memtables:
            entry = table.lookup(key)
            if entry is not None:
                return entry

        # Read after the frozen memtables above, never before them. A flush
        # publishes its table into this list and only then drops the frozen
        # memtable it was written from, so a snapshot taken at this point holds
        # every table whose memtable the walk above may have missed. See this
        # module's docstring for the window the other order would open.
        for handle in self._sstables:
            # A bloom negative is conclusive, so a table that does not hold the
            # key is skipped without opening its data block at all. A positive
            # may be wrong, and the lookup below then finds nothing and the walk
            # carries on to the next table.
            if not handle.bloom_filter.might_contain(key):
                continue
            with handle.borrow_reader() as reader:
                record = reader.lookup(key)
            if record is not None:
                return record

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
            self._frozen_changed.notify_all()

    def flush_frozen(self) -> SSTableLayout | None:
        """Write the oldest frozen memtable out as an SSTable, and drop it.

        Returns the layout of the table that was written, or ``None`` if there was
        no frozen memtable to flush. Oldest first, and the drop only after the
        footer is on disk: this module's docstring works through why both of those
        are the order rather than a preference.

        Runs on the calling thread and takes the flush lock for the whole of it,
        so calling this on an engine whose background thread is running is safe
        but will wait for a flush already in progress. Writes are not waited on
        and do not wait: the write lock is taken only to pick the table and to
        drop it, never while the file is being written.

        Raises whatever the SSTable write raises, and leaves the frozen memtable
        in place when it does. A caller flushing by hand gets the exception where
        it can act on it, rather than in :attr:`flush_error` where the background
        thread has to leave it.
        """
        self._check_open()
        with self._flush_lock:
            return self._flush_oldest_frozen()

    def flush_pending(self) -> tuple[SSTableLayout, ...]:
        """Flush every frozen memtable, oldest first, and return the tables written.

        Returns an empty tuple when there was nothing frozen. What counts as
        "every" is decided as it goes: a table frozen by another thread while this
        is working is flushed too, and the loop ends the first time it finds
        nothing frozen, so this returns rather than following a workload that is
        still writing.
        """
        self._check_open()
        written: list[SSTableLayout] = []
        while True:
            with self._flush_lock:
                layout = self._flush_oldest_frozen()
            if layout is None:
                return tuple(written)
            written.append(layout)

    def wait_for_flush(self, timeout: float | None = None) -> bool:
        """Block until no frozen memtable is left, and report whether that happened.

        For a caller that wants the background flush to have caught up: after a
        batch of writes, before measuring memory, or in a test that has to know
        the table is on disk. ``True`` means nothing is frozen, ``False`` means the
        timeout expired first.

        Waiting on the condition releases the write lock, so a waiter blocks no
        writer and no flusher. A flush that keeps failing therefore reads as a
        timeout rather than as an error raised here, since the frozen table never
        goes away: :attr:`flush_error` is where the reason is, and raising a
        previous flush's exception out of a wait would report it to whoever
        happened to be waiting rather than to whoever asked for the flush.
        """
        with self._frozen_changed:
            return self._frozen_changed.wait_for(lambda: not self._frozen_memtables, timeout)

    def close(self) -> None:
        """Close the log and stop accepting operations. Safe to call more than once.

        Marking the engine closed takes the write lock, so a close cannot land
        between a write's log append and its memtable update and leave the two
        disagreeing. A write already in flight finishes first, because it holds
        that lock, and a write that arrives afterwards finds the engine closed and
        is refused before it touches the log.

        The flush thread is then stopped, and the log is closed after it, both
        outside the write lock. Outside because the flush thread's last act can be to
        take the write lock (a flush ends by dropping the table it wrote), so
        joining it while holding that lock would deadlock. Closing the log outside
        it is safe for the reason above: by that point no write can start. A flush
        already in progress is allowed to finish rather than abandoned, since it
        is about to produce a table whose records would otherwise have to be
        replayed from the log again.

        Frozen memtables that were never flushed are simply released with the
        engine. Nothing is lost by that: the log is not trimmed, so the next open
        replays every record they held.

        The tracked tables' readers are closed last, after the log, so an engine
        that flushed many tables leaves no descriptor behind. The handles stay in
        :attr:`sstables`, closed: the list records which tables this engine wrote,
        and the files are still on disk for the next open to find.

        A second lock serializes closes so that the whole of this runs once, rather
        than the closed flag alone. Without it, a second thread calling close would
        see the flag and return while the first was still joining the flusher and
        fsyncing the log, which would tell it the engine was closed before the log
        was, and would hide a failing final fsync from it.

        The engine is marked closed even if closing the log raises, since the
        handle is gone either way, and the exception is then re-raised rather
        than swallowed: a failure here is a failed final fsync, which means the
        log may be missing its last records, and a caller told the close
        succeeded would assume a durability the disk never confirmed.
        """
        with self._close_lock:
            with self._write_lock:
                if self._closed:
                    return
                self._closed = True
            self._stop_flusher()
            try:
                self._wal.close()
            finally:
                # In a finally so that a failing final fsync still releases the
                # table readers: the exception is the caller's to hear about, and
                # leaking a descriptor per flushed table on the way to raising it
                # would help nobody. A reader that then also refuses to close
                # raises in its place, with the log's failure chained to it as the
                # context, so neither of the two is lost.
                self._close_sstables()

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
        #
        # Under the write lock, although no other thread can reach this engine
        # yet, because the freeze notifies the condition that lock guards and a
        # notification without it held is an error rather than a no-op. Taking it
        # here keeps one rule ("a freeze happens under the write lock") instead of
        # an exception for the one caller that happens to be alone.
        with self._write_lock:
            self._freeze_if_full()

        return RecoveryReport(
            records_replayed=len(result.records),
            end_offset=result.end_offset,
            stopped_at=result.stopped_at,
            reason=result.reason,
        )

    def _freeze_if_full(self) -> None:
        """Freeze and replace the active memtable if it has reached the threshold.

        Call holding the write lock, which every caller does, the constructor's
        replay included. The lock is what story M6.1's atomicity criterion comes
        down to: the write that crossed the threshold and the
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
        self._frozen_changed.notify_all()
        # One store, and the whole of what a write pays for a flush. The flush
        # thread does the file, the fsync and the drop; the writer that filled the
        # table only says that there is now something to do, which is what
        # "writes are never blocked on flush" comes down to in one line.
        self._flush_wakeup.set()

    def _next_free_sstable_sequence(self) -> int:
        """Return the sequence number the next table flushed here should carry.

        One past the highest already in the directory, so a reopened engine cannot
        write over a table an earlier run flushed. The files are judged by name
        only: whether each one is a complete table is startup discovery's question
        (story M9.1), and a damaged or half-written table still owns its name until
        something deletes it, so counting it here is exactly what keeps the next
        flush from landing on top of it.
        """
        highest = -1
        for entry in self._directory.iterdir():
            sequence = parse_sstable_sequence(entry.name)
            if sequence is not None and sequence > highest:
                highest = sequence
        return highest + 1

    def _flush_loop(self) -> None:
        """Flush frozen memtables as they appear, until the engine is closed.

        Waits on an event rather than polling on a timer, so an idle engine costs
        nothing and a freeze is acted on immediately. The event is cleared before
        the tables are drained, not after, so a freeze that happens while this is
        working sets it again and the next wait returns at once instead of the
        wakeup being swallowed.

        A failed flush is recorded and then waited out rather than retried on the
        spot. Retrying immediately would spin on a table that just failed, most
        likely for a reason that has not changed (a full disk, a directory that is
        no longer writable), and burn a core for as long as it lasts. The next
        freeze is the retry.
        """
        while True:
            self._flush_wakeup.wait()
            self._flush_wakeup.clear()
            if self._flush_stopping:
                return
            try:
                while not self._flush_stopping:
                    with self._flush_lock:
                        if self._flush_oldest_frozen() is None:
                            break
            except BaseException as error:
                # Broad on purpose, and not silent: this is a thread boundary, so
                # an exception that escaped here would be printed to stderr by the
                # interpreter and take the flusher down with it, leaving an engine
                # that quietly stops flushing forever. It is recorded where a
                # caller can find it instead.
                self._record_flush_error(error)

    def _flush_oldest_frozen(self) -> SSTableLayout | None:
        """Flush the oldest frozen memtable and drop it, or return ``None`` if there is none.

        Call holding the flush lock, which is what makes the pick, the write and
        the drop one unit: two flushers without it would pick the same table and
        write it twice under two names.

        The sequence number is allocated before the write and is not returned to
        the pool if the write fails. A retry therefore writes the next number
        rather than the failed one, which costs nothing (the numbers only have to
        ascend, not to be contiguous) and avoids reusing a name whose failed
        attempt may have left a file behind.
        """
        frozen = self._frozen_memtables
        if not frozen:
            return None
        # The tuple is newest first, so the oldest frozen table is the last one.
        memtable = frozen[-1]

        sequence = self._next_sstable_sequence
        self._next_sstable_sequence = sequence + 1
        layout = self._write_sstable(memtable, self._directory / sstable_filename(sequence))

        # Published before the memtable it came from is dropped, never after. The
        # two hold the same records, so a reader that sees both sees one answer
        # twice, while the other order leaves a window where the records are in
        # neither the frozen tables nor the table list.
        self._register_sstable(sequence, layout)

        # After the write returns, which is after the footer landed and the file
        # was renamed into place. Until then this table is the only copy of its
        # records that is not in the log.
        self.drop_frozen(memtable)
        self._flush_count += 1
        return layout

    def _register_sstable(self, sequence: int, layout: SSTableLayout) -> SSTableHandle:
        """Record a table this engine just committed at the front of the list.

        Under the write lock, so that the store the lock-free :attr:`sstables`
        reader sees is the one publication of a finished tuple, and so that the
        ordering check below is made against the list the flush is actually
        prepending to rather than against a snapshot of it.

        The sequence number is required to be higher than the newest table's, and
        the check earns its place: everything above this list treats position as
        age, so a table prepended out of order would not read as an ordering bug,
        it would read as correct data. There is one flusher and it takes tables in
        freeze order, which is what makes the invariant hold; asserting it here is
        what would catch a second flusher or a reused sequence number, at the
        point where the mistake is still one table rather than a wrong answer.
        """
        handle = SSTableHandle(sequence, layout)
        with self._write_lock:
            newest = self._sstables[0] if self._sstables else None
            if newest is not None and sequence <= newest.sequence:
                raise ValueError(
                    f"SSTable {layout.path.name} carries sequence number {sequence}, which does "
                    f"not follow the newest tracked table's {newest.sequence}, so the list would "
                    "no longer be ordered newest first"
                )
            self._sstables = (handle, *self._sstables)
        return handle

    def _close_sstables(self) -> None:
        """Release every tracked table's reader. Call without the write lock.

        Every handle is closed even if one of them raises, and the first exception
        is re-raised afterwards, because the point of this is to leave no
        descriptor behind and stopping at the first failure would leak the rest.
        The handles stay in the list: it is a record of what this engine flushed,
        which does not stop being true when the engine closes.
        """
        first_error: BaseException | None = None
        for handle in self._sstables:
            try:
                handle.close()
            except BaseException as error:
                if first_error is None:
                    first_error = error
        if first_error is not None:
            raise first_error

    def _write_sstable(self, memtable: Memtable, path: Path) -> SSTableLayout:
        """Write one frozen memtable to ``path`` as a complete SSTable.

        Every record goes in, tombstones included, because a tombstone is this
        table's answer for its key and dropping it would let an older table's value
        for that key resurface (ARCHITECTURE.md section 3). Compaction is where
        tombstones stop being written, and only once no older table can still
        answer (story M8.3).

        The entries stream straight from the memtable into the writer rather than
        being collected first: they are already in ascending key order, which is
        the order the data block needs, and materializing them would double the
        memory the flush exists to release.

        ``expected_keys`` is the memtable's record count, which lets the writer
        size the bloom filter up front and hash each key as it passes instead of
        holding every key until the end. A frozen memtable is one of the few
        callers that knows this number exactly.

        This is the one place a table is written, and it is a method rather than a
        call inline above so that the whole flush path can be exercised against a
        write that blocks or fails.
        """
        return write_sstable(
            path,
            ((entry.key, entry.value) for entry in memtable.entries()),
            expected_keys=len(memtable),
        )

    def _record_flush_error(self, error: BaseException) -> None:
        """Store a failed flush's exception where a caller can find it."""
        with self._write_lock:
            self._flush_error = error

    def _stop_flusher(self) -> None:
        """Ask the flush thread to stop and wait for it. Call without the write lock.

        Sets the stop flag before the wakeup so the thread cannot wait again after
        seeing it, and joins without a timeout: the only unbounded thing a flush
        does is write a table, and a close that returned while a flush was still
        writing would hand back an engine whose directory is still changing.
        """
        self._flush_stopping = True
        self._flush_wakeup.set()
        flusher = self._flusher
        if flusher is not None:
            flusher.join()
            self._flusher = None

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
