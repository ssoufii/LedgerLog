"""Engine: the write-ahead log and the memtable wired into one key-value store.

Scope of this module today (story M3.1): the write path. A put or a delete is
appended to the WAL first and applied to the memtable second, and a get answers
from the memtable. That is the whole engine for now, and it is already a usable
store: every acknowledged write is in the log before any reader can see it.

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

Lock ordering is fixed and one way, the engine lock then the WAL writer's, so
there is no cycle for two threads to deadlock around.

What is deliberately not here yet:

* Startup replay (story M3.2). Opening an engine over an existing WAL appends
  to that log without reading it back, so writes from a previous run are on
  disk but not in the new memtable, and a get will not find them. Until M3.2
  lands, this engine is durable in the sense that the log holds every
  acknowledged write, not in the sense that a restart recovers them.
* Flushing the memtable to an SSTable and everything downstream of it
  (milestones 4 and up), so the memtable grows without bound and every read is
  answered from memory.
"""

from __future__ import annotations

import os
import threading
from pathlib import Path
from types import TracebackType

from ledgerlog.memtable import Memtable
from ledgerlog.wal import (
    DEFAULT_FSYNC_INTERVAL_SECONDS,
    FsyncPolicy,
    WalWriter,
)

WAL_FILENAME = "wal.log"
"""Name of the write-ahead log inside an engine's directory.

A fixed name rather than a caller-supplied path: the directory is the unit an
engine owns, and from milestone 4 on it holds SSTables that have to be
discovered by name alongside this file. Letting the log live anywhere would mean
a data directory that is only complete if the caller remembers to point at the
matching log.
"""


class LedgerLog:
    """A durable key-value store over a write-ahead log and an in-memory table.

    Keys and values are ``bytes``. Every :meth:`put` and :meth:`delete` is
    appended to the log before it is applied to the memtable, so a write is on
    disk before any reader can observe it. See this module's docstring for why
    that ordering is the guarantee the class exists to provide, and for what a
    restart does not yet recover.

    Safe to use from several threads: writes serialize on one lock and reads
    take none.
    """

    def __init__(
        self,
        directory: str | os.PathLike[str],
        *,
        fsync_policy: FsyncPolicy | str = FsyncPolicy.ALWAYS,
        fsync_interval_seconds: float = DEFAULT_FSYNC_INTERVAL_SECONDS,
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
        """
        self._directory = Path(directory)
        self._directory.mkdir(parents=True, exist_ok=True)

        self._write_lock = threading.Lock()
        self._closed = False
        self._memtable = Memtable()
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
        """
        with self._write_lock:
            self._check_open()
            self._wal.append_put(key, value)
            self._memtable.put(key, value)

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

    def get(self, key: bytes) -> bytes | None:
        """Return the value stored under ``key``, or ``None`` if there is none.

        A deleted key reads as ``None``, the same as a key that was never
        written. The memtable keeps the two apart internally, and the read path
        will need that distinction once there are older layers to fall through
        to (story M7.2), but with the memtable as the only layer both honestly
        mean "this engine has no value for that key".

        A key put with an empty value reads back as ``b""``, which is not
        ``None``. The distinction between an empty value and a deletion is
        carried all the way down into the log's record format, and it survives
        here.
        """
        self._check_open()
        return self._memtable.get(key)

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
