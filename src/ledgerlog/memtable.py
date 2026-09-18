"""Memtable: the in-memory sorted structure that holds recent writes.

Scope of this module today (stories M2.1 and M2.2): the skip list itself, as a
plain sorted container with insert, search, delete and in-order iteration, plus
the key-value layer above it that turns a delete into a tombstone. Keys are
compared as bytes, so iteration order is the byte-lexicographic order that
SSTables are written in later, and a flush can walk this structure front to back
without re-sorting anything.

Two classes, in two layers. :class:`SkipList` is a sorted container and nothing
more: its :meth:`SkipList.delete` unlinks the node, which is the ordinary
data-structure operation. :class:`Memtable` is the key-value view the engine
actually writes through, and its :meth:`Memtable.delete` leaves a tombstone
record in place of the value instead of removing anything. Keeping the two apart
is what the layering in ARCHITECTURE.md section 3 asks for: physical removal is
compaction's job, once no older SSTable can still answer for the key, so nothing
below compaction is allowed to make a key simply disappear.

How a tombstone is represented: :class:`Memtable` stores every value in the skip
list behind a one byte tag, ``\\x01`` for a put and ``\\x02`` for a delete, and a
tombstone is that tag byte on its own. The tag is what makes the distinction
unambiguous, and the alternatives are worse rather than merely different. A
reserved sentinel value cannot work, because a put of ``b""`` is a real write
that has to stay distinguishable from a delete of the same key, exactly as it is
in the WAL. A sentinel object recognized by identity would work but rests on the
identity of a bytes object rather than on anything written down. A parallel set
of deleted keys would keep two structures that have to agree, and a flush walking
one of them would be trusting that the other did not drift.

What the tag costs, stated plainly: a put concatenates the tag onto the value,
and a read slices it back off, so a value is copied once on the way in and once
on the way out. That is the price of keeping the skip list a ``bytes`` to
``bytes`` container. Avoiding it means letting the skip list hold some other
value type, which would move the tombstone concept down into the container that
ARCHITECTURE.md section 3 wants kept clear of it. Correctness of the layering
is worth more here than the copy, and if the copy ever shows up in the M10
benchmarks it can be removed without changing what any caller sees.

Why the tag carries no format version, unlike every on-disk structure in this
engine: it is never written to disk. A flush re-encodes these records into the
SSTable format, which carries its own version byte, so the tag lives and dies
inside one process and there is no future reader to keep compatible with it.

What is deliberately not here yet:

* Thread safety (story M2.4). Neither :class:`SkipList` nor :class:`Memtable`
  carries a lock yet, so neither *makes* a concurrency guarantee today, and
  callers sharing an instance between threads must still synchronize
  externally. The strategy those locks will follow is decided, and is written
  down under "Concurrency strategy" below: story M2.3 chose it, and M2.4 adds
  the lock and the multi-threaded tests that turn it into a promise.
* The size accounting that decides when a memtable is full enough to freeze and
  flush (story M6.1). :func:`len` counts records here, which is not the byte
  measure the flush threshold is expressed in.

Why a skip list rather than a balanced tree: per ARCHITECTURE.md section 2, a
skip list reaches O(log n) expected search and insert without rebalancing. An
insert only relinks the handful of nodes immediately around the new key, so
there is no subtree rotation that a concurrent reader could be walking through
while it happens. That is what makes the concurrency work in M2.4 tractable, and
it is the reason the structure is hand-built here rather than delegated to a
sorted-container library (see CLAUDE.md's ground rules).

How the structure works: every key lives in a node on level 0, so level 0 is an
ordinary sorted linked list and in-order iteration is a walk along it. Each node
is additionally linked into some number of higher levels, chosen at random when
the node is created, and each level skips over more of the list than the one
below it. A search starts at the highest level in use and drops down a level
whenever the next node on the current level would overshoot the target, so the
upper levels act as an express lane that gets the search near the key in O(log n)
steps instead of scanning.

Why the level count is random rather than maintained exactly: keeping an exact
distribution of levels would require rebalancing on every insert and delete,
which is the cost the skip list exists to avoid. Drawing each node's height from
a geometric distribution gives the same expected O(log n) shape on average,
without any node ever needing to be rewritten because of an insert elsewhere.

Concurrency strategy (the story M2.3 decision)
----------------------------------------------

ARCHITECTURE.md section 2 asks that whichever approach is implemented be written
down here, because this is the part of the engine most likely to hold a subtle
bug. The approach chosen is: **one lock around the whole mutating path, and no
lock at all on the read path**. Readers walk the structure while a writer is
linking into it, and what makes that safe is the order in which the writer
publishes its stores, not readers excluding the writer.

Locking granularity, insert versus search:

* Insert, update and physical delete take a single per-instance
  ``threading.Lock`` for the whole call, from the predecessor search
  through the last pointer store, and release it on the way out. The
  granularity is the entire structure. Writers therefore never overlap each
  other, which is exactly the "single writer at a time" that section 2
  specifies, and the predecessor list a writer computed cannot go stale under
  it, since nothing else can relink anything while it holds the lock.
* Search, lookup, iteration and :func:`len` take nothing at all. The
  granularity is zero locks and zero atomic operations. A reader's cost does
  not depend on whether a write is in flight, and a reader cannot deadlock
  against a writer because it holds nothing to deadlock with.

Why not per-node or hand-over-hand locking, the other candidate section 2 names:
per-node locks exist so that two writers can work on disjoint stretches of the
list at once. This design has one writer, so that is concurrency the engine
never uses, and the price is real: a lock object per node (against a node count
whose memory footprint is what decides how often a flush happens), an acquire
and a release per level on the hot insert path, and a lock ordering that has to
be argued deadlock free. None of it helps readers, which is what this milestone
is actually about, since readers take no lock under either scheme.

Why not a genuinely lock-free scheme: linking a node without a lock needs an
atomic compare-and-swap over an object reference, and CPython exposes no such
primitive. Simulating one with a lock is the design above with extra steps.

What the lock-free read side rests on, listed so that M2.4 can test each point
and a later reader can check they all still hold:

1. Publication order. A node's own forward pointer for a level is stored before
   the predecessor at that level is redirected to the node (see
   :meth:`SkipList.insert`). A reader that can reach the node at some level
   therefore finds it already pointing at its successor there, so nothing is
   ever reachable half linked.
2. Single reference stores. Every store a writer makes to publish a node is one
   store of one object reference: ``predecessors[i].forward[i] = node``,
   ``existing.value = value``, ``self._level = level``. Under CPython's global
   interpreter lock such a store does not interleave with a reader's load of the
   same slot, so a reader sees either the old reference or the new one, never a
   half-written one.
3. Bottom-up linking, then the level bump. Levels are linked upwards from 0, and
   ``self._level`` is raised only after the node is linked at the new top level.
   A reader that sees the raised level finds that express lane already
   populated, and a reader that sees the old level just starts one lane lower
   and still reaches the key on the way down.
4. Nodes are never destructively edited. The only field a writer overwrites on a
   node that is already reachable is ``value``, which is point 2. Keys never
   change once linked, and an unlinked node keeps its forward pointers instead
   of having them cleared, so a reader left holding a node that was removed
   underneath it walks forward into the live list rather than off the end.

What a reader is promised as a result: a search returns either the value in
place before a concurrent write or the value after it, never a torn or invented
one, and iteration yields keys in ascending order throughout. What a reader is
not promised: that it observes a write which lands while it is running. That
weakening is deliberate. The memtable is the newest layer of the engine, so a
read that misses a write by microseconds is indistinguishable from the same read
issued microseconds earlier, and paying for a stronger promise would mean
serializing every reader behind the writer, which is the thing this milestone
exists to avoid.

Three scope limits come with the decision and must be carried into M2.4 rather
than quietly dropped:

* Concurrent *physical* removal is out of scope. Point 4 keeps a reader holding
  a removed node from walking off the end, but it cannot keep that reader from
  missing a key inserted into the gap ahead of it after the removal. The engine
  does not have this problem, because :class:`Memtable` never calls
  :meth:`SkipList.delete` (a key-value delete is a tombstone insert), so the
  guarantee is stated for insert and update against concurrent readers, and
  :meth:`SkipList.delete` stays a single-threaded container operation.
* Point 2 is an argument about the GIL build, and nothing stops this package
  being installed on a free-threaded one: ``requires-python`` is a version
  floor, and a free-threaded 3.13 or later (PEP 703) satisfies it while removing
  the interpreter lock the argument is made from. So M2.4 has to check
  ``sys._is_gil_enabled()`` where it exists and fall back to taking the writer
  lock on the read path too, rather than letting a test that happens to pass on
  the maintainer's interpreter stand in for a guarantee on the user's.
* :func:`len` and :attr:`SkipList.level` are approximate while a writer is
  running: they are read without the lock, so they can be one insert behind.
  Nothing in the engine branches on them mid-write (the M6.1 flush threshold is
  checked by the writer, under the lock), so this is a documented limit rather
  than a problem to fix.
"""

from __future__ import annotations

import random
from collections.abc import Iterator
from dataclasses import dataclass
from typing import NoReturn

DEFAULT_MAX_LEVEL = 16
"""Highest number of forward pointers any node may have by default.

With the default probability below, level ``L`` is reached by roughly one key in
``4 ** (L - 1)``, so 16 levels is enough express-lane depth for about four
billion keys. The cap matters because it bounds both the head node's pointer
array and the worst-case work of a search, and because a memtable is flushed to
an SSTable long before it holds anywhere near that many keys.
"""

DEFAULT_LEVEL_PROBABILITY = 0.25
"""Chance that a node promoted to one level is also promoted to the next.

One in four (rather than the also-common one in two) is Pugh's original
recommendation: it gives each node an average of 1/(1 - p) = 1.33 forward
pointers instead of 2, cutting the structure's memory overhead by a third, while
search cost grows only by a small constant factor. Memory is the scarcer resource
here, since the memtable's size is what decides how often a flush happens.
"""


class _Node:
    """One key and its value, plus one forward pointer per level it reaches.

    ``__slots__`` is used because a memtable holds one of these per live key and
    the flush threshold is expressed in bytes, so per-node overhead directly
    decides how many keys fit before a flush. Dropping the per-instance
    ``__dict__`` is the single largest saving available here.
    """

    __slots__ = ("forward", "key", "value")

    def __init__(self, key: bytes, value: bytes, level: int) -> None:
        self.key = key
        self.value = value
        self.forward: list[_Node | None] = [None] * level

    @property
    def level(self) -> int:
        """Number of levels this node is linked into."""
        return len(self.forward)


class SkipList:
    """A sorted map from ``bytes`` keys to ``bytes`` values, backed by a skip list.

    Keys and values are both ``bytes``. Requiring ``bytes`` specifically, rather
    than accepting ``bytearray`` or ``memoryview`` as well, is a correctness
    decision and not just strictness: the structure's entire invariant is that
    nodes sit in ascending key order, and a caller holding a mutable key could
    edit it after insertion and silently move it out of position, leaving a list
    that no longer sorts and searches that miss keys which are present.

    Not thread safe. See this module's docstring for why, and for which story
    changes that.
    """

    def __init__(
        self,
        *,
        max_level: int = DEFAULT_MAX_LEVEL,
        level_probability: float = DEFAULT_LEVEL_PROBABILITY,
        rng: random.Random | None = None,
    ) -> None:
        """Create an empty skip list.

        ``rng`` exists so tests can pin the level draws and assert on the shape
        of the structure that results. Left unset, each instance gets its own
        :class:`random.Random`, rather than sharing the :mod:`random` module's
        global state, so that a caller seeding the global generator for their own
        purposes cannot make one memtable's node heights depend on another's.
        """
        if max_level < 1:
            raise ValueError(f"max_level must be at least 1, got {max_level}")
        if not 0.0 < level_probability < 1.0:
            raise ValueError(
                f"level_probability must be strictly between 0 and 1, got {level_probability}"
            )

        self._max_level = max_level
        self._level_probability = level_probability
        self._rng = rng if rng is not None else random.Random()

        # The head is a sentinel holding no key. It is allocated at full height
        # up front so that promoting a node to a new level never has to grow the
        # head's pointer array, which keeps the insert path free of a
        # reallocation that M2.4 would otherwise have to serialize readers
        # against. On its own this is not a thread-safety guarantee.
        self._head = _Node(b"", b"", max_level)
        self._level = 1
        self._size = 0

    @property
    def max_level(self) -> int:
        """Level cap this instance was created with."""
        return self._max_level

    @property
    def level_probability(self) -> float:
        """Per-level promotion probability this instance was created with."""
        return self._level_probability

    @property
    def level(self) -> int:
        """Number of levels currently in use, at least 1 even when empty."""
        return self._level

    def __len__(self) -> int:
        """Number of keys currently stored."""
        return self._size

    def __contains__(self, key: object) -> bool:
        """Report whether ``key`` is present, without returning its value."""
        if not isinstance(key, bytes):
            return False
        return self._find_node(key) is not None

    def insert(self, key: bytes, value: bytes) -> None:
        """Store ``value`` under ``key``, replacing any value already there.

        An existing key is updated in place rather than given a second node.
        A memtable's job is to answer with the newest write for a key, so two
        nodes with the same key would only ever differ in which of them a search
        happened to stop at. Keeping one node per key also means the length is
        the number of distinct live keys, so measuring the memtable or flushing
        it never has to de-duplicate first.
        """
        _check_key(key)
        _check_value(value)

        predecessors = self._find_predecessors(key)

        existing = predecessors[0].forward[0]
        if existing is not None and existing.key == key:
            existing.value = value
            return

        level = self._random_level()
        node = _Node(key, value, level)

        # Link the new node in one level at a time. The node's own forward
        # pointer for a level is set before the predecessor at that level is
        # redirected to it, so the node is fully formed at a level by the time
        # anything points at it there. That ordering is what M2.4 will build its
        # concurrency argument on; by itself it makes no thread-safety promise.
        for index in range(level):
            node.forward[index] = predecessors[index].forward[index]
            predecessors[index].forward[index] = node

        if level > self._level:
            self._level = level

        self._size += 1

    def search(self, key: bytes) -> bytes | None:
        """Return the value stored under ``key``, or ``None`` if it is absent.

        ``None`` is an unambiguous "absent" answer here because values are always
        ``bytes``: a key explicitly stored with an empty value comes back as
        ``b""``, which is a different result from ``None``. That distinction is
        the same one the WAL draws between a put of an empty value and a delete,
        and it has to survive into the memtable for tombstones (M2.2) to mean
        anything.
        """
        _check_key(key)
        node = self._find_node(key)
        return None if node is None else node.value

    def delete(self, key: bytes) -> bool:
        """Unlink ``key``'s node and return whether it was there to begin with.

        This is the generic container operation: the node is physically removed
        and the key stops being visible to search and iteration. The KV-level
        delete, which must leave a tombstone behind so that it shadows an older
        value on disk, is story M2.2 and is layered on top of this rather than
        changing what this method does.
        """
        _check_key(key)

        predecessors = self._find_predecessors(key)

        node = predecessors[0].forward[0]
        if node is None or node.key != key:
            return False

        # Only unlink at the levels where this node is actually the predecessor's
        # successor. A node of height 2 must not be unlinked at level 5, where
        # some other node is linked instead.
        for index in range(node.level):
            if predecessors[index].forward[index] is node:
                predecessors[index].forward[index] = node.forward[index]

        # Give back any levels the removed node was the only occupant of, so that
        # a search does not keep paying to descend through empty express lanes
        # after a large number of deletes.
        while self._level > 1 and self._head.forward[self._level - 1] is None:
            self._level -= 1

        self._size -= 1
        return True

    def keys(self) -> Iterator[bytes]:
        """Yield every stored key in ascending byte order."""
        node = self._head.forward[0]
        while node is not None:
            yield node.key
            node = node.forward[0]

    def items(self) -> Iterator[tuple[bytes, bytes]]:
        """Yield every ``(key, value)`` pair in ascending key order.

        Iteration walks level 0, which holds every node, so this is a linear scan
        of a sorted linked list and needs no traversal of the upper levels. This
        is the shape a memtable flush wants: an SSTable's data block is written
        in key order, so the flush consumes this iterator directly instead of
        sorting a snapshot.
        """
        node = self._head.forward[0]
        while node is not None:
            yield node.key, node.value
            node = node.forward[0]

    def __iter__(self) -> Iterator[bytes]:
        """Iterate keys in ascending byte order, matching :meth:`keys`."""
        return self.keys()

    def _random_level(self) -> int:
        """Draw a height for a new node from a geometric distribution.

        The draw is independent of the key, so the structure's shape does not
        depend on insertion order in a way an adversarial key sequence could
        exploit into a long scan at every level.
        """
        level = 1
        while level < self._max_level and self._rng.random() < self._level_probability:
            level += 1
        return level

    def _find_predecessors(self, key: bytes) -> list[_Node]:
        """Return, per level, the last node ordering strictly before ``key``.

        Index ``i`` of the result is the node whose level-``i`` forward pointer
        has to change if ``key`` is inserted or removed. Levels at or above the
        current top level are reported as the head, which is correct: nothing is
        linked there yet, so the head is the only possible predecessor.
        """
        predecessors: list[_Node] = [self._head] * self._max_level

        current = self._head
        for index in reversed(range(self._level)):
            successor = current.forward[index]
            while successor is not None and successor.key < key:
                current = successor
                successor = current.forward[index]
            predecessors[index] = current

        return predecessors

    def _find_node(self, key: bytes) -> _Node | None:
        """Return ``key``'s node, or ``None``.

        This is the read path, kept separate from :meth:`_find_predecessors`
        because a search has no use for the per-level predecessors and allocating
        a list of them on every lookup would be pure waste.
        """
        current = self._head
        for index in reversed(range(self._level)):
            successor = current.forward[index]
            while successor is not None and successor.key < key:
                current = successor
                successor = current.forward[index]

        candidate = current.forward[0]
        if candidate is not None and candidate.key == key:
            return candidate
        return None


def _check_key(key: bytes) -> None:
    """Reject anything but ``bytes`` as a key, before it can break the ordering."""
    if not isinstance(key, bytes):
        raise TypeError(f"key must be bytes, got {type(key).__name__}")


def _check_value(value: bytes) -> None:
    """Reject anything but ``bytes`` as a value."""
    if not isinstance(value, bytes):
        raise TypeError(f"value must be bytes, got {type(value).__name__}")


_TAG_PUT = b"\x01"
"""Tag prefixed to a stored value, marking the record as a live write.

One byte rather than a flag alongside the value, because the skip list beneath
holds ``bytes`` and nothing else. See this module's docstring for why the
distinction is carried in the bytes at all.
"""

_TAG_DELETE = b"\x02"
"""Tag marking a record as a tombstone. A tombstone is this byte and nothing else.

The codes deliberately read the same as :class:`ledgerlog.wal.WalOp`'s PUT and
DELETE, so a record means the same thing whichever layer is looking at it, and
they start at 1 there for the same reason: a zero byte is what a partially
written or freshly allocated region looks like, so no run of zeros should decode
as a valid operation. They are a separate constant rather than an import because
CLAUDE.md asks that the WAL and the memtable stay independently testable, and a
shared enum would make each module's tests depend on the other's format.
"""


@dataclass(frozen=True)
class MemtableEntry:
    """One record in the memtable: a key with either a value or a tombstone.

    ``value`` is ``None`` when the record is a tombstone. That is a different
    statement from the ``None`` :meth:`Memtable.get` returns, and the difference
    is the whole point of the type: this ``None`` says "deleted here, stop
    looking", while a missing entry says "not here, keep looking in older
    layers". Per ARCHITECTURE.md section 5 the read path has to be able to tell
    those apart, because a tombstone must shadow a value in an older SSTable
    rather than let the search fall through to it.

    Frozen because an entry is a decoded view of a record the memtable still
    owns. Handing out something mutable would let a caller edit what looks like
    their own copy and find they had changed the memtable's idea of the record,
    or the reverse.
    """

    key: bytes
    value: bytes | None

    @property
    def is_tombstone(self) -> bool:
        """True if this record marks the key deleted rather than holding a value."""
        return self.value is None


def _decode_stored(key: bytes, stored: bytes) -> MemtableEntry:
    """Decode one tagged skip list value into an entry.

    The unknown-tag and trailing-bytes cases are not reachable through
    :class:`Memtable`'s API, which is the only writer of these bytes. They are
    checked rather than assumed because the cost is one comparison on a read
    path that is already slicing, and because the failure they would otherwise
    produce is a silently wrong answer: an unrecognized tag treated as a put
    would hand back a value with a stray byte on the front, and a tombstone with
    a payload would mean a delete and a put had been conflated somewhere above.
    """
    tag = stored[:1]
    if tag == _TAG_PUT:
        return MemtableEntry(key=key, value=stored[1:])
    if tag == _TAG_DELETE:
        if len(stored) != len(_TAG_DELETE):
            raise ValueError(
                f"tombstone record for key {key!r} carries "
                f"{len(stored) - len(_TAG_DELETE)} bytes of value, which a tombstone never has"
            )
        return MemtableEntry(key=key, value=None)
    raise ValueError(f"record for key {key!r} carries an unknown memtable tag {tag!r}")


class Memtable:
    """The key-value view of a memtable: puts, gets, and deletes that leave tombstones.

    This is what the engine writes through. It wraps a :class:`SkipList`, so
    records stay in ascending key order and a flush can stream them straight
    into an SSTable's data block, and it adds the one piece of key-value
    semantics the container itself has no business knowing about: a delete
    records that the key was deleted instead of making it vanish.

    Why a delete must not remove anything here: the memtable is only the newest
    layer. A key it drops could still have an older value sitting in an SSTable,
    and a read that found nothing in memory would fall through and return that
    stale value, resurrecting a key the caller deleted. The tombstone is what
    stops the search at the right layer. It is dropped for real during compaction
    (story M8.3), once no older table can still answer for the key.

    ``in`` is deliberately refused rather than answered. "Is this key in the
    memtable" has two honest answers for a tombstoned key, since the record is
    present and the value is not, and either one silently misleads half its
    callers. :class:`SkipList` can answer the same question because there is only
    one thing it can mean there. Callers here ask what they actually mean:
    :meth:`get` for "is there a value", and :meth:`lookup` for "is there a
    record, and what kind".

    Not thread safe, for the reasons in this module's docstring.
    """

    def __init__(
        self,
        *,
        max_level: int = DEFAULT_MAX_LEVEL,
        level_probability: float = DEFAULT_LEVEL_PROBABILITY,
        rng: random.Random | None = None,
    ) -> None:
        """Create an empty memtable.

        The tuning parameters are forwarded to the underlying :class:`SkipList`
        unchanged, including ``rng``, so a test can pin the node heights and
        assert on the shape of the structure a sequence of puts and deletes
        leaves behind.
        """
        self._skiplist = SkipList(
            max_level=max_level,
            level_probability=level_probability,
            rng=rng,
        )

    def __len__(self) -> int:
        """Number of records held, counting tombstones.

        Tombstones are counted because they are records: they occupy a node, they
        are written out by a flush, and they are what a later compaction has to
        read in order to decide the key can finally go. A count that skipped them
        would understate how much work a flush has to do.
        """
        return len(self._skiplist)

    def __contains__(self, key: object) -> NoReturn:
        """Refuse the membership test, naming the two questions it could mean.

        This raises where most containers answer, and the reason is that
        :meth:`__iter__` below yields records. Without this method, ``key in
        memtable`` would fall back to iteration, compare a key against a run of
        :class:`MemtableEntry` objects, and report ``False`` for every key,
        including ones that are present. A loud refusal is worth more than an
        answer that is wrong every time it is asked.
        """
        raise TypeError(
            "Memtable does not support 'in': a tombstoned key has a record but no value, "
            "so use get(key) is not None for a live value, or lookup(key) is not None for "
            "any record"
        )

    def put(self, key: bytes, value: bytes) -> None:
        """Store ``value`` under ``key``, replacing whatever record was there.

        A put over a tombstone is an ordinary overwrite: the key is live again,
        and nothing remembers that it was briefly deleted. It does not need to,
        because the tombstone existed only to shadow older layers, and this
        newer value now shadows them itself.
        """
        _check_key(key)
        _check_value(value)
        self._skiplist.insert(key, _TAG_PUT + value)

    def delete(self, key: bytes) -> None:
        """Record that ``key`` was deleted, whether or not it held a value here.

        Deleting a key with no record in this memtable still writes a tombstone,
        rather than being treated as a no-op, because the key may well have a
        value in an SSTable that only this tombstone can shadow. Deleting an
        already tombstoned key rewrites the same tombstone, so repeated deletes
        are idempotent and none of them is an error.

        Returns nothing, deliberately, where :meth:`SkipList.delete` returns
        whether the key was there. At this layer that boolean would be actively
        misleading: the memtable can only see its own records, so "there was
        nothing to delete" would be a claim about one layer dressed up as a
        claim about the engine, and answering it honestly needs the whole read
        path (story M7.2).
        """
        _check_key(key)
        self._skiplist.insert(key, _TAG_DELETE)

    def get(self, key: bytes) -> bytes | None:
        """Return the value stored under ``key``, or ``None`` if there is none here.

        A tombstoned key reads as ``None``, the same as a key this memtable has
        never seen. The two are different facts and the difference matters to the
        engine's read path, which is why :meth:`lookup` exists, but to a caller
        asking only for a value they come to the same thing.

        A key put with an empty value reads back as ``b""``, which is not
        ``None``. That distinction is the same one the WAL draws between a put of
        an empty value and a delete, and it survives here intact.
        """
        entry = self.lookup(key)
        if entry is None or entry.is_tombstone:
            return None
        return entry.value

    def lookup(self, key: bytes) -> MemtableEntry | None:
        """Return ``key``'s record, or ``None`` if this memtable holds none.

        This is the read path's primitive rather than :meth:`get`, because it is
        the only one that separates "deleted" from "absent". A tombstone comes
        back as an entry whose value is ``None``, and the read path stops there;
        no record at all comes back as ``None``, and the read path moves on to
        the next layer.
        """
        stored = self._skiplist.search(key)
        if stored is None:
            return None
        return _decode_stored(key, stored)

    def entries(self) -> Iterator[MemtableEntry]:
        """Yield every record in ascending key order, tombstones included.

        This is the shape a flush consumes: an SSTable's data block is written in
        key order, and it stores tombstones alongside values, so the flush walks
        this iterator once and writes what it is given.
        """
        for key, stored in self._skiplist.items():
            yield _decode_stored(key, stored)

    def keys(self) -> Iterator[bytes]:
        """Yield every key in ascending byte order, including tombstoned keys."""
        return self._skiplist.keys()

    def __iter__(self) -> Iterator[MemtableEntry]:
        """Iterate records, matching :meth:`entries`.

        Records rather than keys, which is the opposite of :class:`SkipList`'s
        choice. The two classes have different consumers: the container is asked
        what keys it holds, while a memtable is drained into an SSTable, and the
        thing being drained is the records.
        """
        return self.entries()
