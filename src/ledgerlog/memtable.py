"""Memtable: the in-memory sorted structure that holds recent writes.

Scope of this module today (story M2.1): the skip list itself, as a plain sorted
container with insert, search, delete and in-order iteration. Keys are compared
as bytes, so iteration order is the byte-lexicographic order that SSTables are
written in later, and a flush can walk this structure front to back without
re-sorting anything.

What is deliberately not here yet:

* Tombstones (story M2.2). :meth:`SkipList.delete` unlinks the node, which is the
  ordinary data-structure operation. At the KV level a delete has to leave a
  marker behind instead, so that it shadows an older value sitting in an
  SSTable, and that marker is built on top of this structure rather than inside
  it.
* Thread safety (stories M2.3 and M2.4). :class:`SkipList` carries no lock and
  makes no concurrency guarantee: a reader running against a concurrent insert
  can observe a partially linked node. Story M2.3 chooses the locking strategy
  and M2.4 implements it, and per ARCHITECTURE.md section 2 the chosen approach
  gets documented here once it exists. Until then, callers sharing an instance
  between threads must synchronize externally.

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
"""

from __future__ import annotations

import random
from collections.abc import Iterator

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
