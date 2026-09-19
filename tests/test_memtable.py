"""Tests for the skip list backing the memtable, and the tombstone layer above it.

Covers story M2.1 (insert, search, delete, sorted iteration, and correctness
against a reference sorted structure), story M2.2 (a delete records a tombstone
instead of removing the key), and the prototype that validates the story M2.3
concurrency decision.

The M2.2 tests matter most where they check what did *not* happen: a delete at
the key-value layer has to leave the node in place, and the cheapest way to fake
passing its acceptance criteria would be to remove the node and report not-found,
which every value-level assertion would still agree with. So those tests assert
on the record count, on iteration, and on the level 0 chain directly, not just on
what ``get`` returns.

Two kinds of test live here. The behavioural ones go through the public API only,
since that is what the rest of the engine will use. The structural ones reach
into ``_head`` and ``_level`` on purpose: a skip list can answer every public
call correctly while its upper levels are quietly malformed, because level 0
alone is enough to satisfy search and iteration. Checking the express lanes
directly is the only way to catch a linking bug before it turns into a wrong
answer on some later, larger input.

The M2.1 and M2.2 tests are single threaded, because neither story makes a
thread-safety claim. The M2.3 tests at the bottom of this module are a different
thing again, and the distinction matters when reading them: they do not verify a
guarantee ``SkipList`` makes, because it still makes none. They are the spike's
prototype, and their job is to check that the premises the M2.3 decision rests
on are actually true of this code, so that M2.4 builds its lock on a foundation
that has been measured rather than assumed. M2.4 is what turns them into a
guarantee, with the sustained stress test CLAUDE.md requires of one.

The M2.4 tests in the last section are that guarantee. They differ from the
spike's in where the lock lives: the spike wrapped an unlocked ``SkipList`` in a
prototype writer defined in this module, while these call ``SkipList`` and
``Memtable`` exactly as the engine will and rely on the locking inside them. All
of them use real threads, because a single-threaded stand-in would let every one
of the story's acceptance criteria pass without a second thread ever existing.
"""

from __future__ import annotations

import contextlib
import random
import sys
import threading
from collections.abc import Callable, Iterator

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ledgerlog import memtable as memtable_module
from ledgerlog.memtable import (
    _TAG_DELETE,
    _TAG_PUT,
    DEFAULT_LEVEL_PROBABILITY,
    DEFAULT_MAX_LEVEL,
    READS_TAKE_THE_WRITER_LOCK,
    Memtable,
    MemtableEntry,
    SkipList,
    _Node,
)


class _AlwaysPromote(random.Random):
    """RNG that promotes every node as high as the level cap allows."""

    def random(self) -> float:
        return 0.0


class _NeverPromote(random.Random):
    """RNG that leaves every node at level 1, degenerating to a linked list."""

    def random(self) -> float:
        return 1.0


def level_zero_keys(skiplist: SkipList) -> list[bytes]:
    """Return the keys on level 0, which is the chain holding every node."""
    keys: list[bytes] = []
    node = skiplist._head.forward[0]
    while node is not None:
        keys.append(node.key)
        node = node.forward[0]
    return keys


def assert_structure_is_sound(skiplist: SkipList) -> None:
    """Assert every skip-list invariant that the public API can hide.

    Checks that each level is sorted, that each level is a subsequence of the
    level below it (a node linked at level ``i`` must also be linked at every
    level under ``i``), that no node exceeds the cap, that the reported level
    bookkeeping matches what is actually linked, and that the cached size
    matches the number of nodes.
    """
    assert 1 <= skiplist.level <= skiplist.max_level

    base = level_zero_keys(skiplist)
    assert base == sorted(base), "level 0 is not in ascending key order"
    assert len(base) == len(set(base)), "level 0 holds a duplicate key"
    assert len(base) == len(skiplist), "cached size disagrees with the level 0 chain"

    for index in range(skiplist.max_level):
        chain: list[bytes] = []
        node = skiplist._head.forward[index]
        while node is not None:
            assert node.level > index, "a node is linked above its own height"
            assert node.level <= skiplist.max_level, "a node exceeds the level cap"
            chain.append(node.key)
            node = node.forward[index]

        assert chain == sorted(chain), f"level {index} is not in ascending key order"

        # Every key on this level must appear on level 0, in the same order.
        remaining = iter(base)
        assert all(key in remaining for key in chain), f"level {index} is not a subsequence"

        if index >= skiplist.level:
            assert chain == [], "a level above the reported top level holds nodes"

    if len(skiplist) > 0:
        # The top level in use must not be empty, otherwise a search pays to
        # descend through an express lane that leads nowhere.
        assert skiplist._head.forward[skiplist.level - 1] is not None, "top level is empty"


def test_insert_then_search_returns_the_inserted_value() -> None:
    skiplist = SkipList()
    skiplist.insert(b"key", b"value")

    assert skiplist.search(b"key") == b"value"
    assert len(skiplist) == 1
    assert b"key" in skiplist
    assert_structure_is_sound(skiplist)


def test_search_of_an_absent_key_returns_none() -> None:
    skiplist = SkipList()
    skiplist.insert(b"present", b"1")

    assert skiplist.search(b"absent") is None
    assert b"absent" not in skiplist


def test_insert_replaces_the_value_without_adding_a_second_node() -> None:
    skiplist = SkipList()
    skiplist.insert(b"key", b"first")
    skiplist.insert(b"key", b"second")

    assert skiplist.search(b"key") == b"second"
    assert len(skiplist) == 1
    assert level_zero_keys(skiplist) == [b"key"]
    assert list(skiplist.items()) == [(b"key", b"second")]
    assert_structure_is_sound(skiplist)


def test_an_empty_value_is_stored_and_is_not_the_same_as_absent() -> None:
    skiplist = SkipList()
    skiplist.insert(b"key", b"")

    assert skiplist.search(b"key") == b""
    assert skiplist.search(b"other") is None


def test_the_empty_key_is_a_usable_key_despite_the_sentinel_head() -> None:
    # The head sentinel holds b"" internally, so an empty key is the one input
    # that could collide with it.
    skiplist = SkipList()
    skiplist.insert(b"", b"empty key")
    skiplist.insert(b"a", b"a")

    assert skiplist.search(b"") == b"empty key"
    assert list(skiplist.keys()) == [b"", b"a"]
    assert skiplist.delete(b"") is True
    assert skiplist.search(b"") is None
    assert list(skiplist.keys()) == [b"a"]
    assert_structure_is_sound(skiplist)


def test_delete_physically_removes_the_node() -> None:
    skiplist = SkipList(rng=_AlwaysPromote())
    for key in (b"a", b"b", b"c"):
        skiplist.insert(key, key)

    assert skiplist.delete(b"b") is True

    assert skiplist.search(b"b") is None
    assert b"b" not in skiplist
    assert len(skiplist) == 2
    assert level_zero_keys(skiplist) == [b"a", b"c"]

    # Every node was promoted to the cap, so a level that still reached the
    # deleted node would prove the unlink only happened on level 0.
    for index in range(skiplist.max_level):
        node = skiplist._head.forward[index]
        while node is not None:
            assert node.key != b"b"
            node = node.forward[index]

    assert_structure_is_sound(skiplist)


def test_delete_of_an_absent_key_reports_false_and_changes_nothing() -> None:
    skiplist = SkipList()
    skiplist.insert(b"a", b"1")

    assert skiplist.delete(b"missing") is False
    assert len(skiplist) == 1
    assert skiplist.search(b"a") == b"1"
    assert_structure_is_sound(skiplist)


def test_delete_on_an_empty_list_is_not_an_error() -> None:
    skiplist = SkipList()

    assert skiplist.delete(b"anything") is False
    assert len(skiplist) == 0
    assert list(skiplist.items()) == []
    assert skiplist.level == 1


def test_delete_then_reinsert_restores_the_key() -> None:
    skiplist = SkipList()
    skiplist.insert(b"key", b"first")
    assert skiplist.delete(b"key") is True
    skiplist.insert(b"key", b"second")

    assert skiplist.search(b"key") == b"second"
    assert len(skiplist) == 1
    assert_structure_is_sound(skiplist)


def test_sorted_iteration_yields_shuffled_inserts_in_ascending_order() -> None:
    keys = [f"key{index:03d}".encode() for index in range(200)]
    shuffled = list(keys)
    random.Random(1234).shuffle(shuffled)

    skiplist = SkipList()
    for key in shuffled:
        skiplist.insert(key, key.upper())

    assert list(skiplist.keys()) == keys
    assert list(skiplist) == keys
    assert list(skiplist.items()) == [(key, key.upper()) for key in keys]
    assert_structure_is_sound(skiplist)


def test_iteration_uses_byte_order_not_length_or_numeric_order() -> None:
    skiplist = SkipList()
    for key in (b"b", b"ab", b"a", b"\xff", b"\x00", b"aa", b"10", b"9"):
        skiplist.insert(key, b"v")

    assert list(skiplist.keys()) == [b"\x00", b"10", b"9", b"a", b"aa", b"ab", b"b", b"\xff"]


def test_iterating_an_empty_list_yields_nothing() -> None:
    skiplist = SkipList()

    assert list(skiplist.keys()) == []
    assert list(skiplist.items()) == []
    assert len(skiplist) == 0


@pytest.mark.parametrize("seed", [0, 1, 7, 42, 99, 12345])
def test_randomized_operations_match_a_reference_sorted_dict(seed: int) -> None:
    """Compare against a plain dict kept sorted on read, operation by operation.

    The reference is checked after every single operation rather than only at the
    end, so a divergence is reported at the operation that caused it instead of
    after hundreds of further operations have obscured it.
    """
    rng = random.Random(seed)
    skiplist = SkipList(rng=random.Random(seed + 1))
    reference: dict[bytes, bytes] = {}

    # A small key space relative to the operation count, so that updates of
    # existing keys and deletes of live keys happen often instead of almost
    # every operation touching a fresh key.
    key_space = [f"k{index:02d}".encode() for index in range(40)]

    for step in range(1500):
        key = rng.choice(key_space)
        action = rng.choices(["insert", "delete", "search"], weights=[5, 3, 2])[0]

        if action == "insert":
            value = f"v{step}".encode()
            skiplist.insert(key, value)
            reference[key] = value
        elif action == "delete":
            assert skiplist.delete(key) is (key in reference)
            reference.pop(key, None)
        else:
            assert skiplist.search(key) == reference.get(key)

        assert len(skiplist) == len(reference)
        assert (key in skiplist) is (key in reference)
        assert list(skiplist.items()) == sorted(reference.items())

    assert_structure_is_sound(skiplist)


@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    operations=st.lists(
        st.tuples(
            st.sampled_from(["insert", "delete", "search"]),
            st.binary(min_size=0, max_size=6),
            st.binary(min_size=0, max_size=6),
        ),
        max_size=120,
    ),
    seed=st.integers(min_value=0, max_value=2**32 - 1),
)
def test_arbitrary_operation_sequences_match_a_reference_sorted_dict(
    operations: list[tuple[str, bytes, bytes]], seed: int
) -> None:
    """Same differential check, but over key and value bytes chosen by hypothesis.

    The parametrized test above uses a tidy, uniform key space. This one lets
    keys be arbitrary byte strings of differing lengths, including empty ones,
    which is where a comparison bug that a fixed-width key space would never
    reveal tends to show up.
    """
    skiplist = SkipList(rng=random.Random(seed))
    reference: dict[bytes, bytes] = {}

    for action, key, value in operations:
        if action == "insert":
            skiplist.insert(key, value)
            reference[key] = value
        elif action == "delete":
            assert skiplist.delete(key) is (key in reference)
            reference.pop(key, None)
        else:
            assert skiplist.search(key) == reference.get(key)

        assert len(skiplist) == len(reference)

    assert list(skiplist.items()) == sorted(reference.items())
    assert_structure_is_sound(skiplist)


def test_a_large_key_set_stays_fully_searchable() -> None:
    """Exercise a structure deep enough to use many express lanes at once."""
    skiplist = SkipList(rng=random.Random(7))
    keys = [f"key{index:05d}".encode() for index in range(5000)]
    shuffled = list(keys)
    random.Random(8).shuffle(shuffled)

    for key in shuffled:
        skiplist.insert(key, key)

    assert len(skiplist) == len(keys)
    assert skiplist.level > 1, "5000 keys should have promoted at least one node"
    for key in keys:
        assert skiplist.search(key) == key
    assert list(skiplist.keys()) == keys
    assert_structure_is_sound(skiplist)

    for key in shuffled[: len(shuffled) // 2]:
        assert skiplist.delete(key) is True

    expected = sorted(set(keys) - set(shuffled[: len(shuffled) // 2]))
    assert list(skiplist.keys()) == expected
    assert_structure_is_sound(skiplist)


def test_promotion_never_exceeds_the_level_cap() -> None:
    skiplist = SkipList(max_level=4, rng=_AlwaysPromote())
    for index in range(50):
        skiplist.insert(f"key{index:02d}".encode(), b"v")

    assert skiplist.level == 4
    assert skiplist.max_level == 4
    assert_structure_is_sound(skiplist)


def test_a_list_with_no_promotions_still_behaves_correctly() -> None:
    skiplist = SkipList(rng=_NeverPromote())
    for key in (b"c", b"a", b"b"):
        skiplist.insert(key, key)

    assert skiplist.level == 1
    assert list(skiplist.keys()) == [b"a", b"b", b"c"]
    assert skiplist.search(b"b") == b"b"
    assert skiplist.delete(b"a") is True
    assert list(skiplist.keys()) == [b"b", b"c"]
    assert_structure_is_sound(skiplist)


def test_the_top_level_is_given_back_when_its_only_node_is_deleted() -> None:
    skiplist = SkipList(max_level=8, rng=_NeverPromote())
    skiplist.insert(b"short", b"v")
    assert skiplist.level == 1

    skiplist._rng = _AlwaysPromote()
    skiplist.insert(b"tall", b"v")
    assert skiplist.level == 8

    assert skiplist.delete(b"tall") is True
    assert skiplist.level == 1
    assert list(skiplist.keys()) == [b"short"]
    assert_structure_is_sound(skiplist)


def test_an_injected_rng_makes_the_structure_deterministic() -> None:
    keys = [f"key{index:03d}".encode() for index in range(300)]

    def heights(seed: int) -> list[int]:
        skiplist = SkipList(rng=random.Random(seed))
        for key in keys:
            skiplist.insert(key, b"v")
        result: list[int] = []
        node = skiplist._head.forward[0]
        while node is not None:
            result.append(node.level)
            node = node.forward[0]
        return result

    assert heights(2024) == heights(2024)
    assert heights(2024) != heights(9999), "different seeds should shape the list differently"


def test_default_tuning_is_reported_and_used() -> None:
    skiplist = SkipList()

    assert skiplist.max_level == DEFAULT_MAX_LEVEL
    assert skiplist.level_probability == DEFAULT_LEVEL_PROBABILITY
    assert skiplist.level == 1


@pytest.mark.parametrize("max_level", [0, -1])
def test_an_unusable_level_cap_is_rejected(max_level: int) -> None:
    with pytest.raises(ValueError, match="max_level"):
        SkipList(max_level=max_level)


@pytest.mark.parametrize("probability", [0.0, 1.0, -0.5, 1.5])
def test_an_unusable_level_probability_is_rejected(probability: float) -> None:
    with pytest.raises(ValueError, match="level_probability"):
        SkipList(level_probability=probability)


def test_a_max_level_of_one_is_allowed_and_works() -> None:
    skiplist = SkipList(max_level=1, rng=_AlwaysPromote())
    for key in (b"b", b"a"):
        skiplist.insert(key, key)

    assert skiplist.level == 1
    assert list(skiplist.keys()) == [b"a", b"b"]
    assert_structure_is_sound(skiplist)


@pytest.mark.parametrize("bad_key", ["text", 1, None, bytearray(b"mutable"), memoryview(b"view")])
def test_non_bytes_keys_are_rejected(bad_key: object) -> None:
    skiplist = SkipList()

    with pytest.raises(TypeError, match="key must be bytes"):
        skiplist.insert(bad_key, b"v")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="key must be bytes"):
        skiplist.search(bad_key)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="key must be bytes"):
        skiplist.delete(bad_key)  # type: ignore[arg-type]


@pytest.mark.parametrize("bad_value", ["text", 1, None, bytearray(b"mutable")])
def test_non_bytes_values_are_rejected(bad_value: object) -> None:
    skiplist = SkipList()

    with pytest.raises(TypeError, match="value must be bytes"):
        skiplist.insert(b"key", bad_value)  # type: ignore[arg-type]


def test_a_rejected_insert_leaves_the_list_untouched() -> None:
    skiplist = SkipList()
    skiplist.insert(b"key", b"value")

    with pytest.raises(TypeError):
        skiplist.insert(b"key", "not bytes")  # type: ignore[arg-type]

    assert len(skiplist) == 1
    assert skiplist.search(b"key") == b"value"
    assert_structure_is_sound(skiplist)


def test_containment_of_a_non_bytes_object_is_false_rather_than_an_error() -> None:
    # ``in`` is expected to answer a question, not raise, so an object that
    # could never be a key is simply absent.
    skiplist = SkipList()
    skiplist.insert(b"key", b"v")

    assert "key" not in skiplist
    assert 1 not in skiplist
    assert None not in skiplist


# --- Story M2.2: tombstone support -----------------------------------------


def memtable_level_zero_keys(memtable: Memtable) -> list[bytes]:
    """Return the keys on the underlying skip list's level 0 chain.

    Reaching through to the container on purpose: the acceptance criterion is
    that a delete leaves the node in place, and the node is not something the
    Memtable API is willing to talk about.
    """
    return level_zero_keys(memtable._skiplist)


def stored_value(memtable: Memtable, key: bytes) -> bytes | None:
    """Return the raw tagged bytes the skip list holds for ``key``."""
    return memtable._skiplist.search(key)


def test_put_then_get_returns_the_value() -> None:
    memtable = Memtable()
    memtable.put(b"key", b"value")

    assert memtable.get(b"key") == b"value"
    assert memtable.lookup(b"key") == MemtableEntry(key=b"key", value=b"value")
    assert memtable.lookup(b"key").is_tombstone is False
    assert len(memtable) == 1


def test_get_of_a_key_never_written_is_none_and_lookup_finds_no_record() -> None:
    memtable = Memtable()
    memtable.put(b"present", b"1")

    assert memtable.get(b"absent") is None
    assert memtable.lookup(b"absent") is None


def test_delete_leaves_a_tombstone_instead_of_removing_the_node() -> None:
    memtable = Memtable()
    memtable.put(b"key", b"value")

    memtable.delete(b"key")

    # The record is still there, which is the whole acceptance criterion.
    assert len(memtable) == 1
    assert memtable_level_zero_keys(memtable) == [b"key"]
    assert list(memtable.keys()) == [b"key"]

    # And it is a tombstone rather than the old value.
    assert memtable.lookup(b"key") == MemtableEntry(key=b"key", value=None)
    assert memtable.lookup(b"key").is_tombstone is True
    assert memtable.get(b"key") is None


def test_a_tombstone_is_not_unlinked_from_the_upper_levels_either() -> None:
    """A delete must not touch the express lanes any more than it touches level 0."""
    memtable = Memtable(rng=_AlwaysPromote())
    for key in (b"a", b"b", b"c"):
        memtable.put(key, key.upper())

    memtable.delete(b"b")

    skiplist = memtable._skiplist
    for index in range(skiplist.max_level):
        chain: list[bytes] = []
        node = skiplist._head.forward[index]
        while node is not None:
            chain.append(node.key)
            node = node.forward[index]
        assert chain == [b"a", b"b", b"c"], f"level {index} lost the tombstoned node"

    assert_structure_is_sound(skiplist)


def test_delete_of_a_key_with_no_prior_value_still_records_a_tombstone() -> None:
    memtable = Memtable()

    memtable.delete(b"never-written")

    assert len(memtable) == 1
    assert memtable.lookup(b"never-written") == MemtableEntry(key=b"never-written", value=None)
    assert memtable.get(b"never-written") is None
    assert list(memtable.keys()) == [b"never-written"]


def test_repeated_deletes_are_idempotent_and_do_not_error() -> None:
    memtable = Memtable()
    memtable.put(b"key", b"value")

    for _ in range(5):
        memtable.delete(b"key")

    assert len(memtable) == 1
    assert memtable.lookup(b"key").is_tombstone is True
    assert stored_value(memtable, b"key") == _TAG_DELETE
    assert_structure_is_sound(memtable._skiplist)


def test_delete_returns_nothing_whether_or_not_the_key_was_present() -> None:
    # Deliberately not a bool: see Memtable.delete's docstring. A test pins this
    # so that "did it exist" cannot be added back without a decision.
    memtable = Memtable()
    memtable.put(b"live", b"v")

    assert memtable.delete(b"live") is None
    assert memtable.delete(b"absent") is None


def test_a_put_over_a_tombstone_makes_the_key_live_again() -> None:
    memtable = Memtable()
    memtable.put(b"key", b"first")
    memtable.delete(b"key")

    memtable.put(b"key", b"second")

    assert memtable.get(b"key") == b"second"
    assert memtable.lookup(b"key").is_tombstone is False
    assert len(memtable) == 1
    assert_structure_is_sound(memtable._skiplist)


def test_a_put_of_an_empty_value_is_not_a_tombstone() -> None:
    """The distinction the WAL draws between a put of b"" and a delete survives here."""
    memtable = Memtable()
    memtable.put(b"empty", b"")
    memtable.delete(b"deleted")

    assert memtable.get(b"empty") == b""
    assert memtable.lookup(b"empty") == MemtableEntry(key=b"empty", value=b"")
    assert memtable.lookup(b"empty").is_tombstone is False

    assert memtable.get(b"deleted") is None
    assert memtable.lookup(b"deleted").is_tombstone is True

    # The two records differ in the bytes actually stored, not just in how they
    # are read back, so nothing downstream can conflate them either.
    assert stored_value(memtable, b"empty") != stored_value(memtable, b"deleted")
    assert stored_value(memtable, b"empty") == _TAG_PUT
    assert stored_value(memtable, b"deleted") == _TAG_DELETE


def test_the_empty_key_can_be_put_and_tombstoned() -> None:
    memtable = Memtable()
    memtable.put(b"", b"empty key")
    memtable.put(b"a", b"a")

    memtable.delete(b"")

    assert memtable.get(b"") is None
    assert memtable.lookup(b"").is_tombstone is True
    assert list(memtable.keys()) == [b"", b"a"]
    assert len(memtable) == 2


def test_sorted_iteration_includes_tombstoned_keys_in_key_order() -> None:
    memtable = Memtable()
    for index in range(10):
        memtable.put(f"key{index}".encode(), f"value{index}".encode())
    for index in (1, 4, 7):
        memtable.delete(f"key{index}".encode())
    memtable.delete(b"never-written")

    expected_keys = [b"never-written"] + [f"key{index}".encode() for index in range(10)]
    expected_keys.sort()
    assert list(memtable.keys()) == expected_keys

    expected_entries = [
        MemtableEntry(
            key=key,
            value=None
            if key == b"never-written" or key in (b"key1", b"key4", b"key7")
            else b"value" + key[3:],
        )
        for key in expected_keys
    ]
    assert list(memtable.entries()) == expected_entries
    assert list(memtable) == expected_entries
    assert len(memtable) == len(expected_keys)

    # Tombstones are counted as records, since a flush has to write them out.
    assert sum(1 for entry in memtable.entries() if entry.is_tombstone) == 4


def test_iterating_an_empty_memtable_yields_nothing() -> None:
    memtable = Memtable()

    assert list(memtable.entries()) == []
    assert list(memtable.keys()) == []
    assert len(memtable) == 0


def test_membership_is_refused_rather_than_answered_wrongly() -> None:
    # Memtable.__iter__ yields records, so an inherited membership test would
    # compare a key against entries and report False for a key that is present.
    memtable = Memtable()
    memtable.put(b"key", b"v")

    with pytest.raises(TypeError, match="does not support 'in'"):
        b"key" in memtable  # noqa: B015


@pytest.mark.parametrize("bad_key", ["text", 1, None, bytearray(b"mutable"), memoryview(b"view")])
def test_memtable_rejects_non_bytes_keys(bad_key: object) -> None:
    memtable = Memtable()

    with pytest.raises(TypeError, match="key must be bytes"):
        memtable.put(bad_key, b"v")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="key must be bytes"):
        memtable.get(bad_key)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="key must be bytes"):
        memtable.lookup(bad_key)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="key must be bytes"):
        memtable.delete(bad_key)  # type: ignore[arg-type]


@pytest.mark.parametrize("bad_value", ["text", 1, None, bytearray(b"mutable")])
def test_memtable_rejects_non_bytes_values(bad_value: object) -> None:
    memtable = Memtable()

    with pytest.raises(TypeError, match="value must be bytes"):
        memtable.put(b"key", bad_value)  # type: ignore[arg-type]


def test_a_rejected_put_leaves_the_memtable_untouched() -> None:
    memtable = Memtable()
    memtable.put(b"key", b"value")

    with pytest.raises(TypeError):
        memtable.put(b"key", "not bytes")  # type: ignore[arg-type]

    assert len(memtable) == 1
    assert memtable.get(b"key") == b"value"


def test_an_entry_cannot_be_mutated_by_its_holder() -> None:
    memtable = Memtable()
    memtable.put(b"key", b"value")
    entry = memtable.lookup(b"key")

    with pytest.raises(AttributeError):
        entry.value = b"tampered"  # type: ignore[misc]

    assert memtable.get(b"key") == b"value"


def test_a_record_with_an_unknown_tag_is_reported_rather_than_misread() -> None:
    """Unreachable through the API, so it is provoked by writing the bytes directly."""
    memtable = Memtable()
    memtable._skiplist.insert(b"key", b"\x7fvalue")

    with pytest.raises(ValueError, match="unknown memtable tag"):
        memtable.lookup(b"key")
    with pytest.raises(ValueError, match="unknown memtable tag"):
        list(memtable.entries())


def test_a_tombstone_carrying_a_value_is_reported_rather_than_misread() -> None:
    memtable = Memtable()
    memtable._skiplist.insert(b"key", _TAG_DELETE + b"stowaway")

    with pytest.raises(ValueError, match="tombstone record"):
        memtable.lookup(b"key")


def test_an_empty_stored_record_is_reported_rather_than_misread() -> None:
    memtable = Memtable()
    memtable._skiplist.insert(b"key", b"")

    with pytest.raises(ValueError, match="unknown memtable tag"):
        memtable.lookup(b"key")


def test_memtable_forwards_skip_list_tuning() -> None:
    memtable = Memtable(max_level=4, level_probability=0.5, rng=_AlwaysPromote())
    for index in range(20):
        memtable.put(f"key{index:02d}".encode(), b"v")

    assert memtable._skiplist.max_level == 4
    assert memtable._skiplist.level_probability == 0.5
    assert memtable._skiplist.level == 4
    assert_structure_is_sound(memtable._skiplist)


def test_default_tuning_matches_the_skip_list_defaults() -> None:
    memtable = Memtable()

    assert memtable._skiplist.max_level == DEFAULT_MAX_LEVEL
    assert memtable._skiplist.level_probability == DEFAULT_LEVEL_PROBABILITY


@pytest.mark.parametrize("seed", [0, 3, 11, 42, 2024])
def test_randomized_puts_and_deletes_match_a_tombstone_aware_reference(seed: int) -> None:
    """Differential test against a dict that models tombstones as a None value.

    Checked after every operation, so a divergence is reported where it happened.
    The reference deliberately never drops a key: at this layer a delete only
    ever replaces a value with a tombstone, so a reference that popped the key
    would be modelling the skip list's delete, not the memtable's.
    """
    rng = random.Random(seed)
    memtable = Memtable(rng=random.Random(seed + 1))
    reference: dict[bytes, bytes | None] = {}

    key_space = [f"k{index:02d}".encode() for index in range(30)]

    for step in range(1200):
        key = rng.choice(key_space)
        action = rng.choices(["put", "delete", "get"], weights=[5, 3, 2])[0]

        if action == "put":
            value = f"v{step}".encode()
            memtable.put(key, value)
            reference[key] = value
        elif action == "delete":
            memtable.delete(key)
            reference[key] = None
        else:
            assert memtable.get(key) == reference.get(key)

        assert len(memtable) == len(reference)
        assert memtable.get(key) == reference.get(key)

        expected_entry = (
            None if key not in reference else MemtableEntry(key=key, value=reference[key])
        )
        assert memtable.lookup(key) == expected_entry

        assert list(memtable.entries()) == [
            MemtableEntry(key=reference_key, value=reference_value)
            for reference_key, reference_value in sorted(reference.items())
        ]

    assert_structure_is_sound(memtable._skiplist)


@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    operations=st.lists(
        st.tuples(
            st.sampled_from(["put", "delete", "get"]),
            st.binary(min_size=0, max_size=6),
            st.binary(min_size=0, max_size=6),
        ),
        max_size=120,
    ),
    seed=st.integers(min_value=0, max_value=2**32 - 1),
)
def test_arbitrary_put_delete_sequences_match_a_tombstone_aware_reference(
    operations: list[tuple[str, bytes, bytes]], seed: int
) -> None:
    """Same differential check over arbitrary key and value bytes.

    Values include empty ones, which is where a tombstone representation that
    confused "no value" with "deleted" would show up.
    """
    memtable = Memtable(rng=random.Random(seed))
    reference: dict[bytes, bytes | None] = {}

    for action, key, value in operations:
        if action == "put":
            memtable.put(key, value)
            reference[key] = value
        elif action == "delete":
            memtable.delete(key)
            reference[key] = None
        else:
            assert memtable.get(key) == reference.get(key)

        assert len(memtable) == len(reference)

    assert list(memtable.entries()) == [
        MemtableEntry(key=reference_key, value=reference_value)
        for reference_key, reference_value in sorted(reference.items())
    ]
    assert list(memtable.keys()) == sorted(reference)
    assert_structure_is_sound(memtable._skiplist)


# ---------------------------------------------------------------------------
# Story M2.3: prototype validating the concurrency decision
#
# The decision (see memtable.py's "Concurrency strategy" section) is a single
# writer lock plus a lock-free read path, with correctness coming from the order
# the writer publishes its stores in. These tests are the "only as much
# prototype code as needed" part of the spike: they check the four premises that
# argument rests on, and they prototype the writer lock here in the test module
# rather than in SkipList, because adding it to SkipList is story M2.4's job and
# a spike that quietly shipped the implementation would leave nothing to review.
# ---------------------------------------------------------------------------

SPIKE_KEY_COUNT = 1500
"""Keys the spike's writer thread inserts while readers run against it.

Large enough that the writer is still going long after the readers start, which
is the only state in which the readers are testing anything, and small enough
that the whole module stays fast.
"""

SPIKE_READER_COUNT = 4
"""Reader threads run against the single writer."""

THREAD_JOIN_TIMEOUT = 60.0
"""Seconds to wait for a spike thread before calling it stuck.

A test that deadlocks should fail with a readable message rather than hang a CI
run until something else kills it, so every join is bounded.
"""

EVENT_WAIT_TIMEOUT = 10.0
"""Seconds to wait on a handoff between spike threads before calling it blocked.

Generous on purpose. The blocking this bounds is a reader stuck behind the
writer's lock, which never clears on its own, so a slow machine cannot turn into
a false failure by being slow.
"""


def spike_value_for(key: bytes) -> bytes:
    """Return the one value a given key is ever stored with during the spike.

    Deriving the value from the key is what lets a reader check what it read
    without coordinating with the writer: any answer other than this value or
    ``None`` means the read saw something that was never written.
    """
    return b"value-for-" + key


@contextlib.contextmanager
def forced_thread_interleaving(interval: float = 1e-6) -> Iterator[None]:
    """Shorten the interpreter's thread switch interval for the duration.

    Left at its default, a thread usually runs for milliseconds before being
    switched out, so a writer can finish a whole insert between two of a
    reader's steps and the interleavings the decision is about would hardly ever
    be sampled. Cutting the interval forces switches inside the linking loop,
    which is where a publication-order mistake would show up.
    """
    previous = sys.getswitchinterval()
    sys.setswitchinterval(interval)
    try:
        yield
    finally:
        sys.setswitchinterval(previous)


def start_together(parties: int) -> threading.Barrier:
    """Return a barrier that releases the spike's threads at the same moment.

    Without it the writer, which is started first, can get most of the way
    through its inserts before a reader thread is scheduled at all, and the test
    would then be reading a structure nobody is writing to while still reporting
    a pass. The barrier is what makes the overlap assertion below meaningful
    rather than hopeful.
    """
    return threading.Barrier(parties, timeout=EVENT_WAIT_TIMEOUT)


def assert_readers_overlapped_the_writer(
    reader_operations: list[int],
    sizes_at_first_read: list[int],
    total_keys: int = SPIKE_KEY_COUNT,
) -> None:
    """Assert the readers really did run against a structure still being built.

    A concurrency test's most likely failure is not a wrong answer but a silent
    loss of concurrency: if the readers only start once the writer is done, every
    assertion inside them still passes and the test goes green while testing
    nothing. Checking both that each reader got work in and that at least one of
    them saw the structure only part built is what closes that hole.

    ``total_keys`` is how many records the writer will have written by the end,
    so the M2.4 tests below can reuse this with their own workload sizes.
    """
    assert all(count > 0 for count in reader_operations), "a reader thread never ran"
    assert min(sizes_at_first_read) < total_keys // 2, (
        f"every reader started after the writer had already inserted "
        f"{min(sizes_at_first_read)} of {total_keys} keys, so nothing was read "
        f"concurrently with a write"
    )


def run_in_threads(targets: list[Callable[[], None]]) -> None:
    """Run each callable in its own thread and re-raise on the main thread.

    An assertion that fails inside a thread is otherwise printed to stderr and
    forgotten, leaving the test green. That is the usual way a concurrency test
    ends up asserting nothing at all, so failures are collected and re-raised
    here, and a thread that never finishes is reported as stuck rather than
    hanging the run.
    """
    failures: list[Exception] = []
    failures_lock = threading.Lock()

    def guarded(target: Callable[[], None]) -> Callable[[], None]:
        def run() -> None:
            try:
                target()
            except Exception as error:
                with failures_lock:
                    failures.append(error)

        return run

    # Daemon threads, so that the join timeout below is the whole story. A
    # non-daemon thread that is stuck would be reported here and then hang the
    # interpreter again on the way out, because shutdown joins it with no
    # timeout, turning a failed test into a hung test run.
    threads = [threading.Thread(target=guarded(target), daemon=True) for target in targets]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(THREAD_JOIN_TIMEOUT)

    # Failures first: a thread that is still running is very often a thread
    # waiting on a handoff from one that already died, so reporting the stuck
    # thread ahead of the exception would hide the reason it is stuck.
    if failures:
        raise failures[0]

    stuck = [thread for thread in threads if thread.is_alive()]
    assert not stuck, f"{len(stuck)} thread(s) did not finish within {THREAD_JOIN_TIMEOUT}s"


class PrototypeLockedWriter:
    """The writer half of the M2.3 decision, prototyped around an unlocked SkipList.

    Every mutating call takes one lock for the whole call, which is the
    granularity the decision settles on. Readers are deliberately given no way
    in here: they are expected to call the skip list directly and take nothing,
    and a test that made them go through this wrapper would be testing a design
    nobody proposed.
    """

    def __init__(self, skiplist: SkipList) -> None:
        self._skiplist = skiplist
        self._lock = threading.Lock()

    @property
    def lock(self) -> threading.Lock:
        """The writer lock, exposed so a test can hold it and watch readers proceed."""
        return self._lock

    def insert(self, key: bytes, value: bytes) -> None:
        """Insert under the lock, as :meth:`SkipList.insert` will once M2.4 lands."""
        with self._lock:
            self._skiplist.insert(key, value)


def iterate_nodes(skiplist: SkipList) -> list[_Node]:
    """Return every node on level 0, which is the chain that holds all of them."""
    nodes: list[_Node] = []
    node = skiplist._head.forward[0]
    while node is not None:
        nodes.append(node)
        node = node.forward[0]
    return nodes


def assert_every_level_is_ascending(skiplist: SkipList) -> None:
    """Assert each level is sorted and duplicate free, without the size checks.

    This is the part of :func:`assert_structure_is_sound` that has to hold at
    every single instant, including halfway through an insert. The rest of that
    function does not: the cached size is bumped after linking, and the reported
    level after that, so a mid-flight snapshot legitimately disagrees with both.
    """
    for index in range(skiplist.max_level):
        chain: list[bytes] = []
        node = skiplist._head.forward[index]
        while node is not None:
            chain.append(node.key)
            node = node.forward[index]

        assert chain == sorted(chain), f"level {index} is not in ascending key order"
        assert len(chain) == len(set(chain)), f"level {index} holds a duplicate key"


class PublicationSpy(list):
    """A node's ``forward`` list that reports every store a writer makes into it.

    Wrapping the list is what makes the check happen at the instant of
    publication. The alternative, inspecting the structure after the insert
    returns, cannot tell a writer that linked a node correctly from one that
    published it before filling it in, because both leave the same final state
    and only the second is a bug.
    """

    def __init__(
        self,
        items: list[_Node | None],
        before_store: Callable[[list[_Node | None], int, _Node | None], None],
        after_store: Callable[[], None],
    ) -> None:
        super().__init__(items)
        self.before_store = before_store
        self.after_store = after_store

    def __setitem__(self, index: int, value: _Node | None) -> None:
        self.before_store(self, index, value)
        super().__setitem__(index, value)
        self.after_store()


def test_spike_a_node_is_fully_linked_at_a_level_before_anything_points_at_it() -> None:
    """Premises 1 and 3: publication order, and the level bump coming last.

    The list is built with every node at level 1, then one node is forced to the
    full height, so all six of its links are published in a single insert and the
    order they happen in is observable.
    """
    skiplist = SkipList(max_level=6, rng=_NeverPromote())
    for key in [b"a", b"c", b"e", b"g"]:
        skiplist.insert(key, spike_value_for(key))
    assert skiplist.level == 1

    stores: list[tuple[int, int]] = []

    def before_store(target: list[_Node | None], index: int, value: _Node | None) -> None:
        assert isinstance(value, _Node), "the insert published something that is not a node"
        # Premise 1: the node already points at the successor it is taking over,
        # so a reader that reaches it here finds it whole rather than dangling.
        assert value.forward[index] is target[index], (
            f"node {value.key!r} was published at level {index} before its own "
            f"forward pointer for that level was set"
        )
        # Premise 3: record the lane being linked and the level the structure
        # still reported at that moment, so the bump can be placed afterwards.
        stores.append((index, skiplist.level))

    def after_store() -> None:
        assert_every_level_is_ascending(skiplist)

    for node in [skiplist._head, *iterate_nodes(skiplist)]:
        node.forward = PublicationSpy(node.forward, before_store, after_store)

    skiplist._rng = _AlwaysPromote()
    skiplist.insert(b"d", spike_value_for(b"d"))

    assert [index for index, _ in stores] == [0, 1, 2, 3, 4, 5], (
        "levels were not linked from the bottom upwards"
    )
    assert {level for _, level in stores} == {1}, (
        "the reported level rose before the new top lane had been linked"
    )
    assert skiplist.level == 6
    assert_structure_is_sound(skiplist)
    assert skiplist.search(b"d") == spike_value_for(b"d")


def test_spike_an_unlinked_node_still_leads_back_into_the_live_list() -> None:
    """Premise 4: a removed node keeps its forward pointers rather than dropping them.

    This is what stops a reader that had already stepped onto a node from
    walking off the end when that node is unlinked underneath it.
    """
    skiplist = SkipList()
    for key in [b"a", b"b", b"c", b"d"]:
        skiplist.insert(key, spike_value_for(key))

    stranded = skiplist._find_node(b"b")
    assert stranded is not None
    assert skiplist.delete(b"b") is True

    walked: list[bytes] = []
    node: _Node | None = stranded
    while node is not None:
        walked.append(node.key)
        node = node.forward[0]

    assert walked == [b"b", b"c", b"d"], "an unlinked node no longer leads into the live list"


def test_spike_a_reader_never_sees_a_wrong_value_while_a_writer_inserts() -> None:
    """Premises 1 and 2 under real threads: reads are correct or absent, never wrong.

    Every key has exactly one value it is ever stored with, so a reader can
    check its own answer. Anything other than that value or ``None`` means the
    read observed a node that was not yet whole.
    """
    skiplist = SkipList()
    keys = [f"key{index:05d}".encode() for index in range(SPIKE_KEY_COUNT)]
    insertion_order = random.Random(20260918).sample(keys, len(keys))

    writer_finished = threading.Event()
    started = start_together(SPIKE_READER_COUNT + 1)
    lookups = [0] * SPIKE_READER_COUNT
    sizes_at_first_lookup = [SPIKE_KEY_COUNT] * SPIKE_READER_COUNT

    def writer() -> None:
        try:
            started.wait()
            for key in insertion_order:
                skiplist.insert(key, spike_value_for(key))
        finally:
            # Set from a finally so that a writer which raises cannot leave the
            # readers spinning until the join timeout hides the real failure.
            writer_finished.set()

    def reader_for(slot: int) -> Callable[[], None]:
        def run() -> None:
            rng = random.Random(slot)
            seen = 0
            started.wait()
            sizes_at_first_lookup[slot] = len(skiplist)
            while not writer_finished.is_set():
                key = rng.choice(keys)
                found = skiplist.search(key)
                assert found is None or found == spike_value_for(key), (
                    f"read of {key!r} returned {found!r}, which was never written"
                )
                seen += 1
            lookups[slot] = seen

        return run

    with forced_thread_interleaving():
        run_in_threads([writer, *(reader_for(slot) for slot in range(SPIKE_READER_COUNT))])

    assert_readers_overlapped_the_writer(lookups, sizes_at_first_lookup)
    for key in keys:
        assert skiplist.search(key) == spike_value_for(key), f"insert of {key!r} was lost"
    assert len(skiplist) == len(keys)
    assert_structure_is_sound(skiplist)


def test_spike_concurrent_iteration_stays_sorted_while_a_writer_inserts() -> None:
    """Premise 1 again, from the angle a flush will use: iteration stays ordered.

    A node published before its forward pointer was set would show up here as a
    snapshot that is out of order or holds a key twice, which is the failure
    that would matter most later, since a flush streams this iterator straight
    into an SSTable's data block and would write an unsorted table.
    """
    skiplist = SkipList()
    keys = [f"key{index:05d}".encode() for index in range(SPIKE_KEY_COUNT)]
    key_set = set(keys)
    insertion_order = random.Random(4242).sample(keys, len(keys))

    writer_finished = threading.Event()
    started = start_together(SPIKE_READER_COUNT + 1)
    snapshots = [0] * SPIKE_READER_COUNT
    sizes_at_first_snapshot = [SPIKE_KEY_COUNT] * SPIKE_READER_COUNT

    def writer() -> None:
        try:
            started.wait()
            for key in insertion_order:
                skiplist.insert(key, spike_value_for(key))
        finally:
            writer_finished.set()

    def reader_for(slot: int) -> Callable[[], None]:
        def run() -> None:
            taken = 0
            started.wait()
            sizes_at_first_snapshot[slot] = len(skiplist)
            while not writer_finished.is_set():
                # The size is incremented only after a node is fully linked, so
                # every one of these keys is already on the level 0 chain and a
                # walk starting now has to reach all of them. A walk that comes
                # back short ran off a forward pointer that had not been filled
                # in yet, which is the symptom of a publication-order bug that
                # sortedness alone cannot see.
                at_least = len(skiplist)
                observed = list(skiplist.keys())
                assert observed == sorted(observed), "iteration yielded keys out of order"
                assert len(observed) == len(set(observed)), "iteration yielded a key twice"
                assert set(observed) <= key_set, "iteration yielded a key nobody wrote"
                assert len(observed) >= at_least, (
                    f"iteration reached {len(observed)} keys but {at_least} were already "
                    f"linked when it started, so the walk ended early"
                )
                taken += 1
            snapshots[slot] = taken

        return run

    with forced_thread_interleaving():
        run_in_threads([writer, *(reader_for(slot) for slot in range(SPIKE_READER_COUNT))])

    assert_readers_overlapped_the_writer(snapshots, sizes_at_first_snapshot)
    assert list(skiplist.keys()) == sorted(keys)
    assert_structure_is_sound(skiplist)


def test_spike_readers_do_not_wait_on_the_prototype_writer_lock() -> None:
    """The point of the whole decision: a reader does not queue behind a writer.

    One thread takes the writer lock and holds it. If the read path took that
    lock too, every reader would stop dead until it was released, so the readers
    finishing while it is still held is the observable difference between this
    design and simply wrapping the structure in one mutex.
    """
    skiplist = SkipList()
    keys = [f"key{index:04d}".encode() for index in range(200)]
    for key in keys:
        skiplist.insert(key, spike_value_for(key))

    writer = PrototypeLockedWriter(skiplist)
    lock_held = threading.Event()
    may_release = threading.Event()
    readers_done = [threading.Event() for _ in range(SPIKE_READER_COUNT)]

    def hold_the_lock() -> None:
        with writer.lock:
            lock_held.set()
            assert may_release.wait(EVENT_WAIT_TIMEOUT), "the readers never reported finishing"

    def reader_for(slot: int) -> Callable[[], None]:
        def run() -> None:
            assert lock_held.wait(EVENT_WAIT_TIMEOUT), "the writer never took its lock"
            for key in keys:
                assert skiplist.search(key) == spike_value_for(key)
            readers_done[slot].set()

        return run

    def release_once_readers_are_done() -> None:
        try:
            for slot, done in enumerate(readers_done):
                assert done.wait(EVENT_WAIT_TIMEOUT), (
                    f"reader {slot} did not finish its lookups while the writer held the "
                    f"lock, so the read path is blocking on the writer lock"
                )
        finally:
            # Released from a finally so that a failed assertion above still
            # frees the holder thread instead of leaving it stuck until its own
            # timeout, which would bury this message under a second failure.
            may_release.set()

    run_in_threads(
        [hold_the_lock, *(reader_for(slot) for slot in range(SPIKE_READER_COUNT))]
        + [release_once_readers_are_done]
    )

    assert all(done.is_set() for done in readers_done)


def test_spike_the_prototype_writer_lock_lets_two_writers_share_the_structure() -> None:
    """The writer half: one lock for the whole mutating call is enough.

    Two writer threads with readers alongside. The decision says nothing finer
    grained is needed, so the check is that a single whole-call lock loses no
    write and leaves the express lanes sound, which is where an insert racing
    another insert's relinking would show up.

    The two writers deliberately interleave across the whole key space rather
    than taking a range each. Given a range each they would spend almost all
    their time relinking different stretches of the list, so the predecessors
    one computed would rarely be the ones the other is changing, and the test
    would pass just as happily with no lock at all. Alternating keys puts them in
    each other's way constantly, which is the state the lock exists for.
    """
    skiplist = SkipList()
    writer = PrototypeLockedWriter(skiplist)
    keys = [f"key{index:05d}".encode() for index in range(SPIKE_KEY_COUNT)]
    shuffled = random.Random(90210).sample(keys, len(keys))
    first_share = shuffled[0::2]
    second_share = shuffled[1::2]

    writers_finished = threading.Event()
    remaining = [2]
    remaining_lock = threading.Lock()

    def writer_for(share: list[bytes]) -> Callable[[], None]:
        def run() -> None:
            try:
                for key in share:
                    writer.insert(key, spike_value_for(key))
            finally:
                with remaining_lock:
                    remaining[0] -= 1
                    if remaining[0] == 0:
                        writers_finished.set()

        return run

    def reader() -> None:
        while not writers_finished.is_set():
            observed = list(skiplist.keys())
            assert observed == sorted(observed), "iteration yielded keys out of order"

    with forced_thread_interleaving():
        run_in_threads([writer_for(first_share), writer_for(second_share), reader])

    for key in keys:
        assert skiplist.search(key) == spike_value_for(key), f"insert of {key!r} was lost"
    assert len(skiplist) == len(keys)
    assert_structure_is_sound(skiplist)


# ---------------------------------------------------------------------------
# Story M2.4: concurrent memtable, single writer and concurrent readers
#
# These run against the locking that now lives inside SkipList, so they test the
# shipped guarantee rather than a prototype. Each one is built to fail loudly if
# the lock were removed, or if it were widened to cover the read path, since a
# concurrency test that would pass either way is the usual way this kind of
# guarantee quietly stops being true.
# ---------------------------------------------------------------------------

STRESS_KEY_COUNT = 5000
"""Records the writer thread produces while the reader threads run against it.

Large enough that the writer is still working long after the readers start, and
small enough to keep this module a few seconds rather than a minute.
"""

STRESS_READER_COUNT = 6
"""Reader threads per stress test, comfortably more than one so that readers are
contending with each other as well as with the writer."""

BLOCKED_READER_WAIT = 0.25
"""Seconds to watch a reader that is expected *not* to finish.

Only used where the reader is provably blocked (something else is holding the
lock it needs), so a slow machine can only make this test more likely to pass
honestly, never flakily fail. The wait is short because nothing is being waited
for: it is the length of the observation, not a timeout.
"""

DELETE_EVERY = 3
"""One key in this many is deleted by the writer in the memtable stress test.

Deleting some but not all keys is what makes a reader's answer checkable: a
tombstone for a key outside the delete set would be a record nobody wrote.
"""


def stress_keys(count: int = STRESS_KEY_COUNT) -> list[bytes]:
    """Return the key space for a stress test, in ascending order."""
    return [f"key{index:05d}".encode() for index in range(count)]


@pytest.mark.skipif(
    READS_TAKE_THE_WRITER_LOCK,
    reason="this interpreter has no GIL, so the read path takes the writer lock by design",
)
def test_a_held_writer_lock_does_not_stop_readers_from_finishing() -> None:
    """Criterion 1: a reader does not queue behind a write it does not depend on.

    One thread takes the writer lock and holds it. Every reader has to finish
    all of its lookups before that thread is allowed to let go, so the test can
    only pass if the read path takes no lock at all. Replacing the design with a
    single mutex around the whole structure fails here rather than somewhere
    subtle later.

    Holding the lock directly, rather than parking a writer mid-insert, is
    deliberate: it is the strongest version of the situation, since the lock
    stays held for as long as the readers need instead of for the microseconds
    an insert takes.
    """
    skiplist = SkipList()
    keys = stress_keys(200)
    for key in keys:
        skiplist.insert(key, spike_value_for(key))

    lock_held = threading.Event()
    may_release = threading.Event()
    readers_done = [threading.Event() for _ in range(STRESS_READER_COUNT)]

    def hold_the_writer_lock() -> None:
        with skiplist._lock:
            lock_held.set()
            assert may_release.wait(EVENT_WAIT_TIMEOUT), "the readers never reported finishing"

    def reader_for(slot: int) -> Callable[[], None]:
        def run() -> None:
            assert lock_held.wait(EVENT_WAIT_TIMEOUT), "the writer lock was never taken"
            for key in keys:
                assert skiplist.search(key) == spike_value_for(key)
                assert key in skiplist
            assert list(skiplist.keys()) == keys
            readers_done[slot].set()

        return run

    def release_once_readers_are_done() -> None:
        try:
            for slot, done in enumerate(readers_done):
                assert done.wait(EVENT_WAIT_TIMEOUT), (
                    f"reader {slot} did not finish while the writer lock was held, so the "
                    f"read path is waiting on the writer"
                )
        finally:
            # From a finally, so a failure above still frees the holder thread
            # instead of burying this message under a second, stuck-thread one.
            may_release.set()

    run_in_threads(
        [hold_the_writer_lock]
        + [reader_for(slot) for slot in range(STRESS_READER_COUNT)]
        + [release_once_readers_are_done]
    )

    assert all(done.is_set() for done in readers_done)
    assert_structure_is_sound(skiplist)


def test_readers_and_one_writer_stress_the_skip_list_without_losing_a_write() -> None:
    """Criteria 2 and 3 on the container: sustained load, then every write is there.

    The readers assert on what they see as they see it, which is what catches a
    torn read, and the main thread asserts afterwards on the whole key space,
    which is what catches a lost write. Both halves are needed: readers alone
    would not notice a key that was silently dropped, and the final sweep alone
    would not notice that a reader had briefly been handed a value nobody wrote.
    """
    skiplist = SkipList()
    keys = stress_keys()
    insertion_order = random.Random(20260919).sample(keys, len(keys))

    writer_finished = threading.Event()
    started = start_together(STRESS_READER_COUNT + 1)
    reader_operations = [0] * STRESS_READER_COUNT
    sizes_at_first_read = [STRESS_KEY_COUNT] * STRESS_READER_COUNT

    def writer() -> None:
        try:
            started.wait()
            for key in insertion_order:
                skiplist.insert(key, spike_value_for(key))
        finally:
            # From a finally, so a writer that raises cannot leave the readers
            # spinning until the join timeout hides the real failure.
            writer_finished.set()

    def reader_for(slot: int) -> Callable[[], None]:
        def run() -> None:
            rng = random.Random(slot)
            done = 0
            started.wait()
            sizes_at_first_read[slot] = len(skiplist)
            while not writer_finished.is_set():
                key = rng.choice(keys)
                found = skiplist.search(key)
                assert found is None or found == spike_value_for(key), (
                    f"lookup of {key!r} returned {found!r}, which was never written"
                )
                # Every so often, walk the structure instead of probing it. A
                # publication mistake shows up in a walk as a key out of order
                # or a chain that ends early, neither of which a point lookup
                # for a key that happens to be elsewhere would ever see.
                if done % 50 == 0:
                    linked = len(skiplist)
                    observed = list(skiplist.keys())
                    assert observed == sorted(observed), "iteration yielded keys out of order"
                    assert len(observed) == len(set(observed)), "iteration yielded a key twice"
                    assert len(observed) >= linked, (
                        f"iteration reached {len(observed)} keys but {linked} were already "
                        f"linked when it started, so the walk ended early"
                    )
                done += 1
            reader_operations[slot] = done

        return run

    with forced_thread_interleaving():
        run_in_threads([writer, *(reader_for(slot) for slot in range(STRESS_READER_COUNT))])

    assert_readers_overlapped_the_writer(
        reader_operations, sizes_at_first_read, total_keys=STRESS_KEY_COUNT
    )
    for key in keys:
        assert skiplist.search(key) == spike_value_for(key), f"insert of {key!r} was lost"
    assert len(skiplist) == len(keys)
    assert list(skiplist.keys()) == keys
    assert_structure_is_sound(skiplist)


def test_readers_and_one_writer_stress_the_memtable_including_tombstones() -> None:
    """Criteria 2 and 3 at the layer the engine actually writes through.

    The skip list test above covers inserts of new keys. This one adds the two
    things the engine does that inserts of new keys do not: it overwrites keys
    (a delete is a tombstone written over the value, which takes the update path
    inside insert rather than the linking path) and it asks readers to tell a
    tombstone from an absent record while that is happening.

    A reader here has three honest answers for a key, and one dishonest one. No
    record, the key's one value, and a tombstone are all fine, in that order over
    time; a tombstone for a key the writer never deletes is not, and is what a
    lost or misapplied update would look like.
    """
    memtable = Memtable()
    keys = stress_keys()
    deleted = {key for index, key in enumerate(keys) if index % DELETE_EVERY == 0}
    write_order = random.Random(20260920).sample(keys, len(keys))

    writer_finished = threading.Event()
    started = start_together(STRESS_READER_COUNT + 1)
    reader_operations = [0] * STRESS_READER_COUNT
    sizes_at_first_read = [STRESS_KEY_COUNT] * STRESS_READER_COUNT

    def writer() -> None:
        try:
            started.wait()
            for key in write_order:
                memtable.put(key, spike_value_for(key))
                if key in deleted:
                    memtable.delete(key)
        finally:
            writer_finished.set()

    def reader_for(slot: int) -> Callable[[], None]:
        def run() -> None:
            rng = random.Random(1000 + slot)
            done = 0
            started.wait()
            sizes_at_first_read[slot] = len(memtable)
            while not writer_finished.is_set():
                key = rng.choice(keys)
                entry = memtable.lookup(key)
                if entry is not None and entry.is_tombstone:
                    assert key in deleted, f"{key!r} read as deleted but was never deleted"
                elif entry is not None:
                    assert entry.value == spike_value_for(key), (
                        f"lookup of {key!r} returned {entry.value!r}, which was never written"
                    )
                    assert memtable.get(key) in (spike_value_for(key), None)
                if done % 50 == 0:
                    observed = [record.key for record in memtable.entries()]
                    assert observed == sorted(observed), "iteration yielded records out of order"
                    assert len(observed) == len(set(observed)), "iteration yielded a key twice"
                done += 1
            reader_operations[slot] = done

        return run

    with forced_thread_interleaving():
        run_in_threads([writer, *(reader_for(slot) for slot in range(STRESS_READER_COUNT))])

    assert_readers_overlapped_the_writer(
        reader_operations, sizes_at_first_read, total_keys=STRESS_KEY_COUNT
    )
    for key in keys:
        entry = memtable.lookup(key)
        assert entry is not None, f"record for {key!r} was lost"
        if key in deleted:
            assert entry.is_tombstone, f"delete of {key!r} was lost"
            assert memtable.get(key) is None
        else:
            assert entry.value == spike_value_for(key), f"put of {key!r} was lost"
    assert len(memtable) == len(keys)
    assert list(memtable.keys()) == keys
    assert_structure_is_sound(memtable._skiplist)


def test_two_writer_threads_share_the_structure_without_losing_an_insert() -> None:
    """The writer half of the lock, now that it lives in :meth:`SkipList.insert`.

    The engine runs one writer, so this is not a case it will hit. It is here
    because it is the test that fails if the lock inside ``insert`` is ever
    removed: with one writer the lock is unobservable, and every other test in
    this section would keep passing without it.

    The two writers alternate across the whole key space rather than taking a
    range each. Given a range each they would mostly be relinking different
    stretches of the list and could pass with no lock at all; alternating puts
    them in each other's predecessors constantly, which is the state the lock
    exists for.
    """
    skiplist = SkipList()
    keys = stress_keys()
    shuffled = random.Random(20260921).sample(keys, len(keys))
    shares = [shuffled[0::2], shuffled[1::2]]

    writers_finished = threading.Event()
    remaining = [len(shares)]
    remaining_lock = threading.Lock()

    def writer_for(share: list[bytes]) -> Callable[[], None]:
        def run() -> None:
            try:
                for key in share:
                    skiplist.insert(key, spike_value_for(key))
            finally:
                with remaining_lock:
                    remaining[0] -= 1
                    if remaining[0] == 0:
                        writers_finished.set()

        return run

    def reader() -> None:
        while not writers_finished.is_set():
            observed = list(skiplist.keys())
            assert observed == sorted(observed), "iteration yielded keys out of order"
            assert len(observed) == len(set(observed)), "iteration yielded a key twice"

    with forced_thread_interleaving():
        run_in_threads([writer_for(share) for share in shares] + [reader])

    for key in keys:
        assert skiplist.search(key) == spike_value_for(key), f"insert of {key!r} was lost"
    assert len(skiplist) == len(keys)
    assert_structure_is_sound(skiplist)


def test_every_exit_from_a_mutating_call_releases_the_writer_lock() -> None:
    """A lock left held would deadlock the whole engine, so check every way out.

    Four exits: a rejected argument, the early return when an existing key is
    updated, the early return when a delete finds nothing, and an ordinary
    completed call. Each is followed by a non-blocking acquire, which is the
    only way to ask "is this lock free" without risking a hang in the test that
    is checking for one.
    """
    skiplist = SkipList()

    def assert_lock_is_free(after: str) -> None:
        acquired = skiplist._lock.acquire(blocking=False)
        assert acquired, f"the writer lock was still held after {after}"
        skiplist._lock.release()

    with pytest.raises(TypeError):
        skiplist.insert("not bytes", b"value")  # type: ignore[arg-type]
    assert_lock_is_free("a rejected insert")

    with pytest.raises(TypeError):
        skiplist.delete("not bytes")  # type: ignore[arg-type]
    assert_lock_is_free("a rejected delete")

    skiplist.insert(b"key", b"first")
    assert_lock_is_free("an insert of a new key")

    skiplist.insert(b"key", b"second")
    assert_lock_is_free("an insert that updated an existing key")

    assert skiplist.delete(b"absent") is False
    assert_lock_is_free("a delete of an absent key")

    assert skiplist.delete(b"key") is True
    assert_lock_is_free("a delete that removed a key")


def test_a_memtable_write_is_still_usable_from_a_second_thread_afterwards() -> None:
    """The same check one layer up, where a stranded lock would be hardest to see.

    :class:`Memtable` adds no lock of its own, so this is really asking that it
    has not grown one by accident, and that a rejected put leaves the skip list
    underneath it writable from another thread rather than wedged.
    """
    memtable = Memtable()

    with pytest.raises(TypeError):
        memtable.put(b"key", "not bytes")  # type: ignore[arg-type]

    def write_from_another_thread() -> None:
        memtable.put(b"key", b"value")
        memtable.delete(b"other")

    run_in_threads([write_from_another_thread])

    assert memtable.get(b"key") == b"value"
    assert memtable.lookup(b"other") == MemtableEntry(key=b"other", value=None)


def test_the_read_path_takes_no_lock_on_an_interpreter_that_has_a_gil() -> None:
    """The flag that decides all of this is derived, not guessed.

    Asserting on the flag as well as on the guard keeps the two from drifting:
    a guard that stopped matching the flag would leave the skip test above
    silently skipping on a build it should run on, or the fallback tests below
    testing the wrong path.

    ``sys._is_gil_enabled`` only exists from 3.13, and the supported floor is
    3.11, so the expected value is derived the same way the module derives it.
    Spelling the fallback out again rather than importing it is the point: an
    interpreter too old to have the function is one that cannot drop the GIL, so
    the answer there is that reads need no lock.
    """
    gil_is_enabled = getattr(sys, "_is_gil_enabled", lambda: True)()
    assert READS_TAKE_THE_WRITER_LOCK == (not gil_is_enabled), (
        "the locked-read flag disagrees with this interpreter's GIL state"
    )

    skiplist = SkipList()
    if READS_TAKE_THE_WRITER_LOCK:
        assert skiplist._read_guard is skiplist._lock
    else:
        assert isinstance(skiplist._read_guard, contextlib.nullcontext)


def test_a_lookup_takes_the_writer_lock_when_there_is_no_gil(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The free-threaded fallback: without a GIL, readers wait rather than guess.

    The premise the lock-free read path rests on is that each of the writer's
    publishing stores is indivisible, which is something the GIL provides and a
    free-threaded build does not. This test cannot remove the GIL, so it sets
    the flag that stands for its absence and checks the consequence: a lookup
    started while the writer lock is held does not return until it is released.

    The reader is provably blocked for the whole of the observation window, so
    a slow machine cannot turn this into a false failure. The wait after the
    release is bounded separately, and generously, because that one really is a
    timeout.
    """
    monkeypatch.setattr(memtable_module, "READS_TAKE_THE_WRITER_LOCK", True)
    skiplist = SkipList()
    skiplist.insert(b"key", spike_value_for(b"key"))
    assert skiplist._read_guard is skiplist._lock

    reading = threading.Event()
    returned = threading.Event()
    found: list[bytes | None] = []

    def reader() -> None:
        # Announced immediately before the call, so the window below is watching
        # a lookup that really is under way. Without this the test would also
        # pass if the reader thread had simply not been scheduled yet, which
        # proves nothing about the lock.
        reading.set()
        found.append(skiplist.search(b"key"))
        returned.set()

    thread = threading.Thread(target=reader, daemon=True)
    with skiplist._lock:
        thread.start()
        assert reading.wait(EVENT_WAIT_TIMEOUT), "the reader thread never started"
        assert not returned.wait(BLOCKED_READER_WAIT), (
            "a lookup completed while the writer lock was held, so the free-threaded "
            "fallback is not taking the lock"
        )

    assert returned.wait(EVENT_WAIT_TIMEOUT), "the lookup never finished after the lock was freed"
    thread.join(THREAD_JOIN_TIMEOUT)
    assert found == [spike_value_for(b"key")]


def test_iteration_does_not_hold_the_writer_lock_when_there_is_no_gil(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fallback snapshots instead of streaming, so a walk cannot strand the lock.

    A lazily yielding walk that held the lock would keep it for as long as the
    caller took to consume it, and a caller that wrote to the memtable while
    iterating would deadlock against itself. Both are caught here by checking
    the lock is free the moment the iterator is handed back, which is also the
    only safe way to check: a test that simply tried to write while iterating
    would hang rather than fail if the lock were held.
    """
    monkeypatch.setattr(memtable_module, "READS_TAKE_THE_WRITER_LOCK", True)
    skiplist = SkipList()
    keys = stress_keys(50)
    for key in keys:
        skiplist.insert(key, spike_value_for(key))

    key_iterator = skiplist.keys()
    item_iterator = skiplist.items()

    acquired = skiplist._lock.acquire(blocking=False)
    assert acquired, "iteration handed back an iterator while still holding the writer lock"
    skiplist._lock.release()

    # Safe now that the lock is known to be free: a snapshot taken before this
    # write must not show it, which is what proves the walk already happened
    # rather than being deferred to the first step of the iterator.
    skiplist.insert(b"zzz-late", spike_value_for(b"zzz-late"))

    assert list(key_iterator) == keys
    assert list(item_iterator) == [(key, spike_value_for(key)) for key in keys]
    assert list(skiplist.keys()) == [*keys, b"zzz-late"]


def test_the_module_docstring_documents_the_concurrency_approach() -> None:
    """Criterion 4: the approach is written down where ARCHITECTURE.md asks for it.

    Section 2 of ARCHITECTURE.md requires the chosen approach to be documented
    in this module, because it is the part of the engine most likely to hold a
    subtle bug and the code alone does not say why it is shaped this way. A test
    is a blunt way to hold a doc in place, but the failure it guards against is
    real: the explanation is the only record of what the lock-free read path
    depends on, and nothing else would notice if it were dropped.
    """
    assert memtable_module.__doc__ is not None
    # Collapsed to single spaces so that rewrapping a paragraph, which changes
    # nothing about what it says, cannot fail this test.
    documented = " ".join(memtable_module.__doc__.split())

    for phrase in [
        "Concurrency strategy",
        "one lock around the whole mutating path, and no lock at all on the read path",
        "sys._is_gil_enabled()",
        # The granularity sketch the spike was asked for, and the premises the
        # read path rests on: what a later reader needs in order to check the
        # design still matches the code.
        "Locking granularity, insert versus search",
        "What the lock-free read side rests on",
    ]:
        assert phrase in documented, f"the strategy section no longer explains {phrase!r}"
