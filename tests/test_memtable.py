"""Tests for the skip list backing the memtable, and the tombstone layer above it.

Covers story M2.1 (insert, search, delete, sorted iteration, and correctness
against a reference sorted structure) and story M2.2 (a delete records a
tombstone instead of removing the key).

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

Concurrency is not exercised here, and that is deliberate rather than an
omission: neither story makes a thread-safety claim, since neither
``SkipList`` nor ``Memtable`` carries a lock, so there is nothing to verify yet.
Stories M2.3 and M2.4 add the concurrency strategy and the multi-threaded stress
test that CLAUDE.md requires of it.
"""

from __future__ import annotations

import random

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ledgerlog.memtable import (
    _TAG_DELETE,
    _TAG_PUT,
    DEFAULT_LEVEL_PROBABILITY,
    DEFAULT_MAX_LEVEL,
    Memtable,
    MemtableEntry,
    SkipList,
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
