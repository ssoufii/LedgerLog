"""Tests for the skip list backing the memtable.

Covers story M2.1 (insert, search, delete, sorted iteration, and correctness
against a reference sorted structure).

Two kinds of test live here. The behavioural ones go through the public API only,
since that is what the rest of the engine will use. The structural ones reach
into ``_head`` and ``_level`` on purpose: a skip list can answer every public
call correctly while its upper levels are quietly malformed, because level 0
alone is enough to satisfy search and iteration. Checking the express lanes
directly is the only way to catch a linking bug before it turns into a wrong
answer on some later, larger input.

Concurrency is not exercised here, and that is deliberate rather than an
omission: story M2.1 makes no thread-safety claim (``SkipList`` carries no lock),
so there is nothing to verify yet. Stories M2.3 and M2.4 add the concurrency
strategy and the multi-threaded stress test that CLAUDE.md requires of it.
"""

from __future__ import annotations

import random

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ledgerlog.memtable import (
    DEFAULT_LEVEL_PROBABILITY,
    DEFAULT_MAX_LEVEL,
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
