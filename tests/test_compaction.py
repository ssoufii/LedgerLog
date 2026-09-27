"""Tests for size-tier grouping and the tier merge, stories M8.1 and M8.2.

M8.1's claims are about metadata, so those tests need no files, no engine and no
threads: a table, to the planner, is an age and a size, and the stand-in below is
exactly that. The engine side of that story, that a table arriving from a flush
re-tiers what the engine holds, is tested against the real engine in
``test_engine.py``, where a real flush can produce the table.

M8.2's claims are about bytes, so the second half of this file writes real tables
and reads the merged one back. It splits the same way the code does: the newest
wins rule is checked over in-memory streams, against expectations computed by hand
and against a reference implementation over random inputs, and the file level
merge is then checked for the things only a file can be wrong about (a valid
table with every section, sources left untouched, nothing left behind when a
damaged source stops the merge partway).

What could pass a loose reading of M8.2 while being false:

* Newest wins can be satisfied by accident when the newest table happens to hold
  every key. The tables below shadow each other in both directions, and some keys
  live only in the oldest table, so a merge that simply preferred one table would
  fail.
* "Merged output is a valid SSTable" can be read as "the file exists". It is
  checked by reading it back through the real reader, by iterating its data block
  to confirm the key order on disk, by counting its sparse index entries, and by
  asking the bloom filter it carries for every key the table holds.
* A merge that loaded both sources into a dict would pass every correctness test
  here and still be unable to merge a tier larger than memory, which is the case
  compaction exists for. Pinned by counting how many records a merge pulls before
  it yields its first pair.
* Tombstones are the one thing a merge must keep for now (dropping them is M8.3),
  and a merge that dropped them would look tidier and resurrect deleted keys.
  Checked in the merged output on disk, not just in the stream.

What could pass inspection while being false, and how each is pinned here:

* "Comparable size" is the claim most easily satisfied loosely. A grouping that
  chained tables together, admitting each one as long as it was comparable to the
  previous table rather than to the tier, would put a 1000 byte and a 100 byte
  table in one tier by way of the sizes in between, and every test over a handful
  of well separated sizes would still pass. So the bound is checked as a bound, on
  every pair in every tier, over random size sets as well as hand-computed ones.
* A grouping can also be right on average and unstable: equally sized tables are
  interchangeable, so a planner that leaked its input's iteration order into the
  output would give two different plans for the same tables. Tested by shuffling.
* The trigger could fire off the total number of tables rather than per tier,
  which every single-tier test would agree with. Tested with one full tier beside
  one that is not.
* The order the plan hands tiers back in is not decoration: story M8.2 merges
  ``next_tier`` and story M8.5 deletes its files, so "newest first within a tier"
  is what newest-write-wins will mean, and picking the wrong ready tier is a real
  cost in bytes rewritten. Both orders are asserted directly.

The hypothesis test is the partition check. Grouping is a sweep, and the failures
a sweep has are dropping a table at a boundary or admitting one to two tiers,
neither of which a fixed example set reliably lands on.
"""

from __future__ import annotations

import math
import os
import random
import struct
import threading
from collections.abc import Iterator, Sequence
from dataclasses import FrozenInstanceError, dataclass
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ledgerlog.compaction import (
    DEFAULT_MIN_TIER_TABLES,
    DEFAULT_SIZE_RATIO,
    CompactionPlan,
    CompactionPolicy,
    CompactionTier,
    MergeableTable,
    MergeSourceOrderError,
    SizedTable,
    merge_records,
    merge_sstables,
    merge_tier,
    plan_compaction,
)
from ledgerlog.sstable import (
    FILE_HEADER_SIZE,
    FOOTER_SIZE,
    SSTableFooter,
    SSTableIncompleteError,
    SSTableReader,
    SSTableRecord,
    SSTableStatus,
    SSTableTruncatedRecordError,
    SSTableUnsupportedVersionError,
    inspect_sstable,
    iter_records,
    read_bloom_filter,
    read_footer,
    write_sstable,
)


@dataclass(frozen=True)
class StubTable:
    """An age and a size, which is all :func:`plan_compaction` is allowed to need.

    Frozen and comparable so that two plans built from the same tables compare
    equal, which is what the stability tests below assert on.
    """

    sequence: int
    size_bytes: int


def tables(*sizes: int, first_sequence: int = 0) -> list[StubTable]:
    """Tables of the given sizes, numbered oldest first in the order written.

    The numbering is separate from the sizes on purpose: age and size are
    independent in a real directory (a merged table is old and large, a fresh
    flush is new and small), and tests that let the two agree could not catch a
    grouping that sorted by the wrong one.
    """
    return [
        StubTable(sequence=first_sequence + index, size_bytes=size)
        for index, size in enumerate(sizes)
    ]


def tier_sizes(plan: CompactionPlan[StubTable]) -> list[list[int]]:
    """The plan as plain numbers: one list of sizes per tier, in the plan's order.

    Each tier's sizes come out in the tier's own order, which is newest table
    first, so they are in age order and not in size order. The expectations below
    are written that way deliberately: they would not catch a tier that came back
    in size order if they were sorted before comparing.
    """
    return [[table.size_bytes for table in tier.tables] for tier in plan.tiers]


# ---------------------------------------------------------------------------
# The policy: the two configurable numbers, and the size comparison itself.
# ---------------------------------------------------------------------------


def test_the_default_policy_is_the_documented_pair() -> None:
    policy = CompactionPolicy()

    assert policy.size_ratio == DEFAULT_SIZE_RATIO
    assert policy.min_tier_tables == DEFAULT_MIN_TIER_TABLES


def test_the_default_ratio_stays_below_the_default_threshold() -> None:
    """The relationship the module's docstring rests on, asserted so it cannot drift.

    A merged tier's output is about ``min_tier_tables`` times one source, so a
    ratio at or above the threshold would let that output rejoin the tier it was
    merged from and be merged again forever.
    """
    assert DEFAULT_SIZE_RATIO < DEFAULT_MIN_TIER_TABLES


def test_an_integer_ratio_is_kept_as_the_equal_float() -> None:
    assert CompactionPolicy(size_ratio=2) == CompactionPolicy(size_ratio=2.0)
    assert isinstance(CompactionPolicy(size_ratio=3).size_ratio, float)


def test_the_policy_cannot_be_changed_after_it_is_built() -> None:
    policy = CompactionPolicy()

    with pytest.raises(FrozenInstanceError):
        policy.size_ratio = 8.0  # type: ignore[misc]


@pytest.mark.parametrize("ratio", [1.0, 0.5, 0.0, -2.0])
def test_a_ratio_that_groups_nothing_is_refused(ratio: float) -> None:
    with pytest.raises(ValueError, match="greater than 1"):
        CompactionPolicy(size_ratio=ratio)


@pytest.mark.parametrize("ratio", [math.inf, math.nan, -math.inf])
def test_a_non_finite_ratio_is_refused(ratio: float) -> None:
    with pytest.raises(ValueError):
        CompactionPolicy(size_ratio=ratio)


@pytest.mark.parametrize("ratio", ["2.0", None, True])
def test_a_ratio_that_is_not_a_number_is_refused(ratio: object) -> None:
    with pytest.raises(TypeError, match="size_ratio"):
        CompactionPolicy(size_ratio=ratio)  # type: ignore[arg-type]


@pytest.mark.parametrize("threshold", [1, 0, -1])
def test_a_threshold_below_two_is_refused(threshold: int) -> None:
    """One table is not a group: merging it would rewrite every record to no effect."""
    with pytest.raises(ValueError, match="min_tier_tables"):
        CompactionPolicy(min_tier_tables=threshold)


@pytest.mark.parametrize("threshold", [4.0, "4", None, True])
def test_a_threshold_that_is_not_an_int_is_refused(threshold: object) -> None:
    with pytest.raises(TypeError, match="min_tier_tables"):
        CompactionPolicy(min_tier_tables=threshold)  # type: ignore[arg-type]


def test_equal_sizes_are_comparable_whatever_the_ratio() -> None:
    policy = CompactionPolicy(size_ratio=1.5)

    assert policy.fits_tier(1000, 1000) is True
    assert policy.fits_tier(0, 0) is True


def test_a_size_inside_the_ratio_is_comparable_and_one_at_the_ratio_is_not() -> None:
    """The boundary is exclusive, which is the case a merge lands on exactly."""
    policy = CompactionPolicy(size_ratio=2.0)

    assert policy.fits_tier(1000, 501) is True
    assert policy.fits_tier(1000, 500) is False
    assert policy.fits_tier(1000, 499) is False


# ---------------------------------------------------------------------------
# Criterion 1: tables of varying size are grouped into comparably sized tiers,
# per the configured ratio.
# ---------------------------------------------------------------------------


def test_planning_no_tables_gives_an_empty_plan() -> None:
    plan = plan_compaction([])

    assert plan.tiers == ()
    assert plan.table_count == 0
    assert plan.ready_tiers == ()
    assert plan.needs_compaction is False
    assert plan.next_tier is None


def test_one_table_is_a_tier_of_one_that_is_not_ready() -> None:
    plan = plan_compaction(tables(1000))

    assert tier_sizes(plan) == [[1000]]
    assert plan.tiers[0].ready is False
    assert plan.needs_compaction is False


def test_tables_of_the_same_size_land_in_one_tier() -> None:
    plan = plan_compaction(tables(500, 500, 500))

    assert tier_sizes(plan) == [[500, 500, 500]]


def test_varying_sizes_are_split_where_the_ratio_says_they_stop_being_comparable() -> None:
    """The hand-computed case, at the default ratio of two.

    1000 and 900 are within a factor of two of each other, 500 is exactly a factor
    of two from 1000 so it opens the next tier, 450 joins it, and 100 is more than
    a factor of two from 500 so it opens a third.
    """
    plan = plan_compaction(tables(1000, 900, 500, 450, 100))

    assert tier_sizes(plan) == [[100], [450, 500], [900, 1000]]


def test_a_wider_ratio_merges_tiers_that_a_narrow_one_separates() -> None:
    """Criterion 1's "per a configurable ratio": the same tables, two answers."""
    sizes = (1000, 900, 500, 450, 100)

    narrow = plan_compaction(tables(*sizes), CompactionPolicy(size_ratio=1.5))
    wide = plan_compaction(tables(*sizes), CompactionPolicy(size_ratio=10.0))

    assert tier_sizes(narrow) == [[100], [450, 500], [900, 1000]]
    assert tier_sizes(wide) == [[100], [450, 500, 900, 1000]]


def test_every_tier_holds_tables_within_the_ratio_of_each_other() -> None:
    """The bound as a bound, over sizes dense enough to chain if it were applied pairwise.

    Each size here is 1.4 times the one below it, so every neighbouring pair is
    comparable at a ratio of 1.5 while the ends of the list are a factor of five
    apart. A grouping that compared each table to the previous one instead of to
    the tier would return a single tier.
    """
    sizes = [int(100 * 1.4**step) for step in range(5)]

    plan = plan_compaction(tables(*sizes), CompactionPolicy(size_ratio=1.5))

    assert len(plan.tiers) > 1
    for tier in plan.tiers:
        assert tier.largest_size_bytes < 1.5 * tier.smallest_size_bytes


def test_empty_tables_group_together_rather_than_one_tier_each() -> None:
    """Defensive: no SSTable is zero bytes, but a size comparison should not say
    two identical sizes are incomparable."""
    plan = plan_compaction(tables(0, 0, 0))

    assert tier_sizes(plan) == [[0, 0, 0]]


def test_the_plan_does_not_depend_on_the_order_the_tables_arrive_in() -> None:
    """Equal sizes are interchangeable, so the plan must not leak the input's order."""
    original = tables(1000, 900, 500, 500, 500, 100)
    shuffled = list(original)
    random.Random(20260926).shuffle(shuffled)

    assert plan_compaction(shuffled) == plan_compaction(original)


def test_a_tier_lists_its_tables_newest_first() -> None:
    """The order story M8.2 merges in: the first table holding a key holds the winner."""
    # Sizes deliberately at odds with the ages: sequence 0 is the largest table.
    plan = plan_compaction(
        [
            StubTable(sequence=0, size_bytes=1000),
            StubTable(sequence=1, size_bytes=900),
            StubTable(sequence=2, size_bytes=950),
        ]
    )

    assert len(plan.tiers) == 1
    assert plan.tiers[0].sequences == (2, 1, 0)


def test_tiers_are_listed_smallest_first() -> None:
    plan = plan_compaction(tables(4000, 1000, 250))

    assert tier_sizes(plan) == [[250], [1000], [4000]]
    largest = [tier.largest_size_bytes for tier in plan.tiers]
    assert largest == sorted(largest)


def test_a_tier_reports_the_bytes_a_merge_of_it_would_move() -> None:
    plan = plan_compaction(tables(500, 400, 300))

    tier = plan.tiers[0]
    assert tier.table_count == 3
    assert tier.total_size_bytes == 1200
    assert tier.largest_size_bytes == 500
    assert tier.smallest_size_bytes == 300


def test_the_plan_records_the_policy_it_was_made_under() -> None:
    policy = CompactionPolicy(size_ratio=3.0, min_tier_tables=7)

    plan = plan_compaction(tables(10), policy)

    assert plan.policy == policy


def test_the_plan_covers_every_table_it_was_given() -> None:
    given_tables = tables(1000, 900, 500, 450, 100, 90)

    plan = plan_compaction(given_tables)

    planned = [table for tier in plan.tiers for table in tier.tables]
    assert sorted(planned, key=lambda table: table.sequence) == given_tables
    assert plan.table_count == len(given_tables)


@settings(max_examples=200, deadline=None)
@given(
    sizes=st.lists(st.integers(min_value=1, max_value=10**9), min_size=1, max_size=60),
    ratio=st.floats(min_value=1.01, max_value=16.0, allow_nan=False, allow_infinity=False),
    threshold=st.integers(min_value=2, max_value=8),
)
def test_a_plan_partitions_its_tables_into_tiers_that_respect_the_ratio(
    sizes: list[int], ratio: float, threshold: int
) -> None:
    """The invariants that have to hold for any sizes, any ratio, any threshold.

    Partition: every table appears in exactly one tier, and no table is invented.
    Bound: within a tier the largest is within the ratio of the smallest. Order:
    tiers ascend by the size they are grouped around, and tables inside a tier
    descend by age. Readiness: exactly the tiers at or over the threshold.
    """
    policy = CompactionPolicy(size_ratio=ratio, min_tier_tables=threshold)
    planned_tables = tables(*sizes)

    plan = plan_compaction(planned_tables, policy)

    seen = [table for tier in plan.tiers for table in tier.tables]
    assert sorted(seen, key=lambda table: table.sequence) == planned_tables

    for tier in plan.tiers:
        assert tier.tables, "a tier must not be empty"
        assert (
            tier.largest_size_bytes == tier.smallest_size_bytes
            or tier.largest_size_bytes < ratio * tier.smallest_size_bytes
        )
        assert list(tier.sequences) == sorted(tier.sequences, reverse=True)
        assert tier.ready == (tier.table_count >= threshold)

    references = [tier.largest_size_bytes for tier in plan.tiers]
    assert references == sorted(references)


# ---------------------------------------------------------------------------
# Criterion 2: a tier is ready once it holds at least the configured number of
# tables.
# ---------------------------------------------------------------------------


def test_a_tier_one_table_short_of_the_threshold_is_not_ready() -> None:
    plan = plan_compaction(tables(500, 500, 500), CompactionPolicy(min_tier_tables=4))

    assert plan.tiers[0].ready is False
    assert plan.ready_tiers == ()
    assert plan.needs_compaction is False
    assert plan.next_tier is None


def test_a_tier_at_the_threshold_is_ready() -> None:
    plan = plan_compaction(tables(500, 500, 500, 500), CompactionPolicy(min_tier_tables=4))

    assert plan.tiers[0].ready is True
    assert plan.ready_tiers == plan.tiers
    assert plan.needs_compaction is True
    assert plan.next_tier is plan.tiers[0]


def test_a_tier_past_the_threshold_stays_ready() -> None:
    plan = plan_compaction(tables(500, 500, 500, 500, 500), CompactionPolicy(min_tier_tables=4))

    assert plan.tiers[0].ready is True
    assert plan.tiers[0].table_count == 5


def test_the_threshold_is_configurable() -> None:
    sizes = (500, 500)

    strict = plan_compaction(tables(*sizes), CompactionPolicy(min_tier_tables=4))
    eager = plan_compaction(tables(*sizes), CompactionPolicy(min_tier_tables=2))

    assert strict.needs_compaction is False
    assert eager.needs_compaction is True


def test_readiness_is_decided_per_tier_and_not_over_the_whole_set() -> None:
    """Six tables, a threshold of four, and only one tier that has four of them."""
    plan = plan_compaction(
        tables(10_000, 9_000, 500, 500, 500, 500),
        CompactionPolicy(min_tier_tables=4),
    )

    assert [tier.ready for tier in plan.tiers] == [True, False]
    assert tier_sizes(plan) == [[500, 500, 500, 500], [9_000, 10_000]]
    assert plan.ready_tiers == (plan.tiers[0],)


def test_the_next_tier_to_compact_is_the_smallest_ready_one() -> None:
    """Two ready tiers, and the cheap one is the one offered first."""
    plan = plan_compaction(
        tables(10_000, 10_000, 10_000, 10_000, 100, 100, 100, 100),
        CompactionPolicy(min_tier_tables=4),
    )

    assert len(plan.ready_tiers) == 2
    next_tier = plan.next_tier
    assert next_tier is not None
    assert next_tier.largest_size_bytes == 100


# ---------------------------------------------------------------------------
# Criterion 3: adding a table re-evaluates membership and the trigger. The
# engine's flush path is tested in test_engine.py; these are the planner's half,
# which is that a plan is a function of the tables it was given.
# ---------------------------------------------------------------------------


def test_adding_a_table_that_fills_a_tier_flips_the_trigger() -> None:
    policy = CompactionPolicy(min_tier_tables=4)
    held = tables(500, 500, 500)

    before = plan_compaction(held, policy)
    after = plan_compaction([*held, StubTable(sequence=3, size_bytes=500)], policy)

    assert before.needs_compaction is False
    assert after.needs_compaction is True
    assert after.next_tier is not None
    assert after.next_tier.sequences == (3, 2, 1, 0)


def test_adding_a_table_of_a_different_size_re_evaluates_membership() -> None:
    policy = CompactionPolicy(min_tier_tables=4)
    held = tables(500, 500, 500, 500)

    after = plan_compaction([*held, StubTable(sequence=4, size_bytes=100)], policy)

    assert tier_sizes(after) == [[100], [500, 500, 500, 500]]
    assert after.next_tier is not None
    assert after.next_tier.sequences == (3, 2, 1, 0), "the small table joined the ready tier"


def test_a_merged_table_replacing_its_sources_leaves_no_tier_ready() -> None:
    """What story M8.2's output will look like to the planner, at the default ratio.

    Four tables of 500 bytes merge into roughly 2000, which is a factor of four
    from its sources and so lands in a tier of its own rather than back in theirs.
    That is the whole point of the ratio being smaller than the threshold: the
    merge has to make progress.
    """
    policy = CompactionPolicy()
    sources = tables(500, 500, 500, 500)
    assert plan_compaction(sources, policy).needs_compaction is True

    after = plan_compaction([StubTable(sequence=4, size_bytes=2000)], policy)

    assert tier_sizes(after) == [[2000]]
    assert after.needs_compaction is False


def test_a_plan_cannot_be_edited_after_it_is_built() -> None:
    plan = plan_compaction(tables(500, 500))

    with pytest.raises(FrozenInstanceError):
        plan.tiers = ()  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        plan.tiers[0].ready = True  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Inputs the planner refuses. A table's sequence number is its identity to every
# caller above this module, so a repeated or nonsensical one is refused here
# rather than passed on to a merge that would act on it.
# ---------------------------------------------------------------------------


def test_two_tables_with_the_same_sequence_number_are_refused() -> None:
    duplicated = [StubTable(sequence=1, size_bytes=500), StubTable(sequence=1, size_bytes=900)]

    with pytest.raises(ValueError, match="sequence number 1"):
        plan_compaction(duplicated)


@pytest.mark.parametrize(
    ("sequence", "size_bytes", "message"),
    [
        (-1, 500, "sequence must not be negative"),
        (0, -500, "size_bytes must not be negative"),
    ],
)
def test_a_negative_sequence_or_size_is_refused(
    sequence: int, size_bytes: int, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        plan_compaction([StubTable(sequence=sequence, size_bytes=size_bytes)])


@pytest.mark.parametrize(
    ("sequence", "size_bytes"),
    [("0", 500), (0, "500"), (0, 500.0), (True, 500)],
)
def test_a_sequence_or_size_that_is_not_an_int_is_refused(
    sequence: object, size_bytes: object
) -> None:
    with pytest.raises(TypeError):
        plan_compaction([StubTable(sequence=sequence, size_bytes=size_bytes)])  # type: ignore[arg-type]


def test_a_tier_must_hold_at_least_one_table() -> None:
    with pytest.raises(ValueError, match="at least one table"):
        CompactionTier(tables=(), ready=False)


def test_a_tier_must_be_ordered_newest_first() -> None:
    oldest_first = (StubTable(sequence=0, size_bytes=500), StubTable(sequence=1, size_bytes=500))

    with pytest.raises(ValueError, match="newest first"):
        CompactionTier(tables=oldest_first, ready=False)


def test_a_tier_cannot_hold_one_table_twice() -> None:
    same_twice = (StubTable(sequence=1, size_bytes=500), StubTable(sequence=1, size_bytes=500))

    with pytest.raises(ValueError, match="newest first"):
        CompactionTier(tables=same_twice, ready=False)


def test_anything_with_a_sequence_and_a_size_is_a_sized_table() -> None:
    """The protocol is structural on purpose: the planner must not need the engine."""
    assert isinstance(StubTable(sequence=0, size_bytes=1), SizedTable)
    assert not isinstance(object(), SizedTable)


# ---------------------------------------------------------------------------
# The merge, story M8.2: newest wins over sorted streams, with no files yet.
# ---------------------------------------------------------------------------

Pair = tuple[bytes, bytes | None]


def stream(*pairs: Pair) -> Iterator[SSTableRecord]:
    """One source's records, in the order an SSTable's data block holds them.

    The offsets are made up and ascending, because a record read from a real table
    carries the span of the file it came from and the merge must not care: the
    merged table's offsets are the writer's, and a merge that passed a source's
    offsets through would be describing the wrong file.
    """
    return iter(
        [
            SSTableRecord(key=key, value=value, offset=index * 100, end_offset=index * 100 + 100)
            for index, (key, value) in enumerate(pairs)
        ]
    )


def counted(pairs: Sequence[Pair], counter: list[int], rank: int) -> Iterator[SSTableRecord]:
    """A source stream that records how many of its records have been pulled."""
    for record in stream(*pairs):
        counter[rank] += 1
        yield record


def test_a_key_in_several_sources_takes_the_newest_sources_value() -> None:
    """The story's first criterion, over sources that shadow each other both ways.

    ``b"shared"`` is in all three tables and only the newest value may survive;
    ``b"middle"`` is in the two older ones, so the winner is not simply the newest
    table; ``b"oldest"`` is only in the oldest table and must not be lost.
    """
    newest = stream((b"apple", b"n1"), (b"shared", b"n2"))
    middle = stream((b"middle", b"m1"), (b"shared", b"m2"))
    oldest = stream((b"middle", b"o1"), (b"oldest", b"o2"), (b"shared", b"o3"))

    assert list(merge_records([newest, middle, oldest])) == [
        (b"apple", b"n1"),
        (b"middle", b"m1"),
        (b"oldest", b"o2"),
        (b"shared", b"n2"),
    ]


def test_a_newer_tombstone_shadows_an_older_value_and_is_kept() -> None:
    """A tombstone wins its key like any other record, and stays in the output.

    Dropping it is only safe once no older table outside the merge can hold the
    key, which is story M8.3. A merge that dropped it here would let the older
    value below resurface, which is the resurrection ARCHITECTURE.md section 4
    warns about.
    """
    newest = stream((b"gone", None))
    oldest = stream((b"gone", b"value"))

    assert list(merge_records([newest, oldest])) == [(b"gone", None)]


def test_a_newer_value_shadows_an_older_tombstone() -> None:
    """A key deleted and then written again is present, with the newer value."""
    newest = stream((b"back", b"again"))
    oldest = stream((b"back", None))

    assert list(merge_records([newest, oldest])) == [(b"back", b"again")]


def test_sources_with_no_keys_in_common_interleave_into_one_sorted_run() -> None:
    newest = stream((b"a", b"1"), (b"d", b"4"))
    oldest = stream((b"b", b"2"), (b"c", b"3"), (b"e", b"5"))

    assert list(merge_records([newest, oldest])) == [
        (b"a", b"1"),
        (b"b", b"2"),
        (b"c", b"3"),
        (b"d", b"4"),
        (b"e", b"5"),
    ]


def test_one_source_merges_to_its_own_records() -> None:
    pairs: list[Pair] = [(b"a", b"1"), (b"b", None), (b"c", b"3")]

    assert list(merge_records([stream(*pairs)])) == pairs


def test_merging_nothing_yields_nothing() -> None:
    assert list(merge_records([])) == []
    assert list(merge_records([stream(), stream()])) == []


def test_an_empty_source_beside_a_full_one_changes_nothing() -> None:
    assert list(merge_records([stream(), stream((b"a", b"1"))])) == [(b"a", b"1")]


def test_the_merge_holds_one_record_per_source_rather_than_reading_them_in() -> None:
    """The claim that makes a merge usable on a tier larger than memory.

    A merge that loaded its sources would pull every record before yielding
    anything, and would pass every other test in this file. After one pair has
    been taken, each source may have been pulled at most twice: once to seed the
    heap, and once more for the source the pair came from.
    """
    pulled = [0, 0]
    long_run: list[Pair] = [(bytes([index]), b"v") for index in range(1, 50)]
    merged = merge_records(
        [counted(long_run, pulled, 0), counted(long_run, pulled, 1)],
    )

    first = next(merged)

    assert first == (b"\x01", b"v")
    assert pulled == [2, 2]


def test_a_source_whose_keys_do_not_ascend_is_refused_by_name() -> None:
    """A sorted run is what a source is, and the merge says which one was not.

    Re-sorting it would mean deciding which of two values for one key the table
    meant to be newer, which is information the file no longer carries.
    """
    merged = merge_records(
        [stream((b"b", b"1"), (b"a", b"2"))],
        source_names=["sstable-0000000007.sst"],
    )

    with pytest.raises(MergeSourceOrderError, match="sstable-0000000007.sst"):
        list(merged)


def test_a_source_that_repeats_a_key_is_refused() -> None:
    """Strictly ascending, not merely non-descending: one table, one record per key."""
    with pytest.raises(MergeSourceOrderError, match="ascending"):
        list(merge_records([stream((b"a", b"1"), (b"a", b"2"))]))


def test_an_unsorted_source_is_named_by_index_when_no_names_were_given() -> None:
    with pytest.raises(MergeSourceOrderError, match="index 1"):
        list(merge_records([stream((b"a", b"1")), stream((b"z", b"1"), (b"b", b"2"))]))


def test_a_name_per_source_is_required_when_names_are_given() -> None:
    with pytest.raises(ValueError, match="source names"):
        merge_records([stream(), stream()], source_names=["only-one"])


@settings(max_examples=200, deadline=None)
@given(
    st.lists(
        st.dictionaries(
            keys=st.binary(min_size=1, max_size=3),
            values=st.one_of(st.none(), st.binary(max_size=3)),
            max_size=8,
        ),
        max_size=4,
    )
)
def test_the_merge_matches_a_reference_newest_wins_computation(
    sources: list[dict[bytes, bytes | None]],
) -> None:
    """The rule, checked against the obvious slow implementation over random input.

    The reference loads everything into one dict, oldest source first so that
    newer writes overwrite older ones, and sorts at the end. That is what the
    streaming merge must agree with on every input, including the shapes a fixed
    example set does not reach: sources of very different lengths, keys that are
    prefixes of each other, and a key held by every source.
    """
    streams = [stream(*sorted(source.items())) for source in sources]
    reference: dict[bytes, bytes | None] = {}
    for source in reversed(sources):
        reference.update(source)

    assert list(merge_records(streams)) == sorted(reference.items())


# ---------------------------------------------------------------------------
# The merge against real files: a valid table out, sources untouched.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StubMergeTable:
    """An age, a size and a path, which is all :func:`merge_tier` is allowed to need.

    Deliberately not :class:`~ledgerlog.engine.SSTableHandle`: the merge is meant
    to be usable without the engine (CLAUDE.md), so the tier it is handed here is
    built from a stand-in that satisfies the protocol and nothing else.
    """

    sequence: int
    size_bytes: int
    path: Path


def write_table(path: Path, pairs: Sequence[Pair], *, index_interval: int = 64) -> Path:
    """Write one real SSTable holding ``pairs``, which must be in ascending key order."""
    write_sstable(path, pairs, index_interval=index_interval, expected_keys=len(pairs))
    return path


def table_records(path: Path) -> list[Pair]:
    """Every record in the table's data block, in the order the bytes hold them.

    Read by walking the block rather than by looking keys up, so that the merged
    table's ordering on disk is asserted rather than assumed: a reader's binary
    search would find a key in an unsorted table often enough to hide the bug.
    """
    with open(path, "rb") as handle:
        footer = read_footer(handle)
        return [
            (record.key, record.value)
            for record in iter_records(
                handle,
                start_offset=footer.data_block_offset,
                end_offset=footer.data_block_end,
            )
        ]


def tier_of(paths: Sequence[Path]) -> CompactionTier[StubMergeTable]:
    """A tier holding ``paths``, newest first, as :attr:`CompactionTier.tables` requires."""
    return CompactionTier(
        tables=tuple(
            StubMergeTable(
                sequence=len(paths) - index,
                size_bytes=path.stat().st_size,
                path=path,
            )
            for index, path in enumerate(paths)
        ),
        ready=True,
    )


def three_shadowing_tables(directory: Path) -> tuple[list[Path], list[Pair]]:
    """Three real tables that shadow each other, and the newest-wins result by hand.

    Written oldest to newest so the ages in the filenames read the way the engine's
    do, and returned newest first, which is the order a merge takes.
    """
    oldest = write_table(
        directory / "oldest.sst",
        [(b"k1", b"old-1"), (b"k2", b"old-2"), (b"k3", b"old-3"), (b"k9", b"old-9")],
    )
    middle = write_table(
        directory / "middle.sst",
        [(b"k2", b"mid-2"), (b"k4", b"mid-4"), (b"k9", None)],
    )
    newest = write_table(
        directory / "newest.sst",
        [(b"k1", b"new-1"), (b"k5", b"new-5")],
    )
    expected: list[Pair] = [
        (b"k1", b"new-1"),
        (b"k2", b"mid-2"),
        (b"k3", b"old-3"),
        (b"k4", b"mid-4"),
        (b"k5", b"new-5"),
        (b"k9", None),
    ]
    return [newest, middle, oldest], expected


def test_the_merged_table_holds_the_newest_write_of_every_key(tmp_path: Path) -> None:
    """The story's merge correctness criterion, against a hand-computed expectation."""
    sources, expected = three_shadowing_tables(tmp_path)

    layout = merge_sstables(sources, tmp_path / "merged.sst")

    assert layout.path == tmp_path / "merged.sst"
    assert layout.record_count == len(expected)
    assert table_records(layout.path) == expected


def test_the_merged_table_is_a_valid_sstable_with_every_section(tmp_path: Path) -> None:
    """Valid as the rest of the codebase judges validity, not merely present on disk.

    The sparse index is checked by count (one entry per ``index_interval``
    records, starting at the first) and by using it: every key is looked up
    through the real reader, which finds a record only by binary searching that
    index and scanning forward from it. The bloom filter is read back out of the
    file rather than taken from the layout, so the section the footer points at is
    what gets asked.
    """
    pairs: list[Pair] = [(f"key-{index:04d}".encode(), b"v") for index in range(120)]
    first = write_table(tmp_path / "first.sst", pairs[::2])
    second = write_table(tmp_path / "second.sst", pairs[1::2])

    layout = merge_sstables([first, second], tmp_path / "merged.sst", index_interval=16)

    assert inspect_sstable(layout.path).status is SSTableStatus.VALID
    assert layout.record_count == len(pairs)
    assert len(layout.index) == math.ceil(len(pairs) / 16)
    with open(layout.path, "rb") as handle:
        footer = read_footer(handle)
        bloom = read_bloom_filter(handle, footer)
    assert all(bloom.might_contain(key) for key, _ in pairs)
    with SSTableReader.open(layout.path) as reader:
        assert reader.record_count == len(pairs)
        assert all(reader.lookup(key) is not None for key, _ in pairs)


def test_tombstones_survive_the_merge_as_tombstones(tmp_path: Path) -> None:
    """On disk, not just in the stream: a reader must see the delete, not a miss.

    A merge that dropped the tombstone would make ``get`` on that key fall through
    to any older table beyond this merge, which is the resurrection M8.3 exists to
    make safe. Until then the tombstone stays.
    """
    newer = write_table(tmp_path / "newer.sst", [(b"deleted", None)])
    older = write_table(tmp_path / "older.sst", [(b"deleted", b"value"), (b"kept", b"value")])

    layout = merge_sstables([newer, older], tmp_path / "merged.sst")

    with SSTableReader.open(layout.path) as reader:
        deleted = reader.lookup(b"deleted")
        kept = reader.lookup(b"kept")
    assert deleted is not None and deleted.is_tombstone
    assert kept is not None and kept.value == b"value"


def test_the_merged_table_can_be_merged_again(tmp_path: Path) -> None:
    """The output is an ordinary table, which is what bounds write amplification.

    A record passes through one merge per tier, so a merged table has to be a
    legal source for the next merge up. Merging the merged table with a newer one
    also checks the output's records are framed exactly as a flush's are, since
    the second merge reads them back through the normal record path.
    """
    sources, expected = three_shadowing_tables(tmp_path)
    first_pass = merge_sstables(sources, tmp_path / "merged-1.sst")
    newer = write_table(tmp_path / "newer.sst", [(b"k3", b"newer-3")])

    second_pass = merge_sstables([newer, first_pass.path], tmp_path / "merged-2.sst")

    assert table_records(second_pass.path) == [
        (key, b"newer-3" if key == b"k3" else value) for key, value in expected
    ]


def test_the_sources_are_left_exactly_as_they_were(tmp_path: Path) -> None:
    """A merge reads its sources and nothing else, which is what M8.5 rests on.

    Deleting a source is M8.5's decision and it can only be made once the merged
    table is on disk and valid. A merge that truncated or rewrote a source would
    leave nothing for a crash mid-compaction to recover from.
    """
    sources, _ = three_shadowing_tables(tmp_path)
    before = {path: path.read_bytes() for path in sources}

    merge_sstables(sources, tmp_path / "merged.sst")

    assert {path: path.read_bytes() for path in sources} == before


def test_a_merge_leaves_no_temporary_file_behind(tmp_path: Path) -> None:
    sources, _ = three_shadowing_tables(tmp_path)

    merge_sstables(sources, tmp_path / "merged.sst")

    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "merged.sst",
        "middle.sst",
        "newest.sst",
        "oldest.sst",
    ]


def test_merge_tier_reads_a_tier_newest_first(tmp_path: Path) -> None:
    """The tier's own ordering is what decides which write wins.

    Built with the sequence numbers descending across the tier, as
    :class:`CompactionTier` requires, and with the newest table holding the
    losing-looking value for one key so that a merge reading the tier backwards
    would produce a different, detectably wrong table.
    """
    sources, expected = three_shadowing_tables(tmp_path)

    layout = merge_tier(tier_of(sources), tmp_path / "merged.sst")

    assert table_records(layout.path) == expected


def test_merge_tier_merges_the_tier_the_plan_picked(tmp_path: Path) -> None:
    """Plan then merge, the two halves of M8 as a caller will use them."""
    paths = [
        write_table(tmp_path / f"table-{index}.sst", [(f"k{index}".encode(), b"v")])
        for index in range(DEFAULT_MIN_TIER_TABLES)
    ]
    tables = [
        StubMergeTable(sequence=index, size_bytes=path.stat().st_size, path=path)
        for index, path in enumerate(paths)
    ]
    plan = plan_compaction(tables)
    next_tier = plan.next_tier
    assert next_tier is not None

    layout = merge_tier(next_tier, tmp_path / "merged.sst")

    assert table_records(layout.path) == [
        (f"k{index}".encode(), b"v") for index in range(len(paths))
    ]


def test_a_tier_below_the_threshold_still_merges(tmp_path: Path) -> None:
    """Readiness is the caller's question. Merging two tables is correct, just cheap."""
    newer = write_table(tmp_path / "newer.sst", [(b"k", b"new")])
    older = write_table(tmp_path / "older.sst", [(b"k", b"old")])
    tier = CompactionTier(
        tables=(
            StubMergeTable(sequence=1, size_bytes=newer.stat().st_size, path=newer),
            StubMergeTable(sequence=0, size_bytes=older.stat().st_size, path=older),
        ),
        ready=False,
    )

    layout = merge_tier(tier, tmp_path / "merged.sst")

    assert table_records(layout.path) == [(b"k", b"new")]


def test_a_table_with_a_path_satisfies_the_mergeable_protocol() -> None:
    """Structural, so that a merge needs no engine type and a tier needs no file."""
    with_path = StubMergeTable(sequence=0, size_bytes=1, path=Path("x.sst"))

    assert isinstance(with_path, MergeableTable)
    assert isinstance(with_path, SizedTable)
    assert not isinstance(StubTable(sequence=0, size_bytes=1), MergeableTable)


# ---------------------------------------------------------------------------
# A merge that cannot finish: damaged sources, and what is left behind.
# ---------------------------------------------------------------------------


def test_a_source_without_a_footer_stops_the_merge(tmp_path: Path) -> None:
    """A file with no footer is not a table, and a merge must not guess at its records.

    This is what an interrupted flush leaves. Discovery discards it and the WAL
    covers its writes (ARCHITECTURE.md section 6), so a merge that read it would
    be merging records nothing has committed.
    """
    good = write_table(tmp_path / "good.sst", [(b"k", b"v")])
    partial = write_table(tmp_path / "partial.sst", [(b"k2", b"v2")])
    with open(partial, "r+b") as handle:
        handle.truncate(FILE_HEADER_SIZE + 4)

    with pytest.raises(SSTableIncompleteError):
        merge_sstables([good, partial], tmp_path / "merged.sst")

    assert not (tmp_path / "merged.sst").exists()


def test_a_source_claiming_a_record_longer_than_its_block_stops_the_merge(
    tmp_path: Path,
) -> None:
    """A length off a damaged disk is measured before it is used, not allocated from.

    The length prefix of the first record is rewritten to claim a megabyte inside
    a data block holding a few bytes. The merge has to refuse it rather than read
    past the block into the index, or size an allocation from a number no writer
    wrote.
    """
    good = write_table(tmp_path / "good.sst", [(b"k", b"v")])
    damaged = write_table(tmp_path / "damaged.sst", [(b"k2", b"v2")])
    with open(damaged, "r+b") as handle:
        handle.seek(FILE_HEADER_SIZE)
        handle.write(struct.pack("<I", 1_000_000))

    with pytest.raises(SSTableTruncatedRecordError):
        merge_sstables([good, damaged], tmp_path / "merged.sst")

    assert not (tmp_path / "merged.sst").exists()


def test_a_source_in_an_unknown_format_version_is_refused(tmp_path: Path) -> None:
    """Per CLAUDE.md: a layout this build does not know is reported, never guessed at."""
    good = write_table(tmp_path / "good.sst", [(b"k", b"v")])
    future = write_table(tmp_path / "future.sst", [(b"k2", b"v2")])
    with open(future, "r+b") as handle:
        handle.seek(FILE_HEADER_SIZE - 1)
        handle.write(bytes([99]))

    with pytest.raises(SSTableUnsupportedVersionError):
        merge_sstables([good, future], tmp_path / "merged.sst")

    assert not (tmp_path / "merged.sst").exists()


def test_a_merge_that_fails_partway_leaves_no_partial_table_behind(tmp_path: Path) -> None:
    """The failure has to happen after records have been written, not before.

    The damaged record is the second one in its source, so the writer has already
    taken a record and opened its temporary file when the merge raises. What must
    not survive is a file at the destination name, which a later discovery pass
    would load as a table, or a temporary file nobody owns any more.
    """
    good = write_table(tmp_path / "good.sst", [(b"a", b"v"), (b"b", b"v")])
    damaged = write_table(tmp_path / "damaged.sst", [(b"c", b"v"), (b"d", b"v")])
    with open(damaged, "rb") as handle:
        footer = read_footer(handle)
        records = list(
            iter_records(
                handle,
                start_offset=footer.data_block_offset,
                end_offset=footer.data_block_end,
            )
        )
    with open(damaged, "r+b") as handle:
        handle.seek(records[1].offset)
        handle.write(struct.pack("<I", 1_000_000))

    with pytest.raises(SSTableTruncatedRecordError):
        merge_sstables([good, damaged], tmp_path / "merged.sst")

    assert sorted(path.name for path in tmp_path.iterdir()) == ["damaged.sst", "good.sst"]


@pytest.mark.skipif(
    not Path("/proc/self/fd").is_dir(),
    reason="counting open descriptors needs /proc",
)
def test_a_failed_merge_closes_every_source_it_opened(tmp_path: Path) -> None:
    """Handles are released on the way out, whether the merge finished or raised.

    A compaction pass that leaked one descriptor per source would run out within a
    few tiers, and the leak would not show up in any correctness test. Counted
    around both a successful merge and a failed one.
    """
    good = write_table(tmp_path / "good.sst", [(b"a", b"v")])
    damaged = write_table(tmp_path / "damaged.sst", [(b"c", b"v")])
    with open(damaged, "r+b") as handle:
        handle.seek(FILE_HEADER_SIZE)
        handle.write(struct.pack("<I", 1_000_000))

    def open_descriptors() -> int:
        return len(os.listdir("/proc/self/fd"))

    before = open_descriptors()
    merge_sstables([good], tmp_path / "merged.sst")
    after_success = open_descriptors()
    with pytest.raises(SSTableTruncatedRecordError):
        merge_sstables([good, damaged], tmp_path / "failed.sst")

    assert after_success == before
    assert open_descriptors() == before


# ---------------------------------------------------------------------------
# Arguments a merge refuses, because finishing one would destroy data.
# ---------------------------------------------------------------------------


def test_a_merge_needs_at_least_one_source(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="at least one source"):
        merge_sstables([], tmp_path / "merged.sst")


def test_the_same_source_cannot_be_listed_twice(tmp_path: Path) -> None:
    """Two copies of one table give a key two writes with no way to age them."""
    source = write_table(tmp_path / "source.sst", [(b"k", b"v")])

    with pytest.raises(ValueError, match="listed twice"):
        merge_sstables([source, source], tmp_path / "merged.sst")


def test_the_destination_cannot_be_one_of_the_sources(tmp_path: Path) -> None:
    """Finishing renames over the destination, which would be a source mid-read."""
    first = write_table(tmp_path / "first.sst", [(b"k", b"v")])
    second = write_table(tmp_path / "second.sst", [(b"k2", b"v2")])

    with pytest.raises(ValueError, match="also a source"):
        merge_sstables([first, second], first)

    assert table_records(first) == [(b"k", b"v")]


def test_an_existing_destination_is_refused_rather_than_replaced(tmp_path: Path) -> None:
    """A merge names a new table. Renaming over a live one would discard it silently."""
    source = write_table(tmp_path / "source.sst", [(b"k", b"v")])
    occupied = write_table(tmp_path / "occupied.sst", [(b"other", b"value")])

    with pytest.raises(ValueError, match="already exists"):
        merge_sstables([source], occupied)

    assert table_records(occupied) == [(b"other", b"value")]


# ---------------------------------------------------------------------------
# A merge alongside readers, since an SSTable is immutable and both may run.
# ---------------------------------------------------------------------------


def test_a_merge_does_not_disturb_readers_of_the_same_source_tables(tmp_path: Path) -> None:
    """Real threads, because the claim is about handles and only threads can test it.

    The merge opens each source itself instead of borrowing a reader's handle, so
    a ``get`` being served from a source table while a compaction reads it must
    see its own file position. If the two shared a cursor, the reads below would
    return the wrong records or fail to decode, and the merged table would be
    damaged too.
    """
    pairs: list[Pair] = [
        (f"key-{index:04d}".encode(), f"v{index}".encode()) for index in range(200)
    ]
    newer = write_table(tmp_path / "newer.sst", pairs[100:])
    older = write_table(tmp_path / "older.sst", pairs[:100])
    start = threading.Barrier(3)
    stop = threading.Event()
    failures: list[BaseException] = []

    def read_until_stopped(path: Path, expected: Sequence[Pair]) -> None:
        try:
            with SSTableReader.open(path) as reader:
                start.wait(timeout=10)
                while not stop.is_set():
                    for key, value in expected:
                        record = reader.lookup(key)
                        assert record is not None and record.value == value
        except BaseException as error:  # reported to the main thread, never swallowed
            failures.append(error)

    readers = [
        threading.Thread(target=read_until_stopped, args=(newer, pairs[100:])),
        threading.Thread(target=read_until_stopped, args=(older, pairs[:100])),
    ]
    for thread in readers:
        thread.start()
    try:
        start.wait(timeout=10)
        layout = merge_sstables([newer, older], tmp_path / "merged.sst")
    finally:
        stop.set()
        for thread in readers:
            thread.join(timeout=10)

    assert failures == []
    assert table_records(layout.path) == pairs


def test_a_footer_claiming_impossibly_many_records_does_not_size_the_merge(
    tmp_path: Path,
) -> None:
    """A count off a damaged disk must not become the size of an allocation.

    The footer's record count is only checked for being non-negative when it is
    read, and the merge uses it to size the output's bloom filter. Left unbounded,
    the footer rewritten below (a valid checksum over a count of a trillion) would
    ask the filter for more than a gigabyte of bits, or be refused outright by the
    sizing formula, before a single record had been read. The data block's extent
    bounds it instead, so the merge proceeds on the records that are really there.
    """
    good = write_table(tmp_path / "good.sst", [(b"k", b"v")])
    lying = write_table(tmp_path / "lying.sst", [(b"k2", b"v2")])
    with open(lying, "r+b") as handle:
        footer = read_footer(handle)
        handle.seek(lying.stat().st_size - FOOTER_SIZE)
        handle.write(
            SSTableFooter(
                data_block_offset=footer.data_block_offset,
                data_block_end=footer.data_block_end,
                index_offset=footer.index_offset,
                index_end=footer.index_end,
                bloom_filter_offset=footer.bloom_filter_offset,
                bloom_filter_end=footer.bloom_filter_end,
                record_count=2**40,
            ).encode()
        )

    layout = merge_sstables([good, lying], tmp_path / "merged.sst")

    assert table_records(layout.path) == [(b"k", b"v"), (b"k2", b"v2")]
    assert layout.bloom_filter.bit_count < 1024
