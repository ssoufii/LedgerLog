"""Tests for size-tier grouping and the compaction trigger, story M8.1.

The story's claims are about metadata, so these tests need no files, no engine and
no threads: a table, to this planner, is an age and a size, and the stand-in below
is exactly that. The engine side of the story, that a table arriving from a flush
re-tiers what the engine holds, is tested against the real engine in
``test_engine.py``, where a real flush can produce the table.

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
import random
from dataclasses import FrozenInstanceError, dataclass

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from ledgerlog.compaction import (
    DEFAULT_MIN_TIER_TABLES,
    DEFAULT_SIZE_RATIO,
    CompactionPlan,
    CompactionPolicy,
    CompactionTier,
    SizedTable,
    plan_compaction,
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
