"""Tier grouping, the tier merge, the tombstone rule and the swap: M8.1 to M8.3 and M8.5.

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
* Tombstones are what a merge must keep unless it has been told what lies
  outside it, and a merge that dropped them by default would look tidier and
  resurrect deleted keys. Checked in the merged output on disk, not just in the
  stream.

What could pass a loose reading of M8.3 while being false, since "the tombstone
was dropped" and "the tombstone was kept" are each satisfied by a merge that
always does that one thing:

* A merge that dropped every tombstone passes the drop criterion. So the same
  two source tables are merged three ways below, changing only what is said to
  lie outside the merge, and the three outputs are asserted to differ in exactly
  the tombstones the rule predicts.
* A merge that kept every tombstone passes the retain criterion, and is what the
  code did before this story. Pinned by asserting the dropped key is absent from
  the merged table's data block, not merely that the surviving ones are present.
* "No older value outside the merge" can be read as "no older table outside the
  merge", which would keep a tombstone forever behind any older table at all.
  Tested with an older table that does not hold the key, and with one that holds
  only a tombstone for it.
* The point of retaining is that a read must not resurrect the key. That is
  asserted as a read, walking the tables newest first the way the engine does,
  rather than by inspecting the merged file alone.

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

What could pass a loose reading of M8.5 while being false, since the story is an
ordering between two things that both happen anyway:

* "Only deleted after the footer is written" can be satisfied by code that
  deletes after the merge call returns, which is not the same claim: the merge
  returning says the writer thinks it wrote a footer. Pinned by truncating,
  corrupting and version-stamping the merged file between the merge and the swap
  and asserting that each refusal leaves every source byte-identical.
* A crash mid-merge is the case the ordering exists for, and nothing short of a
  real kill exercises it, since an exception in-process still runs the writer's
  cleanup. So the merge below happens in a child process that is stopped at a
  known record and sent SIGKILL, and the sources, the destination name and the
  debris left behind are all asserted afterwards.
* "Source tables are gone" can be read as "gone in some order", and the order is
  a correctness rule: a merge may drop a tombstone, so a crash partway through
  the deletions can resurrect a key if the newest sources go first. Both the
  order and the counterfactual are asserted directly.

The hypothesis test is the partition check. Grouping is a sweep, and the failures
a sweep has are dropping a table at a boundary or admitting one to two tiers,
neither of which a fixed example set reliably lands on.
"""

from __future__ import annotations

import math
import os
import random
import select
import signal
import struct
import subprocess
import sys
import threading
from collections.abc import Callable, Iterator, Sequence
from dataclasses import FrozenInstanceError, dataclass, replace
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import ledgerlog
from ledgerlog.compaction import (
    DEFAULT_MIN_TIER_TABLES,
    DEFAULT_SIZE_RATIO,
    CompactionPlan,
    CompactionPolicy,
    CompactionSwap,
    CompactionTier,
    MergeableTable,
    MergeNotCommittedError,
    MergeSourceOrderError,
    OlderTable,
    OlderTableProbe,
    SizedTable,
    compact_tier,
    discard_partial_tables,
    merge_records,
    merge_sstables,
    merge_tier,
    older_outside_tables,
    plan_compaction,
    swap_in_merged_table,
)
from ledgerlog.sstable import (
    FILE_HEADER_SIZE,
    FOOTER_SIZE,
    SSTABLE_FORMAT_VERSION,
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
    temp_table_path,
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

    A merge told nothing about what lies outside it keeps every tombstone, which
    is what this asserts. Dropping one would make ``get`` on that key fall through
    to any older table beyond the merge, and since a merge given no
    ``older_tables`` has no idea whether such a table exists, keeping is the only
    safe answer. What happens once it is told is M8.3, tested below.
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


# ---------------------------------------------------------------------------
# M8.3: a tombstone is dropped only once nothing older can resurrect its key.
# ---------------------------------------------------------------------------


class RecordingProbe:
    """An :data:`~ledgerlog.compaction.OlderValueProbe` with a fixed answer and a log.

    The answer is fixed so a stream-level test can state "something older holds
    this key" without a file behind it, and the log is what distinguishes a merge
    that consults the rule from one that happens to agree with it.
    """

    def __init__(self, answer: bool = False, *, keys: Sequence[bytes] = ()) -> None:
        self.answer = answer
        self.keys: set[bytes] = set(keys)
        self.asked: list[bytes] = []

    def __call__(self, key: bytes) -> bool:
        self.asked.append(key)
        return self.answer or key in self.keys


class CountingReader(SSTableReader):
    """A real reader that records every key it was asked to look up.

    A subclass rather than a stand-in, so that what the bloom filter is being
    credited with skipping is a real index search and a real scan.
    """

    def __init__(
        self,
        stream: object,
        *,
        path: Path | None = None,
        owns_stream: bool = False,
    ) -> None:
        super().__init__(stream, path=path, owns_stream=owns_stream)  # type: ignore[arg-type]
        self.lookups: list[bytes] = []

    def lookup(self, key: bytes) -> SSTableRecord | None:
        self.lookups.append(key)
        return super().lookup(key)


def read_newest_first(paths: Sequence[Path], key: bytes) -> bytes | None:
    """Answer ``key`` the way the engine's read path does, newest table first.

    Returns the value, or ``None`` for not-found, which is what both a tombstone
    and running out of tables mean. Written out here rather than borrowed from
    the engine because the claim under test is about what the files say, and a
    test that reused the engine's own walk could only agree with it.
    """
    for path in paths:
        with SSTableReader.open(path) as reader:
            record = reader.lookup(key)
            if record is not None:
                return record.value
    return None


def merge_over_a_deleted_key(directory: Path) -> tuple[list[Path], Path]:
    """Two tables to merge, plus one older table left outside the merge.

    The sources hold two tombstones that differ in exactly one way: ``shadowed``
    has an older value in the outside table and ``solo`` does not. So one merge
    over one pair of sources exercises both halves of the rule, and a merge that
    always dropped or always kept cannot pass both.

    Returned as (sources newest first, outside table).
    """
    outside = write_table(directory / "outside.sst", [(b"shadowed", b"outside")])
    older = write_table(
        directory / "older.sst",
        [(b"live", b"inside"), (b"shadowed", b"inside"), (b"solo", b"inside")],
    )
    newer = write_table(directory / "newer.sst", [(b"shadowed", None), (b"solo", None)])
    return [newer, older], outside


def test_a_tombstone_with_no_older_value_outside_the_merge_is_dropped(tmp_path: Path) -> None:
    """The story's first criterion, with the outside table stated to hold nothing older.

    Asserted as absence from the merged table's data block, not just as a reader
    returning not-found: a retained tombstone also reads as not-found, so a
    lookup alone would pass either way.
    """
    sources, _ = merge_over_a_deleted_key(tmp_path)

    layout = merge_sstables(sources, tmp_path / "merged.sst", older_tables=[])

    assert table_records(layout.path) == [(b"live", b"inside")]
    assert layout.record_count == 1


def test_a_tombstone_with_an_older_value_outside_the_merge_is_retained(tmp_path: Path) -> None:
    """The story's second criterion: the key that outside still holds keeps its tombstone.

    ``solo`` is in the same merge and has no older value anywhere, so it goes.
    The difference between the two is the rule, and a merge that kept both or
    dropped both fails this.
    """
    sources, outside = merge_over_a_deleted_key(tmp_path)

    layout = merge_sstables(sources, tmp_path / "merged.sst", older_tables=[outside])

    assert table_records(layout.path) == [(b"live", b"inside"), (b"shadowed", None)]


def test_drop_and_retain_differ_only_in_what_lies_outside_the_merge(tmp_path: Path) -> None:
    """The story's third criterion: both cases simulated over one set of sources.

    Three merges of the same two tables, differing only in what they are told is
    outside them. The outputs have to differ in exactly the tombstones the rule
    predicts, which is what rules out a merge with a fixed opinion about
    tombstones.
    """
    sources, outside = merge_over_a_deleted_key(tmp_path)

    told_nothing = merge_sstables(sources, tmp_path / "told-nothing.sst")
    told_empty = merge_sstables(sources, tmp_path / "told-empty.sst", older_tables=[])
    told_outside = merge_sstables(sources, tmp_path / "told-outside.sst", older_tables=[outside])

    assert table_records(told_nothing.path) == [
        (b"live", b"inside"),
        (b"shadowed", None),
        (b"solo", None),
    ]
    assert table_records(told_empty.path) == [(b"live", b"inside")]
    assert table_records(told_outside.path) == [(b"live", b"inside"), (b"shadowed", None)]


def test_a_retained_tombstone_keeps_the_deleted_key_deleted_on_the_read_path(
    tmp_path: Path,
) -> None:
    """The reason the tombstone is retained, asserted as a read rather than as a file.

    The engine after this compaction holds the merged table and the outside table,
    newest first. A read of ``shadowed`` must stop at the merged table's tombstone
    instead of falling through to the value the outside table still holds.
    """
    sources, outside = merge_over_a_deleted_key(tmp_path)

    layout = merge_sstables(sources, tmp_path / "merged.sst", older_tables=[outside])

    assert read_newest_first([layout.path, outside], b"shadowed") is None
    assert read_newest_first([layout.path, outside], b"live") == b"inside"
    assert read_newest_first([layout.path, outside], b"solo") is None


def test_dropping_a_tombstone_that_outside_still_holds_would_resurrect_the_key(
    tmp_path: Path,
) -> None:
    """The failure the rule prevents, shown by merging as though outside did not exist.

    Not a test of the rule but of the stakes: told there is nothing older, the
    merge drops the tombstone, and the same newest-first read then finds the
    outside table's value for a key that was deleted. If this ever stops
    resurrecting, the test above has stopped proving anything.
    """
    sources, outside = merge_over_a_deleted_key(tmp_path)

    layout = merge_sstables(sources, tmp_path / "merged.sst", older_tables=[])

    assert read_newest_first([layout.path, outside], b"shadowed") == b"outside"


def test_an_outside_table_that_does_not_hold_the_key_does_not_keep_the_tombstone(
    tmp_path: Path,
) -> None:
    """Older tables alone do not keep a tombstone alive, only an older value does.

    A rule that kept a tombstone whenever any older table existed would never
    reclaim anything in an engine with more than one tier.
    """
    outside = write_table(tmp_path / "outside.sst", [(b"unrelated", b"outside")])
    newer = write_table(tmp_path / "newer.sst", [(b"deleted", None)])
    older = write_table(tmp_path / "older.sst", [(b"deleted", b"inside")])

    layout = merge_sstables([newer, older], tmp_path / "merged.sst", older_tables=[outside])

    assert table_records(layout.path) == []
    assert layout.record_count == 0


def test_an_outside_table_holding_only_a_tombstone_does_not_keep_the_tombstone(
    tmp_path: Path,
) -> None:
    """A delete outside the merge keeps the key deleted by itself, so ours may go.

    A read falling through the merged table reaches the outside tombstone and
    still answers not-found, which is asserted here rather than argued: the merged
    table is empty and the key is still gone.
    """
    outside = write_table(tmp_path / "outside.sst", [(b"deleted", None)])
    newer = write_table(tmp_path / "newer.sst", [(b"deleted", None)])
    older = write_table(tmp_path / "older.sst", [(b"deleted", b"inside")])

    layout = merge_sstables([newer, older], tmp_path / "merged.sst", older_tables=[outside])

    assert table_records(layout.path) == []
    assert read_newest_first([layout.path, outside], b"deleted") is None


def test_a_merge_that_drops_a_tombstone_still_writes_a_valid_sstable(tmp_path: Path) -> None:
    """Dropping records must not leave the index, the filter or the footer disagreeing.

    A dropped key is not added to the bloom filter and not counted, so a writer
    that had already committed to a count or an index entry before the drop would
    produce a table that fails validation or answers for a key it does not hold.
    """
    kept: list[Pair] = [(f"key-{index:04d}".encode(), b"v") for index in range(0, 120, 2)]
    deleted: list[Pair] = [(f"key-{index:04d}".encode(), None) for index in range(1, 120, 2)]
    newer = write_table(tmp_path / "newer.sst", deleted)
    older = write_table(tmp_path / "older.sst", kept)

    layout = merge_sstables(
        [newer, older], tmp_path / "merged.sst", index_interval=8, older_tables=[]
    )

    assert inspect_sstable(layout.path).status is SSTableStatus.VALID
    assert table_records(layout.path) == kept
    assert layout.record_count == len(kept)
    assert len(layout.index) == math.ceil(len(kept) / 8)
    with open(layout.path, "rb") as handle:
        bloom = read_bloom_filter(handle, read_footer(handle))
    assert all(bloom.might_contain(key) for key, _ in kept)
    with SSTableReader.open(layout.path) as reader:
        assert all(reader.lookup(key) is None for key, _ in deleted)


def test_a_merge_can_drop_every_record_it_was_given(tmp_path: Path) -> None:
    """An all-tombstone tier merges to an empty but valid table, which is the win.

    The case compaction exists for at the extreme: a tier that is nothing but
    deletes collapses to a table holding nothing, rather than to a file that
    cannot be read back.
    """
    newer = write_table(tmp_path / "newer.sst", [(b"a", None), (b"b", None)])
    older = write_table(tmp_path / "older.sst", [(b"a", b"v"), (b"b", b"v")])

    layout = merge_sstables([newer, older], tmp_path / "merged.sst", older_tables=[])

    assert inspect_sstable(layout.path).status is SSTableStatus.VALID
    assert table_records(layout.path) == []
    with SSTableReader.open(layout.path) as reader:
        assert reader.lookup(b"a") is None


# ---------------------------------------------------------------------------
# The rule itself, over streams, with no files in the way.
# ---------------------------------------------------------------------------


def records(pairs: Sequence[Pair]) -> list[SSTableRecord]:
    """Records for ``pairs`` at throwaway offsets, since a merge ignores offsets."""
    return [
        SSTableRecord(key=key, value=value, offset=index, end_offset=index + 1)
        for index, (key, value) in enumerate(pairs)
    ]


def test_the_probe_is_asked_only_about_the_keys_a_tombstone_wins() -> None:
    """A lookup per live key would make every merge pay for a rule about deletes.

    The probe costs an index search and a scan per older table, so asking it
    about keys that are not being deleted would add that cost to every record in
    every merge and change no answer.
    """
    probe = RecordingProbe(answer=False)
    stream = records([(b"a", b"v"), (b"b", None), (b"c", b"v")])

    merged = list(merge_records([stream], has_older_value=probe))

    assert merged == [(b"a", b"v"), (b"c", b"v")]
    assert probe.asked == [b"b"]


def test_a_tombstone_shadowing_a_value_inside_the_merge_is_still_asked_about() -> None:
    """The older copy inside the merge is dropped either way, so it decides nothing.

    What matters is whether something older sits outside, and the merge cannot
    see that. A merge that skipped the probe when it had already seen an older
    copy of the key would keep exactly the tombstones it could most safely drop.
    """
    probe = RecordingProbe(answer=False)
    newer = records([(b"k", None)])
    older = records([(b"k", b"old")])

    merged = list(merge_records([newer, older], has_older_value=probe))

    assert merged == []
    assert probe.asked == [b"k"]


def test_a_kept_tombstone_is_yielded_where_its_key_belongs() -> None:
    """Dropping some tombstones must not disturb the sorted run around them."""
    probe = RecordingProbe(keys={b"b"})
    stream = records([(b"a", None), (b"b", None), (b"c", b"v"), (b"d", None)])

    merged = list(merge_records([stream], has_older_value=probe))

    assert merged == [(b"b", None), (b"c", b"v")]
    assert probe.asked == [b"a", b"b", b"d"]


def test_without_a_probe_no_tombstone_is_dropped() -> None:
    """The default, stated as a test because it is the safe reading of silence."""
    stream = records([(b"a", None), (b"b", b"v")])

    assert list(merge_records([stream])) == [(b"a", None), (b"b", b"v")]


def test_the_probe_decides_per_key_rather_than_once() -> None:
    """A merge that cached the first answer would drop or keep every tombstone."""
    probe = RecordingProbe(keys={b"b", b"d"})
    stream = records([(b"a", None), (b"b", None), (b"c", None), (b"d", None)])

    merged = list(merge_records([stream], has_older_value=probe))

    assert merged == [(b"b", None), (b"d", None)]
    assert probe.asked == [b"a", b"b", b"c", b"d"]


# ---------------------------------------------------------------------------
# Reading the answer off real tables: OlderTable and OlderTableProbe.
# ---------------------------------------------------------------------------


def test_an_older_table_reports_a_value_and_not_a_tombstone(tmp_path: Path) -> None:
    table = write_table(tmp_path / "older.sst", [(b"gone", None), (b"here", b"v")])

    with SSTableReader.open(table) as reader:
        older = OlderTable(reader)

        assert older.holds_value(b"here") is True
        assert older.holds_value(b"gone") is False
        assert older.holds_value(b"absent") is False


def test_the_bloom_filter_spares_the_older_table_a_lookup(tmp_path: Path) -> None:
    """The filter is why consulting older tables is affordable per tombstone.

    Most keys a merge asks about are in no given older table, and a filter that
    was carried but not consulted would leave every one of those costing a seek
    and a scan.
    """
    table = write_table(tmp_path / "older.sst", [(b"here", b"v")])
    with open(table, "rb") as handle:
        bloom = read_bloom_filter(handle, read_footer(handle))
    rejected = next(
        key
        for key in (f"absent-{index}".encode() for index in range(1000))
        if not bloom.might_contain(key)
    )

    with CountingReader.open(table) as reader:
        older = OlderTable(reader, bloom)

        assert older.holds_value(rejected) is False
        assert reader.lookups == []
        assert older.holds_value(b"here") is True
        assert reader.lookups == [b"here"]


def test_an_older_table_without_a_filter_answers_the_same(tmp_path: Path) -> None:
    """The filter is an optimisation, so its absence may cost time and nothing else."""
    table = write_table(tmp_path / "older.sst", [(b"here", b"v")])

    with SSTableReader.open(table) as reader:
        assert OlderTable(reader, None).holds_value(b"here") is True
        assert OlderTable(reader, None).holds_value(b"absent") is False


def test_a_probe_over_no_tables_keeps_no_tombstone() -> None:
    """Which is what makes ``older_tables=[]`` mean "nothing older exists"."""
    probe = OlderTableProbe([])

    assert probe.tables == ()
    assert probe(b"anything") is False


def test_a_probe_answers_yes_if_any_of_its_tables_holds_the_key(tmp_path: Path) -> None:
    first = write_table(tmp_path / "first.sst", [(b"a", b"v")])
    second = write_table(tmp_path / "second.sst", [(b"b", b"v")])

    with SSTableReader.open(first) as one, SSTableReader.open(second) as two:
        probe = OlderTableProbe([OlderTable(one), OlderTable(two)])

        assert len(probe.tables) == 2
        assert probe(b"a") is True
        assert probe(b"b") is True
        assert probe(b"c") is False


# ---------------------------------------------------------------------------
# Which tables count as older than a tier, which is the rule's other half.
# ---------------------------------------------------------------------------


def outside_table(sequence: int) -> StubMergeTable:
    """A table outside a tier, identified by its age; its size and path are unused here."""
    return StubMergeTable(sequence=sequence, size_bytes=1, path=Path(f"t-{sequence}.sst"))


def tier_of_sequences(sequences: Sequence[int]) -> CompactionTier[StubMergeTable]:
    """A tier holding tables of those ages, newest first as a tier requires."""
    return CompactionTier(
        tables=tuple(outside_table(sequence) for sequence in sorted(sequences, reverse=True)),
        ready=True,
    )


def test_tables_newer_than_the_tier_are_not_older_than_the_merge() -> None:
    """A newer table wins a key outright, so no tombstone below it protects anything."""
    tier = tier_of_sequences([3, 4])

    assert older_outside_tables(tier, [outside_table(5), outside_table(9)]) == ()


def test_tables_older_than_the_tier_are_returned_newest_first() -> None:
    tier = tier_of_sequences([5, 6])

    older = older_outside_tables(tier, [outside_table(1), outside_table(4), outside_table(2)])

    assert [table.sequence for table in older] == [4, 2, 1]


def test_the_tier_s_own_tables_are_not_counted_as_outside_it() -> None:
    """A source counted as outside would keep every tombstone the merge could drop."""
    tier = tier_of_sequences([2, 3, 4])

    older = older_outside_tables(tier, [*tier.tables, outside_table(1)])

    assert [table.sequence for table in older] == [1]


def test_a_table_inside_the_tier_s_range_counts_as_older_than_the_merge() -> None:
    """Tiers group by size, so they are not always a contiguous run of ages.

    A table between the tier's oldest and newest is older than at least one
    source, so a tombstone from a newer source can hide its values. Measuring
    from the tier's oldest table would miss it, and missing one is a resurrected
    key.
    """
    tier = tier_of_sequences([2, 7])

    older = older_outside_tables(tier, [outside_table(5)])

    assert [table.sequence for table in older] == [5]


# ---------------------------------------------------------------------------
# merge_tier, which is where a tier and the tables around it come together.
# ---------------------------------------------------------------------------


def test_merge_tier_keeps_a_tombstone_an_older_table_outside_the_tier_needs(
    tmp_path: Path,
) -> None:
    """The engine's case: the tier is compacted, the tables around it are not."""
    sources, outside = merge_over_a_deleted_key(tmp_path)
    tier = tier_of(sources)
    outside_handle = StubMergeTable(sequence=0, size_bytes=outside.stat().st_size, path=outside)

    layout = merge_tier(tier, tmp_path / "merged.sst", other_tables=[outside_handle])

    assert table_records(layout.path) == [(b"live", b"inside"), (b"shadowed", None)]


def test_merge_tier_drops_the_tombstone_when_the_tier_is_all_there_is(tmp_path: Path) -> None:
    sources, _ = merge_over_a_deleted_key(tmp_path)

    layout = merge_tier(tier_of(sources), tmp_path / "merged.sst", other_tables=[])

    assert table_records(layout.path) == [(b"live", b"inside")]


def test_merge_tier_told_nothing_keeps_every_tombstone(tmp_path: Path) -> None:
    sources, _ = merge_over_a_deleted_key(tmp_path)

    layout = merge_tier(tier_of(sources), tmp_path / "merged.sst")

    assert table_records(layout.path) == [
        (b"live", b"inside"),
        (b"shadowed", None),
        (b"solo", None),
    ]


def test_merge_tier_ignores_a_newer_table_outside_the_tier(tmp_path: Path) -> None:
    """A newer table is read before the merged one, so it cannot resurrect anything."""
    sources, _ = merge_over_a_deleted_key(tmp_path)
    tier = tier_of(sources)
    newest = write_table(tmp_path / "newest.sst", [(b"shadowed", b"newest")])
    newer_handle = StubMergeTable(
        sequence=max(table.sequence for table in tier.tables) + 1,
        size_bytes=newest.stat().st_size,
        path=newest,
    )

    layout = merge_tier(tier, tmp_path / "merged.sst", other_tables=[newer_handle])

    assert table_records(layout.path) == [(b"live", b"inside")]


def test_merge_tier_accepts_the_whole_table_list_including_the_tier(tmp_path: Path) -> None:
    """So a caller can pass everything the engine holds without computing a difference."""
    sources, outside = merge_over_a_deleted_key(tmp_path)
    tier = tier_of(sources)
    outside_handle = StubMergeTable(sequence=0, size_bytes=outside.stat().st_size, path=outside)

    layout = merge_tier(tier, tmp_path / "merged.sst", other_tables=[*tier.tables, outside_handle])

    assert table_records(layout.path) == [(b"live", b"inside"), (b"shadowed", None)]


# ---------------------------------------------------------------------------
# Older tables a merge refuses, and the handles it must not leak reading them.
# ---------------------------------------------------------------------------


def test_a_source_cannot_also_be_listed_as_older_than_the_merge(tmp_path: Path) -> None:
    newer = write_table(tmp_path / "newer.sst", [(b"k", None)])
    older = write_table(tmp_path / "older.sst", [(b"k", b"v")])

    with pytest.raises(ValueError, match="cannot also be outside"):
        merge_sstables([newer, older], tmp_path / "merged.sst", older_tables=[older])

    assert not (tmp_path / "merged.sst").exists()


def test_the_destination_cannot_be_listed_as_older_than_the_merge(tmp_path: Path) -> None:
    newer = write_table(tmp_path / "newer.sst", [(b"k", None)])

    with pytest.raises(ValueError, match="destination of the merge"):
        merge_sstables([newer], tmp_path / "merged.sst", older_tables=[tmp_path / "merged.sst"])


def test_an_older_table_cannot_be_listed_twice(tmp_path: Path) -> None:
    newer = write_table(tmp_path / "newer.sst", [(b"k", None)])
    outside = write_table(tmp_path / "outside.sst", [(b"k", b"v")])

    with pytest.raises(ValueError, match="listed twice among the older tables"):
        merge_sstables([newer], tmp_path / "merged.sst", older_tables=[outside, outside])


def test_a_damaged_older_table_stops_the_merge_and_leaves_nothing_behind(
    tmp_path: Path,
) -> None:
    """An older table is validated like a source, since it is parsed like one.

    A tombstone dropped on the word of a table whose footer could not be trusted
    would be a delete decided by damaged bytes. Truncating the file past its
    header leaves a file with no readable footer.
    """
    newer = write_table(tmp_path / "newer.sst", [(b"k", None)])
    older = write_table(tmp_path / "older.sst", [(b"k", b"v")])
    damaged = write_table(tmp_path / "damaged.sst", [(b"k", b"v")])
    with open(damaged, "r+b") as handle:
        handle.truncate(FILE_HEADER_SIZE + 1)

    with pytest.raises(SSTableIncompleteError):
        merge_sstables([newer, older], tmp_path / "merged.sst", older_tables=[damaged])

    assert not (tmp_path / "merged.sst").exists()
    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "damaged.sst",
        "newer.sst",
        "older.sst",
    ]


def test_an_older_table_in_an_unknown_format_version_is_refused(tmp_path: Path) -> None:
    newer = write_table(tmp_path / "newer.sst", [(b"k", None)])
    older = write_table(tmp_path / "older.sst", [(b"k", b"v")])
    with open(older, "r+b") as handle:
        handle.seek(older.stat().st_size - FOOTER_SIZE)
        footer = SSTableFooter.decode(handle.read(FOOTER_SIZE))
        handle.seek(older.stat().st_size - FOOTER_SIZE)
        handle.write(
            SSTableFooter(
                data_block_offset=footer.data_block_offset,
                data_block_end=footer.data_block_end,
                index_offset=footer.index_offset,
                index_end=footer.index_end,
                bloom_filter_offset=footer.bloom_filter_offset,
                bloom_filter_end=footer.bloom_filter_end,
                record_count=footer.record_count,
                format_version=SSTABLE_FORMAT_VERSION + 1,
            ).encode()
        )

    with pytest.raises(SSTableUnsupportedVersionError):
        merge_sstables([newer], tmp_path / "merged.sst", older_tables=[older])


def test_the_sources_and_the_older_tables_are_left_exactly_as_they_were(
    tmp_path: Path,
) -> None:
    """Consulting an older table must not change it: M8.5 deletes sources, not these."""
    sources, outside = merge_over_a_deleted_key(tmp_path)
    before = {path: path.read_bytes() for path in [*sources, outside]}

    merge_sstables(sources, tmp_path / "merged.sst", older_tables=[outside])

    assert {path: path.read_bytes() for path in [*sources, outside]} == before


@pytest.mark.skipif(
    not Path("/proc/self/fd").is_dir(),
    reason="counting open descriptors needs /proc",
)
def test_a_merge_closes_every_older_table_it_opened(tmp_path: Path) -> None:
    """One descriptor per older table per pass would exhaust the process in a few tiers.

    Counted around a merge that succeeds and one that fails after the older
    tables were already open, since the failing path is the one where a handle
    goes unreleased.
    """
    sources, outside = merge_over_a_deleted_key(tmp_path)
    damaged = write_table(tmp_path / "damaged.sst", [(b"a", b"v"), (b"b", b"v")])
    with open(damaged, "r+b") as handle:
        footer = read_footer(handle)
        on_disk = list(
            iter_records(
                handle,
                start_offset=footer.data_block_offset,
                end_offset=footer.data_block_end,
            )
        )
        handle.seek(on_disk[1].offset)
        handle.write(struct.pack("<I", 1_000_000))

    def open_descriptors() -> int:
        return len(os.listdir("/proc/self/fd"))

    before = open_descriptors()
    merge_sstables(sources, tmp_path / "merged.sst", older_tables=[outside])
    after_success = open_descriptors()
    with pytest.raises(SSTableTruncatedRecordError):
        merge_sstables([damaged], tmp_path / "failed.sst", older_tables=[outside, *sources])

    assert after_success == before
    assert open_descriptors() == before


def test_consulting_an_older_table_does_not_disturb_a_reader_of_it(tmp_path: Path) -> None:
    """Real threads, because the claim is about file handles and only threads test it.

    The merge now opens tables it is not merging, and the engine may be serving
    ``get`` from those same files at the same time: an older table outside a
    compaction is exactly a table reads are still being answered from. The merge
    opens its own handle for each, so neither side moves the other's cursor. If
    they shared one, the lookups below would decode records from the wrong offset
    and the tombstone decision would be made on whatever came back.
    """
    pairs: list[Pair] = [(f"key-{index:04d}".encode(), f"v{index}".encode()) for index in range(80)]
    outside = write_table(tmp_path / "outside.sst", pairs)
    newer = write_table(tmp_path / "newer.sst", [(key, None) for key, _ in pairs])
    older = write_table(tmp_path / "older.sst", [(b"zzz", b"kept")])
    start = threading.Barrier(2)
    stop = threading.Event()
    failures: list[BaseException] = []

    def read_until_stopped() -> None:
        try:
            with SSTableReader.open(outside) as reader:
                start.wait(timeout=10)
                while not stop.is_set():
                    for key, value in pairs:
                        record = reader.lookup(key)
                        assert record is not None and record.value == value
        except BaseException as error:  # reported to the main thread, never swallowed
            failures.append(error)

    thread = threading.Thread(target=read_until_stopped)
    thread.start()
    try:
        start.wait(timeout=10)
        layout = merge_sstables([newer, older], tmp_path / "merged.sst", older_tables=[outside])
    finally:
        stop.set()
        thread.join(timeout=10)

    assert failures == []
    assert table_records(layout.path) == [*[(key, None) for key, _ in pairs], (b"zzz", b"kept")]


# ---------------------------------------------------------------------------
# M8.5: the swap. Nothing is deleted until the merged table's footer is on the
# disk and reads back valid, and what a crash can leave behind at each point.
# ---------------------------------------------------------------------------


_CHILD_SOURCE = '''\
"""Runs one compaction in a child process the test can SIGKILL at a chosen moment.

A real process and a real signal, because the claim under test is about what a
crash leaves on the disk. An exception raised in-process still runs the writer's
cleanup, which is exactly the code path a crash does not take, so a test that
raised instead of killing would be asserting that the cleanup works rather than
that the on-disk ordering is safe without it.

Takes the directory holding the ``source-N.sst`` tables and the moment to stop
at, and announces on stdout once it is there, so the parent kills it at a known
point in the compaction rather than a likely one.
"""

import sys
from dataclasses import dataclass
from pathlib import Path

from ledgerlog import compaction
from ledgerlog.compaction import CompactionTier
from ledgerlog.sstable import SSTableWriter

STALL_AFTER_RECORDS = 7


@dataclass(frozen=True)
class Table:
    """The age, size and path a tier needs of its tables, and nothing else."""

    sequence: int
    size_bytes: int
    path: Path


def announce(message):
    sys.stdout.write(message + "\\n")
    sys.stdout.flush()


def wait_to_be_killed():
    """Block until the signal lands. The parent never writes to stdin."""
    sys.stdin.read()
    raise SystemExit("parent closed stdin instead of killing the child")


class StallingWriter(SSTableWriter):
    """A writer that stops for good partway through the merged table's data block.

    Interposed on the name the merge looks its writer up under, so everything
    around it is the real thing: real sources, real records, a real temporary
    file holding a real partial data block, and no index, filter or footer.
    Stopping on a record count rather than after a delay is what makes the kill
    land at a known point instead of a likely one.
    """

    def add(self, key, value):
        if self.record_count >= STALL_AFTER_RECORDS:
            announce("stalled")
            wait_to_be_killed()
        return super().add(key, value)


def stall_instead_of_swapping(*args, **kwargs):
    """Stand in for the swap, so the child dies in the window the merge opens."""
    announce("merged")
    wait_to_be_killed()


def main():
    directory = Path(sys.argv[1])
    moment = sys.argv[2]
    if moment == "mid-merge":
        compaction.SSTableWriter = StallingWriter
    elif moment == "after-merge":
        compaction.swap_in_merged_table = stall_instead_of_swapping
    else:
        raise SystemExit(f"unknown moment {moment!r}")

    # Newest first, which is the order a tier lists its tables in and the order
    # that decides which write of a repeated key wins.
    paths = sorted(directory.glob("source-*.sst"), reverse=True)
    tier = CompactionTier(
        tables=tuple(
            Table(
                sequence=int(path.stem.split("-")[1]),
                size_bytes=path.stat().st_size,
                path=path,
            )
            for path in paths
        ),
        ready=True,
    )
    compaction.compact_tier(tier, directory / "merged.sst", other_tables=())
    announce("finished")


main()
'''


class _KilledCompaction:
    """A compaction running in a child process that the test can SIGKILL."""

    def __init__(self, directory: Path, script: Path, moment: str) -> None:
        script.write_text(_CHILD_SOURCE)
        source_root = str(Path(ledgerlog.__file__).resolve().parent.parent)
        environment = dict(os.environ)
        existing = environment.get("PYTHONPATH")
        environment["PYTHONPATH"] = (
            source_root if not existing else os.pathsep.join([source_root, existing])
        )
        self._process = subprocess.Popen(
            [sys.executable, str(script), str(directory), moment],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            env=environment,
        )

    def wait_for(self, announcement: str, *, timeout: float = 60.0) -> None:
        """Block until the child says it has reached ``announcement``.

        The wait has a deadline because these tests run unattended: a child that
        neither answers nor exits should fail the suite rather than hang it, and
        a blocking read would hang it.
        """
        assert self._process.stdout is not None
        ready, _, _ = select.select([self._process.stdout], [], [], timeout)
        assert ready, f"child did not reach {announcement!r} within {timeout} seconds"
        line = self._process.stdout.readline()
        assert line.strip() == announcement, f"child announced {line!r} instead of {announcement!r}"

    def kill(self) -> None:
        """Kill the child outright and wait for the operating system to reap it."""
        os.kill(self._process.pid, signal.SIGKILL)
        assert self._process.wait(timeout=30) != 0, "the child exited cleanly instead of dying"

    def close(self) -> None:
        """Make sure the child is gone, whatever the test did or did not do."""
        if self._process.poll() is None:
            self._process.kill()
            self._process.wait(timeout=30)
        for stream in (self._process.stdin, self._process.stdout):
            if stream is not None:
                stream.close()


@pytest.fixture
def killed_compaction(tmp_path: Path) -> Iterator[Callable[[Path, str], _KilledCompaction]]:
    """Factory for child compactions, each cleaned up when the test finishes."""
    children: list[_KilledCompaction] = []

    def start(directory: Path, moment: str) -> _KilledCompaction:
        child = _KilledCompaction(directory, tmp_path / f"child_{len(children)}.py", moment)
        children.append(child)
        return child

    try:
        yield start
    finally:
        for child in children:
            child.close()


def data_directory(tmp_path: Path) -> Path:
    """A directory holding only tables, so a listing of it is an assertion.

    Separate from ``tmp_path`` because the child process's script is written
    there, and "the directory holds exactly the merged table" is most of what
    this story claims.
    """
    directory = tmp_path / "data"
    directory.mkdir()
    return directory


def tier_with_a_dropped_tombstone(directory: Path) -> tuple[list[Path], list[Pair], bytes]:
    """Three real source tables, their newest-wins merge by hand, and the deleted key.

    Thirty keys rather than a handful, so that a merge of these is still
    streaming records when the child process above is killed partway through it,
    and so the temporary file that kill leaves behind is unmistakably a partial
    table rather than an empty one.

    The newest table deletes ``k19``, which both older tables hold values for.
    With nothing older outside the merge that tombstone is dropped (story M8.3),
    so the merged table holds no record at all for a key its sources hold values
    for. That is the case the retirement order has to survive: it is the one key
    a surviving source could resurrect.

    Returned as (sources newest first, the merged records, the deleted key).
    """
    oldest = write_table(
        directory / "source-0.sst",
        [(f"k{index:02d}".encode(), f"old-{index:02d}".encode()) for index in range(20)],
    )
    middle = write_table(
        directory / "source-1.sst",
        [(f"k{index:02d}".encode(), f"mid-{index:02d}".encode()) for index in range(10, 30)],
    )
    newest = write_table(
        directory / "source-2.sst",
        [
            *[(f"k{index:02d}".encode(), f"new-{index:02d}".encode()) for index in range(5, 15)],
            (b"k19", None),
        ],
    )
    expected: list[Pair] = []
    for index in range(30):
        key = f"k{index:02d}".encode()
        if index == 19:
            continue
        if 5 <= index <= 14:
            expected.append((key, f"new-{index:02d}".encode()))
        elif index <= 9:
            expected.append((key, f"old-{index:02d}".encode()))
        else:
            expected.append((key, f"mid-{index:02d}".encode()))
    return [newest, middle, oldest], expected, b"k19"


def merged_table(sources: Sequence[Path], destination: Path) -> Path:
    """Merge ``sources`` to ``destination`` the way the swap tests need it merged.

    ``older_tables=[]`` states that the tier is all there is, which is what lets
    the tombstone be dropped. A swap over a merge that kept every tombstone would
    be a weaker test: the key a retirement could resurrect would still be in the
    merged table.
    """
    return merge_sstables(sources, destination, older_tables=[]).path


# ---------------------------------------------------------------------------
# Criterion 1: sources go only after the merged footer is written and valid.
# ---------------------------------------------------------------------------


def test_a_merged_table_whose_footer_never_landed_retires_nothing(tmp_path: Path) -> None:
    """The swap reads the commit point off the disk, not off the merge's return value.

    The file here is a real merge output with its last byte removed, which is
    what a kill a few microseconds earlier would have left at that name. A swap
    that trusted the layout the merge handed back would delete all three sources
    on the strength of a footer that is not there.
    """
    sources, _, _ = tier_with_a_dropped_tombstone(tmp_path)
    before = {path: path.read_bytes() for path in sources}
    merged = merged_table(sources, tmp_path / "merged.sst")
    with open(merged, "r+b") as handle:
        handle.truncate(merged.stat().st_size - 1)

    with pytest.raises(MergeNotCommittedError) as refusal:
        swap_in_merged_table(merged, sources)

    assert refusal.value.inspection.status is SSTableStatus.INCOMPLETE
    assert {path: path.read_bytes() for path in sources} == before


def test_a_merged_table_with_a_footer_pointing_outside_itself_retires_nothing(
    tmp_path: Path,
) -> None:
    """A whole footer is not enough: it has to describe the file it is in.

    A footer whose sections fall outside the file cannot be the writer's, whatever
    its checksum says, and following one would seek past the end of the table. The
    sources are the only other copy of these records, so this is a refusal rather
    than a repair.
    """
    sources, _, _ = tier_with_a_dropped_tombstone(tmp_path)
    layout = merge_sstables(sources, tmp_path / "merged.sst", older_tables=[])
    with open(layout.path, "r+b") as handle:
        handle.seek(layout.footer_offset)
        handle.write(replace(layout.footer, bloom_filter_end=10**9).encode())

    with pytest.raises(MergeNotCommittedError) as refusal:
        swap_in_merged_table(layout.path, sources)

    assert refusal.value.inspection.status is SSTableStatus.CORRUPT
    assert all(inspect_sstable(path).status is SSTableStatus.VALID for path in sources)


def test_a_merged_table_in_an_unknown_format_version_retires_nothing(tmp_path: Path) -> None:
    """A committed table this build cannot read is still no reason to delete the sources.

    Its footer landed, so the records are real, but nothing here can prove that by
    reading them. Retiring the sources would leave the engine holding one table it
    refuses to open and no other copy of what is in it.
    """
    sources, _, _ = tier_with_a_dropped_tombstone(tmp_path)
    layout = merge_sstables(sources, tmp_path / "merged.sst", older_tables=[])
    with open(layout.path, "r+b") as handle:
        handle.seek(layout.footer_offset)
        handle.write(replace(layout.footer, format_version=SSTABLE_FORMAT_VERSION + 1).encode())

    with pytest.raises(MergeNotCommittedError) as refusal:
        swap_in_merged_table(layout.path, sources)

    assert refusal.value.inspection.status is SSTableStatus.UNSUPPORTED_VERSION
    assert all(path.is_file() for path in sources)


def test_a_merged_table_that_is_not_there_retires_nothing(tmp_path: Path) -> None:
    sources, _, _ = tier_with_a_dropped_tombstone(tmp_path)

    with pytest.raises(FileNotFoundError):
        swap_in_merged_table(tmp_path / "merged.sst", sources)

    assert all(path.is_file() for path in sources)


def test_the_refusal_names_the_table_and_says_what_was_wrong_with_it(tmp_path: Path) -> None:
    """An operator reading this has to know which file failed which check."""
    sources, _, _ = tier_with_a_dropped_tombstone(tmp_path)
    merged = merged_table(sources, tmp_path / "merged.sst")
    with open(merged, "r+b") as handle:
        handle.truncate(merged.stat().st_size - 1)

    with pytest.raises(MergeNotCommittedError, match="merged.sst") as refusal:
        swap_in_merged_table(merged, sources)

    message = str(refusal.value)
    assert "incomplete" in message
    assert "no source table was deleted" in message


# ---------------------------------------------------------------------------
# Criterion 3: after a crash-free compaction only the merged table is left, and
# it answers for everything the retired tables held.
# ---------------------------------------------------------------------------


def test_a_crash_free_compaction_leaves_only_the_merged_table(tmp_path: Path) -> None:
    directory = data_directory(tmp_path)
    sources, expected, _ = tier_with_a_dropped_tombstone(directory)

    swap = compact_tier(tier_of(sources), directory / "merged.sst", other_tables=())

    assert [path.name for path in directory.iterdir()] == ["merged.sst"]
    assert swap.merged == directory / "merged.sst"
    assert swap.retired == tuple(sources)
    assert swap.retired_count == len(sources)
    assert table_records(swap.merged) == expected


def test_the_merged_table_answers_for_every_key_the_retired_sources_held(tmp_path: Path) -> None:
    """ "Covering their key ranges", asserted as reads against the one surviving table.

    Checked by looking every key up rather than by comparing the data block,
    because what the engine will do with this table is read it, and a table whose
    records are all present but unreachable through its index would pass a
    contents comparison.
    """
    directory = data_directory(tmp_path)
    sources, expected, deleted_key = tier_with_a_dropped_tombstone(directory)

    swap = compact_tier(tier_of(sources), directory / "merged.sst", other_tables=())

    assert [(key, read_newest_first([swap.merged], key)) for key, _ in expected] == expected
    assert read_newest_first([swap.merged], deleted_key) is None


def test_the_swap_reports_the_footer_it_read_back_off_the_disk(tmp_path: Path) -> None:
    directory = data_directory(tmp_path)
    sources, expected, _ = tier_with_a_dropped_tombstone(directory)

    swap = compact_tier(tier_of(sources), directory / "merged.sst", other_tables=())

    with open(swap.merged, "rb") as handle:
        assert swap.footer == read_footer(handle)
    assert swap.footer.record_count == len(expected)


def test_a_swap_retires_exactly_the_sources_it_was_given(tmp_path: Path) -> None:
    """The swap deletes from its argument list, not from the directory it finds itself in."""
    directory = data_directory(tmp_path)
    sources, _, _ = tier_with_a_dropped_tombstone(directory)
    bystander = write_table(directory / "bystander.sst", [(b"k", b"v")])
    merged = merged_table(sources, directory / "merged.sst")

    swap_in_merged_table(merged, sources)

    assert sorted(path.name for path in directory.iterdir()) == ["bystander.sst", "merged.sst"]
    assert bystander.is_file()


def test_a_completed_swap_cannot_be_changed_afterwards(tmp_path: Path) -> None:
    directory = data_directory(tmp_path)
    sources, _, _ = tier_with_a_dropped_tombstone(directory)

    swap = compact_tier(tier_of(sources), directory / "merged.sst", other_tables=())

    assert isinstance(swap, CompactionSwap)
    with pytest.raises(FrozenInstanceError):
        swap.retired = ()  # type: ignore[misc]


def test_compact_tier_leaves_everything_alone_when_the_merge_fails(tmp_path: Path) -> None:
    """A merge that raises retires nothing and leaves no debris at either name."""
    directory = data_directory(tmp_path)
    sources, _, _ = tier_with_a_dropped_tombstone(directory)
    with open(sources[0], "r+b") as handle:
        handle.truncate(sources[0].stat().st_size - FOOTER_SIZE)

    with pytest.raises(SSTableIncompleteError):
        compact_tier(tier_of(sources), directory / "merged.sst", other_tables=())

    assert sorted(path.name for path in directory.iterdir()) == [
        "source-0.sst",
        "source-1.sst",
        "source-2.sst",
    ]


# ---------------------------------------------------------------------------
# The retirement order, which is what makes a crash between two unlinks safe.
# ---------------------------------------------------------------------------


def test_the_sources_are_retired_oldest_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Asserted as the order of the removals, since the end state cannot show it."""
    sources, _, _ = tier_with_a_dropped_tombstone(tmp_path)
    merged = merged_table(sources, tmp_path / "merged.sst")
    removals: list[Path] = []
    unlink = Path.unlink

    def recording_unlink(self: Path, missing_ok: bool = False) -> None:
        removals.append(self)
        unlink(self, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", recording_unlink)

    swap_in_merged_table(merged, sources)

    assert removals == list(reversed(sources))


def test_a_retirement_stopped_partway_cannot_resurrect_a_deleted_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The reason the order is a correctness rule and not tidiness.

    The merged table holds no record for ``k19``, because the tombstone that
    deleted it was dropped as safe to drop. A read now falls through the merged
    table into whichever sources a crash left behind, so what those are decides
    whether the key stays deleted. Removing the oldest first leaves the newest,
    which is where the tombstone is.
    """
    sources, _, deleted_key = tier_with_a_dropped_tombstone(tmp_path)
    merged = merged_table(sources, tmp_path / "merged.sst")
    removals: list[Path] = []
    unlink = Path.unlink

    def failing_unlink(self: Path, missing_ok: bool = False) -> None:
        if removals:
            raise OSError("the power went out between two unlinks")
        removals.append(self)
        unlink(self, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", failing_unlink)

    with pytest.raises(OSError, match="between two unlinks"):
        swap_in_merged_table(merged, sources)

    monkeypatch.undo()
    survivors = [path for path in sources if path.is_file()]
    assert removals == [sources[-1]]
    assert survivors == sources[:-1]
    assert read_newest_first([merged, *survivors], deleted_key) is None


def test_retiring_the_newest_source_first_would_resurrect_the_deleted_key(tmp_path: Path) -> None:
    """The counterfactual, by hand: the same partial state in the opposite order.

    Without this the test above would pass for a swap that deleted in any order,
    since every order leaves the key deleted when nothing goes wrong. This is what
    makes it an assertion about the order.
    """
    sources, _, deleted_key = tier_with_a_dropped_tombstone(tmp_path)
    merged = merged_table(sources, tmp_path / "merged.sst")

    sources[0].unlink()

    assert read_newest_first([merged, *sources[1:]], deleted_key) == b"mid-19"


def test_a_swap_run_again_finishes_an_interrupted_retirement(tmp_path: Path) -> None:
    """A source already gone is not an error, so a retry is how a swap is finished."""
    sources, _, _ = tier_with_a_dropped_tombstone(tmp_path)
    merged = merged_table(sources, tmp_path / "merged.sst")
    sources[-1].unlink()

    swap = swap_in_merged_table(merged, sources)

    assert swap.retired == tuple(sources)
    assert not any(path.exists() for path in sources)


# ---------------------------------------------------------------------------
# Criterion 2: a real process killed mid-merge. The sources survive and the
# partial output is discarded.
# ---------------------------------------------------------------------------


def test_a_process_killed_mid_merge_leaves_every_source_table_intact(
    tmp_path: Path, killed_compaction: Callable[[Path, str], _KilledCompaction]
) -> None:
    directory = data_directory(tmp_path)
    sources, _, _ = tier_with_a_dropped_tombstone(directory)
    before = {path: path.read_bytes() for path in sources}
    child = killed_compaction(directory, "mid-merge")
    child.wait_for("stalled")

    child.kill()

    assert {path: path.read_bytes() for path in sources} == before
    assert all(inspect_sstable(path).status is SSTableStatus.VALID for path in sources)
    assert not (directory / "merged.sst").exists()


def test_a_process_killed_mid_merge_leaves_a_partial_table_the_sweep_discards(
    tmp_path: Path, killed_compaction: Callable[[Path, str], _KilledCompaction]
) -> None:
    """What is left at the temporary name is not a table, whatever is in it.

    That the kill landed in the middle of a merge rather than before one started
    is established by the announcement the test waited for, which the child only
    makes from inside its eighth record.

    Nothing is asserted about the size, because a killed writer's bytes are
    whatever its buffer happened to have flushed, which for a merge this small is
    none of them. An empty file at that name is as much debris as a half-written
    one, and a sweep that only recognised the second would leave the first
    behind.
    """
    directory = data_directory(tmp_path)
    sources, _, _ = tier_with_a_dropped_tombstone(directory)
    debris = temp_table_path(directory / "merged.sst")
    child = killed_compaction(directory, "mid-merge")
    child.wait_for("stalled")
    child.kill()
    assert debris.is_file()
    assert inspect_sstable(debris).status is SSTableStatus.INCOMPLETE

    discarded = discard_partial_tables(directory)

    assert discarded == (debris,)
    assert sorted(path.name for path in directory.iterdir()) == [
        "source-0.sst",
        "source-1.sst",
        "source-2.sst",
    ]


def test_a_compaction_rerun_after_a_crash_mid_merge_loses_nothing(
    tmp_path: Path, killed_compaction: Callable[[Path, str], _KilledCompaction]
) -> None:
    """The whole point of the ordering: the second attempt has everything it needs."""
    directory = data_directory(tmp_path)
    sources, expected, _ = tier_with_a_dropped_tombstone(directory)
    child = killed_compaction(directory, "mid-merge")
    child.wait_for("stalled")
    child.kill()
    discard_partial_tables(directory)

    swap = compact_tier(tier_of(sources), directory / "merged.sst", other_tables=())

    assert table_records(swap.merged) == expected
    assert [path.name for path in directory.iterdir()] == ["merged.sst"]


def test_a_process_killed_between_the_merge_and_the_swap_keeps_both(
    tmp_path: Path, killed_compaction: Callable[[Path, str], _KilledCompaction]
) -> None:
    """The window the ordering deliberately opens, and what it costs: space, not data.

    Killed after the merged table was committed and before a single source was
    unlinked, the disk holds both. Nothing is lost and nothing is resurrected,
    since the merged table is newer than every source it was merged from, and the
    swap can be finished afterwards.
    """
    directory = data_directory(tmp_path)
    sources, expected, _ = tier_with_a_dropped_tombstone(directory)
    child = killed_compaction(directory, "after-merge")
    child.wait_for("merged")

    child.kill()

    merged = directory / "merged.sst"
    assert inspect_sstable(merged).status is SSTableStatus.VALID
    assert all(path.is_file() for path in sources)
    assert discard_partial_tables(directory) == ()

    swap = swap_in_merged_table(merged, sources)

    assert swap.retired == tuple(sources)
    assert [path.name for path in directory.iterdir()] == ["merged.sst"]
    assert table_records(merged) == expected


# ---------------------------------------------------------------------------
# The startup sweep on its own.
# ---------------------------------------------------------------------------


def test_the_sweep_discards_debris_and_leaves_committed_tables_alone(tmp_path: Path) -> None:
    directory = data_directory(tmp_path)
    table = write_table(directory / "table.sst", [(b"k", b"v")])
    debris = temp_table_path(directory / "merged.sst")
    debris.write_bytes(b"half a table")

    discarded = discard_partial_tables(directory)

    assert discarded == (debris,)
    assert [path.name for path in directory.iterdir()] == [table.name]
    assert inspect_sstable(table).status is SSTableStatus.VALID


def test_the_sweep_finds_nothing_to_discard_in_a_clean_directory(tmp_path: Path) -> None:
    directory = data_directory(tmp_path)
    write_table(directory / "table.sst", [(b"k", b"v")])

    empty = tmp_path / "empty"
    empty.mkdir()

    assert discard_partial_tables(directory) == ()
    assert discard_partial_tables(empty) == ()


def test_the_sweep_discards_every_partial_table_it_finds(tmp_path: Path) -> None:
    """A crash leaves one, but a directory can accumulate them across crashes."""
    directory = data_directory(tmp_path)
    first = temp_table_path(directory / "merged-1.sst")
    second = temp_table_path(directory / "merged-2.sst")
    for debris in (first, second):
        debris.write_bytes(b"half a table")

    assert discard_partial_tables(directory) == (first, second)
    assert list(directory.iterdir()) == []


def test_the_sweep_leaves_a_directory_named_like_debris_alone(tmp_path: Path) -> None:
    """Named like debris but not a file, so unlinking it would fail rather than help."""
    directory = data_directory(tmp_path)
    lookalike = temp_table_path(directory / "merged.sst")
    lookalike.mkdir()

    assert discard_partial_tables(directory) == ()
    assert lookalike.is_dir()


def test_a_stale_partial_table_does_not_block_the_next_merge(tmp_path: Path) -> None:
    """A swept directory is not a precondition for compacting again.

    The writer opens its temporary file for writing, which truncates whatever was
    there, so debris at that name is overwritten rather than appended to. Worth
    pinning: the opposite would mean a crash made a destination name unusable
    until something swept it.
    """
    directory = data_directory(tmp_path)
    sources, expected, _ = tier_with_a_dropped_tombstone(directory)
    temp_table_path(directory / "merged.sst").write_bytes(b"debris from an earlier crash")

    swap = compact_tier(tier_of(sources), directory / "merged.sst", other_tables=())

    assert table_records(swap.merged) == expected
    assert [path.name for path in directory.iterdir()] == ["merged.sst"]


# ---------------------------------------------------------------------------
# What a swap refuses to be asked.
# ---------------------------------------------------------------------------


def test_the_merged_table_cannot_be_one_of_the_sources(tmp_path: Path) -> None:
    """Otherwise the swap would delete the table it had just confirmed."""
    sources, _, _ = tier_with_a_dropped_tombstone(tmp_path)
    merged = merged_table(sources, tmp_path / "merged.sst")

    with pytest.raises(ValueError, match="would delete the merged table itself"):
        swap_in_merged_table(merged, [merged, *sources])

    assert merged.is_file()
    assert all(path.is_file() for path in sources)


def test_a_swap_needs_at_least_one_source_to_retire(tmp_path: Path) -> None:
    sources, _, _ = tier_with_a_dropped_tombstone(tmp_path)
    merged = merged_table(sources, tmp_path / "merged.sst")

    with pytest.raises(ValueError, match="at least one source"):
        swap_in_merged_table(merged, [])


def test_the_same_source_cannot_be_listed_twice_in_a_swap(tmp_path: Path) -> None:
    """The order of the removals is what a crash mid-swap rests on, and a repeat has none."""
    sources, _, _ = tier_with_a_dropped_tombstone(tmp_path)
    merged = merged_table(sources, tmp_path / "merged.sst")

    with pytest.raises(ValueError, match="listed twice"):
        swap_in_merged_table(merged, [*sources, sources[0]])

    assert all(path.is_file() for path in sources)
