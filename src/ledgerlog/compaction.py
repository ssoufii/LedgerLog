"""Compaction: which SSTables belong together, and when a group is worth merging.

Scope of this module today (story M8.1): grouping the tables an engine holds into
size tiers, and saying which of those tiers has accumulated enough tables to be
worth merging. Nothing here reads a table, writes a table or deletes one. It
takes a set of tables, each of which knows its own age and its own size, and
returns a plan: the tiers, and a flag per tier saying whether it is ready. The
merge that consumes a ready tier is story M8.2, the tombstone and expiry rules it
applies are M8.3 and M8.4, and the atomic swap that retires the sources is M8.5.

Why the planning is a separate, file-free step: what to compact is a policy
decision made from metadata alone, and it is the part of compaction with the most
ways to be subtly wrong (tables grouped by the wrong measure, a tier that
triggers forever, a merged table falling back into the tier it came from). Split
out like this it can be tested exhaustively against hand-computed groupings and
against random size sets, with no directory, no bytes on disk and no threads, per
CLAUDE.md's rule that each component stay independently testable. The merge, when
it arrives, is then free to be about bytes.

Why size-tiered, and what the two knobs mean (ARCHITECTURE.md section 4): tables
of similar size are merged together, so a record is rewritten roughly once per
tier it passes through rather than once per compaction pass, which is what bounds
write amplification. :attr:`CompactionPolicy.size_ratio` is what "similar" means,
as a bound on how far the largest table in a tier may exceed the smallest.
:attr:`CompactionPolicy.min_tier_tables` is how many tables a tier must hold
before merging them buys enough: merging fewer means rewriting almost as many
bytes for a smaller reduction in the number of tables a read has to check.

A tuning caveat for whoever sets those two, since the interaction is not obvious
and ROADMAP.md milestone 10 is where it gets measured: the merged output of a
ready tier is close to ``min_tier_tables`` times the size of one source, so a
``size_ratio`` well above ``min_tier_tables`` puts that output back in the tier it
was merged from, which is how a tier gets rewritten over and over and write
amplification stops being bounded. The defaults below keep the ratio under the
threshold deliberately. A merged table that lands in its sources' tier anyway,
because deletes and overwrites made it much smaller than the sum of its parts, is
a different case and the right one: it really is comparable in size to what is
left beside it, and merging it again is the correct call.

Nothing in this module is a promise about durability, so nothing in it touches a
file. It is pure: the same tables and the same policy give the same plan, which is
also what lets a caller recompute a plan on every change instead of maintaining
one incrementally and having to prove the incremental update agrees with the
recomputation.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Generic, Protocol, TypeVar, runtime_checkable

DEFAULT_SIZE_RATIO = 2.0
"""How far the largest table in a tier may exceed the smallest, by default.

Two, so that the output of a merge (roughly :data:`DEFAULT_MIN_TIER_TABLES`
sources' worth of bytes) lands in a tier above the sources it came from rather
than back among them. A larger ratio makes tiers fewer and wider, which means
more tables merged at once and more bytes rewritten per pass; a ratio just above
one makes a tier of almost exactly equal files, which flushes do produce but
merged outputs do not, so tiers above the first would rarely fill. A starting
point rather than a measured optimum: ROADMAP.md milestone 10 (story M10.4) is
where this is chosen against benchmark results.
"""

DEFAULT_MIN_TIER_TABLES = 4
"""How many tables a tier must hold before it is worth merging, by default.

Four is the usual size-tiered starting point. It is the point where a merge pays:
four tables become one, so a read that had to check four files checks one, for
one rewrite of the data. Two would rewrite the same bytes to remove one file,
and a much larger number would leave a read checking many tables while the tier
waits to fill.
"""

MIN_MIN_TIER_TABLES = 2
"""Smallest legal :attr:`CompactionPolicy.min_tier_tables`.

A tier of one table has nothing to merge with: compacting it would rewrite every
record to produce a file holding the same records, so a threshold of one would
mean compacting forever and changing nothing.
"""


@runtime_checkable
class SizedTable(Protocol):
    """What this module needs to know about an SSTable, and nothing more.

    A protocol rather than a concrete type so that planning does not depend on the
    engine: :class:`~ledgerlog.engine.SSTableHandle` satisfies it, and so does a
    two-field stand-in in a test, which is what lets the grouping rules be tested
    against hand-computed answers without writing any files.

    ``sequence`` is the table's age, ascending with the order the tables were
    created, and it is here for the merge rather than for the grouping: grouping
    cares only about sizes, but a tier is handed to M8.2 to merge with newest
    write wins semantics, so the tables in it have to come back in a known order.
    """

    @property
    def sequence(self) -> int:
        """Number identifying the table's age. Higher is newer."""

    @property
    def size_bytes(self) -> int:
        """Size of the table on disk, in bytes."""


TableT = TypeVar("TableT", bound=SizedTable)


@dataclass(frozen=True)
class CompactionPolicy:
    """The two numbers that decide what gets grouped and what gets merged.

    Frozen, because a plan records the policy it was made under and a policy that
    could change afterwards would make that record a lie. Changing policy means
    building another one and planning again, which is cheap (see
    :func:`plan_compaction`).

    Both knobs are validated here rather than where they are used, since a bad
    value produces a plan that is wrong rather than an error: a ratio of one puts
    every table in a tier of its own and nothing ever compacts, and a threshold of
    one marks every single table ready forever.
    """

    size_ratio: float = DEFAULT_SIZE_RATIO
    min_tier_tables: int = DEFAULT_MIN_TIER_TABLES

    def __post_init__(self) -> None:
        if isinstance(self.size_ratio, bool) or not isinstance(self.size_ratio, (int, float)):
            raise TypeError(f"size_ratio must be a float, got {type(self.size_ratio).__name__}")
        if not math.isfinite(self.size_ratio):
            raise ValueError(f"size_ratio must be finite, got {self.size_ratio}")
        if self.size_ratio <= 1.0:
            raise ValueError(f"size_ratio must be greater than 1, got {self.size_ratio}")
        if isinstance(self.min_tier_tables, bool) or not isinstance(self.min_tier_tables, int):
            raise TypeError(
                f"min_tier_tables must be an int, got {type(self.min_tier_tables).__name__}"
            )
        if self.min_tier_tables < MIN_MIN_TIER_TABLES:
            raise ValueError(
                f"min_tier_tables must be at least {MIN_MIN_TIER_TABLES}, "
                f"got {self.min_tier_tables}"
            )
        # Normalizing an int ratio to float here keeps ``CompactionPolicy(size_ratio=2)``
        # and ``CompactionPolicy(size_ratio=2.0)`` equal as dataclasses, so two
        # plans built from what a caller meant as the same policy compare equal.
        object.__setattr__(self, "size_ratio", float(self.size_ratio))

    def fits_tier(self, reference_size_bytes: int, size_bytes: int) -> bool:
        """True if a table of ``size_bytes`` is comparable to a tier's largest table.

        ``reference_size_bytes`` is the size of the largest table already in the
        tier, so the bound this enforces is on the whole tier: every member is
        within :attr:`size_ratio` of the biggest one, and therefore of each other.
        Comparing against the tier's largest rather than against its mean is what
        makes that a guarantee instead of a tendency, since a mean drifts down as
        small tables join and would let a tier stretch arbitrarily wide.

        The bound is strict, so two tables exactly :attr:`size_ratio` apart go to
        different tiers. That is the case a merge produces (see this module's
        docstring), and treating it as comparable is exactly what would feed a
        merged table back into the tier it was merged from.

        Two tables of exactly the same size are comparable whatever the ratio
        says, which is the one case the multiplication below gets wrong on its
        own: at a reference of zero it would put every zero byte table in a tier
        by itself. No SSTable this engine writes is empty (a table is at least a
        header and a footer), so that is defensiveness rather than a live case,
        but a size comparison that says two equal sizes are incomparable would be
        wrong in a way later callers would have to work around.

        Multiplying the candidate up rather than dividing the reference down
        avoids a division by a size, so a zero byte table cannot turn the check
        into a division by zero.
        """
        if size_bytes == reference_size_bytes:
            return True
        return size_bytes * self.size_ratio > reference_size_bytes


@dataclass(frozen=True)
class CompactionTier(Generic[TableT]):
    """One group of comparably sized tables, and whether it is ready to merge.

    ``tables`` is newest first, the order :attr:`~ledgerlog.engine.LedgerLog.sstables`
    uses and the order the merge in story M8.2 needs: a key present in several of
    these tables takes its value from the first one that holds it, so the ordering
    is what newest write wins means once the tier is being merged.

    ``ready`` is stored rather than recomputed from ``len(tables)`` on demand
    because readiness is a policy question, and a tier that answered it from a
    policy of its own would be a second place where the threshold lives. The
    plan applies the policy once, here.
    """

    tables: tuple[TableT, ...]
    ready: bool

    def __post_init__(self) -> None:
        if not self.tables:
            raise ValueError("a CompactionTier must hold at least one table")
        sequences = [table.sequence for table in self.tables]
        if sequences != sorted(sequences, reverse=True) or len(set(sequences)) != len(sequences):
            raise ValueError(
                "a CompactionTier's tables must be ordered newest first by distinct "
                f"sequence number, got {sequences}"
            )

    @property
    def table_count(self) -> int:
        """How many tables the tier holds."""
        return len(self.tables)

    @property
    def total_size_bytes(self) -> int:
        """Bytes a merge of this tier would have to read, and roughly write."""
        return sum(table.size_bytes for table in self.tables)

    @property
    def largest_size_bytes(self) -> int:
        """Size of the tier's largest table, which is what membership is measured against."""
        return max(table.size_bytes for table in self.tables)

    @property
    def smallest_size_bytes(self) -> int:
        """Size of the tier's smallest table."""
        return min(table.size_bytes for table in self.tables)

    @property
    def sequences(self) -> tuple[int, ...]:
        """Sequence numbers of the tier's tables, newest first."""
        return tuple(table.sequence for table in self.tables)


@dataclass(frozen=True)
class CompactionPlan(Generic[TableT]):
    """Every table an engine holds, grouped into tiers, with the ready ones flagged.

    A snapshot and not a live view. It describes the tables it was given at the
    moment it was built, so a caller keeps a plan only as long as the set of
    tables it came from is unchanged, and builds another one when that set
    changes. That is the whole of the "re-evaluate on a new table" requirement:
    there is no incremental update to get wrong.

    ``tiers`` runs smallest tier first, by the size of the tier's largest table,
    which is the order compaction should consider them in. A tier of small tables
    is the cheapest to merge per file it removes, and it is where flushes keep
    adding work, so draining it first is what keeps the number of tables a read
    must check from growing while a big tier is being merged.
    """

    policy: CompactionPolicy = field(default_factory=CompactionPolicy)
    tiers: tuple[CompactionTier[TableT], ...] = ()

    @property
    def table_count(self) -> int:
        """How many tables the plan covers, across every tier."""
        return sum(tier.table_count for tier in self.tiers)

    @property
    def ready_tiers(self) -> tuple[CompactionTier[TableT], ...]:
        """The tiers holding at least :attr:`CompactionPolicy.min_tier_tables` tables.

        In the same smallest-first order as :attr:`tiers`.
        """
        return tuple(tier for tier in self.tiers if tier.ready)

    @property
    def needs_compaction(self) -> bool:
        """True if any tier is ready to be merged."""
        return any(tier.ready for tier in self.tiers)

    @property
    def next_tier(self) -> CompactionTier[TableT] | None:
        """The tier a compaction should merge next, or ``None`` if none is ready.

        The smallest ready tier, for the reason :attr:`tiers` is ordered the way it
        is. Picking the largest instead would spend the most I/O for the same
        reduction of one merge's worth of files, while small tables kept piling up
        behind it.
        """
        ready = self.ready_tiers
        return ready[0] if ready else None


def plan_compaction(
    tables: Iterable[TableT],
    policy: CompactionPolicy | None = None,
) -> CompactionPlan[TableT]:
    """Group ``tables`` into size tiers and flag the ones ready to merge.

    ``tables`` may come in any order, since grouping is by size and the ordering
    inside a tier is imposed here. Passing
    :attr:`~ledgerlog.engine.LedgerLog.sstables` straight in is the expected use.

    The grouping walks the tables from largest to smallest, opening a tier at the
    largest table not yet placed and adding each following table while
    :meth:`CompactionPolicy.fits_tier` accepts it against that first one. Walking
    downwards is what makes the tier's reference size fixed for the whole tier:
    the largest member is known before any other is admitted, so the bound holds
    for every pair in the tier and does not depend on the order the tables
    arrived in. Sorting by size and sweeping once also means a table cannot be
    admitted to two tiers, which a nearest-tier search over drifting references
    could otherwise allow.

    Ties in size are broken by sequence number, newest first, purely so that the
    plan is a function of its inputs: equally sized tables are interchangeable to
    the grouping, and leaving their relative order to the iteration order of the
    caller's container would make two plans over the same tables differ.

    Raises ``TypeError`` or ``ValueError`` for a table whose sequence or size is
    not a usable number, and for a repeated sequence number. The last of those
    matters more than it looks: a sequence number is the table's identity to every
    caller above this module, so two tables claiming one would give a merge two
    different sets of records to believe (and, in M8.5, the wrong file to delete).
    """
    policy = policy if policy is not None else CompactionPolicy()
    ordered = sorted(_validated(tables), key=lambda table: (-table.size_bytes, -table.sequence))

    tiers: list[CompactionTier[TableT]] = []
    current: list[TableT] = []
    reference = 0
    for table in ordered:
        if current and not policy.fits_tier(reference, table.size_bytes):
            tiers.append(_build_tier(current, policy))
            current = []
        if not current:
            reference = table.size_bytes
        current.append(table)
    if current:
        tiers.append(_build_tier(current, policy))

    # Built largest tier first by the sweep above, published smallest first, per
    # CompactionPlan.tiers.
    return CompactionPlan(policy=policy, tiers=tuple(reversed(tiers)))


def _validated(tables: Iterable[TableT]) -> list[TableT]:
    """Return ``tables`` as a list, having checked each one can be planned with.

    Materialized rather than validated lazily, because the grouping sorts its
    input and so has to hold it anyway, and because a duplicate sequence number
    can only be found by looking at the whole set.
    """
    materialized = list(tables)
    seen: set[int] = set()
    for table in materialized:
        sequence = table.sequence
        size_bytes = table.size_bytes
        if isinstance(sequence, bool) or not isinstance(sequence, int):
            raise TypeError(f"table sequence must be an int, got {type(sequence).__name__}")
        if isinstance(size_bytes, bool) or not isinstance(size_bytes, int):
            raise TypeError(f"table size_bytes must be an int, got {type(size_bytes).__name__}")
        if sequence < 0:
            raise ValueError(f"table sequence must not be negative, got {sequence}")
        if size_bytes < 0:
            raise ValueError(f"table size_bytes must not be negative, got {size_bytes}")
        if sequence in seen:
            raise ValueError(f"two tables carry sequence number {sequence}")
        seen.add(sequence)
    return materialized


def _build_tier(tables: list[TableT], policy: CompactionPolicy) -> CompactionTier[TableT]:
    """Freeze one group of tables into a tier, newest first, readiness applied."""
    newest_first = tuple(sorted(tables, key=lambda table: table.sequence, reverse=True))
    return CompactionTier(
        tables=newest_first,
        ready=len(newest_first) >= policy.min_tier_tables,
    )
