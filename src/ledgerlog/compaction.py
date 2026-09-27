"""Compaction: which SSTables belong together, and merging a group into one.

Scope of this module today (stories M8.1 and M8.2): grouping the tables an engine
holds into size tiers, saying which of those tiers has accumulated enough tables
to be worth merging, and merging one of those tiers into a single new SSTable
under newest write wins semantics. Planning takes a set of tables, each of which
knows its own age and its own size, and returns a plan: the tiers, and a flag per
tier saying whether it is ready. Merging takes the tables of one tier and writes
their records out once, deduplicated, keeping the newest write for each key.

What is still not here: a merge drops nothing it is handed, so the tombstone rule
(M8.3) and the expiry rule (M8.4) are yet to come, and nothing in this module
deletes a source table or tells the engine to start reading the merged one, which
is the atomic swap of M8.5. A merge today produces a new file and reports where it
is, and that is all.

Why the planning is a separate, file-free step: what to compact is a policy
decision made from metadata alone, and it is the part of compaction with the most
ways to be subtly wrong (tables grouped by the wrong measure, a tier that
triggers forever, a merged table falling back into the tier it came from). Split
out like this it can be tested exhaustively against hand-computed groupings and
against random size sets, with no directory, no bytes on disk and no threads, per
CLAUDE.md's rule that each component stay independently testable. The merge is
then free to be about bytes, and it splits the same way for the same reason:
:func:`merge_records` is the newest-wins rule over sorted streams of anything,
testable against hand-computed answers with no files, and :func:`merge_sstables`
is the part that opens tables and writes one.

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

Planning touches no file and makes no promise about durability. It is pure: the
same tables and the same policy give the same plan, which is also what lets a
caller recompute a plan on every change instead of maintaining one incrementally
and having to prove the incremental update agrees with the recomputation.

Merging does touch files, and what it promises is deliberately narrow. It reads
each source through a handle of its own and streams the merged records into a
temporary file that :meth:`~ledgerlog.sstable.SSTableWriter.finish` renames into
place, so the destination path only ever names a complete table. A merge that
raises partway through leaves its sources exactly as they were and nothing at the
destination, since the temporary file is removed on the way out; a process killed
mid-merge leaves that temporary file behind, under a name no reader looks for a
table at. What merging never does is open a source for writing, unlink one, or
hand the engine the result: choosing when the sources may go is M8.5's decision,
and it needs the merged table to be on disk and valid first, which is what this
returns.
"""

from __future__ import annotations

import heapq
import math
import os
from collections.abc import Iterable, Iterator, Sequence
from contextlib import ExitStack
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generic, Protocol, TypeVar, runtime_checkable

from ledgerlog.sstable import (
    DEFAULT_BLOOM_FALSE_POSITIVE_RATE,
    DEFAULT_INDEX_INTERVAL,
    RECORD_LENGTH_SIZE,
    RECORD_PAYLOAD_HEADER_SIZE,
    SSTABLE_FORMAT_VERSION,
    SSTableFooter,
    SSTableLayout,
    SSTableRecord,
    SSTableUnsupportedVersionError,
    SSTableWriter,
    iter_records,
    read_file_header,
    read_footer,
)

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
    cares only about sizes, but a tier is handed to :func:`merge_tier` to merge
    with newest write wins semantics, so the tables in it have to come back in a
    known order.
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
    uses and the order :func:`merge_tier` reads them in: a key present in several
    of these tables takes its value from the first one that holds it, so the
    ordering is what newest write wins means once the tier is being merged.

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


@runtime_checkable
class MergeableTable(SizedTable, Protocol):
    """A table a merge can read: one that knows its age, its size and its path.

    :class:`SizedTable` plus the one thing merging needs and planning does not.
    Kept separate rather than folded into ``SizedTable`` so that the grouping
    rules stay testable against a two-field stand-in with no file behind it:
    demanding a path to be tiered would mean every grouping test had to write a
    table first. :class:`~ledgerlog.engine.SSTableHandle` satisfies both.
    """

    @property
    def path(self) -> Path:
        """Where the table's bytes are."""


MergeableTableT = TypeVar("MergeableTableT", bound=MergeableTable)


class MergeSourceOrderError(ValueError):
    """Raised when a merge source's records do not ascend strictly by key.

    A merge reads its sources as sorted runs and holds only one record from each,
    which is what keeps its memory flat in the size of the tables rather than
    their bytes. A source that is not sorted breaks the assumption that makes that
    work: the record in hand is no longer the smallest the source has left, so a
    key already emitted could turn up again afterwards and the output would be
    neither sorted nor newest wins.

    Raised rather than worked around by sorting the source in memory. Every table
    this engine writes is sorted, since
    :meth:`~ledgerlog.sstable.SSTableWriter.add` refuses a key that does not
    ascend, so a source that is not sorted is damaged or was not written by this
    engine, and re-sorting damaged records would mean guessing which of two values
    for one key the table meant to be the newer.
    """


def merge_records(
    streams: Sequence[Iterable[SSTableRecord]],
    *,
    source_names: Sequence[str] | None = None,
) -> Iterator[tuple[bytes, bytes | None]]:
    """Merge sorted record streams, newest first, into one sorted run of records.

    ``streams`` holds one stream per source table, newest table first, each
    yielding that table's records in ascending key order.
    :attr:`CompactionTier.tables` is in exactly that order, which is why a tier
    can be merged by reading its tables in the order it lists them.

    Yields ``(key, value)`` pairs in ascending key order, one pair per distinct
    key, taking the value from the earliest stream that holds that key. That is
    what newest write wins means once a tier is being merged: a key appears in
    several tables because it was written several times, and only the newest of
    those writes is live. Every older copy is dropped here, and dropping them is
    the space compaction reclaims.

    A tombstone (a value of ``None``) wins its key like any other record and is
    yielded like any other record. Dropping it would be a decision about tables
    outside this merge, which this function cannot see and story M8.3 is where it
    is made: an older table beyond the tier can still hold a value for that key,
    and a merge that dropped the tombstone would let that value resurrect a
    deleted key (ARCHITECTURE.md section 4).

    Pairs rather than :class:`~ledgerlog.sstable.SSTableRecord` objects, because
    the offsets a record carries describe the file it was read from and mean
    nothing in the merged table, and because pairs are what
    :func:`~ledgerlog.sstable.write_sstable` and
    :meth:`~ledgerlog.sstable.SSTableWriter.add` take.

    Streaming, not loading. At most one record per stream is held at a time, so a
    merge of ``k`` tables costs memory in ``k`` and not in the tables' size, which
    is the point: compaction exists for data that does not fit in memory, and a
    merge that first read a whole tier would be a merge that could not run when it
    was most needed.

    ``source_names`` is used only to name a source in a
    :exc:`MergeSourceOrderError`, so that a damaged table can be identified from
    the error instead of from the position of a stream in a list.
    """
    if source_names is not None and len(source_names) != len(streams):
        raise ValueError(
            f"got {len(source_names)} source names for {len(streams)} streams, so a name "
            "cannot be matched to the stream it describes"
        )
    return _merge_records(
        tuple(streams),
        tuple(source_names) if source_names is not None else None,
    )


def _merge_records(
    streams: tuple[Iterable[SSTableRecord], ...],
    source_names: tuple[str, ...] | None,
) -> Iterator[tuple[bytes, bytes | None]]:
    """Generator half of :func:`merge_records`, kept separate so its checks run eagerly."""
    cursors = [iter(stream) for stream in streams]
    # A heap of (key, rank), where rank is the stream's index and therefore its
    # age: the smallest key pops first, and among equal keys the smallest rank,
    # which is the newest table holding that key. So the first pop for a key is
    # always the winner and no comparison of values is needed to find it.
    #
    # The records themselves stay in ``pending`` rather than on the heap so that
    # two heap entries are never compared by anything beyond the rank. Ranks are
    # unique, so the comparison always stops there, and a record type that cannot
    # be ordered (SSTableRecord is frozen, not ordered) never has to be.
    heap: list[tuple[bytes, int]] = []
    pending: dict[int, SSTableRecord] = {}
    last_keys: list[bytes | None] = [None] * len(cursors)

    def advance(rank: int) -> None:
        """Pull the next record from one stream onto the heap, if it has one."""
        record = next(cursors[rank], None)
        if record is None:
            return
        previous = last_keys[rank]
        if previous is not None and record.key <= previous:
            name = source_names[rank] if source_names is not None else f"source at index {rank}"
            raise MergeSourceOrderError(
                f"{name} yields key {record.key!r} after {previous!r}, so its records are not "
                "in strictly ascending key order and cannot be merged as a sorted run"
            )
        last_keys[rank] = record.key
        pending[rank] = record
        heapq.heappush(heap, (record.key, rank))

    for rank in range(len(cursors)):
        advance(rank)

    while heap:
        key, rank = heapq.heappop(heap)
        winner = pending.pop(rank)
        advance(rank)
        # Every other stream sitting on this key holds an older write of it, so
        # each is stepped past without being looked at. They are dropped here
        # rather than by the writer refusing a repeated key, because the writer
        # would be right to refuse it and the merge would be the thing at fault.
        while heap and heap[0][0] == key:
            _, shadowed_rank = heapq.heappop(heap)
            pending.pop(shadowed_rank)
            advance(shadowed_rank)
        yield winner.key, winner.value


def merge_sstables(
    sources: Sequence[str | os.PathLike[str]],
    destination: str | os.PathLike[str],
    *,
    index_interval: int = DEFAULT_INDEX_INTERVAL,
    bloom_false_positive_rate: float = DEFAULT_BLOOM_FALSE_POSITIVE_RATE,
) -> SSTableLayout:
    """Merge the SSTables at ``sources`` into one new SSTable at ``destination``.

    ``sources`` is newest table first, the ordering :func:`merge_records` resolves
    a repeated key by. The result is a complete SSTable in its own right (data
    block, sparse index, bloom filter, footer), holding one record per distinct
    key across the sources, so it can be read by
    :class:`~ledgerlog.sstable.SSTableReader` and tiered by
    :func:`plan_compaction` like any table a flush produced. Returns its
    :class:`~ledgerlog.sstable.SSTableLayout`.

    Each source gets its own file handle, for two reasons. A record iterator moves
    the one cursor its handle has, so two streams over one handle would pull each
    other's position and the merge would read nonsense; and an SSTable is
    immutable, so reading one through a private handle cannot disturb a reader
    serving :meth:`~ledgerlog.engine.LedgerLog.get` from the same file at the same
    time. Every handle is opened into an :class:`~contextlib.ExitStack` and so is
    closed on the way out whether the merge finished, raised or was abandoned:
    a merge of a large tier that leaked a descriptor per source would exhaust the
    process's supply within a few passes.

    Each source is validated before a record is read from it, through the same
    path :class:`~ledgerlog.sstable.SSTableReader` uses: the footer is located
    from the file's real size and every offset in it is measured against that size
    (:func:`~ledgerlog.sstable.read_footer`), the footer's format version is
    checked, and the file header is checked. So a truncated or corrupted source
    stops the merge with the reason, rather than the merge sizing a read from a
    length nobody wrote or decoding the index as records. The bounds on each
    record are :func:`~ledgerlog.sstable.iter_records`' to enforce, for the same
    reason.

    ``destination`` must not exist and must not be one of the sources. The write
    ends in a rename over that path, which would replace an existing table rather
    than fail, and a merge that replaced a live table would destroy the very data
    the atomic swap in M8.5 exists to protect. This is a check against a caller
    passing the wrong name, not a lock: nothing stops another process creating the
    file in between, which is why the engine, and not this function, is what
    allocates table names.

    ``index_interval`` and ``bloom_false_positive_rate`` shape the output exactly
    as they do for a flush, and default to the same values, so a merged table is
    not a differently tuned kind of table. The bloom filter is sized from the sum
    of the sources' key counts (see :func:`_plausible_record_count`), which is an
    upper bound on the number of distinct keys the output can hold: it is reached
    when the sources share no keys, and an over-estimate costs a slightly larger
    filter and a slightly lower false-positive rate than the target, never a false
    negative.
    """
    if not sources:
        raise ValueError("a merge needs at least one source table")

    source_paths = [Path(source) for source in sources]
    destination_path = Path(destination)
    seen: dict[Path, Path] = {}
    for source_path in source_paths:
        resolved = source_path.resolve()
        if resolved in seen:
            raise ValueError(
                f"source table {source_path} is listed twice (also as {seen[resolved]}), so "
                "which of the two copies holds the newer write of a key is undefined"
            )
        seen[resolved] = source_path
    if destination_path.resolve() in seen:
        raise ValueError(
            f"destination {destination_path} is also a source of the merge, and finishing the "
            "merge would replace a table it is still reading"
        )
    if destination_path.exists():
        raise ValueError(
            f"destination {destination_path} already exists, and a merge finishes by renaming "
            "over its destination, which would discard whatever is there"
        )

    with ExitStack() as stack:
        streams: list[Iterator[SSTableRecord]] = []
        expected_keys = 0
        for source_path in source_paths:
            handle = stack.enter_context(open(source_path, "rb"))
            file_size = handle.seek(0, os.SEEK_END)
            footer = read_footer(handle, file_size=file_size)
            if footer.format_version != SSTABLE_FORMAT_VERSION:
                raise SSTableUnsupportedVersionError(footer.format_version)
            handle.seek(0)
            read_file_header(handle)
            expected_keys += _plausible_record_count(footer)
            streams.append(
                iter_records(
                    handle,
                    start_offset=footer.data_block_offset,
                    end_offset=footer.data_block_end,
                )
            )

        merged = merge_records(streams, source_names=[str(path) for path in source_paths])
        with SSTableWriter(
            destination_path,
            index_interval=index_interval,
            expected_keys=expected_keys,
            bloom_false_positive_rate=bloom_false_positive_rate,
        ) as writer:
            for key, value in merged:
                writer.add(key, value)
            return writer.finish()


def _plausible_record_count(footer: SSTableFooter) -> int:
    """Return a source's record count, bounded by what its data block could hold.

    The count in a footer is a number read off a disk, and the only thing checked
    about it there is that it is not negative. It is used here to size the output's
    bloom filter, and sizing an allocation from an unbounded number off a damaged
    file is how a corrupted footer turns a merge into a request for a gigabyte of
    memory (or a refusal from the sizing formula) before a single record has been
    read.

    The data block's own extent is an independent bound: a record is at least a
    length prefix plus a kind and a key length, so a block cannot hold more
    records than its bytes divided by that minimum. For a table this engine wrote
    the count is far below that bound and this changes nothing, which is the point.
    A count above it is a footer disagreeing with its own file, and the merge does
    not need to decide which half is wrong: the bloom filter's size is a
    performance hint, so the safe bound is enough here, and a real disagreement
    surfaces as a record error when the block is actually read.
    """
    smallest_record = RECORD_LENGTH_SIZE + RECORD_PAYLOAD_HEADER_SIZE
    capacity = (footer.data_block_end - footer.data_block_offset) // smallest_record
    return min(footer.record_count, capacity)


def merge_tier(
    tier: CompactionTier[MergeableTableT],
    destination: str | os.PathLike[str],
    *,
    index_interval: int = DEFAULT_INDEX_INTERVAL,
    bloom_false_positive_rate: float = DEFAULT_BLOOM_FALSE_POSITIVE_RATE,
) -> SSTableLayout:
    """Merge one tier's tables into a single new SSTable at ``destination``.

    The tier a caller reaches for is :attr:`CompactionPlan.next_tier`, and this is
    the whole of what happens to it here: its tables are read in the order the
    tier lists them, which is newest first, so the resulting table holds the
    newest write of every key the tier held. The tier's files are left in place.

    Nothing checks that the tier is :attr:`~CompactionTier.ready`. Readiness says
    that merging the tier is worth the bytes it costs, which is a question for
    whoever decides to compact, and a merge of a tier below the threshold is
    correct, just not usually worth doing.
    """
    return merge_sstables(
        [table.path for table in tier.tables],
        destination,
        index_interval=index_interval,
        bloom_false_positive_rate=bloom_false_positive_rate,
    )
