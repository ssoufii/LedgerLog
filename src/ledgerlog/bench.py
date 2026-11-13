"""Benchmark harness: what this engine actually does, measured rather than claimed.

Scope of this module today (stories M10.1, M10.2 and M10.4): a sequential write
phase that reports throughput, a randomized read phase that reports a latency
distribution, an optional read-amplification phase that reports how many
SSTables a ``get`` searches before and after a compaction, and the CLI arguments
that make the record count, the value size, the fsync policy and the four
tunables M10.4 chooses part of the measurement instead of baked into it. Run it
with::

    python -m ledgerlog.bench --records 100000
    python -m ledgerlog.bench --records 100000 --read-amplification \\
        --memtable-threshold-bytes 65536
    python -m ledgerlog.bench --records 100000 --read-amplification \\
        --memtable-threshold-bytes 65536 --index-interval 128 --bloom-fp-rate 0.01

Why the sparse index interval, the bloom target and the two size-tier knobs are
command line arguments rather than constants the engine reads at the point it
uses them: story M10.4 picks their defaults from measurements, and a sweep that
had to monkey-patch another module's globals to vary them would be measuring a
configuration no caller of this library could ask for. They reach the engine the
way any caller's would, through its constructor, and the merged tables the
amplification phase writes get the same two table settings the run's flushes
did, so the two sides of that comparison differ by the merge alone.

Read amplification is measured from inside the read path, through
:class:`~ledgerlog.engine.ReadProbe`, and not inferred from how many tables the
engine holds. The two are different numbers and only the first is the one the
phrase means: a read stops at the first layer holding its key and never opens a
table whose bloom filter rejects it, so an engine serving forty tables may
search one. A figure taken from the table count would describe the directory and
would be exactly the kind of estimate this module exists to replace. The table
count is still printed next to the read phase, as context for the latency
figure.

Why the amplification phase runs against copies of the run's tables in a
directory of its own, rather than against the engine the other two phases used:
the before and after measurements have to differ by the compaction and by
nothing else. The live engine still holds the records written since its last
freeze in its active memtable, and a read that memory answers searches no table
at all, so measuring the "before" side there would mix a memtable hit rate into
the figure and quietly deflate it. Both sides instead run against an engine
opened on tables alone, with no log to replay and therefore an empty memtable,
so every read in both reaches disk and the only thing that changed between them
is that a compaction ran. The copies are hard links where the filesystem allows
them, so the snapshot costs no space and no copy time, and the compaction
unlinks its sources without touching the run's own tables.

Why the amplification phase is opt-in rather than always on: it reads the whole
dataset back twice more and rewrites it once, which is a large share of the
run's cost for a figure a run measuring write throughput has not asked for. It
also says nothing at a memtable threshold that produced one table, which is what
the default threshold does at a modest record count, so turning it on is a
decision that comes with choosing a threshold low enough to make several.

Why throughput and latency are reported differently, and not as two views of one
number: they answer different questions and have different right answers. A
sequential write is interesting in bulk, because a caller writing a million
records cares how long the million takes and not how long any one of them took,
so the write phase reports records per second over the whole phase. A read is
interesting at the tail, because a caller serving requests cares about the slow
ones, and a mean read latency hides precisely the reads worth knowing about
behind the many that hit the memtable. So the read phase keeps a sample per
operation and reports percentiles over them.

Why percentiles are nearest-rank rather than interpolated: an interpolated p99
is a number no read ever took, computed between two that did. A nearest-rank p99
is a latency this run actually measured, which is the thing a performance claim
should be able to point at. The cost is that the figure moves in steps when the
sample is small, which is honest about the resolution the sample has.

Why the value payload is built once, before the write phase, and the key is
formatted inside it: the alternative to each is worse. Building a fresh value per
record would put the harness's own byte-building in the timed region and charge
the engine for it, and the content of a value changes no work the engine does, so
one buffer reused for every record measures the same engine and less of Python.
Keys cannot be shared, since writing one key N times would measure overwriting a
single record rather than writing N of them, so a key is formatted per record and
that fixed, tiny cost is inside the timed region and reported as part of it. The
alternative, materializing every key in a list first, would make the harness's
memory rather than the engine's the thing that caps ``--records``.

Why the write phase drains pending flushes before its timer stops, unless told
not to: a read phase run while the background flusher is still working measures
a tree that is changing underneath it, so the same command would report a
different latency depending on how the thread happened to be scheduled. Draining
first makes the run repeatable. The drain is charged to the write phase, and its
share is reported on its own line, because the flushes it waits for are work the
writes caused: stopping the clock before them would make the throughput depend
on whether the last flush happened to finish inside the loop or just after it.
Only frozen memtables are drained. The active memtable keeps whatever was written
after the last freeze, as it would in any running engine, so the report prints
how many bytes the read phase found there next to the table count.
``--no-drain-flushes`` keeps the other case available, since reading against an
in-flight flush is a real workload and worth being able to measure.

Why a named ``--directory`` must be empty: the engine adopts whatever log and
tables it finds there, so a second run in the same directory overwrites the
first run's keys and reads across its tables too. That run measures a different
workload while printing the same invocation, which is exactly the untraceable
figure story M10.3 rules out. Refusing is cheaper than explaining.

Why the storage under the run directory is reported: ``fsync`` on a RAM-backed
filesystem (tmpfs, which is where ``/tmp`` lives on many Linux systems) returns
at once, so an fsync-always run there measures memory while calling itself
durable. The harness cannot pick a better disk for the caller, but it can say
which filesystem it ran on and warn when that filesystem cannot honor an fsync.

Why keys are derived from their index by a pure function rather than drawn at
random: the read phase has to ask for keys the write phase wrote, and a formula
lets it do that from an index alone, with no list of keys to carry and no chance
of asking for a key that was never written. Randomness enters only in which
indices the read phase visits, seeded from ``--seed`` so a run can be repeated.

This module writes no file format of its own and reads none, so it carries no
format version: it is a caller of the public engine API and nothing else.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import shutil
import statistics
import sys
import tempfile
import time
from array import array
from collections import Counter
from collections.abc import Sequence
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path

from ledgerlog.compaction import (
    DEFAULT_MIN_TIER_TABLES,
    DEFAULT_SIZE_RATIO,
    CompactionPolicy,
    compact_tier,
    plan_compaction,
)
from ledgerlog.engine import (
    DEFAULT_MEMTABLE_THRESHOLD_BYTES,
    LedgerLog,
    ReadProbe,
    discover_sstables,
    sstable_filename,
)
from ledgerlog.sstable import (
    DEFAULT_BLOOM_FALSE_POSITIVE_RATE,
    DEFAULT_INDEX_INTERVAL,
    MAX_RECORD_PAYLOAD_SIZE,
    RECORD_PAYLOAD_HEADER_SIZE,
    validate_bloom_false_positive_rate,
    validate_index_interval,
)
from ledgerlog.wal import (
    DEFAULT_FSYNC_INTERVAL_SECONDS,
    MAX_PAYLOAD_SIZE,
    PAYLOAD_HEADER_SIZE,
    FsyncPolicy,
)

KEY_PREFIX = b"bench/key/"
"""Namespace every benchmark key sits under.

A prefix plus a zero-padded index, rather than a bare index, so that keys sort in
the same order as the indices they came from. Sequential writes to an LSM engine
are the case the write phase means to measure, and an unpadded decimal index
would have key 10 sort before key 9, which is a different workload (scattered
inserts into the memtable) wearing the word "sequential".
"""

KEY_INDEX_DIGITS = 16
"""Width the index is padded to, fixed so that every key is the same length.

Sixteen digits covers any record count this harness can be asked for and keeps
the padding from being the thing that has to change when it is asked for more.
"""

MAX_RECORDS = 10**KEY_INDEX_DIGITS - 1
"""Largest record count whose index still fits in :data:`KEY_INDEX_DIGITS` digits.

Past this the padding stops padding and keys stop being one width, which quietly
breaks both the key ordering the write phase relies on and the payload
arithmetic. Far beyond anything runnable, and checked anyway, because the
failure it prevents is a wrong number rather than an error.
"""

DEFAULT_VALUE_SIZE_BYTES = 100
"""Value size used when none is given, in bytes.

A hundred bytes is small enough that the engine's own per-record overhead (the
framing, the checksum, the index entry) is visible in the result rather than lost
behind the payload, which is the regime where this engine's design choices show
up at all.
"""

KEY_LENGTH = len(KEY_PREFIX) + KEY_INDEX_DIGITS
"""Length in bytes of every benchmark key, which is fixed by construction."""

MAX_VALUE_SIZE_BYTES = (
    min(
        MAX_PAYLOAD_SIZE - PAYLOAD_HEADER_SIZE,
        MAX_RECORD_PAYLOAD_SIZE - RECORD_PAYLOAD_HEADER_SIZE,
    )
    - KEY_LENGTH
)
"""Largest value a benchmark record can carry and still fit both record formats.

Derived from the WAL and SSTable payload limits rather than restated, so the
bench refuses an oversized value up front with one line instead of failing on
the first ``put`` with a traceback after it has already created a directory.
"""

DEFAULT_MAX_READS = 1_000_000
"""Ceiling on the default read count when ``--reads`` is not given.

The read phase keeps one latency sample per read and sorts them once to take
percentiles, so its memory grows with the read count. A million samples is
enough to resolve p99.9 to a thousand observations and costs tens of megabytes;
tying the default to ``--records`` without a ceiling would make the harness's
own memory, not the engine, the thing that limits a large run. An explicit
``--reads`` is honored as given.
"""

RAM_BACKED_FILESYSTEMS = frozenset({"tmpfs", "ramfs"})
"""Filesystem types on which ``fsync`` does not reach durable storage."""

AMPLIFICATION_DIRNAME = "read-amplification"
"""Subdirectory of the run directory the read-amplification phase works in.

A subdirectory rather than a temporary directory elsewhere, for two reasons. The
snapshot it holds is made of hard links into the run directory, which only works
within one filesystem, and the engine it opens there should be reading the same
disk the rest of the run measured. It is not a name
:func:`~ledgerlog.engine.sstable_filename` could produce and it is a directory
rather than a file, so the run's own engine passes over it.
"""

INCREMENTAL_DIRNAME = "read-amplification-incremental"
"""Subdirectory the incremental compaction schedule rebuilds the dataset in.

A second directory rather than the snapshot itself, because the incremental
schedule consumes its tables as it goes and the snapshot has to survive the
measurement taken against it. Both hold hard links into the run directory where
the filesystem allows them, so the second copy costs no bytes either.
"""

COMPACTION_SCHEDULE_BULK = "bulk"
"""Compact once, at the end, over every table the run flushed.

The cheapest schedule and the one that answers story M10.2's question: it is the
floor on tables searched per read, since every tier that can merge does, and it
is what the harness did before the schedules were named.
"""

COMPACTION_SCHEDULE_INCREMENTAL = "incremental"
"""Add the run's tables one flush at a time, merging whenever a tier fills.

The schedule a running engine would follow, and the only one under which the
size-tier knobs change anything. Compacting a finished directory in bulk puts
every equally sized flush in one tier and merges the lot in a single pass, so
:attr:`~ledgerlog.compaction.CompactionPolicy.min_tier_tables` and
:attr:`~ledgerlog.compaction.CompactionPolicy.size_ratio` land on the same
answer whatever they are set to. Rebuilding the directory flush by flush is what
produces tables of several sizes, which is the situation the two knobs exist to
describe, so story M10.4 measures them here.

It costs what it measures: the merges a real engine would have run spread over
the run's lifetime all happen inside this phase, so a run asking for it pays for
every one of them.
"""

COMPACTION_SCHEDULES = (COMPACTION_SCHEDULE_BULK, COMPACTION_SCHEDULE_INCREMENTAL)
"""Every schedule ``--compaction-schedule`` accepts."""

MAX_HISTOGRAM_ROWS = 10
"""How many distinct per-read table counts the text report lists in full.

The histogram has one row per distinct number of tables a read searched, which
is bounded by the table count and so is short after a compaction and can be long
before one. The remainder is summarized in one line rather than dropped, and
``--json`` carries every row regardless, so the cap shortens the report without
hiding anything.
"""

PERCENTILES: tuple[float, ...] = (50.0, 90.0, 99.0, 99.9)
"""Percentiles the read phase reports.

p50 and p99 are what the story asks for. p90 sits between them so that a run
where the two are far apart says where the distribution turns, and p99.9 is there
because an engine that fsyncs on every write has a tail that only shows up past
p99.
"""


def key_for_index(index: int) -> bytes:
    """Return the benchmark key for record ``index``.

    Pure and total, so that the read phase can reconstruct any key the write
    phase wrote from its index alone. See this module's docstring for why that
    matters more than it looks: it is what lets the read phase hold no key list
    and still be certain it never asks for a key that was never written.
    """
    return KEY_PREFIX + str(index).rjust(KEY_INDEX_DIGITS, "0").encode("ascii")


def draw_read_indices(records: int, read_count: int, seed: int) -> array[int]:
    """Return the record indices the read phase will visit, in visit order.

    Drawn up front so that the read phase's timed region
    contains no random number generation, and so the draw itself is testable:
    "the same seed replays the same read pattern" is what makes two runs
    comparable, and that claim is only worth something if something checks it.

    With replacement rather than as a shuffled permutation, because a workload
    that touches each key exactly once is a scan wearing the word "random", and
    because a permutation would cap the read count at the record count, which
    ``--reads`` exists to lift.

    ``random.Random`` is instantiated here rather than the module-level generator
    being used, so that a run's read pattern depends only on ``seed`` and not on
    whatever else in the process has drawn from the global one.

    Returned as a packed ``array`` of 64 bit integers rather than a list, which
    holds the same indices in a fraction of the memory: a list of Python ints
    costs several times as much per entry, and at a large read count that
    difference is the harness, not the engine, running out of room.
    """
    rng = random.Random(seed)
    return array("q", (rng.randrange(records) for _ in range(read_count)))


def filesystem_type(path: Path, mounts_text: str | None = None) -> str | None:
    """Return the type of the filesystem ``path`` lives on, or ``None`` if unknown.

    Read from ``/proc/mounts``, choosing the longest mount point that contains
    ``path``, because that is the mount the kernel resolves the path through.
    Linux only: elsewhere there is no portable way to ask without a dependency,
    and ``None`` (reported as "unknown") is the honest answer. ``mounts_text``
    lets a test supply a mount table instead of the machine's. The path is taken
    as given rather than resolved, so a caller should pass a resolved path when
    symlinks could lead to a different mount.
    """
    if mounts_text is None:
        try:
            mounts_text = Path("/proc/mounts").read_text(encoding="utf-8", errors="replace")
        except OSError:
            return None

    resolved = path.absolute()
    best_length = -1
    best_type: str | None = None
    for line in mounts_text.splitlines():
        fields = line.split()
        if len(fields) < 3:
            continue
        # /proc/mounts escapes spaces in mount points as \040.
        mount_point = Path(fields[1].replace("\\040", " "))
        if resolved != mount_point and mount_point not in resolved.parents:
            continue
        length = len(mount_point.parts)
        if length > best_length:
            best_length = length
            best_type = fields[2]
    return best_type


def nearest_rank_percentile(sorted_samples: Sequence[int], percentile: float) -> int:
    """Return the nearest-rank ``percentile`` of ``sorted_samples``, which must be sorted.

    Nearest-rank means the result is always one of the samples, never a value
    computed between two of them. See this module's docstring for why a reported
    tail latency should be a latency that actually happened.

    The rank is computed with integer arithmetic (ceil of a fraction, via
    ``-(-a // b)``) rather than by rounding a float, because a float rank can land
    on the wrong side of an exact boundary: at p50 of two samples, ``0.5`` rounding
    either way is a different answer, and which way it goes should be a decision
    here rather than a property of binary floating point.
    """
    if not sorted_samples:
        raise ValueError("cannot take a percentile of an empty sample")
    if not 0.0 <= percentile <= 100.0:
        raise ValueError(f"percentile must be between 0 and 100, got {percentile!r}")

    count = len(sorted_samples)
    # Scaled to a thousandth of a percent so that p99.9 is exact in integers,
    # which it would not be if the percentage went through a float multiply.
    scaled = int(round(percentile * 1000))
    rank = -(-scaled * count // 100_000)
    index = min(max(rank, 1), count) - 1
    return sorted_samples[index]


@dataclass(frozen=True)
class BenchConfig:
    """Everything that decides what a run measures, in one value.

    Frozen and separate from the argument parser so that a test can build a
    configuration directly instead of going through a command line, and so that
    the report can print the configuration it ran under verbatim. The second
    reason is the one story M10.3 rests on: a number is only traceable to a bench
    invocation if the invocation is printed next to it.
    """

    records: int
    value_size_bytes: int = DEFAULT_VALUE_SIZE_BYTES
    reads: int | None = None
    fsync_policy: FsyncPolicy = FsyncPolicy.ALWAYS
    fsync_interval_seconds: float = DEFAULT_FSYNC_INTERVAL_SECONDS
    memtable_threshold_bytes: int = DEFAULT_MEMTABLE_THRESHOLD_BYTES
    index_interval: int = DEFAULT_INDEX_INTERVAL
    bloom_false_positive_rate: float = DEFAULT_BLOOM_FALSE_POSITIVE_RATE
    size_ratio: float = DEFAULT_SIZE_RATIO
    min_tier_tables: int = DEFAULT_MIN_TIER_TABLES
    compaction_schedule: str = COMPACTION_SCHEDULE_BULK
    seed: int = 0
    directory: Path | None = None
    drain_flushes: bool = True
    read_amplification: bool = False

    def __post_init__(self) -> None:
        """Reject a configuration that cannot produce a meaningful measurement.

        Checked here rather than only in the parser so that the guarantee holds
        for a caller who builds a configuration in code, and so the parser has
        one place to delegate to rather than its own copy of the rules.
        """
        if self.records < 1:
            raise ValueError(f"records must be at least 1, got {self.records}")
        if self.records > MAX_RECORDS:
            # Not a real workload limit, a limit on the key padding: past this
            # the index stops fitting in KEY_INDEX_DIGITS, keys stop being one
            # width, and both the sequential ordering the write phase means to
            # measure and the payload arithmetic in _run_write_phase stop
            # holding. Refused rather than silently measuring something else.
            raise ValueError(
                f"records must be at most {MAX_RECORDS} so that every key is the same width,"
                f" got {self.records}"
            )
        if self.value_size_bytes < 0:
            raise ValueError(f"value_size_bytes cannot be negative, got {self.value_size_bytes}")
        if self.value_size_bytes > MAX_VALUE_SIZE_BYTES:
            raise ValueError(
                f"value_size_bytes must be at most {MAX_VALUE_SIZE_BYTES} to fit the record"
                f" formats, got {self.value_size_bytes}"
            )
        # Checked for every policy, not only interval, because the WAL checks it
        # for every policy too: an unchecked value here would pass this method
        # and then fail inside the engine with a traceback.
        interval = self.fsync_interval_seconds
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError(
                f"fsync_interval_seconds must be a finite positive number, got {interval!r}"
            )
        if self.reads is not None and self.reads < 1:
            raise ValueError(f"reads must be at least 1 when given, got {self.reads}")
        if self.memtable_threshold_bytes < 1:
            raise ValueError(
                f"memtable_threshold_bytes must be at least 1, got {self.memtable_threshold_bytes}"
            )
        # Delegated to the engine's own validators rather than restated, so a
        # value this harness accepts is exactly a value an engine accepts and a
        # sweep cannot report a figure for a setting no caller could have used.
        # The compaction knobs are checked by building the policy the run would
        # have used, which is where their rules live.
        validate_index_interval(self.index_interval)
        validate_bloom_false_positive_rate(self.bloom_false_positive_rate)
        self.compaction_policy()
        if self.compaction_schedule not in COMPACTION_SCHEDULES:
            raise ValueError(
                f"compaction_schedule must be one of {', '.join(COMPACTION_SCHEDULES)},"
                f" got {self.compaction_schedule!r}"
            )

    def compaction_policy(self) -> CompactionPolicy:
        """Return the size-tier policy this run compacts under.

        Built on demand rather than stored on the configuration, because
        :class:`~ledgerlog.compaction.CompactionPolicy` validates in its own
        constructor and a configuration holding a pre-built one would have to
        either duplicate those rules or accept a policy that disagreed with the
        two numbers printed in the invocation.
        """
        return CompactionPolicy(size_ratio=self.size_ratio, min_tier_tables=self.min_tier_tables)

    @property
    def read_count(self) -> int:
        """How many reads the read phase performs, defaulting to one per record written.

        One per record by default because that is the sample size the write phase
        already paid for, and a default that scaled differently from ``--records``
        would make two runs at different record counts report tails measured from
        different numbers of samples. Capped at :data:`DEFAULT_MAX_READS` so that
        the sample list cannot outgrow memory on a very large run; the printed
        invocation always states the read count, so the cap never hides itself.
        """
        if self.reads is None:
            return min(self.records, DEFAULT_MAX_READS)
        return self.reads

    def to_dict(self) -> dict[str, object]:
        """Return the configuration as plain JSON-safe values."""
        return {
            "records": self.records,
            "value_size_bytes": self.value_size_bytes,
            "reads": self.read_count,
            "fsync_policy": self.fsync_policy.value,
            "fsync_interval_seconds": self.fsync_interval_seconds,
            "memtable_threshold_bytes": self.memtable_threshold_bytes,
            "index_interval": self.index_interval,
            "bloom_false_positive_rate": self.bloom_false_positive_rate,
            "size_ratio": self.size_ratio,
            "min_tier_tables": self.min_tier_tables,
            "compaction_schedule": self.compaction_schedule,
            "seed": self.seed,
            "drain_flushes": self.drain_flushes,
            "read_amplification": self.read_amplification,
        }

    def command_line(self) -> str:
        """Return the ``python -m ledgerlog.bench`` command that reproduces this run.

        Printed with the results so that a figure quoted anywhere else can be
        traced back to the invocation that produced it, which is what story M10.3
        needs from this harness. Only the settings that change what is measured
        are included: the output directory is not one of them, since a run in a
        different, empty directory on the same disk measures the same thing. The
        directory must be empty for that to hold, which :func:`run_bench`
        enforces, and the filesystem it sits on is printed in the report.

        The four tuning knobs are printed even when they hold the module
        defaults, unlike ``--fsync-interval``, which is printed only when the
        policy reads it. They are the defaults story M10.4 changes, so a figure
        that named them only when they were overridden would stop describing its
        own run the moment a later commit moved a default, and the matrix in
        docs/TUNING.md is exactly a set of runs that differ in nothing else.
        """
        parts = [
            "python -m ledgerlog.bench",
            f"--records {self.records}",
            f"--value-size {self.value_size_bytes}",
            f"--reads {self.read_count}",
            f"--fsync-policy {self.fsync_policy.value}",
            f"--memtable-threshold-bytes {self.memtable_threshold_bytes}",
            f"--index-interval {self.index_interval}",
            f"--bloom-fp-rate {self.bloom_false_positive_rate}",
            f"--size-ratio {self.size_ratio}",
            f"--min-tier-tables {self.min_tier_tables}",
            f"--compaction-schedule {self.compaction_schedule}",
            f"--seed {self.seed}",
        ]
        if self.fsync_policy is FsyncPolicy.INTERVAL:
            parts.append(f"--fsync-interval {self.fsync_interval_seconds}")
        if not self.drain_flushes:
            parts.append("--no-drain-flushes")
        if self.read_amplification:
            parts.append("--read-amplification")
        return " ".join(parts)


@dataclass(frozen=True)
class LatencySummary:
    """A latency distribution reduced to the figures worth printing, in nanoseconds.

    Nanosecond integers throughout rather than seconds as floats, because the
    samples come from :func:`time.perf_counter_ns` as integers and converting
    each one on the way in would round away resolution before the percentiles are
    taken. The conversion to milliseconds happens once, in the formatting.
    """

    count: int
    min_ns: int
    mean_ns: float
    max_ns: int
    percentiles_ns: dict[str, int]

    @classmethod
    def from_samples(cls, samples: Sequence[int]) -> LatencySummary:
        """Summarize ``samples``, which need not be sorted.

        Sorts a copy rather than the caller's sequence: a summary that reordered
        its input would be a surprising thing for a measurement function to do,
        and the read phase's sample list is worth keeping in arrival order for
        anyone who wants to look at it.
        """
        if not samples:
            raise ValueError("cannot summarize an empty latency sample")

        ordered = sorted(samples)
        return cls(
            count=len(ordered),
            min_ns=ordered[0],
            mean_ns=statistics.fmean(ordered),
            max_ns=ordered[-1],
            percentiles_ns={
                _percentile_label(percentile): nearest_rank_percentile(ordered, percentile)
                for percentile in PERCENTILES
            },
        )

    def to_dict(self) -> dict[str, object]:
        """Return the summary as plain JSON-safe values."""
        return {
            "count": self.count,
            "min_ns": self.min_ns,
            "mean_ns": self.mean_ns,
            "max_ns": self.max_ns,
            "percentiles_ns": dict(self.percentiles_ns),
        }


def _percentile_label(percentile: float) -> str:
    """Return the short name a percentile is reported under, for example ``p99.9``.

    Trailing ``.0`` is dropped so that the common percentiles read as ``p50``
    rather than ``p50.0``, which is how anyone quoting them writes them.
    """
    if percentile == int(percentile):
        return f"p{int(percentile)}"
    return f"p{percentile}"


@dataclass(frozen=True)
class WritePhaseResult:
    """What the sequential write phase measured."""

    records: int
    value_size_bytes: int
    payload_bytes: int
    elapsed_seconds: float
    drain_seconds: float = 0.0
    """Share of ``elapsed_seconds`` spent draining flushes after the last put."""

    @property
    def records_per_second(self) -> float:
        """Sequential write throughput in records per second.

        Guarded against a zero elapsed time rather than trusting the clock to be
        finer than the phase is short: a phase that measures as instantaneous
        would otherwise divide by zero and lose the whole run over a figure that
        was never meaningful at that record count anyway.
        """
        if self.elapsed_seconds <= 0.0:
            return float("inf")
        return self.records / self.elapsed_seconds

    @property
    def mib_per_second(self) -> float:
        """Key and value bytes written per second, in MiB/s, excluding engine framing.

        Payload only, because framing overhead is the engine's choice rather than
        the caller's, and a figure that counted it would improve when the format
        got less efficient.
        """
        if self.elapsed_seconds <= 0.0:
            return float("inf")
        return self.payload_bytes / self.elapsed_seconds / (1024 * 1024)

    def to_dict(self) -> dict[str, object]:
        """Return the phase result as plain JSON-safe values."""
        return {
            "records": self.records,
            "value_size_bytes": self.value_size_bytes,
            "payload_bytes": self.payload_bytes,
            "elapsed_seconds": self.elapsed_seconds,
            "drain_seconds": self.drain_seconds,
            "records_per_second": self.records_per_second,
            "mib_per_second": self.mib_per_second,
        }


@dataclass(frozen=True)
class ReadPhaseResult:
    """What the randomized read phase measured.

    ``misses`` counts reads that came back absent and ``wrong_values`` reads that
    came back with something other than what the write phase wrote. Both are
    reported rather than merely asserted because every key the read phase asks
    for was written by this run with one known value, so either is a correctness
    failure in the engine and not a property of the workload. A bench run that
    quietly reported a fast latency over reads that found nothing, or found the
    wrong thing, would be worse than no bench at all.

    ``sstables_at_read_time`` and ``memtable_bytes_at_read_time`` describe the
    tree the phase started against. They are context for the latency and not a
    count of the tables any read searched, which is what read amplification
    means and what :class:`AmplificationPhaseResult` reports.
    """

    reads: int
    hits: int
    misses: int
    elapsed_seconds: float
    sstables_at_read_time: int
    latency: LatencySummary
    wrong_values: int = 0
    memtable_bytes_at_read_time: int = 0

    @property
    def reads_per_second(self) -> float:
        """Read throughput in reads per second, for the phase as a whole."""
        if self.elapsed_seconds <= 0.0:
            return float("inf")
        return self.reads / self.elapsed_seconds

    def to_dict(self) -> dict[str, object]:
        """Return the phase result as plain JSON-safe values."""
        return {
            "reads": self.reads,
            "hits": self.hits,
            "misses": self.misses,
            "elapsed_seconds": self.elapsed_seconds,
            "reads_per_second": self.reads_per_second,
            "wrong_values": self.wrong_values,
            "sstables_at_read_time": self.sstables_at_read_time,
            "memtable_bytes_at_read_time": self.memtable_bytes_at_read_time,
            "latency": self.latency.to_dict(),
        }


@dataclass(frozen=True)
class AmplificationSummary:
    """How many SSTables the reads of one phase searched, as a distribution.

    A mean alone would hide the shape this phase exists to show. Before a
    compaction the counts spread out, because a key written early sits under
    every table written after it while a key written late is found in the first
    table tried, and the reads worth worrying about are the ones at the top of
    that spread rather than the average of it. So the percentiles and the
    histogram are reported next to the mean, and the histogram is exact: there
    are at most as many distinct values as there are tables, so nothing has to
    be bucketed and no row is an approximation.

    Counts are tables searched, which is tables whose data block was read. A
    table a bloom filter rejected is not one of them, and that is the point:
    the filter exists to keep those out of this number.
    """

    count: int
    min_tables: int
    mean_tables: float
    max_tables: int
    percentiles: dict[str, int]
    histogram: tuple[tuple[int, int], ...]
    """Pairs of (tables searched, reads that searched that many), ascending."""

    @classmethod
    def from_samples(cls, samples: Sequence[int]) -> AmplificationSummary:
        """Summarize per-read table counts, which need not be sorted."""
        if not samples:
            raise ValueError("cannot summarize an empty read-amplification sample")

        ordered = sorted(samples)
        return cls(
            count=len(ordered),
            min_tables=ordered[0],
            mean_tables=statistics.fmean(ordered),
            max_tables=ordered[-1],
            percentiles={
                _percentile_label(percentile): nearest_rank_percentile(ordered, percentile)
                for percentile in PERCENTILES
            },
            histogram=tuple(sorted(Counter(ordered).items())),
        )

    def to_dict(self) -> dict[str, object]:
        """Return the summary as plain JSON-safe values."""
        return {
            "count": self.count,
            "min_tables": self.min_tables,
            "mean_tables": self.mean_tables,
            "max_tables": self.max_tables,
            "percentiles": dict(self.percentiles),
            "histogram": [
                {"tables_searched": tables, "reads": reads} for tables, reads in self.histogram
            ],
        }

    def format_histogram(self, max_rows: int = MAX_HISTOGRAM_ROWS) -> str:
        """Return the histogram on one line, as ``tables searched x reads`` pairs."""
        shown = self.histogram[:max_rows]
        parts = [f"{tables}x{reads}" for tables, reads in shown]
        remaining = len(self.histogram) - len(shown)
        if remaining > 0:
            parts.append(f"(+{remaining} more)")
        return " ".join(parts)


@dataclass(frozen=True)
class AmplificationPhaseResult:
    """One side of the read-amplification comparison, before or after compaction.

    ``records_on_disk`` is how many records the tables held, which is also how
    many distinct keys the phase could draw from: the write phase writes each
    key exactly once, so a table's record count is a key count, and the two
    sides must agree on it or they are not measuring one dataset.

    ``served_from_memory`` should be zero on both sides, since both run against
    an engine with nothing to replay. It is counted rather than assumed because
    a read memory answered searches no table, so any of them would pull the
    figure down while looking like a good result.
    """

    label: str
    sstables: int
    table_bytes: int
    records_on_disk: int
    reads: int
    hits: int
    misses: int
    wrong_values: int
    served_from_memory: int
    tables_skipped_by_bloom: int
    bloom_false_positives: int
    elapsed_seconds: float
    searched: AmplificationSummary

    @property
    def mean_tables_per_read(self) -> float:
        """Mean number of SSTables searched per read: the read-amplification figure."""
        return self.searched.mean_tables

    @property
    def bloom_false_positive_rate(self) -> float:
        """Share of the probes for a key a table did not hold that the filter admitted.

        The denominator is every table this phase asked about a key the table
        turned out not to have, which is the tables the filters rejected plus
        the ones they admitted and that then held nothing. That is the rate a
        bloom filter is configured for, so this figure can be read against
        :data:`~ledgerlog.sstable.DEFAULT_BLOOM_FALSE_POSITIVE_RATE` directly.

        Dividing by the tables searched instead would answer a different
        question (how much of a read's work the filter failed to prevent) and
        would come out an order of magnitude higher, since most of what a filter
        does is keep tables out of that number entirely.

        Zero when no probe missed, which is the honest answer for a phase run
        against a single table that holds every key.
        """
        absent_probes = self.tables_skipped_by_bloom + self.bloom_false_positives
        if absent_probes <= 0:
            return 0.0
        return self.bloom_false_positives / absent_probes

    def to_dict(self) -> dict[str, object]:
        """Return the phase result as plain JSON-safe values."""
        return {
            "label": self.label,
            "sstables": self.sstables,
            "table_bytes": self.table_bytes,
            "records_on_disk": self.records_on_disk,
            "reads": self.reads,
            "hits": self.hits,
            "misses": self.misses,
            "wrong_values": self.wrong_values,
            "served_from_memory": self.served_from_memory,
            "tables_skipped_by_bloom": self.tables_skipped_by_bloom,
            "bloom_false_positives": self.bloom_false_positives,
            "bloom_false_positive_rate": self.bloom_false_positive_rate,
            "elapsed_seconds": self.elapsed_seconds,
            "mean_tables_per_read": self.mean_tables_per_read,
            "searched": self.searched.to_dict(),
        }


@dataclass(frozen=True)
class CompactionSummary:
    """What the compaction between the two measurements did.

    ``passes`` is how many tiers were merged. Zero means no tier was ready, so
    the two sides describe the same tables and the comparison, while still true,
    shows nothing: the report says so rather than letting an unchanged figure
    read as a result.
    """

    passes: int
    tables_before: int
    tables_after: int
    bytes_before: int
    bytes_after: int
    elapsed_seconds: float
    schedule: str = COMPACTION_SCHEDULE_BULK
    bytes_rewritten: int = 0
    """Bytes of merged table this compaction wrote, summed over its passes.

    The write-amplification side of the size-tier trade-off, and the reason the
    two knobs are not free: a policy that merges sooner leaves fewer tables for
    a read to search and rewrites more bytes to get there. It is the sum over
    passes rather than the final table's size, so a record merged through three
    tiers is counted three times, which is what it cost.
    """

    @property
    def write_amplification(self) -> float:
        """Merged bytes written per byte the run flushed, or zero if nothing was flushed."""
        if self.bytes_before <= 0:
            return 0.0
        return self.bytes_rewritten / self.bytes_before

    def to_dict(self) -> dict[str, object]:
        """Return the summary as plain JSON-safe values."""
        return {
            "schedule": self.schedule,
            "passes": self.passes,
            "tables_before": self.tables_before,
            "tables_after": self.tables_after,
            "bytes_before": self.bytes_before,
            "bytes_after": self.bytes_after,
            "bytes_rewritten": self.bytes_rewritten,
            "write_amplification": self.write_amplification,
            "elapsed_seconds": self.elapsed_seconds,
        }


@dataclass(frozen=True)
class AmplificationComparison:
    """The before and after measurements, and the compaction that separates them."""

    before: AmplificationPhaseResult
    after: AmplificationPhaseResult
    compaction: CompactionSummary

    @property
    def ran_a_compaction(self) -> bool:
        """True if at least one tier was merged between the two measurements."""
        return self.compaction.passes > 0

    @property
    def reduced(self) -> bool:
        """True if the compaction did not increase the mean tables searched per read.

        "Did not increase" rather than "decreased" because a compaction that
        found no ready tier leaves the two sides identical, and because one that
        merged every table into one cannot do better than the single table a
        read already had to search.
        """
        return self.after.mean_tables_per_read <= self.before.mean_tables_per_read

    @property
    def reduction_factor(self) -> float:
        """How many times fewer tables a read searches after the compaction.

        ``inf`` when the after side searched nothing at all, which cannot happen
        for a dataset whose keys are all on disk but is defined rather than left
        to raise.
        """
        after = self.after.mean_tables_per_read
        if after <= 0.0:
            return float("inf")
        return self.before.mean_tables_per_read / after

    def to_dict(self) -> dict[str, object]:
        """Return the comparison as plain JSON-safe values."""
        return {
            "before": self.before.to_dict(),
            "after": self.after.to_dict(),
            "compaction": self.compaction.to_dict(),
            "reduced": self.reduced,
            "reduction_factor": self.reduction_factor,
        }


@dataclass(frozen=True)
class BenchResult:
    """A complete run: the configuration it ran under and both phases' figures."""

    config: BenchConfig
    write: WritePhaseResult
    read: ReadPhaseResult
    filesystem: str | None = None
    """Type of the filesystem the run directory sat on, or ``None`` if unknown."""

    amplification: AmplificationComparison | None = None
    """The read-amplification comparison, or ``None`` if the phase did not run."""

    @property
    def ram_backed(self) -> bool:
        """True if the run directory was on a filesystem where fsync reaches no disk."""
        return self.filesystem in RAM_BACKED_FILESYSTEMS

    def to_dict(self) -> dict[str, object]:
        """Return the whole run as plain JSON-safe values, for ``--json``."""
        return {
            "command": self.config.command_line(),
            "config": self.config.to_dict(),
            "filesystem": self.filesystem,
            "ram_backed_filesystem": self.ram_backed,
            "write": self.write.to_dict(),
            "read": self.read.to_dict(),
            "amplification": (
                self.amplification.to_dict() if self.amplification is not None else None
            ),
        }

    def format_report(self) -> str:
        """Return the human-readable report, one figure per line.

        The invocation is printed first, before any number, for the reason
        :meth:`BenchConfig.command_line` exists: a figure read out of this report
        and quoted elsewhere should carry the parameters that produced it, and the
        surest way to get that is for them to be impossible to miss.
        """
        latency = self.read.latency
        lines = [
            "LedgerLog benchmark",
            f"  invocation: {self.config.command_line()}",
            f"  filesystem: {self.filesystem or 'unknown'}",
        ]
        if self.ram_backed:
            lines.append(
                "  warning:    RAM-backed filesystem, fsync does not reach a disk;"
                " pass --directory on a real disk for durable-write figures"
            )
        lines += [
            "",
            "Sequential write phase",
            f"  records:            {self.write.records}",
            f"  value size:         {self.write.value_size_bytes} B",
            f"  payload written:    {self.write.payload_bytes / (1024 * 1024):.2f} MiB",
            f"  elapsed:            {self.write.elapsed_seconds:.3f} s"
            f" (of which flush drain {self.write.drain_seconds:.3f} s)",
            f"  throughput:         {self.write.records_per_second:,.0f} records/s"
            f" ({self.write.mib_per_second:.2f} MiB/s payload)",
            "",
            "Randomized read phase",
            f"  reads:              {self.read.reads}",
            f"  hits / misses:      {self.read.hits} / {self.read.misses}",
            f"  wrong values:       {self.read.wrong_values}",
            f"  sstables at read:   {self.read.sstables_at_read_time}"
            " (tables held, not tables searched; see --read-amplification)",
            f"  memtable at read:   {self.read.memtable_bytes_at_read_time / (1024 * 1024):.2f} MiB"
            " (records not yet frozen, served from memory)",
            f"  elapsed:            {self.read.elapsed_seconds:.3f} s",
            f"  throughput:         {self.read.reads_per_second:,.0f} reads/s",
            f"  latency min:        {latency.min_ns / 1e6:.4f} ms",
            f"  latency mean:       {latency.mean_ns / 1e6:.4f} ms",
        ]
        lines.extend(
            f"  latency {label + ':':<12}{value / 1e6:.4f} ms"
            for label, value in latency.percentiles_ns.items()
        )
        lines.append(f"  latency max:        {latency.max_ns / 1e6:.4f} ms")
        if self.amplification is not None:
            lines.extend(_format_amplification(self.amplification))
        return "\n".join(lines)


def _format_amplification(comparison: AmplificationComparison) -> list[str]:
    """Return the read-amplification block of the report, as lines.

    A function rather than a method on the comparison because it is the report's
    layout rather than the measurement's meaning, and :meth:`BenchResult.format_report`
    is where the rest of that layout lives.
    """
    compaction = comparison.compaction
    lines = [
        "",
        "Read amplification (SSTables searched per get, measured in the read path)",
        f"  schedule:           {compaction.schedule}",
        f"  compaction:         {compaction.passes} tier merge(s) in"
        f" {compaction.elapsed_seconds:.3f} s,"
        f" {compaction.tables_before} tables -> {compaction.tables_after},"
        f" {compaction.bytes_before / (1024 * 1024):.2f} MiB ->"
        f" {compaction.bytes_after / (1024 * 1024):.2f} MiB",
        f"  bytes rewritten:    {compaction.bytes_rewritten / (1024 * 1024):.2f} MiB"
        f" ({compaction.write_amplification:.2f}x the bytes flushed)",
    ]
    if not comparison.ran_a_compaction:
        lines.append(
            "  note:               no tier was ready to merge, so both sides below describe"
            " the same tables; lower --memtable-threshold-bytes or raise --records"
        )
    for phase in (comparison.before, comparison.after):
        searched = phase.searched
        lines += [
            "",
            f"  {phase.label}",
            f"    sstables:         {phase.sstables}"
            f" ({phase.records_on_disk} records, served from memory: {phase.served_from_memory})",
            f"    reads:            {phase.reads}"
            f" (hits {phase.hits}, misses {phase.misses}, wrong values {phase.wrong_values})",
            f"    searched mean:    {phase.mean_tables_per_read:.3f} tables/read",
            f"    searched min:     {searched.min_tables}",
        ]
        lines.extend(
            f"    searched {label + ':':<9}{value}" for label, value in searched.percentiles.items()
        )
        lines += [
            f"    searched max:     {searched.max_tables}",
            f"    distribution:     {searched.format_histogram()}",
            f"    bloom skipped:    {phase.tables_skipped_by_bloom} tables"
            f" (false positives {phase.bloom_false_positives},"
            f" {phase.bloom_false_positive_rate * 100:.2f}% of absent-key probes)",
        ]
    lines += [
        "",
        f"  before -> after:    {comparison.before.mean_tables_per_read:.3f} ->"
        f" {comparison.after.mean_tables_per_read:.3f} tables/read"
        f" ({comparison.reduction_factor:.2f}x"
        f" {'fewer' if comparison.reduced else 'MORE'})",
    ]
    return lines


class BenchError(RuntimeError):
    """Raised when a run cannot produce a trustworthy measurement.

    Separate from ``ValueError``, which :func:`main` reports as a rejected
    configuration with exit code 2: this is a run that was configured correctly
    and could not complete, which exits 1.
    """


def _run_write_phase(engine: LedgerLog, config: BenchConfig) -> WritePhaseResult:
    """Write ``config.records`` records in ascending key order and time the whole phase.

    Wall clock across the phase rather than a sum of per-call times, because the
    engine is allowed to do work between calls (freezing a full memtable, handing
    it to the flush thread) and that work is part of what writing N records costs.
    A sum of per-call times would also pay for a timer call per record, which at
    this phase's rate is a measurable share of what is being measured.

    When draining is on, the clock stops only after every frozen memtable is on
    disk, and the drain's share is recorded separately. See this module's
    docstring for why the drain belongs to the write phase.
    """
    value = _value_for(config)
    records = config.records
    # Computed rather than accumulated in the loop, so the timed region holds the
    # key formatting and the put and nothing else. It is exact rather than an
    # estimate because every key is the same length, which
    # BenchConfig.__post_init__ enforces by capping the record count at what the
    # padding width can hold.
    payload_bytes = records * (KEY_LENGTH + config.value_size_bytes)

    start = time.perf_counter_ns()
    for index in range(records):
        engine.put(key_for_index(index), value)
    puts_done = time.perf_counter_ns()
    if config.drain_flushes:
        _drain_flushes(engine)
    end = time.perf_counter_ns()

    return WritePhaseResult(
        records=records,
        value_size_bytes=config.value_size_bytes,
        payload_bytes=payload_bytes,
        elapsed_seconds=(end - start) / 1e9,
        drain_seconds=(end - puts_done) / 1e9,
    )


def _value_for(config: BenchConfig) -> bytes:
    """Return the one value every record in a run carries.

    A function rather than an inline expression because the read phase has to
    compare against exactly what the write phase wrote, and two copies of the
    expression are two places for them to drift apart.
    """
    return b"v" * config.value_size_bytes


def _drain_flushes(engine: LedgerLog) -> None:
    """Flush every frozen memtable on this thread, raising :class:`BenchError` on failure.

    ``flush_pending`` takes the flush lock for each table and loops until nothing
    is frozen, so when it returns a table the background thread was midway
    through has been finished too, and no wait is needed afterwards. A table the
    background thread failed to flush is still frozen, so this retries it here,
    where a failure is raised rather than parked in ``flush_error``. That is the
    failure path a real engine produces, and it is reported as a bench error
    rather than measured around, because a read phase over an engine that cannot
    flush is a measurement of a broken engine.
    """
    try:
        engine.flush_pending()
    except Exception as error:
        raise BenchError(f"flush failed while draining before the read phase: {error!r}") from error


def _run_read_phase(engine: LedgerLog, config: BenchConfig) -> ReadPhaseResult:
    """Read randomly chosen written keys and summarize the per-read latencies.

    The index sequence comes from :func:`draw_read_indices` before the timing
    starts, so that the random number generator's own cost is not charged to the
    engine. See that function for why the draw is with replacement and why it
    carries its own generator.

    Each value is compared with what the write phase wrote, after that read's
    latency sample is taken, so the comparison is outside every per-read figure.
    It is inside the phase's overall elapsed time, which makes ``reads_per_second``
    include one bytes comparison per read: a small, fixed cost that buys the
    guarantee that a fast read is also a correct one.

    The tree's shape is sampled before the loop rather than after, so the figures
    describe what the reads were served from. With flushes drained (the default)
    the two are the same, but under ``--no-drain-flushes`` a flush can land
    mid-phase, and the shape that makes the latency interpretable is the one the
    phase started against.
    """
    read_count = config.read_count
    indices = draw_read_indices(config.records, read_count, config.seed)
    expected = _value_for(config)

    latencies_ns = array("q")
    hits = 0
    wrong_values = 0
    sstables_at_read_time = len(engine.sstables)
    memtable_bytes_at_read_time = engine.memtable_nbytes

    phase_start = time.perf_counter_ns()
    for index in indices:
        key = key_for_index(index)
        start = time.perf_counter_ns()
        value = engine.get(key)
        latencies_ns.append(time.perf_counter_ns() - start)
        if value is not None:
            hits += 1
            if value != expected:
                wrong_values += 1
    phase_elapsed_ns = time.perf_counter_ns() - phase_start

    return ReadPhaseResult(
        reads=read_count,
        hits=hits,
        misses=read_count - hits,
        elapsed_seconds=phase_elapsed_ns / 1e9,
        sstables_at_read_time=sstables_at_read_time,
        latency=LatencySummary.from_samples(latencies_ns),
        wrong_values=wrong_values,
        memtable_bytes_at_read_time=memtable_bytes_at_read_time,
    )


def _snapshot_committed_tables(engine: LedgerLog, destination: Path) -> tuple[int, int]:
    """Put a copy of ``engine``'s committed tables in ``destination``. Returns count and bytes.

    Hard links first, a byte copy only where the filesystem refuses one. A link
    costs no space and no time, and the compacted side needs to be free to
    delete its sources, which it can do to a link without touching the run's own
    table. The fallback exists because a hard link is not available everywhere
    (a filesystem may simply refuse, and the destination is under the run
    directory precisely so that the two are never on different devices).

    The tables are immutable once committed (ARCHITECTURE.md section 3) so there
    is nothing to tear here: a flush landing mid-snapshot can add a table this
    misses, never change one it took. What it does mean is that the snapshot is
    the table list as it was read, which is why that list is read once into a
    tuple rather than walked twice.

    A file that disappears between the listing and the link is reported rather
    than skipped: it would mean something retired a committed table underneath a
    running engine, and a snapshot quietly short of a table would make the
    amplification figure describe a dataset the run never wrote.

    ``destination`` must not already exist. A run that snapshotted into a
    directory holding tables could not tell its own from whatever was there, and
    would compact and measure both.
    """
    try:
        destination.mkdir(parents=True, exist_ok=False)
    except OSError as error:
        raise BenchError(
            f"could not create the read-amplification workspace {str(destination)!r},"
            f" so this run cannot tell its own tables from another run's: {error!r}"
        ) from error
    handles = engine.sstables
    total_bytes = 0
    for handle in handles:
        target = destination / handle.path.name
        try:
            os.link(handle.path, target)
        except OSError:
            try:
                shutil.copy2(handle.path, target)
            except OSError as error:
                raise BenchError(
                    f"could not snapshot table {str(handle.path)!r} for the"
                    f" read-amplification phase: {error!r}"
                ) from error
        total_bytes += handle.size_bytes
    return len(handles), total_bytes


def _compact_until_settled(
    directory: Path,
    policy: CompactionPolicy,
    *,
    index_interval: int = DEFAULT_INDEX_INTERVAL,
    bloom_false_positive_rate: float = DEFAULT_BLOOM_FALSE_POSITIVE_RATE,
) -> CompactionSummary:
    """Merge ready tiers in ``directory`` until none is ready, and report what happened.

    The engine does not run compactions of its own: ``engine.py`` leaves that to
    a caller and publishes a plan describing which tiers are worth merging, and
    this harness is such a caller. So the loop here is the whole policy: ask
    :func:`~ledgerlog.compaction.plan_compaction` for the next ready tier, merge
    it with :func:`~ledgerlog.compaction.compact_tier`, and look again. The
    directory is rescanned each time rather than the plan being adjusted in
    place, because a merge changes which tier every remaining table belongs to.

    No engine may be open on ``directory`` while this runs. A merge deletes the
    tables it consumed, and an engine holds a reader open on each one.

    ``other_tables`` gets the whole scan, tier included, which is what
    :func:`~ledgerlog.compaction.merge_tier` documents as the expected call: it
    picks the tables older than the tier out of that list itself, and those are
    what decide whether a tombstone can be dropped. No clock is passed, so no
    record is dropped for having expired. This harness writes no TTLs, and
    handing a merge a clock to drop records that cannot exist would put a line
    in the report describing work that never happened.

    The merged tables are written with the same index cadence and bloom target
    the run's flushes used, so that the after side of the comparison differs
    from the before side by the merge and not by a table layout the sweep did
    not ask for. Leaving them at the module defaults would quietly make every
    "after" figure in the M10.4 matrix describe the default layout, whatever
    setting the run was measuring.

    The pass count is bounded by the number of tables the directory started
    with. Each merge replaces at least two tables with one, so the loop
    terminates on its own; the bound is there so that a policy that somehow made
    a merge a no-op would raise instead of spinning on the disk forever.
    """
    start = time.perf_counter_ns()
    initial = discover_sstables(directory).tables
    tables_before = len(initial)
    bytes_before = sum(table.size_bytes for table in initial)

    passes, rewritten = _merge_ready_tiers(
        directory,
        policy,
        pass_limit=tables_before,
        index_interval=index_interval,
        bloom_false_positive_rate=bloom_false_positive_rate,
    )

    final = discover_sstables(directory).tables
    return CompactionSummary(
        passes=passes,
        tables_before=tables_before,
        tables_after=len(final),
        bytes_before=bytes_before,
        bytes_after=sum(table.size_bytes for table in final),
        elapsed_seconds=(time.perf_counter_ns() - start) / 1e9,
        schedule=COMPACTION_SCHEDULE_BULK,
        bytes_rewritten=rewritten,
    )


def _merge_ready_tiers(
    directory: Path,
    policy: CompactionPolicy,
    *,
    pass_limit: int,
    index_interval: int,
    bloom_false_positive_rate: float,
) -> tuple[int, int]:
    """Merge ready tiers in ``directory`` until none is ready. Returns passes and bytes written.

    The loop both schedules share, factored out so that the incremental schedule
    runs the same merges the bulk one does and the two can only differ in when
    they are asked to. The directory is rescanned on every iteration rather than
    the plan adjusted in place, because a merge changes which tier every
    remaining table belongs to.

    ``pass_limit`` bounds the loop. Each merge replaces at least two tables with
    one, so it terminates on its own and the bound only catches a policy that
    somehow made a merge a no-op, which would otherwise spin on the disk.

    The merged size is read from the swap's own file rather than summed from the
    sources, since dropped tombstones and expired records make those two
    different numbers and the one worth reporting is what was actually written.
    """
    passes = 0
    rewritten = 0
    while True:
        tables = discover_sstables(directory).tables
        plan = plan_compaction(tables, policy)
        tier = plan.next_tier
        if tier is None:
            return passes, rewritten
        if passes >= pass_limit:
            raise BenchError(
                f"compaction made {passes} passes without settling, which should be"
                " impossible: each merge replaces at least two tables with one"
            )
        destination = directory / sstable_filename(max(table.sequence for table in tables) + 1)
        swap = compact_tier(
            tier,
            destination,
            other_tables=tables,
            index_interval=index_interval,
            bloom_false_positive_rate=bloom_false_positive_rate,
        )
        rewritten += swap.merged.stat().st_size
        passes += 1


def _compact_incrementally(
    source: Path,
    destination: Path,
    policy: CompactionPolicy,
    *,
    index_interval: int,
    bloom_false_positive_rate: float,
) -> CompactionSummary:
    """Rebuild ``source``'s tables in ``destination`` flush by flush, merging as tiers fill.

    The schedule a running engine follows, reconstructed after the fact: add one
    flushed table, ask whether any tier is now ready, merge the ones that are,
    then add the next. What comes out is the table-size distribution the policy
    settles at rather than the single table a bulk compaction of a finished
    directory always produces, and that distribution is the thing the two
    size-tier knobs decide. See :data:`COMPACTION_SCHEDULE_INCREMENTAL`.

    Tables are added oldest first, renumbered so that each one is newer than
    everything already in ``destination``, including the merges. Keeping their
    original sequence numbers would make a table added after a merge look older
    than that merge, which would invert newest-wins for any key the two shared.
    This harness writes each key exactly once so no key is shared, and the
    renumbering is here so that the reconstruction does not depend on that.

    ``destination`` must not already exist, for the reason the snapshot's own
    directory must not: tables already there would be merged and measured as if
    this run had written them.

    The links are made from ``source`` rather than from the engine, so this never
    touches the run's own tables: the merges here unlink names in
    ``destination`` and the snapshot's links keep the inodes alive.
    """
    start = time.perf_counter_ns()
    try:
        destination.mkdir(parents=True, exist_ok=False)
    except OSError as error:
        raise BenchError(
            f"could not create the incremental compaction workspace {str(destination)!r}: {error!r}"
        ) from error

    flushed = sorted(discover_sstables(source).tables, key=lambda table: table.sequence)
    tables_before = len(flushed)
    bytes_before = sum(table.size_bytes for table in flushed)

    passes = 0
    rewritten = 0
    next_sequence = 0
    for table in flushed:
        target = destination / sstable_filename(next_sequence)
        next_sequence += 1
        try:
            os.link(table.path, target)
        except OSError:
            try:
                shutil.copy2(table.path, target)
            except OSError as error:
                raise BenchError(
                    f"could not stage table {str(table.path)!r} for the incremental"
                    f" compaction schedule: {error!r}"
                ) from error
        # Bounded by the tables staged so far, which is the most merges that
        # could possibly be outstanding: every one of them removes at least two.
        merged, merged_bytes = _merge_ready_tiers(
            destination,
            policy,
            pass_limit=next_sequence,
            index_interval=index_interval,
            bloom_false_positive_rate=bloom_false_positive_rate,
        )
        passes += merged
        rewritten += merged_bytes
        if merged:
            # A merge names its output above every sequence present, so the next
            # staged table has to clear that too or it would reuse a name.
            next_sequence = max(
                next_sequence,
                max(existing.sequence for existing in discover_sstables(destination).tables) + 1,
            )

    final = discover_sstables(destination).tables
    return CompactionSummary(
        passes=passes,
        tables_before=tables_before,
        tables_after=len(final),
        bytes_before=bytes_before,
        bytes_after=sum(table.size_bytes for table in final),
        elapsed_seconds=(time.perf_counter_ns() - start) / 1e9,
        schedule=COMPACTION_SCHEDULE_INCREMENTAL,
        bytes_rewritten=rewritten,
    )


def _measure_amplification(
    directory: Path,
    config: BenchConfig,
    label: str,
    indices: Sequence[int] | None = None,
) -> tuple[AmplificationPhaseResult, Sequence[int]]:
    """Measure tables searched per read against the tables in ``directory``.

    Opens an engine of its own over ``directory``. There is no log there to
    replay on the first call and nothing writes to the one the engine creates,
    so the memtable stays empty and every read reaches the tables, which is what
    makes the two sides of the comparison differ only by the compaction between
    them. The count of reads memory answered is returned with the rest so that
    the claim is checked rather than stated.

    ``indices`` is the read pattern, and it is handed straight back so that the
    second call can be given the one the first used. Drawing twice from the same
    seed would produce the same pattern only as long as both sides agreed on the
    key range, and passing the sequence across means that agreement is something
    the comparison does not have to rely on. It is returned rather than copied
    because a copy into a narrower array type would be one more place for a
    large run's indices to have to fit.

    The probe is reset before each read and its counts taken after, which is how
    a per-read figure comes out of a cumulative object. That is an attribute
    write per read inside the loop, which is why this phase reports no latency:
    it measures what a read did, and the other read phase measures what it cost.
    """
    with LedgerLog(
        directory,
        fsync_policy=config.fsync_policy,
        fsync_interval_seconds=config.fsync_interval_seconds,
        memtable_threshold_bytes=config.memtable_threshold_bytes,
        index_interval=config.index_interval,
        bloom_false_positive_rate=config.bloom_false_positive_rate,
        compaction_policy=config.compaction_policy(),
    ) as engine:
        handles = engine.sstables
        records_on_disk = sum(handle.record_count for handle in handles)
        if records_on_disk < 1:
            raise BenchError(
                f"the read-amplification phase found no records on disk {label}: the run"
                " wrote no SSTable, so lower --memtable-threshold-bytes or raise --records"
            )
        if indices is None:
            indices = draw_read_indices(records_on_disk, config.read_count, config.seed)

        expected = _value_for(config)
        probe = ReadProbe()
        samples = array("i")
        hits = 0
        wrong_values = 0
        skipped_by_bloom = 0
        false_positives = 0
        served_from_memory = 0

        start = time.perf_counter_ns()
        for index in indices:
            probe.reset()
            value = engine.get(key_for_index(index), probe=probe)
            samples.append(probe.sstables_consulted)
            skipped_by_bloom += probe.sstables_skipped_by_bloom
            false_positives += probe.sstables_without_match
            served_from_memory += probe.found_in_memory
            if value is not None:
                hits += 1
                if value != expected:
                    wrong_values += 1
        elapsed_ns = time.perf_counter_ns() - start

    result = AmplificationPhaseResult(
        label=label,
        sstables=len(handles),
        table_bytes=sum(handle.size_bytes for handle in handles),
        records_on_disk=records_on_disk,
        reads=len(samples),
        hits=hits,
        misses=len(samples) - hits,
        wrong_values=wrong_values,
        served_from_memory=served_from_memory,
        tables_skipped_by_bloom=skipped_by_bloom,
        bloom_false_positives=false_positives,
        elapsed_seconds=elapsed_ns / 1e9,
        searched=AmplificationSummary.from_samples(samples),
    )
    return result, indices


def _run_amplification_phase(
    engine: LedgerLog, config: BenchConfig, run_directory: Path
) -> AmplificationComparison:
    """Measure read amplification over the run's tables, before and after a compaction.

    Story M10.2's two halves, in the only order they can happen in: snapshot the
    run's committed tables into a workspace, measure there, compact there,
    measure again. The workspace is a directory of its own so that the run's own
    engine keeps serving its own tables, and so the compaction deletes links
    rather than the tables the run wrote.

    The engine opened for the first measurement is closed before the compaction
    starts, because a merge deletes the files it consumed and that engine holds
    a reader open on each of them.

    Under the incremental schedule the second measurement runs in a directory of
    its own, since that schedule rebuilds the dataset table by table rather than
    compacting the snapshot in place. Both directories hold links to the same
    inodes, so the two sides still describe one dataset, which the record count
    check below confirms rather than assumes.

    Both measurements have to describe one dataset or the comparison means
    nothing. The read pattern is the same sequence on both sides by
    construction, so the one thing left that could differ is how many records
    the tables hold, and that is checked. A merge of tables holding no
    tombstones and no expiries reproduces every record it read, so a
    disagreement there is a defect rather than a property of the workload, and
    it is raised rather than printed under a heading that says the two are
    comparable.
    """
    workspace = run_directory / AMPLIFICATION_DIRNAME
    _snapshot_committed_tables(engine, workspace)

    before, indices = _measure_amplification(workspace, config, "before compaction")
    policy = config.compaction_policy()
    if config.compaction_schedule == COMPACTION_SCHEDULE_INCREMENTAL:
        measured_in = run_directory / INCREMENTAL_DIRNAME
        compaction = _compact_incrementally(
            workspace,
            measured_in,
            policy,
            index_interval=config.index_interval,
            bloom_false_positive_rate=config.bloom_false_positive_rate,
        )
    else:
        measured_in = workspace
        compaction = _compact_until_settled(
            workspace,
            policy,
            index_interval=config.index_interval,
            bloom_false_positive_rate=config.bloom_false_positive_rate,
        )
    after, _ = _measure_amplification(measured_in, config, "after compaction", indices)

    if after.records_on_disk != before.records_on_disk:
        raise BenchError(
            f"compaction changed the record count from {before.records_on_disk} to"
            f" {after.records_on_disk}: the two sides of the comparison no longer"
            " describe one dataset"
        )
    return AmplificationComparison(before=before, after=after, compaction=compaction)


def run_bench(config: BenchConfig) -> BenchResult:
    """Run both phases against a fresh engine and return the measured figures.

    An :class:`~contextlib.ExitStack` owns both the temporary directory and the
    engine, so a failure anywhere in either phase still closes the engine and
    still removes the directory. That matters more for a benchmark than it looks:
    the fsync-always runs this harness exists to measure leave a log file behind
    per run, and a harness that leaked one per crash would fill a disk before
    anybody noticed.

    When ``config.directory`` is given it is used and kept afterwards, because a
    caller who names a directory wants the files. It must be empty or not yet
    exist, and a ``ValueError`` is raised otherwise: see this module's docstring
    for why reusing a directory would make the printed invocation lie. When no
    directory is given, the run happens in a temporary directory that is removed
    on the way out.

    The read-amplification phase, when it is asked for, runs last and inside the
    same directory, in a subdirectory of its own. It runs while the engine above
    is still open, which costs a descriptor per table and nothing else: it works
    on its own copies and never writes through this engine.
    """
    with ExitStack() as stack:
        if config.directory is None:
            directory = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="ledgerlog-")))
        else:
            directory = config.directory
            if directory.exists() and (not directory.is_dir() or any(directory.iterdir())):
                raise ValueError(
                    f"directory {str(directory)!r} must be empty or not exist, so the run"
                    " starts from no data and its invocation reproduces it"
                )
            directory.mkdir(parents=True, exist_ok=True)

        engine = stack.enter_context(
            LedgerLog(
                directory,
                fsync_policy=config.fsync_policy,
                fsync_interval_seconds=config.fsync_interval_seconds,
                memtable_threshold_bytes=config.memtable_threshold_bytes,
                index_interval=config.index_interval,
                bloom_false_positive_rate=config.bloom_false_positive_rate,
                compaction_policy=config.compaction_policy(),
            )
        )

        write = _run_write_phase(engine, config)
        read = _run_read_phase(engine, config)
        amplification = (
            _run_amplification_phase(engine, config, directory)
            if config.read_amplification
            else None
        )
        filesystem = filesystem_type(directory.resolve())

    return BenchResult(
        config=config,
        write=write,
        read=read,
        filesystem=filesystem,
        amplification=amplification,
    )


def build_parser() -> argparse.ArgumentParser:
    """Return the argument parser for ``python -m ledgerlog.bench``.

    Built by a function rather than at import time so that a test can get a fresh
    parser, and so importing this module to call :func:`run_bench` does not build
    a command line parser nobody asked for.
    """
    parser = argparse.ArgumentParser(
        prog="python -m ledgerlog.bench",
        description=("Measure LedgerLog sequential write throughput and random read latency."),
    )
    parser.add_argument(
        "--records",
        type=int,
        required=True,
        help="number of records to write in the sequential write phase",
    )
    parser.add_argument(
        "--value-size",
        type=int,
        default=DEFAULT_VALUE_SIZE_BYTES,
        dest="value_size_bytes",
        help=f"value size in bytes (default: {DEFAULT_VALUE_SIZE_BYTES})",
    )
    parser.add_argument(
        "--reads",
        type=int,
        default=None,
        help=(
            "number of randomized reads"
            f" (default: one per record written, at most {DEFAULT_MAX_READS})"
        ),
    )
    parser.add_argument(
        "--fsync-policy",
        type=FsyncPolicy,
        choices=tuple(FsyncPolicy),
        default=FsyncPolicy.ALWAYS,
        help="WAL fsync policy for the run (default: always)",
    )
    parser.add_argument(
        "--fsync-interval",
        type=float,
        default=DEFAULT_FSYNC_INTERVAL_SECONDS,
        dest="fsync_interval_seconds",
        help=(
            "fsync cadence in seconds, used only by the interval policy "
            f"(default: {DEFAULT_FSYNC_INTERVAL_SECONDS})"
        ),
    )
    parser.add_argument(
        "--memtable-threshold-bytes",
        type=int,
        default=DEFAULT_MEMTABLE_THRESHOLD_BYTES,
        help=(
            "payload size at which the memtable is frozen and flushed "
            f"(default: {DEFAULT_MEMTABLE_THRESHOLD_BYTES})"
        ),
    )
    parser.add_argument(
        "--index-interval",
        type=int,
        default=DEFAULT_INDEX_INTERVAL,
        help=(
            "records between two sparse index entries in the tables this run writes "
            f"(default: {DEFAULT_INDEX_INTERVAL})"
        ),
    )
    parser.add_argument(
        "--bloom-fp-rate",
        type=float,
        default=DEFAULT_BLOOM_FALSE_POSITIVE_RATE,
        dest="bloom_false_positive_rate",
        help=(
            "target bloom filter false-positive rate for the tables this run writes "
            f"(default: {DEFAULT_BLOOM_FALSE_POSITIVE_RATE})"
        ),
    )
    parser.add_argument(
        "--size-ratio",
        type=float,
        default=DEFAULT_SIZE_RATIO,
        help=(
            "how far the largest table in a size tier may exceed the smallest "
            f"(default: {DEFAULT_SIZE_RATIO})"
        ),
    )
    parser.add_argument(
        "--min-tier-tables",
        type=int,
        default=DEFAULT_MIN_TIER_TABLES,
        help=(
            f"tables a size tier must hold before it is merged (default: {DEFAULT_MIN_TIER_TABLES})"
        ),
    )
    parser.add_argument(
        "--compaction-schedule",
        choices=COMPACTION_SCHEDULES,
        default=COMPACTION_SCHEDULE_BULK,
        help=(
            "when the read-amplification phase compacts: once over every table"
            " (bulk), or flush by flush as a running engine would (incremental,"
            " the only schedule the size-tier options change)"
            f" (default: {COMPACTION_SCHEDULE_BULK})"
        ),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="seed for the read phase's key choices, so a run can be repeated (default: 0)",
    )
    parser.add_argument(
        "--directory",
        type=Path,
        default=None,
        help=(
            "empty or new directory to run in and keep"
            " (default: a temporary directory, removed afterwards)"
        ),
    )
    parser.add_argument(
        "--no-drain-flushes",
        action="store_false",
        dest="drain_flushes",
        help="start the read phase without waiting for pending flushes to finish",
    )
    parser.add_argument(
        "--read-amplification",
        action="store_true",
        dest="read_amplification",
        help=(
            "after the read phase, measure how many SSTables a read searches before and"
            " after compacting the run's tables (needs a memtable threshold low enough"
            " to produce several tables)"
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="emit_json",
        help="print the results as JSON instead of the human-readable report",
    )
    return parser


def config_from_args(args: argparse.Namespace) -> BenchConfig:
    """Build a :class:`BenchConfig` from parsed arguments.

    Separate from :func:`main` so that the mapping from arguments to
    configuration can be tested without running a benchmark, which at a useful
    record count is slow enough that nobody would run such a test often.
    """
    return BenchConfig(
        records=args.records,
        value_size_bytes=args.value_size_bytes,
        reads=args.reads,
        fsync_policy=args.fsync_policy,
        fsync_interval_seconds=args.fsync_interval_seconds,
        memtable_threshold_bytes=args.memtable_threshold_bytes,
        index_interval=args.index_interval,
        bloom_false_positive_rate=args.bloom_false_positive_rate,
        size_ratio=args.size_ratio,
        min_tier_tables=args.min_tier_tables,
        compaction_schedule=args.compaction_schedule,
        seed=args.seed,
        directory=args.directory,
        drain_flushes=args.drain_flushes,
        read_amplification=args.read_amplification,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the benchmark described by ``argv`` and print the report. Returns an exit code.

    A rejected configuration exits with code 2, the code ``argparse`` already uses
    for a bad command line, rather than raising: a benchmark invoked from a shell
    script should fail the way other command line tools fail. A traceback for
    ``--records 0`` would tell a caller less than one line saying the number has
    to be at least one.

    A run whose read phase missed any key, or read back a wrong value, exits 1,
    as does a run that could not drain its flushes, because every key the read
    phase asks for was written in the same run with one known value. A named
    directory that is not empty is a rejected configuration and exits 2. See
    :class:`ReadPhaseResult` for why a bad read is treated as a failure rather
    than a footnote: the alternative is a harness that reports an excellent
    latency for reads that found nothing.
    """
    args = build_parser().parse_args(argv)
    try:
        config = config_from_args(args)
        result = run_bench(config)
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    except BenchError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    if args.emit_json:
        print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    else:
        print(result.format_report())

    if result.ram_backed:
        print(
            f"warning: the run directory is on {result.filesystem}, where fsync reaches no"
            " disk; write figures do not describe durable writes",
            file=sys.stderr,
        )

    status = 0
    if result.read.misses:
        print(
            f"error: {result.read.misses} of {result.read.reads} reads found no value"
            " for a key this run wrote",
            file=sys.stderr,
        )
        status = 1
    if result.read.wrong_values:
        print(
            f"error: {result.read.wrong_values} of {result.read.reads} reads returned a"
            " value other than the one this run wrote",
            file=sys.stderr,
        )
        status = 1
    if result.amplification is not None:
        status = max(status, _report_amplification_problems(result.amplification))
    return status


def _report_amplification_problems(comparison: AmplificationComparison) -> int:
    """Print whatever makes the amplification figures untrustworthy. Returns an exit code.

    The same rule the read phase follows: a read that found nothing, or found
    the wrong value, is a correctness failure rather than a slow read, and a
    harness that printed a tidy figure over it would be worse than one that
    printed none. A read memory answered is added to that list here, because it
    searched no table and so pulls the mean down while looking like a result.

    A compaction that found no ready tier is a warning and not a failure: the
    figures are true, they just describe the same tables twice.
    """
    status = 0
    for phase in (comparison.before, comparison.after):
        if phase.misses:
            print(
                f"error: {phase.misses} of {phase.reads} read-amplification reads"
                f" {phase.label} found no value for a key this run wrote",
                file=sys.stderr,
            )
            status = 1
        if phase.wrong_values:
            print(
                f"error: {phase.wrong_values} of {phase.reads} read-amplification reads"
                f" {phase.label} returned a value other than the one this run wrote",
                file=sys.stderr,
            )
            status = 1
        if phase.served_from_memory:
            print(
                f"error: {phase.served_from_memory} of {phase.reads} read-amplification"
                f" reads {phase.label} were answered from memory, so they searched no"
                " table and the reported figure understates the real one",
                file=sys.stderr,
            )
            status = 1
    if not comparison.ran_a_compaction:
        print(
            "warning: no tier was ready to merge, so the before and after figures describe"
            " the same tables; lower --memtable-threshold-bytes or raise --records",
            file=sys.stderr,
        )
    return status


if __name__ == "__main__":
    sys.exit(main())
