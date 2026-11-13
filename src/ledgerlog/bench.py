"""Benchmark harness: what this engine actually does, measured rather than claimed.

Scope of this module today (story M10.1): a sequential write phase that reports
throughput, a randomized read phase that reports a latency distribution, and the
CLI arguments that make the record count, the value size and the fsync policy
part of the measurement instead of baked into it. Run it with::

    python -m ledgerlog.bench --records 100000

Read amplification, meaning how many SSTables a single ``get`` consults, is
deliberately not measured here. Counting that needs the read path itself
instrumented, which is story M10.2, and a figure guessed from the table count
would be exactly the kind of estimate this module exists to replace. What is
printed below is the number of tables the engine was serving when the read phase
ran, which is context for reading the latency figure, not a measurement of the
read path.

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
import random
import statistics
import sys
import tempfile
import time
from array import array
from collections.abc import Sequence
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path

from ledgerlog.engine import DEFAULT_MEMTABLE_THRESHOLD_BYTES, LedgerLog
from ledgerlog.sstable import MAX_RECORD_PAYLOAD_SIZE, RECORD_PAYLOAD_HEADER_SIZE
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
    seed: int = 0
    directory: Path | None = None
    drain_flushes: bool = True

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
            "seed": self.seed,
            "drain_flushes": self.drain_flushes,
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
        """
        parts = [
            "python -m ledgerlog.bench",
            f"--records {self.records}",
            f"--value-size {self.value_size_bytes}",
            f"--reads {self.read_count}",
            f"--fsync-policy {self.fsync_policy.value}",
            f"--memtable-threshold-bytes {self.memtable_threshold_bytes}",
            f"--seed {self.seed}",
        ]
        if self.fsync_policy is FsyncPolicy.INTERVAL:
            parts.append(f"--fsync-interval {self.fsync_interval_seconds}")
        if not self.drain_flushes:
            parts.append("--no-drain-flushes")
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
    tree the phase started against. They are context for the latency, not a
    count of tables any read consulted: that is read amplification, story M10.2.
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
class BenchResult:
    """A complete run: the configuration it ran under and both phases' figures."""

    config: BenchConfig
    write: WritePhaseResult
    read: ReadPhaseResult
    filesystem: str | None = None
    """Type of the filesystem the run directory sat on, or ``None`` if unknown."""

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
            " (context only, read amplification is story M10.2)",
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
        return "\n".join(lines)


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
            )
        )

        write = _run_write_phase(engine, config)
        read = _run_read_phase(engine, config)
        filesystem = filesystem_type(directory.resolve())

    return BenchResult(config=config, write=write, read=read, filesystem=filesystem)


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
        seed=args.seed,
        directory=args.directory,
        drain_flushes=args.drain_flushes,
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
    return status


if __name__ == "__main__":
    sys.exit(main())
