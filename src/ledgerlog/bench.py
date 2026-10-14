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

Why the read phase drains pending flushes first, unless told not to: a read
phase run while the background flusher is still working measures a tree that is
changing underneath it, so the same command would report a different latency
depending on how the thread happened to be scheduled. Draining first makes the
run repeatable and makes the reported figure attributable to a settled set of
tables. ``--no-drain-flushes`` keeps the other case available, since reading
against an in-flight flush is a real workload and worth being able to measure.

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
import random
import statistics
import sys
import tempfile
import time
from collections.abc import Sequence
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path

from ledgerlog.engine import DEFAULT_MEMTABLE_THRESHOLD_BYTES, LedgerLog
from ledgerlog.wal import DEFAULT_FSYNC_INTERVAL_SECONDS, FsyncPolicy

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

FLUSH_DRAIN_TIMEOUT_SECONDS = 300.0
"""How long the read phase waits for pending flushes before giving up.

Generous, since a drain at a large record count has real SSTables to write, and
bounded all the same: the one state that never clears on its own is a flush that
keeps failing, and against that an unbounded wait is a command that hangs with
nothing printed instead of a command that says what went wrong.
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


def draw_read_indices(records: int, read_count: int, seed: int) -> list[int]:
    """Return the record indices the read phase will visit, in visit order.

    Drawn up front and returned as a list so that the read phase's timed region
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
    """
    rng = random.Random(seed)
    return [rng.randrange(records) for _ in range(read_count)]


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
        different numbers of samples.
        """
        return self.records if self.reads is None else self.reads

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
        different directory on the same disk measures the same thing.
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
    def megabytes_per_second(self) -> float:
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
            "records_per_second": self.records_per_second,
            "mib_per_second": self.megabytes_per_second,
        }


@dataclass(frozen=True)
class ReadPhaseResult:
    """What the randomized read phase measured.

    ``misses`` counts reads that came back absent. It is reported rather than
    merely asserted because every key the read phase asks for was written by the
    write phase, so a miss is a correctness failure in the engine and not a
    property of the workload. A bench run that quietly reported a fast latency
    over reads that found nothing would be worse than no bench at all.
    """

    reads: int
    hits: int
    misses: int
    elapsed_seconds: float
    sstables_consulted_from: int
    latency: LatencySummary

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
            "sstables_at_read_time": self.sstables_consulted_from,
            "latency": self.latency.to_dict(),
        }


@dataclass(frozen=True)
class BenchResult:
    """A complete run: the configuration it ran under and both phases' figures."""

    config: BenchConfig
    write: WritePhaseResult
    read: ReadPhaseResult

    def to_dict(self) -> dict[str, object]:
        """Return the whole run as plain JSON-safe values, for ``--json``."""
        return {
            "command": self.config.command_line(),
            "config": self.config.to_dict(),
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
            "",
            "Sequential write phase",
            f"  records:            {self.write.records}",
            f"  value size:         {self.write.value_size_bytes} B",
            f"  payload written:    {self.write.payload_bytes / (1024 * 1024):.2f} MiB",
            f"  elapsed:            {self.write.elapsed_seconds:.3f} s",
            f"  throughput:         {self.write.records_per_second:,.0f} records/s"
            f" ({self.write.megabytes_per_second:.2f} MiB/s payload)",
            "",
            "Randomized read phase",
            f"  reads:              {self.read.reads}",
            f"  hits / misses:      {self.read.hits} / {self.read.misses}",
            f"  sstables at read:   {self.read.sstables_consulted_from}"
            " (context only, read amplification is story M10.2)",
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


def _run_write_phase(engine: LedgerLog, config: BenchConfig) -> WritePhaseResult:
    """Write ``config.records`` records in ascending key order and time the whole phase.

    Wall clock across the phase rather than a sum of per-call times, because the
    engine is allowed to do work between calls (freezing a full memtable, handing
    it to the flush thread) and that work is part of what writing N records costs.
    A sum of per-call times would also pay for a timer call per record, which at
    this phase's rate is a measurable share of what is being measured.
    """
    value = b"v" * config.value_size_bytes
    records = config.records
    # Computed rather than accumulated in the loop, so the timed region holds the
    # key formatting and the put and nothing else. It is exact rather than an
    # estimate because every key is the same length, which
    # BenchConfig.__post_init__ enforces by capping the record count at what the
    # padding width can hold.
    payload_bytes = records * (len(KEY_PREFIX) + KEY_INDEX_DIGITS + config.value_size_bytes)

    start = time.perf_counter_ns()
    for index in range(records):
        engine.put(key_for_index(index), value)
    elapsed_ns = time.perf_counter_ns() - start

    return WritePhaseResult(
        records=records,
        value_size_bytes=config.value_size_bytes,
        payload_bytes=payload_bytes,
        elapsed_seconds=elapsed_ns / 1e9,
    )


def _run_read_phase(engine: LedgerLog, config: BenchConfig) -> ReadPhaseResult:
    """Read randomly chosen written keys and summarize the per-read latencies.

    The index sequence comes from :func:`draw_read_indices` before the timing
    starts, so that the random number generator's own cost is not charged to the
    engine. See that function for why the draw is with replacement and why it
    carries its own generator.

    The table count is sampled before the loop rather than after, so the figure
    describes the tree the reads were served from. With the background flusher
    drained (the default) the two are the same, but under
    ``--no-drain-flushes`` a flush can land mid-phase, and the count that makes
    the latency interpretable is the one the phase started against.
    """
    read_count = config.read_count
    indices = draw_read_indices(config.records, read_count, config.seed)

    latencies_ns: list[int] = []
    hits = 0
    sstables_at_read_time = len(engine.sstables)

    phase_start = time.perf_counter_ns()
    for index in indices:
        key = key_for_index(index)
        start = time.perf_counter_ns()
        value = engine.get(key)
        latencies_ns.append(time.perf_counter_ns() - start)
        if value is not None:
            hits += 1
    phase_elapsed_ns = time.perf_counter_ns() - phase_start

    return ReadPhaseResult(
        reads=read_count,
        hits=hits,
        misses=read_count - hits,
        elapsed_seconds=phase_elapsed_ns / 1e9,
        sstables_consulted_from=sstables_at_read_time,
        latency=LatencySummary.from_samples(latencies_ns),
    )


def run_bench(config: BenchConfig) -> BenchResult:
    """Run both phases against a fresh engine and return the measured figures.

    An :class:`~contextlib.ExitStack` owns both the temporary directory and the
    engine, so a failure anywhere in either phase still closes the engine and
    still removes the directory. That matters more for a benchmark than it looks:
    the fsync-always runs this harness exists to measure leave a log file behind
    per run, and a harness that leaked one per crash would fill a disk before
    anybody noticed.

    When ``config.directory`` is given it is used as it stands and is not removed,
    because a caller who names a directory wants the files afterwards. When it is
    not, the run happens in a temporary directory that is removed on the way out:
    a benchmark that left its data in the working tree would be measuring a
    different disk each time depending on where it was started from.
    """
    with ExitStack() as stack:
        if config.directory is None:
            directory = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="ledgerlog-")))
        else:
            directory = config.directory
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

        if config.drain_flushes:
            # Flush on this thread first so the drain cannot sit waiting on a
            # background thread that is between wakeups, then wait for anything
            # the flusher had already picked up. See the module docstring for why
            # the read phase wants a settled set of tables.
            engine.flush_pending()
            if not engine.wait_for_flush(timeout=FLUSH_DRAIN_TIMEOUT_SECONDS):
                # Bounded rather than an open-ended wait: a flush that keeps
                # failing never clears the frozen table, and wait_for_flush
                # reports that as a timeout rather than raising, so an unbounded
                # wait here would hang the command with nothing printed. Raised
                # rather than measured around, because a flush that cannot
                # complete makes the read phase's figure a measurement of a
                # broken engine.
                raise RuntimeError(
                    "timed out after "
                    f"{FLUSH_DRAIN_TIMEOUT_SECONDS}s waiting for flushes to drain; "
                    f"last flush error: {engine.flush_error!r}"
                )

        read = _run_read_phase(engine, config)

    return BenchResult(config=config, write=write, read=read)


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
        help="number of randomized reads (default: one per record written)",
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
        help="directory to run in and keep (default: a temporary directory, removed afterwards)",
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

    A run whose read phase missed any key exits non-zero as well, because every
    key it asks for was written in the same run. See :class:`ReadPhaseResult` for
    why that is treated as a failure rather than a footnote: the alternative is a
    harness that reports an excellent latency for reads that found nothing.
    """
    args = build_parser().parse_args(argv)
    try:
        config = config_from_args(args)
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    result = run_bench(config)

    if args.emit_json:
        print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    else:
        print(result.format_report())

    if result.read.misses:
        print(
            f"error: {result.read.misses} of {result.read.reads} reads found no value"
            " for a key this run wrote",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
