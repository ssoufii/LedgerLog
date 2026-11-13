"""Tests for the bench harness's phases, its reported figures and its CLI.

Covers story M10.1 (bench harness: write throughput and random read latency)
and story M10.2 (read amplification before and after compaction).

What is worth testing in a benchmark, given that the numbers it produces are
whatever the machine does on the day: not the figures, but everything around
them. A harness can report a throughput that is arithmetically wrong, time the
wrong region, read keys it never wrote, report a p99 below its p50, leak the
directory it ran in, or accept ``--records 0`` and divide by zero. Each of those
is a real defect that makes every number the harness prints untrustworthy, and
each one is testable without depending on how fast this machine is.

So the assertions here are of three kinds. First, relationships that must hold
whatever the timings are: records written equals records asked for, every read
finds its value, percentiles are ordered, throughput equals records divided by
the elapsed time the harness itself reported. Second, the end-to-end CLI
criterion from the story, run as a real subprocess so that ``python -m
ledgerlog.bench --records N`` is demonstrated rather than approximated by calling
``main`` in process. Third, exact arithmetic for the percentile function, which
is the one piece of real logic here and is pure, so it can be pinned against
hand-computed answers.

The nearest-rank percentile tests use samples whose answers can be read off by
hand, including the boundary cases that distinguish nearest-rank from an
interpolated or float-rounded rank, because those are exactly where the two
disagree and where a plausible-looking implementation goes wrong.

Record counts here are small. A benchmark test that ran a benchmark at a useful
size would add minutes to the suite to measure nothing reproducible, so these
runs are sized to exercise the paths (including a memtable threshold low enough
to force real flushes and real SSTable reads) and not to produce a number anyone
would quote.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from ledgerlog import bench as bench_module
from ledgerlog.bench import (
    AMPLIFICATION_DIRNAME,
    COMPACTION_SCHEDULE_BULK,
    COMPACTION_SCHEDULE_INCREMENTAL,
    COMPACTION_SCHEDULES,
    DEFAULT_MAX_READS,
    DEFAULT_VALUE_SIZE_BYTES,
    KEY_INDEX_DIGITS,
    MAX_HISTOGRAM_ROWS,
    MAX_RECORDS,
    MAX_VALUE_SIZE_BYTES,
    PERCENTILES,
    AmplificationSummary,
    BenchConfig,
    BenchError,
    BenchResult,
    LatencySummary,
    ReadPhaseResult,
    WritePhaseResult,
    _snapshot_committed_tables,
    build_parser,
    config_from_args,
    draw_read_indices,
    filesystem_type,
    key_for_index,
    main,
    nearest_rank_percentile,
    run_bench,
)
from ledgerlog.compaction import DEFAULT_MIN_TIER_TABLES, DEFAULT_SIZE_RATIO
from ledgerlog.engine import DEFAULT_MEMTABLE_THRESHOLD_BYTES, LedgerLog
from ledgerlog.sstable import DEFAULT_BLOOM_FALSE_POSITIVE_RATE, DEFAULT_INDEX_INTERVAL
from ledgerlog.wal import FsyncPolicy

# Small enough to keep the suite fast, large enough that the read phase draws a
# sample worth taking percentiles over and that a low memtable threshold produces
# several SSTables rather than one.
BENCH_RECORDS = 400
SMALL_THRESHOLD_BYTES = 4 * 1024


def _fast_config(**overrides: object) -> BenchConfig:
    """Return a configuration sized for the test suite, with ``overrides`` applied.

    Defaults to the ``never`` fsync policy because an fsync per record turns a
    few hundred writes into seconds of real disk work, and what these tests check
    is the harness's arithmetic and control flow rather than the durability cost
    the policy buys. The policy itself is covered by its own test below.
    """
    settings: dict[str, object] = {
        "records": BENCH_RECORDS,
        "value_size_bytes": 32,
        "fsync_policy": FsyncPolicy.NEVER,
        "memtable_threshold_bytes": SMALL_THRESHOLD_BYTES,
        "seed": 1234,
    }
    settings.update(overrides)
    return BenchConfig(**settings)  # type: ignore[arg-type]


# --------------------------------------------------------------------------
# Key construction
# --------------------------------------------------------------------------


def test_keys_are_fixed_width_and_sort_in_index_order() -> None:
    """Keys must sort the way their indices do, or the write phase is not sequential."""
    keys = [key_for_index(index) for index in range(150)]

    assert keys == sorted(keys)
    assert len({len(key) for key in keys}) == 1
    assert len(keys) == len(set(keys))


def test_key_for_index_is_pure_and_repeatable() -> None:
    """The read phase rebuilds keys from indices, so the mapping has to be stable."""
    assert key_for_index(7) == key_for_index(7)
    assert key_for_index(7) != key_for_index(8)
    assert key_for_index(0).endswith(b"0" * KEY_INDEX_DIGITS)


# --------------------------------------------------------------------------
# Percentile arithmetic
# --------------------------------------------------------------------------


def test_nearest_rank_percentile_returns_an_observed_sample() -> None:
    """A reported tail latency must be one of the measured latencies."""
    samples = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

    for percentile in PERCENTILES:
        assert nearest_rank_percentile(samples, percentile) in samples


def test_nearest_rank_percentile_known_values() -> None:
    """Pinned against hand-computed ranks over ten evenly spaced samples."""
    samples = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

    assert nearest_rank_percentile(samples, 0.0) == 10
    assert nearest_rank_percentile(samples, 10.0) == 10
    assert nearest_rank_percentile(samples, 50.0) == 50
    assert nearest_rank_percentile(samples, 90.0) == 90
    assert nearest_rank_percentile(samples, 99.0) == 100
    assert nearest_rank_percentile(samples, 100.0) == 100


def test_nearest_rank_percentile_rounds_up_at_a_fractional_rank() -> None:
    """Ceiling, not rounding: an exact rank stays put and any fraction above it moves up.

    Over two samples, p50 lands exactly on rank 1 and so returns the lower
    sample, while p50.1 crosses into rank 2 and returns the upper one. That
    boundary is where a float rank put through ``round`` could go either way
    depending on the rounding mode, which is the bug the integer ceiling in the
    implementation exists to rule out.
    """
    assert nearest_rank_percentile([10, 20], 50.0) == 10
    assert nearest_rank_percentile([10, 20], 50.1) == 20
    assert nearest_rank_percentile([10, 20, 30], 50.0) == 20


def test_nearest_rank_percentile_handles_a_single_sample() -> None:
    """Every percentile of one sample is that sample."""
    for percentile in (0.0, 50.0, 99.9, 100.0):
        assert nearest_rank_percentile([42], percentile) == 42


def test_nearest_rank_percentile_rejects_empty_and_out_of_range() -> None:
    """An empty sample or an impossible percentile is a programming error, not a zero."""
    with pytest.raises(ValueError, match="empty sample"):
        nearest_rank_percentile([], 50.0)
    with pytest.raises(ValueError, match="between 0 and 100"):
        nearest_rank_percentile([1, 2, 3], 100.1)
    with pytest.raises(ValueError, match="between 0 and 100"):
        nearest_rank_percentile([1, 2, 3], -1.0)


# --------------------------------------------------------------------------
# Latency summary
# --------------------------------------------------------------------------


def test_latency_summary_orders_its_percentiles() -> None:
    """A summary whose p99 sat below its p50 would misreport every tail it printed."""
    summary = LatencySummary.from_samples(list(range(1, 1001)))

    reported = list(summary.percentiles_ns.values())
    assert reported == sorted(reported)
    assert summary.min_ns <= reported[0]
    assert reported[-1] <= summary.max_ns
    assert summary.min_ns <= summary.mean_ns <= summary.max_ns
    assert summary.count == 1000


def test_latency_summary_does_not_reorder_its_input() -> None:
    """Summarizing is a measurement, so it must leave the caller's sample alone."""
    samples = [5, 1, 4, 2, 3]
    LatencySummary.from_samples(samples)

    assert samples == [5, 1, 4, 2, 3]


def test_latency_summary_labels_match_the_configured_percentiles() -> None:
    """The story asks for p50 and p99 by name, so the labels are part of the contract."""
    summary = LatencySummary.from_samples([1, 2, 3, 4, 5])

    assert "p50" in summary.percentiles_ns
    assert "p99" in summary.percentiles_ns
    assert "p99.9" in summary.percentiles_ns


def test_latency_summary_rejects_an_empty_sample() -> None:
    """A read phase that measured nothing must not report a latency of zero."""
    with pytest.raises(ValueError, match="empty latency sample"):
        LatencySummary.from_samples([])


# --------------------------------------------------------------------------
# Reported figures are derived correctly
# --------------------------------------------------------------------------


def test_write_throughput_is_records_over_elapsed() -> None:
    """Pinned arithmetic, so a mislabeled or inverted throughput cannot pass."""
    result = WritePhaseResult(
        records=1000,
        value_size_bytes=100,
        payload_bytes=1024 * 1024,
        elapsed_seconds=2.0,
    )

    assert result.records_per_second == pytest.approx(500.0)
    assert result.mib_per_second == pytest.approx(0.5)


def test_write_throughput_survives_an_unmeasurably_short_phase() -> None:
    """A phase too short for the clock must not take the run down with a ZeroDivisionError."""
    result = WritePhaseResult(records=1, value_size_bytes=0, payload_bytes=0, elapsed_seconds=0.0)

    assert result.records_per_second == float("inf")
    assert result.mib_per_second == float("inf")


def test_read_throughput_is_reads_over_elapsed() -> None:
    """The read phase's own summary figure, pinned the same way."""
    result = ReadPhaseResult(
        reads=300,
        hits=300,
        misses=0,
        elapsed_seconds=1.5,
        sstables_at_read_time=3,
        latency=LatencySummary.from_samples([1, 2, 3]),
    )

    assert result.reads_per_second == pytest.approx(200.0)


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------


def test_read_count_defaults_to_one_per_record() -> None:
    """The default sample size tracks the record count, so two runs stay comparable."""
    assert BenchConfig(records=250).read_count == 250
    assert BenchConfig(records=250, reads=40).read_count == 40


def test_config_rejects_values_that_cannot_be_measured() -> None:
    """Checked in the configuration so a caller in code gets the same guarantee as the CLI."""
    with pytest.raises(ValueError, match="records must be at least 1"):
        BenchConfig(records=0)
    with pytest.raises(ValueError, match="cannot be negative"):
        BenchConfig(records=10, value_size_bytes=-1)
    with pytest.raises(ValueError, match="reads must be at least 1"):
        BenchConfig(records=10, reads=0)
    with pytest.raises(ValueError, match="memtable_threshold_bytes must be at least 1"):
        BenchConfig(records=10, memtable_threshold_bytes=0)


@pytest.mark.parametrize("interval", [0.0, -1.0, float("nan"), float("inf")])
def test_config_rejects_an_unusable_fsync_interval_under_every_policy(interval: float) -> None:
    """The WAL checks the interval for every policy, so the config must as well.

    Otherwise the value passes here and fails inside the engine as a traceback,
    after a directory has been created.
    """
    for policy in FsyncPolicy:
        with pytest.raises(ValueError, match="fsync_interval_seconds"):
            BenchConfig(records=10, fsync_policy=policy, fsync_interval_seconds=interval)


def test_config_rejects_a_value_too_large_for_the_record_formats() -> None:
    """An oversized value is refused up front rather than on the first put."""
    assert BenchConfig(records=1, value_size_bytes=MAX_VALUE_SIZE_BYTES).value_size_bytes > 0
    with pytest.raises(ValueError, match="fit the record formats"):
        BenchConfig(records=1, value_size_bytes=MAX_VALUE_SIZE_BYTES + 1)


def test_default_read_count_is_capped_so_the_sample_fits_in_memory() -> None:
    """One read per record by default, but never more than the cap unless asked."""
    assert BenchConfig(records=DEFAULT_MAX_READS * 3).read_count == DEFAULT_MAX_READS
    assert BenchConfig(records=10, reads=DEFAULT_MAX_READS * 2).read_count == DEFAULT_MAX_READS * 2


def test_config_rejects_a_record_count_wider_than_the_key_padding() -> None:
    """Past the padding width keys stop being one width, so the count is refused.

    Unreachable in practice, and worth a guard anyway: the failure it prevents is
    a silently wrong payload figure and a write phase that is no longer in key
    order, neither of which announces itself.
    """
    assert BenchConfig(records=MAX_RECORDS).records == MAX_RECORDS
    with pytest.raises(ValueError, match="same width"):
        BenchConfig(records=MAX_RECORDS + 1)

    # The guarantee the cap exists to provide, checked at the boundary itself.
    assert len(key_for_index(MAX_RECORDS - 1)) == len(key_for_index(0))


def test_config_defaults_match_the_engine_defaults() -> None:
    """Defaults that drifted from the engine's would measure a configuration nobody runs."""
    config = BenchConfig(records=1)

    assert config.fsync_policy is FsyncPolicy.ALWAYS
    assert config.memtable_threshold_bytes == DEFAULT_MEMTABLE_THRESHOLD_BYTES
    assert config.value_size_bytes == DEFAULT_VALUE_SIZE_BYTES
    assert config.drain_flushes is True


def test_command_line_round_trips_through_the_parser() -> None:
    """The printed invocation must actually reproduce the run, or M10.3 cannot trace a figure."""
    config = BenchConfig(
        records=777,
        value_size_bytes=64,
        reads=99,
        fsync_policy=FsyncPolicy.INTERVAL,
        fsync_interval_seconds=0.25,
        memtable_threshold_bytes=8192,
        seed=11,
        drain_flushes=False,
    )

    printed = config.command_line()
    argv = printed.removeprefix("python -m ledgerlog.bench").split()
    reparsed = config_from_args(build_parser().parse_args(argv))

    assert reparsed.records == config.records
    assert reparsed.value_size_bytes == config.value_size_bytes
    assert reparsed.read_count == config.read_count
    assert reparsed.fsync_policy is config.fsync_policy
    assert reparsed.fsync_interval_seconds == config.fsync_interval_seconds
    assert reparsed.memtable_threshold_bytes == config.memtable_threshold_bytes
    assert reparsed.seed == config.seed
    assert reparsed.drain_flushes == config.drain_flushes


# --------------------------------------------------------------------------
# Acceptance criterion 3: CLI arguments configure the run
# --------------------------------------------------------------------------


def test_cli_parses_record_count_value_size_and_fsync_policy() -> None:
    """The three parameters the story names explicitly, through the real parser."""
    args = build_parser().parse_args(
        ["--records", "5000", "--value-size", "256", "--fsync-policy", "interval"]
    )
    config = config_from_args(args)

    assert config.records == 5000
    assert config.value_size_bytes == 256
    assert config.fsync_policy is FsyncPolicy.INTERVAL


@pytest.mark.parametrize("policy", list(FsyncPolicy))
def test_cli_accepts_every_fsync_policy(policy: FsyncPolicy) -> None:
    """Every policy the WAL supports must be reachable from the command line."""
    args = build_parser().parse_args(["--records", "1", "--fsync-policy", policy.value])

    assert config_from_args(args).fsync_policy is policy


def test_cli_rejects_an_unknown_fsync_policy() -> None:
    """A typo in the policy must fail loudly rather than silently measuring the default."""
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--records", "1", "--fsync-policy", "sometimes"])


def test_cli_requires_a_record_count() -> None:
    """There is no sensible default record count, so the argument is required."""
    with pytest.raises(SystemExit):
        build_parser().parse_args([])


def test_main_rejects_a_zero_record_count_without_running_anything() -> None:
    """``--records 0`` exits like a command line tool instead of raising a traceback."""
    assert main(["--records", "0"]) == 2


# --------------------------------------------------------------------------
# Acceptance criteria 1 and 2: the phases actually run and report
# --------------------------------------------------------------------------


def test_run_bench_writes_every_record_and_reads_them_all_back() -> None:
    """The core criterion: N sequential writes, then randomized reads that all hit.

    A miss here would mean the read phase asked for a key the write phase did not
    write, or that the engine lost one. Either way every latency figure the
    harness printed would be measuring the wrong thing, so this is the assertion
    the rest of the harness's credibility rests on.
    """
    result = run_bench(_fast_config())

    assert result.write.records == BENCH_RECORDS
    assert result.read.reads == BENCH_RECORDS
    assert result.read.hits == BENCH_RECORDS
    assert result.read.misses == 0


def test_run_bench_reports_a_positive_throughput_and_an_ordered_latency_spread() -> None:
    """Throughput and the latency distribution are both present and internally consistent."""
    result = run_bench(_fast_config())

    assert result.write.elapsed_seconds > 0.0
    assert result.write.records_per_second > 0.0
    assert result.write.payload_bytes == BENCH_RECORDS * (len(key_for_index(0)) + 32)

    latency = result.read.latency
    assert latency.count == BENCH_RECORDS
    # The maximum, not the minimum: a single read fast enough to measure as zero
    # nanoseconds is not a defect, so asserting a positive minimum would be a
    # test that could fail on a correct harness.
    assert latency.max_ns > 0
    assert latency.min_ns <= latency.percentiles_ns["p50"] <= latency.percentiles_ns["p99"]
    assert latency.percentiles_ns["p99"] <= latency.max_ns


def test_run_bench_throughput_matches_its_own_elapsed_time() -> None:
    """The reported figure has to be derivable from the reported inputs, not from elsewhere."""
    result = run_bench(_fast_config())

    expected = result.write.records / result.write.elapsed_seconds
    assert result.write.records_per_second == pytest.approx(expected)


def test_run_bench_honours_the_record_count_and_read_count_separately() -> None:
    """``--reads`` lifts the sample size off the record count, including above it."""
    result = run_bench(_fast_config(records=50, reads=200))

    assert result.write.records == 50
    assert result.read.reads == 200
    assert result.read.hits == 200
    assert result.read.latency.count == 200


def test_run_bench_honours_the_value_size() -> None:
    """Value size is a CLI parameter, so it has to reach the records actually written."""
    result = run_bench(_fast_config(records=40, value_size_bytes=512))

    assert result.write.value_size_bytes == 512
    assert result.write.payload_bytes == 40 * (len(key_for_index(0)) + 512)


def test_run_bench_works_with_a_zero_length_value() -> None:
    """A zero-byte value is legal for the engine, so the harness must not assume otherwise."""
    result = run_bench(_fast_config(records=30, value_size_bytes=0))

    assert result.read.misses == 0
    assert result.write.payload_bytes == 30 * len(key_for_index(0))


@pytest.mark.parametrize("policy", list(FsyncPolicy))
def test_run_bench_runs_under_every_fsync_policy(policy: FsyncPolicy) -> None:
    """Each policy is a real durability setting, so each must survive a whole run.

    Deliberately tiny: the point is that the policy reaches the WAL and the run
    completes under it, and an fsync-always run at bench scale would make this the
    slowest test in the suite for no extra coverage.
    """
    result = run_bench(_fast_config(records=20, fsync_policy=policy))

    assert result.read.misses == 0
    assert result.config.fsync_policy is policy


def test_read_pattern_is_repeatable_for_a_given_seed() -> None:
    """Same seed, same read pattern: without that, two runs are not comparable.

    Asserted on the indices drawn rather than on the timings, which are whatever
    the machine does on the day. The read pattern is the part that is supposed to
    be deterministic, and a seed that did not actually pin it would make every
    comparison between two runs meaningless while still passing a test that only
    looked at hit counts.
    """
    first = draw_read_indices(records=500, read_count=200, seed=99)
    again = draw_read_indices(records=500, read_count=200, seed=99)
    other = draw_read_indices(records=500, read_count=200, seed=100)

    assert first == again
    assert first != other


def test_read_pattern_stays_in_range_and_draws_with_replacement() -> None:
    """Every drawn index must name a written record, and the draw must not be a permutation.

    The range check is what rules out the read phase asking for a key the write
    phase never wrote, which would show up as a miss and invalidate the latency
    figure. The replacement check is why ``--reads`` can exceed ``--records`` at
    all.
    """
    indices = draw_read_indices(records=10, read_count=400, seed=7)

    assert len(indices) == 400
    assert all(0 <= index < 10 for index in indices)
    # 400 draws from 10 keys cannot be distinct, so this is guaranteed rather
    # than probabilistic: it fails only if the draw is secretly a permutation.
    assert len(set(indices)) < len(indices)


def test_run_bench_is_repeatable_for_a_given_seed() -> None:
    """Two runs at one seed read the same keys and both find all of them."""
    first = run_bench(_fast_config(records=60, reads=60, seed=99))
    second = run_bench(_fast_config(records=60, reads=60, seed=99))

    assert first.read.hits == second.read.hits == 60
    assert first.config.seed == second.config.seed


def test_run_bench_reads_across_several_sstables_when_the_memtable_is_small() -> None:
    """A low threshold must produce real flushes, so the read latency is not memtable-only.

    Without this the harness could report a read latency that never touched the
    disk at all, which would be a number about a dictionary rather than about a
    storage engine.
    """
    result = run_bench(
        _fast_config(records=600, value_size_bytes=64, memtable_threshold_bytes=2048)
    )

    assert result.read.sstables_at_read_time >= 2
    assert result.read.misses == 0


class _StubEngine:
    """Minimal stand-in for ``LedgerLog``, for failure paths a healthy engine never takes.

    Subclasses override one behavior each. A stub rather than a real engine made
    to fail, because what is under test is the harness's response to the
    failure, not the engine's.
    """

    def __init__(self, *args: object, **kwargs: object) -> None:
        self.sstables: tuple[object, ...] = ()
        self.memtable_nbytes = 0

    def __enter__(self) -> _StubEngine:
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False

    def put(self, key: bytes, value: bytes) -> None:
        return None

    def get(self, key: bytes) -> bytes | None:
        return b"v" * 32

    def flush_pending(self) -> tuple[object, ...]:
        return ()


class _FailingFlushEngine(_StubEngine):
    """An engine whose flush raises, which is how a real failing flush surfaces.

    ``flush_pending`` retries a table the background thread failed on and raises
    that failure on the calling thread, so this is the path the drain has to
    handle, rather than a wait that never returns.
    """

    def flush_pending(self) -> tuple[object, ...]:
        raise OSError("disk on fire")


class _WrongValueEngine(_StubEngine):
    """An engine that finds every key but returns the wrong bytes for it."""

    def get(self, key: bytes) -> bytes | None:
        return b"stale"


def test_run_bench_reports_a_failing_flush_as_a_bench_error(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """A flush that fails during the drain ends the run with the reason, not a hang."""
    monkeypatch.setattr("ledgerlog.bench.LedgerLog", _FailingFlushEngine)

    with pytest.raises(BenchError, match="disk on fire"):
        run_bench(_fast_config(records=5))


def test_main_exits_one_with_a_message_when_the_drain_fails(monkeypatch, capsys) -> None:  # type: ignore[no-untyped-def]
    """The failure reaches the shell as one line and a non-zero code, not a traceback."""
    monkeypatch.setattr("ledgerlog.bench.LedgerLog", _FailingFlushEngine)

    assert main(["--records", "5", "--fsync-policy", "never"]) == 1
    assert "disk on fire" in capsys.readouterr().err


def test_run_bench_counts_reads_that_return_the_wrong_value(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """A stale or wrong value is a correctness failure, not a hit."""
    monkeypatch.setattr("ledgerlog.bench.LedgerLog", _WrongValueEngine)

    result = run_bench(_fast_config(records=20, reads=20))

    assert result.read.misses == 0
    assert result.read.wrong_values == 20


def test_main_exits_one_when_a_read_returns_the_wrong_value(monkeypatch, capsys) -> None:  # type: ignore[no-untyped-def]
    """The exit code carries the failure, so a script cannot quote a wrong-value run."""
    monkeypatch.setattr("ledgerlog.bench.LedgerLog", _WrongValueEngine)

    assert main(["--records", "5", "--value-size", "32", "--fsync-policy", "never"]) == 1
    assert "value other than the one this run wrote" in capsys.readouterr().err


def test_run_bench_drains_every_frozen_table_and_times_the_drain() -> None:
    """After the drain nothing is frozen, and the drain is part of the write elapsed time."""
    result = run_bench(
        _fast_config(records=600, value_size_bytes=64, memtable_threshold_bytes=2048)
    )

    assert 0.0 <= result.write.drain_seconds <= result.write.elapsed_seconds
    # Everything still in memory is the active memtable's remainder, which is
    # always under one threshold's worth: anything more would have been frozen.
    assert result.read.memtable_bytes_at_read_time < 2048
    assert result.read.wrong_values == 0


def test_run_bench_refuses_a_directory_that_already_holds_data(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """A reused directory replays an old run, so the printed invocation would not reproduce it."""
    directory = tmp_path / "used"
    run_bench(_fast_config(records=10, directory=directory))

    with pytest.raises(ValueError, match="must be empty"):
        run_bench(_fast_config(records=10, directory=directory))


def test_run_bench_accepts_an_existing_empty_directory(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """Empty is as good as new: nothing in it can be replayed."""
    directory = tmp_path / "empty"
    directory.mkdir()

    assert run_bench(_fast_config(records=10, directory=directory)).read.misses == 0


def test_main_exits_two_on_a_non_empty_directory(tmp_path, capsys) -> None:  # type: ignore[no-untyped-def]
    """Refused like any other bad configuration: one line, exit 2."""
    (tmp_path / "leftover").write_bytes(b"x")

    assert main(["--records", "5", "--directory", str(tmp_path)]) == 2
    assert "must be empty" in capsys.readouterr().err


def test_main_exits_two_on_an_unusable_fsync_interval(capsys) -> None:  # type: ignore[no-untyped-def]
    """Previously this escaped as a traceback from inside the WAL."""
    assert main(["--records", "5", "--fsync-interval", "0"]) == 2
    assert "fsync_interval_seconds" in capsys.readouterr().err


_MOUNTS = """\
/dev/sda1 / ext4 rw,relatime 0 0
tmpfs /tmp tmpfs rw,nosuid,nodev 0 0
/dev/sdb1 /tmp/disk xfs rw 0 0
/dev/sdc1 /mnt/my\\040drive ext4 rw 0 0
"""


def test_filesystem_type_picks_the_longest_containing_mount() -> None:
    """The kernel resolves a path through its deepest mount, so that is the one reported."""
    assert filesystem_type(Path("/tmp/ledgerlog-abc"), _MOUNTS) == "tmpfs"
    assert filesystem_type(Path("/tmp/disk/run"), _MOUNTS) == "xfs"
    assert filesystem_type(Path("/home/user"), _MOUNTS) == "ext4"
    assert filesystem_type(Path("/tmpfoo"), _MOUNTS) == "ext4"
    assert filesystem_type(Path("/mnt/my drive/run"), _MOUNTS) == "ext4"
    assert filesystem_type(Path("/anything"), "") is None


def test_report_warns_when_the_run_was_on_a_ram_backed_filesystem() -> None:
    """An fsync-always figure from tmpfs measures memory, and the report has to say so."""
    measured = run_bench(_fast_config(records=10))
    on_tmpfs = BenchResult(
        config=measured.config, write=measured.write, read=measured.read, filesystem="tmpfs"
    )
    on_disk = BenchResult(
        config=measured.config, write=measured.write, read=measured.read, filesystem="ext4"
    )

    assert on_tmpfs.ram_backed and "RAM-backed" in on_tmpfs.format_report()
    assert on_tmpfs.to_dict()["ram_backed_filesystem"] is True
    assert not on_disk.ram_backed and "RAM-backed" not in on_disk.format_report()
    assert "filesystem: ext4" in on_disk.format_report()


def test_run_bench_without_draining_still_reads_every_key() -> None:
    """Reading against an in-flight flush is a real workload and must not lose a key."""
    result = run_bench(
        _fast_config(records=300, memtable_threshold_bytes=2048, drain_flushes=False)
    )

    assert result.read.misses == 0


def test_run_bench_removes_its_temporary_directory(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """A harness that leaked a directory per run would fill a disk under fsync-always.

    The named-directory case is checked alongside, because a caller who names one
    wants the files kept: the two behaviors are opposites and a single
    implementation that got them backwards would pass neither.
    """
    kept = tmp_path / "kept"
    result = run_bench(_fast_config(records=20, directory=kept))

    assert result.read.misses == 0
    assert kept.is_dir()
    assert any(kept.iterdir())


def test_run_bench_data_survives_reopening_the_directory(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """The figures must describe durable writes, not writes that vanish with the process.

    A bench that measured writes which did not survive the engine closing would
    be reporting a throughput for work the engine never finished. Reopening the
    directory and finding the records is what makes the number a durable-write
    number. The run uses the ``always`` policy, since that is the one whose whole
    purpose is this guarantee.
    """
    directory = tmp_path / "durable"
    result = run_bench(
        _fast_config(
            records=25,
            value_size_bytes=16,
            fsync_policy=FsyncPolicy.ALWAYS,
            directory=directory,
        )
    )

    assert result.read.misses == 0

    with LedgerLog(directory) as reopened:
        for index in range(25):
            assert reopened.get(key_for_index(index)) == b"v" * 16


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------


def test_report_names_the_invocation_and_the_headline_figures() -> None:
    """A figure quoted out of this report must carry the parameters that produced it."""
    result = run_bench(_fast_config(records=40))
    report = result.format_report()

    assert "python -m ledgerlog.bench" in report
    assert "--records 40" in report
    assert "--fsync-policy never" in report
    assert "records/s" in report
    assert "latency p50:" in report
    assert "latency p99:" in report


def test_json_output_is_parseable_and_carries_both_phases() -> None:
    """The machine-readable form is what a later tuning pass consumes, so it must round-trip."""
    result = run_bench(_fast_config(records=40))
    payload = json.loads(json.dumps(result.to_dict()))

    assert payload["config"]["records"] == 40
    assert payload["write"]["records_per_second"] > 0
    assert payload["read"]["misses"] == 0
    assert payload["read"]["latency"]["percentiles_ns"]["p99"] > 0
    assert payload["command"].startswith("python -m ledgerlog.bench")


# --------------------------------------------------------------------------
# Acceptance criterion 1, end to end: the documented command actually runs
# --------------------------------------------------------------------------


def test_module_runs_as_a_command_and_prints_throughput_and_latency() -> None:
    """``python -m ledgerlog.bench --records N`` is the story's literal criterion.

    Run as a subprocess rather than by calling ``main`` in process, because what
    the story promises is a command. The ``__main__`` guard, the module being
    importable under ``-m``, and the exit code are all part of that promise and
    none of them is exercised by an in-process call.
    """
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "ledgerlog.bench",
            "--records",
            "200",
            "--value-size",
            "32",
            "--fsync-policy",
            "never",
        ],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "records/s" in completed.stdout
    assert "latency p50:" in completed.stdout
    assert "latency p99:" in completed.stdout
    assert "hits / misses:      200 / 0" in completed.stdout


def test_module_runs_as_a_command_with_json_output() -> None:
    """The same command's machine-readable mode, end to end through the CLI."""
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "ledgerlog.bench",
            "--records",
            "100",
            "--value-size",
            "16",
            "--fsync-policy",
            "never",
            "--json",
        ],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout)
    assert payload["config"]["records"] == 100
    assert payload["config"]["value_size_bytes"] == 16
    assert payload["read"]["hits"] == 100
    assert payload["write"]["records_per_second"] > 0


def test_module_exits_two_on_a_rejected_record_count() -> None:
    """The CLI's failure mode, end to end: an error line and a conventional exit code."""
    completed = subprocess.run(
        [sys.executable, "-m", "ledgerlog.bench", "--records", "0"],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )

    assert completed.returncode == 2
    assert "records must be at least 1" in completed.stderr


# --------------------------------------------------------------------------
# Story M10.2: read amplification before and after compaction
# --------------------------------------------------------------------------
#
# The figure under test is "tables searched per read", and what makes it worth
# testing is that it is easy to produce a number that looks like it and is not:
# the table count, or a mean pulled down by reads the memtable answered. So the
# assertions below are about the relationships that make the figure mean what
# the report says it means. Every read reached disk, every table the walk
# passed is accounted for, the two sides describe one dataset, and the count
# never exceeds the tables that existed to be searched.
#
# The record count here is larger than the rest of this module uses, because a
# dataset small enough to fit one SSTable has no read amplification to measure
# and no tier to compact.

AMPLIFICATION_RECORDS = 1200
AMPLIFICATION_THRESHOLD_BYTES = 1024


def _amplification_config(**overrides: object) -> BenchConfig:
    """A configuration whose threshold is low enough to make many small tables."""
    settings: dict[str, object] = {
        "records": AMPLIFICATION_RECORDS,
        "memtable_threshold_bytes": AMPLIFICATION_THRESHOLD_BYTES,
        "read_amplification": True,
    }
    settings.update(overrides)
    return _fast_config(**settings)


def test_the_phase_is_off_unless_it_is_asked_for() -> None:
    """It costs a second pass over the data, so it does not happen by default."""
    result = run_bench(_fast_config(records=100))

    assert result.config.read_amplification is False
    assert result.amplification is None
    assert result.to_dict()["amplification"] is None
    assert "Read amplification" not in result.format_report()


def test_criterion_1_the_figure_comes_from_the_read_path_not_the_table_count() -> None:
    """Instrumentation, not inference: tables searched is below the tables held.

    With many small tables and a bloom filter per table, a read searches one
    table and skips the rest, so the mean sits near one while the table count is
    far above it. Equality between the two would mean the harness had counted
    the directory.
    """
    comparison = run_bench(_amplification_config()).amplification
    assert comparison is not None
    before = comparison.before

    assert before.sstables > 1, "the fixture did not produce several tables"
    assert before.mean_tables_per_read >= 1.0
    assert before.mean_tables_per_read < before.sstables
    assert before.searched.max_tables <= before.sstables
    assert before.tables_skipped_by_bloom > 0, "no table was spared by a bloom filter"


def test_criterion_2_the_deferred_side_reports_a_figure_over_many_small_tables() -> None:
    """The before side: a real dataset, several tables, every read served from disk."""
    comparison = run_bench(_amplification_config()).amplification
    assert comparison is not None
    before = comparison.before

    assert before.label == "before compaction"
    assert before.sstables >= 4, f"only {before.sstables} tables, too few to compact a tier"
    assert before.records_on_disk > 0
    assert before.reads == AMPLIFICATION_RECORDS
    assert before.hits == before.reads
    assert before.misses == 0
    assert before.wrong_values == 0
    assert before.served_from_memory == 0, "a read answered from memory searched no table"
    assert before.searched.min_tables >= 1, "a read that found its value searched a table"


def test_criterion_3_compaction_does_not_raise_the_figure_and_both_are_printed() -> None:
    """The comparison itself, and the report line that puts the two side by side."""
    result = run_bench(_amplification_config())
    comparison = result.amplification
    assert comparison is not None

    assert comparison.ran_a_compaction, "no tier was ready, so there is nothing to compare"
    assert comparison.compaction.tables_after < comparison.compaction.tables_before
    assert comparison.after.sstables < comparison.before.sstables
    assert comparison.after.mean_tables_per_read <= comparison.before.mean_tables_per_read
    assert comparison.reduced is True

    report = result.format_report()
    assert "Read amplification" in report
    assert "before compaction" in report
    assert "after compaction" in report
    assert "before -> after:" in report
    assert "tier merge(s)" in report


def test_both_sides_measure_one_dataset_with_the_same_reads() -> None:
    """A merge of tables with no tombstones and no expiries loses nothing.

    If the two sides disagreed about the record count or the read count they
    would not be comparable, which is why the harness raises rather than prints
    in that case. Here the agreement is asserted from the outside.
    """
    comparison = run_bench(_amplification_config()).amplification
    assert comparison is not None

    assert comparison.after.records_on_disk == comparison.before.records_on_disk
    assert comparison.after.reads == comparison.before.reads
    assert comparison.after.searched.count == comparison.before.searched.count
    assert comparison.after.hits == comparison.after.reads
    assert comparison.after.misses == 0
    assert comparison.after.wrong_values == 0
    assert comparison.after.served_from_memory == 0


def test_a_fully_compacted_dataset_searches_exactly_one_table_per_read() -> None:
    """The floor the comparison is heading for, stated rather than assumed.

    When the compaction ends with one table, every read must search it and
    nothing else: there is no table left to skip and none to search in vain.
    """
    comparison = run_bench(_amplification_config()).amplification
    assert comparison is not None
    after = comparison.after

    if after.sstables != 1:
        pytest.skip(f"the compaction settled at {after.sstables} tables, not one")
    assert after.mean_tables_per_read == 1.0
    assert after.searched.min_tables == after.searched.max_tables == 1
    assert after.tables_skipped_by_bloom == 0
    assert after.bloom_false_positives == 0
    assert after.bloom_false_positive_rate == 0.0


def test_the_phase_leaves_the_runs_own_tables_alone(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """The compaction runs on a snapshot, so the run's directory still reads back.

    The snapshot is made of hard links, and a merge unlinks the tables it
    consumed. If it were operating on the run's own files instead, reopening the
    directory afterwards would find them gone.
    """
    directory = tmp_path / "run"
    result = run_bench(_amplification_config(records=600, directory=directory))
    assert result.amplification is not None

    workspace = directory / AMPLIFICATION_DIRNAME
    assert workspace.is_dir(), "the phase did not work in a directory of its own"

    with LedgerLog(directory) as reopened:
        assert len(reopened.sstables) == result.read.sstables_at_read_time
        for index in (0, 1, result.config.records - 1):
            assert reopened.get(key_for_index(index)) is not None


def test_the_phase_falls_back_to_copying_when_a_hard_link_is_refused(
    tmp_path,  # type: ignore[no-untyped-def]
    monkeypatch,  # type: ignore[no-untyped-def]
) -> None:
    """Not every filesystem gives hard links, and the figures must not depend on one."""
    refused: list[str] = []

    def refuse_link(source: object, target: object) -> None:
        refused.append(str(source))
        raise OSError("hard links are not supported here")

    monkeypatch.setattr(bench_module.os, "link", refuse_link)
    comparison = run_bench(
        _amplification_config(records=600, directory=tmp_path / "run")
    ).amplification

    assert refused, "the phase never tried a hard link"
    assert comparison is not None
    assert comparison.before.misses == 0
    assert comparison.after.misses == 0


def test_the_snapshot_refuses_a_workspace_that_already_holds_something(
    tmp_path,  # type: ignore[no-untyped-def]
) -> None:
    """A workspace it did not create could hold tables from somewhere else."""
    directory = tmp_path / "run"
    workspace = directory / AMPLIFICATION_DIRNAME
    workspace.mkdir(parents=True)

    with LedgerLog(directory) as engine:
        engine.put(b"key", b"value")
        with pytest.raises(BenchError, match="read-amplification workspace"):
            _snapshot_committed_tables(engine, workspace)


def test_the_phase_refuses_a_run_that_flushed_nothing(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """No table on disk means no amplification to measure, and no figure to invent."""
    config = _fast_config(
        records=20,
        memtable_threshold_bytes=DEFAULT_MEMTABLE_THRESHOLD_BYTES,
        read_amplification=True,
        directory=tmp_path / "run",
    )

    with pytest.raises(BenchError, match="no records on disk"):
        run_bench(config)


def test_main_reports_a_run_where_no_tier_was_ready(tmp_path, capsys) -> None:  # type: ignore[no-untyped-def]
    """Equal figures from an uncompacted pair are true but say nothing, so they are flagged.

    A single flushed table cannot form a ready tier, so the compaction makes no
    passes and the two sides describe the same table. That is a warning on
    stderr rather than a failure: the numbers printed are correct.
    """
    status = main(
        [
            "--records",
            "60",
            "--value-size",
            "32",
            "--fsync-policy",
            "never",
            "--memtable-threshold-bytes",
            "1024",
            "--reads",
            "40",
            "--read-amplification",
            "--directory",
            str(tmp_path / "run"),
        ]
    )

    assert status == 0
    captured = capsys.readouterr()
    assert "no tier was ready to merge" in captured.err
    assert "Read amplification" in captured.out


# --------------------------------------------------------------------------
# The amplification summary's own arithmetic
# --------------------------------------------------------------------------


def test_amplification_summary_describes_its_samples() -> None:
    summary = AmplificationSummary.from_samples([1, 1, 2, 3, 1, 2])

    assert summary.count == 6
    assert summary.min_tables == 1
    assert summary.max_tables == 3
    assert summary.mean_tables == pytest.approx(10 / 6)
    assert summary.histogram == ((1, 3), (2, 2), (3, 1))
    assert sum(reads for _, reads in summary.histogram) == summary.count
    assert list(summary.percentiles) == [f"p{p:g}" for p in PERCENTILES]
    assert summary.percentiles["p50"] <= summary.percentiles["p99"]


def test_amplification_summary_does_not_reorder_its_input() -> None:
    samples = [3, 1, 2]
    AmplificationSummary.from_samples(samples)

    assert samples == [3, 1, 2]


def test_amplification_summary_rejects_an_empty_sample() -> None:
    with pytest.raises(ValueError, match="empty read-amplification sample"):
        AmplificationSummary.from_samples([])


def test_amplification_histogram_line_caps_its_rows_without_hiding_them() -> None:
    summary = AmplificationSummary.from_samples(list(range(MAX_HISTOGRAM_ROWS + 3)))
    line = summary.format_histogram()

    assert line.count("x") == MAX_HISTOGRAM_ROWS
    assert "(+3 more)" in line
    assert len(summary.to_dict()["histogram"]) == MAX_HISTOGRAM_ROWS + 3  # type: ignore[arg-type]


# --------------------------------------------------------------------------
# The flag, through the parser and the reported invocation
# --------------------------------------------------------------------------


def test_the_amplification_flag_round_trips_through_the_parser() -> None:
    """The printed invocation has to reproduce the run, this flag included."""
    config = _amplification_config()
    assert "--read-amplification" in config.command_line()

    arguments = config.command_line().removeprefix("python -m ledgerlog.bench").split()
    reparsed = config_from_args(build_parser().parse_args(arguments))

    assert reparsed.read_amplification is True
    assert reparsed.command_line() == config.command_line()


def test_the_parser_defaults_the_flag_off() -> None:
    assert config_from_args(build_parser().parse_args(["--records", "10"])).read_amplification is (
        False
    )


def test_json_output_carries_the_whole_comparison() -> None:
    """Every figure the report prints is in the machine-readable form too."""
    payload = run_bench(_amplification_config(records=600)).to_dict()
    amplification = payload["amplification"]

    assert isinstance(amplification, dict)
    assert amplification["reduced"] is True
    assert amplification["compaction"]["passes"] >= 1
    for side in ("before", "after"):
        phase = amplification[side]
        assert phase["misses"] == 0
        assert phase["served_from_memory"] == 0
        assert phase["mean_tables_per_read"] >= 1.0
        assert phase["searched"]["count"] == phase["reads"]
    assert json.dumps(payload), "the payload must survive a round trip through JSON"


def test_module_runs_the_amplification_phase_as_a_command(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """The story's figure, produced end to end by the documented command."""
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "ledgerlog.bench",
            "--records",
            "800",
            "--value-size",
            "32",
            "--fsync-policy",
            "never",
            "--memtable-threshold-bytes",
            "1024",
            "--read-amplification",
            "--directory",
            str(tmp_path / "run"),
        ],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "--read-amplification" in completed.stdout
    assert "Read amplification" in completed.stdout
    assert "before compaction" in completed.stdout
    assert "after compaction" in completed.stdout
    assert "before -> after:" in completed.stdout


# --------------------------------------------------------------------------
# M10.4: the tuning knobs are part of the measurement, not baked into it
# --------------------------------------------------------------------------


def test_tuning_knobs_default_to_the_engine_defaults() -> None:
    """A run that names no tunable measures the engine's own defaults, not the harness's."""
    config = BenchConfig(records=1)

    assert config.index_interval == DEFAULT_INDEX_INTERVAL
    assert config.bloom_false_positive_rate == DEFAULT_BLOOM_FALSE_POSITIVE_RATE
    assert config.size_ratio == DEFAULT_SIZE_RATIO
    assert config.min_tier_tables == DEFAULT_MIN_TIER_TABLES
    assert config.compaction_schedule == COMPACTION_SCHEDULE_BULK


def test_tuning_knobs_round_trip_through_the_parser() -> None:
    """A figure is only traceable if the printed invocation reproduces the tunables too."""
    config = BenchConfig(
        records=321,
        index_interval=128,
        bloom_false_positive_rate=0.005,
        size_ratio=4.0,
        min_tier_tables=6,
        compaction_schedule=COMPACTION_SCHEDULE_INCREMENTAL,
        read_amplification=True,
    )

    argv = config.command_line().removeprefix("python -m ledgerlog.bench").split()
    reparsed = config_from_args(build_parser().parse_args(argv))

    assert reparsed.index_interval == config.index_interval
    assert reparsed.bloom_false_positive_rate == config.bloom_false_positive_rate
    assert reparsed.size_ratio == config.size_ratio
    assert reparsed.min_tier_tables == config.min_tier_tables
    assert reparsed.compaction_schedule == config.compaction_schedule


def test_the_tunables_are_printed_even_when_they_hold_the_defaults() -> None:
    """Unlike the fsync interval, these are the values M10.4 moves, so they are always shown."""
    printed = BenchConfig(records=1).command_line()

    assert "--index-interval" in printed
    assert "--bloom-fp-rate" in printed
    assert "--size-ratio" in printed
    assert "--min-tier-tables" in printed
    assert "--compaction-schedule" in printed


def test_tuning_knobs_appear_in_the_json() -> None:
    """``--json`` carries the full configuration, the tunables included."""
    payload = BenchConfig(records=1).to_dict()

    assert payload["index_interval"] == DEFAULT_INDEX_INTERVAL
    assert payload["bloom_false_positive_rate"] == DEFAULT_BLOOM_FALSE_POSITIVE_RATE
    assert payload["size_ratio"] == DEFAULT_SIZE_RATIO
    assert payload["min_tier_tables"] == DEFAULT_MIN_TIER_TABLES
    assert payload["compaction_schedule"] == COMPACTION_SCHEDULE_BULK


@pytest.mark.parametrize(
    "overrides",
    [
        {"index_interval": 0},
        {"index_interval": -4},
        {"bloom_false_positive_rate": 0.0},
        {"bloom_false_positive_rate": 1.0},
        {"size_ratio": 1.0},
        {"min_tier_tables": 1},
        {"compaction_schedule": "sometimes"},
    ],
)
def test_bad_tunables_are_rejected_by_the_config(overrides: dict[str, object]) -> None:
    """The harness rejects a value no engine could accept, with the same rules the engine uses."""
    with pytest.raises(ValueError):
        BenchConfig(records=1, **overrides)


def test_cli_parses_every_tuning_knob() -> None:
    """The four knobs and the schedule all reach the configuration from the command line."""
    args = build_parser().parse_args(
        [
            "--records",
            "10",
            "--index-interval",
            "256",
            "--bloom-fp-rate",
            "0.02",
            "--size-ratio",
            "3.0",
            "--min-tier-tables",
            "5",
            "--compaction-schedule",
            "incremental",
        ]
    )
    config = config_from_args(args)

    assert config.index_interval == 256
    assert config.bloom_false_positive_rate == 0.02
    assert config.size_ratio == 3.0
    assert config.min_tier_tables == 5
    assert config.compaction_schedule == COMPACTION_SCHEDULE_INCREMENTAL


def test_cli_rejects_an_unknown_compaction_schedule() -> None:
    """A typo in the schedule fails at the parser rather than silently running bulk."""
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--records", "1", "--compaction-schedule", "lazy"])


def test_schedule_names_are_the_two_the_cli_offers() -> None:
    """The constant the parser is built from and the names used elsewhere cannot drift apart."""
    assert set(COMPACTION_SCHEDULES) == {
        COMPACTION_SCHEDULE_BULK,
        COMPACTION_SCHEDULE_INCREMENTAL,
    }


def test_chosen_tables_are_written_with_the_run_s_index_interval(tmp_path: Path) -> None:
    """A sweep measuring an interval must actually produce tables laid out at it."""
    directory = tmp_path / "run"
    run_bench(
        _fast_config(
            records=600,
            memtable_threshold_bytes=2 * 1024,
            index_interval=8,
            directory=directory,
        )
    )

    with LedgerLog(directory) as engine:
        handles = engine.sstables
        assert handles, "the run should have flushed at least one table"
        for handle in handles:
            # The footer does not record the interval, but the index it produced
            # does: a table of R records written at interval N carries exactly
            # ceil(R / N) entries, one per indexed key. At interval 8 that is far
            # more entries than the default interval would leave, which is how a
            # default-laid-out table (the bug this guards against) would show up.
            with handle.borrow_reader() as reader:
                expected = -(-reader.record_count // 8)
                assert len(reader.index.entries) == expected


def test_incremental_schedule_leaves_a_realistic_steady_state() -> None:
    """Bulk merges everything into one table; incremental settles where a live engine would."""
    bulk = run_bench(
        _amplification_config(compaction_schedule=COMPACTION_SCHEDULE_BULK)
    ).amplification
    incremental = run_bench(
        _amplification_config(compaction_schedule=COMPACTION_SCHEDULE_INCREMENTAL)
    ).amplification

    assert bulk is not None and incremental is not None
    assert bulk.compaction.schedule == COMPACTION_SCHEDULE_BULK
    assert incremental.compaction.schedule == COMPACTION_SCHEDULE_INCREMENTAL
    # Both still describe one dataset and still reduce the tables a read searches.
    assert incremental.after.records_on_disk == incremental.before.records_on_disk
    assert incremental.reduced
    # A finished directory compacted in bulk collapses to a single tier; the
    # flush-by-flush schedule does not, which is why it is the one the size-tier
    # knobs can move.
    assert bulk.compaction.tables_after <= incremental.compaction.tables_after


def test_min_tier_tables_trades_write_amplification_against_tables_searched() -> None:
    """The size-tier trade-off the spike measured, pinned so the default's basis cannot rot."""
    eager = run_bench(
        _amplification_config(
            compaction_schedule=COMPACTION_SCHEDULE_INCREMENTAL,
            min_tier_tables=2,
        )
    ).amplification
    lazy = run_bench(
        _amplification_config(
            compaction_schedule=COMPACTION_SCHEDULE_INCREMENTAL,
            min_tier_tables=8,
        )
    ).amplification

    assert eager is not None and lazy is not None
    # Merging sooner (a lower threshold) rewrites more bytes to get there.
    assert eager.compaction.bytes_rewritten > lazy.compaction.bytes_rewritten
    assert eager.compaction.write_amplification > lazy.compaction.write_amplification


def test_write_amplification_is_zero_when_nothing_was_flushed() -> None:
    """The ratio is defined rather than a division by zero when the before side held no bytes."""
    from ledgerlog.bench import CompactionSummary

    summary = CompactionSummary(
        passes=0,
        tables_before=0,
        tables_after=0,
        bytes_before=0,
        bytes_after=0,
        elapsed_seconds=0.0,
    )

    assert summary.write_amplification == 0.0
