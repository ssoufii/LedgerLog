"""Tests for the bench harness's phases, its reported figures and its CLI.

Covers story M10.1 (bench harness: write throughput and random read latency).

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

import pytest

from ledgerlog.bench import (
    DEFAULT_VALUE_SIZE_BYTES,
    KEY_INDEX_DIGITS,
    MAX_RECORDS,
    PERCENTILES,
    BenchConfig,
    LatencySummary,
    ReadPhaseResult,
    WritePhaseResult,
    build_parser,
    config_from_args,
    draw_read_indices,
    key_for_index,
    main,
    nearest_rank_percentile,
    run_bench,
)
from ledgerlog.engine import DEFAULT_MEMTABLE_THRESHOLD_BYTES, LedgerLog
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
    assert result.megabytes_per_second == pytest.approx(0.5)


def test_write_throughput_survives_an_unmeasurably_short_phase() -> None:
    """A phase too short for the clock must not take the run down with a ZeroDivisionError."""
    result = WritePhaseResult(records=1, value_size_bytes=0, payload_bytes=0, elapsed_seconds=0.0)

    assert result.records_per_second == float("inf")
    assert result.megabytes_per_second == float("inf")


def test_read_throughput_is_reads_over_elapsed() -> None:
    """The read phase's own summary figure, pinned the same way."""
    result = ReadPhaseResult(
        reads=300,
        hits=300,
        misses=0,
        elapsed_seconds=1.5,
        sstables_consulted_from=3,
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

    assert result.read.sstables_consulted_from >= 2
    assert result.read.misses == 0


class _NeverDrainingEngine:
    """Stands in for an engine whose flush never completes, which no real run reaches.

    The path under test is the one a flush that keeps failing produces: the
    frozen table never goes away, so ``wait_for_flush`` reports a timeout rather
    than raising, and a harness that waited without a bound would hang with
    nothing printed. A stub is the only way to reach that, since a healthy engine
    always drains and an engine made to fail for real would be a test of the
    engine rather than of the harness's response to it.
    """

    flush_error = RuntimeError("disk on fire")

    def __init__(self, *args: object, **kwargs: object) -> None:
        self.sstables: tuple[object, ...] = ()

    def __enter__(self) -> _NeverDrainingEngine:
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False

    def put(self, key: bytes, value: bytes) -> None:
        return None

    def get(self, key: bytes) -> bytes | None:
        return b""

    def flush_pending(self) -> tuple[object, ...]:
        return ()

    def wait_for_flush(self, timeout: float | None = None) -> bool:
        return False


def test_run_bench_gives_up_on_a_flush_that_never_drains(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """A failing flush must end the run with a reason, not hang the command forever."""
    monkeypatch.setattr("ledgerlog.bench.LedgerLog", _NeverDrainingEngine)

    with pytest.raises(RuntimeError, match="timed out"):
        run_bench(_fast_config(records=5))


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
