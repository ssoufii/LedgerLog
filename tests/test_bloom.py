"""Tests for the bloom filter's sizing, its no-false-negatives guarantee and its measured rate.

Covers story M5.1 (bloom filter core: bit array plus k hash functions).

The no-false-negatives criterion is a property test, as the story asks. It is
worth saying why that is the right shape here rather than a nicety: the failure
mode this structure has to rule out is a key whose bits were set by ``add`` and
read back from somewhere else by ``might_contain``, and whether that happens
depends on the key bytes, on ``m``, and on ``k`` together. A fixed set of keys
tests one point in that space. Hypothesis tests the space, including the awkward
corners (empty keys, keys that differ in one bit, a key added twice) that nobody
writes down by hand.

The false-positive criterion is measured, not asserted from the formula. A
filter can size itself perfectly and still miss its target because its probe
sequence collapses onto fewer distinct bits than ``k``, and that bug is
invisible to any test that checks ``m`` and ``k`` and then trusts them. So these
tests add a known key set, query a large disjoint sample, and compare the
observed rate against the configured target.

The measurement tests seed their own ``random.Random`` rather than using the
global one, and query keys are drawn from a namespace that cannot collide with
the added keys (a distinct prefix, plus an explicit set difference). A stray
collision would put a genuinely present key in the "absent" sample and be
counted as a false positive, which would make the test fail for a reason that
has nothing to do with the filter. The tolerance is a band around the target
rather than an exact figure, sized well outside the sampling noise for the
sample sizes used here, because the rate is a statistical property and a test
that demanded an exact number would fail on a correct implementation.

The hashing test asserts the digest is stable across processes rather than
merely deterministic within one. ``PYTHONHASHSEED`` is the difference, and a
filter built on the built-in ``hash()`` would pass every test in this file that
ran in a single process while being unusable for the SSTable case M5.3 needs it
for, where a filter is written by one process and queried by another.
"""

from __future__ import annotations

import ast
import math
import os
import pathlib
import random
import subprocess
import sys
import threading

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ledgerlog.bloom import BloomFilter, optimal_bit_count, optimal_hash_count

# ---------------------------------------------------------------------------
# Criterion 1: sizing from a target rate and an expected key count
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("expected_keys", [1, 10, 1_000, 100_000])
@pytest.mark.parametrize("rate", [0.5, 0.1, 0.01, 0.001])
def test_optimal_bit_count_matches_the_standard_formula(expected_keys: int, rate: float) -> None:
    """``m`` is ``-n ln p / (ln 2)^2``, rounded up."""
    expected = math.ceil(-expected_keys * math.log(rate) / (math.log(2) ** 2))
    assert optimal_bit_count(expected_keys, rate) == max(1, expected)


@pytest.mark.parametrize("expected_keys", [1, 10, 1_000, 100_000])
@pytest.mark.parametrize("rate", [0.1, 0.01, 0.001])
def test_optimal_hash_count_matches_the_standard_formula(expected_keys: int, rate: float) -> None:
    """``k`` is ``(m / n) ln 2``, rounded to nearest, and at least one."""
    bit_count = optimal_bit_count(expected_keys, rate)
    expected = max(1, round((bit_count / expected_keys) * math.log(2)))
    assert optimal_hash_count(bit_count, expected_keys) == expected


def test_a_tighter_target_rate_asks_for_more_bits() -> None:
    """Sizing has to respond to the target, not just produce some number."""
    loose = optimal_bit_count(10_000, 0.1)
    tight = optimal_bit_count(10_000, 0.001)
    assert tight > loose


def test_more_expected_keys_asks_for_more_bits_at_the_same_rate() -> None:
    """Holding the target fixed, the array grows linearly in the key count."""
    small = optimal_bit_count(1_000, 0.01)
    large = optimal_bit_count(10_000, 0.01)
    assert large > small
    # Linear in n, so ten times the keys is ten times the bits, give or take
    # the rounding up of a single bit at each size.
    assert abs(large - 10 * small) <= 10


def test_for_target_builds_an_empty_filter_with_the_computed_geometry() -> None:
    """The constructor wires the two formulas together and starts with no bits set."""
    bloom = BloomFilter.for_target(expected_keys=5_000, false_positive_rate=0.01)

    assert bloom.bit_count == optimal_bit_count(5_000, 0.01)
    assert bloom.hash_count == optimal_hash_count(bloom.bit_count, 5_000)
    assert bloom.byte_count == (bloom.bit_count + 7) // 8
    assert bloom.set_bit_count == 0
    assert bloom.added_count == 0
    assert bloom.target_false_positive_rate == pytest.approx(0.01)


def test_a_filter_sized_at_one_percent_uses_about_ten_bits_per_key() -> None:
    """A sanity anchor against the well-known figure, so a formula typo shows up.

    The textbook number for a one percent target is roughly 9.6 bits per key
    and about 7 hash functions. A sizing that came out an order of magnitude
    off would still satisfy every relational test above.
    """
    bloom = BloomFilter.for_target(expected_keys=100_000, false_positive_rate=0.01)

    assert 9.0 <= bloom.bit_count / 100_000 <= 10.5
    assert bloom.hash_count == 7


@pytest.mark.parametrize("bad_rate", [0.0, 1.0, -0.1, 1.5, float("nan"), float("inf")])
def test_an_out_of_range_target_rate_is_refused(bad_rate: float) -> None:
    """Zero and one are not achievable targets, and neither is anything outside them."""
    with pytest.raises(ValueError):
        BloomFilter.for_target(expected_keys=100, false_positive_rate=bad_rate)


@pytest.mark.parametrize("bad_count", [0, -1])
def test_a_non_positive_expected_key_count_is_refused(bad_count: int) -> None:
    with pytest.raises(ValueError):
        BloomFilter.for_target(expected_keys=bad_count, false_positive_rate=0.01)


@pytest.mark.parametrize("bad_value", [True, 1.5, "100", None])
def test_a_non_integer_expected_key_count_is_refused(bad_value: object) -> None:
    """``bool`` is included deliberately: it is an ``int`` subclass in Python."""
    with pytest.raises(TypeError):
        BloomFilter.for_target(expected_keys=bad_value, false_positive_rate=0.01)  # type: ignore[arg-type]


def test_a_sizing_request_beyond_the_bit_ceiling_is_refused_not_quietly_shrunk() -> None:
    """An out of range request must not come back as a filter that cannot hit its target.

    Clamping to the ceiling would return an undersized filter, and an
    undersized filter answers "possibly present" to everything, which is a
    legal answer no caller could distinguish from a working one.
    """
    with pytest.raises(ValueError, match="bit ceiling"):
        optimal_bit_count(10**12, 0.001)
    with pytest.raises(ValueError, match="bit ceiling"):
        BloomFilter.for_target(expected_keys=10**12, false_positive_rate=0.001)


def test_a_sizing_request_beyond_the_hash_ceiling_is_refused_not_quietly_shrunk() -> None:
    """Same argument for ``k``: a silently reduced ``k`` misses the target invisibly."""
    with pytest.raises(ValueError, match="ceiling"):
        optimal_hash_count(bit_count=1_000_000, expected_keys=1)


def test_a_bit_count_beyond_the_ceiling_is_refused_by_the_constructor() -> None:
    """The bound that matters for M5.2: a length read off disk must not reach an allocation."""
    with pytest.raises(ValueError, match="bit_count must be at most"):
        BloomFilter(bit_count=(1 << 33) + 1, hash_count=4)
    with pytest.raises(ValueError, match="hash_count must be at most"):
        BloomFilter(bit_count=1_024, hash_count=65)


def test_a_bits_buffer_of_the_wrong_length_is_refused() -> None:
    """A short buffer would answer some keys and raise on others, so it is rejected up front."""
    bloom = BloomFilter(bit_count=1_024, hash_count=4)
    assert bloom.byte_count == 128

    with pytest.raises(ValueError, match="exactly 128 bytes"):
        BloomFilter(bit_count=1_024, hash_count=4, bits=bytearray(127))
    with pytest.raises(ValueError, match="exactly 128 bytes"):
        BloomFilter(bit_count=1_024, hash_count=4, bits=bytearray(129))


def test_a_bits_buffer_of_the_right_length_is_adopted_by_value() -> None:
    """The filter copies the buffer, so a later edit of the caller's array cannot clear a bit."""
    source = bytearray(128)
    bloom = BloomFilter(bit_count=1_024, hash_count=4, bits=source)
    bloom.add(b"anchor")
    assert bloom.might_contain(b"anchor")

    source[:] = bytearray(128)
    assert bloom.might_contain(b"anchor")


def test_the_exposed_bit_array_cannot_be_used_to_clear_a_bit() -> None:
    """``bits`` hands out an immutable copy: a false negative must not be reachable from outside."""
    bloom = BloomFilter.for_target(expected_keys=100, false_positive_rate=0.01)
    bloom.add(b"anchor")

    exposed = bloom.bits
    assert isinstance(exposed, bytes)
    assert bloom.might_contain(b"anchor")


# ---------------------------------------------------------------------------
# Criterion 2: no false negatives, as a property over randomized key sets
# ---------------------------------------------------------------------------


@given(
    keys=st.lists(st.binary(min_size=0, max_size=64), min_size=1, max_size=200),
    rate=st.sampled_from([0.5, 0.1, 0.01, 0.001]),
)
@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_every_added_key_is_reported_present(keys: list[bytes], rate: float) -> None:
    """The core guarantee: ``add`` then ``might_contain`` is ``True``, always.

    The filter is sized for the key set it is given, so this also exercises
    small ``m`` and large ``m`` as hypothesis varies the list length.
    """
    bloom = BloomFilter.for_target(expected_keys=len(keys), false_positive_rate=rate)
    for key in keys:
        bloom.add(key)

    for key in keys:
        assert bloom.might_contain(key), f"false negative for {key!r}"
        assert key in bloom


@given(keys=st.lists(st.binary(min_size=0, max_size=32), min_size=1, max_size=100))
@settings(max_examples=100, deadline=None)
def test_no_false_negative_survives_a_badly_undersized_filter(keys: list[bytes]) -> None:
    """Saturating the array ruins the false-positive rate but must not lose a key.

    A filter sized for one key and handed a hundred is close to all ones. That
    is a useless filter and a correct one: the guarantee is one-sided, and
    over-filling only ever costs precision.
    """
    bloom = BloomFilter.for_target(expected_keys=1, false_positive_rate=0.5)
    for key in keys:
        bloom.add(key)

    for key in keys:
        assert bloom.might_contain(key)


@given(keys=st.lists(st.binary(min_size=0, max_size=32), min_size=1, max_size=100))
@settings(max_examples=100, deadline=None)
def test_adding_a_key_never_unsets_a_bit(keys: list[bytes]) -> None:
    """Bits only ever go from clear to set, which is why a false negative is impossible.

    Checked directly on the array rather than through query answers, because
    that is the invariant the argument in the module docstring rests on.
    """
    bloom = BloomFilter.for_target(expected_keys=len(keys), false_positive_rate=0.01)
    previous = bloom.bits
    for key in keys:
        bloom.add(key)
        current = bloom.bits
        for before, after in zip(previous, current, strict=True):
            assert before & after == before, "a previously set bit was cleared"
        previous = current


def test_adding_the_same_key_twice_changes_no_bits() -> None:
    """Idempotence of ``add`` on the bits, which is what makes repeats harmless."""
    bloom = BloomFilter.for_target(expected_keys=100, false_positive_rate=0.01)
    bloom.add(b"repeated")
    after_first = bloom.bits

    bloom.add(b"repeated")
    assert bloom.bits == after_first
    # The call counter does move, since it counts calls and not distinct keys.
    assert bloom.added_count == 2


def test_an_empty_filter_reports_everything_absent() -> None:
    """Nothing added means every bit is clear, so no key can pass all ``k`` probes."""
    bloom = BloomFilter.for_target(expected_keys=1_000, false_positive_rate=0.01)
    rng = random.Random(20260923)
    for _ in range(1_000):
        key = rng.randbytes(16)
        assert not bloom.might_contain(key)


def test_an_empty_key_is_an_ordinary_key() -> None:
    """``b""`` is a legal key elsewhere in the engine, so it has to be one here."""
    bloom = BloomFilter.for_target(expected_keys=10, false_positive_rate=0.01)
    bloom.add(b"")
    assert bloom.might_contain(b"")


@pytest.mark.parametrize("bad_key", ["str-key", 42, None, bytearray(b"x")])
def test_a_non_bytes_key_is_refused(bad_key: object) -> None:
    """Keys are bytes throughout the engine, and a ``str`` key would hash differently."""
    bloom = BloomFilter.for_target(expected_keys=10, false_positive_rate=0.01)
    with pytest.raises(TypeError):
        bloom.add(bad_key)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        bloom.might_contain(bad_key)  # type: ignore[arg-type]


def test_concurrent_readers_never_see_a_false_negative() -> None:
    """Back the module's read-only concurrency claim with real threads.

    CLAUDE.md asks that a concurrency claim be exercised rather than asserted,
    and ``BloomFilter``'s docstring makes one: a filter nobody is adding to can
    be queried from many threads at once. That is the shape the read path uses,
    since a filter is frozen the moment its SSTable is committed.

    Each thread queries the full key set and reports what it saw, so a torn
    read or a bad index would show up as a false negative in some thread rather
    than as a crash. The barrier makes the threads start together, which is
    what gives the overlap any chance of exposing a problem.
    """
    rng = random.Random(90210)
    keys = [b"present:" + rng.randbytes(12) for _ in range(2_000)]
    absent = [b"absent:" + rng.randbytes(12) for _ in range(2_000)]

    bloom = BloomFilter.for_target(expected_keys=len(keys), false_positive_rate=0.01)
    for key in keys:
        bloom.add(key)

    reader_count = 8
    barrier = threading.Barrier(reader_count)
    false_negatives: list[bytes] = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def reader() -> None:
        try:
            barrier.wait(timeout=30)
            missed = [key for key in keys if not bloom.might_contain(key)]
            # Absent keys are queried too, so the readers are doing the mixed
            # work the read path actually does rather than one hot path.
            for key in absent:
                bloom.might_contain(key)
        except BaseException as exc:  # noqa: BLE001 - recorded and re-raised below
            with lock:
                errors.append(exc)
            return
        if missed:
            with lock:
                false_negatives.extend(missed)

    threads = [
        threading.Thread(target=reader, name=f"bloom-reader-{i}") for i in range(reader_count)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)

    assert not [thread for thread in threads if thread.is_alive()], "a reader thread hung"
    assert not errors, f"reader threads raised: {errors}"
    assert not false_negatives, f"{len(false_negatives)} false negatives under concurrent reads"
    # The filter is read-only for the duration, so not one bit should have moved.
    assert bloom.added_count == len(keys)


# ---------------------------------------------------------------------------
# Criterion 3: measured false-positive rate close to the configured target
# ---------------------------------------------------------------------------


def _measure_false_positive_rate(
    *,
    expected_keys: int,
    target_rate: float,
    sample_size: int,
    seed: int,
) -> float:
    """Fill a filter to its design point and measure its rate over absent keys.

    Added and queried keys are drawn from disjoint prefixes and the overlap is
    then excluded explicitly, so a key counted as a false positive really was
    never added. Without that, the measurement would drift upward for a reason
    that has nothing to do with the filter.
    """
    rng = random.Random(seed)
    bloom = BloomFilter.for_target(expected_keys=expected_keys, false_positive_rate=target_rate)

    added: set[bytes] = set()
    while len(added) < expected_keys:
        added.add(b"present:" + rng.randbytes(12))
    for key in added:
        bloom.add(key)

    probes: set[bytes] = set()
    while len(probes) < sample_size:
        candidate = b"absent:" + rng.randbytes(12)
        if candidate not in added:
            probes.add(candidate)

    hits = sum(1 for key in probes if bloom.might_contain(key))
    return hits / len(probes)


@pytest.mark.parametrize("target_rate", [0.1, 0.01])
def test_measured_false_positive_rate_is_close_to_the_target(target_rate: float) -> None:
    """The headline criterion, measured over a large sample of never-added keys.

    The band is generous on the low side (a filter that is more accurate than
    asked for is not a failure) and held to twice the target on the high side.
    At these sample sizes the standard error of the estimate is far below that,
    so a filter whose effective ``k`` had collapsed would land outside it while
    a correct one has no realistic chance of doing so.
    """
    measured = _measure_false_positive_rate(
        expected_keys=20_000,
        target_rate=target_rate,
        sample_size=50_000,
        seed=20260923,
    )
    assert measured <= target_rate * 2.0, f"measured {measured} against target {target_rate}"
    assert measured >= target_rate * 0.25, f"measured {measured} against target {target_rate}"


def test_a_tighter_target_actually_produces_fewer_false_positives() -> None:
    """The target has to steer the measured behavior, not only the reported geometry."""
    loose = _measure_false_positive_rate(
        expected_keys=10_000, target_rate=0.1, sample_size=30_000, seed=11
    )
    tight = _measure_false_positive_rate(
        expected_keys=10_000, target_rate=0.001, sample_size=30_000, seed=11
    )
    assert tight < loose


def test_the_estimated_rate_tracks_the_measured_rate() -> None:
    """The estimate read off the array agrees with what querying actually shows.

    This is the cheap check the engine can run on a live filter, so it should
    not disagree with the expensive one.
    """
    rng = random.Random(4242)
    bloom = BloomFilter.for_target(expected_keys=10_000, false_positive_rate=0.01)
    added = {b"present:" + rng.randbytes(12) for _ in range(10_000)}
    for key in added:
        bloom.add(key)

    probes = [b"absent:" + rng.randbytes(12) for _ in range(30_000)]
    measured = sum(1 for key in probes if bloom.might_contain(key)) / len(probes)

    assert bloom.estimated_false_positive_rate == pytest.approx(measured, abs=0.01)


def test_over_filling_pushes_the_measured_rate_above_the_target() -> None:
    """The target is a design point tied to the key count, and the tests should show that.

    Stated as a test so the promise in ``for_target``'s docstring is not just a
    docstring: a filter handed ten times the keys it was sized for really does
    degrade, which is why the expected count is an argument.
    """
    rng = random.Random(777)
    bloom = BloomFilter.for_target(expected_keys=1_000, false_positive_rate=0.01)
    added = {b"present:" + rng.randbytes(12) for _ in range(10_000)}
    for key in added:
        bloom.add(key)

    probes = [b"absent:" + rng.randbytes(12) for _ in range(20_000)]
    measured = sum(1 for key in probes if bloom.might_contain(key)) / len(probes)
    assert measured > 0.01


# ---------------------------------------------------------------------------
# Criterion 4: standard library hashing only, and stable across processes
# ---------------------------------------------------------------------------


def test_the_same_key_always_maps_to_the_same_bits_within_a_process() -> None:
    """Two filters with the same geometry set the same bits for the same key."""
    left = BloomFilter(bit_count=4_096, hash_count=5)
    right = BloomFilter(bit_count=4_096, hash_count=5)
    for key in (b"", b"a", b"alpha", bytes(range(256))):
        left.add(key)
        right.add(key)
    assert left.bits == right.bits


def test_the_bit_pattern_is_identical_in_a_separate_process_with_a_different_hash_seed() -> None:
    """The digest must not depend on ``PYTHONHASHSEED``.

    A filter built on the built-in ``hash()`` would produce a different bit
    pattern in a process started with a different seed. That is a false
    negative after a restart, which is the one error this structure cannot
    make, so it is checked by actually running the build in child processes
    with opposing seeds rather than by inspecting the implementation.
    """
    program = (
        "from ledgerlog.bloom import BloomFilter\n"
        "f = BloomFilter(bit_count=8192, hash_count=6)\n"
        "for k in (b'', b'alpha', b'beta', bytes(range(256))):\n"
        "    f.add(k)\n"
        "print(f.bits.hex())\n"
    )
    outputs = []
    for seed in ("0", "1", "123456"):
        # The parent environment is inherited rather than replaced so the child
        # keeps whatever import path makes the package reachable here (installed
        # or only on PYTHONPATH). Only the seed is forced, since that is the
        # single variable this test is about.
        env = dict(os.environ)
        env["PYTHONHASHSEED"] = seed
        env["PYTHONPATH"] = os.pathsep.join(
            path for path in (_package_search_path(), env.get("PYTHONPATH", "")) if path
        )
        completed = subprocess.run(
            [sys.executable, "-c", program],
            capture_output=True,
            text=True,
            check=True,
            env=env,
        )
        outputs.append(completed.stdout.strip())

    assert len(set(outputs)) == 1, "bit pattern changed with PYTHONHASHSEED"
    # Guard against the check passing because every run produced nothing.
    assert outputs[0]


def _package_search_path() -> str:
    """Give a child process the import root this test process resolved the package from."""
    import ledgerlog

    # ``.../<root>/ledgerlog/__init__.py`` -> ``.../<root>``
    return str(pathlib.Path(ledgerlog.__file__).parent.parent)


def test_the_module_imports_nothing_outside_the_standard_library() -> None:
    """Criterion 4, checked against what the module actually imports.

    Read off the source's import statements rather than by looking for the
    names of known bloom filter packages: a list of banned names only catches
    the dependencies somebody thought to ban, while the standard library is a
    set Python itself publishes.
    """
    import ledgerlog.bloom as bloom_module

    source = pathlib.Path(bloom_module.__file__).read_text(encoding="utf-8")
    imported: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            imported.add(node.module.split(".")[0])

    assert imported, "the import scan found nothing, so it proves nothing"
    third_party = {
        name for name in imported if name not in sys.stdlib_module_names and name != "ledgerlog"
    }
    assert not third_party, f"bloom.py imports non-standard-library modules: {sorted(third_party)}"
