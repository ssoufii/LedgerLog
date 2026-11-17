"""Tests for the introspection snapshot and the single compaction step, story M11.1.

The module under test makes three kinds of claim, and the tests are grouped by
which one they are about.

The first is that the snapshot is *data*: plain JSON types all the way down, so
it survives a trip through ``json.dumps`` and back unchanged. That is checked by
walking the whole structure rather than by spot-checking a few fields, because
one ``Path`` or one ``bytes`` left in a corner is enough to break the bridge the
web demo sends it over, and it would be in whichever corner the spot check did
not look.

The second is that it is *true*: each number is compared against the thing it
claims to describe, taken from somewhere other than the snapshot. The WAL
checksums are compared against CRC32s computed from the log file's own bytes,
the table sizes against ``stat`` on the files, the key ranges against the keys
that were written. A snapshot that read its fields off the same object twice
would pass a test that compared it to itself.

The third is that reading changes nothing. Two snapshots in a row are equal, and
every key reads the same before and after one, which is the property a tool
polling this for a live display depends on.

The compaction tests are about the one thing a single step has to get right that
the compaction code cannot check for itself: where the merged table lands in the
age ordering. A merged table placed above a table the merge did not read would
shadow newer values with older ones, so there are tests for a merge with nothing
newer than the tier, for one with a free sequence number above the tier, and for
the two shapes where there is no safe slot and the engine must decline to merge.
The last two reach for a private helper, which is marked where it happens: the
public path can only be driven into one of those two states, and the states
differ in why they are unsafe.

Finally, one test runs the whole API with ``threading.Thread.start``
monkeypatched to raise. Pyodide cannot start a thread at all, so the web demo
needs the guarantee that nothing on this path tries to.
"""

from __future__ import annotations

import json
import threading
import zlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any, NoReturn

import pytest

from ledgerlog import introspect
from ledgerlog.compaction import CompactionPolicy, CompactionTier
from ledgerlog.engine import LedgerLog, SSTableHandle, sstable_filename
from ledgerlog.memtable import DEFAULT_MAX_LEVEL
from ledgerlog.sstable import iter_records, read_footer
from ledgerlog.wal import RECORD_HEADER_SIZE

JSON_TYPES = (dict, list, str, int, float, bool, type(None))


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[LedgerLog]:
    """An engine configured the way the web demo configures it.

    No background flusher and no fsync, which is what runs in a browser: there
    are no threads there, and the filesystem is memory, so a sync would cost
    time and promise nothing.
    """
    with LedgerLog(
        tmp_path / "data", fsync_policy="never", flush_in_background=False
    ) as open_engine:
        yield open_engine


@pytest.fixture
def table_engine(tmp_path: Path) -> Iterator[LedgerLog]:
    """An engine that writes one SSTable per put, for tests about tables and tiers.

    A threshold of one byte freezes the memtable after every write, so a put
    followed by a flush is one table. That is the cheapest way to build a
    directory of many small tables, which is what size tiering needs to have
    anything to group.
    """
    with LedgerLog(
        tmp_path / "data",
        fsync_policy="never",
        flush_in_background=False,
        memtable_threshold_bytes=1,
    ) as open_engine:
        yield open_engine


def flushed_tables(engine: LedgerLog, keys: list[bytes], value: bytes = b"v" * 10) -> None:
    """Write one table per key in ``keys``, in order, on a one-byte-threshold engine."""
    for key in keys:
        engine.put(key, value)
    engine.flush_pending()


def walk(value: object, path: str = "snapshot") -> Iterator[tuple[str, object]]:
    """Yield every value in a nested structure, with a readable path to it."""
    yield path, value
    if isinstance(value, dict):
        for name, child in value.items():
            yield from walk(child, f"{path}[{name!r}]")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from walk(child, f"{path}[{index}]")


# ---------------------------------------------------------------------------
# The snapshot is data: JSON types only, and it round-trips
# ---------------------------------------------------------------------------


def test_snapshot_holds_json_types_only(table_engine: LedgerLog) -> None:
    table_engine.put(b"alpha", b"one", ttl=60)
    table_engine.delete(b"beta")
    flushed_tables(table_engine, [b"gamma", b"delta"])

    snapshot = introspect.snapshot(table_engine)

    for path, value in walk(snapshot):
        assert isinstance(value, JSON_TYPES), f"{path} is a {type(value).__name__}"
    for path, value in walk(snapshot):
        if isinstance(value, dict):
            for name in value:
                assert isinstance(name, str), f"{path} has a non-string key {name!r}"


def test_snapshot_round_trips_through_json_unchanged(table_engine: LedgerLog) -> None:
    table_engine.put(b"alpha", b"one", ttl=60)
    table_engine.delete(b"alpha")
    flushed_tables(table_engine, [b"beta", b"gamma", b"delta", b"epsilon"])
    introspect.compact_once(table_engine)
    table_engine.put(b"zeta", b"six")

    snapshot = introspect.snapshot(table_engine)

    assert json.loads(json.dumps(snapshot)) == snapshot


def test_snapshot_reports_its_shape_version(engine: LedgerLog) -> None:
    assert introspect.snapshot(engine)["snapshot_version"] == introspect.SNAPSHOT_VERSION


def test_snapshot_reports_the_engine_settings_a_tool_shows(engine: LedgerLog) -> None:
    snapshot = introspect.snapshot(engine)

    assert snapshot["directory"] == str(engine.directory)
    assert snapshot["fsync_policy"] == "never"
    assert snapshot["memtable_threshold_bytes"] == engine.memtable_threshold_bytes
    assert snapshot["closed"] is False
    assert snapshot["flush_count"] == 0


# ---------------------------------------------------------------------------
# The WAL half of the snapshot
# ---------------------------------------------------------------------------


def test_wal_records_describe_every_write_in_order(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")
    engine.delete(b"alpha")
    engine.put(b"beta", b"two", ttl=120)

    records = introspect.snapshot(engine)["wal"]["records"]

    assert [(record["kind"], record["key"]) for record in records] == [
        ("put", "alpha"),
        ("delete", "alpha"),
        ("put", "beta"),
    ]
    assert [record["value_length"] for record in records] == [3, 0, 3]
    assert records[0]["expires_at_ms"] is None
    assert records[1]["expires_at_ms"] is None
    assert isinstance(records[2]["expires_at_ms"], int)


def test_wal_record_offsets_and_lengths_tile_the_log_file(engine: LedgerLog) -> None:
    for index in range(5):
        engine.put(f"key-{index}".encode(), b"value" * index)

    snapshot = introspect.snapshot(engine)
    records = snapshot["wal"]["records"]
    raw = engine.wal_path.read_bytes()

    for previous, current in zip(records, records[1:], strict=False):
        assert previous["offset"] + previous["length"] == current["offset"]
    last = records[-1]
    assert last["offset"] + last["length"] == len(raw) == snapshot["wal"]["end_offset"]


def test_wal_checksums_are_the_ones_the_file_carries(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")
    engine.put(b"beta", b"two")
    engine.delete(b"alpha")

    records = introspect.snapshot(engine)["wal"]["records"]
    raw = engine.wal_path.read_bytes()

    for record in records:
        payload_start = record["offset"] + RECORD_HEADER_SIZE
        payload = raw[payload_start : record["offset"] + record["length"]]
        assert record["checksum"] == zlib.crc32(payload) & 0xFFFFFFFF


def test_wal_snapshot_reports_an_intact_log_as_intact(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")

    wal = introspect.snapshot(engine)["wal"]

    assert wal["intact"] is True
    assert wal["stopped_at"] is None
    assert wal["reason"] is None
    assert wal["path"] == engine.wal_path.name


def test_wal_snapshot_reports_a_torn_tail_without_repairing_it(tmp_path: Path) -> None:
    directory = tmp_path / "data"
    with LedgerLog(directory, fsync_policy="never", flush_in_background=False) as engine:
        engine.put(b"alpha", b"one")
        engine.put(b"beta", b"two")
        wal_path = engine.wal_path

    intact_size = wal_path.stat().st_size
    with open(wal_path, "r+b") as handle:
        handle.truncate(intact_size - 4)

    with LedgerLog(directory, fsync_policy="never", flush_in_background=False) as reopened:
        # The engine's own startup replay truncated the torn tail, so what the
        # snapshot should show is the surviving prefix and an intact log.
        wal = introspect.snapshot(reopened)["wal"]
        assert wal["intact"] is True
        assert [record["key"] for record in wal["records"]] == ["alpha"]
        assert reopened.get(b"beta") is None


# ---------------------------------------------------------------------------
# The memtable half of the snapshot
# ---------------------------------------------------------------------------


def test_memtable_entries_come_back_in_key_order(engine: LedgerLog) -> None:
    for key in [b"delta", b"alpha", b"charlie", b"bravo"]:
        engine.put(key, b"value")

    entries = introspect.snapshot(engine)["memtable"]["entries"]

    assert [entry["key"] for entry in entries] == ["alpha", "bravo", "charlie", "delta"]


def test_memtable_entry_carries_the_value_and_its_byte_length(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one two three")

    (entry,) = introspect.snapshot(engine)["memtable"]["entries"]

    assert entry["value"] == "one two three"
    assert entry["value_length"] == 13
    assert entry["tombstone"] is False
    assert entry["expires_at_ms"] is None


def test_memtable_entry_marks_a_tombstone_and_carries_no_value(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")
    engine.delete(b"alpha")

    (entry,) = introspect.snapshot(engine)["memtable"]["entries"]

    assert entry["tombstone"] is True
    assert entry["value"] is None
    assert entry["value_length"] is None


def test_memtable_entry_reports_the_absolute_deadline_a_ttl_resolved_to(
    engine: LedgerLog,
) -> None:
    engine.put(b"alpha", b"one", ttl=3600)

    (entry,) = introspect.snapshot(engine)["memtable"]["entries"]

    # The stored deadline, not a remaining lifetime: a snapshot must not depend
    # on when it was taken.
    assert entry["expires_at_ms"] == engine.active_memtable.lookup(b"alpha").expires_at_ms


def test_memtable_counts_and_weight_match_the_engine(engine: LedgerLog) -> None:
    for index in range(10):
        engine.put(f"key-{index}".encode(), b"value")

    memtable = introspect.snapshot(engine)["memtable"]

    assert memtable["entry_count"] == 10
    assert memtable["nbytes"] == engine.memtable_nbytes
    assert memtable["frozen"] is False


def test_tower_levels_are_real_draws_inside_the_structures_range(engine: LedgerLog) -> None:
    for index in range(500):
        engine.put(f"key-{index:04d}".encode(), b"value")

    levels = [entry["level"] for entry in introspect.snapshot(engine)["memtable"]["entries"]]

    assert len(levels) == 500
    assert all(1 <= level <= DEFAULT_MAX_LEVEL for level in levels)
    # Heights are drawn per key from a geometric distribution, so a sample this
    # size being all ones would mean the snapshot is reporting a constant rather
    # than the node's real height. The chance of that happening honestly is
    # 2 ** -500.
    assert max(levels) > 1


def test_a_keys_tower_does_not_change_when_its_value_is_rewritten(engine: LedgerLog) -> None:
    for index in range(200):
        engine.put(f"key-{index:04d}".encode(), b"value")

    before = {
        entry["key"]: entry["level"] for entry in introspect.snapshot(engine)["memtable"]["entries"]
    }
    for index in range(200):
        engine.put(f"key-{index:04d}".encode(), b"rewritten")
    after = {
        entry["key"]: entry["level"] for entry in introspect.snapshot(engine)["memtable"]["entries"]
    }

    # An update replaces the value in the node that is already there, so the
    # tower a visitor is looking at must not jump when they overwrite a key.
    assert after == before


def test_frozen_memtables_are_listed_separately_and_marked_frozen(
    table_engine: LedgerLog,
) -> None:
    table_engine.put(b"alpha", b"one")
    table_engine.put(b"beta", b"two")

    snapshot = introspect.snapshot(table_engine)

    assert len(snapshot["frozen_memtables"]) == len(table_engine.frozen_memtables)
    assert snapshot["frozen_memtables"]
    assert all(frozen["frozen"] is True for frozen in snapshot["frozen_memtables"])
    # Newest first, the order a read consults them in, so the table frozen last
    # comes first.
    assert [frozen["entries"][0]["key"] for frozen in snapshot["frozen_memtables"]] == [
        "beta",
        "alpha",
    ]


def test_keys_that_are_not_utf8_come_back_escaped_with_their_real_length(
    engine: LedgerLog,
) -> None:
    engine.put(b"\xff\xfekey", b"\x00\xff")

    (entry,) = introspect.snapshot(engine)["memtable"]["entries"]

    assert entry["key"] == "\\xff\\xfekey"
    assert entry["value_length"] == 2
    assert introspect.snapshot(engine)["wal"]["records"][0]["key"] == "\\xff\\xfekey"


# ---------------------------------------------------------------------------
# The SSTable half of the snapshot
# ---------------------------------------------------------------------------


def test_sstables_are_listed_newest_first_with_their_sequence_and_name(
    table_engine: LedgerLog,
) -> None:
    flushed_tables(table_engine, [b"alpha", b"beta", b"gamma"])

    tables = introspect.snapshot(table_engine)["sstables"]

    assert [table["sequence"] for table in tables] == [2, 1, 0]
    assert [table["name"] for table in tables] == [
        sstable_filename(2),
        sstable_filename(1),
        sstable_filename(0),
    ]


def test_sstable_sizes_and_counts_match_the_files_on_disk(table_engine: LedgerLog) -> None:
    flushed_tables(table_engine, [b"alpha", b"beta"])

    tables = introspect.snapshot(table_engine)["sstables"]

    for table in tables:
        path = table_engine.directory / table["name"]
        assert table["size_bytes"] == path.stat().st_size
        assert table["record_count"] == 1
        assert table["format_version"] >= 1


def keys_in(path: Path) -> list[bytes]:
    """Read every key out of a table file, in the order the data block holds them."""
    with open(path, "rb") as stream:
        footer = read_footer(stream)
        return [
            record.key
            for record in iter_records(
                stream,
                start_offset=footer.data_block_offset,
                end_offset=footer.data_block_end,
            )
        ]


def test_sstable_key_range_spans_the_keys_the_table_holds(tmp_path: Path) -> None:
    with LedgerLog(
        tmp_path / "data",
        fsync_policy="never",
        flush_in_background=False,
        memtable_threshold_bytes=200,
        index_interval=4,
    ) as engine:
        for index in range(60):
            engine.put(f"key-{index:02d}".encode(), b"value")
        engine.flush_pending()

        tables = introspect.snapshot(engine)["sstables"]
        # A threshold several records wide, so the tables under test hold a range
        # rather than a single key.
        assert tables
        assert all(table["record_count"] > 1 for table in tables)
        for table in tables:
            keys = keys_in(engine.directory / table["name"])
            assert table["min_key"] == keys[0].decode()
            assert table["max_key"] == keys[-1].decode()
            assert keys == sorted(keys)


def test_sstable_index_and_bloom_figures_come_from_the_table(tmp_path: Path) -> None:
    with LedgerLog(
        tmp_path / "data",
        fsync_policy="never",
        flush_in_background=False,
        memtable_threshold_bytes=1,
        index_interval=4,
    ) as engine:
        for index in range(32):
            engine.put(f"key-{index:02d}".encode(), b"value")
        engine.flush_pending()

        snapshot = introspect.snapshot(engine)
        for table, handle in zip(snapshot["sstables"], engine.sstables, strict=True):
            with handle.borrow_reader() as reader:
                assert table["index_entry_count"] == len(reader.index)
            assert table["bloom_bit_count"] == handle.bloom_filter.bit_count
            assert table["bloom_bits_set"] == handle.bloom_filter.set_bit_count
            assert table["bloom_hash_count"] == handle.bloom_filter.hash_count
            assert 0 < table["bloom_bits_set"] <= table["bloom_bit_count"]


def test_compaction_tiers_mirror_the_engines_plan(table_engine: LedgerLog) -> None:
    flushed_tables(table_engine, [b"a", b"b", b"c", b"d", b"e"])

    compaction = introspect.snapshot(table_engine)["compaction"]
    plan = table_engine.compaction_plan

    assert compaction["needs_compaction"] is plan.needs_compaction
    assert compaction["policy"] == {
        "size_ratio": plan.policy.size_ratio,
        "min_tier_tables": plan.policy.min_tier_tables,
    }
    assert [tier["sequences"] for tier in compaction["tiers"]] == [
        list(tier.sequences) for tier in plan.tiers
    ]
    assert [tier["table_count"] for tier in compaction["tiers"]] == [
        tier.table_count for tier in plan.tiers
    ]
    assert [tier["total_size_bytes"] for tier in compaction["tiers"]] == [
        tier.total_size_bytes for tier in plan.tiers
    ]
    assert [tier["ready"] for tier in compaction["tiers"]] == [tier.ready for tier in plan.tiers]


# ---------------------------------------------------------------------------
# Reading changes nothing
# ---------------------------------------------------------------------------


def test_two_snapshots_in_a_row_are_equal(table_engine: LedgerLog) -> None:
    table_engine.put(b"alpha", b"one", ttl=600)
    table_engine.delete(b"beta")
    flushed_tables(table_engine, [b"gamma", b"delta", b"epsilon", b"zeta"])

    first = introspect.snapshot(table_engine)
    second = introspect.snapshot(table_engine)

    assert first == second


def test_a_snapshot_changes_no_answer_the_engine_gives(table_engine: LedgerLog) -> None:
    keys = [f"key-{index}".encode() for index in range(6)]
    flushed_tables(table_engine, keys)
    table_engine.put(b"in-memory", b"fresh")
    table_engine.delete(keys[0])

    probed = [*keys, b"in-memory", b"absent"]
    before = {key: table_engine.get(key) for key in probed}
    introspect.snapshot(table_engine)
    after = {key: table_engine.get(key) for key in probed}

    assert after == before
    assert before[keys[0]] is None
    assert before[b"in-memory"] == b"fresh"


def test_a_snapshot_leaves_the_files_byte_for_byte_the_same(table_engine: LedgerLog) -> None:
    flushed_tables(table_engine, [b"alpha", b"beta"])
    table_engine.put(b"gamma", b"three")

    before = {path.name: path.read_bytes() for path in sorted(table_engine.directory.iterdir())}
    introspect.snapshot(table_engine)
    after = {path.name: path.read_bytes() for path in sorted(table_engine.directory.iterdir())}

    assert after == before


# ---------------------------------------------------------------------------
# compact_once
# ---------------------------------------------------------------------------


def test_compact_once_returns_none_when_there_is_nothing_to_merge(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")

    assert introspect.compact_once(engine) is None


def test_compact_once_returns_none_before_a_tier_is_full(table_engine: LedgerLog) -> None:
    flushed_tables(table_engine, [b"alpha", b"beta"])

    assert table_engine.compaction_plan.needs_compaction is False
    assert introspect.compact_once(table_engine) is None
    assert len(table_engine.sstables) == 2


def test_compact_once_merges_one_ready_tier_into_one_table(table_engine: LedgerLog) -> None:
    flushed_tables(table_engine, [b"alpha", b"beta", b"gamma", b"delta"])
    sources = [handle.sequence for handle in table_engine.sstables]

    result = introspect.compact_once(table_engine)

    assert result is not None
    assert result["source_sequences"] == sources
    assert result["output_sequence"] == 4
    assert result["source_record_count"] == 4
    assert result["output_record_count"] == 4
    assert result["records_dropped"] == 0
    assert [handle.sequence for handle in table_engine.sstables] == [4]


def test_compact_once_retires_the_source_files_and_leaves_the_merged_one(
    table_engine: LedgerLog,
) -> None:
    flushed_tables(table_engine, [b"alpha", b"beta", b"gamma", b"delta"])
    source_paths = [handle.path for handle in table_engine.sstables]

    result = introspect.compact_once(table_engine)

    assert result is not None
    assert all(not path.exists() for path in source_paths)
    merged = table_engine.directory / sstable_filename(result["output_sequence"])
    assert merged.exists()


def test_compact_once_drops_the_older_copies_and_a_safe_tombstone(
    table_engine: LedgerLog,
) -> None:
    for value in [b"first", b"second", b"third"]:
        table_engine.put(b"shared", value)
        table_engine.flush_pending()
    table_engine.delete(b"gone")
    table_engine.flush_pending()

    result = introspect.compact_once(table_engine)

    assert result is not None
    assert result["source_record_count"] == 4
    # One live value for "shared" survives; its two older copies and the
    # tombstone for a key no older table holds are all reclaimed.
    assert result["output_record_count"] == 1
    assert result["records_dropped"] == 3
    assert table_engine.get(b"shared") == b"third"
    assert table_engine.get(b"gone") is None


def test_the_newest_write_still_wins_after_a_compaction(table_engine: LedgerLog) -> None:
    table_engine.put(b"shared", b"old")
    table_engine.flush_pending()
    table_engine.put(b"kept", b"value")
    table_engine.flush_pending()
    table_engine.put(b"shared", b"new")
    table_engine.flush_pending()
    table_engine.delete(b"kept")
    table_engine.flush_pending()

    introspect.compact_once(table_engine)

    assert table_engine.get(b"shared") == b"new"
    assert table_engine.get(b"kept") is None


def test_compaction_survives_a_reopen_of_the_directory(tmp_path: Path) -> None:
    directory = tmp_path / "data"
    with LedgerLog(
        directory,
        fsync_policy="never",
        flush_in_background=False,
        memtable_threshold_bytes=1,
    ) as engine:
        for index in range(4):
            engine.put(f"key-{index}".encode(), b"value")
        engine.put(b"key-0", b"rewritten")
        engine.flush_pending()
        result = introspect.compact_once(engine)
        assert result is not None

    with LedgerLog(directory, fsync_policy="never", flush_in_background=False) as reopened:
        assert [handle.sequence for handle in reopened.sstables] == [result["output_sequence"]]
        assert reopened.get(b"key-0") == b"rewritten"
        assert reopened.get(b"key-3") == b"value"


def test_snapshot_shows_the_merged_table_in_place_of_its_sources(
    table_engine: LedgerLog,
) -> None:
    flushed_tables(table_engine, [b"alpha", b"beta", b"gamma", b"delta"])

    before = introspect.snapshot(table_engine)
    result = introspect.compact_once(table_engine)
    after = introspect.snapshot(table_engine)

    assert result is not None
    assert len(before["sstables"]) == 4
    assert [table["sequence"] for table in after["sstables"]] == [result["output_sequence"]]
    assert after["compaction"]["needs_compaction"] is False
    assert after["sstables"][0]["record_count"] == result["output_record_count"]


def test_compact_once_repeated_empties_the_plan(table_engine: LedgerLog) -> None:
    flushed_tables(table_engine, [f"key-{index}".encode() for index in range(8)])

    steps = 0
    while introspect.compact_once(table_engine) is not None:
        steps += 1
        assert steps < 10

    assert steps >= 1
    assert table_engine.compaction_plan.needs_compaction is False
    for index in range(8):
        assert table_engine.get(f"key-{index}".encode()) == b"v" * 10


def test_compact_once_is_refused_on_a_closed_engine(table_engine: LedgerLog) -> None:
    flushed_tables(table_engine, [b"alpha", b"beta", b"gamma", b"delta"])
    table_engine.close()

    with pytest.raises(ValueError, match="closed"):
        introspect.compact_once(table_engine)


# ---------------------------------------------------------------------------
# Where the merged table lands in the age ordering
# ---------------------------------------------------------------------------


def test_a_tier_with_a_newer_table_right_above_it_is_not_merged(
    table_engine: LedgerLog,
) -> None:
    # Four same-sized tables form a ready tier, and the fifth table is more than
    # the size ratio larger, so it is a tier of its own and it is newer. The
    # merged table would have to sit between sequence 3 and sequence 4, and
    # there is no such number, so the engine must decline rather than publish a
    # table that shadows the newer one.
    flushed_tables(table_engine, [b"a", b"b", b"c", b"d"])
    table_engine.put(b"big", b"B" * 400)
    table_engine.flush_pending()

    assert table_engine.compaction_plan.needs_compaction is True
    assert introspect.compact_once(table_engine) is None
    assert [handle.sequence for handle in table_engine.sstables] == [4, 3, 2, 1, 0]
    assert table_engine.get(b"big") == b"B" * 400
    assert table_engine.get(b"a") == b"v" * 10


def tier_over(engine: LedgerLog, sequences: list[int]) -> CompactionTier[SSTableHandle]:
    """Build a tier over the engine's real handles for the given sequences, newest first."""
    handles = {handle.sequence: handle for handle in engine.sstables}
    return CompactionTier(
        tables=tuple(handles[sequence] for sequence in sorted(sequences, reverse=True)),
        ready=True,
    )


def test_a_tier_with_nothing_newer_takes_the_next_free_sequence(
    table_engine: LedgerLog,
) -> None:
    flushed_tables(table_engine, [b"a", b"b", b"c", b"d"])

    # Reaching for the private chooser on purpose: the public path can only be
    # driven into some of the states this rule has to cover, and each state
    # needs a different directory to exist.
    assert table_engine._merged_sequence_for(tier_over(table_engine, [0, 1, 2, 3])) == 4


def two_merged_tables(engine: LedgerLog) -> None:
    """Leave the engine holding tables 8 and 17, with 9 to 16 retired and free.

    Two rounds of eight single-record tables, each merged, which is how a gap of
    free sequence numbers opens up above an older table: retiring a tier gives
    its numbers back.
    """
    flushed_tables(engine, [f"first-{index}".encode() for index in range(8)])
    assert introspect.compact_once(engine) is not None
    flushed_tables(engine, [f"second-{index}".encode() for index in range(8)])
    assert introspect.compact_once(engine) is not None
    assert [handle.sequence for handle in engine.sstables] == [17, 8]


def test_a_tier_with_a_gap_above_it_takes_the_lowest_free_sequence(
    table_engine: LedgerLog,
) -> None:
    two_merged_tables(table_engine)

    # Table 8 is older than table 17 and the numbers between them are free, so a
    # merge of 8 on its own lands at 9: above what it read, below what it did not.
    assert table_engine._merged_sequence_for(tier_over(table_engine, [8])) == 9


def test_a_tier_a_newer_table_sits_inside_is_not_merged(table_engine: LedgerLog) -> None:
    flushed_tables(table_engine, [b"a", b"b", b"c", b"d"])

    # Sequence 2 is not in the tier and is newer than sequence 1, so no single
    # position in the ordering is both above sequence 3 and below sequence 2.
    assert table_engine._merged_sequence_for(tier_over(table_engine, [0, 1, 3])) is None


def test_a_sequence_already_on_disk_is_not_handed_to_a_merge(
    table_engine: LedgerLog,
) -> None:
    two_merged_tables(table_engine)
    # Debris left under the lowest free name, which a merge must not rename over.
    (table_engine.directory / sstable_filename(9)).write_bytes(b"not a table")

    assert table_engine._merged_sequence_for(tier_over(table_engine, [8])) == 10


def test_the_smallest_mergeable_ready_tier_is_the_one_that_runs(tmp_path: Path) -> None:
    with LedgerLog(
        tmp_path / "data",
        fsync_policy="never",
        flush_in_background=False,
        memtable_threshold_bytes=1,
        compaction_policy=CompactionPolicy(min_tier_tables=2),
    ) as engine:
        for index in range(2):
            engine.put(f"big-{index}".encode(), b"B" * 400)
            engine.flush_pending()
        for index in range(2):
            engine.put(f"small-{index}".encode(), b"v")
            engine.flush_pending()

        plan = engine.compaction_plan
        assert [tier.sequences for tier in plan.ready_tiers] == [(3, 2), (1, 0)]

        result = introspect.compact_once(engine)

        assert result is not None
        assert result["source_sequences"] == [3, 2]
        assert [handle.sequence for handle in engine.sstables] == [4, 1, 0]


# ---------------------------------------------------------------------------
# Pyodide: nothing on this path may start a thread
# ---------------------------------------------------------------------------


def test_the_whole_api_runs_without_starting_a_thread(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(self: threading.Thread, *args: Any, **kwargs: Any) -> NoReturn:
        raise AssertionError("a thread was started")

    monkeypatch.setattr(threading.Thread, "start", refuse)

    with LedgerLog(
        tmp_path / "data",
        fsync_policy="never",
        flush_in_background=False,
        memtable_threshold_bytes=1,
    ) as engine:
        for index in range(4):
            engine.put(f"key-{index}".encode(), b"value", ttl=600)
        engine.delete(b"key-0")
        engine.flush_pending()

        snapshot = introspect.snapshot(engine)
        assert snapshot["sstables"]

        result = introspect.compact_once(engine)
        assert result is not None

        assert introspect.snapshot(engine)["compaction"]["needs_compaction"] is False
        assert engine.get(b"key-1") == b"value"
        assert engine.get(b"key-0") is None


# ---------------------------------------------------------------------------
# locate: one key's passage through the layers (story M11.5)
# ---------------------------------------------------------------------------


def test_locate_is_json_and_round_trips(table_engine: LedgerLog) -> None:
    table_engine.put(b"alpha", b"one")
    table_engine.flush_pending()
    table_engine.put(b"alpha", b"two")

    located = introspect.locate(table_engine, "alpha")

    for path, value in walk(located):
        assert isinstance(value, JSON_TYPES), f"{path} is a {type(value).__name__}"
    assert json.loads(json.dumps(located)) == located


def test_locate_takes_bytes_and_text_alike(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")

    assert introspect.locate(engine, b"alpha") == introspect.locate(engine, "alpha")


def test_locate_refuses_a_key_that_is_neither_bytes_nor_text(engine: LedgerLog) -> None:
    with pytest.raises(TypeError, match="key must be bytes or str"):
        introspect.locate(engine, 7)  # type: ignore[arg-type]


def test_locate_finds_a_fresh_write_in_the_log_and_the_memtable(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")
    engine.put(b"bravo", b"two")

    located = introspect.locate(engine, "alpha")

    wal = located["wal"]
    assert wal is not None
    # The offset and the framing come from the log file itself, not from the
    # engine's report of it.
    raw = engine.wal_path.read_bytes()
    payload = raw[wal["offset"] + RECORD_HEADER_SIZE : wal["offset"] + wal["length"]]
    assert wal["checksum"] == zlib.crc32(payload) & 0xFFFFFFFF
    assert wal["kind"] == "put"
    assert located["memtable"] == {
        "key": "alpha",
        "value": "one",
        "value_length": 3,
        "tombstone": False,
        "expires_at_ms": None,
        "level": located["memtable"]["level"],
        "frozen": False,
    }
    assert 1 <= located["memtable"]["level"] <= DEFAULT_MAX_LEVEL
    assert located["sstables"] == []
    assert located["answered_by"] == "memtable"
    assert located["answering_sequence"] is None


def test_locate_reports_the_newest_log_record_for_a_key(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")
    first = introspect.locate(engine, "alpha")["wal"]
    engine.put(b"alpha", b"two")

    second = introspect.locate(engine, "alpha")["wal"]

    assert first is not None and second is not None
    assert second["offset"] > first["offset"]
    assert second["value_length"] == 3


def test_locate_says_nothing_it_was_not_told(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")

    located = introspect.locate(engine, "never-written")

    assert located["wal"] is None
    assert located["memtable"] is None
    assert located["answered_by"] is None
    assert located["answering_sequence"] is None


def test_locate_follows_a_key_onto_disk_after_a_flush(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")
    engine.flush()

    located = introspect.locate(engine, "alpha")

    assert located["memtable"] is None
    assert located["answered_by"] == "sstable"
    table = located["sstables"][0]
    assert table["sequence"] == located["answering_sequence"]
    assert table["present"] is True
    assert table["searched"] is True
    assert table["tombstone"] is False
    # The offset names a real record in a real file.
    handle = engine.sstables[0]
    with handle.borrow_reader() as reader:
        # The first record is always indexed, so the index's first offset is
        # where the data block starts.
        block_start = reader.index.entries[0].offset
    with open(handle.path, "rb") as stream:
        records = {
            record.offset: record.key
            for record in iter_records(
                stream, start_offset=block_start, end_offset=handle.footer.data_block_end
            )
        }
    assert records[table["record_offset"]] == b"alpha"


def test_locate_reports_the_bloom_bits_the_key_lands_on(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")
    engine.flush()

    table = introspect.locate(engine, "alpha")["sstables"][0]

    handle = engine.sstables[0]
    assert table["bloom_bit_positions"] == list(handle.bloom_filter.bit_positions(b"alpha"))
    assert table["bloom_bit_count"] == handle.bloom_filter.bit_count
    # A key that was added has every one of its bits set, which is why the
    # filter can never give a false negative.
    assert all(table["bloom_bits_set_for_key"])
    assert table["bloom_might_contain"] is True


def test_locate_does_not_open_a_table_the_filter_rejects(engine: LedgerLog) -> None:
    engine.put(b"alpha", b"one")
    engine.flush()

    table = introspect.locate(engine, "zulu")["sstables"][0]

    assert table["bloom_might_contain"] is False
    assert table["searched"] is False
    assert table["present"] is False
    assert table["record_offset"] is None
    assert table["index_offset"] is None
    assert not all(table["bloom_bits_set_for_key"])


def test_locate_distinguishes_an_indexed_key_from_a_scanned_one(tmp_path: Path) -> None:
    # An index interval of two indexes every other key, so one of a pair is
    # pointed at directly and the other is reached by scanning forward from it.
    with LedgerLog(
        tmp_path / "data",
        fsync_policy="never",
        flush_in_background=False,
        index_interval=2,
    ) as engine:
        for index in range(6):
            engine.put(f"key-{index}".encode(), b"value")
        engine.flush()

        indexed = introspect.locate(engine, "key-0")["sstables"][0]
        scanned = introspect.locate(engine, "key-1")["sstables"][0]

        assert indexed["indexed"] is True
        assert indexed["scan_from_key"] == "key-0"
        assert scanned["indexed"] is False
        assert scanned["scan_from_key"] == "key-0"
        assert scanned["index_offset"] == indexed["index_offset"]
        assert scanned["present"] is True


def test_locate_walks_the_tables_newest_first_and_stops_at_the_winner(
    table_engine: LedgerLog,
) -> None:
    table_engine.put(b"alpha", b"old")
    table_engine.flush_pending()
    table_engine.put(b"alpha", b"new")
    table_engine.flush_pending()

    located = introspect.locate(table_engine, "alpha")

    sequences = [table["sequence"] for table in located["sstables"]]
    assert sequences == sorted(sequences, reverse=True)
    # Both tables hold the key, and the newest is the one that answers.
    assert [table["present"] for table in located["sstables"]] == [True, True]
    assert located["answering_sequence"] == sequences[0]
    assert table_engine.get(b"alpha") == b"new"


def test_locate_sees_a_tombstone_on_disk_as_a_record(table_engine: LedgerLog) -> None:
    table_engine.put(b"alpha", b"one")
    table_engine.flush_pending()
    table_engine.delete(b"alpha")
    table_engine.flush_pending()

    located = introspect.locate(table_engine, "alpha")

    newest = located["sstables"][0]
    assert newest["present"] is True
    assert newest["tombstone"] is True
    assert located["answering_sequence"] == newest["sequence"]
    assert table_engine.get(b"alpha") is None


def test_locate_sees_a_frozen_memtable_between_memory_and_disk(tmp_path: Path) -> None:
    with LedgerLog(
        tmp_path / "data",
        fsync_policy="never",
        flush_in_background=False,
        memtable_threshold_bytes=1,
    ) as engine:
        engine.put(b"alpha", b"one")

        located = introspect.locate(engine, "alpha")

        assert engine.frozen_memtables
        assert located["memtable"] is not None
        assert located["memtable"]["frozen"] is True
        assert located["answered_by"] == "frozen_memtable"


def test_locate_agrees_with_the_engine_after_a_compaction(table_engine: LedgerLog) -> None:
    for index in range(4):
        table_engine.put(b"alpha", f"value-{index}".encode())
        table_engine.flush_pending()
    merged = introspect.compact_once(table_engine)

    located = introspect.locate(table_engine, "alpha")

    assert merged is not None
    assert [table["sequence"] for table in located["sstables"]] == [merged["output_sequence"]]
    assert located["answering_sequence"] == merged["output_sequence"]
    assert table_engine.get(b"alpha") == b"value-3"


def test_locating_a_key_changes_nothing(table_engine: LedgerLog) -> None:
    for index in range(3):
        table_engine.put(f"key-{index}".encode(), b"value")
        table_engine.flush_pending()

    before = introspect.snapshot(table_engine)
    first = introspect.locate(table_engine, "key-1")
    second = introspect.locate(table_engine, "key-1")

    assert first == second
    assert introspect.snapshot(table_engine) == before
    assert table_engine.get(b"key-1") == b"value"


def test_locate_runs_without_starting_a_thread(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(self: threading.Thread, *args: Any, **kwargs: Any) -> NoReturn:
        raise AssertionError("a thread was started")

    monkeypatch.setattr(threading.Thread, "start", refuse)

    with LedgerLog(
        tmp_path / "data",
        fsync_policy="never",
        flush_in_background=False,
        memtable_threshold_bytes=1,
    ) as engine:
        engine.put(b"alpha", b"one")
        engine.flush_pending()

        assert introspect.locate(engine, "alpha")["answered_by"] == "sstable"
