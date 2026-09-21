"""Tests for the SSTable data block, the sparse index built alongside it, and the writer.

Covers story M4.1 (SSTable writer: data block plus sparse index) and story M4.2
(footer with format version and section offsets).

The layout tests decode what landed on disk with plain ``struct`` calls rather
than with this module's own decoder, because what they are checking is that the
bytes match the documented format, and a decoder that shares code with the
encoder can agree with it and still be wrong. The parsing tests go the other
way: they hand the reader byte sequences the writer would never produce, so its
bounds checks are exercised against real damage instead of well-formed input.

The round-trip test deliberately reloads the sparse index from the file rather
than using the one the writer returns in memory. The acceptance criterion is
that a key can be located by binary searching the index and scanning forward,
and that only means something if the index being searched is the one a later
reader will actually find on disk.

The footer tests for M4.2's third criterion (a file counts as a table only once
its footer is fully written) truncate a real table at every byte offset rather
than at a chosen few. A commit point that holds at most lengths but not all of
them is not a commit point, and the offsets where it would fail are exactly the
ones nobody thinks to pick by hand.
"""

from __future__ import annotations

import os
import struct
import zlib
from pathlib import Path

import pytest

from ledgerlog import sstable
from ledgerlog.sstable import (
    BLOOM_PLACEHOLDER_SIZE,
    DEFAULT_INDEX_INTERVAL,
    FILE_HEADER_SIZE,
    FOOTER_CHECKSUM_SIZE,
    FOOTER_FIELDS_SIZE,
    FOOTER_MAGIC_SIZE,
    FOOTER_SIZE,
    INDEX_COUNT_SIZE,
    INDEX_ENTRY_HEADER_SIZE,
    INDEX_ENTRY_OFFSET_SIZE,
    MAX_RECORD_PAYLOAD_SIZE,
    RECORD_LENGTH_SIZE,
    RECORD_PAYLOAD_HEADER_SIZE,
    SSTABLE_FOOTER_MAGIC,
    SSTABLE_FORMAT_VERSION,
    SSTABLE_MAGIC,
    IndexEntry,
    SparseIndex,
    SSTableFooter,
    SSTableFooterError,
    SSTableHeaderError,
    SSTableIncompleteError,
    SSTableIndexError,
    SSTableInvalidRecordError,
    SSTableLayout,
    SSTableOp,
    SSTableTruncatedRecordError,
    SSTableUnsupportedVersionError,
    SSTableWriter,
    encode_record,
    is_complete_sstable,
    iter_records,
    read_file_header,
    read_footer,
    write_sstable,
)


def keyed(index: int) -> bytes:
    """Return a fixed width key, so byte order and numeric order agree."""
    return f"key{index:05d}".encode()


def read_records(layout: SSTableLayout) -> list[tuple[bytes, bytes | None]]:
    """Read the whole data block back as key/value pairs."""
    with open(layout.path, "rb") as handle:
        return [
            (record.key, record.value)
            for record in iter_records(
                handle,
                start_offset=layout.data_block_offset,
                end_offset=layout.data_block_end,
            )
        ]


def load_index_from_disk(layout: SSTableLayout) -> SparseIndex:
    """Decode the sparse index section out of the finished file."""
    with open(layout.path, "rb") as handle:
        handle.seek(layout.index_offset)
        raw = handle.read(layout.index_end - layout.index_offset)
    return SparseIndex.decode(
        raw,
        data_block_start=layout.data_block_offset,
        data_block_end=layout.data_block_end,
    )


def lookup(layout: SSTableLayout, index: SparseIndex, key: bytes) -> tuple[bool, bytes | None]:
    """Find ``key`` the way a reader will: binary search the index, then scan forward.

    Returns a found flag alongside the value, so a tombstone (found, value
    ``None``) stays distinguishable from a key the table does not hold.
    """
    start = index.offset_for(key)
    if start is None:
        return False, None
    with open(layout.path, "rb") as handle:
        for record in iter_records(handle, start_offset=start, end_offset=layout.data_block_end):
            if record.key == key:
                return True, record.value
            if record.key > key:
                # The block is sorted, so a greater key means the target is absent.
                break
    return False, None


# Acceptance criterion: a sorted iterator of key-value and tombstone records is
# written as a data block holding those records in the same sorted order.


def test_data_block_preserves_sorted_order_including_tombstones(tmp_path: Path) -> None:
    records: list[tuple[bytes, bytes | None]] = [
        (b"alpha", b"one"),
        (b"bravo", None),
        (b"charlie", b"three"),
        (b"delta", None),
        (b"echo", b"five"),
    ]
    layout = write_sstable(tmp_path / "ordered.sst", records)

    assert read_records(layout) == records
    assert layout.record_count == len(records)


def test_records_are_contiguous_with_no_gaps_or_overlaps(tmp_path: Path) -> None:
    layout = write_sstable(tmp_path / "contiguous.sst", [(keyed(i), b"v") for i in range(20)])

    with open(layout.path, "rb") as handle:
        spans = [
            (record.offset, record.end_offset)
            for record in iter_records(
                handle,
                start_offset=layout.data_block_offset,
                end_offset=layout.data_block_end,
            )
        ]

    assert spans[0][0] == layout.data_block_offset
    assert spans[-1][1] == layout.data_block_end
    for (_, previous_end), (start, _) in zip(spans, spans[1:], strict=False):
        assert start == previous_end


def test_record_bytes_match_the_documented_layout(tmp_path: Path) -> None:
    path = tmp_path / "layout.sst"
    with SSTableWriter(path) as writer:
        writer.add_put(b"kk", b"vvv")
        layout = writer.finish()

    raw = path.read_bytes()
    magic, version = struct.unpack("<8sB", raw[:FILE_HEADER_SIZE])
    assert magic == SSTABLE_MAGIC
    assert version == SSTABLE_FORMAT_VERSION

    cursor = layout.data_block_offset
    (payload_length,) = struct.unpack("<I", raw[cursor : cursor + RECORD_LENGTH_SIZE])
    cursor += RECORD_LENGTH_SIZE
    kind, key_length = struct.unpack("<BI", raw[cursor : cursor + RECORD_PAYLOAD_HEADER_SIZE])
    cursor += RECORD_PAYLOAD_HEADER_SIZE

    assert payload_length == RECORD_PAYLOAD_HEADER_SIZE + len(b"kk") + len(b"vvv")
    assert kind == SSTableOp.PUT
    assert key_length == len(b"kk")
    assert raw[cursor : cursor + key_length] == b"kk"
    assert raw[cursor + key_length : cursor + key_length + 3] == b"vvv"


def test_tombstone_differs_from_a_put_of_an_empty_value(tmp_path: Path) -> None:
    layout = write_sstable(tmp_path / "tombstone.sst", [(b"deleted", None), (b"empty", b"")])

    records = {record[0]: record[1] for record in read_records(layout)}
    assert records[b"deleted"] is None
    assert records[b"empty"] == b""

    index = load_index_from_disk(layout)
    assert lookup(layout, index, b"deleted") == (True, None)
    assert lookup(layout, index, b"empty") == (True, b"")


def test_binary_keys_and_values_round_trip(tmp_path: Path) -> None:
    records: list[tuple[bytes, bytes | None]] = [
        (b"\x00\x01", b"\xff\x00\xfe"),
        (b"\x00\x02", b""),
        (b"\xf0", b"\x00" * 300),
    ]
    layout = write_sstable(tmp_path / "binary.sst", records)

    assert read_records(layout) == records


def test_writer_rejects_keys_that_do_not_ascend(tmp_path: Path) -> None:
    with SSTableWriter(tmp_path / "unsorted.sst") as writer:
        writer.add_put(b"b", b"1")
        with pytest.raises(ValueError, match="ascending key order"):
            writer.add_put(b"a", b"2")
        with pytest.raises(ValueError, match="ascending key order"):
            writer.add_put(b"b", b"3")


# Acceptance criterion: the sparse index records the byte offset of every Nth
# key, not every key.


@pytest.mark.parametrize(("record_count", "interval"), [(100, 8), (64, 16), (33, 5), (10, 3)])
def test_sparse_index_holds_every_nth_key_only(
    tmp_path: Path, record_count: int, interval: int
) -> None:
    keys = [keyed(i) for i in range(record_count)]
    layout = write_sstable(
        tmp_path / "sparse.sst",
        [(key, b"value") for key in keys],
        index_interval=interval,
    )
    index = load_index_from_disk(layout)

    expected_positions = list(range(0, record_count, interval))
    assert [entry.key for entry in index] == [keys[position] for position in expected_positions]
    assert len(index) < record_count

    # Each indexed offset is the offset of that key's own record, not of a
    # neighbour, which is what a forward scan from it depends on.
    with open(layout.path, "rb") as handle:
        for entry in index:
            record = next(
                iter_records(handle, start_offset=entry.offset, end_offset=layout.data_block_end)
            )
            assert record.key == entry.key


def test_default_index_interval_is_used_when_none_is_given(tmp_path: Path) -> None:
    record_count = DEFAULT_INDEX_INTERVAL * 2 + 1
    layout = write_sstable(
        tmp_path / "default.sst", [(keyed(i), b"v") for i in range(record_count)]
    )

    assert layout.index.entries[0].key == keyed(0)
    assert [entry.key for entry in layout.index] == [
        keyed(position) for position in range(0, record_count, DEFAULT_INDEX_INTERVAL)
    ]


def test_index_interval_must_be_at_least_one(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="at least 1"):
        SSTableWriter(tmp_path / "bad.sst", index_interval=0)
    with pytest.raises(ValueError, match="at least 1"):
        SSTableWriter(tmp_path / "bad.sst", index_interval=-4)
    assert list(tmp_path.iterdir()) == []


# Acceptance criterion: a table with fewer than N keys still has the first key's
# offset in its index.


@pytest.mark.parametrize("record_count", [1, 2, 7])
def test_table_smaller_than_the_interval_still_indexes_its_first_key(
    tmp_path: Path, record_count: int
) -> None:
    keys = [keyed(i) for i in range(record_count)]
    layout = write_sstable(tmp_path / "small.sst", [(key, b"v") for key in keys], index_interval=64)
    index = load_index_from_disk(layout)

    assert len(index) == 1
    assert index.entries[0].key == keys[0]
    assert index.entries[0].offset == layout.data_block_offset
    # The one entry is enough to find every key in the table.
    for key in keys:
        assert lookup(layout, index, key) == (True, b"v")


def test_empty_table_writes_an_empty_index(tmp_path: Path) -> None:
    layout = write_sstable(tmp_path / "empty.sst", [])

    assert layout.record_count == 0
    assert layout.data_block_size == 0
    assert len(load_index_from_disk(layout)) == 0
    assert read_records(layout) == []


# Acceptance criterion: every key written can be located by combining a binary
# search on the sparse index with a forward scan.


@pytest.mark.parametrize("interval", [1, 3, 8, DEFAULT_INDEX_INTERVAL])
def test_every_key_is_locatable_through_index_search_and_forward_scan(
    tmp_path: Path, interval: int
) -> None:
    record_count = 257
    expected: dict[bytes, bytes | None] = {
        keyed(i): (None if i % 7 == 0 else f"value-{i}".encode()) for i in range(record_count)
    }
    layout = write_sstable(
        tmp_path / "roundtrip.sst",
        [(key, value) for key, value in sorted(expected.items())],
        index_interval=interval,
    )
    index = load_index_from_disk(layout)

    for key, value in expected.items():
        assert lookup(layout, index, key) == (True, value)


def test_absent_keys_are_not_found_by_the_same_search(tmp_path: Path) -> None:
    layout = write_sstable(
        tmp_path / "absent.sst",
        [(keyed(i), b"v") for i in range(0, 100, 2)],
        index_interval=8,
    )
    index = load_index_from_disk(layout)

    # A key below every indexed key, one between two stored keys, and one past
    # the end of the table.
    assert lookup(layout, index, b"aaa") == (False, None)
    assert lookup(layout, index, keyed(11)) == (False, None)
    assert lookup(layout, index, b"zzz") == (False, None)


def test_offset_for_returns_the_greatest_indexed_key_at_or_below_the_target() -> None:
    index = SparseIndex([IndexEntry(key=b"b", offset=10), IndexEntry(key=b"d", offset=40)])

    assert index.offset_for(b"a") is None
    assert index.offset_for(b"b") == 10
    assert index.offset_for(b"c") == 10
    assert index.offset_for(b"d") == 40
    assert index.offset_for(b"z") == 40


def test_sparse_index_section_matches_the_documented_layout(tmp_path: Path) -> None:
    layout = write_sstable(
        tmp_path / "indexbytes.sst",
        [(keyed(i), b"v") for i in range(4)],
        index_interval=2,
    )
    raw = layout.path.read_bytes()

    cursor = layout.index_offset
    (count,) = struct.unpack("<I", raw[cursor : cursor + INDEX_COUNT_SIZE])
    cursor += INDEX_COUNT_SIZE
    assert count == 2

    decoded: list[tuple[bytes, int]] = []
    for _ in range(count):
        (key_length,) = struct.unpack("<I", raw[cursor : cursor + INDEX_ENTRY_HEADER_SIZE])
        cursor += INDEX_ENTRY_HEADER_SIZE
        key = raw[cursor : cursor + key_length]
        cursor += key_length
        (offset,) = struct.unpack("<Q", raw[cursor : cursor + INDEX_ENTRY_OFFSET_SIZE])
        cursor += INDEX_ENTRY_OFFSET_SIZE
        decoded.append((key, offset))

    assert cursor == layout.index_end
    assert [key for key, _ in decoded] == [keyed(0), keyed(2)]
    assert decoded[0][1] == layout.data_block_offset


def test_bloom_filter_placeholder_is_an_empty_section(tmp_path: Path) -> None:
    layout = write_sstable(tmp_path / "bloom.sst", [(b"k", b"v")])
    raw = layout.path.read_bytes()

    assert layout.bloom_filter_offset == layout.index_end
    assert layout.bloom_filter_end - layout.bloom_filter_offset == BLOOM_PLACEHOLDER_SIZE
    (section_length,) = struct.unpack(
        "<I", raw[layout.bloom_filter_offset : layout.bloom_filter_end]
    )
    assert section_length == 0
    # The placeholder is no longer the last thing in the file: M4.2's footer
    # follows it, and starts exactly where it ends.
    assert layout.footer_offset == layout.bloom_filter_end
    assert layout.footer_end == len(raw)


# File header: the format version that CLAUDE.md requires for an on-disk layout.


def test_header_records_the_format_version(tmp_path: Path) -> None:
    layout = write_sstable(tmp_path / "version.sst", [(b"k", b"v")])

    with open(layout.path, "rb") as handle:
        assert read_file_header(handle) == SSTABLE_FORMAT_VERSION
        assert handle.tell() == layout.data_block_offset


def test_foreign_file_is_rejected_as_not_an_sstable(tmp_path: Path) -> None:
    path = tmp_path / "foreign.sst"
    path.write_bytes(b"NOTATABL" + bytes([SSTABLE_FORMAT_VERSION]))

    with open(path, "rb") as handle, pytest.raises(SSTableHeaderError, match="magic mismatch"):
        read_file_header(handle)


def test_unknown_format_version_is_reported_as_unsupported(tmp_path: Path) -> None:
    path = tmp_path / "future.sst"
    path.write_bytes(SSTABLE_MAGIC + bytes([SSTABLE_FORMAT_VERSION + 1]))

    with open(path, "rb") as handle, pytest.raises(SSTableUnsupportedVersionError) as caught:
        read_file_header(handle)
    assert caught.value.found_version == SSTABLE_FORMAT_VERSION + 1


def test_header_shorter_than_a_header_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "short.sst"
    path.write_bytes(SSTABLE_MAGIC[:3])

    with open(path, "rb") as handle, pytest.raises(SSTableHeaderError, match="truncated"):
        read_file_header(handle)


# Damaged input: lengths read off the disk are bounds checked before they are
# used to slice or allocate.


def test_record_reaching_past_the_data_block_is_reported_as_truncated(tmp_path: Path) -> None:
    path = tmp_path / "truncated.sst"
    record = encode_record(b"k", b"value")
    path.write_bytes(sstable.encode_file_header() + record)
    block_end = FILE_HEADER_SIZE + len(record)

    with open(path, "rb") as handle:
        # Claim the block extends one byte past where the record really ends.
        with pytest.raises(SSTableTruncatedRecordError, match="remain in the data block"):
            list(iter_records(handle, start_offset=FILE_HEADER_SIZE, end_offset=block_end + 1))


def test_record_cut_off_mid_payload_is_reported_rather_than_returned(tmp_path: Path) -> None:
    path = tmp_path / "cut.sst"
    record = encode_record(b"k", b"a long enough value to cut")
    raw = sstable.encode_file_header() + record[:-5]
    path.write_bytes(raw)

    with open(path, "rb") as handle, pytest.raises(SSTableTruncatedRecordError):
        list(iter_records(handle, start_offset=FILE_HEADER_SIZE, end_offset=len(raw)))


def test_absurd_record_length_is_rejected_without_allocating(tmp_path: Path) -> None:
    path = tmp_path / "huge.sst"
    raw = sstable.encode_file_header() + struct.pack("<I", 0xFFFFFFFF) + b"\x01\x01\x00\x00\x00k"
    path.write_bytes(raw)

    with open(path, "rb") as handle, pytest.raises(SSTableInvalidRecordError, match="above the"):
        list(iter_records(handle, start_offset=FILE_HEADER_SIZE, end_offset=len(raw)))


def test_record_length_below_the_payload_header_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "tiny.sst"
    raw = sstable.encode_file_header() + struct.pack("<I", 2) + b"\x01\x00"
    path.write_bytes(raw)

    with open(path, "rb") as handle, pytest.raises(SSTableInvalidRecordError, match="below the"):
        list(iter_records(handle, start_offset=FILE_HEADER_SIZE, end_offset=len(raw)))


def test_key_length_reaching_past_its_own_payload_is_rejected(tmp_path: Path) -> None:
    payload = struct.pack("<BI", int(SSTableOp.PUT), 4096) + b"k"
    raw = sstable.encode_file_header() + struct.pack("<I", len(payload)) + payload
    path = tmp_path / "keylen.sst"
    path.write_bytes(raw)

    with (
        open(path, "rb") as handle,
        pytest.raises(SSTableInvalidRecordError, match="payload bytes follow"),
    ):
        list(iter_records(handle, start_offset=FILE_HEADER_SIZE, end_offset=len(raw)))


def test_unknown_record_kind_is_rejected(tmp_path: Path) -> None:
    payload = struct.pack("<BI", 9, 1) + b"k"
    raw = sstable.encode_file_header() + struct.pack("<I", len(payload)) + payload
    path = tmp_path / "kind.sst"
    path.write_bytes(raw)

    with open(path, "rb") as handle, pytest.raises(SSTableInvalidRecordError, match="kind 9"):
        list(iter_records(handle, start_offset=FILE_HEADER_SIZE, end_offset=len(raw)))


def test_tombstone_carrying_a_value_is_rejected(tmp_path: Path) -> None:
    payload = struct.pack("<BI", int(SSTableOp.DELETE), 1) + b"k" + b"value"
    raw = sstable.encode_file_header() + struct.pack("<I", len(payload)) + payload
    path = tmp_path / "fat_tombstone.sst"
    path.write_bytes(raw)

    with (
        open(path, "rb") as handle,
        pytest.raises(SSTableInvalidRecordError, match="empty-value convention"),
    ):
        list(iter_records(handle, start_offset=FILE_HEADER_SIZE, end_offset=len(raw)))


def test_encoding_a_payload_above_the_format_limit_is_refused() -> None:
    oversized = b"v" * (MAX_RECORD_PAYLOAD_SIZE - RECORD_PAYLOAD_HEADER_SIZE)
    with pytest.raises(sstable.SSTableFormatError, match="format limit"):
        encode_record(b"k", oversized)


def test_index_entry_count_larger_than_the_section_is_rejected() -> None:
    raw = struct.pack("<I", 1000) + struct.pack("<I", 1) + b"k" + struct.pack("<Q", 9)

    with pytest.raises(SSTableIndexError, match="more than the"):
        SparseIndex.decode(raw)


def test_index_entry_key_length_past_the_section_is_rejected() -> None:
    raw = struct.pack("<I", 1) + struct.pack("<I", 64) + b"k" + struct.pack("<Q", 9)

    with pytest.raises(SSTableIndexError, match="remain in the section"):
        SparseIndex.decode(raw)


def test_index_section_shorter_than_its_count_field_is_rejected() -> None:
    with pytest.raises(SSTableIndexError, match="too short"):
        SparseIndex.decode(b"\x00\x00")


def test_index_section_with_trailing_bytes_is_rejected() -> None:
    raw = SparseIndex([IndexEntry(key=b"k", offset=9)]).encode() + b"junk"

    with pytest.raises(SSTableIndexError, match="left over"):
        SparseIndex.decode(raw)


def test_index_offset_outside_the_data_block_is_rejected(tmp_path: Path) -> None:
    layout = write_sstable(tmp_path / "bounds.sst", [(keyed(i), b"v") for i in range(4)])
    raw = layout.path.read_bytes()[layout.index_offset : layout.index_end]

    with pytest.raises(SSTableIndexError, match="before the data block"):
        SparseIndex.decode(
            raw,
            data_block_start=layout.data_block_offset + 1,
            data_block_end=layout.data_block_end,
        )
    with pytest.raises(SSTableIndexError, match="past the end of the data block"):
        SparseIndex.decode(
            raw,
            data_block_start=layout.data_block_offset,
            data_block_end=layout.data_block_offset,
        )


def test_index_keys_must_ascend() -> None:
    with pytest.raises(SSTableIndexError, match="must ascend"):
        SparseIndex([IndexEntry(key=b"b", offset=0), IndexEntry(key=b"a", offset=8)])


# The finished file appears atomically, and an abandoned one leaves nothing
# behind.


def test_final_path_only_appears_once_the_table_is_complete(tmp_path: Path) -> None:
    path = tmp_path / "atomic.sst"
    with SSTableWriter(path) as writer:
        writer.add_put(b"k", b"v")
        assert not path.exists()
        assert writer.temp_path.exists()
        layout = writer.finish()

    assert path.exists()
    assert not writer.temp_path.exists()
    assert read_records(layout) == [(b"k", b"v")]


def test_abandoning_a_writer_leaves_no_file_behind(tmp_path: Path) -> None:
    path = tmp_path / "abandoned.sst"
    with SSTableWriter(path) as writer:
        writer.add_put(b"k", b"v")

    assert not path.exists()
    assert list(tmp_path.iterdir()) == []


def test_an_exception_mid_write_leaves_no_file_behind(tmp_path: Path) -> None:
    path = tmp_path / "raised.sst"
    with pytest.raises(RuntimeError):
        with SSTableWriter(path) as writer:
            writer.add_put(b"k", b"v")
            raise RuntimeError("flush failed partway")

    assert not path.exists()
    assert list(tmp_path.iterdir()) == []


def test_failed_rename_leaves_no_temporary_file_behind(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "rename_fails.sst"

    def refuse(source: object, destination: object) -> None:
        raise OSError("rename refused")

    writer = SSTableWriter(path)
    writer.add_put(b"k", b"v")
    monkeypatch.setattr(sstable.os, "replace", refuse)
    with pytest.raises(OSError, match="rename refused"):
        writer.finish()

    assert not path.exists()
    assert not writer.temp_path.exists()
    assert list(tmp_path.iterdir()) == []


def test_finishing_twice_is_refused(tmp_path: Path) -> None:
    with SSTableWriter(tmp_path / "twice.sst") as writer:
        writer.add_put(b"k", b"v")
        writer.finish()
        with pytest.raises(ValueError, match="already been finished"):
            writer.finish()


def test_adding_after_finish_is_refused(tmp_path: Path) -> None:
    with SSTableWriter(tmp_path / "closed.sst") as writer:
        writer.add_put(b"k", b"v")
        writer.finish()
        with pytest.raises(ValueError, match="has been finished"):
            writer.add_put(b"z", b"v")


def test_writer_leaves_no_open_handles(tmp_path: Path) -> None:
    path = tmp_path / "handles.sst"
    with SSTableWriter(path) as writer:
        writer.add_put(b"k", b"v")
        writer.finish()
    assert writer.closed

    abandoned = SSTableWriter(tmp_path / "abandoned.sst")
    abandoned.discard()
    assert abandoned.closed
    # A second discard is a no-op rather than an error on the already closed handle.
    abandoned.discard()


def test_overwriting_an_existing_table_replaces_it_atomically(tmp_path: Path) -> None:
    path = tmp_path / "replaced.sst"
    write_sstable(path, [(b"old", b"1")])
    layout = write_sstable(path, [(b"new", b"2")])

    assert read_records(layout) == [(b"new", b"2")]
    assert list(os.listdir(tmp_path)) == [path.name]


# Story M4.2: the footer.
#
# Acceptance criterion: the footer is written last, after the data block, the
# sparse index and the bloom filter placeholder.


def test_footer_is_written_last_after_every_section_it_describes(tmp_path: Path) -> None:
    layout = write_sstable(
        tmp_path / "ordered.sst", [(keyed(i), b"v") for i in range(8)], index_interval=2
    )
    raw = layout.path.read_bytes()

    assert layout.data_block_offset == FILE_HEADER_SIZE
    assert layout.index_offset == layout.data_block_end
    assert layout.bloom_filter_offset == layout.index_end
    assert layout.footer_offset == layout.bloom_filter_end
    assert layout.footer_end - layout.footer_offset == FOOTER_SIZE
    assert layout.footer_end == len(raw)
    # The magic that marks a complete table is the very last thing in the file.
    assert raw[-FOOTER_MAGIC_SIZE:] == SSTABLE_FOOTER_MAGIC


def test_footer_bytes_match_the_documented_layout(tmp_path: Path) -> None:
    layout = write_sstable(
        tmp_path / "layout.sst", [(keyed(i), b"value") for i in range(5)], index_interval=2
    )
    raw = layout.path.read_bytes()
    footer_bytes = raw[layout.footer_offset :]
    assert len(footer_bytes) == FOOTER_SIZE

    fields = footer_bytes[:FOOTER_FIELDS_SIZE]
    (
        data_block_offset,
        data_block_end,
        index_offset,
        index_end,
        bloom_filter_offset,
        bloom_filter_end,
        record_count,
        format_version,
    ) = struct.unpack("<7QB", fields)
    (checksum,) = struct.unpack(
        "<I", footer_bytes[FOOTER_FIELDS_SIZE : FOOTER_FIELDS_SIZE + FOOTER_CHECKSUM_SIZE]
    )
    (magic,) = struct.unpack("<8s", footer_bytes[-FOOTER_MAGIC_SIZE:])

    assert data_block_offset == layout.data_block_offset
    assert data_block_end == layout.data_block_end
    assert index_offset == layout.index_offset
    assert index_end == layout.index_end
    assert bloom_filter_offset == layout.bloom_filter_offset
    assert bloom_filter_end == layout.bloom_filter_end
    assert record_count == 5
    assert format_version == SSTABLE_FORMAT_VERSION
    assert checksum == zlib.crc32(fields) & 0xFFFFFFFF
    assert magic == SSTABLE_FOOTER_MAGIC


# Acceptance criterion: the footer carries a format version byte and the byte
# offsets of the sparse index and the bloom filter sections.


def test_footer_records_the_format_version(tmp_path: Path) -> None:
    layout = write_sstable(tmp_path / "version.sst", [(b"k", b"v")])
    with open(layout.path, "rb") as handle:
        footer = read_footer(handle)

    assert footer.format_version == SSTABLE_FORMAT_VERSION
    # The header says the same thing, read from the other end of the file.
    with open(layout.path, "rb") as handle:
        assert read_file_header(handle) == footer.format_version


@pytest.mark.parametrize("record_count", [0, 1, 7, 64])
def test_footer_offsets_match_what_the_writer_reported(tmp_path: Path, record_count: int) -> None:
    layout = write_sstable(
        tmp_path / f"offsets{record_count}.sst",
        [(keyed(i), b"v") for i in range(record_count)],
        index_interval=4,
    )
    with open(layout.path, "rb") as handle:
        footer = read_footer(handle)

    assert footer == layout.footer
    assert footer.index_offset == layout.index_offset
    assert footer.index_end == layout.index_end
    assert footer.bloom_filter_offset == layout.bloom_filter_offset
    assert footer.bloom_filter_end == layout.bloom_filter_end
    assert footer.record_count == record_count


def test_sections_can_be_located_from_the_footer_alone(tmp_path: Path) -> None:
    """The footer's reason to exist: reaching a section without scanning for it."""
    path = tmp_path / "located.sst"
    records = [(keyed(i), f"value{i}".encode()) for i in range(20)]
    write_sstable(path, records, index_interval=5)

    with open(path, "rb") as handle:
        footer = read_footer(handle)
        handle.seek(footer.index_offset)
        index_bytes = handle.read(footer.index_end - footer.index_offset)
        index = SparseIndex.decode(
            index_bytes,
            data_block_start=footer.data_block_offset,
            data_block_end=footer.data_block_end,
        )
        handle.seek(footer.bloom_filter_offset)
        bloom_bytes = handle.read(footer.bloom_filter_end - footer.bloom_filter_offset)
        recovered = [
            (record.key, record.value)
            for record in iter_records(
                handle,
                start_offset=footer.data_block_offset,
                end_offset=footer.data_block_end,
            )
        ]

    assert [entry.key for entry in index] == [keyed(i) for i in range(0, 20, 5)]
    assert len(bloom_bytes) == BLOOM_PLACEHOLDER_SIZE
    assert struct.unpack("<I", bloom_bytes)[0] == 0
    assert recovered == records


def test_footer_round_trips_through_encode_and_decode() -> None:
    footer = SSTableFooter(
        data_block_offset=FILE_HEADER_SIZE,
        data_block_end=100,
        index_offset=100,
        index_end=140,
        bloom_filter_offset=140,
        bloom_filter_end=144,
        record_count=3,
    )
    assert SSTableFooter.decode(footer.encode()) == footer


# Acceptance criterion: a file is only a valid, complete SSTable once its footer
# has been fully written.


def test_a_finished_table_is_complete(tmp_path: Path) -> None:
    layout = write_sstable(tmp_path / "complete.sst", [(keyed(i), b"v") for i in range(3)])
    assert is_complete_sstable(layout.path) is True


def test_table_is_incomplete_at_every_truncation_before_the_footer_ends(tmp_path: Path) -> None:
    path = tmp_path / "truncated.sst"
    layout = write_sstable(path, [(keyed(i), b"payload") for i in range(12)], index_interval=4)
    whole = path.read_bytes()
    assert len(whole) == layout.footer_end

    partial_path = tmp_path / "partial.sst"
    for length in range(len(whole)):
        partial_path.write_bytes(whole[:length])
        assert is_complete_sstable(partial_path) is False, f"accepted a {length} byte table"

    partial_path.write_bytes(whole)
    assert is_complete_sstable(partial_path) is True


def test_the_table_is_incomplete_while_the_writer_is_still_streaming(tmp_path: Path) -> None:
    path = tmp_path / "inflight.sst"
    with SSTableWriter(path) as writer:
        for i in range(4):
            writer.add_put(keyed(i), b"v")
        # Reaching into the writer's handle to push its buffer to disk, so that
        # the check below is made against a file that really holds records. The
        # point is a table whose data is on disk and whose footer is not, which
        # is what a crash mid-flush leaves, and an unflushed buffer would make
        # the same assertion pass for the weaker reason that the file is empty.
        writer._file.flush()

        # Mid-flush the bytes exist, but under a temporary name and without a
        # footer, so nothing yet counts as a table.
        assert not path.exists()
        assert writer.temp_path.exists()
        assert is_complete_sstable(writer.temp_path) is False

        writer.finish()

    assert is_complete_sstable(path) is True


def test_file_too_short_to_hold_a_footer_is_incomplete(tmp_path: Path) -> None:
    empty = tmp_path / "empty.sst"
    empty.write_bytes(b"")
    header_only = tmp_path / "header.sst"
    header_only.write_bytes(sstable.encode_file_header())

    assert is_complete_sstable(empty) is False
    assert is_complete_sstable(header_only) is False
    with open(empty, "rb") as handle:
        with pytest.raises(SSTableIncompleteError, match="no footer was fully written"):
            read_footer(handle)


def test_missing_footer_magic_is_reported_as_an_incomplete_table(tmp_path: Path) -> None:
    path = tmp_path / "nomagic.sst"
    layout = write_sstable(path, [(b"k", b"v")])
    raw = bytearray(path.read_bytes())
    raw[-1] ^= 0xFF
    path.write_bytes(bytes(raw))

    assert layout.footer_end == len(raw)
    with open(path, "rb") as handle:
        with pytest.raises(SSTableIncompleteError, match="never fully written"):
            read_footer(handle)


def test_footer_whose_fields_did_not_land_is_rejected_despite_its_magic(tmp_path: Path) -> None:
    """A crash can store the blocks of one write out of order, magic first."""
    path = tmp_path / "torn.sst"
    layout = write_sstable(path, [(b"k", b"v")])
    raw = bytearray(path.read_bytes())
    # Flip a bit inside the offsets, leaving the checksum and the magic intact.
    raw[layout.footer_offset] ^= 0x01
    path.write_bytes(bytes(raw))

    assert is_complete_sstable(path) is False
    with open(path, "rb") as handle:
        with pytest.raises(SSTableIncompleteError, match="checksum mismatch"):
            read_footer(handle)


def test_a_file_that_merely_ends_in_the_footer_magic_is_not_a_table(tmp_path: Path) -> None:
    path = tmp_path / "lookalike.sst"
    path.write_bytes(sstable.encode_file_header() + b"\x00" * FOOTER_SIZE)
    raw = bytearray(path.read_bytes())
    raw[-FOOTER_MAGIC_SIZE:] = SSTABLE_FOOTER_MAGIC
    path.write_bytes(bytes(raw))

    assert is_complete_sstable(path) is False


def test_footer_of_the_wrong_length_is_rejected() -> None:
    footer = SSTableFooter(
        data_block_offset=FILE_HEADER_SIZE,
        data_block_end=FILE_HEADER_SIZE,
        index_offset=FILE_HEADER_SIZE,
        index_end=FILE_HEADER_SIZE,
        bloom_filter_offset=FILE_HEADER_SIZE,
        bloom_filter_end=FILE_HEADER_SIZE,
        record_count=0,
    )
    with pytest.raises(SSTableIncompleteError, match=f"expected exactly {FOOTER_SIZE}"):
        SSTableFooter.decode(footer.encode()[:-1])


def test_footer_sections_that_run_backwards_are_rejected() -> None:
    fields = struct.pack(
        "<7QB",
        FILE_HEADER_SIZE,
        FILE_HEADER_SIZE + 50,
        FILE_HEADER_SIZE + 50,
        FILE_HEADER_SIZE + 10,  # index ends before it starts
        FILE_HEADER_SIZE + 60,
        FILE_HEADER_SIZE + 64,
        1,
        SSTABLE_FORMAT_VERSION,
    )
    raw = (
        fields
        + struct.pack("<I", zlib.crc32(fields) & 0xFFFFFFFF)
        + struct.pack("<8s", SSTABLE_FOOTER_MAGIC)
    )
    with pytest.raises(SSTableFooterError, match="before it starts"):
        SSTableFooter.decode(raw)


def test_footer_section_inside_the_file_header_is_rejected() -> None:
    fields = struct.pack("<7QB", 0, 50, 50, 60, 60, 64, 1, SSTABLE_FORMAT_VERSION)
    raw = (
        fields
        + struct.pack("<I", zlib.crc32(fields) & 0xFFFFFFFF)
        + struct.pack("<8s", SSTABLE_FOOTER_MAGIC)
    )
    with pytest.raises(SSTableFooterError, match="inside the"):
        SSTableFooter.decode(raw)


def test_encoding_a_negative_footer_offset_is_refused() -> None:
    footer = SSTableFooter(
        data_block_offset=FILE_HEADER_SIZE,
        data_block_end=-1,
        index_offset=FILE_HEADER_SIZE,
        index_end=FILE_HEADER_SIZE,
        bloom_filter_offset=FILE_HEADER_SIZE,
        bloom_filter_end=FILE_HEADER_SIZE,
        record_count=0,
    )
    with pytest.raises(SSTableFooterError, match="data_block_end is negative"):
        footer.encode()


def test_read_footer_leaves_the_stream_at_the_start_of_the_footer(tmp_path: Path) -> None:
    layout = write_sstable(tmp_path / "position.sst", [(keyed(i), b"v") for i in range(4)])
    with open(layout.path, "rb") as handle:
        read_footer(handle)
        assert handle.tell() == layout.footer_offset


def test_read_footer_accepts_a_file_size_the_caller_already_knows(tmp_path: Path) -> None:
    layout = write_sstable(tmp_path / "sized.sst", [(b"k", b"v")])
    size = layout.path.stat().st_size
    with open(layout.path, "rb") as handle:
        assert read_footer(handle, file_size=size) == layout.footer


def test_is_complete_sstable_closes_the_file_it_opened(tmp_path: Path) -> None:
    """Checked by counting descriptors, because a refcounted handle closes itself.

    A leak here would only show up under an implementation that keeps the object
    alive, so looping until the process runs out of descriptors would prove
    nothing on CPython. The descriptor table is the thing that actually answers
    the question, and it is readable on Linux, which is where the suite runs.
    """
    fd_dir = Path("/proc/self/fd")
    if not fd_dir.is_dir():
        pytest.skip("descriptor table is not readable on this platform")

    path = tmp_path / "handles2.sst"
    write_sstable(path, [(b"k", b"v")])
    before = len(os.listdir(fd_dir))
    assert is_complete_sstable(path) is True
    # Also on the path that returns False, which leaves through an except block.
    truncated = tmp_path / "handles2_partial.sst"
    truncated.write_bytes(path.read_bytes()[:-1])
    assert is_complete_sstable(truncated) is False
    assert len(os.listdir(fd_dir)) == before


def test_an_unknown_footer_version_is_reported_rather_than_rejected_here() -> None:
    """Judging the version is the reader's call in M4.3, so decoding surfaces it."""
    fields = struct.pack(
        "<7QB",
        FILE_HEADER_SIZE,
        FILE_HEADER_SIZE + 40,
        FILE_HEADER_SIZE + 40,
        FILE_HEADER_SIZE + 60,
        FILE_HEADER_SIZE + 60,
        FILE_HEADER_SIZE + 64,
        2,
        SSTABLE_FORMAT_VERSION + 7,
    )
    raw = (
        fields
        + struct.pack("<I", zlib.crc32(fields) & 0xFFFFFFFF)
        + struct.pack("<8s", SSTABLE_FOOTER_MAGIC)
    )

    footer = SSTableFooter.decode(raw)
    assert footer.format_version == SSTABLE_FORMAT_VERSION + 7


def test_a_damaged_version_byte_fails_the_footer_checksum() -> None:
    footer = SSTableFooter(
        data_block_offset=FILE_HEADER_SIZE,
        data_block_end=FILE_HEADER_SIZE + 40,
        index_offset=FILE_HEADER_SIZE + 40,
        index_end=FILE_HEADER_SIZE + 60,
        bloom_filter_offset=FILE_HEADER_SIZE + 60,
        bloom_filter_end=FILE_HEADER_SIZE + 64,
        record_count=2,
    )
    raw = bytearray(footer.encode())
    raw[FOOTER_FIELDS_SIZE - 1] ^= 0xFF  # the version byte, last of the checksummed fields

    with pytest.raises(SSTableIncompleteError, match="checksum mismatch"):
        SSTableFooter.decode(bytes(raw))
