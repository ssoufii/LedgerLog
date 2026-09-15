"""Tests for WAL record framing and the append-only writer (story M1.1).

The records are decoded here with plain ``struct`` calls rather than with a
reader from the library, because the point of these tests is that the bytes on
disk match the documented layout. A decoder that shared code with the encoder
could agree with it and still be wrong about the format. The sequential reader
is story M1.4.
"""

from __future__ import annotations

import struct
import threading
import zlib
from dataclasses import dataclass
from pathlib import Path

import pytest

from ledgerlog import wal
from ledgerlog.wal import (
    PAYLOAD_HEADER_SIZE,
    RECORD_HEADER_SIZE,
    WalFormatError,
    WalOp,
    WalWriter,
    encode_record,
)


@dataclass(frozen=True)
class DecodedRecord:
    offset: int
    payload_length: int
    checksum: int
    op: int
    key: bytes
    value: bytes

    @property
    def total_size(self) -> int:
        return RECORD_HEADER_SIZE + self.payload_length


def decode_all(raw: bytes) -> list[DecodedRecord]:
    """Walk a WAL file's bytes and decode every record, asserting each checksum."""
    records: list[DecodedRecord] = []
    offset = 0
    while offset < len(raw):
        assert len(raw) - offset >= RECORD_HEADER_SIZE, "truncated record header"
        payload_length, checksum = struct.unpack_from("<II", raw, offset)
        payload_start = offset + RECORD_HEADER_SIZE
        payload_end = payload_start + payload_length
        assert payload_end <= len(raw), "record claims more bytes than the file holds"
        payload = raw[payload_start:payload_end]
        assert zlib.crc32(payload) & 0xFFFFFFFF == checksum, "checksum mismatch"

        op, key_length = struct.unpack_from("<BI", payload, 0)
        key_start = PAYLOAD_HEADER_SIZE
        key_end = key_start + key_length
        assert key_end <= len(payload), "key length runs past the payload"
        records.append(
            DecodedRecord(
                offset=offset,
                payload_length=payload_length,
                checksum=checksum,
                op=op,
                key=payload[key_start:key_end],
                value=payload[key_end:],
            )
        )
        offset = payload_end
    return records


def test_put_record_framing(tmp_path: Path) -> None:
    path = tmp_path / "put.wal"
    with WalWriter(path) as writer:
        offset = writer.append_put(b"alpha", b"one")

    raw = path.read_bytes()
    assert offset == 0

    payload_length, checksum = struct.unpack_from("<II", raw, 0)
    payload = raw[RECORD_HEADER_SIZE:]
    assert payload_length == len(payload)
    assert payload_length == PAYLOAD_HEADER_SIZE + len(b"alpha") + len(b"one")
    assert checksum == zlib.crc32(payload) & 0xFFFFFFFF

    op, key_length = struct.unpack_from("<BI", payload, 0)
    assert op == WalOp.PUT
    assert key_length == len(b"alpha")
    assert payload[PAYLOAD_HEADER_SIZE : PAYLOAD_HEADER_SIZE + key_length] == b"alpha"
    assert payload[PAYLOAD_HEADER_SIZE + key_length :] == b"one"
    assert len(raw) == RECORD_HEADER_SIZE + payload_length


def test_delete_record_uses_delete_op_and_empty_value(tmp_path: Path) -> None:
    path = tmp_path / "delete.wal"
    with WalWriter(path) as writer:
        writer.append_delete(b"alpha")

    (record,) = decode_all(path.read_bytes())
    assert record.op == WalOp.DELETE
    assert record.key == b"alpha"
    assert record.value == b""


def test_put_of_empty_value_differs_from_delete(tmp_path: Path) -> None:
    """The op byte, not an empty value, is what marks a key as deleted."""
    path = tmp_path / "empty.wal"
    with WalWriter(path) as writer:
        writer.append_put(b"alpha", b"")
        writer.append_delete(b"alpha")

    put_record, delete_record = decode_all(path.read_bytes())
    assert put_record.op == WalOp.PUT
    assert delete_record.op == WalOp.DELETE
    assert put_record.value == delete_record.value == b""
    assert put_record.payload_length == delete_record.payload_length


def test_checksum_covers_op_key_and_value(tmp_path: Path) -> None:
    path = tmp_path / "crc.wal"
    with WalWriter(path) as writer:
        writer.append_put(b"alpha", b"one")

    raw = bytearray(path.read_bytes())
    (record,) = decode_all(bytes(raw))
    stored_checksum = record.checksum

    payload_start = RECORD_HEADER_SIZE
    for index in (
        payload_start,  # op byte
        payload_start + PAYLOAD_HEADER_SIZE,  # first key byte
        len(raw) - 1,  # last value byte
    ):
        mutated = bytearray(raw)
        mutated[index] ^= 0xFF
        payload = bytes(mutated[payload_start:])
        assert zlib.crc32(payload) & 0xFFFFFFFF != stored_checksum


def test_key_length_is_inside_the_checksummed_payload(tmp_path: Path) -> None:
    """Corrupting the key/value boundary must break the checksum, not silently split."""
    path = tmp_path / "boundary.wal"
    with WalWriter(path) as writer:
        writer.append_put(b"alpha", b"one")

    raw = bytearray(path.read_bytes())
    (record,) = decode_all(bytes(raw))

    key_length_offset = RECORD_HEADER_SIZE + 1
    raw[key_length_offset] = 2  # claim a 2 byte key instead of a 5 byte one
    payload = bytes(raw[RECORD_HEADER_SIZE:])
    assert zlib.crc32(payload) & 0xFFFFFFFF != record.checksum


def test_records_land_sequentially_with_no_gaps_or_overlaps(tmp_path: Path) -> None:
    path = tmp_path / "sequential.wal"
    written = [(b"k1", b"v1"), (b"k2", b"value-two"), (b"k3", b""), (b"k4", b"x" * 300)]

    offsets: list[int] = []
    with WalWriter(path) as writer:
        for key, value in written:
            offsets.append(writer.append_put(key, value))
        offsets.append(writer.append_delete(b"k5"))

    raw = path.read_bytes()
    records = decode_all(raw)
    assert [(r.key, r.value) for r in records] == [*written, (b"k5", b"")]
    assert [r.offset for r in records] == offsets

    expected_offset = 0
    for record in records:
        assert record.offset == expected_offset
        expected_offset += record.total_size
    assert expected_offset == len(raw)


def test_reopening_appends_without_disturbing_existing_records(tmp_path: Path) -> None:
    path = tmp_path / "reopen.wal"
    with WalWriter(path) as writer:
        writer.append_put(b"k1", b"v1")
        writer.append_put(b"k2", b"v2")
    prefix = path.read_bytes()

    with WalWriter(path) as writer:
        third_offset = writer.append_put(b"k3", b"v3")

    raw = path.read_bytes()
    assert raw[: len(prefix)] == prefix
    assert third_offset == len(prefix)
    assert [r.key for r in decode_all(raw)] == [b"k1", b"k2", b"k3"]


class _SeekRecordingFile:
    """Delegating wrapper that records any backward positioning call."""

    def __init__(self, wrapped: object) -> None:
        self._wrapped = wrapped
        self.violations: list[str] = []

    def tell(self) -> int:
        return self._wrapped.tell()

    def write(self, data: bytes) -> int:
        return self._wrapped.write(data)

    def flush(self) -> None:
        self._wrapped.flush()

    def close(self) -> None:
        self._wrapped.close()

    def seek(self, *args: object, **kwargs: object) -> int:
        self.violations.append("seek")
        return self._wrapped.seek(*args, **kwargs)

    def truncate(self, *args: object, **kwargs: object) -> int:
        self.violations.append("truncate")
        return self._wrapped.truncate(*args, **kwargs)


def test_appending_never_seeks_backwards_or_truncates(tmp_path: Path) -> None:
    path = tmp_path / "append_only.wal"
    writer = WalWriter(path)
    guard = _SeekRecordingFile(writer._file)
    writer._file = guard
    try:
        for index in range(10):
            writer.append_put(f"k{index}".encode(), b"v")
        writer.append_delete(b"k0")
    finally:
        writer.close()

    assert guard.violations == []
    assert len(decode_all(path.read_bytes())) == 11


def test_concurrent_appends_produce_intact_records(tmp_path: Path) -> None:
    """The engine writes to the WAL from several threads, so records must not interleave."""
    path = tmp_path / "concurrent.wal"
    thread_count = 8
    per_thread = 50
    barrier = threading.Barrier(thread_count)
    failures: list[BaseException] = []

    with WalWriter(path) as writer:

        def append_many(thread_index: int) -> None:
            try:
                barrier.wait()
                for record_index in range(per_thread):
                    key = f"t{thread_index}-k{record_index}".encode()
                    writer.append_put(key, bytes([thread_index]) * (record_index % 17))
            except BaseException as exc:
                # Collected rather than swallowed: the main thread asserts this is empty.
                failures.append(exc)

        threads = [
            threading.Thread(target=append_many, args=(index,)) for index in range(thread_count)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    assert failures == []

    records = decode_all(path.read_bytes())  # decode_all asserts every checksum
    assert len(records) == thread_count * per_thread
    expected = {
        f"t{thread_index}-k{record_index}".encode(): bytes([thread_index]) * (record_index % 17)
        for thread_index in range(thread_count)
        for record_index in range(per_thread)
    }
    assert {record.key: record.value for record in records} == expected


def test_append_after_close_raises(tmp_path: Path) -> None:
    writer = WalWriter(tmp_path / "closed.wal")
    writer.append_put(b"k", b"v")
    writer.close()
    writer.close()  # idempotent
    assert writer.closed
    with pytest.raises(ValueError, match="closed"):
        writer.append_put(b"k", b"v")


def test_encode_record_rejects_oversized_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(wal, "MAX_PAYLOAD_SIZE", PAYLOAD_HEADER_SIZE + 8)
    encode_record(WalOp.PUT, b"key", b"value")  # exactly at the limit
    with pytest.raises(WalFormatError, match="format limit"):
        encode_record(WalOp.PUT, b"key", b"value!")


def test_encode_record_rejects_non_bytes_and_valued_deletes() -> None:
    with pytest.raises(TypeError, match="op must be a WalOp"):
        encode_record(1, b"alpha", b"one")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="key must be bytes"):
        encode_record(WalOp.PUT, "alpha", b"one")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="value must be bytes"):
        encode_record(WalOp.PUT, b"alpha", "one")  # type: ignore[arg-type]
    with pytest.raises(WalFormatError, match="empty value"):
        encode_record(WalOp.DELETE, b"alpha", b"one")


def test_op_codes_never_decode_from_zero_bytes() -> None:
    """A run of zeroes is the usual shape of a torn tail, so 0 must not be a valid op."""
    assert 0 not in {int(op) for op in WalOp}


def test_writer_exposes_path_and_releases_the_file(tmp_path: Path) -> None:
    path = tmp_path / "handle.wal"
    with WalWriter(path) as writer:
        assert writer.path == path
        assert not writer.closed
    assert writer.closed
