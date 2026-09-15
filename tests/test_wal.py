"""Tests for the WAL file header, record framing and the append-only writer.

Covers stories M1.1 (record framing, append writer) and M1.2 (file header with a
format version byte).

The bytes are decoded here with plain ``struct`` calls rather than with a reader
from the library, because the point of these tests is that what lands on disk
matches the documented layout. A decoder that shared code with the encoder could
agree with it and still be wrong about the format. The sequential reader is
story M1.4.
"""

from __future__ import annotations

import builtins
import io
import struct
import threading
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from ledgerlog import wal
from ledgerlog.wal import (
    FILE_HEADER_SIZE,
    PAYLOAD_HEADER_SIZE,
    RECORD_HEADER_SIZE,
    WAL_FORMAT_VERSION,
    WAL_MAGIC,
    WalFormatError,
    WalHeaderError,
    WalOp,
    WalUnsupportedVersionError,
    WalWriter,
    encode_file_header,
    encode_record,
    read_file_header,
    read_file_header_from_path,
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
    """Walk a WAL file's bytes and decode every record, asserting each checksum.

    The header is verified and skipped first, mirroring the order a reader has to
    use: the format version is checked before any record bytes are interpreted.
    """
    assert len(raw) >= FILE_HEADER_SIZE, "file is shorter than a WAL header"
    magic, version = struct.unpack_from("<8sB", raw, 0)
    assert magic == WAL_MAGIC
    assert version == WAL_FORMAT_VERSION

    records: list[DecodedRecord] = []
    offset = FILE_HEADER_SIZE
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
    assert offset == FILE_HEADER_SIZE

    payload_length, checksum = struct.unpack_from("<II", raw, FILE_HEADER_SIZE)
    payload = raw[FILE_HEADER_SIZE + RECORD_HEADER_SIZE :]
    assert payload_length == len(payload)
    assert payload_length == PAYLOAD_HEADER_SIZE + len(b"alpha") + len(b"one")
    assert checksum == zlib.crc32(payload) & 0xFFFFFFFF

    op, key_length = struct.unpack_from("<BI", payload, 0)
    assert op == WalOp.PUT
    assert key_length == len(b"alpha")
    assert payload[PAYLOAD_HEADER_SIZE : PAYLOAD_HEADER_SIZE + key_length] == b"alpha"
    assert payload[PAYLOAD_HEADER_SIZE + key_length :] == b"one"
    assert len(raw) == FILE_HEADER_SIZE + RECORD_HEADER_SIZE + payload_length


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

    payload_start = FILE_HEADER_SIZE + RECORD_HEADER_SIZE
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

    key_length_offset = FILE_HEADER_SIZE + RECORD_HEADER_SIZE + 1
    raw[key_length_offset] = 2  # claim a 2 byte key instead of a 5 byte one
    payload = bytes(raw[FILE_HEADER_SIZE + RECORD_HEADER_SIZE :])
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

    expected_offset = FILE_HEADER_SIZE
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


# --- File header with a format version byte (story M1.2) ---------------------


def test_new_file_starts_with_magic_and_version_byte(tmp_path: Path) -> None:
    path = tmp_path / "header.wal"
    with WalWriter(path) as writer:
        assert writer.format_version == WAL_FORMAT_VERSION

    raw = path.read_bytes()
    assert len(raw) == FILE_HEADER_SIZE, "a WAL with no records is header only"
    assert raw[:8] == WAL_MAGIC
    assert raw[8] == WAL_FORMAT_VERSION


def test_header_precedes_the_first_record(tmp_path: Path) -> None:
    path = tmp_path / "header_first.wal"
    with WalWriter(path) as writer:
        first_offset = writer.append_put(b"alpha", b"one")

    raw = path.read_bytes()
    assert first_offset == FILE_HEADER_SIZE
    assert raw[:FILE_HEADER_SIZE] == encode_file_header()
    assert [record.key for record in decode_all(raw)] == [b"alpha"]


def test_reopening_validates_the_header_without_writing_a_second_one(tmp_path: Path) -> None:
    path = tmp_path / "reopen_header.wal"
    with WalWriter(path) as writer:
        writer.append_put(b"k1", b"v1")
    with WalWriter(path) as writer:
        writer.append_put(b"k2", b"v2")

    raw = path.read_bytes()
    assert raw.count(WAL_MAGIC) == 1
    assert [record.key for record in decode_all(raw)] == [b"k1", b"k2"]


def test_read_file_header_returns_version_and_stops_at_the_first_record(tmp_path: Path) -> None:
    path = tmp_path / "position.wal"
    with WalWriter(path) as writer:
        writer.append_put(b"alpha", b"one")

    with open(path, "rb") as stream:
        assert read_file_header(stream) == WAL_FORMAT_VERSION
        assert stream.tell() == FILE_HEADER_SIZE
        remaining = stream.read()

    assert remaining == path.read_bytes()[FILE_HEADER_SIZE:]
    assert read_file_header_from_path(path) == WAL_FORMAT_VERSION


def test_unrecognized_version_is_rejected_with_a_clear_error(tmp_path: Path) -> None:
    """An unknown version must not be parsed as if it were the current format."""
    path = tmp_path / "future.wal"
    future_version = WAL_FORMAT_VERSION + 1
    # A well-formed record body, so the only thing wrong with the file is its version.
    path.write_bytes(
        encode_file_header(version=future_version) + encode_record(WalOp.PUT, b"alpha", b"one")
    )
    before = path.read_bytes()

    with pytest.raises(WalUnsupportedVersionError) as excinfo:
        read_file_header_from_path(path)
    assert excinfo.value.found_version == future_version
    assert excinfo.value.expected_version == WAL_FORMAT_VERSION
    assert str(future_version) in str(excinfo.value)

    with pytest.raises(WalUnsupportedVersionError):
        WalWriter(path)
    assert path.read_bytes() == before, "a rejected open must not append to the file"


def test_foreign_file_is_rejected_as_a_header_error_not_a_version_error(tmp_path: Path) -> None:
    path = tmp_path / "not_a_wal.wal"
    path.write_bytes(b"SQLite format 3\x00" + b"\x00" * 64)

    with pytest.raises(WalHeaderError, match="magic mismatch") as excinfo:
        read_file_header_from_path(path)
    assert not isinstance(excinfo.value, WalUnsupportedVersionError)

    with pytest.raises(WalHeaderError, match="magic mismatch"):
        WalWriter(path)


@pytest.mark.parametrize("kept_bytes", list(range(FILE_HEADER_SIZE)))
def test_truncated_header_is_rejected_at_every_length(tmp_path: Path, kept_bytes: int) -> None:
    """A header torn partway through is corrupt, not an empty WAL to be re-stamped."""
    path = tmp_path / f"short_{kept_bytes}.wal"
    path.write_bytes(encode_file_header()[:kept_bytes])

    if kept_bytes == 0:
        # An empty file is the one case that is not corruption: it is a fresh WAL.
        with WalWriter(path) as writer:
            writer.append_put(b"alpha", b"one")
        assert path.read_bytes()[:FILE_HEADER_SIZE] == encode_file_header()
        return

    with pytest.raises(WalHeaderError, match="truncated"):
        read_file_header_from_path(path)
    with pytest.raises(WalHeaderError, match="truncated"):
        WalWriter(path)
    assert len(path.read_bytes()) == kept_bytes, "a rejected open must not append to the file"


def test_read_file_header_rejects_a_short_stream_without_over_reading() -> None:
    stream = io.BytesIO(WAL_MAGIC[:4])
    with pytest.raises(WalHeaderError, match="truncated"):
        read_file_header(stream)


def test_encode_file_header_rejects_a_version_outside_one_byte() -> None:
    assert len(encode_file_header()) == FILE_HEADER_SIZE
    with pytest.raises(WalFormatError, match="one byte"):
        encode_file_header(version=256)
    with pytest.raises(WalFormatError, match="one byte"):
        encode_file_header(version=-1)


def test_rejected_open_does_not_leak_a_file_handle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The append handle is opened before the header is validated, so it must be closed."""
    path = tmp_path / "leak.wal"
    path.write_bytes(encode_file_header(version=WAL_FORMAT_VERSION + 1))

    opened: list[Any] = []
    real_open = builtins.open

    def recording_open(*args: Any, **kwargs: Any) -> Any:
        handle = real_open(*args, **kwargs)
        opened.append(handle)
        return handle

    monkeypatch.setattr(builtins, "open", recording_open)
    with pytest.raises(WalUnsupportedVersionError):
        WalWriter(path)

    assert opened, "expected the writer to have opened the file"
    assert all(handle.closed for handle in opened)
