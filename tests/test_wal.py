"""Tests for the WAL file header, record framing, append-only writer, fsync policy
and sequential reader.

Covers stories M1.1 (record framing, append writer), M1.2 (file header with a
format version byte), M1.3 (configurable fsync policy) and M1.4 (sequential
reader).

The writer tests decode bytes with plain ``struct`` calls rather than with the
reader from the library, because the point of those tests is that what lands on
disk matches the documented layout. A decoder that shared code with the encoder
could agree with it and still be wrong about the format. The reader tests go the
other way: they build files the writer would never produce, so that the reader's
bounds checks are exercised against real damage rather than against well-formed
input.
"""

from __future__ import annotations

import builtins
import io
import os
import struct
import threading
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from ledgerlog import wal
from ledgerlog.wal import (
    DEFAULT_FSYNC_INTERVAL_SECONDS,
    FILE_HEADER_SIZE,
    MAX_PAYLOAD_SIZE,
    PAYLOAD_HEADER_SIZE,
    RECORD_HEADER_SIZE,
    WAL_FORMAT_VERSION,
    WAL_MAGIC,
    FsyncPolicy,
    WalFormatError,
    WalHeaderError,
    WalInvalidRecordError,
    WalOp,
    WalReader,
    WalTruncatedRecordError,
    WalUnsupportedVersionError,
    WalWriter,
    decode_payload,
    encode_file_header,
    encode_record,
    iter_records,
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

    def fileno(self) -> int:
        return self._wrapped.fileno()

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


# --- Configurable fsync policy (story M1.3) ----------------------------------


class _FsyncRecorder:
    """Stand-in for ``os.fsync`` that records each call and then syncs for real.

    Counting calls is the only way to observe an fsync from the outside: the
    acceptance criteria for this story are about how often the writer syncs, and
    nothing in the file's own bytes says whether they reached the platter. Each
    call also captures the file's inode and size as the kernel sees them at that
    moment, which is what lets a test assert the sync targeted the WAL itself and
    happened after the record was flushed, not before.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[int, int]] = []
        self._real_fsync = os.fsync

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(os, "fsync", self)

    def __call__(self, fd: int) -> None:
        stat = os.fstat(fd)
        self.calls.append((stat.st_ino, stat.st_size))
        self._real_fsync(fd)

    @property
    def count(self) -> int:
        return len(self.calls)


class _FakeClock:
    """Monotonic clock a test advances by hand, so cadence tests never sleep."""

    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_default_policy_is_always(tmp_path: Path) -> None:
    """A write-ahead log's default must be the durable one."""
    with WalWriter(tmp_path / "default.wal") as writer:
        assert writer.fsync_policy is FsyncPolicy.ALWAYS
        assert writer.fsync_interval_seconds == DEFAULT_FSYNC_INTERVAL_SECONDS


@pytest.mark.parametrize("policy", list(FsyncPolicy))
def test_writer_accepts_each_policy_as_an_enum_or_as_its_string(
    tmp_path: Path, policy: FsyncPolicy
) -> None:
    with WalWriter(tmp_path / f"{policy.value}-enum.wal", fsync_policy=policy) as writer:
        assert writer.fsync_policy is policy
    with WalWriter(tmp_path / f"{policy.value}-str.wal", fsync_policy=policy.value) as writer:
        assert writer.fsync_policy is policy


def test_unknown_policy_is_rejected_before_the_file_is_created(tmp_path: Path) -> None:
    path = tmp_path / "unknown_policy.wal"
    with pytest.raises(ValueError, match="unknown fsync policy") as excinfo:
        WalWriter(path, fsync_policy="sometimes")
    for policy in FsyncPolicy:
        assert policy.value in str(excinfo.value)
    assert not path.exists(), "a rejected policy must not leave a WAL behind"


@pytest.mark.parametrize("interval", [0, -1.0, float("nan"), float("inf")])
def test_unusable_fsync_interval_is_rejected(tmp_path: Path, interval: float) -> None:
    """A NaN interval would silently downgrade the interval policy to never."""
    path = tmp_path / "bad_interval.wal"
    with pytest.raises(ValueError, match="finite positive"):
        WalWriter(path, fsync_policy=FsyncPolicy.INTERVAL, fsync_interval_seconds=interval)
    assert not path.exists()


def test_always_policy_fsyncs_before_each_append_returns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "always.wal"
    recorder = _FsyncRecorder()
    recorder.install(monkeypatch)

    with WalWriter(path, fsync_policy=FsyncPolicy.ALWAYS) as writer:
        assert recorder.count == 1, "the file header is a durable write too"
        for index in range(5):
            expected_count = recorder.count + 1
            writer.append_put(f"k{index}".encode(), b"value")
            assert recorder.count == expected_count, "append returned without fsyncing"
            synced_inode, synced_size = recorder.calls[-1]
            assert synced_inode == path.stat().st_ino, "fsynced a file other than the WAL"
            assert synced_size == path.stat().st_size, "fsynced before the record was flushed"
        writer.append_delete(b"k0")
        assert recorder.count == 7

    assert recorder.count == 7, "close has nothing left to sync under the always policy"
    assert len(decode_all(path.read_bytes())) == 6


def test_never_policy_makes_no_fsync_calls(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "never.wal"
    recorder = _FsyncRecorder()
    recorder.install(monkeypatch)

    with WalWriter(path, fsync_policy=FsyncPolicy.NEVER) as writer:
        for index in range(20):
            writer.append_put(f"k{index}".encode(), b"value")
        writer.append_delete(b"k0")

    assert recorder.count == 0, "the never policy must not fsync, not even on close"
    assert len(decode_all(path.read_bytes())) == 21, "records still reach the file"


def test_interval_policy_fsyncs_on_cadence_not_on_every_append(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "interval.wal"
    recorder = _FsyncRecorder()
    recorder.install(monkeypatch)
    clock = _FakeClock()

    with WalWriter(
        path,
        fsync_policy=FsyncPolicy.INTERVAL,
        fsync_interval_seconds=1.0,
        clock=clock,
    ) as writer:
        assert recorder.count == 0, "opening a file does not start the cadence with a sync"

        for index in range(10):
            writer.append_put(f"early{index}".encode(), b"value")
        assert recorder.count == 0, "synced before the interval elapsed"

        clock.advance(1.0)
        writer.append_put(b"due", b"value")
        assert recorder.count == 1, "the first append past the deadline must sync"
        assert recorder.calls[-1][1] == path.stat().st_size

        for index in range(10):
            writer.append_put(f"late{index}".encode(), b"value")
        assert recorder.count == 1, "the deadline must reset after a sync"

        clock.advance(2.5)
        writer.append_put(b"due-again", b"value")
        assert recorder.count == 2

    assert recorder.count == 2, "nothing was pending, so close syncs nothing"
    assert len(decode_all(path.read_bytes())) == 22


def test_interval_policy_syncs_the_pending_tail_on_close(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Closing is not permission to drop records the cadence has not reached yet."""
    path = tmp_path / "interval_close.wal"
    recorder = _FsyncRecorder()
    recorder.install(monkeypatch)
    clock = _FakeClock()

    writer = WalWriter(
        path,
        fsync_policy=FsyncPolicy.INTERVAL,
        fsync_interval_seconds=60.0,
        clock=clock,
    )
    writer.append_put(b"tail", b"value")
    assert recorder.count == 0

    writer.close()
    assert recorder.count == 1
    assert recorder.calls[-1][1] == path.stat().st_size

    writer.close()
    assert recorder.count == 1, "close is idempotent and must not sync twice"


def test_sync_forces_a_sync_whatever_the_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "explicit_sync.wal"
    recorder = _FsyncRecorder()
    recorder.install(monkeypatch)

    with WalWriter(path, fsync_policy=FsyncPolicy.NEVER) as writer:
        writer.append_put(b"k", b"value")
        assert recorder.count == 0
        writer.sync()
        assert recorder.count == 1
        assert recorder.calls[-1][1] == path.stat().st_size

    assert recorder.count == 1, "an explicit sync leaves nothing for close to do"


def test_explicit_sync_restarts_the_interval_cadence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "sync_resets.wal"
    recorder = _FsyncRecorder()
    recorder.install(monkeypatch)
    clock = _FakeClock()

    with WalWriter(
        path,
        fsync_policy=FsyncPolicy.INTERVAL,
        fsync_interval_seconds=1.0,
        clock=clock,
    ) as writer:
        writer.append_put(b"k1", b"value")
        clock.advance(0.9)
        writer.sync()
        assert recorder.count == 1

        clock.advance(0.5)  # 1.4s since the append, but only 0.5s since the sync
        writer.append_put(b"k2", b"value")
        assert recorder.count == 1, "the deadline must run from the last sync"

        clock.advance(0.5)
        writer.append_put(b"k3", b"value")
        assert recorder.count == 2


def test_sync_after_close_raises(tmp_path: Path) -> None:
    writer = WalWriter(tmp_path / "closed_sync.wal", fsync_policy=FsyncPolicy.NEVER)
    writer.close()
    with pytest.raises(ValueError, match="closed"):
        writer.sync()


def _append_from_threads(writer: WalWriter, thread_count: int, per_thread: int) -> list[Exception]:
    """Run ``thread_count`` threads appending ``per_thread`` records each, all at once."""
    barrier = threading.Barrier(thread_count)
    failures: list[Exception] = []

    def append_many(thread_index: int) -> None:
        try:
            barrier.wait()
            for record_index in range(per_thread):
                writer.append_put(f"t{thread_index}-k{record_index}".encode(), b"value")
        except Exception as exc:
            # Collected rather than swallowed: the caller asserts this is empty.
            failures.append(exc)

    threads = [threading.Thread(target=append_many, args=(index,)) for index in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return failures


def test_always_policy_syncs_once_per_append_under_concurrency(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Concurrent appenders each get their own sync, none piggybacks on another's."""
    path = tmp_path / "concurrent_always.wal"
    recorder = _FsyncRecorder()
    recorder.install(monkeypatch)
    thread_count, per_thread = 4, 20

    with WalWriter(path, fsync_policy=FsyncPolicy.ALWAYS) as writer:
        failures = _append_from_threads(writer, thread_count, per_thread)

    assert failures == []
    assert recorder.count == thread_count * per_thread + 1  # the records plus the header
    assert len(decode_all(path.read_bytes())) == thread_count * per_thread


def test_interval_policy_deadline_is_not_raced_by_concurrent_appenders(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The cadence state is shared, so it is only correct if the lock covers it.

    The clock is frozen for the duration, which makes the expected sync count
    exact (zero while appending, one for the pending tail at close) no matter how
    the threads interleave. A racy deadline update would show up as a sync that
    nothing was due for.
    """
    path = tmp_path / "concurrent_interval.wal"
    recorder = _FsyncRecorder()
    recorder.install(monkeypatch)
    clock = _FakeClock()
    thread_count, per_thread = 8, 25

    writer = WalWriter(
        path,
        fsync_policy=FsyncPolicy.INTERVAL,
        fsync_interval_seconds=30.0,
        clock=clock,
    )
    try:
        failures = _append_from_threads(writer, thread_count, per_thread)
    finally:
        writer.close()

    assert failures == []
    assert recorder.count == 1, "only the close-time sync of the pending tail was due"

    records = decode_all(path.read_bytes())  # decode_all asserts every checksum
    assert len(records) == thread_count * per_thread
    assert {record.key for record in records} == {
        f"t{thread_index}-k{record_index}".encode()
        for thread_index in range(thread_count)
        for record_index in range(per_thread)
    }


# --- Sequential reader (story M1.4) ------------------------------------------


def write_raw_wal(path: Path, *record_bytes: bytes) -> Path:
    """Write a WAL file with a valid header followed by the given raw record bytes.

    Built here rather than through WalWriter because most of these tests need a
    file the writer would never produce (a length field claiming more bytes than
    exist, an op code no writer emits), which is exactly what a reader has to
    survive.
    """
    path.write_bytes(encode_file_header() + b"".join(record_bytes))
    return path


class _ReadSizeRecorder(io.BytesIO):
    """A BytesIO that remembers the largest single read it was ever asked for.

    Used to assert that a corrupted length field never reaches a read call: the
    difference between rejecting a bogus length and honoring it is invisible in
    the raised exception but very visible in how many bytes were requested.
    """

    def __init__(self, data: bytes) -> None:
        super().__init__(data)
        self.max_read_request = 0

    def read(self, size: int | None = -1, /) -> bytes:
        if size is not None and size >= 0:
            self.max_read_request = max(self.max_read_request, size)
        return super().read(size)


def read_all(path: Path) -> list[wal.WalRecord]:
    with WalReader(path) as reader:
        return list(reader)


def test_reader_yields_every_record_in_write_order(tmp_path: Path) -> None:
    path = tmp_path / "order.wal"
    expected = [
        (WalOp.PUT, b"k1", b"v1"),
        (WalOp.DELETE, b"k1", b""),
        (WalOp.PUT, b"k2", b""),
        (WalOp.PUT, b"\x00\xffbinary", b"\x01\x02\x03"),
        (WalOp.PUT, b"k3", b"x" * 5000),
        (WalOp.DELETE, b"", b""),
    ]

    offsets: list[int] = []
    with WalWriter(path) as writer:
        for op, key, value in expected:
            if op is WalOp.PUT:
                offsets.append(writer.append_put(key, value))
            else:
                offsets.append(writer.append_delete(key))

    records = read_all(path)
    assert [(r.op, r.key, r.value) for r in records] == expected
    assert [r.offset for r in records] == offsets


def test_reader_reports_contiguous_record_spans(tmp_path: Path) -> None:
    """Each record's end offset is the next one's start, and the last one ends at EOF."""
    path = tmp_path / "spans.wal"
    with WalWriter(path) as writer:
        writer.append_put(b"alpha", b"one")
        writer.append_delete(b"beta")
        writer.append_put(b"gamma", b"three")

    records = read_all(path)
    assert records[0].offset == FILE_HEADER_SIZE
    for earlier, later in zip(records, records[1:], strict=False):
        assert earlier.end_offset == later.offset
    assert records[-1].end_offset == path.stat().st_size


def test_reader_round_trips_many_records(tmp_path: Path) -> None:
    path = tmp_path / "many.wal"
    written = [(f"key-{index:04d}".encode(), b"v" * (index % 37)) for index in range(500)]

    with WalWriter(path, fsync_policy=FsyncPolicy.NEVER) as writer:
        for key, value in written:
            writer.append_put(key, value)

    records = read_all(path)
    assert [(r.key, r.value) for r in records] == written


def test_reader_on_header_only_file_yields_no_records(tmp_path: Path) -> None:
    path = tmp_path / "empty.wal"
    with WalWriter(path):
        pass

    assert path.stat().st_size == FILE_HEADER_SIZE
    assert read_all(path) == []


def test_reader_validates_the_header_before_any_record(tmp_path: Path) -> None:
    """A file this build cannot parse is rejected at open, not decoded as records."""
    record = encode_record(WalOp.PUT, b"alpha", b"one")

    future = tmp_path / "future.wal"
    future.write_bytes(encode_file_header(WAL_FORMAT_VERSION + 1) + record)
    with pytest.raises(WalUnsupportedVersionError):
        WalReader(future)

    foreign = tmp_path / "foreign.wal"
    foreign.write_bytes(b"NOTAWAL!" + bytes([WAL_FORMAT_VERSION]) + record)
    with pytest.raises(WalHeaderError):
        WalReader(foreign)

    empty = tmp_path / "zero.wal"
    empty.write_bytes(b"")
    with pytest.raises(WalHeaderError):
        WalReader(empty)


def test_reader_rejects_a_length_reaching_past_the_end_of_the_file(tmp_path: Path) -> None:
    """The prior records still come back; the record that overruns the file does not."""
    good = [encode_record(WalOp.PUT, b"k1", b"v1"), encode_record(WalOp.DELETE, b"k2", b"")]
    overrun_payload = struct.pack("<BI", int(WalOp.PUT), 3) + b"key" + b"value"
    # A header claiming twice the payload that follows it.
    overrun = struct.pack("<II", len(overrun_payload) * 2, 0) + overrun_payload
    path = write_raw_wal(tmp_path / "overrun.wal", *good, overrun)

    with WalReader(path) as reader:
        iterator = iter(reader)
        recovered = [next(iterator), next(iterator)]
        with pytest.raises(WalTruncatedRecordError) as excinfo:
            next(iterator)

    assert [(r.op, r.key, r.value) for r in recovered] == [
        (WalOp.PUT, b"k1", b"v1"),
        (WalOp.DELETE, b"k2", b""),
    ]
    assert excinfo.value.offset == FILE_HEADER_SIZE + len(good[0]) + len(good[1])


@pytest.mark.parametrize(
    ("kept_payload_bytes", "description"),
    [
        (-6, "mid length field"),
        (-3, "mid checksum"),
        (2, "mid payload"),
    ],
)
def test_reader_rejects_a_truncated_tail_record(
    tmp_path: Path, kept_payload_bytes: int, description: str
) -> None:
    """A crash mid-append leaves a short tail, whichever field it lands in."""
    complete = encode_record(WalOp.PUT, b"k1", b"v1")
    torn = encode_record(WalOp.PUT, b"k2", b"v2")
    kept = RECORD_HEADER_SIZE + kept_payload_bytes
    path = write_raw_wal(tmp_path / "torn.wal", complete, torn[:kept])

    with WalReader(path) as reader:
        iterator = iter(reader)
        first = next(iterator)
        with pytest.raises(WalTruncatedRecordError) as excinfo:
            next(iterator)

    assert (first.key, first.value) == (b"k1", b"v1"), description
    assert excinfo.value.offset == FILE_HEADER_SIZE + len(complete)


def test_reader_rejects_a_length_above_the_format_limit_without_over_reading(
    tmp_path: Path,
) -> None:
    """A 32 bit length off a damaged disk must not become a multi-gigabyte allocation."""
    header = struct.pack("<II", MAX_PAYLOAD_SIZE + 1, 0)
    raw = encode_file_header() + header

    stream = _ReadSizeRecorder(raw)
    assert read_file_header(stream) == WAL_FORMAT_VERSION
    with pytest.raises(WalInvalidRecordError) as excinfo:
        list(iter_records(stream))

    assert excinfo.value.offset == FILE_HEADER_SIZE
    assert stream.max_read_request <= len(raw)

    path = write_raw_wal(tmp_path / "huge.wal", header)
    with pytest.raises(WalInvalidRecordError):
        read_all(path)


def test_reader_never_requests_more_bytes_than_the_file_holds() -> None:
    """Every read is bounded by the file, including for a length that merely overruns it."""
    payload = struct.pack("<BI", int(WalOp.PUT), 3) + b"key"
    raw = encode_file_header() + struct.pack("<II", 4096, 0) + payload

    stream = _ReadSizeRecorder(raw)
    read_file_header(stream)
    with pytest.raises(WalTruncatedRecordError):
        list(iter_records(stream))

    assert stream.max_read_request <= len(raw)


def test_reader_rejects_a_payload_length_below_the_minimum(tmp_path: Path) -> None:
    path = write_raw_wal(tmp_path / "short.wal", struct.pack("<II", PAYLOAD_HEADER_SIZE - 1, 0))
    with pytest.raises(WalInvalidRecordError):
        read_all(path)


def test_reader_rejects_a_key_length_reaching_past_its_payload(tmp_path: Path) -> None:
    """The key/value boundary is bounds checked, not clamped to whatever is there."""
    payload = struct.pack("<BI", int(WalOp.PUT), 99) + b"key" + b"value"
    record = struct.pack("<II", len(payload), zlib.crc32(payload) & 0xFFFFFFFF) + payload
    path = write_raw_wal(tmp_path / "boundary.wal", record)

    with pytest.raises(WalInvalidRecordError) as excinfo:
        read_all(path)
    assert excinfo.value.offset == FILE_HEADER_SIZE


def test_reader_rejects_an_unknown_op_code(tmp_path: Path) -> None:
    payload = struct.pack("<BI", 0, 3) + b"key"
    record = struct.pack("<II", len(payload), zlib.crc32(payload) & 0xFFFFFFFF) + payload
    path = write_raw_wal(tmp_path / "op.wal", record)

    with pytest.raises(WalInvalidRecordError):
        read_all(path)


def test_reader_rejects_a_delete_carrying_a_value(tmp_path: Path) -> None:
    """The empty-value convention is a format rule, so the reader enforces it too."""
    payload = struct.pack("<BI", int(WalOp.DELETE), 3) + b"key" + b"value"
    record = struct.pack("<II", len(payload), zlib.crc32(payload) & 0xFFFFFFFF) + payload
    path = write_raw_wal(tmp_path / "valued-delete.wal", record)

    with pytest.raises(WalInvalidRecordError):
        read_all(path)


def test_decode_payload_reports_the_offset_it_was_given() -> None:
    with pytest.raises(WalInvalidRecordError) as excinfo:
        decode_payload(struct.pack("<BI", int(WalOp.PUT), 10) + b"key", offset=123)
    assert excinfo.value.offset == 123
    assert "123" in str(excinfo.value)

    with pytest.raises(WalInvalidRecordError):
        decode_payload(b"\x01\x02")


def test_reading_a_damaged_file_leaves_its_bytes_untouched(tmp_path: Path) -> None:
    """Truncating at the first bad record is M1.5's job; looking must not change the log."""
    complete = encode_record(WalOp.PUT, b"k1", b"v1")
    torn = encode_record(WalOp.PUT, b"k2", b"v2")[:4]
    path = write_raw_wal(tmp_path / "untouched.wal", complete, torn)
    before = path.read_bytes()

    with pytest.raises(WalTruncatedRecordError):
        read_all(path)

    assert path.read_bytes() == before


def test_reader_can_be_iterated_more_than_once(tmp_path: Path) -> None:
    path = tmp_path / "reiterate.wal"
    with WalWriter(path) as writer:
        writer.append_put(b"alpha", b"one")
        writer.append_delete(b"beta")

    with WalReader(path) as reader:
        first_pass = [(r.op, r.key, r.value) for r in reader]
        second_pass = [(r.op, r.key, r.value) for r in reader]

    assert first_pass == second_pass
    assert len(first_pass) == 2


def test_records_appended_after_open_are_outside_the_reader_snapshot(tmp_path: Path) -> None:
    """The size sampled at open bounds the pass, so a log being appended to still ends."""
    path = tmp_path / "snapshot.wal"
    with WalWriter(path, fsync_policy=FsyncPolicy.NEVER) as writer:
        writer.append_put(b"before", b"1")
        writer.sync()

        with WalReader(path) as reader:
            writer.append_put(b"after", b"2")
            writer.sync()
            keys = [record.key for record in reader]

    assert keys == [b"before"]
    assert [record.key for record in read_all(path)] == [b"before", b"after"]


def test_reader_releases_its_file_handle(tmp_path: Path) -> None:
    path = tmp_path / "handle.wal"
    with WalWriter(path) as writer:
        writer.append_put(b"alpha", b"one")

    reader = WalReader(path)
    assert reader.path == path
    assert reader.format_version == WAL_FORMAT_VERSION
    assert reader.file_size == path.stat().st_size
    assert not reader.closed

    handle = reader._file
    reader.close()
    reader.close()  # idempotent
    assert reader.closed
    assert handle.closed

    with pytest.raises(ValueError, match="closed"):
        iter(reader)


def test_rejected_reader_open_does_not_leak_a_file_handle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "bad-version.wal"
    path.write_bytes(encode_file_header(WAL_FORMAT_VERSION + 1))

    opened: list[Any] = []
    real_open = builtins.open

    def tracking_open(*args: Any, **kwargs: Any) -> Any:
        handle = real_open(*args, **kwargs)
        opened.append(handle)
        return handle

    monkeypatch.setattr(builtins, "open", tracking_open)
    with pytest.raises(WalUnsupportedVersionError):
        WalReader(path)

    assert opened, "the reader opened no file at all"
    assert all(handle.closed for handle in opened)


def test_iter_records_needs_a_bound_it_can_check_lengths_against() -> None:
    class _Unseekable(io.RawIOBase):
        def readable(self) -> bool:
            return True

        def seekable(self) -> bool:
            return False

    with pytest.raises(ValueError, match="seekable"):
        iter_records(_Unseekable())


def test_iter_records_accepts_an_explicit_file_size() -> None:
    record = encode_record(WalOp.PUT, b"alpha", b"one")
    stream = io.BytesIO(encode_file_header() + record)
    read_file_header(stream)

    records = list(iter_records(stream, file_size=FILE_HEADER_SIZE + len(record)))
    assert [(r.op, r.key, r.value) for r in records] == [(WalOp.PUT, b"alpha", b"one")]


def test_reading_while_a_writer_appends_yields_whole_records_in_order(tmp_path: Path) -> None:
    """A read overlapping an append yields whole records in order, or stops at the tail.

    Threads rather than an interleaved single-threaded sequence, because the
    claim is about a read that overlaps an append in time, and only a real
    writer thread running against real reader threads can put the reader's size
    sample in the middle of the writer's work.

    A torn tail is expected here, not a failure. The writer appends through a
    buffered handle, so a record straddling the buffer boundary reaches the
    operating system as two writes, and a reader sampling the file size between
    them sees a header whose payload is not all there yet. What the test holds
    the reader to is what it actually promises: every record handed back is
    complete and in write order, damage only ever appears at the tail and only
    as a truncation, and it is reported at the offset where the last intact
    record ended.
    """
    path = tmp_path / "live.wal"
    record_count = 400
    values = [b"v" * (index % 20) for index in range(record_count)]
    expected_keys = [f"k{index:04d}".encode() for index in range(record_count)]
    # Where each record ends, so a reported truncation offset can be checked
    # against the end of the last record the reader handed back.
    record_ends: list[int] = []
    end = FILE_HEADER_SIZE
    for key, value in zip(expected_keys, values, strict=True):
        end += RECORD_HEADER_SIZE + PAYLOAD_HEADER_SIZE + len(key) + len(value)
        record_ends.append(end)

    failures: list[Exception] = []
    reads: list[tuple[list[bytes], WalTruncatedRecordError | None]] = []
    writing_done = threading.Event()

    def write_records(writer: WalWriter) -> None:
        try:
            for key, value in zip(expected_keys, values, strict=True):
                writer.append_put(key, value)
        except Exception as exc:
            # Collected rather than swallowed: the test asserts this is empty.
            failures.append(exc)
        finally:
            writing_done.set()

    def drain(reader: WalReader) -> tuple[list[bytes], WalTruncatedRecordError | None]:
        keys: list[bytes] = []
        try:
            for record in reader:
                keys.append(record.key)
        except WalTruncatedRecordError as exc:
            return keys, exc
        return keys, None

    def read_until_writing_stops() -> None:
        try:
            while True:
                still_writing = not writing_done.is_set()
                with WalReader(path) as reader:
                    reads.append(drain(reader))
                if not still_writing:
                    return
        except Exception as exc:
            # Any exception other than the tail truncation drain() handles.
            failures.append(exc)

    with WalWriter(path, fsync_policy=FsyncPolicy.NEVER) as writer:
        writer_thread = threading.Thread(target=write_records, args=(writer,))
        reader_threads = [threading.Thread(target=read_until_writing_stops) for _ in range(4)]
        writer_thread.start()
        for thread in reader_threads:
            thread.start()
        writer_thread.join(timeout=30)
        for thread in reader_threads:
            thread.join(timeout=30)

    assert failures == [], f"reads failed for something other than a torn tail: {failures!r}"
    assert not writer_thread.is_alive() and all(not t.is_alive() for t in reader_threads)
    assert reads, "no reader completed a pass"

    for keys, truncation in reads:
        assert keys == expected_keys[: len(keys)], "a read returned something other than a prefix"
        if truncation is not None:
            expected_offset = record_ends[len(keys) - 1] if keys else FILE_HEADER_SIZE
            assert truncation.offset == expected_offset, (
                "a truncation was reported somewhere other than the end of the last intact record"
            )

    complete = [keys for keys, truncation in reads if truncation is None]
    assert complete, "no reader completed a pass without hitting the tail"
    assert max(len(keys) for keys in complete) == record_count, "no reader saw the finished log"


# --- Torn-write and corruption detection with truncation (story M1.5) --------


def build_log(path: Path, *records: tuple[WalOp, bytes, bytes]) -> list[bytes]:
    """Write a well-formed log through the writer, returning each record's raw bytes.

    The raw bytes are what the damage in these tests is inflicted on: a test that
    wants to cut a record in half, or flip a byte inside one, needs to know
    exactly where that record sits in the file.
    """
    encoded = [encode_record(op, key, value) for op, key, value in records]
    with WalWriter(path) as writer:
        for op, key, value in records:
            if op is WalOp.PUT:
                writer.append_put(key, value)
            else:
                writer.append_delete(key)
    assert path.stat().st_size == FILE_HEADER_SIZE + sum(len(r) for r in encoded)
    return encoded


def corrupt_byte(path: Path, offset: int) -> None:
    """Flip every bit of the byte at ``offset``, in place, leaving the file's size alone."""
    raw = bytearray(path.read_bytes())
    raw[offset] ^= 0xFF
    path.write_bytes(bytes(raw))


def test_replay_returns_every_record_of_an_intact_log(tmp_path: Path) -> None:
    path = tmp_path / "intact.wal"
    build_log(
        path,
        (WalOp.PUT, b"k1", b"v1"),
        (WalOp.DELETE, b"k1", b""),
        (WalOp.PUT, b"k2", b"v2"),
    )
    before = path.read_bytes()

    result = wal.replay(path)

    assert [(r.op, r.key, r.value) for r in result.records] == [
        (WalOp.PUT, b"k1", b"v1"),
        (WalOp.DELETE, b"k1", b""),
        (WalOp.PUT, b"k2", b"v2"),
    ]
    assert result.is_intact
    assert result.stopped_at is None
    assert result.damage is None and result.reason is None
    assert not result.truncated
    assert result.end_offset == path.stat().st_size
    assert path.read_bytes() == before, "an intact log must not be rewritten"


def test_replay_of_a_header_only_log_returns_no_records(tmp_path: Path) -> None:
    path = tmp_path / "empty.wal"
    with WalWriter(path):
        pass

    result = wal.replay(path)

    assert result.records == ()
    assert result.is_intact and not result.truncated
    assert result.end_offset == FILE_HEADER_SIZE
    assert path.stat().st_size == FILE_HEADER_SIZE


@pytest.mark.parametrize(
    ("kept_bytes", "field"),
    [
        (2, "mid length field"),
        (RECORD_HEADER_SIZE - 2, "mid checksum"),
        (RECORD_HEADER_SIZE + 1, "mid op and key length"),
        (RECORD_HEADER_SIZE + PAYLOAD_HEADER_SIZE + 1, "mid key"),
        (RECORD_HEADER_SIZE + PAYLOAD_HEADER_SIZE + 3, "mid value"),
    ],
)
def test_replay_stops_and_truncates_at_a_torn_tail(
    tmp_path: Path, kept_bytes: int, field: str
) -> None:
    """A crash mid-append leaves a short tail, and replay ends the file where it starts."""
    path = tmp_path / "torn.wal"
    encoded = build_log(path, (WalOp.PUT, b"k1", b"v1"), (WalOp.DELETE, b"k0", b""))
    torn = encode_record(WalOp.PUT, b"k2", b"v2")
    assert kept_bytes < len(torn), "the cut must leave a partial record, not a whole one"
    good_end = FILE_HEADER_SIZE + len(encoded[0]) + len(encoded[1])
    with open(path, "ab") as handle:
        handle.write(torn[:kept_bytes])

    result = wal.replay(path)

    assert [(r.op, r.key) for r in result.records] == [
        (WalOp.PUT, b"k1"),
        (WalOp.DELETE, b"k0"),
    ], field
    assert result.stopped_at == good_end, field
    assert result.end_offset == good_end
    assert isinstance(result.damage, WalTruncatedRecordError)
    assert result.damage.offset == good_end
    assert result.reason is not None and str(good_end) in result.reason
    assert result.truncated
    assert path.stat().st_size == good_end, "the torn tail is still on disk"


@pytest.mark.parametrize("kept_bytes", list(range(1, 17)))
def test_replay_handles_a_tail_cut_at_every_byte_offset(tmp_path: Path, kept_bytes: int) -> None:
    """Exhaustive over where a crash can land inside one record, not just three samples."""
    path = tmp_path / "every-cut.wal"
    encoded = build_log(path, (WalOp.PUT, b"k1", b"v1"))
    torn = encode_record(WalOp.PUT, b"k2", b"v2")
    assert len(torn) == 17, "the parametrization above tracks this record's length"
    good_end = FILE_HEADER_SIZE + len(encoded[0])
    with open(path, "ab") as handle:
        handle.write(torn[:kept_bytes])

    result = wal.replay(path)

    assert [(r.key, r.value) for r in result.records] == [(b"k1", b"v1")]
    assert result.stopped_at == good_end
    assert result.truncated and path.stat().st_size == good_end
    assert wal.replay(path).is_intact, "the truncated file is a valid log again"


def test_replay_discards_a_record_whose_checksum_does_not_match(tmp_path: Path) -> None:
    """A checksum mismatch halts replay exactly like a torn write does."""
    path = tmp_path / "checksum.wal"
    encoded = build_log(
        path,
        (WalOp.PUT, b"k1", b"v1"),
        (WalOp.PUT, b"k2", b"v2"),
        (WalOp.PUT, b"k3", b"v3"),
    )
    second_offset = FILE_HEADER_SIZE + len(encoded[0])
    # A byte inside the second record's value: its length field and its framing
    # are untouched, so only the checksum can catch this.
    corrupt_byte(path, second_offset + len(encoded[1]) - 1)

    result = wal.replay(path)

    assert [(r.key, r.value) for r in result.records] == [(b"k1", b"v1")]
    assert result.stopped_at == second_offset
    assert isinstance(result.damage, wal.WalChecksumError)
    assert result.damage.expected != result.damage.found
    assert result.truncated and path.stat().st_size == second_offset


@pytest.mark.parametrize(
    ("field_offset", "field"),
    [
        (0, "op byte"),
        (1, "key length"),
        (PAYLOAD_HEADER_SIZE, "key"),
        (PAYLOAD_HEADER_SIZE + 2, "value"),
    ],
)
def test_corruption_anywhere_in_a_payload_is_caught(
    tmp_path: Path, field_offset: int, field: str
) -> None:
    """Every payload field is covered by the checksum, including the key/value boundary."""
    path = tmp_path / "field.wal"
    encoded = build_log(path, (WalOp.PUT, b"k1", b"v1"), (WalOp.PUT, b"k2", b"v2"))
    second_offset = FILE_HEADER_SIZE + len(encoded[0])
    corrupt_byte(path, second_offset + RECORD_HEADER_SIZE + field_offset)

    result = wal.replay(path)

    assert [r.key for r in result.records] == [b"k1"], field
    assert result.stopped_at == second_offset
    # A damaged op byte or key length is reported as a checksum failure rather
    # than as an unknown op or an out of range key length: the checksum is
    # verified first, so a damaged record is never explained by one of its own
    # untrustworthy fields.
    assert isinstance(result.damage, wal.WalChecksumError), field
    assert path.stat().st_size == second_offset


def test_replay_drops_valid_records_that_follow_a_bad_one(tmp_path: Path) -> None:
    """Halt when torn: a good record after a bad one is not resumed, it is discarded.

    Skipping past the damage would hand recovery a state that never existed, with
    a later write present while the earlier one it depended on is missing.
    """
    path = tmp_path / "halt.wal"
    encoded = build_log(
        path,
        (WalOp.PUT, b"keep", b"1"),
        (WalOp.PUT, b"damaged", b"2"),
        (WalOp.PUT, b"after", b"3"),
        (WalOp.DELETE, b"keep", b""),
    )
    damaged_offset = FILE_HEADER_SIZE + len(encoded[0])
    corrupt_byte(path, damaged_offset + RECORD_HEADER_SIZE + PAYLOAD_HEADER_SIZE)

    result = wal.replay(path)

    assert [r.key for r in result.records] == [b"keep"]
    assert result.stopped_at == damaged_offset
    assert path.stat().st_size == damaged_offset
    # The delete that followed is gone too, so the key it would have removed is
    # still present after recovery.
    assert [r.key for r in wal.replay(path).records] == [b"keep"]


def test_replay_truncates_the_whole_log_when_the_first_record_is_torn(tmp_path: Path) -> None:
    path = tmp_path / "first.wal"
    with WalWriter(path):
        pass
    with open(path, "ab") as handle:
        handle.write(encode_record(WalOp.PUT, b"k1", b"v1")[:5])

    result = wal.replay(path)

    assert result.records == ()
    assert result.stopped_at == FILE_HEADER_SIZE
    assert result.truncated
    assert path.stat().st_size == FILE_HEADER_SIZE, "the header itself must survive"
    assert read_file_header_from_path(path) == WAL_FORMAT_VERSION


def test_replay_can_report_damage_without_touching_the_file(tmp_path: Path) -> None:
    """truncate=False is what an inspection tool uses: find out, change nothing."""
    path = tmp_path / "dry-run.wal"
    encoded = build_log(path, (WalOp.PUT, b"k1", b"v1"))
    with open(path, "ab") as handle:
        handle.write(encode_record(WalOp.PUT, b"k2", b"v2")[:6])
    before = path.read_bytes()

    result = wal.replay(path, truncate=False)

    assert [r.key for r in result.records] == [b"k1"]
    assert result.stopped_at == FILE_HEADER_SIZE + len(encoded[0])
    assert not result.truncated
    assert path.read_bytes() == before


def test_the_log_is_appendable_again_after_replay_truncates_it(tmp_path: Path) -> None:
    """The point of truncating: the next append continues the log, it does not bury the damage."""
    path = tmp_path / "reuse.wal"
    build_log(path, (WalOp.PUT, b"before", b"1"))
    with open(path, "ab") as handle:
        handle.write(encode_record(WalOp.PUT, b"torn", b"2")[:10])

    recovered = wal.replay(path)
    assert [r.key for r in recovered.records] == [b"before"]

    with WalWriter(path) as writer:
        writer.append_put(b"after", b"3")

    replayed = wal.replay(path)
    assert [(r.key, r.value) for r in replayed.records] == [(b"before", b"1"), (b"after", b"3")]
    assert replayed.is_intact and not replayed.truncated
    assert [r.key for r in read_all(path)] == [b"before", b"after"]


def test_replay_refuses_a_file_that_is_not_a_readable_wal(tmp_path: Path) -> None:
    """A header this build cannot parse is raised on, never truncated away."""
    record = encode_record(WalOp.PUT, b"k1", b"v1")

    future = tmp_path / "future.wal"
    future.write_bytes(encode_file_header(WAL_FORMAT_VERSION + 1) + record)
    with pytest.raises(WalUnsupportedVersionError):
        wal.replay(future)
    assert future.stat().st_size == FILE_HEADER_SIZE + len(record)

    foreign = tmp_path / "foreign.wal"
    foreign.write_bytes(b"NOTAWAL!" + bytes([WAL_FORMAT_VERSION]) + record)
    before = foreign.read_bytes()
    with pytest.raises(WalHeaderError):
        wal.replay(foreign)
    assert foreign.read_bytes() == before


def test_replay_forces_the_truncation_onto_the_disk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A truncation left in the page cache can be undone by the next crash."""
    path = tmp_path / "durable.wal"
    build_log(path, (WalOp.PUT, b"k1", b"v1"))
    with open(path, "ab") as handle:
        handle.write(encode_record(WalOp.PUT, b"k2", b"v2")[:7])

    synced: list[int] = []
    real_fsync = os.fsync

    def recording_fsync(fd: int) -> None:
        synced.append(fd)
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", recording_fsync)

    assert wal.replay(path).truncated
    assert synced, "the shrunk file was never fsynced"


def test_replay_releases_every_file_handle_it_opens(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "handles.wal"
    build_log(path, (WalOp.PUT, b"k1", b"v1"))
    with open(path, "ab") as handle:
        handle.write(encode_record(WalOp.PUT, b"k2", b"v2")[:3])

    opened: list[Any] = []
    real_open = builtins.open

    def tracking_open(*args: Any, **kwargs: Any) -> Any:
        handle = real_open(*args, **kwargs)
        opened.append(handle)
        return handle

    monkeypatch.setattr(builtins, "open", tracking_open)
    assert wal.replay(path).truncated

    assert len(opened) >= 2, "replay opened neither a read nor a write handle"
    assert all(handle.closed for handle in opened)


def test_truncation_refuses_to_eat_the_header_or_to_extend_the_file(tmp_path: Path) -> None:
    """Guards on the one call in this module that makes a WAL file shorter.

    Unreachable through replay, whose offsets always land on a record boundary,
    and checked anyway: this is the only code here that can destroy a durable
    record, so a wrong offset must fail loudly rather than quietly reshape the
    file.
    """
    path = tmp_path / "guards.wal"
    build_log(path, (WalOp.PUT, b"k1", b"v1"))
    before = path.read_bytes()

    with pytest.raises(ValueError, match="header"):
        wal._truncate_to(path, FILE_HEADER_SIZE - 1)
    with pytest.raises(ValueError, match="extend"):
        wal._truncate_to(path, len(before) + 1)

    assert path.read_bytes() == before


def test_reader_reports_a_checksum_mismatch_with_both_values(tmp_path: Path) -> None:
    """The read-only path detects the same damage, and still changes nothing."""
    payload = struct.pack("<BI", int(WalOp.PUT), 3) + b"key" + b"value"
    stored_checksum = (zlib.crc32(payload) & 0xFFFFFFFF) ^ 0xFFFFFFFF
    record = struct.pack("<II", len(payload), stored_checksum) + payload
    path = write_raw_wal(tmp_path / "mismatch.wal", record)
    before = path.read_bytes()

    with pytest.raises(wal.WalChecksumError) as excinfo:
        read_all(path)

    assert excinfo.value.offset == FILE_HEADER_SIZE
    assert excinfo.value.expected == stored_checksum
    assert excinfo.value.found == zlib.crc32(payload) & 0xFFFFFFFF
    assert isinstance(excinfo.value, wal.WalRecordError)
    assert path.read_bytes() == before, "reading never writes"


def test_replay_survives_a_writer_killed_mid_append(tmp_path: Path) -> None:
    """End to end: appends with no clean close, a process death mid-record, then recovery.

    The kill is simulated by writing part of a record through a raw handle, which
    is the state a real crash leaves: the bytes that reached the disk before the
    process stopped, and nothing after them.
    """
    path = tmp_path / "crash.wal"
    durable = [(f"key{index}".encode(), f"value{index}".encode()) for index in range(50)]
    writer = WalWriter(path, fsync_policy=FsyncPolicy.ALWAYS)
    for key, value in durable:
        writer.append_put(key, value)
    # No close(): the process is gone. The always policy means every record above
    # is on the disk, so recovery must return all of them.
    partial = encode_record(WalOp.PUT, b"key50", b"value50")
    with open(path, "ab") as handle:
        handle.write(partial[: len(partial) // 2])
        handle.flush()
        os.fsync(handle.fileno())
    writer._file.close()

    result = wal.replay(path)

    assert [(r.key, r.value) for r in result.records] == durable
    assert result.truncated
    assert result.stopped_at == path.stat().st_size
    assert wal.replay(path).is_intact
