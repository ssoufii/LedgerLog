"""Write-ahead log: file header, record framing and the append-only writer.

Scope of this module today (stories M1.1 and M1.2): encoding a put or a delete
into an on-disk record, stamping every WAL file with a header that carries the
format version, and appending records to a file in call order. The fsync policy
(M1.3), sequential replay (M1.4) and torn-write truncation (M1.5) are separate
stories and are not implemented here.

On-disk file layout (little endian, no padding)::

    [ 8B magic ][ 1B format version ][ record ][ record ]...

On-disk record layout::

    [ 4B payload length ][ 4B CRC32 ][ payload ]

    payload = [ 1B op ][ 4B key length ][ key ][ value ]

Why the header carries a magic string as well as the version byte: a lone
version byte accepts any file whose first byte happens to hold a recognized
number, so pointing the WAL at an unrelated file would be read as a valid log
with a garbage tail. The magic makes "this is not a WAL at all" and "this is a
WAL written by a different version" two distinct, reportable failures.

Why the header is not checksummed: it is nine fixed bytes written once, at
creation, before any record exists. A torn write can leave it short, which a
length check catches, and any corruption of it shows up as an unrecognized
magic or version. A CRC32 here would add a field to validate without covering a
failure the magic and the length check do not already reject.

Why the length and the checksum come first: a reader validates a record before
it trusts any of its contents. The length says how many bytes to read, and the
CRC32 says whether those bytes are the ones that were written. That ordering is
what lets recovery detect a record that was only partially written when the
process died, rather than handing a half-record to the memtable.

Why the key length lives inside the checksummed payload rather than in the
record header: the key length is what splits the key from the value. If it sat
outside the CRC32, a single flipped bit in it would move the key/value boundary
while the checksum still matched, and recovery would happily replay a corrupt
key with a corrupt value. Keeping it inside the payload means any corruption of
the boundary is caught by the same checksum that covers the op, the key and the
value.

The payload length itself is deliberately not covered by the CRC32, because it
is the field a reader needs before it can read anything else. A corrupted length
is still caught: the bytes it selects will not match the stored checksum. The
reader story (M1.4) is responsible for validating a length against the bytes
actually remaining in the file before allocating or slicing, which is why
``MAX_PAYLOAD_SIZE`` below is part of the format rather than a writer detail.
"""

from __future__ import annotations

import os
import struct
import threading
import zlib
from enum import IntEnum
from pathlib import Path
from types import TracebackType
from typing import BinaryIO

WAL_FORMAT_VERSION = 1
"""Version of the file and record layout described in this module's docstring.

Every WAL file stamps this number into its header, and the writer refuses to
append to a file stamped with anything else, so a layout change is detected
instead of silently misread.
"""

WAL_MAGIC = b"LEDGRWAL"
"""Fixed marker at the start of every WAL file. Eight bytes, no terminator."""

_FILE_HEADER_FORMAT = "<8sB"
_RECORD_HEADER_FORMAT = "<II"
_PAYLOAD_HEADER_FORMAT = "<BI"

FILE_HEADER_SIZE = struct.calcsize(_FILE_HEADER_FORMAT)
RECORD_HEADER_SIZE = struct.calcsize(_RECORD_HEADER_FORMAT)
PAYLOAD_HEADER_SIZE = struct.calcsize(_PAYLOAD_HEADER_FORMAT)

MAX_PAYLOAD_SIZE = 64 * 1024 * 1024
"""Largest payload the format allows, in bytes.

This is a format-level bound, not a writer preference. A reader that trusts an
arbitrary 32 bit length read off a corrupted disk can be told to allocate up to
4 GiB for a record that was never written, so the format caps what a length
field is allowed to claim and the writer refuses to produce anything larger.
"""


class WalOp(IntEnum):
    """Operation code stored in the first byte of a record payload.

    Codes start at 1 so that a run of zero bytes, the most common shape of a
    partially written or sparsely allocated region, can never decode as a valid
    operation.
    """

    PUT = 1
    DELETE = 2


class WalFormatError(ValueError):
    """Raised when WAL bytes cannot be represented in, or read as, the on-disk format."""


class WalHeaderError(WalFormatError):
    """Raised when a file's header is missing, too short, or not a WAL header at all.

    Distinct from :class:`WalUnsupportedVersionError` because the two call for
    different responses: this one means the file is not a LedgerLog WAL, while an
    unsupported version means it is one that this build cannot parse.
    """


class WalUnsupportedVersionError(WalHeaderError):
    """Raised when a WAL header carries a format version this build does not know."""

    def __init__(self, found_version: int, expected_version: int = WAL_FORMAT_VERSION) -> None:
        super().__init__(
            f"WAL format version {found_version} is not supported by this build, "
            f"which reads and writes version {expected_version}"
        )
        self.found_version = found_version
        self.expected_version = expected_version


def encode_file_header(version: int = WAL_FORMAT_VERSION) -> bytes:
    """Return the bytes of a WAL file header stamping ``version``.

    The version parameter exists so tests and future migration tooling can write
    a header this build would reject. Normal callers take the default.
    """
    if not 0 <= version <= 0xFF:
        raise WalFormatError(f"WAL format version {version} does not fit in one byte")
    return struct.pack(_FILE_HEADER_FORMAT, WAL_MAGIC, version)


def read_file_header(stream: BinaryIO) -> int:
    """Read and validate a WAL file header from ``stream``, returning its version.

    Reads exactly :data:`FILE_HEADER_SIZE` bytes from the stream's current
    position and leaves it positioned at the first record. A short read is
    treated as a corrupt header rather than as end of file, because a WAL that
    exists at all was created header-first: fewer bytes than a header means the
    file was truncated or was never a WAL.
    """
    raw = stream.read(FILE_HEADER_SIZE)
    if len(raw) < FILE_HEADER_SIZE:
        raise WalHeaderError(
            f"WAL header is {len(raw)} bytes, expected {FILE_HEADER_SIZE}: "
            "the file is truncated or is not a WAL"
        )

    magic, version = struct.unpack(_FILE_HEADER_FORMAT, raw)
    if magic != WAL_MAGIC:
        raise WalHeaderError(f"WAL magic mismatch: found {magic!r}, expected {WAL_MAGIC!r}")
    if version != WAL_FORMAT_VERSION:
        raise WalUnsupportedVersionError(version)
    return version


def read_file_header_from_path(path: str | os.PathLike[str]) -> int:
    """Open ``path``, validate its WAL header, and return the format version."""
    with open(path, "rb") as stream:
        return read_file_header(stream)


def encode_record(op: WalOp, key: bytes, value: bytes) -> bytes:
    """Return the on-disk bytes for a single WAL record.

    The empty-value convention: a DELETE record always carries a zero length
    value. The op byte, not the value, is what marks a key as deleted, so a
    PUT of an empty value and a DELETE of the same key encode differently and
    replay differently.
    """
    if not isinstance(op, WalOp):
        raise TypeError(f"op must be a WalOp, got {type(op).__name__}")
    if not isinstance(key, bytes):
        raise TypeError(f"key must be bytes, got {type(key).__name__}")
    if not isinstance(value, bytes):
        raise TypeError(f"value must be bytes, got {type(value).__name__}")
    if op is WalOp.DELETE and value:
        raise WalFormatError("a DELETE record must carry an empty value")

    payload_size = PAYLOAD_HEADER_SIZE + len(key) + len(value)
    if payload_size > MAX_PAYLOAD_SIZE:
        raise WalFormatError(
            f"record payload of {payload_size} bytes exceeds the "
            f"{MAX_PAYLOAD_SIZE} byte format limit"
        )

    payload = struct.pack(_PAYLOAD_HEADER_FORMAT, int(op), len(key)) + key + value
    checksum = zlib.crc32(payload) & 0xFFFFFFFF
    return struct.pack(_RECORD_HEADER_FORMAT, len(payload), checksum) + payload


class WalWriter:
    """Append-only writer for a WAL file.

    The file is opened in append mode and is never seeked backwards or
    truncated, so a record that has already been written cannot be damaged by a
    later append. Each record is encoded in full before the write call, so a
    record reaches the file as one contiguous run of bytes rather than as a
    sequence of partial writes that another caller could interleave with.

    Appends are guarded by a lock. The engine reaches the WAL from more than one
    thread, and the lock is what makes "records land in call order with no gaps
    or overlaps" true rather than merely likely. Durability is a separate
    concern: this writer flushes to the operating system on every append, but
    when the bytes reach the physical disk is decided by the fsync policy in
    story M1.3.

    Opening a file is where the format version is enforced. A new file is
    stamped with a header before any record can be appended, and an existing one
    has its header validated before the writer will add to it. Doing this at open
    time rather than at replay time means a WAL in an unreadable format is
    rejected while it is still empty of this run's writes, instead of after a
    crash when those writes are the only copy.

    A file whose header is itself incomplete is rejected, not re-stamped. Such a
    file holds no records (the header is written before the first append, in one
    call), so refusing it loses nothing, while silently overwriting a short
    header would also overwrite a foreign file that happened to be short.
    """

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self._path = Path(path)
        self._lock = threading.Lock()
        self._closed = False
        self._file = open(self._path, "ab")
        try:
            if self._file.tell() == 0:
                self._file.write(encode_file_header())
                self._file.flush()
                self._format_version = WAL_FORMAT_VERSION
            else:
                # Validated through a separate read-only handle so the append
                # handle is never seeked, keeping the append-only claim intact.
                self._format_version = read_file_header_from_path(self._path)
        except BaseException:
            self._file.close()
            self._closed = True
            raise

    @property
    def path(self) -> Path:
        """Path of the WAL file being appended to."""
        return self._path

    @property
    def closed(self) -> bool:
        """True once :meth:`close` has run."""
        return self._closed

    @property
    def format_version(self) -> int:
        """Format version in the header of the file being appended to.

        Read from the file's own header on open rather than returned as the
        module constant, so the value reflects what is actually on disk.
        """
        return self._format_version

    def append_put(self, key: bytes, value: bytes) -> int:
        """Append a PUT record and return the byte offset it was written at."""
        return self._append(encode_record(WalOp.PUT, key, value))

    def append_delete(self, key: bytes) -> int:
        """Append a DELETE record and return the byte offset it was written at.

        There is no value parameter: the format's empty-value convention is
        enforced by the API rather than left to the caller to honor.
        """
        return self._append(encode_record(WalOp.DELETE, key, b""))

    def close(self) -> None:
        """Close the underlying file. Safe to call more than once."""
        with self._lock:
            if self._closed:
                return
            try:
                self._file.close()
            finally:
                self._closed = True

    def __enter__(self) -> WalWriter:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def _append(self, record: bytes) -> int:
        with self._lock:
            if self._closed:
                raise ValueError("cannot append to a closed WalWriter")
            self._file.write(record)
            self._file.flush()
            # The start offset is derived after the flush rather than read before the
            # write, because the file is opened O_APPEND: the kernel picks the write
            # position at write time, so a position sampled beforehand is a guess.
            return self._file.tell() - len(record)
