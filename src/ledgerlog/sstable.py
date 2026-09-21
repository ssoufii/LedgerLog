"""SSTable: the immutable, sorted, on-disk form a frozen memtable is flushed into.

Scope of this module today (story M4.1): framing a record for the data block,
streaming a sorted run of records into a file while building a sparse index from
the offsets that stream produces, and reading those two sections back. The
footer that records the format version and the section offsets is M4.2, the
reader that drives a lookup from that footer is M4.3, and the bloom filter
section is written here as an empty placeholder whose real contents land in
M5.3.

On-disk layout (little endian, no padding), with the sections this story writes::

    [ 8B magic ][ 1B format version ]
    [ data block:   record record record ... ]
    [ sparse index: 4B entry count, then entry entry ... ]
    [ bloom filter placeholder: 4B section length, currently zero ]

On-disk record layout::

    [ 4B payload length ][ payload ]

    payload = [ 1B kind ][ 4B key length ][ key ][ value ]

On-disk sparse index entry layout::

    [ 4B key length ][ key ][ 8B absolute byte offset into the data block ]

Why a file header carries the version even though the footer will carry one
too: CLAUDE.md asks that a change to an on-disk layout be detectable rather than
silently misread, and this story introduces a layout. A file whose first bytes
are not this magic is not an SSTable at all, which is a different failure from a
file that is one this build cannot parse, and neither is a question a reader
should have to answer by seeking to the end of a file whose end may not have
been written yet. Once M4.2 adds the footer, the footer is the commit point (a
file is only a complete SSTable once it is there, per ARCHITECTURE.md section 6)
and the header is what says the bytes in front of it are ours to read.

Why the record length prefix is not checksummed the way a WAL record's is: the
two formats are written under different rules. A WAL record is appended live,
one at a time, at the moment a write is acknowledged, so the process can die
between any two bytes and recovery has to be able to spot the record that was
caught in the middle. An SSTable is written once, in full, and only becomes
visible to a reader when it is complete, so the failure a per-record checksum
would catch here (a table that is half written) is caught instead by the
commit rule, and the one it would not catch is unchanged by its absence. What
does have to hold either way is that no length read off the disk is trusted:
every one of them is checked against the bytes actually remaining in its own
section before it is used to slice or allocate, because a damaged length field
is how a parser is talked into an unbounded read.

Why offsets in the index are absolute rather than relative to the start of the
data block: the index is consulted by seeking, and an absolute offset is the
number ``seek`` wants. A relative one would have to be added to a base that the
reader gets from somewhere else (the footer, in M4.2), which means a corrupted
base would silently shift every entry in the index at once rather than showing
up as a single entry that fails its bounds check.
"""

from __future__ import annotations

import os
import struct
from bisect import bisect_right
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path
from types import TracebackType
from typing import BinaryIO

SSTABLE_FORMAT_VERSION = 1
"""Version of the layout described in this module's docstring.

Stamped into every SSTable's file header. A build that reads a different number
reports it rather than parsing bytes whose meaning it is guessing at.
"""

SSTABLE_MAGIC = b"LEDGRSST"
"""Fixed marker at the start of every SSTable file. Eight bytes, no terminator."""

_FILE_HEADER_FORMAT = "<8sB"
_RECORD_LENGTH_FORMAT = "<I"
_RECORD_PAYLOAD_HEADER_FORMAT = "<BI"
_INDEX_COUNT_FORMAT = "<I"
_INDEX_ENTRY_HEADER_FORMAT = "<I"
_INDEX_ENTRY_OFFSET_FORMAT = "<Q"
_BLOOM_PLACEHOLDER_FORMAT = "<I"

FILE_HEADER_SIZE = struct.calcsize(_FILE_HEADER_FORMAT)
RECORD_LENGTH_SIZE = struct.calcsize(_RECORD_LENGTH_FORMAT)
RECORD_PAYLOAD_HEADER_SIZE = struct.calcsize(_RECORD_PAYLOAD_HEADER_FORMAT)
INDEX_COUNT_SIZE = struct.calcsize(_INDEX_COUNT_FORMAT)
INDEX_ENTRY_HEADER_SIZE = struct.calcsize(_INDEX_ENTRY_HEADER_FORMAT)
INDEX_ENTRY_OFFSET_SIZE = struct.calcsize(_INDEX_ENTRY_OFFSET_FORMAT)
BLOOM_PLACEHOLDER_SIZE = struct.calcsize(_BLOOM_PLACEHOLDER_FORMAT)

MAX_RECORD_PAYLOAD_SIZE = 64 * 1024 * 1024
"""Largest record payload the format allows, in bytes.

A format-level bound rather than a writer preference, for the same reason the
WAL has one: a reader handed a corrupted 32 bit length can otherwise be asked to
allocate for a record nobody ever wrote. The writer refuses to produce anything
larger, so a payload above this is damage by definition.
"""

DEFAULT_INDEX_INTERVAL = 64
"""Default number of records between two sparse index entries.

The trade-off, per ARCHITECTURE.md section 3, is index size against the length
of the forward scan a lookup pays after the binary search. Indexing one key in
64 keeps the resident index roughly two orders of magnitude smaller than the
table while bounding the scan at 63 records, which for the record sizes this
engine targets is a small number of sequential bytes off a page or two. The real
value belongs to the tuning spike in M10, which will pick it from measurements
rather than from this reasoning.
"""


class SSTableOp(IntEnum):
    """Kind byte at the front of a record payload.

    Codes start at 1, and match :class:`ledgerlog.wal.WalOp`'s numbering, so a
    record means the same thing at every layer and no run of zero bytes (a
    partially written or sparsely allocated region) can decode as a valid kind.
    They are declared here rather than imported so that CLAUDE.md's rule about
    components staying independently testable holds: the SSTable format does not
    depend on the WAL module to describe itself.
    """

    PUT = 1
    DELETE = 2


class SSTableFormatError(ValueError):
    """Raised when bytes cannot be represented in, or read as, the SSTable format."""


class SSTableHeaderError(SSTableFormatError):
    """Raised when a file's header is missing, too short, or not an SSTable header.

    Distinct from :class:`SSTableUnsupportedVersionError` because the two call
    for different responses: this one says the file is not a LedgerLog SSTable,
    while an unsupported version says it is one this build cannot parse.
    """


class SSTableUnsupportedVersionError(SSTableHeaderError):
    """Raised when an SSTable header carries a format version this build does not know."""

    def __init__(self, found_version: int, expected_version: int = SSTABLE_FORMAT_VERSION) -> None:
        super().__init__(
            f"SSTable format version {found_version} is not supported by this build, "
            f"which reads and writes version {expected_version}"
        )
        self.found_version = found_version
        self.expected_version = expected_version


class SSTableRecordError(SSTableFormatError):
    """Raised when the bytes at a given offset cannot be read as a data block record.

    Carries the offset because that is the part a caller can act on: it says
    where the reader stopped trusting the file, which is what an error report or
    a discard decision is made from.
    """

    def __init__(self, message: str, offset: int) -> None:
        super().__init__(f"{message} (record at byte offset {offset})")
        self.offset = offset


class SSTableTruncatedRecordError(SSTableRecordError):
    """Raised when a record claims more bytes than its section actually holds.

    Kept distinct from :class:`SSTableInvalidRecordError` because a short tail is
    what a file cut off mid-write looks like, while a record whose own fields
    contradict each other means the bytes are damaged rather than merely missing.
    """


class SSTableInvalidRecordError(SSTableRecordError):
    """Raised when a record's fields are internally inconsistent or out of range."""


class SSTableIndexError(SSTableFormatError):
    """Raised when the sparse index cannot be read, or describes something impossible.

    An entry count or key length that reaches past the section, an offset that
    points outside the data block, or keys that are not in ascending order: each
    means the index cannot be the one the writer produced, and a binary search
    over it would return a confidently wrong offset rather than fail.
    """


def encode_file_header(version: int = SSTABLE_FORMAT_VERSION) -> bytes:
    """Return the bytes of an SSTable file header stamping ``version``.

    The version parameter exists so tests and future migration tooling can write
    a header this build would reject. Normal callers take the default.
    """
    if not 0 <= version <= 0xFF:
        raise SSTableFormatError(f"SSTable format version {version} does not fit in one byte")
    return struct.pack(_FILE_HEADER_FORMAT, SSTABLE_MAGIC, version)


def read_file_header(stream: BinaryIO) -> int:
    """Read and validate an SSTable file header from ``stream``, returning its version.

    Reads exactly :data:`FILE_HEADER_SIZE` bytes from the stream's current
    position and leaves it at the first record. A short read is a corrupt header
    rather than an empty table: an SSTable that exists at all was written header
    first, so fewer bytes than a header means the file was cut off or was never
    an SSTable.
    """
    raw = stream.read(FILE_HEADER_SIZE)
    if len(raw) < FILE_HEADER_SIZE:
        raise SSTableHeaderError(
            f"SSTable header is {len(raw)} bytes, expected {FILE_HEADER_SIZE}: "
            "the file is truncated or is not an SSTable"
        )

    magic, version = struct.unpack(_FILE_HEADER_FORMAT, raw)
    if magic != SSTABLE_MAGIC:
        raise SSTableHeaderError(
            f"SSTable magic mismatch: found {magic!r}, expected {SSTABLE_MAGIC!r}"
        )
    if version != SSTABLE_FORMAT_VERSION:
        raise SSTableUnsupportedVersionError(version)
    return version


@dataclass(frozen=True)
class SSTableRecord:
    """One decoded data block record, with the span of the file it came from.

    ``value`` is ``None`` when the record is a tombstone, which is a different
    statement from a key simply being absent: per ARCHITECTURE.md section 5 a
    tombstone has to shadow an older table's value rather than let the read path
    fall through to it, so the two cannot be represented by the same thing.

    Frozen because a record is a decoded view of bytes that are immutable on
    disk, and a mutation on the way to a caller would make what they hold
    disagree with what the table says.
    """

    key: bytes
    value: bytes | None
    offset: int
    end_offset: int

    @property
    def is_tombstone(self) -> bool:
        """True if this record marks the key deleted rather than holding a value."""
        return self.value is None


def encode_record(key: bytes, value: bytes | None) -> bytes:
    """Return the on-disk bytes for one data block record.

    A ``value`` of ``None`` encodes a tombstone, which always carries a zero
    length value: the kind byte, not the value, is what marks a key deleted, so a
    put of an empty value and a delete of the same key encode differently and
    read back differently.
    """
    if not isinstance(key, bytes):
        raise TypeError(f"key must be bytes, got {type(key).__name__}")
    if value is not None and not isinstance(value, bytes):
        raise TypeError(f"value must be bytes or None, got {type(value).__name__}")

    kind = SSTableOp.DELETE if value is None else SSTableOp.PUT
    body = b"" if value is None else value
    payload_size = RECORD_PAYLOAD_HEADER_SIZE + len(key) + len(body)
    if payload_size > MAX_RECORD_PAYLOAD_SIZE:
        raise SSTableFormatError(
            f"record payload of {payload_size} bytes exceeds the "
            f"{MAX_RECORD_PAYLOAD_SIZE} byte format limit"
        )

    payload = struct.pack(_RECORD_PAYLOAD_HEADER_FORMAT, int(kind), len(key)) + key + body
    return struct.pack(_RECORD_LENGTH_FORMAT, len(payload)) + payload


def decode_record_payload(payload: bytes, *, offset: int = 0) -> tuple[bytes, bytes | None]:
    """Decode a record payload into its key and its value or tombstone marker.

    ``offset`` is the record's position in the file and is used only to point
    errors at the right place, so decoding a payload in isolation does not have
    to invent one.

    Every field is checked against the payload's own length before it is used to
    slice. A key length read from a damaged file can claim far more than the
    payload holds, and slicing would quietly hand back a short key instead of
    reporting the damage.
    """
    if len(payload) < RECORD_PAYLOAD_HEADER_SIZE:
        raise SSTableInvalidRecordError(
            f"record payload is {len(payload)} bytes, too short to hold the "
            f"{RECORD_PAYLOAD_HEADER_SIZE} byte kind and key length header",
            offset,
        )

    kind_code, key_length = struct.unpack(
        _RECORD_PAYLOAD_HEADER_FORMAT, payload[:RECORD_PAYLOAD_HEADER_SIZE]
    )
    try:
        kind = SSTableOp(kind_code)
    except ValueError:
        raise SSTableInvalidRecordError(
            f"unknown SSTable record kind {kind_code}", offset
        ) from None

    available = len(payload) - RECORD_PAYLOAD_HEADER_SIZE
    if key_length > available:
        raise SSTableInvalidRecordError(
            f"record claims a {key_length} byte key but only {available} payload bytes follow "
            "its header",
            offset,
        )

    key_end = RECORD_PAYLOAD_HEADER_SIZE + key_length
    key = payload[RECORD_PAYLOAD_HEADER_SIZE:key_end]
    body = payload[key_end:]
    if kind is SSTableOp.DELETE:
        if body:
            raise SSTableInvalidRecordError(
                f"tombstone record carries a {len(body)} byte value, but the format's "
                "empty-value convention requires none",
                offset,
            )
        return key, None
    return key, body


def iter_records(
    stream: BinaryIO, *, start_offset: int, end_offset: int
) -> Iterator[SSTableRecord]:
    """Yield the records stored between two offsets, in the order they were written.

    The bounds are explicit rather than "read until end of file" because the data
    block is a section in the middle of a file, not the whole of it: the sparse
    index follows it, and a reader that ran off the end of the block would start
    decoding index bytes as records. M4.2's footer is what will tell a reader
    where the block ends; until then the writer reports it.

    Iteration is lazy and reads one record at a time. A lookup scans forward from
    an index offset for at most one index interval, so loading the block to reach
    a record near its start would be reading the whole table to avoid reading a
    page of it.

    Being lazy, the iterator seeks on its first step rather than on this call,
    and it moves the one file position the stream has. Two iterators over the
    same handle therefore cannot be advanced alternately, each would pull the
    other's cursor; a caller that wants two live cursors over one table opens
    two handles.

    Every length is bounds checked against the bytes remaining in the section
    before any of them are read, so a corrupted length cannot cause an unbounded
    read or an allocation for a record that was never written. A record that
    reaches past ``end_offset`` raises rather than being trimmed to fit, because
    either means the caller is looking at damage, and guessing what the writer
    meant is how a table invents a record.
    """
    if start_offset < 0:
        raise ValueError(f"start_offset must not be negative, got {start_offset}")
    if end_offset < start_offset:
        raise ValueError(
            f"end_offset {end_offset} is before start_offset {start_offset}, so the data "
            "block has no extent to read"
        )
    return _iter_records(stream, start_offset, end_offset)


def _iter_records(stream: BinaryIO, start_offset: int, end_offset: int) -> Iterator[SSTableRecord]:
    """Generator half of :func:`iter_records`, kept separate so its argument checks run eagerly."""
    stream.seek(start_offset)
    while True:
        offset = stream.tell()
        remaining = end_offset - offset
        if remaining <= 0:
            return
        if remaining < RECORD_LENGTH_SIZE:
            raise SSTableTruncatedRecordError(
                f"{remaining} bytes remain in the data block, fewer than the "
                f"{RECORD_LENGTH_SIZE} byte record length prefix",
                offset,
            )

        raw_length = stream.read(RECORD_LENGTH_SIZE)
        if len(raw_length) < RECORD_LENGTH_SIZE:
            # The file is shorter than the extent the caller was told to read.
            raise SSTableTruncatedRecordError(
                f"read {len(raw_length)} of {RECORD_LENGTH_SIZE} record length bytes", offset
            )

        (payload_length,) = struct.unpack(_RECORD_LENGTH_FORMAT, raw_length)
        if payload_length < RECORD_PAYLOAD_HEADER_SIZE:
            raise SSTableInvalidRecordError(
                f"record claims a {payload_length} byte payload, below the "
                f"{RECORD_PAYLOAD_HEADER_SIZE} byte minimum for a kind and a key length",
                offset,
            )
        if payload_length > MAX_RECORD_PAYLOAD_SIZE:
            raise SSTableInvalidRecordError(
                f"record claims a {payload_length} byte payload, above the "
                f"{MAX_RECORD_PAYLOAD_SIZE} byte format limit",
                offset,
            )
        available = remaining - RECORD_LENGTH_SIZE
        if payload_length > available:
            raise SSTableTruncatedRecordError(
                f"record claims a {payload_length} byte payload but only {available} bytes "
                "remain in the data block",
                offset,
            )

        payload = stream.read(payload_length)
        if len(payload) < payload_length:
            raise SSTableTruncatedRecordError(
                f"read {len(payload)} of {payload_length} payload bytes", offset
            )

        key, value = decode_record_payload(payload, offset=offset)
        yield SSTableRecord(
            key=key,
            value=value,
            offset=offset,
            end_offset=offset + RECORD_LENGTH_SIZE + payload_length,
        )


@dataclass(frozen=True)
class IndexEntry:
    """One sparse index entry: a key and the absolute offset of its record."""

    key: bytes
    offset: int


class SparseIndex:
    """Every Nth key of a data block, with the byte offset its record starts at.

    Sparse rather than complete, per ARCHITECTURE.md section 3: indexing one key
    in N costs a forward scan of at most N-1 records on a lookup and saves the
    memory that holding every key would need, which is what lets a table far
    larger than memory be searched with an index that comfortably fits in it.

    The first record is always indexed, whatever N is, so a table with fewer than
    N keys still has an entry to search from. Without that, a lookup in a small
    table would have no offset to start at and would have to fall back to
    scanning the block from its start, which is the behavior the index exists to
    avoid.

    Keys are stored in the ascending order the data block is written in, which is
    what makes :meth:`offset_for` a binary search rather than a scan. The order
    is checked when an index is decoded rather than assumed, because an index
    read off a damaged disk that merely looks sorted enough would send a search
    to a confidently wrong offset instead of failing.
    """

    def __init__(self, entries: Iterable[IndexEntry]) -> None:
        """Build an index over ``entries``, which must be in ascending key order."""
        self._entries: tuple[IndexEntry, ...] = tuple(entries)
        for previous, current in zip(self._entries, self._entries[1:], strict=False):
            if current.key <= previous.key:
                raise SSTableIndexError(
                    f"sparse index keys must ascend, but {current.key!r} follows {previous.key!r}"
                )
        # Kept alongside the entries so a lookup binary searches a plain list of
        # keys. bisect over the entries themselves would need a key function on
        # every comparison, which is the one place a sparse index is hot.
        self._keys: tuple[bytes, ...] = tuple(entry.key for entry in self._entries)

    @property
    def entries(self) -> tuple[IndexEntry, ...]:
        """The index entries, in ascending key order."""
        return self._entries

    def __len__(self) -> int:
        """Number of indexed keys, which is not the number of records in the table."""
        return len(self._entries)

    def __iter__(self) -> Iterator[IndexEntry]:
        """Iterate entries in ascending key order."""
        return iter(self._entries)

    def offset_for(self, key: bytes) -> int | None:
        """Return the offset to start scanning from to find ``key``, or ``None``.

        The offset belongs to the greatest indexed key that is less than or equal
        to ``key``, which is the last position in the block where a record for
        ``key`` could begin at or after. ``None`` means ``key`` sorts before every
        indexed key, and because the first record is always indexed, that is a
        definite answer: the key is not in this table, and no scan is needed.
        """
        if not isinstance(key, bytes):
            raise TypeError(f"key must be bytes, got {type(key).__name__}")
        position = bisect_right(self._keys, key)
        if position == 0:
            return None
        return self._entries[position - 1].offset

    def encode(self) -> bytes:
        """Return the on-disk bytes of the sparse index section.

        The entry count is written first so a reader knows how many entries to
        expect before it parses any, which is what lets it tell a section that
        ended early from one that simply had few entries.
        """
        parts = [struct.pack(_INDEX_COUNT_FORMAT, len(self._entries))]
        for entry in self._entries:
            if entry.offset < 0:
                raise SSTableIndexError(
                    f"index entry for {entry.key!r} has a negative offset {entry.offset}"
                )
            parts.append(struct.pack(_INDEX_ENTRY_HEADER_FORMAT, len(entry.key)))
            parts.append(entry.key)
            parts.append(struct.pack(_INDEX_ENTRY_OFFSET_FORMAT, entry.offset))
        return b"".join(parts)

    @classmethod
    def decode(
        cls, raw: bytes, *, data_block_start: int | None = None, data_block_end: int | None = None
    ) -> SparseIndex:
        """Decode a sparse index section.

        ``data_block_start`` and ``data_block_end`` bound where an entry is
        allowed to point. They are optional only so the section can be decoded on
        its own (a test, a debugging tool); a reader that knows the block's
        extent should pass it, because an offset that lands outside the block is
        the shape a corrupted index takes, and following one would seek into the
        index section or past the end of the file and decode whatever is there.

        Every length is checked against the bytes remaining in the section before
        it is used to slice, so a damaged count or key length raises here rather
        than producing a short index that reads as merely small.
        """
        position = 0
        if len(raw) < INDEX_COUNT_SIZE:
            raise SSTableIndexError(
                f"sparse index section is {len(raw)} bytes, too short to hold the "
                f"{INDEX_COUNT_SIZE} byte entry count"
            )
        (count,) = struct.unpack(_INDEX_COUNT_FORMAT, raw[:INDEX_COUNT_SIZE])
        position += INDEX_COUNT_SIZE

        # An entry needs at least its two fixed fields, so a count claiming more
        # entries than the section could hold is rejected before a single
        # allocation is made for them.
        smallest_entry = INDEX_ENTRY_HEADER_SIZE + INDEX_ENTRY_OFFSET_SIZE
        largest_possible_count = (len(raw) - position) // smallest_entry
        if count > largest_possible_count:
            raise SSTableIndexError(
                f"sparse index claims {count} entries, more than the "
                f"{len(raw) - position} remaining bytes can hold"
            )

        entries: list[IndexEntry] = []
        for ordinal in range(count):
            remaining = len(raw) - position
            if remaining < INDEX_ENTRY_HEADER_SIZE:
                raise SSTableIndexError(
                    f"sparse index entry {ordinal} has {remaining} bytes left, fewer than the "
                    f"{INDEX_ENTRY_HEADER_SIZE} byte key length"
                )
            (key_length,) = struct.unpack(
                _INDEX_ENTRY_HEADER_FORMAT, raw[position : position + INDEX_ENTRY_HEADER_SIZE]
            )
            position += INDEX_ENTRY_HEADER_SIZE

            remaining = len(raw) - position
            needed = key_length + INDEX_ENTRY_OFFSET_SIZE
            if needed > remaining:
                raise SSTableIndexError(
                    f"sparse index entry {ordinal} claims a {key_length} byte key plus an "
                    f"offset, needing {needed} bytes, but only {remaining} remain in the section"
                )
            key = raw[position : position + key_length]
            position += key_length
            (offset,) = struct.unpack(
                _INDEX_ENTRY_OFFSET_FORMAT, raw[position : position + INDEX_ENTRY_OFFSET_SIZE]
            )
            position += INDEX_ENTRY_OFFSET_SIZE

            if data_block_start is not None and offset < data_block_start:
                raise SSTableIndexError(
                    f"sparse index entry {ordinal} for key {key!r} points to offset {offset}, "
                    f"before the data block starts at {data_block_start}"
                )
            if data_block_end is not None and offset >= data_block_end:
                raise SSTableIndexError(
                    f"sparse index entry {ordinal} for key {key!r} points to offset {offset}, "
                    f"at or past the end of the data block at {data_block_end}"
                )
            entries.append(IndexEntry(key=key, offset=offset))

        if position != len(raw):
            raise SSTableIndexError(
                f"sparse index section has {len(raw) - position} bytes left over after its "
                f"{count} entries"
            )
        return cls(entries)


@dataclass(frozen=True)
class SSTableLayout:
    """Where each section of a finished SSTable lives, and what it holds.

    Returned by :meth:`SSTableWriter.finish`. Until M4.2 writes a footer, this is
    how a caller learns the extents it needs to read the file back, and once the
    footer exists it is what the footer is written from. The sparse index is
    carried here as the object rather than as an offset alone because the writer
    built it in memory on the way past, and making the caller decode from disk
    what the writer already has would be work done twice.
    """

    path: Path
    format_version: int
    data_block_offset: int
    data_block_end: int
    index_offset: int
    index_end: int
    bloom_filter_offset: int
    bloom_filter_end: int
    record_count: int
    index: SparseIndex

    @property
    def data_block_size(self) -> int:
        """Size of the data block in bytes."""
        return self.data_block_end - self.data_block_offset


def _fsync_directory(path: Path) -> None:
    """Force a directory entry change (the rename below) onto the disk.

    Renaming a file is a change to the directory, and on POSIX the directory has
    to be synced for that change to survive a power loss, even when the file's
    own bytes are already down. Skipped where directories cannot be opened for
    this, which is the case on Windows, rather than guarded by catching whatever
    the platform raises: a real fsync failure on a platform that supports it is
    something a caller has to hear about, not something to swallow as a
    portability quirk.
    """
    if os.name != "posix":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class SSTableWriter:
    """Streams a sorted run of records into a new SSTable file.

    Records are appended in the order they are given and the writer refuses
    anything that does not ascend strictly, because everything above this file
    rests on the block being sorted: the sparse index is only searchable if it
    is, and a merge (M8) reads tables as sorted runs. A frozen memtable already
    yields records in key order, so a violation here means a bug in the caller,
    and the cheapest place to find it is at the write that caused it rather than
    at a read months later.

    The index is built inline from the offsets the stream produces, one entry
    every ``index_interval`` records, starting at the first. That is the reason
    M4.1 covers both the block and the index: the offsets exist only while the
    records are being written, so building the index afterwards would mean
    reading the table back to recover numbers the writer just had.

    Writes go to a temporary file in the destination directory and are moved to
    the final path by :meth:`finish` with an atomic rename, so the destination
    either does not exist or is a complete table. ARCHITECTURE.md section 6 makes
    the footer the commit point, and it still is inside the file (M4.2); the
    rename adds that a crashed flush leaves its debris under a temporary name
    where a reader is not looking for a table, rather than leaving a half table
    at the name the engine will try to open. The temporary file is in the same
    directory because a rename is only atomic within one filesystem.

    The temporary name is derived from the destination name, so two writers
    aimed at the same destination at the same time would write over each other's
    temporary file. That is not a case this guards against, because the two would
    be producing the same table twice: the engine names a table once, when it
    decides to flush or to merge, and no second writer is given that name.

    The writer is a context manager, and leaving its block without calling
    :meth:`finish` discards the temporary file instead of leaving it behind. That
    covers the case where the caller fails partway through producing records: an
    exception on the way to a flush should not leave the data directory
    accumulating the remains of tables that were never finished.
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        index_interval: int = DEFAULT_INDEX_INTERVAL,
    ) -> None:
        """Open a temporary file for the SSTable that will be moved to ``path``.

        ``index_interval`` is N, the number of records between index entries. It
        is a constructor argument rather than a module constant because the right
        value depends on record size and read pattern, which the engine knows and
        the format does not.
        """
        self._index_interval = _validate_index_interval(index_interval)
        self._path = Path(path)
        self._temp_path = self._path.with_name(f".{self._path.name}.tmp")
        self._index_entries: list[IndexEntry] = []
        self._record_count = 0
        self._last_key: bytes | None = None
        self._finished = False
        self._closed = False
        self._file = open(self._temp_path, "wb")
        try:
            self._file.write(encode_file_header())
            self._data_block_offset = self._file.tell()
        except BaseException:
            self._discard()
            raise

    @property
    def path(self) -> Path:
        """Final path the finished table will be moved to."""
        return self._path

    @property
    def temp_path(self) -> Path:
        """Path the table is being written at until :meth:`finish` renames it."""
        return self._temp_path

    @property
    def index_interval(self) -> int:
        """Number of records between two sparse index entries."""
        return self._index_interval

    @property
    def record_count(self) -> int:
        """Number of records written so far."""
        return self._record_count

    @property
    def closed(self) -> bool:
        """True once the writer has finished or discarded its temporary file."""
        return self._closed

    def add(self, key: bytes, value: bytes | None) -> int:
        """Append one record and return the byte offset it starts at.

        ``value`` of ``None`` writes a tombstone. Tombstones are written like any
        other record, because an SSTable is immutable and a delete is a fact
        about a key at a point in time, not the absence of one: dropping it here
        would let an older table's value for that key resurface on a read.
        """
        # Finished is checked before closed because finishing also closes: a
        # caller who added a record after finishing wants to hear that, not the
        # more general complaint that the handle is gone.
        if self._finished:
            raise ValueError("cannot add to an SSTableWriter that has been finished")
        if self._closed:
            raise ValueError("cannot add to a closed SSTableWriter")
        if not isinstance(key, bytes):
            raise TypeError(f"key must be bytes, got {type(key).__name__}")
        if self._last_key is not None and key <= self._last_key:
            raise ValueError(
                f"SSTable records must be added in strictly ascending key order, but "
                f"{key!r} does not follow {self._last_key!r}"
            )

        record = encode_record(key, value)
        offset = self._file.tell()
        # The index entry is appended before the write rather than after, so that
        # its offset is the position the record is about to occupy. Taking it
        # afterwards would record where the next record starts.
        if self._record_count % self._index_interval == 0:
            self._index_entries.append(IndexEntry(key=key, offset=offset))
        self._file.write(record)
        self._record_count += 1
        self._last_key = key
        return offset

    def add_put(self, key: bytes, value: bytes) -> int:
        """Append a record holding ``value`` for ``key``."""
        if not isinstance(value, bytes):
            raise TypeError(f"value must be bytes, got {type(value).__name__}")
        return self.add(key, value)

    def add_delete(self, key: bytes) -> int:
        """Append a tombstone for ``key``.

        There is no value parameter: the format's empty-value convention is
        enforced by the API rather than left to the caller to honor.
        """
        return self.add(key, None)

    def finish(self) -> SSTableLayout:
        """Write the remaining sections, sync, and move the table to its final path.

        The bloom filter section is written as a zero length placeholder. Its
        position in the layout is fixed now, between the index and the footer
        M4.2 adds, so that M5.3 can fill it in without moving anything else. A
        placeholder with an explicit length rather than no section at all means a
        reader is never guessing whether the bytes at that offset are a filter or
        the start of something else.

        The file's bytes are forced to disk before the rename and the directory
        entry is forced after it, so a table that appears at its final path is
        one whose contents are really there. Without the first sync, a crash
        could leave the name visible while the blocks behind it are not.
        """
        if self._finished:
            raise ValueError("SSTableWriter has already been finished")
        if self._closed:
            raise ValueError("cannot finish a closed SSTableWriter")

        try:
            data_block_end = self._file.tell()
            index = SparseIndex(self._index_entries)
            self._file.write(index.encode())
            index_end = self._file.tell()
            self._file.write(struct.pack(_BLOOM_PLACEHOLDER_FORMAT, 0))
            bloom_filter_end = self._file.tell()

            self._file.flush()
            os.fsync(self._file.fileno())
        except BaseException:
            self._discard()
            raise

        self._file.close()
        self._closed = True
        try:
            os.replace(self._temp_path, self._path)
        except BaseException:
            # The rename is what makes the table exist, so a failure here leaves
            # nothing worth keeping. Cleaning up before re-raising matters
            # because the writer is marked finished just below, after which
            # discard() is a no-op: without this the temporary file would
            # outlive every handle that knew its name.
            self._temp_path.unlink(missing_ok=True)
            raise
        self._finished = True
        # After the rename rather than before it: the table is at its final path
        # from here on, and a caller told that the sync failed is being told the
        # table exists but its durability is not confirmed, which is a different
        # thing from a flush that did not happen.
        _fsync_directory(self._path.parent)

        return SSTableLayout(
            path=self._path,
            format_version=SSTABLE_FORMAT_VERSION,
            data_block_offset=self._data_block_offset,
            data_block_end=data_block_end,
            index_offset=data_block_end,
            index_end=index_end,
            bloom_filter_offset=index_end,
            bloom_filter_end=bloom_filter_end,
            record_count=self._record_count,
            index=index,
        )

    def discard(self) -> None:
        """Close the writer and remove its temporary file. Safe to call more than once.

        This is what an abandoned flush calls. A finished writer has nothing left
        to discard, so calling it then is a no-op rather than an error: the
        context manager takes this path on every exit and must not undo a
        successful finish.
        """
        if self._finished or self._closed:
            return
        self._discard()

    def _discard(self) -> None:
        """Close the handle and unlink the temporary file, whichever of those still applies."""
        try:
            self._file.close()
        finally:
            self._closed = True
            self._temp_path.unlink(missing_ok=True)

    def __enter__(self) -> SSTableWriter:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.discard()


def _validate_index_interval(index_interval: int) -> int:
    """Return ``index_interval`` if it is a usable cadence, else raise ``ValueError``.

    Checked in the constructor rather than at the first record: an interval of
    zero would divide by zero partway through a flush, and a negative one would
    index nothing at all, and both are easier to understand as a rejected
    argument than as a failure halfway through writing a table.
    """
    if not isinstance(index_interval, int) or isinstance(index_interval, bool):
        raise TypeError(f"index_interval must be an int, got {type(index_interval).__name__}")
    if index_interval < 1:
        raise ValueError(f"index_interval must be at least 1, got {index_interval}")
    return index_interval


def write_sstable(
    path: str | os.PathLike[str],
    records: Iterable[tuple[bytes, bytes | None]],
    *,
    index_interval: int = DEFAULT_INDEX_INTERVAL,
) -> SSTableLayout:
    """Write ``records`` to a new SSTable at ``path`` and return its layout.

    ``records`` is an iterable of ``(key, value)`` pairs in ascending key order,
    where a value of ``None`` is a tombstone. Plain tuples rather than a memtable
    type, so that CLAUDE.md's rule about independently testable components holds:
    the SSTable format does not need to import the memtable to be written, and a
    caller flushing one adapts its entries in a generator expression.
    """
    with SSTableWriter(path, index_interval=index_interval) as writer:
        for key, value in records:
            writer.add(key, value)
        return writer.finish()
