"""SSTable: the immutable, sorted, on-disk form a frozen memtable is flushed into.

Scope of this module today (stories M4.1 through M4.4, plus M5.3): framing a
record for the data block, streaming a sorted run of records into a file while
building a sparse index from the offsets that stream produces and a bloom filter
from the keys, committing the file with a fixed size footer that records the
format version and where every section starts and ends, reading a key back out
of a finished table by way of that footer, and judging whether a file found on
disk is a complete table at all.

On-disk layout (little endian, no padding), with the sections this story writes::

    [ 8B magic ][ 1B format version ]
    [ data block:   record record record ... ]
    [ sparse index: 4B entry count, then entry entry ... ]
    [ bloom filter: one serialized ledgerlog.bloom blob, self-describing ]
    [ footer ]

On-disk footer layout, a fixed size trailer::

    [ 8B data block offset ][ 8B data block end ]
    [ 8B sparse index offset ][ 8B sparse index end ]
    [ 8B bloom filter offset ][ 8B bloom filter end ]
    [ 8B record count ][ 1B format version ]
    [ 4B CRC32 over the 57 bytes above ]
    [ 8B footer magic ]

On-disk record layout::

    [ 4B payload length ][ payload ]

    payload = [ 1B kind ][ 4B key length ][ key ][ value ]

On-disk sparse index entry layout::

    [ 4B key length ][ key ][ 8B absolute byte offset into the data block ]

Why a file header carries the version even though the footer carries one too:
CLAUDE.md asks that a change to an on-disk layout be detectable rather than
silently misread. A file whose first bytes are not this magic is not an SSTable
at all, which is a different failure from a file that is one this build cannot
parse, and neither is a question a reader should have to answer by seeking to
the end of a file whose end may not have been written yet. The footer is the
commit point (a file is only a complete SSTable once it is there, per
ARCHITECTURE.md section 6) and the header is what says the bytes in front of it
are ours to read.

Why the footer is fixed size and ends with its own magic: a reader has to find
the footer before it can be told where anything is, and the only position it
knows without reading the file is the end. A fixed size trailer is therefore
seekable from the end in one step, and the magic is the last thing written so
that its presence is evidence the bytes in front of it are a whole footer and
not the tail of a data block that a crashed flush stopped in the middle of.
A CRC over the fields backs that up for the case the magic alone cannot rule
out: a crash can land the blocks of one write out of order, so a file can end
with the right eight bytes while the offsets before them were never stored.

Why the footer repeats offsets the reader could otherwise recompute: it could
not. The data block's start is fixed by the header, but nothing in the file says
where it ends, and a reader that guessed would decode index bytes as records.
Each boundary is recorded once, as an absolute offset, so a lookup seeks instead
of scanning, which is the whole point of the section for the sparse index.

Why adding the footer did not bump the format version: a file without a footer
was never a complete SSTable under this format (the footer is the commit point),
so there were no valid version 1 files with the older shape for a bumped version
to protect. A later change to the shape of a section that complete tables
already carry is what the version byte is for, and that one does bump it.

Why filling the bloom filter section in did bump it, to version 2: that is the
change just described. A version 1 table was committed with a four byte
placeholder at the bloom filter offset, and the bytes at that offset now mean a
serialized filter instead. Nothing distinguishes the two by inspection at the
section level, since a placeholder is a legal length prefix and the reader that
wants a filter would hand those four bytes to
:meth:`~ledgerlog.bloom.BloomFilter.deserialize` and get a truncation error that
says nothing about which format it is looking at. The version byte is what
answers that, and per CLAUDE.md it has to move rather than let the meaning of
stored bytes change underneath it. A version 1 table is therefore reported as an
unsupported version, not as damage, which is the outcome
:func:`inspect_sstable` already distinguishes: it was committed correctly, and
this build is the part that no longer matches.

Why the bloom filter is stored as one self-contained blob rather than as fields
spread into the footer: the failure a bloom filter can cause is the one failure
this format cannot detect after the fact. A filter rebuilt from the wrong or
damaged bytes can answer "definitely absent" for a key the table holds, the read
path skips the table on the strength of that (ARCHITECTURE.md section 5), and
the value is gone with nothing raised. ``ledgerlog.bloom``'s blob carries its own
magic, length and checksum for exactly that reason, so storing it verbatim keeps
that protection instead of replacing it with the footer's word for where the
bits are. It also keeps the two formats versioned separately, which is what lets
the bit order change without moving an SSTable's sections, or the reverse.

Why a file that cannot be opened is sorted into three outcomes rather than just
refused: the three call for different responses, and only one of them names a
file the engine may throw away. A file with no valid footer was never committed,
so per ARCHITECTURE.md section 6 the writes behind it are still in the WAL and
discarding it loses nothing. A footer that landed whole and still places a
section outside the file holding it cannot be one this writer produced, so
something rewrote the file after the fact and none of what its offsets point at
can be trusted. A footer that is whole and consistent but carries a version this
build does not know belongs to a table another build committed correctly, whose
data is real and whose reader is the part that is missing. Collapsing that last
case into the first is how an engine deletes a good table on the way past, which
is why :func:`inspect_sstable` names the outcome instead of answering yes or no.

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
import zlib
from bisect import bisect_right
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from enum import Enum, IntEnum
from pathlib import Path
from types import TracebackType
from typing import BinaryIO

from ledgerlog.bloom import BloomFilter

SSTABLE_FORMAT_VERSION = 2
"""Version of the layout described in this module's docstring.

Stamped into every SSTable's file header. A build that reads a different number
reports it rather than parsing bytes whose meaning it is guessing at.
"""

SSTABLE_MAGIC = b"LEDGRSST"
"""Fixed marker at the start of every SSTable file. Eight bytes, no terminator."""

SSTABLE_FOOTER_MAGIC = b"LEDGRFTR"
"""Fixed marker at the very end of every complete SSTable file.

Different bytes from :data:`SSTABLE_MAGIC` so that a file consisting of nothing
but a header cannot be mistaken for one that ends in a footer, which is exactly
the file a flush that died early leaves behind.
"""

_FILE_HEADER_FORMAT = "<8sB"
_RECORD_LENGTH_FORMAT = "<I"
_RECORD_PAYLOAD_HEADER_FORMAT = "<BI"
_INDEX_COUNT_FORMAT = "<I"
_INDEX_ENTRY_HEADER_FORMAT = "<I"
_INDEX_ENTRY_OFFSET_FORMAT = "<Q"
_FOOTER_FIELDS_FORMAT = "<7QB"
_FOOTER_CHECKSUM_FORMAT = "<I"
_FOOTER_MAGIC_FORMAT = "<8s"

FILE_HEADER_SIZE = struct.calcsize(_FILE_HEADER_FORMAT)
RECORD_LENGTH_SIZE = struct.calcsize(_RECORD_LENGTH_FORMAT)
RECORD_PAYLOAD_HEADER_SIZE = struct.calcsize(_RECORD_PAYLOAD_HEADER_FORMAT)
INDEX_COUNT_SIZE = struct.calcsize(_INDEX_COUNT_FORMAT)
INDEX_ENTRY_HEADER_SIZE = struct.calcsize(_INDEX_ENTRY_HEADER_FORMAT)
INDEX_ENTRY_OFFSET_SIZE = struct.calcsize(_INDEX_ENTRY_OFFSET_FORMAT)
FOOTER_FIELDS_SIZE = struct.calcsize(_FOOTER_FIELDS_FORMAT)
FOOTER_CHECKSUM_SIZE = struct.calcsize(_FOOTER_CHECKSUM_FORMAT)
FOOTER_MAGIC_SIZE = struct.calcsize(_FOOTER_MAGIC_FORMAT)
FOOTER_SIZE = FOOTER_FIELDS_SIZE + FOOTER_CHECKSUM_SIZE + FOOTER_MAGIC_SIZE
"""Total size of the footer in bytes.

Fixed, and part of the format rather than a detail of this implementation: a
reader finds the footer by seeking this far back from the end of the file, so
the number has to be knowable without having read anything.
"""

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

DEFAULT_BLOOM_FALSE_POSITIVE_RATE = 0.01
"""Default target false-positive rate for the bloom filter a table carries.

One in a hundred, which costs about ten bits per key and seven hash probes per
query. The trade-off, per ARCHITECTURE.md section 3, is the size of the filter
against how often a read opens a table that cannot hold the key: halving the
rate costs roughly another 1.44 bits per key, and each halving saves half of an
already small number of wasted lookups, so the returns fall off quickly on the
accuracy side while the memory cost keeps climbing linearly. Like the index
interval above, the real value belongs to the M10 tuning spike, which will pick
it from a measured read amplification rather than from this reasoning.
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


class SSTableFooterError(SSTableFormatError):
    """Raised when a file's footer cannot be read as the footer the writer produces."""


class SSTableIncompleteError(SSTableFooterError):
    """Raised when a file has no fully written footer, so it is not a complete SSTable.

    This is the shape a flush killed partway through leaves behind, and per
    ARCHITECTURE.md section 6 the right response to it is to discard the file and
    recover its writes from the WAL, not to read what did land. It is separate
    from :class:`SSTableUnsupportedVersionError` because that one says the file is
    a complete table written by a different build, which is a table whose data is
    real and whose reader is missing, the opposite problem.
    """


class SSTableCorruptFooterError(SSTableFooterError):
    """Raised when a whole footer describes a file that cannot exist.

    Separate from :class:`SSTableIncompleteError` because the two say different
    things about how the file got this way. An incomplete table stopped before
    its footer, which is what an interrupted flush leaves and is therefore an
    ordinary thing to find after a crash. A corrupt footer passed both its magic
    and its checksum, so those bytes did land whole, and it still puts a section
    outside the file that carries it or ends a section before it starts. No
    sequence of writes this module performs produces that, so the file was
    changed by something else, and reading it would mean taking offsets from a
    footer that demonstrably does not describe the bytes in front of it.
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
class SSTableFooter:
    """The fixed size trailer that makes a file a complete SSTable.

    Written last, after the data block, the sparse index and the bloom filter
    section, and it is the commit point per ARCHITECTURE.md section 6: a file
    without one is a flush that did not finish, whatever bytes it does hold.

    It carries the format version a second time, after the header's copy, because
    the two answer different questions at different moments. The header's version
    is read going forwards, before any record is parsed. The footer's is read by a
    reader that seeked to the end, and having it there means that reader can tell
    which layout the offsets it is about to use were written in without first
    trusting the front of the file, which is the part the footer's own validity
    says nothing about.

    Offsets are ends as well as starts rather than starts alone, even though the
    sections are contiguous today. A section's extent is what every bounds check
    in this module is made against, and deriving the end of one section from the
    start of the next assumes an adjacency the format does not otherwise promise,
    which would quietly turn a padded or reordered future layout into silently
    misread bytes.
    """

    data_block_offset: int
    data_block_end: int
    index_offset: int
    index_end: int
    bloom_filter_offset: int
    bloom_filter_end: int
    record_count: int
    format_version: int = SSTABLE_FORMAT_VERSION

    def encode(self) -> bytes:
        """Return the on-disk bytes of the footer.

        The checksum covers every field including the version byte, so a damaged
        version is caught here rather than being reported as a file written by
        some other build.
        """
        for name, value in (
            ("data_block_offset", self.data_block_offset),
            ("data_block_end", self.data_block_end),
            ("index_offset", self.index_offset),
            ("index_end", self.index_end),
            ("bloom_filter_offset", self.bloom_filter_offset),
            ("bloom_filter_end", self.bloom_filter_end),
            ("record_count", self.record_count),
        ):
            if value < 0:
                raise SSTableFooterError(f"footer field {name} is negative ({value})")
        if not 0 <= self.format_version <= 0xFF:
            raise SSTableFooterError(
                f"SSTable format version {self.format_version} does not fit in one byte"
            )

        fields = struct.pack(
            _FOOTER_FIELDS_FORMAT,
            self.data_block_offset,
            self.data_block_end,
            self.index_offset,
            self.index_end,
            self.bloom_filter_offset,
            self.bloom_filter_end,
            self.record_count,
            self.format_version,
        )
        checksum = zlib.crc32(fields) & 0xFFFFFFFF
        return (
            fields
            + struct.pack(_FOOTER_CHECKSUM_FORMAT, checksum)
            + struct.pack(_FOOTER_MAGIC_FORMAT, SSTABLE_FOOTER_MAGIC)
        )

    @classmethod
    def decode(cls, raw: bytes) -> SSTableFooter:
        """Decode exactly :data:`FOOTER_SIZE` bytes into a footer.

        A missing magic or a failed checksum is reported as an incomplete table
        rather than as corruption, because both are what an interrupted write
        produces and neither can be told apart from one by looking. The version
        this returns is not judged here: deciding whether a version is one this
        build can act on belongs to the reader (M4.3), which is the thing that
        would act on it.

        Section boundaries are checked for internal consistency, since offsets
        that run backwards cannot be the writer's and would otherwise be handed
        to a seek. Whether they fall inside the file is
        :func:`validate_footer_sections`'s check, which needs a file to measure
        them against and so cannot be made from these bytes.
        """
        if len(raw) != FOOTER_SIZE:
            raise SSTableIncompleteError(
                f"footer is {len(raw)} bytes, expected exactly {FOOTER_SIZE}"
            )

        (found_magic,) = struct.unpack(_FOOTER_MAGIC_FORMAT, raw[-FOOTER_MAGIC_SIZE:])
        if found_magic != SSTABLE_FOOTER_MAGIC:
            raise SSTableIncompleteError(
                f"SSTable footer magic mismatch: found {found_magic!r}, expected "
                f"{SSTABLE_FOOTER_MAGIC!r}, so the footer was never fully written"
            )

        fields = raw[:FOOTER_FIELDS_SIZE]
        (found_checksum,) = struct.unpack(
            _FOOTER_CHECKSUM_FORMAT,
            raw[FOOTER_FIELDS_SIZE : FOOTER_FIELDS_SIZE + FOOTER_CHECKSUM_SIZE],
        )
        expected_checksum = zlib.crc32(fields) & 0xFFFFFFFF
        if found_checksum != expected_checksum:
            raise SSTableIncompleteError(
                f"SSTable footer checksum mismatch: found {found_checksum:#010x}, computed "
                f"{expected_checksum:#010x}, so the footer did not land whole"
            )

        (
            data_block_offset,
            data_block_end,
            index_offset,
            index_end,
            bloom_filter_offset,
            bloom_filter_end,
            record_count,
            format_version,
        ) = struct.unpack(_FOOTER_FIELDS_FORMAT, fields)

        footer = cls(
            data_block_offset=data_block_offset,
            data_block_end=data_block_end,
            index_offset=index_offset,
            index_end=index_end,
            bloom_filter_offset=bloom_filter_offset,
            bloom_filter_end=bloom_filter_end,
            record_count=record_count,
            format_version=format_version,
        )
        footer._validate_section_order()
        return footer

    def _validate_section_order(self) -> None:
        """Raise unless every section starts after the header and ends at or after its start."""
        boundaries = (
            ("data block", self.data_block_offset, self.data_block_end),
            ("sparse index", self.index_offset, self.index_end),
            ("bloom filter", self.bloom_filter_offset, self.bloom_filter_end),
        )
        for name, start, end in boundaries:
            if start < FILE_HEADER_SIZE:
                raise SSTableCorruptFooterError(
                    f"footer puts the {name} section at offset {start}, inside the "
                    f"{FILE_HEADER_SIZE} byte file header"
                )
            if end < start:
                raise SSTableCorruptFooterError(
                    f"footer ends the {name} section at offset {end}, before it starts at {start}"
                )


def validate_footer_sections(footer: SSTableFooter, *, file_size: int) -> None:
    """Raise unless every section ``footer`` describes fits inside a file this size.

    :meth:`SSTableFooter.decode` checks the fields against each other, which is
    all the bytes of a footer can answer on their own. This is the other half of
    the question and it needs the file: an offset that is internally consistent
    can still land past the end of the file, and following one would seek outside
    the table and size a read by a number nobody wrote. Doing it here, once,
    rather than at each place a section is about to be read, is what makes every
    section safe to reach for: what is being decided is whether the footer can be
    the writer's at all, not whether the one section a given caller wants is
    plausible.
    """
    footer_offset = file_size - FOOTER_SIZE
    sections = (
        ("data block", footer.data_block_offset, footer.data_block_end),
        ("sparse index", footer.index_offset, footer.index_end),
        ("bloom filter", footer.bloom_filter_offset, footer.bloom_filter_end),
    )
    for name, start, end in sections:
        if start < FILE_HEADER_SIZE or end < start or end > footer_offset:
            raise SSTableCorruptFooterError(
                f"footer places the {name} section at bytes {start} to {end}, outside the "
                f"{FILE_HEADER_SIZE} to {footer_offset} range this {file_size} byte file can "
                "hold it in"
            )


def read_bloom_filter(
    stream: BinaryIO, footer: SSTableFooter, *, file_size: int | None = None
) -> BloomFilter:
    """Read and rebuild the bloom filter of the table open on ``stream``.

    ``footer`` says where the section is, and is re-measured against the file
    here rather than taken on trust, so that this function is safe to call with a
    footer from anywhere: a caller that already validated it pays one comparison,
    and a caller that did not cannot turn a damaged offset into a read sized by a
    number nobody wrote. Only the bytes the section actually spans are read, and
    everything inside them is :meth:`~ledgerlog.bloom.BloomFilter.deserialize`'s
    to check.

    This is deliberately not done at open time by :class:`SSTableReader`. A
    filter exists to let the read path skip a table it would otherwise open
    (ARCHITECTURE.md section 5), which is a decision made one level up, across
    tables, and that level arrives in M7. Loading it here would mean every reader
    paid for a filter whether or not anything consulted it.
    """
    if file_size is None:
        file_size = stream.seek(0, os.SEEK_END)
    validate_footer_sections(footer, file_size=file_size)

    start = footer.bloom_filter_offset
    length = footer.bloom_filter_end - start
    stream.seek(start)
    raw = stream.read(length)
    if len(raw) < length:
        raise SSTableFooterError(
            f"read {len(raw)} of {length} bytes of the bloom filter section at offset {start}"
        )
    return BloomFilter.deserialize(raw)


def read_footer(stream: BinaryIO, *, file_size: int | None = None) -> SSTableFooter:
    """Read and validate the footer of the SSTable open on ``stream``.

    ``file_size`` is taken as given when passed and measured by seeking to the
    end otherwise, so a caller that already knows it does not pay a second seek.
    The stream is left at the start of the footer.

    A file shorter than a header plus a footer cannot hold both, so it is
    reported as incomplete before any offset is read off it: that check is what
    keeps the seek below from landing at a negative position on a file that a
    crash left a few bytes long.

    A footer that decodes is then measured against the file through
    :func:`validate_footer_sections`, so that no caller receives offsets it would
    have to bounds check itself before using. A footer is only useful for finding
    sections, and one that points outside its own file is not a footer this
    writer wrote, whatever its checksum says about the bytes arriving intact.
    """
    if file_size is None:
        file_size = stream.seek(0, os.SEEK_END)
    smallest_complete_table = FILE_HEADER_SIZE + FOOTER_SIZE
    if file_size < smallest_complete_table:
        raise SSTableIncompleteError(
            f"file is {file_size} bytes, smaller than the {smallest_complete_table} bytes a "
            "table with a header and a footer needs, so no footer was fully written"
        )

    footer_offset = file_size - FOOTER_SIZE
    stream.seek(footer_offset)
    raw = stream.read(FOOTER_SIZE)
    if len(raw) < FOOTER_SIZE:
        raise SSTableIncompleteError(
            f"read {len(raw)} of {FOOTER_SIZE} footer bytes at offset {footer_offset}"
        )
    stream.seek(footer_offset)
    footer = SSTableFooter.decode(raw)
    validate_footer_sections(footer, file_size=file_size)
    return footer


class SSTableStatus(Enum):
    """What :func:`inspect_sstable` concluded about one file.

    ``VALID`` is a complete table this build can read. ``INCOMPLETE`` is a file
    with no whole footer, which is what an interrupted flush leaves and what
    ARCHITECTURE.md section 6 says to discard in favor of the WAL. ``CORRUPT`` is
    a whole footer describing a file that cannot be, or a committed table whose
    header was damaged afterwards. ``UNSUPPORTED_VERSION`` is a table another
    build committed properly in a layout this one does not know, which is the one
    rejection that is not a reason to delete anything.
    """

    VALID = "valid"
    INCOMPLETE = "incomplete"
    CORRUPT = "corrupt"
    UNSUPPORTED_VERSION = "unsupported_version"


@dataclass(frozen=True)
class SSTableInspection:
    """The verdict on one file, with the evidence behind it.

    ``footer`` is filled in whenever one was read whole, which includes the
    unsupported version case: a caller deciding what to do with such a file wants
    to know which version it claims, and this is what says so.

    ``reason`` carries the message of the error that decided a rejection, so that
    an operator reading a startup log is told which check the file failed rather
    than only that it failed one.
    """

    path: Path
    status: SSTableStatus
    reason: str | None = None
    footer: SSTableFooter | None = None

    @property
    def is_valid(self) -> bool:
        """True if this build can read the file as a complete SSTable."""
        return self.status is SSTableStatus.VALID

    @property
    def is_complete(self) -> bool:
        """True if a committed table is there, whether or not this build can read it.

        An unsupported version counts as complete: its footer landed, which is
        the commit point, so the data is real and only the reader for it is
        missing. That is the distinction that decides whether a file may be
        thrown away, so it is answered here rather than left to every caller to
        rebuild out of the status.
        """
        return self.status in (SSTableStatus.VALID, SSTableStatus.UNSUPPORTED_VERSION)


def inspect_sstable(path: str | os.PathLike[str]) -> SSTableInspection:
    """Decide whether ``path`` holds a table this build can read, and say why not.

    This is the question startup asks of every file in a data directory (M9), and
    it answers with a verdict rather than by raising because a partial table is
    an expected thing to find after a crash, not a mistake by the caller. A file
    that cannot be opened at all is still an error, because that is the
    filesystem saying something the engine should not paper over.

    The checks are made in the order the reader makes them, so that a file called
    valid here is one :class:`SSTableReader` can open and a file rejected here is
    one it would refuse. Anything else would be worse than no check at all: a
    discovery pass would hand the engine tables that then fail to open, or
    quietly drop tables that would have read fine.

    What is examined is the frame of the file: that a footer landed whole, that
    the sections it names fit inside the file, that the header is one of ours,
    and that the version stamps are ones this build knows. What is not examined
    is the content of those sections, because the format gives nothing to examine
    it with: there is no checksum over the data block, so a table whose records
    were rewritten under a footer that still matches reads as valid here. Finding
    that would not be a stricter version of this check, it would be a different
    format.
    """
    # Opened through a context manager so the descriptor is released on every
    # path out, including the ones that end in a rejection rather than a raise.
    with open(path, "rb") as handle:
        return _inspect_stream(handle, Path(path))


def _inspect_stream(stream: BinaryIO, path: Path) -> SSTableInspection:
    """Classify the file open on ``stream``, turning each typed failure into a status."""
    try:
        footer = read_footer(stream)
    except SSTableIncompleteError as error:
        # Checked before its parent class below, since an incomplete table is a
        # footer error and the two verdicts are not interchangeable.
        return SSTableInspection(path=path, status=SSTableStatus.INCOMPLETE, reason=str(error))
    except SSTableFooterError as error:
        return SSTableInspection(path=path, status=SSTableStatus.CORRUPT, reason=str(error))

    if footer.format_version != SSTABLE_FORMAT_VERSION:
        return SSTableInspection(
            path=path,
            status=SSTableStatus.UNSUPPORTED_VERSION,
            reason=str(SSTableUnsupportedVersionError(footer.format_version)),
            footer=footer,
        )

    stream.seek(0)
    try:
        read_file_header(stream)
    except SSTableUnsupportedVersionError as error:
        return SSTableInspection(
            path=path,
            status=SSTableStatus.UNSUPPORTED_VERSION,
            reason=str(error),
            footer=footer,
        )
    except SSTableHeaderError as error:
        # A whole footer with a header that is not ours means the file was
        # committed and then damaged at the front, which is corruption rather
        # than a write that never finished.
        return SSTableInspection(
            path=path, status=SSTableStatus.CORRUPT, reason=str(error), footer=footer
        )
    return SSTableInspection(path=path, status=SSTableStatus.VALID, footer=footer)


def is_complete_sstable(path: str | os.PathLike[str]) -> bool:
    """True if ``path`` holds a committed table, whether or not this build reads it.

    The yes-or-no form of :func:`inspect_sstable`, for callers that only want the
    commit point's answer. A table stamped with a version this build does not
    know still counts: it was committed, and whether this build can parse it is
    the separate question the inspection's status answers. A footer that never
    landed and one that points outside its own file both count as no.
    """
    return inspect_sstable(path).is_complete


@dataclass(frozen=True)
class SSTableLayout:
    """Where each section of a finished SSTable lives, and what it holds.

    Returned by :meth:`SSTableWriter.finish`. It is what the footer is written
    from, and it says the same thing the footer does plus the three things the
    footer has no reason to carry: the path, the sparse index as an object, and
    the bloom filter as an object. Those last two are here because the writer
    built them in memory on the way past, and making the caller decode from disk
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
    footer_offset: int
    footer_end: int
    record_count: int
    index: SparseIndex
    bloom_filter: BloomFilter

    @property
    def data_block_size(self) -> int:
        """Size of the data block in bytes."""
        return self.data_block_end - self.data_block_offset

    @property
    def footer(self) -> SSTableFooter:
        """The footer that was written at :attr:`footer_offset`."""
        return SSTableFooter(
            data_block_offset=self.data_block_offset,
            data_block_end=self.data_block_end,
            index_offset=self.index_offset,
            index_end=self.index_end,
            bloom_filter_offset=self.bloom_filter_offset,
            bloom_filter_end=self.bloom_filter_end,
            record_count=self.record_count,
            format_version=self.format_version,
        )


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

    The bloom filter is built from every key, tombstones included. A tombstone is
    the table's answer for that key and it has to stop the read path rather than
    let an older table's value surface (ARCHITECTURE.md section 5), so a filter
    that omitted deleted keys would send a lookup straight past the table that
    holds the delete. Sizing a filter needs the key count up front, which a
    streaming writer does not have, so ``expected_keys`` is how a caller that
    knows it (a flush knows its memtable's length; a merge knows the sum of its
    sources' record counts) keeps the writer streaming. Without it the keys are
    held until :meth:`finish` and the filter is sized exactly, which costs memory
    proportional to the key set and is why the hint exists.

    Writes go to a temporary file in the destination directory and are moved to
    the final path by :meth:`finish` with an atomic rename, so the destination
    either does not exist or is a complete table. ARCHITECTURE.md section 6 makes
    the footer the commit point, and it still is inside the file; the
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
        expected_keys: int | None = None,
        bloom_false_positive_rate: float = DEFAULT_BLOOM_FALSE_POSITIVE_RATE,
    ) -> None:
        """Open a temporary file for the SSTable that will be moved to ``path``.

        ``index_interval`` is N, the number of records between index entries. It
        is a constructor argument rather than a module constant because the right
        value depends on record size and read pattern, which the engine knows and
        the format does not.

        ``expected_keys``, when given, sizes the bloom filter immediately so keys
        are hashed into it as they stream past and none are held. It is a hint
        and not a promise: writing more keys than were expected saturates the
        array and pushes the real false-positive rate above the target, which
        costs lookups and never correctness, since a bloom filter's bits are only
        ever set and no amount of saturation can produce a false negative.

        ``bloom_false_positive_rate`` is checked here rather than where the
        filter is built, because in the deferred case that is :meth:`finish`, and
        a rate the sizing formula cannot accept should be reported when the
        writer is opened rather than after a whole table has been streamed into
        it. ``ledgerlog.bloom`` is what enforces the rule; this repeats its range
        so the report arrives at the useful moment.
        """
        self._index_interval = _validate_index_interval(index_interval)
        self._bloom_rate = _validate_false_positive_rate(bloom_false_positive_rate)
        self._expected_keys = _validate_expected_keys(expected_keys)
        # Either a filter that is already sized and takes keys as they arrive, or
        # no filter and a list of the keys to size one from at finish. Never
        # both: the buffer exists only because the size is not yet known.
        self._bloom: BloomFilter | None = None
        self._pending_keys: list[bytes] | None = None
        if self._expected_keys is None:
            self._pending_keys = []
        else:
            self._bloom = BloomFilter.for_target(max(1, self._expected_keys), self._bloom_rate)
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
    def bloom_false_positive_rate(self) -> float:
        """Target false-positive rate the table's bloom filter is sized for."""
        return self._bloom_rate

    @property
    def expected_keys(self) -> int | None:
        """Key count the filter was sized from, or ``None`` if sized at finish."""
        return self._expected_keys

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
        # Every key reaches the filter, one way or the other: hashed straight in
        # when the size was known up front, held for finish to size from when it
        # was not.
        bloom = self._bloom
        if bloom is not None:
            bloom.add(key)
        elif self._pending_keys is not None:
            self._pending_keys.append(key)
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

        The bloom filter section holds one serialized filter, built from every
        key in the table. When the writer was not told how many keys to expect,
        this is where the filter is sized and filled, from the keys held since
        the first :meth:`add`.

        The footer goes last, after every section it describes, which is what
        makes it the commit point: it cannot be written until the offsets it
        records are facts, and until it is there the file answers no to
        :func:`is_complete_sstable`.

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
            bloom = self._build_bloom_filter()
            self._file.write(bloom.serialize())
            bloom_filter_end = self._file.tell()

            footer = SSTableFooter(
                data_block_offset=self._data_block_offset,
                data_block_end=data_block_end,
                index_offset=data_block_end,
                index_end=index_end,
                bloom_filter_offset=index_end,
                bloom_filter_end=bloom_filter_end,
                record_count=self._record_count,
            )
            footer_offset = bloom_filter_end
            self._file.write(footer.encode())
            footer_end = self._file.tell()

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
            footer_offset=footer_offset,
            footer_end=footer_end,
            record_count=self._record_count,
            index=index,
            bloom_filter=bloom,
        )

    def _build_bloom_filter(self) -> BloomFilter:
        """Return the filter to store, sizing and filling it now if that was deferred.

        An empty table still gets a filter rather than an empty section. A one
        bit filter with nothing added answers "definitely absent" for every key,
        which is the true answer for a table with no keys, and it keeps the
        section one shape: a reader never has to handle a table whose filter is
        missing, and so never has a path where a missing filter is quietly read
        as "might contain anything".

        The held keys are dropped once they are hashed in. They were only ever
        kept to count them, and a filter stores no keys, so holding them past
        this point would be holding the one thing the structure exists not to
        store.
        """
        if self._bloom is not None:
            return self._bloom
        pending = self._pending_keys or []
        bloom = BloomFilter.for_target(max(1, len(pending)), self._bloom_rate)
        for key in pending:
            bloom.add(key)
        self._bloom = bloom
        self._pending_keys = None
        return bloom

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


def _validate_expected_keys(expected_keys: int | None) -> int | None:
    """Return ``expected_keys`` if it is a usable count, else raise.

    Zero is accepted, since a table with no records is a table a flush can
    legitimately produce, and it is not the same as ``None``: zero says the
    caller counted and found nothing, ``None`` says the caller did not count.
    """
    if expected_keys is None:
        return None
    if not isinstance(expected_keys, int) or isinstance(expected_keys, bool):
        raise TypeError(f"expected_keys must be an int or None, got {type(expected_keys).__name__}")
    if expected_keys < 0:
        raise ValueError(f"expected_keys must not be negative, got {expected_keys}")
    return expected_keys


def _validate_false_positive_rate(rate: float) -> float:
    """Return ``rate`` if a filter can be sized for it, else raise.

    The same range ``ledgerlog.bloom`` enforces, checked early: zero makes the
    sizing formula diverge and one means every query is a false positive, which
    is the same as carrying no filter at all.
    """
    if not isinstance(rate, (int, float)) or isinstance(rate, bool):
        raise TypeError(f"bloom_false_positive_rate must be a float, got {type(rate).__name__}")
    if not 0.0 < rate < 1.0:
        raise ValueError(f"bloom_false_positive_rate must be strictly between 0 and 1, got {rate}")
    return float(rate)


def write_sstable(
    path: str | os.PathLike[str],
    records: Iterable[tuple[bytes, bytes | None]],
    *,
    index_interval: int = DEFAULT_INDEX_INTERVAL,
    expected_keys: int | None = None,
    bloom_false_positive_rate: float = DEFAULT_BLOOM_FALSE_POSITIVE_RATE,
) -> SSTableLayout:
    """Write ``records`` to a new SSTable at ``path`` and return its layout.

    ``records`` is an iterable of ``(key, value)`` pairs in ascending key order,
    where a value of ``None`` is a tombstone. Plain tuples rather than a memtable
    type, so that CLAUDE.md's rule about independently testable components holds:
    the SSTable format does not need to import the memtable to be written, and a
    caller flushing one adapts its entries in a generator expression.

    ``expected_keys`` and ``bloom_false_positive_rate`` are passed through to
    :class:`SSTableWriter`. They are not defaulted from ``len(records)`` here,
    because ``records`` is an iterable and measuring it would mean consuming it,
    which would defeat the streaming this function exists to do.
    """
    with SSTableWriter(
        path,
        index_interval=index_interval,
        expected_keys=expected_keys,
        bloom_false_positive_rate=bloom_false_positive_rate,
    ) as writer:
        for key, value in records:
            writer.add(key, value)
        return writer.finish()


class SSTableReader:
    """Answers lookups against one finished SSTable file.

    The read path is the one ARCHITECTURE.md section 5 describes for a single
    table: parse the footer, binary search the sparse index for the greatest
    indexed key at or below the target, then scan the data block forward from
    that offset. The scan stops at the first key greater than the target, since
    the block is sorted and no later record can hold the key, so a lookup reads
    one index interval of records at most, plus the one that ends the scan,
    whether it hits or misses. That bound, not the search itself, is the reason
    the index exists: without it every miss would cost a pass over the table.

    The footer is read first because nothing else in the file can be located
    until it has been: the data block's end, and therefore where the scan has to
    stop, is recorded there and nowhere else. The header is validated straight
    after, before any record is decoded, so that a file this build cannot parse
    is reported as such rather than being half read.

    The sparse index is decoded once, at open, and kept resident. It is the one
    section small enough to hold for the life of the reader (one key in N), and
    holding it is what makes a lookup one seek and a short scan rather than a
    walk of the file. The data block is never held: it is the part that does not
    fit, which is the whole premise of the format.

    The reader owns the stream's position, and a lookup moves it. Two lookups
    cannot be interleaved on one reader for the same reason two record iterators
    cannot share a handle, so a caller wanting concurrent lookups opens a reader
    per thread rather than sharing one.

    What this reader does not do is decide the fate of a file it cannot open. It
    raises, with the distinction that matters preserved (an incomplete table, a
    footer that cannot describe this file, a table in a version it does not know,
    an index that cannot be the writer's), and :func:`inspect_sstable` is what
    turns those into the verdict a discovery pass acts on. The bounds checks the
    offsets go through before any of them reaches a seek are
    :func:`read_footer`'s, because a length taken off a damaged disk must not be
    handed to ``read`` before it is known to fit in the file.
    """

    def __init__(
        self,
        stream: BinaryIO,
        *,
        path: str | os.PathLike[str] | None = None,
        owns_stream: bool = False,
    ) -> None:
        """Parse the table open on ``stream`` and keep it ready for lookups.

        Taking a stream rather than a path, with :meth:`open` as the classmethod
        that supplies one, keeps ownership explicit: a reader built here reads a
        handle somebody else opened and will not close it unless told to, which
        is what lets a caller layer something over the handle (a counter, a
        cache) or hand the same file to two readers.

        ``path`` is carried for error messages and for callers tracking which
        table a reader belongs to. It is not opened or resolved here.
        """
        self._stream = stream
        self._path = Path(path) if path is not None else None
        self._owns_stream = owns_stream
        self._closed = False

        file_size = stream.seek(0, os.SEEK_END)
        footer = read_footer(stream, file_size=file_size)
        if footer.format_version != SSTABLE_FORMAT_VERSION:
            raise SSTableUnsupportedVersionError(footer.format_version)
        self._footer = footer

        stream.seek(0)
        read_file_header(stream)

        raw_index = self._read_section("sparse index", footer.index_offset, footer.index_end)
        self._index = SparseIndex.decode(
            raw_index,
            data_block_start=footer.data_block_offset,
            data_block_end=footer.data_block_end,
        )

    @classmethod
    def open(cls, path: str | os.PathLike[str]) -> SSTableReader:
        """Open the SSTable at ``path`` and return a reader that owns its handle.

        The handle is closed if parsing fails, so a file that turns out not to be
        a complete table leaves no descriptor behind for a caller who never got
        an object to close.
        """
        # The builtin, not this classmethod: a method body resolves names
        # against module and builtin scope, never against the class body.
        stream = open(path, "rb")
        try:
            return cls(stream, path=path, owns_stream=True)
        except BaseException:
            stream.close()
            raise

    @property
    def path(self) -> Path | None:
        """Path this table was opened from, when the caller supplied one."""
        return self._path

    @property
    def footer(self) -> SSTableFooter:
        """The footer this reader parsed, which is where every section's extent comes from."""
        return self._footer

    @property
    def index(self) -> SparseIndex:
        """The sparse index decoded from the file, held resident for lookups."""
        return self._index

    @property
    def record_count(self) -> int:
        """Number of records in the data block, as the footer reports it."""
        return self._footer.record_count

    @property
    def closed(self) -> bool:
        """True once :meth:`close` has been called."""
        return self._closed

    def lookup(self, key: bytes) -> SSTableRecord | None:
        """Return the record stored for ``key``, or ``None`` if this table has none.

        A returned record whose value is ``None`` is a tombstone, which is a
        different answer from ``None`` here: the table says the key was deleted,
        and per ARCHITECTURE.md section 5 that has to stop the read path rather
        than let an older table's value for the key surface. Collapsing the two
        into one return value is exactly the bug the distinction exists to
        prevent, so the record is handed back whole and the caller decides.
        """
        if not isinstance(key, bytes):
            raise TypeError(f"key must be bytes, got {type(key).__name__}")
        self._ensure_open()

        start = self._index.offset_for(key)
        if start is None:
            # The first record is always indexed, so a key below every indexed
            # key is below every key in the table. No scan can find it.
            return None

        for record in iter_records(
            self._stream, start_offset=start, end_offset=self._footer.data_block_end
        ):
            if record.key == key:
                return record
            if record.key > key:
                # Records ascend, so the target would have been passed by now.
                return None
        return None

    def close(self) -> None:
        """Release the stream if this reader owns it. Safe to call more than once.

        A reader built over a caller's stream leaves it open, because closing a
        handle this object was only lent would break whoever lent it.
        """
        if self._closed:
            return
        self._closed = True
        if self._owns_stream:
            self._stream.close()

    def _ensure_open(self) -> None:
        """Raise if the reader has been closed, rather than reading a dead handle."""
        if self._closed:
            raise ValueError("cannot read from a closed SSTableReader")

    def _read_section(self, name: str, start: int, end: int) -> bytes:
        """Read a bounds-checked section's bytes, insisting on all of them.

        The bounds are :func:`read_footer`'s, already applied by the time this
        runs. The short read below is therefore not the bounds check itself but
        the case it cannot cover: a file that shrank between the two reads.
        """
        length = end - start
        self._stream.seek(start)
        raw = self._stream.read(length)
        if len(raw) < length:
            raise SSTableFooterError(
                f"read {len(raw)} of {length} bytes of the {name} section at offset {start}"
            )
        return raw

    def __enter__(self) -> SSTableReader:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
