"""Read-only, JSON-shaped views of a running engine, for tools and the web demo.

Why this module exists at all, since every number in it is already readable from
:class:`~ledgerlog.engine.LedgerLog`: a tool that shows the engine's internals
needs the WAL's framing, the memtable's shape and each table's index and filter
in one consistent lump, and it needs them as data it can send somewhere else. A
UI assembling that for itself would have to walk the engine's attributes, decode
records, and reach past the public surface for the things the engine has no
reason to publish in that form, like the height of the skip list node holding a
key. Then every later change to the engine's internals would be a change to the
UI. :func:`snapshot` is the one place that translation happens, and it is the
only thing the web demo's bridge is allowed to call (story M11.1).

Everything here is plain JSON: ``dict``, ``list``, ``str``, ``int``, ``float``,
``bool`` and ``None``, nothing else, so a snapshot survives
``json.dumps``/``json.loads`` unchanged and can cross the boundary between
Python in a Web Worker and the page that drew it. Numbers are reported as the
engine holds them, never rounded for display: a caller that wants three
significant figures can round what it is given, but a caller handed a rounded
number cannot get the real one back.

Keys and values are ``bytes`` in the engine and text here. They are decoded as
UTF-8 with ``errors="backslashreplace"``, so a key that is not valid UTF-8 comes
back as readable text with the offending bytes written as ``\\xNN`` escapes
rather than raising or being silently replaced by question marks. That is a
lossy, one-way transformation, and it is the right one for the one purpose this
module has: the text is for a human to read. A caller that needs the bytes back
should ask the engine, not a snapshot. Byte lengths are reported alongside, from
the ``bytes`` themselves, so a value's size is always the real one even where its
text is an escape of it.

Nothing here writes to the engine, with the single exception of
:func:`compact_once`, which exists because a compaction is the one piece of the
engine's behaviour that a visitor cannot otherwise see happen.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ledgerlog.engine import LedgerLog, SSTableHandle
from ledgerlog.memtable import Memtable
from ledgerlog.sstable import iter_records
from ledgerlog.wal import NO_EXPIRY, WalOp, replay

SNAPSHOT_VERSION = 1
"""Shape version of the dictionary :func:`snapshot` returns.

Not an on-disk format and not a compatibility promise about the engine: it is
there so that a consumer written against one shape of this dictionary can tell
it is being handed another, in the one place where a mismatch would otherwise
surface as a missing key deep in a UI. Bumped whenever a field is removed or
changes meaning, not when one is added.
"""

LOCATE_VERSION = 1
"""Shape version of the dictionary :func:`locate` returns.

Separate from :data:`SNAPSHOT_VERSION` because the two answer different
questions and will not change together: a snapshot describes the engine, and a
locate describes one key's journey through it.
"""

__all__ = ["LOCATE_VERSION", "SNAPSHOT_VERSION", "compact_once", "locate", "snapshot"]


def _text(data: bytes) -> str:
    """Decode engine bytes into text for a reader, never for a round trip.

    See this module's docstring for why ``backslashreplace``: a snapshot is read
    by a person, and a key that is not UTF-8 should still be legible rather than
    raise from inside a tool that was only trying to show it.
    """
    return data.decode("utf-8", errors="backslashreplace")


def _memtable_entries(memtable: Memtable) -> list[dict[str, Any]]:
    """Describe one memtable's records, in key order, each with its tower height.

    The height is the number of skip list levels the record's node is linked
    into, which is drawn at random when the key is first inserted
    (ARCHITECTURE.md section 2). It is the one thing about a memtable that is
    neither the data nor derivable from it, which is why it is here: a drawing of
    the structure is a drawing of these heights.

    Records and heights come from one walk of the structure
    (:meth:`~ledgerlog.memtable.Memtable.entries_with_levels`), so they describe
    one state of it even if a writer is inserting while this runs.
    """
    return [
        {
            "key": _text(entry.key),
            "value": None if entry.value is None else _text(entry.value),
            "value_length": None if entry.value is None else len(entry.value),
            "tombstone": entry.is_tombstone,
            "expires_at_ms": entry.expires_at_ms if entry.has_expiry else None,
            "level": level,
        }
        for entry, level in memtable.entries_with_levels()
    ]


def _memtable_snapshot(memtable: Memtable) -> dict[str, Any]:
    """Describe one memtable: its records, how many there are and what they weigh."""
    entries = _memtable_entries(memtable)
    return {
        "entry_count": len(entries),
        "nbytes": memtable.nbytes,
        "frozen": memtable.is_frozen,
        "entries": entries,
    }


def _wal_snapshot(path: Path) -> dict[str, Any]:
    """Describe the log at ``path``: its header, its records and any damage.

    Read through :func:`~ledgerlog.wal.replay` with ``truncate=False``, which is
    the reading recovery would do without the shortening it would do after it. So
    what a snapshot shows is exactly what a restart would replay, and a torn tail
    shows up here as the reason replay would stop rather than as a record that
    quietly is not listed.

    Reading the whole log per snapshot is deliberate and is affordable for what
    this is for: a tool showing the last few records of a log a visitor is typing
    into. The log is read start to finish or not at all (ARCHITECTURE.md section
    1), so there is no cheaper way to find the tail that does not involve
    trusting an offset nobody checked.
    """
    result = replay(path, truncate=False)
    records = [
        {
            "offset": record.offset,
            "length": record.end_offset - record.offset,
            "checksum": record.checksum,
            "kind": "delete" if record.op is WalOp.DELETE else "put",
            "key": _text(record.key),
            "value_length": len(record.value),
            "expires_at_ms": (None if record.expires_at_ms == NO_EXPIRY else record.expires_at_ms),
        }
        for record in result.records
    ]
    return {
        "path": path.name,
        "record_count": len(records),
        "end_offset": result.end_offset,
        "intact": result.is_intact,
        "stopped_at": result.stopped_at,
        "reason": result.reason,
        "records": records,
    }


def _sstable_snapshot(handle: SSTableHandle) -> dict[str, Any]:
    """Describe one committed table: its footer's counts, its index and its filter.

    The first and last keys are read from the file rather than held by the
    engine, because nothing in the read path needs a table's key range and so
    nothing keeps it. They are found the way the table is built to be read
    (ARCHITECTURE.md section 3): the first record is always indexed, so the
    index's first entry is the smallest key, and the last record is at most one
    index interval past the index's last entry, so the largest key is a short
    forward scan rather than a scan of the table.

    That scan opens its own handle on the file instead of using the reader the
    engine keeps. A reader owns its stream's position, so scanning through it
    would move a cursor a concurrent :meth:`~ledgerlog.engine.LedgerLog.get` is
    using; an SSTable is immutable, so a second handle on it cannot disturb
    anything (see :func:`~ledgerlog.compaction.merge_sstables`, which opens its
    sources the same way and for the same reason).
    """
    footer = handle.footer
    bloom = handle.bloom_filter
    with handle.borrow_reader() as reader:
        index_entries = reader.index.entries

    first_key: str | None = None
    last_key: str | None = None
    if index_entries:
        first_key = _text(index_entries[0].key)
        with open(handle.path, "rb") as stream:
            scanned = list(
                iter_records(
                    stream,
                    start_offset=index_entries[-1].offset,
                    end_offset=footer.data_block_end,
                )
            )
        if scanned:
            last_key = _text(scanned[-1].key)

    return {
        "sequence": handle.sequence,
        "name": handle.path.name,
        "format_version": footer.format_version,
        "record_count": handle.record_count,
        "size_bytes": handle.size_bytes,
        "min_key": first_key,
        "max_key": last_key,
        "index_entry_count": len(index_entries),
        "bloom_bit_count": bloom.bit_count,
        "bloom_bits_set": bloom.set_bit_count,
        "bloom_hash_count": bloom.hash_count,
    }


def _compaction_snapshot(engine: LedgerLog) -> dict[str, Any]:
    """Describe the size tiers the engine's tables currently fall into.

    The plan as the engine published it, not a plan recomputed here, so what a
    tool shows is what :meth:`~ledgerlog.engine.LedgerLog.compact_once` would act
    on if it were called next.
    """
    plan = engine.compaction_plan
    policy = plan.policy
    return {
        "policy": {
            "size_ratio": policy.size_ratio,
            "min_tier_tables": policy.min_tier_tables,
        },
        "needs_compaction": plan.needs_compaction,
        "tiers": [
            {
                "sequences": list(tier.sequences),
                "table_count": tier.table_count,
                "total_size_bytes": tier.total_size_bytes,
                "ready": tier.ready,
            }
            for tier in plan.tiers
        ],
    }


def snapshot(engine: LedgerLog) -> dict[str, Any]:
    """Return a JSON-shaped description of everything ``engine`` currently holds.

    The four layers a write passes through, in the order it passes through them:
    the log, the active memtable, the memtables frozen behind it, and the tables
    on disk, plus the size tiers those tables fall into. That ordering is the
    read path's (ARCHITECTURE.md section 5), so a reader of a snapshot is reading
    the layers in the order a ``get`` would consult them.

    Read-only. Nothing in the engine changes, no record is hidden or dropped, and
    two snapshots taken with nothing happening in between are equal, which is a
    property the tests pin: a tool polling this must not be a tool that moves
    what it is watching. The one visible effect is that a table's reader may be
    opened to read its sparse index, which is a file handle the engine would open
    on the next read of that table anyway.

    Nothing here is relative to a clock. An expiry is reported as the absolute
    deadline that was stored, never as a remaining lifetime or as an "expired"
    flag, because a snapshot that compared deadlines against ``now`` would differ
    from the one taken a moment before it for reasons that have nothing to do
    with the engine. Whether a deadline has passed is the caller's to decide, and
    a caller showing it to a person has a clock of its own.
    """
    return {
        "snapshot_version": SNAPSHOT_VERSION,
        "directory": str(engine.directory),
        "closed": engine.closed,
        "fsync_policy": str(engine.fsync_policy),
        "memtable_threshold_bytes": engine.memtable_threshold_bytes,
        "flush_count": engine.flush_count,
        "wal": _wal_snapshot(engine.wal_path),
        "memtable": _memtable_snapshot(engine.active_memtable),
        "frozen_memtables": [_memtable_snapshot(memtable) for memtable in engine.frozen_memtables],
        "sstables": [_sstable_snapshot(handle) for handle in engine.sstables],
        "compaction": _compaction_snapshot(engine),
    }


def _locate_in_table(handle: SSTableHandle, key: bytes) -> dict[str, Any]:
    """Describe what one table has to say about ``key``, the way a read asks it.

    The order of the questions is the read path's (ARCHITECTURE.md section 5),
    and it is the point of this function: the filter is asked first, and the
    data block is only opened if the filter says it might be worth opening. A
    table the filter rejects reports ``searched`` false and no record, which is
    the saving a bloom filter buys, stated rather than implied.

    ``indexed`` is about this key and not about the table: the index holds every
    Nth key, so a key that is not one of them is still found, by scanning
    forward from ``scan_from_key``. Showing the difference is the only way a
    reader of this can see what "sparse" costs and saves.
    """
    bloom = handle.bloom_filter
    positions = bloom.bit_positions(key)
    might_contain = bloom.might_contain(key)

    indexed = False
    index_offset: int | None = None
    scan_from_key: str | None = None
    record_offset: int | None = None
    record_end_offset: int | None = None
    tombstone = False
    present = False

    if might_contain:
        with handle.borrow_reader() as reader:
            index = reader.index
            index_offset = index.offset_for(key)
            for entry in index.entries:
                if entry.key == key:
                    indexed = True
                if entry.offset == index_offset:
                    scan_from_key = _text(entry.key)
            record = reader.lookup(key)
        if record is not None:
            present = True
            record_offset = record.offset
            record_end_offset = record.end_offset
            tombstone = record.is_tombstone

    return {
        "sequence": handle.sequence,
        "name": handle.path.name,
        "bloom_bit_count": bloom.bit_count,
        "bloom_bit_positions": list(positions),
        "bloom_bits_set_for_key": [bloom.bit_is_set(position) for position in positions],
        "bloom_might_contain": might_contain,
        "searched": might_contain,
        "indexed": indexed,
        "index_offset": index_offset,
        "scan_from_key": scan_from_key,
        "present": present,
        "record_offset": record_offset,
        "record_end_offset": record_end_offset,
        "tombstone": tombstone,
    }


def locate(engine: LedgerLog, key: bytes | str) -> dict[str, Any]:
    """Return a JSON-shaped account of where one key is, layer by layer.

    :func:`snapshot` describes the engine; this describes one key's passage
    through it, which is a different question and not one a caller can answer
    from a snapshot. A snapshot says a table holds 400 records between two
    bounds and that its filter has 912 bits set; it cannot say whether this key
    is one of the indexed ones, which bits this key lands on, or which table
    actually answers for it. Working that out from a snapshot would mean
    reimplementing the read path in the caller, which is exactly what the web
    demo must not do.

    The layers are reported in the order a ``get`` consults them: the log the
    write landed in first, the active memtable, the memtables frozen behind it,
    then every table on disk newest first. ``answered_by`` names the layer that
    would win, so a caller does not have to re-derive precedence either.

    Read-only, like :func:`snapshot`, and in the same way: a table's reader may
    be opened to read its index, which the next ``get`` on that table would
    open anyway. Nothing is written and no record is dropped or hidden, an
    expired one included, for the reason given on :func:`snapshot`: whether a
    deadline has passed is the caller's to decide against its own clock.

    ``key`` is taken as ``bytes``, or as text that is encoded UTF-8 first, which
    is the same encoding :func:`snapshot` decodes with. Every key in the result
    is text, decoded the way this module's docstring describes.

    Not cheap, and not meant to be: it may open every table whose filter accepts
    the key. It is a tool's call, made when a person asks where something is,
    not something to put on a read path.
    """
    key_bytes = key.encode("utf-8") if isinstance(key, str) else key
    if not isinstance(key_bytes, bytes):
        raise TypeError(f"key must be bytes or str, got {type(key).__name__}")
    key_text = _text(key_bytes)

    # The newest log record naming this key. Later records shadow earlier ones,
    # so the last match is the one the log would replay last and therefore the
    # one that is true.
    wal_record: dict[str, Any] | None = None
    for record in _wal_snapshot(engine.wal_path)["records"]:
        if record["key"] == key_text:
            wal_record = record

    memtable_entry: dict[str, Any] | None = None
    answered_by: str | None = None
    for position, memtable in enumerate((engine.active_memtable, *engine.frozen_memtables)):
        match = next(
            (entry for entry in _memtable_entries(memtable) if entry["key"] == key_text),
            None,
        )
        if match is not None:
            memtable_entry = {**match, "frozen": position > 0}
            answered_by = "memtable" if position == 0 else "frozen_memtable"
            break

    tables = [_locate_in_table(handle, key_bytes) for handle in engine.sstables]
    answering_table: int | None = None
    for table in tables:
        if table["present"]:
            answering_table = table["sequence"]
            break
    if answered_by is None and answering_table is not None:
        answered_by = "sstable"

    return {
        "locate_version": LOCATE_VERSION,
        "key": key_text,
        "key_bytes": len(key_bytes),
        "wal": wal_record,
        "memtable": memtable_entry,
        "sstables": tables,
        "answered_by": answered_by,
        "answering_sequence": answering_table,
    }


def compact_once(engine: LedgerLog) -> dict[str, Any] | None:
    """Run one tier merge on ``engine``, synchronously, and describe what it did.

    Returns ``None`` when there was nothing to merge, and otherwise the source
    tables' sequence numbers, the sequence number of the table that replaced
    them, and how many records the merge did not carry forward: the older copies
    of repeated keys, the tombstones nothing older still needed, and the records
    whose deadline had passed (ARCHITECTURE.md section 4).

    The merge itself is :meth:`~ledgerlog.engine.LedgerLog.compact_once`'s, which
    is the engine's own compaction code; this is the JSON shape of its result and
    nothing more. It is exposed here, beside :func:`snapshot`, because a tool
    driving the engine should need one import and not two, and because a snapshot
    taken either side of this call is how a visitor sees what a compaction is.

    One step, on the calling thread, is the only form offered. The web demo runs
    this engine in Pyodide, where a thread cannot be started at all, so a
    compaction that ran in the background would not run; and a compaction worth
    watching is one that happens when the visitor asks and has finished when the
    call returns.
    """
    run = engine.compact_once()
    if run is None:
        return None
    return {
        "source_sequences": list(run.source_sequences),
        "output_sequence": run.output_sequence,
        "source_record_count": run.source_record_count,
        "output_record_count": run.output_record_count,
        "records_dropped": run.records_dropped,
    }
