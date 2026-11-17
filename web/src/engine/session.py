"""The demo's one Python object: a LedgerLog database and the calls onto it.

This file is the whole of the Python the web demo adds to the engine, and it
deliberately adds no behaviour. Every answer it returns comes from
``ledgerlog``: the records from :mod:`ledgerlog.introspect`, the merges from
:meth:`ledgerlog.engine.LedgerLog.compact_once`, the tables from
:meth:`ledgerlog.engine.LedgerLog.flush`. What it does is translate, in two
directions:

* text to bytes. A visitor types text, and the engine stores bytes, so keys and
  values are encoded as UTF-8 here and decoded the same way on the way back.
  Doing it in Python rather than in the worker keeps one encoding decision in
  one place, next to the engine that holds the bytes.
* objects to JSON. Everything crosses into JavaScript as a JSON string, so the
  worker never holds a reference to a Python object and nothing has to be freed
  on the JavaScript side.

The engine is opened with ``fsync_policy="never"`` and
``flush_in_background=False`` because of where this runs. Pyodide's filesystem
is memory, so a sync would cost time and promise nothing, and Pyodide cannot
start a thread at all, so the flush and the compaction have to happen on the
calling thread (which is a Web Worker, so the page stays responsive).
"""

from __future__ import annotations

import json
import os
import shutil
import sys
from typing import Any

from ledgerlog import LedgerLog, faults, introspect

DATA_DIR = "/data"
"""Directory the demo's database lives in, inside Pyodide's in-memory filesystem."""

DEMO_KEY = "user:1042:login"
"""The key the session writes once at boot, so the page has a real record to follow.

Why a real write rather than a worked example in the markup: the four stage
strip (story M11.5) shows where one record is at each stage of the write path,
read from the engine. Before a visitor has typed anything there is nothing to
read, and a card filled with plausible numbers would be the invented proof
docs/FRONTEND_DESIGN.md section 9 rules out. One write at boot costs a few
bytes of the log and makes every figure on the page true.

Written at boot and not by :meth:`DemoSession.reset`, which is the visitor
asking for an empty database and gets one.
"""

DEMO_VALUE = "2026-11-16T05:00:00Z"
"""The value stored under :data:`DEMO_KEY`, chosen to look like a real session record."""


def _decode(data: bytes) -> str:
    """Decode engine bytes for display, the way the snapshot does."""
    return data.decode("utf-8", errors="backslashreplace")


class DemoSession:
    """One open database, plus the calls the bridge is allowed to make on it."""

    def __init__(self) -> None:
        self._engine: LedgerLog | None = None
        self.reset()
        # The one write the visitor did not ask for. See DEMO_KEY: the strip
        # has to have a real record to follow before anyone has typed one.
        self.put(DEMO_KEY, DEMO_VALUE)

    @property
    def engine(self) -> LedgerLog:
        """The open engine, or an error naming the state the caller is in."""
        if self._engine is None:  # pragma: no cover - reset always opens one
            raise RuntimeError("the demo session has no open engine")
        return self._engine

    def reset(self) -> dict[str, Any]:
        """Close the current database, throw the directory away and open a new one.

        A new directory rather than a new engine over the old files, because
        "start again" has to mean the log and the tables are gone too: an engine
        reopened over the same directory would replay the visitor's last session
        back into memory, which is the right behaviour for the engine and the
        wrong one for a reset button.
        """
        if self._engine is not None:
            self._engine.close()
            self._engine = None
        shutil.rmtree(DATA_DIR, ignore_errors=True)
        os.makedirs(DATA_DIR, exist_ok=True)
        self._engine = LedgerLog(
            DATA_DIR,
            fsync_policy="never",
            flush_in_background=False,
        )
        return {"directory": str(self._engine.directory)}

    def info(self) -> dict[str, Any]:
        """Which Python is running and which engine modules it imported."""
        return {
            "pythonVersion": sys.version.split()[0],
            "modules": sorted(
                name for name in sys.modules if name == "ledgerlog" or name.startswith("ledgerlog.")
            ),
        }

    def put(self, key: str, value: str, ttl: float | None = None) -> dict[str, Any]:
        """Store ``value`` under ``key``, and report where the log recorded it."""
        engine = self.engine
        raw_key = key.encode("utf-8")
        raw_value = value.encode("utf-8")
        before = engine.wal_path.stat().st_size
        engine.put(raw_key, raw_value, ttl=ttl)
        entry = engine.active_memtable.lookup(raw_key)
        return {
            "key": key,
            "value_length": len(raw_value),
            # The offset the record starts at, which is where the log ended
            # before the append. Read from the file rather than computed from the
            # record's size, so it is the position the record really occupies.
            "wal_offset": before,
            "expires_at_ms": (
                entry.expires_at_ms if entry is not None and entry.has_expiry else None
            ),
        }

    def get(self, key: str) -> dict[str, Any]:
        """Look ``key`` up, keeping "no record" distinct from "an empty value"."""
        raw_value = self.engine.get(key.encode("utf-8"))
        return {
            "key": key,
            "found": raw_value is not None,
            "value": None if raw_value is None else _decode(raw_value),
            "value_length": None if raw_value is None else len(raw_value),
        }

    def delete(self, key: str) -> dict[str, Any]:
        """Tombstone ``key``, and report where the log recorded the delete."""
        engine = self.engine
        before = engine.wal_path.stat().st_size
        engine.delete(key.encode("utf-8"))
        return {"key": key, "wal_offset": before}

    def flush(self) -> dict[str, Any]:
        """Turn what is in memory into tables on disk, and describe them.

        The tables are described from the snapshot rather than from the layouts
        the flush returned, so that what the page shows about a table is the
        same account of it that the rest of the page is reading.
        """
        engine = self.engine
        written = engine.flush()
        sequences = {layout.path.name for layout in written}
        tables = [
            {
                "sequence": table["sequence"],
                "name": table["name"],
                "record_count": table["record_count"],
                "size_bytes": table["size_bytes"],
                "min_key": table["min_key"],
                "max_key": table["max_key"],
            }
            for table in reversed(introspect.snapshot(engine)["sstables"])
            if table["name"] in sequences
        ]
        return {"tables": tables}

    def compact_once(self) -> dict[str, Any] | None:
        """Run one tier merge, or report that there was nothing to merge."""
        return introspect.compact_once(self.engine)

    def snapshot(self) -> dict[str, Any]:
        """Everything the engine holds, as :mod:`ledgerlog.introspect` describes it."""
        return introspect.snapshot(self.engine)

    def crash(self, scenario: str) -> dict[str, Any]:
        """Stage one crash on a copy of this database and report what recovered.

        The copy, the damage and the reopen are all
        :func:`ledgerlog.faults.run_scenario`'s, which is the same code the
        engine's own crash tests drive. This session does nothing but name the
        directory, so the page cannot be shown a recovery the test suite does
        not also assert.

        A scenario the database has nothing for (a table damage on a database
        with no tables) comes back as a refusal rather than as a result with
        nothing in it, because "nothing was dropped" would read as the engine
        surviving something that never happened.
        """
        try:
            return {"ok": True, "report": faults.run_scenario(DATA_DIR, scenario)}
        except faults.ScenarioNotPossible as refused:
            return {"ok": False, "reason": str(refused)}

    def locate(self, key: str) -> dict[str, Any]:
        """Where one key is, layer by layer, as :mod:`ledgerlog.introspect` describes it.

        Separate from :meth:`snapshot` because it answers a different question
        and costs more: it may open every table whose filter accepts the key.
        The page asks for it once per refresh, for the one key the strip is
        following, not for every key on screen.
        """
        return introspect.locate(self.engine, key)


SESSION = DemoSession()


def handle(name: str, payload_json: str) -> str:
    """Run one bridge call and return its result as a JSON string.

    One entry point rather than one exported function per call, so the worker
    holds a single Python callable and the list of what the page may ask for is
    in one readable place. An unknown name is an error here rather than a
    silently empty answer: the only caller is the typed bridge, so a name that
    does not match means the two have drifted apart.
    """
    payload = json.loads(payload_json) if payload_json else {}
    if name == "put":
        result: Any = SESSION.put(payload["key"], payload["value"], payload.get("ttl"))
    elif name == "get":
        result = SESSION.get(payload["key"])
    elif name == "delete":
        result = SESSION.delete(payload["key"])
    elif name == "flush":
        result = SESSION.flush()
    elif name == "compactOnce":
        result = SESSION.compact_once()
    elif name == "snapshot":
        result = SESSION.snapshot()
    elif name == "locate":
        result = SESSION.locate(payload["key"])
    elif name == "crash":
        result = SESSION.crash(payload["scenario"])
    elif name == "reset":
        result = SESSION.reset()
    elif name == "info":
        result = SESSION.info()
    else:
        raise ValueError(f"unknown engine call {name!r}")
    return json.dumps(result)
