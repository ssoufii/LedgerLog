"""LedgerLog, a log-structured key-value storage engine.

:class:`~ledgerlog.engine.LedgerLog` is the public entry point: it wires the
write-ahead log and the memtable into a store with ``put``, ``get`` and
``delete``. The components it is built from stay importable on their own, for
example ``from ledgerlog.wal import WalWriter``, because each one is meant to be
usable and testable without booting the engine (see ``CLAUDE.md``).

The class is re-exported here rather than defined here so that importing the
package does not mean importing every component, and so ``engine.py`` can hold
the ordering argument the write path rests on next to the code that implements
it.
"""

from ledgerlog.engine import LedgerLog

__all__: list[str] = ["LedgerLog"]
