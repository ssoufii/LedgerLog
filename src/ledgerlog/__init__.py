"""LedgerLog, a log-structured key-value storage engine.

The public ``LedgerLog`` class is added once the engine is wired together (see
``docs/ROADMAP.md``). Until then the components live in their own modules and are
imported directly, for example ``from ledgerlog.wal import WalWriter``.
"""

__all__: list[str] = []
