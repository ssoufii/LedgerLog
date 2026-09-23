"""Bloom filter: the per-SSTable membership summary that lets a read skip a file.

Scope of this module today (story M5.1): sizing a filter from an expected key
count and a target false-positive rate, adding keys to it, and answering
"possibly present" or "definitely absent" for a key. Serialization to and from
bytes is story M5.2 and is deliberately not here yet, so nothing in this module
writes to disk. The bit order and the hash construction are still documented
below, because M5.2 has to freeze exactly these choices into a stored format and
a format is much harder to justify after the fact than before.

What the structure is, in one paragraph: a bit array of ``m`` bits and ``k``
hash functions. Adding a key sets the ``k`` bits that key hashes to. Querying a
key checks those same ``k`` bits: if any one of them is clear, no ``add`` can
have touched it, so the key was definitely never added. If all ``k`` are set,
the key was probably added, but some other combination of keys may have set
those bits between them. That asymmetry is the whole point for the read path in
ARCHITECTURE.md section 5: a "definitely absent" is trustworthy enough to skip
opening an SSTable, and a false "possibly present" only costs the search that
would have happened anyway.

Why false negatives are impossible by construction and not by testing: bits are
only ever set, never cleared. There is no ``remove``, and adding this filter to
a deleting API would be a bug rather than a feature, because clearing a bit for
one key would silently un-add every other key that shares it. A key whose bits
were set by ``add`` therefore still has them set at query time, no matter what
was added in between. The property test in ``tests/test_bloom.py`` exists to
catch an implementation that does not match this argument (an indexing mistake
that reads a different bit than it wrote, say), not to establish the argument.

Why ``hashlib.blake2b`` and not the built-in ``hash()``: ``hash()`` on bytes is
salted per process by ``PYTHONHASHSEED``, so a filter built in one process and
queried in another would set one set of bits and check a different one. That is
a false negative, the one error this structure is not allowed to make, and it
would only appear after a restart. Since M5.3 stores these filters inside
SSTable files that outlive the process that wrote them, the digest has to be a
fixed function of the key bytes and nothing else. ``blake2b`` is in the standard
library (CLAUDE.md rules out a dependency for this), is fast on short inputs,
and takes a ``digest_size`` argument, so the 16 bytes wanted here are produced
directly instead of by hashing wide and discarding most of it.

Why two digests drive ``k`` hash functions instead of ``k`` separate hashes:
Kirsch and Mitzenmacher showed that ``g_i(x) = h1(x) + i * h2(x)`` behaves, for
bloom filter purposes, like ``k`` independent hashes, with no measurable
penalty to the false-positive rate. It replaces ``k`` hash computations per
operation with one, which matters because the read path calls
:meth:`BloomFilter.might_contain` once per SSTable per lookup. The two halves of
a single 128 bit blake2b digest supply ``h1`` and ``h2``.

Why ``h2`` is forced odd: the index is ``(h1 + i * h2) % m``. If ``h2`` and
``m`` share a factor, the sequence of indices cycles through fewer than ``k``
distinct positions, which quietly lowers the effective ``k`` and raises the
false-positive rate above the configured target. Setting the low bit of ``h2``
makes it odd, so it is coprime with any power of two and shares a factor with
far fewer values of ``m`` in general. This costs one bit of entropy in ``h2``
and removes the worst degenerate case.

Bit order inside the array, fixed here for M5.2 to serialize: bit ``i`` lives in
byte ``i // 8`` at bit position ``i % 8``, counting from the least significant
bit of that byte. This is stated as a rule rather than left as whatever the code
happens to do, because the array is going to be written to disk verbatim and a
reader in another language has to be able to find bit ``i`` without reading this
implementation.
"""

from __future__ import annotations

import hashlib
import math
import struct

__all__: list[str] = [
    "BloomFilter",
    "optimal_bit_count",
    "optimal_hash_count",
]


# The digest is split into two 64 bit halves, little endian, to feed the double
# hashing above. 16 bytes is the smallest blake2b digest that supplies both
# halves at full width.
_DIGEST_SIZE = 16
_DIGEST_STRUCT = struct.Struct("<QQ")

# Guard rails on sizing. A filter is an in-memory bit array, so a caller that
# asks for an absurd expected key count or a vanishingly small false-positive
# rate is asking for an allocation large enough to take the process down. These
# turn that into an argument error instead of an attempted allocation. The
# ceiling is 2**33 bits, which is 1 GiB of backing bytes: far above any SSTable
# this engine builds (ten bits per key is the usual figure, so this covers most
# of a billion keys) and far below anything that would exhaust memory on its
# own. The bound matters most for the constructor rather than for the formulas,
# because M5.2 will build a filter from a bit count read out of a file, and a
# length read off disk is exactly the number that must not be trusted into an
# allocation. Capping k bounds the per-query work the same way.
_MAX_BIT_COUNT = 1 << 33
_MAX_HASH_COUNT = 64


def optimal_bit_count(expected_keys: int, false_positive_rate: float) -> int:
    """Return the bit array size ``m`` for ``expected_keys`` at the target rate.

    The standard result: ``m = -n * ln(p) / (ln 2)^2``, rounded up. It is the
    smallest array for which the expected fraction of set bits, after ``n``
    insertions with the matching optimal ``k``, leaves the chance of all ``k``
    bits of an absent key being set at ``p``.

    Exposed as a module function rather than buried in the constructor so the
    sizing can be checked on its own. A filter that is simply too small answers
    every query "possibly present", which is not wrong, only useless, and would
    pass any test that looked at answers alone.
    """
    _require_positive_int("expected_keys", expected_keys)
    _require_rate(false_positive_rate)
    bits = math.ceil(-expected_keys * math.log(false_positive_rate) / (math.log(2) ** 2))
    # Refused rather than clamped to the ceiling. Silently returning a smaller
    # array would hand back a filter that cannot reach the rate it was asked
    # for, and the caller would have no way to notice: an undersized filter
    # answers every query "possibly present", which is a legal answer. An
    # argument this far out of range is a mistake worth reporting.
    if bits > _MAX_BIT_COUNT:
        raise ValueError(
            f"a filter for {expected_keys} keys at rate {false_positive_rate} needs "
            f"{bits} bits, above the {_MAX_BIT_COUNT} bit ceiling"
        )
    # A very high target rate can make the formula ask for fewer bits than there
    # are keys, or even zero bits. One bit is the floor that keeps the array
    # addressable; the rate such a filter delivers is the caller's own choice.
    return max(1, bits)


def optimal_hash_count(bit_count: int, expected_keys: int) -> int:
    """Return the number of hash functions ``k`` that minimizes the rate.

    The standard result: ``k = (m / n) * ln 2``, rounded to the nearest integer.
    Both directions away from it cost accuracy: too few hashes and each key
    constrains too little of the array, too many and the array saturates after
    fewer keys than it was sized for.

    Refused rather than clamped above the ceiling, for the same reason as
    :func:`optimal_bit_count`: a quietly reduced ``k`` is a filter that misses
    its target with nothing to show for it. Reaching the ceiling takes a target
    rate below about ``1e-19``, which is not a rate anything here asks for.
    """
    _require_positive_int("bit_count", bit_count)
    _require_positive_int("expected_keys", expected_keys)
    hashes = round((bit_count / expected_keys) * math.log(2))
    if hashes > _MAX_HASH_COUNT:
        raise ValueError(
            f"{bit_count} bits for {expected_keys} keys implies {hashes} hash functions, "
            f"above the {_MAX_HASH_COUNT} ceiling"
        )
    return max(1, hashes)


def _require_positive_int(name: str, value: int) -> None:
    """Reject a non-integer or non-positive sizing argument.

    ``bool`` is excluded explicitly because it is a subclass of ``int``, and
    ``BloomFilter(bit_count=True)`` should be an error rather than a one bit
    filter that answers "possibly present" to everything.
    """
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be an int, got {type(value).__name__}")
    if value < 1:
        raise ValueError(f"{name} must be at least 1, got {value}")


def _require_rate(false_positive_rate: float) -> None:
    """Reject a target rate outside the open interval (0, 1).

    Zero is not merely unreachable, it makes the sizing formula diverge: no
    finite bit array gives a bloom filter a false-positive rate of zero. One
    means every query is a false positive, which is the same as having no
    filter.
    """
    if not isinstance(false_positive_rate, (int, float)) or isinstance(false_positive_rate, bool):
        raise TypeError(
            f"false_positive_rate must be a float, got {type(false_positive_rate).__name__}"
        )
    if not math.isfinite(false_positive_rate):
        raise ValueError(f"false_positive_rate must be finite, got {false_positive_rate}")
    if not 0.0 < false_positive_rate < 1.0:
        raise ValueError(
            f"false_positive_rate must be strictly between 0 and 1, got {false_positive_rate}"
        )


class BloomFilter:
    """A bit array plus ``k`` hash functions, answering set membership one-sidedly.

    Construct one with :meth:`for_target` when the expected key count and the
    tolerable false-positive rate are known, which is the SSTable case: the
    number of keys in a frozen memtable is known before the table is written.
    The direct constructor takes ``bit_count`` and ``hash_count`` instead, for
    the case where those two numbers came from somewhere other than the sizing
    formula, which is what M5.2 needs when it rebuilds a filter out of bytes
    read from a file.

    Not thread-safe for concurrent ``add``, and no lock is taken to make it so.
    Concurrent :meth:`might_contain` against a filter nothing is adding to is
    safe, and that is the only pattern the read path uses: a filter is built
    once, single threaded, while its SSTable is being written, and is read-only
    from the moment that file is committed. CLAUDE.md asks that a concurrency
    claim be backed by a test rather than by a docstring, so the read-only half
    of this one is exercised by many reader threads in
    ``tests/test_bloom.py::test_concurrent_readers_never_see_a_false_negative``.
    The writer half is not claimed, so a caller that does need to add from
    several threads has to serialize those calls itself.
    """

    __slots__ = ("_bits", "_bit_count", "_hash_count", "_added_count", "_target_rate")

    def __init__(
        self,
        bit_count: int,
        hash_count: int,
        *,
        bits: bytes | bytearray | None = None,
        added_count: int = 0,
        target_false_positive_rate: float | None = None,
    ) -> None:
        _require_positive_int("bit_count", bit_count)
        _require_positive_int("hash_count", hash_count)
        if bit_count > _MAX_BIT_COUNT:
            raise ValueError(f"bit_count must be at most {_MAX_BIT_COUNT}, got {bit_count}")
        if hash_count > _MAX_HASH_COUNT:
            raise ValueError(f"hash_count must be at most {_MAX_HASH_COUNT}, got {hash_count}")
        if not isinstance(added_count, int) or isinstance(added_count, bool):
            raise TypeError(f"added_count must be an int, got {type(added_count).__name__}")
        if added_count < 0:
            raise ValueError(f"added_count must not be negative, got {added_count}")

        byte_count = (bit_count + 7) // 8
        if bits is None:
            self._bits = bytearray(byte_count)
        else:
            # The length check is the point of accepting a buffer at all. A
            # caller handing over a short array (M5.2 will hand over one it read
            # from a file) would otherwise get a filter that raises IndexError
            # on some keys and answers normally on others, which is worse than
            # refusing the array outright.
            if not isinstance(bits, (bytes, bytearray)):
                raise TypeError(f"bits must be bytes or bytearray, got {type(bits).__name__}")
            if len(bits) != byte_count:
                raise ValueError(
                    f"bits must be exactly {byte_count} bytes for {bit_count} bits, got {len(bits)}"
                )
            self._bits = bytearray(bits)

        self._bit_count = bit_count
        self._hash_count = hash_count
        self._added_count = added_count
        self._target_rate = target_false_positive_rate

    @classmethod
    def for_target(cls, expected_keys: int, false_positive_rate: float) -> BloomFilter:
        """Build an empty filter sized for ``expected_keys`` at the target rate.

        The target is a design point, not a promise: it is the rate this filter
        is expected to deliver once roughly ``expected_keys`` keys have been
        added. Adding many more than that saturates the array and pushes the
        real rate above the target, which is why the count is an argument
        rather than something the filter grows into.
        """
        bit_count = optimal_bit_count(expected_keys, false_positive_rate)
        hash_count = optimal_hash_count(bit_count, expected_keys)
        return cls(
            bit_count,
            hash_count,
            target_false_positive_rate=float(false_positive_rate),
        )

    @property
    def bit_count(self) -> int:
        """Size of the bit array, ``m``."""
        return self._bit_count

    @property
    def hash_count(self) -> int:
        """Number of hash functions, ``k``."""
        return self._hash_count

    @property
    def byte_count(self) -> int:
        """Size of the backing byte array, ``ceil(m / 8)``."""
        return len(self._bits)

    @property
    def added_count(self) -> int:
        """How many times :meth:`add` was called.

        This counts calls, not distinct keys: the filter cannot tell a repeat
        from a new key, since adding a key already present sets bits that were
        already set. It is reported for diagnostics and for
        :attr:`estimated_false_positive_rate`'s sanity, never consulted by a
        query.
        """
        return self._added_count

    @property
    def target_false_positive_rate(self) -> float | None:
        """The rate this filter was sized for, or ``None`` if sized directly.

        Informational only. It records the intent behind ``m`` and ``k``, and
        changing it would not change a single answer, because the behavior is
        entirely determined by those two numbers and the bits.
        """
        return self._target_rate

    @property
    def bits(self) -> bytes:
        """An immutable copy of the backing array, in the bit order fixed above.

        A copy rather than the live ``bytearray``: handing out the buffer would
        let a caller clear a bit, and a cleared bit is a false negative for
        every key that set it. M5.2 serializes from this.
        """
        return bytes(self._bits)

    def add(self, key: bytes) -> None:
        """Record ``key`` as present, setting its ``k`` bits."""
        if not isinstance(key, bytes):
            raise TypeError(f"key must be bytes, got {type(key).__name__}")
        bits = self._bits
        for index in self._indexes(key):
            bits[index >> 3] |= 1 << (index & 7)
        self._added_count += 1

    def might_contain(self, key: bytes) -> bool:
        """Return ``False`` only if ``key`` was definitely never added.

        A ``True`` result means the key was added or the bits of some other
        combination of keys happen to cover it. Callers must treat it as "look
        in the table", never as "the key is there".
        """
        if not isinstance(key, bytes):
            raise TypeError(f"key must be bytes, got {type(key).__name__}")
        bits = self._bits
        for index in self._indexes(key):
            if not bits[index >> 3] & (1 << (index & 7)):
                return False
        return True

    def __contains__(self, key: bytes) -> bool:
        """Alias for :meth:`might_contain`, so ``key in filter`` reads naturally.

        The one-sidedness survives the sugar: ``key not in filter`` is a fact,
        ``key in filter`` is a guess.
        """
        return self.might_contain(key)

    @property
    def set_bit_count(self) -> int:
        """How many bits in the array are set.

        Walks the whole array, so it is a diagnostic rather than something a
        read path should call.
        """
        return sum(byte.bit_count() for byte in self._bits)

    @property
    def estimated_false_positive_rate(self) -> float:
        """Estimate the current rate from how full the array actually is.

        ``(set bits / m) ** k`` is the chance that all ``k`` bits an absent key
        lands on are already set, assuming the set bits are spread evenly. This
        is measured from the array rather than predicted from the configured
        target, so it stays honest if far more keys were added than the filter
        was sized for.
        """
        return (self.set_bit_count / self._bit_count) ** self._hash_count

    def _indexes(self, key: bytes) -> list[int]:
        """Return the ``k`` bit positions ``key`` maps to.

        Materialized as a list rather than yielded from a generator because
        this runs once per SSTable per lookup and ``k`` is small (single digits
        for any sensible target rate). A generator would add a frame and an
        iterator object per call for no benefit at this size.
        """
        digest = hashlib.blake2b(key, digest_size=_DIGEST_SIZE).digest()
        h1, h2 = _DIGEST_STRUCT.unpack(digest)
        # See the module docstring: an odd step keeps the probe sequence from
        # collapsing onto fewer than k distinct positions.
        h2 |= 1
        bit_count = self._bit_count
        return [(h1 + i * h2) % bit_count for i in range(self._hash_count)]

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(bit_count={self._bit_count}, "
            f"hash_count={self._hash_count}, added_count={self._added_count})"
        )
