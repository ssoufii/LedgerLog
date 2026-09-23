"""Bloom filter: the per-SSTable membership summary that lets a read skip a file.

Scope of this module today (stories M5.1 and M5.2): sizing a filter from an
expected key count and a target false-positive rate, adding keys to it,
answering "possibly present" or "definitely absent" for a key, and turning a
filter into bytes and back. This module still writes no files of its own. It
produces and consumes one self-contained blob, which M5.3 stores as the bloom
filter section of an SSTable.

Serialized layout (little endian, no padding)::

    [ 8B magic ][ 1B format version ][ 1B hash count ][ 8B bit count ]
    [ 8B added count ][ 8B target false-positive rate ][ 4B bit array length ]
    [ bit array ]
    [ 4B CRC32 over every byte above ]

Why the blob is self-describing when the SSTable footer already says where the
section starts and ends: those offsets are only as good as the footer they came
from, and the failure they produce when they are wrong is the worst one this
structure has. A filter rebuilt from the wrong bytes, or from the right bytes
with one flipped, answers "definitely absent" for keys the table actually
holds. That is a false negative, the read path skips the table on the strength
of it (ARCHITECTURE.md section 5), and the value is gone with nothing logged.
Every other component here can afford to trust its caller's offsets because a
bad read shows up as a parse failure. This one cannot, so the section carries
its own magic, its own length and its own checksum, and a filter is rebuilt only
from bytes that pass all three.

Why a CRC rather than structural validation alone: corruption inside the bit
array is invisible to structure. Every arrangement of bits is a legal filter,
so nothing about a damaged array looks wrong, and the damage only shows as a key
that quietly stops being found. The checksum is what turns that silent wrong
answer into a refusal to load the filter at all.

Why the target rate is stored despite changing no answer: it records what the
filter was sized for, which is what a later bench or a tuning pass compares the
measured rate against (M10.3). A rate of exactly ``0.0`` means "not recorded",
which is unambiguous because zero is not a legal target: the sizing formula
diverges there, so no filter can have been built for it.

The bit order and the hash construction are documented below and are now frozen:
the array is written to disk verbatim, so changing either would change what
stored bits mean, which per CLAUDE.md requires a new format version rather than
a quiet edit.

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
import zlib

__all__: list[str] = [
    "BLOOM_CHECKSUM_SIZE",
    "BLOOM_FORMAT_VERSION",
    "BLOOM_HEADER_SIZE",
    "BLOOM_MAGIC",
    "BloomChecksumError",
    "BloomFilter",
    "BloomFormatError",
    "BloomHeaderError",
    "BloomTruncatedError",
    "BloomUnsupportedVersionError",
    "optimal_bit_count",
    "optimal_hash_count",
]

BLOOM_FORMAT_VERSION = 1
"""Version of the serialized layout described in this module's docstring.

Stamped into every serialized filter. A build that reads a different number
reports it rather than parsing bytes whose meaning it is guessing at. Numbered
separately from the SSTable's own format version even though M5.3 stores these
blobs inside SSTables, because the two layouts change for different reasons: a
new bit order here does not move an SSTable's sections, and a new footer field
there does not change what a stored filter means.
"""

BLOOM_MAGIC = b"LEDGRBLM"
"""Fixed marker at the start of a serialized filter. Eight bytes, no terminator.

Distinct from the SSTable's markers so that a footer offset pointing at the
wrong section produces a magic mismatch instead of a filter built from a data
block.
"""

# Header fields, then the bit array, then a CRC32 over both. The length of the
# array is stored even though it is implied by the bit count, because the two
# disagreeing is exactly the evidence that the blob is not what the writer
# produced, and a reader that recomputed the length instead would have nothing
# to compare against.
_HEADER_FORMAT = "<8sBBQQdI"
_HEADER_STRUCT = struct.Struct(_HEADER_FORMAT)
_CHECKSUM_STRUCT = struct.Struct("<I")

BLOOM_HEADER_SIZE = _HEADER_STRUCT.size
"""Size of the fixed header in front of a serialized filter's bit array."""

BLOOM_CHECKSUM_SIZE = _CHECKSUM_STRUCT.size
"""Size of the CRC32 trailer that ends a serialized filter."""

# Sentinel for "this filter was sized directly, not from a target rate". Zero is
# safe to overload because _require_rate refuses it: the sizing formula diverges
# at a false-positive rate of zero, so no filter can legitimately carry it.
_RATE_UNRECORDED = 0.0


class BloomFormatError(ValueError):
    """Raised when bytes cannot be read as a serialized bloom filter."""


class BloomHeaderError(BloomFormatError):
    """Raised when a blob's header is missing or is not a bloom filter header.

    Distinct from :class:`BloomUnsupportedVersionError` because the two call for
    different responses: this one says the bytes are not a LedgerLog filter at
    all, which usually means they were read from the wrong offset, while an
    unsupported version says they are one this build cannot parse.
    """


class BloomTruncatedError(BloomFormatError):
    """Raised when a blob is shorter than the filter it claims to hold.

    Kept separate from :class:`BloomChecksumError` because a short blob is what
    a write cut off partway leaves behind, while a checksum failure means every
    byte arrived and one of them is wrong.
    """


class BloomChecksumError(BloomFormatError):
    """Raised when a blob's contents do not match the checksum stored with it.

    This is the error that makes the format safe to trust. Damage inside the bit
    array changes no structure, so without this check the filter would load and
    answer "definitely absent" for keys it was built from, and the read path
    would skip a table that holds them.
    """


class BloomUnsupportedVersionError(BloomHeaderError):
    """Raised when a serialized filter carries a format version this build does not know."""

    def __init__(self, found_version: int, expected_version: int = BLOOM_FORMAT_VERSION) -> None:
        super().__init__(
            f"bloom filter format version {found_version} is not supported by this build, "
            f"which reads and writes version {expected_version}"
        )
        self.found_version = found_version
        self.expected_version = expected_version


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
        every key that set it.
        """
        return bytes(self._bits)

    def serialize(self) -> bytes:
        """Return this filter as one self-contained blob, per the module docstring.

        The bit array is copied out verbatim rather than re-derived from the
        keys, which the filter no longer has: a bloom filter stores no keys, so
        the array is the only representation of what was added and the format
        has to preserve it byte for byte.
        """
        header = _HEADER_STRUCT.pack(
            BLOOM_MAGIC,
            BLOOM_FORMAT_VERSION,
            self._hash_count,
            self._bit_count,
            self._added_count,
            _RATE_UNRECORDED if self._target_rate is None else self._target_rate,
            len(self._bits),
        )
        body = header + bytes(self._bits)
        return body + _CHECKSUM_STRUCT.pack(zlib.crc32(body))

    @classmethod
    def deserialize(cls, blob: bytes | bytearray | memoryview) -> BloomFilter:
        """Rebuild a filter from :meth:`serialize` output, or refuse the bytes.

        Every field that sizes an allocation or bounds a slice is checked
        against the bytes actually present before it is used, so a truncated or
        damaged blob cannot turn into a large allocation or a filter built from
        a short array. The checks run in the order that gives the most specific
        answer: whether these are filter bytes at all, then whether this build
        can read them, then whether they arrived intact, then whether what they
        describe is internally consistent.

        Raises a subclass of :class:`BloomFormatError` in every rejection case.
        Returning a filter here that does not behave like the one that was
        serialized is the failure mode the whole format exists to prevent, so
        there is deliberately no lenient path and no partial recovery: a filter
        is either exactly the one that was stored or it is an exception.
        """
        if not isinstance(blob, (bytes, bytearray, memoryview)):
            raise TypeError(f"blob must be bytes-like, got {type(blob).__name__}")
        raw = bytes(blob)

        # Checked before any unpack so that a short blob is reported as short
        # rather than as a struct.error from reading past its end.
        if len(raw) < BLOOM_HEADER_SIZE + BLOOM_CHECKSUM_SIZE:
            raise BloomTruncatedError(
                f"serialized bloom filter is {len(raw)} bytes, which is shorter than the "
                f"{BLOOM_HEADER_SIZE + BLOOM_CHECKSUM_SIZE} byte header and checksum alone"
            )

        magic, version, hash_count, bit_count, added_count, target_rate, array_length = (
            _HEADER_STRUCT.unpack_from(raw)
        )
        if magic != BLOOM_MAGIC:
            raise BloomHeaderError(
                f"bloom filter magic mismatch: found {magic!r}, expected {BLOOM_MAGIC!r}"
            )
        if version != BLOOM_FORMAT_VERSION:
            raise BloomUnsupportedVersionError(version)

        # Before any header field is acted on. Past this point the fields are
        # known to be the ones the writer stored, so the checks below are
        # looking for a writer bug or a different format, not for damage.
        stored_checksum = _CHECKSUM_STRUCT.unpack_from(raw, len(raw) - BLOOM_CHECKSUM_SIZE)[0]
        # Checksummed through a memoryview rather than a slice: a slice would
        # copy the whole blob, and a blob may be most of a gigabyte at the
        # largest geometry this module allows.
        computed_checksum = zlib.crc32(memoryview(raw)[:-BLOOM_CHECKSUM_SIZE])
        if stored_checksum != computed_checksum:
            raise BloomChecksumError(
                f"serialized bloom filter checksum mismatch: stored {stored_checksum:#010x}, "
                f"computed {computed_checksum:#010x}: the bytes are damaged"
            )

        available = len(raw) - BLOOM_HEADER_SIZE - BLOOM_CHECKSUM_SIZE
        if array_length != available:
            raise BloomTruncatedError(
                f"serialized bloom filter declares a {array_length} byte bit array but carries "
                f"{available} bytes between its header and its checksum"
            )
        # The declared bit count is what a later allocation would be sized from,
        # so it is reconciled against the array that is actually present rather
        # than trusted. The range check below is deliberately redundant: the
        # length comparison after it already rejects every out of range count,
        # because an array long enough to match one could not have fit in the
        # blob. It is kept because it bounds the field where it is read, which
        # is where a reader looking for that guarantee will look, and it gives
        # the out of range case its own message instead of a length mismatch
        # reported in the billions. The check that actually carries the weight
        # is the one comparing the count to the bytes present.
        if bit_count < 1 or bit_count > _MAX_BIT_COUNT:
            raise BloomFormatError(
                f"serialized bloom filter declares {bit_count} bits, outside the supported "
                f"range of 1 to {_MAX_BIT_COUNT}"
            )
        expected_length = (bit_count + 7) // 8
        if array_length != expected_length:
            raise BloomFormatError(
                f"serialized bloom filter declares {bit_count} bits, which needs "
                f"{expected_length} bytes, but carries a {array_length} byte array"
            )
        if hash_count < 1 or hash_count > _MAX_HASH_COUNT:
            raise BloomFormatError(
                f"serialized bloom filter declares {hash_count} hash functions, outside the "
                f"supported range of 1 to {_MAX_HASH_COUNT}"
            )

        if target_rate == _RATE_UNRECORDED:
            recorded_rate: float | None = None
        else:
            # A stored rate outside (0, 1) cannot have come from for_target, so
            # the blob is not one this module wrote even though it checksums.
            if not math.isfinite(target_rate) or not 0.0 < target_rate < 1.0:
                raise BloomFormatError(
                    f"serialized bloom filter records a target false-positive rate of "
                    f"{target_rate}, which is not a rate any filter can be sized for"
                )
            recorded_rate = target_rate

        return cls(
            bit_count,
            hash_count,
            bits=raw[BLOOM_HEADER_SIZE : BLOOM_HEADER_SIZE + array_length],
            added_count=added_count,
            target_false_positive_rate=recorded_rate,
        )

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
