# Tuning the default sparse index interval, bloom target, and size-tier thresholds

This is the writeup story M10.4 asks for: the trade-offs measured across a matrix
of candidate values for the three tunable defaults, and the values chosen from
them. Every figure below comes from a `python -m ledgerlog.bench` run whose exact
invocation is printed next to it. The bench harness prints the full invocation at
the top of every report (see `src/ledgerlog/bench.py`), so any number here can be
reproduced by running the command shown above its table.

The spike also added the two pieces of bench machinery it needed to measure these
at all: the four tunables are now CLI arguments that reach the engine through its
constructor (so a sweep measures a configuration a real caller could ask for, not
a monkey-patched global), and the read-amplification phase learned an
`incremental` compaction schedule that rebuilds the dataset flush by flush. The
second one matters for the size-tier knobs: compacting a finished directory in
one `bulk` pass puts every equally sized flush in a single tier and merges the
lot at once, so `size_ratio` and `min_tier_tables` land on the same answer
whatever they are set to. Only a flush-by-flush schedule produces the mix of
table sizes those two knobs exist to govern.

## Machine and fixed parameters

- CPU: Intel Xeon @ 2.10GHz, 4 vCPU
- OS: Ubuntu 24.04.5 LTS, Linux 6.18
- Python: CPython 3.11.18
- Filesystem: ext4 on a real disk (not tmpfs; the bench report's filesystem line
  confirms this, so fsync figures would describe durable writes)

Every run below holds these constant and varies one axis at a time:

```
--records 100000 --value-size 100 --reads 20000 --fsync-policy never
--memtable-threshold-bytes 131072 --seed 0 --read-amplification
```

`--fsync-policy never` on purpose: this spike compares the read path and the
compaction trade-off, where the WAL's durability policy changes nothing and only
adds timing noise. `--memtable-threshold-bytes 131072` is low enough to produce
roughly 96 small SSTables from 100k records, which is what makes read
amplification and compaction measurable at all. Latency figures are the median of
three trials, because repeated runs showed read throughput and latency carry a
run-to-run spread of roughly 7 to 20 percent on this shared machine, enough to
swamp a single sample but not the monotonic trends below.

## Sparse index interval: chose 32 (was 64)

Interval N is the number of records between two sparse index entries. A larger N
means a smaller resident index and a longer forward scan after the binary search;
a smaller N means the opposite. Varied with:

```
python -m ledgerlog.bench ... --index-interval {N} --bloom-fp-rate 0.01 \
    --size-ratio 2.0 --min-tier-tables 4 --compaction-schedule bulk
```

| N   | index bytes/record | median p50 read | median p99 read |
|-----|--------------------|-----------------|-----------------|
| 16  | 2.38               | 0.138 ms        | 0.345 ms        |
| 32  | 1.19               | 0.173 ms        | 0.423 ms        |
| 64  | 0.59               | 0.200 ms        | 0.500 ms        |
| 128 | 0.30               | 0.294 ms        | 0.684 ms        |
| 256 | 0.15               | 0.435 ms        | 1.044 ms        |
| 512 | 0.08               | 0.814 ms        | 1.912 ms        |

The curve has a sharp cliff above 64 and a gentle, near-linear trade below it.
Widening 64 to 128 costs about 94 microseconds of p50 for 0.3 fewer index bytes
per record (about 950 microseconds saved per byte per record if read the other
way), while halving 64 to 32 buys about 28 microseconds for 0.59 more bytes
(about 47 per byte) and 32 to 16 buys about 35 for 1.19 more (about 29 per byte).

Chose 32. It takes a real step down from the old untuned default of 64 on this
engine's headline metric, read latency, for an index that is still well under one
percent of the table it indexes even for a table far larger than one flush
produces, and it stops short of 16, whose further gain no longer justified
doubling the index again. The old 64 was a reasoning-based guess (its own
docstring said so); 32 is the measured choice.

## Bloom filter false-positive target: kept 0.01

The bloom target trades filter size (bits per key) against how often a read opens
a table that cannot hold its key. Measured in the compacted steady state a
running engine lives in (the `incremental` schedule), since that is where a read
actually searches more than one table:

```
python -m ledgerlog.bench ... --index-interval 64 --bloom-fp-rate {rate} \
    --size-ratio 2.0 --min-tier-tables 4 --compaction-schedule incremental
```

| target | bits/key (approx) | measured FP rate | mean tables searched/read |
|--------|-------------------|------------------|---------------------------|
| 0.1    | 4.8               | 9.89%            | 1.149                     |
| 0.05   | 6.2               | 5.03%            | 1.076                     |
| 0.01   | 9.6               | 1.03%            | 1.015                     |
| 0.005  | 11.0              | 0.58%            | 1.009                     |
| 0.001  | 14.4              | 0.12%            | 1.002                     |

One in a hundred is the knee. Loosening to one in twenty raises the mean tables a
read searches from about 1.015 to 1.076 and the cost climbs steeply above that
(1.149 at one in ten). Tightening to one in two hundred buys only about 0.007
fewer tables searched per read for almost another 1.4 bits per key, and one in a
thousand even less for more again. Kept 0.01: the measurement put it exactly at
the knee, which is also the classic 10-bits-per-key point.

## Size-tier thresholds: kept size_ratio 2.0, min_tier_tables 4

`min_tier_tables` is how many similarly sized tables a tier must hold before it is
merged; `size_ratio` is how far the largest table in a tier may exceed the
smallest. Both only bite under the `incremental` schedule. Measured at two
dataset sizes so the choice is not fitted to one table count:

```
python -m ledgerlog.bench ... --index-interval 64 --bloom-fp-rate 0.01 \
    --size-ratio {ratio} --min-tier-tables {min} --compaction-schedule incremental
```

100k records (96 tables before compaction):

| ratio | min_tier | merge passes | write amplification | mean tables searched/read |
|-------|----------|--------------|---------------------|---------------------------|
| 1.5   | 4        | 31           | 2.66                | 1.015                     |
| 2.0   | 2        | 93           | 6.39                | 1.019                     |
| 2.0   | 3        | 46           | 3.62                | 1.029                     |
| 2.0   | 4        | 31           | 2.66                | 1.015                     |
| 2.0   | 6        | 18           | 1.75                | 1.040                     |
| 2.0   | 8        | 13           | 1.67                | 1.029                     |
| 4.0   | 4        | 29           | 2.27                | 1.061                     |

200k records (192 tables before compaction), same pattern:

| ratio | min_tier | write amplification | mean tables searched/read |
|-------|----------|---------------------|---------------------------|
| 2.0   | 2        | 6.79                | 1.028                     |
| 2.0   | 4        | 3.00                | 1.010                     |
| 2.0   | 8        | 2.00                | 1.010                     |
| 4.0   | 4        | 3.05                | 1.041                     |

Write amplification here is merged bytes written per byte flushed, the cost side
of compacting sooner. `min_tier_tables` of 2 roughly doubles that cost (about 6.8
against about 3.0 at four) for no improvement in tables searched. Raising it to 8
cuts the cost further but leaves a read searching more tables at 100k and reacts
more slowly as the dataset grows. `size_ratio` of 4 leaves more tables standing
for a read to search at a similar write cost, and 1.5 moves neither figure enough
to matter. Kept ratio 2.0 and min_tier_tables 4: the measurement put the balance
between read amplification and write amplification right there, which is also the
usual size-tiered default.

## Summary of chosen defaults

| parameter                     | old   | chosen | source |
|-------------------------------|-------|--------|--------|
| sparse index interval         | 64    | **32** | `sstable.DEFAULT_INDEX_INTERVAL` |
| bloom false-positive target   | 0.01  | 0.01   | `sstable.DEFAULT_BLOOM_FALSE_POSITIVE_RATE` |
| size-tier ratio               | 2.0   | 2.0    | `compaction.DEFAULT_SIZE_RATIO` |
| size-tier min tables          | 4     | 4      | `compaction.DEFAULT_MIN_TIER_TABLES` |

Only the index interval moved. The other three were measured and kept at the knee
of their own curves rather than left unexamined; the sweeps above are the record
of that.
