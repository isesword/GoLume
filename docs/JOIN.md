# Hash Join route

## 1. Reusable primitives

The low-level join package keeps specialized reusable build/probe tables:

```text
join.I32Table
join.DictionaryTable
join.PairTable
join.BytesTable
join.RadixPairTable       // opt-in experiment
```

They support inner / left / semi / anti joins. Duplicate build rows are retained
for one-to-many semantics. SQL equality NULL semantics are strict: a NULL key
never equals another NULL key.

## 2. Key shapes

### Fixed key

Fixed int32/int64/uint32/uint64 values and Arrow-compatible date/time integer
families are normalized into a fixed physical key. Two-column keys use
`hashagg.Key128` plus a validity mask, avoiding tuple objects, reflection and
string concatenation.

### Dictionary key

Equal dictionary domains can join directly by code. Different domains perform
canonicalization once at dictionary cardinality, then row-level probe remains an
integer-code operation. See `DICTIONARY_JOIN.md`.

## 3. One columnar execution path

`PrepareJoin`, `CollectJoin`, `PreparedJoin`, `JoinResult`, `JoinedRow` and
`RowRef` have been removed. Use `Join(...).Collect()` for an owned DataFrame or
`Join(...).Prepare()` for repeated execution. Close returned frames and prepared
plans. There is no partition-row-reference compatibility engine.

`physical.JoinSource` implements `source.Source` and emits materialized joined
columnar batches, enabling:

```text
Join -> Filter -> SelectExprs/WithColumns -> GroupBy -> Agg
```

without a second execution engine.

## 4. Streaming preparation

Phase 2.6 removed the old exact-cardinality probe-side prepass.

Preparation:

```text
right projected source
 -> retain required build payload
 -> build specialized hash table
 -> track max duplicate multiplicity
 -> reserve retained memory

left source
 -> partitions/stats only
 -> derive safe per-partition output upper bound
```

Execution:

```text
left partition        // read once
 -> probe immutable build table
 -> validate actual rows <= prepared upper bound
 -> reader-local joined columnar batch
 -> downstream source operator
```

`source.Stats.RowsUpperBound` is true for JoinSource because output cardinality
is discovered during probe rather than during Prepare.

## 5. Parallel radix fixed-key join

`RadixPairTable` now supports both serial and parallel build/probe. Parallel mode
uses the engine persistent scheduler through the minimal `join.ParallelExecutor`
contract; no join-specific goroutine pool is created.

Probe execution:

```text
histogram -> prefix -> scatter probe keys/row ids
                         |
                         +-> bucket-owned workers
                         |
                         +-> local PairTable probes
                         |
                         +-> per-bucket Pair buffers
                         |
                         -> serial callback replay
```

`ParallelRadixProbeWorkspace` retains histogram/scatter/result buffers per
reader. Fixed-key and validity scratch are also reader-local, avoiding new
key/mask slices for each partition.

The strategy can be forced from Lazy API with `JoinOptions.RadixBits/Workers`.
The automatic Phase 2.7 policy is deliberately conservative and only enables
radix-256 parallel when all are true:

```text
fixed key
workers >= 4
build >= 1,048,576 rows
probe >= 4,194,304 rows
```

Validation showed no durable win at 262K/1M, but about a 2.3x improvement at
1M/4M on the shared EPYC host. See the Phase 2.7 design/validation documents.

## 6. Payload projection

Optimizer dependency analysis runs before JoinSource construction. Inputs keep
only:

- equality keys;
- side-local residual-filter dependencies;
- payload columns required above the join.

Generic sources that cannot natively push projection receive a physical
ProjectSource fallback.

## 7. Memory ownership

Right build payload, row locations and hash table reservations are retained by
JoinSource and charged to `MemoryManager`. `PreparedFrame.Close` or aggregate
Prepared close releases them after active executions finish.

Joined output buffers and radix probe scratch are reader-local and reused between
partitions.

## 8. Next join work

1. LLC/NUMA-aware join cost model;
2. parallel resident radix execution for dictionary and canonical-byte keys.

## 9. Generic canonical-key joins and bounded execution

Plain UTF8 strings, mixed dictionary/plain strings, and composites containing
more than two supported integer/temporal/string columns use an injective
canonical key (type tags, fixed-width integers, and length-prefixed strings).
Dictionary-local codes are never used as equality across domains. Every NULL
component makes the equality key invalid. Inner, left, right, full, semi and
anti modes are supported.

Memory-only execution retains an exact-byte hash table. With a finite query
memory limit and an explicit finite spill quota, the same generic keys also use
execution-scoped radix partitioning and columnar spill runs. Partition hashes
only route rows; resident table lookup and the single-key-skew fallback compare
complete canonical keys. Oversized multi-key partitions are repartitioned, while
one-key duplicate partitions are processed run by run. Variable-width output
morsels are split using measured payload-byte estimates. Generic resident radix
options and bounded execution without spill configuration remain unsupported.

Spill joins do not promise the memory-only probe/build row ordering. Their
partition/run order is deterministic for unchanged sources, but callers needing
a particular result order should use an explicit sort. Output batches are
borrowed until the next stream call or Close. Run handles, metadata, encoding
scratch, decoded payloads, key tables, pair buffers and output are admitted
against the query budget; exhausted memory or disk quotas fail explicitly and
clean up the owned runs. A single input morsel, dictionary, key or output row
that cannot fit still fails rather than using an unbounded fallback. Very
fragmented inputs may also exhaust the finite metadata budget.
