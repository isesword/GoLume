# Multi-column int64 aggregation

Aggregations may reduce multiple int64 columns in the same query. This applies
to grouped and global aggregates, materialized results and native columnar
streams. Repeated aggregate expressions on one input share its state lane.

```go
frame.GroupBy("customer").Agg(
    lazy.Sum("amount", "total"),
    lazy.Min("latency", "fastest"),
    lazy.Avg("latency", "average"),
    lazy.Count("rows"),
)
```

## Shared state and execution

Each distinct input column has one explicit aggregate layout and one
structure-of-arrays state lane. A single copied-key index assigns stable dense
group IDs shared by all lanes. Rehashing preserves those IDs. It does not build
one hash table per value column or join independently grouped results.

Queries whose value reductions use one int64 column retain their specialized
hash tables and update kernels. Multi-column queries use the new state bundle
inside the existing resident, parallel partition-owner, sorted-adjacent and
radix-spill operators. Predicate kernels still use the session's kernel set.
The new scatter state-update loop is scalar; it makes no unmeasured SIMD
speedup claim. Direct-fused strategy hints currently require the legacy
single-value-column path. Multi-column resident queries use bitmap-direct or
partitioned-hash; sorted/global and bounded spill remain available.

The generic key index starts with at most 64 estimated groups, regardless of
an optimistic cardinality hint. Hints do not force an oversized initial
allocation. The index and state grow as actual groups arrive; this does not cap
cardinality. Keys are owned in one byte arena. Every backing replacement is
reserved before allocation while the old backing remains charged. State growth
precedes new-key insertion, so failed growth cannot produce an addressable key
with missing state. Failures release ownership through existing operator cleanup.
Prepared-state caching is deliberately disabled for the new bundle until its
reset/reuse lifecycle has its own validation.

## Merge and spill

Multi-column spill records use a separate versioned codec, `aggregate-int64-bundle-v2`.
They include exact lane-layout metadata and raw sum/count/min/max state for
all columns. Restores validate shape, layouts, counts, validity flags, lengths
and trailing bytes before records can be merged. Readers borrow state bytes
without allocating per-record arrays. Existing single-column V1 records are
unchanged. The store still validates its outer checksum and schema fingerprint.

## Semantics and current boundary

Each lane preserves the existing int64 SQL semantics, including null-on-empty
SUM and wrapped int64 accumulation. In particular AVG still divides the wrapped
int64 sum: adding an unrelated aggregate on another column does not change its
answer. An overflow-safe mean is a separate semantic change for both paths.
COUNT(*) is stored once, on lane zero. NULLs are counted independently per input
column. Result ordering is not guaranteed for hash aggregation.

The compatibility `AggregateRow.Sum`/`Checksum` summarize the first value column
encountered in logical aggregate order; named entries in `Aggregates` identify
all columns. `AggregateRow.Count` reflects COUNT(*) when requested.

This increment supports existing group key shapes: int32, int64, uint32, plain
or dictionary strings, and supported fixed two-column combinations. Native
non-int64 value types, broader key types/composites, and nonnumeric COUNT(column)
are subsequent increments, not implied by this implementation.
