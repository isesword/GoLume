# Global aggregation

A query with no group keys computes one row over the selected input. Both
`expr.Query{Aggregates: ...}` and `LazyFrame.Agg(...)` support this shape.
`LazyFrame.GroupBy().Agg(...)` is equivalent. Existing grouped APIs are unchanged.

```go
frame.Agg(lazy.Sum("amount", "total"), lazy.Count("rows"))
```

The initial global implementation supports the existing int64 reductions:
SUM, COUNT(*), COUNT(column), MIN, MAX, AVG. Value reductions still share one
int64 input column. Typed multi-column reductions are a separate migration.

## Identities and compatibility

- Empty input or a filter selecting no rows produces exactly one row.
- COUNT(*) and COUNT(column) produce zero for empty input.
- SUM, MIN, MAX and AVG produce NULL when no non-NULL values are selected.
- NULL values do not contribute to the value reductions. COUNT(*) includes them.
- Outputs contain no hidden or synthetic group key.
- Integer SUM retains existing wrapping int64 arithmetic. AVG retains the
  current int64-sum implementation, including its overflow limitation.

These are the existing GoLume SQL-style identities, deliberately retained for
compatibility. Polars' empty/all-NULL SUM identity is zero, so callers must not
assume unconditional semantic equivalence with Polars.

## Execution and budgets

No-key aggregation is inherently a single group regardless of input ordering.
It reuses the existing sorted-adjacent operator with one budgeted aggregate
state and one output row. Input remains streamed through the native pipeline.
There is no hash table, synthetic key buffer or spill run. Auto, MemoryOnly and
Bounded policies use this bounded operator; Bounded still requires a positive
query memory limit. Predicate, exchange, output and collected-result allocations
retain their existing query-budget ownership and cancellation semantics.

Diagnostics report `sorted`, one aggregation worker, with plan reason
`global aggregation uses one bounded state`. This describes the implementation,
not a claim that the source is sorted. Explicit non-sorted strategy hints are
rejected, not silently ignored. Parallel global reduction and dedicated SIMD
reduction kernels are not implemented by this increment; existing predicate
kernel selection is unchanged. No speedup or cross-architecture claim is made.

Tests cover empty, filtered-empty, mixed and all-NULL inputs, count-only
columnless input, record and columnar consumers, repeated prepared execution,
forced-strategy rejection, cancellation, budget failure and final-output
lifecycle. Grouped resident/parallel/sorted/spill behavior remains unchanged.
