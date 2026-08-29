# Phase 2.15 — Window foundation

Phase 2.15 adds an explicit analytic-window boundary to the LazyFrame DAG.
It intentionally keeps the first implementation narrow: one `WindowSpec` per
`WithWindows` call, direct partition columns, existing scalar ordering
expressions, and inclusive `ROWS` frames.

## API

```go
spec := lazy.NewWindow(
    lazy.PartitionBy("account"),
    lazy.OrderBy(lazy.Desc("event_time"), lazy.AscExpr(lazy.Length(lazy.Col("name")))),
)

out := frame.WithWindows(spec,
    lazy.RowNumber("row_number"),
    lazy.Rank("rank"),
    lazy.DenseRank("dense_rank"),
    lazy.RollingSum("amount", lazy.RowsBetween(lazy.Preceding(6), lazy.CurrentRow), "seven_rows"),
)
```

`WithWindows` retains input columns and appends (or replaces) named outputs.
The returned result is restored to the input row order; only frame membership
and rank positions use window ordering.

Every spec must include at least one `OrderBy` key. `PartitionBy` may be empty,
which denotes one whole-table partition. Partition keys must be direct columns;
ordering supports `Asc`, `Desc`, `AscExpr`, and `DescExpr` with the same NULL
placement as Lazy Sort.

## Ranking and frame contract

`RowNumber`, `Rank`, and `DenseRank` produce non-NULL `int64` results.
Positions are one-based. Equal order keys retain input order; `rank` leaves
gaps after ties while `dense_rank` does not. NULL equals NULL and NaN equals
NaN for partition membership and ordering ties.

`RowsBetween` uses the exported bounds `UnboundedPreceding`, `Preceding(n)`,
`CurrentRow`, `Following(n)`, and `UnboundedFollowing`. Bounds are inclusive
and clipped to the current partition. A start boundary after the end boundary
is a schema error. An empty frame yields zero for count functions and NULL for
all other aggregates.

`RollingCountRows` and `RollingCount` return `int64`. Signed integer sums
return `int64`, unsigned sums return `uint64`, and floating sums plus all
averages return `float64`. Min/max preserve their numeric input type. NULLs
are ignored; an all-NULL frame is NULL except for counts. Integer sum uses an
exact accumulator and returns an execution error if its output cannot fit the
documented result type. Float operations use IEEE-754: a frame containing NaN
produces NaN for sum/average, and NaN sorts above finite values for min/max.

## Planning and resources

Window is a strict optimizer boundary. A filter, limit, or sort above it cannot
move below it, because doing so could change the rows participating in a rank
or rolling frame. Predicates already inside the child subtree can still be
pushed into the source. Projection dependency analysis preserves partition,
ordering, and aggregate-value inputs, and removes a whole Window only after
every generated output has been pruned.

Physical execution materializes the child once, builds a stable ordering of
`partition keys + order keys + input ordinal`, computes each expression per
partition, and writes the results by ordinal back into one global output
partition. Computed sort keys are internal columns and are dropped before the
user-visible schema. The materialization is reserved through the query memory
manager and checks context cancellation while scanning partitions and frames.

This phase deliberately excludes spill, `RANGE` frames, lead/lag, ntile,
percentile functions, time-zone conversions, and a SQL frontend.
