# Phase 2.19 — DataFrame ergonomics

Phase 2.19 replaces the public-field `Frame` result with an owned `DataFrame`
whose schema, batches, row count, reservations, and closed state are private.
`Frame` remains a type alias, but callers use methods rather than mutating
result internals.

```go
df, err := query.CollectFrame(ctx)
if err != nil {
    return err
}
defer df.Close()

rows, columns := df.Shape()
scores, err := df.Column("score")
chunks, err := scores.Float64Chunks()
```

`Head`, `Tail`, and `Slice` create borrowed row ranges without copying a batch.
Series retain the same range and expose read-only typed chunks across every
physical batch. Flattening is explicit through `CopyFloat64`, `CopyInt64`,
`CopyStrings`, or `CopyStringLists`; `ToAnySlice` is intentionally documented
as a low-performance convenience API.

Views and chunks share the owner's lifecycle. Closing the DataFrame releases
retained Arrow arrays and materialization reservations. Operations that can
report an error return `ErrDataFrameClosed` after closure; formatting through
`String` returns `<closed DataFrame>`.

Materialized specialized buffers are charged to the session memory manager
before cloning and remain charged until `DataFrame.Close`. Partial reads,
budget failures, clone failures, and repeated Close calls release exactly the
reservations they own.

Default display is bounded to head/tail samples from 20 rows and left/right
samples from 12 columns, with a 32-cell and 120-table display-width limit.
Grapheme-aware width and truncation preserve CJK, combining sequences, and
emoji. NULL, an empty string, an empty list, and a NULL list remain visibly
distinct. Formatting reads only materialized buffers and never executes a
lazy plan.
