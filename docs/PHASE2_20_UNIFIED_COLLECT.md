# Phase 2.20 — unified DataFrame collection

`LazyFrame.Collect` and `LazyFrame.Prepare` now use DataFrame as the primary
result contract for both ordinary rows and terminal aggregate plans:

```go
df, err := source.
    GroupBy("category").
    Agg(lazy.Sum("amount", "total"), lazy.Count("count")).
    Sort(lazy.Desc("total")).
    Collect(ctx)
```

The existing aggregate engine remains unchanged. Terminal aggregates are
exposed through the same `aggregateResultSource` already used when Sort,
Filter, Projection, Top-K, or Limit follows an Aggregate. The resulting source
is cloned into an owned, budgeted DataFrame like any other row pipeline.

Applications that need aggregate execution diagnostics or the specialized
`engine.Result` use `CollectResult` and `PrepareResult`. `CollectFrame` and
`PrepareFrame` remain deprecated forwarding spellings during the compatibility
window. This is an intentional compile-time break for the old `Collect` and
`Prepare` signatures; no global aggregate without a GroupBy is introduced.
