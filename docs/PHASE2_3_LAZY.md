# Phase 2.3 — Lazy API and Pushdown

Phase 2.3 introduces the first lazy logical-planning layer for Go Columnar Engine.
The design goal is the same architectural separation used by modern analytical
engines: callers describe **what** they want, the optimizer decides **how** it
should be executed, and physical execution begins only at `Collect` or
`Prepare`.

## 1. Lazy execution contract

`LazyFrame` is immutable. Calls such as `Filter`, `Select`, `GroupBy`, and `Agg`
only create logical-plan nodes. They do not open a Parquet row group or decode a
column.

```go
session := lazy.NewSession(engine.DefaultOptions())
defer session.Close()

lf, err := session.ScanParquet("orders.parquet")
if err != nil {
    return err
}

lf = lf.
    Filter(lazy.Col("pred").Gt(lazy.Lit(32767))).
    Select("key", "value").
    GroupBy("key").
    Agg(
        lazy.Sum("value", "sum"),
        lazy.Count("count"),
    )

result, err := lf.Collect(ctx)
```

The execution boundary is:

```text
Lazy API
   |
LogicalPlan
   |
Optimizer
   |
Source pushdown
   |
Physical planner
   |
PreparedQuery
   |
Execution
```

For repeated execution, prefer:

```go
prepared, err := lf.Prepare(ctx, engine.PrepareOptions{MaxConcurrency: 2})
if err != nil {
    return err
}
defer prepared.Close()

for i := 0; i < 100; i++ {
    result, err := prepared.Execute(ctx, engine.ExecuteOptions{})
    if err != nil {
        return err
    }
    _ = result
}
```

Logical optimization and source pushdown occur during `Prepare`; they are not
performed for every batch or row.

## 2. Logical-plan nodes

Phase 2.3 contains four logical nodes:

```text
Scan
Filter
Projection
Aggregate
```

A query such as:

```text
ScanParquet
  .Filter(pred > 32767)
  .Select(key, value)
  .GroupBy(key)
  .Agg(sum(value), count(*))
```

starts as:

```text
AGGREGATE keys=[key]
  PROJECTION [key, value]
    FILTER pred > 32767
      SCAN columns=ALL
```

The logical layer is intentionally independent from hash-table, SIMD, radix,
and Parquet implementation details.

## 3. Projection pushdown

Projection pushdown performs whole-plan column dependency analysis.

For:

```text
Filter uses:       pred
Projection emits:  key, value
GroupBy uses:      key
Aggregate uses:    value
```

only these source columns are required:

```text
pred, key, value
```

An unrelated source column such as `unused` is not decoded at all.

The optimizer does not simply copy the final `Select` list into the scan.
Columns referenced by residual predicates, grouping keys, and aggregates are
also retained until the operator that consumes them.

The Parquet source receives the resulting projection through
`source.PushdownRequest`. Its reader then opens only the selected physical
column chunks.

## 4. Predicate pushdown

Predicate pushdown has three separate responsibilities.

### 4.1 Logical pushdown

The optimizer records predicates on the `Scan` node whenever they can be moved
through the logical plan without changing semantics. Filters remain present as
residual row-level predicates.

Keeping the residual predicate is deliberate: row-group statistics can prove
that a partition has **no possible TRUE row**, but they usually cannot determine
which individual rows satisfy the predicate.

### 4.2 Parquet row-group pruning

`source/parquet` evaluates safe predicates against Parquet column statistics:

```text
min
max
null_count
```

Phase 2.3 supports row-group pruning for int64 predicates containing:

```text
>
>=
<
<=
=
AND
OR
NOT
IS NULL
IS NOT NULL
```

The evaluator is conservative and models SQL three-valued truth possibilities.
A row group is removed only if metadata proves the predicate cannot evaluate to
SQL TRUE for any row.

Example:

```text
predicate: pred > 150

row group 0: min=0   max=99   -> prune
row group 1: min=100 max=199  -> keep
row group 2: min=200 max=299  -> keep
```

### 4.3 Residual execution

Selected row groups still execute the normal expression program:

```text
Parquet row-group pruning
        |
        v
Decode selected columns
        |
        v
Residual Filter
        |
        v
Selection / Aggregate
```

This split prevents metadata pruning from changing query results.

## 5. Explain surfaces

Phase 2.3 exposes three levels of diagnostic output.

### `Explain`

Shows the original logical plan.

### `ExplainOptimized`

Shows optimizer rules and logical scan annotations:

```text
SCAN columns=[pred, key, value]
     pushed_predicate=(pred > 32767)
```

### `ExplainPhysical`

Also applies source metadata and physical planning. Parquet diagnostics include:

```text
row_groups=2/3
pruned=1
rows=200/300
compressed_bytes=.../...
```

and execution diagnostics include:

```text
strategy=radix
workers=4
radix_bits=8
key_width=4
predicate_ops=1
```

This API is intended to become the basis of a future `ExplainAnalyze` operator
with actual per-operator runtime statistics.

## 6. Schema inference

Logical plans are validated before physical execution. `logical.InferSchema`
currently validates:

- source field existence;
- predicate column references;
- projection columns;
- group keys;
- aggregate value columns;
- output aggregate types and nullability.

The schema system is deliberately placed above the physical engine so a future
SQL frontend can compile into the same logical IR.

## 7. Current boundary

Phase 2.3 deliberately does **not** claim a complete Polars-compatible lazy
engine.

Current lazy execution supports:

- flat primitive Parquet `INT32` and `INT64` scan columns;
- column-only `Select`;
- predicates supported by the existing expression IR;
- one/two-key grouped aggregation through the existing physical engine;
- projection pushdown;
- Parquet row-group predicate pruning;
- `Collect`, `Prepare`, and explain APIs.

Not yet implemented in the lazy DAG:

- expression projections / `WithColumns`;
- lazy join nodes;
- sort/window/limit nodes;
- page-index, Bloom-filter, or dictionary-level pruning;
- generic Arrow `RecordBatch` scan for every Arrow type;
- projection-only materialization without aggregate;
- cost-based join reorder;
- runtime `ExplainAnalyze` feedback.

These are extensions of the logical/physical boundary introduced here rather
than reasons to change the boundary itself.
