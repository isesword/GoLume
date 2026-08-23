# Aggregate-state design

## 1. Design goal

Aggregate execution is split into a semantic control plane and a specialized
physical hot path. The engine does **not** store one Go interface object per
group and does not perform interface dispatch for every input row.

```text
expr.Aggregate descriptors
        |
        v
aggregate.Layout               compile time
        |
        v
aggregate.Int64Vector          SoA physical state
        |
        +-- sums[]
        +-- countRows[]
        +-- countValues[]
        +-- mins[] + valid[]
        +-- maxs[] + valid[]
        `-- AVG reuses sum + valid count
```

The standalone `aggregate.State` interface remains useful for semantic tests,
worker-local partial states, future spill merge and non-hot control paths.

## 2. Built-in SQL semantics

Phase 2.2 supports these built-ins for an int64 aggregate input:

- `SUM(value)`
- `COUNT(*)`
- `COUNT(value)`
- `MIN(value)`
- `MAX(value)`
- `AVG(value)`

`SUM/MIN/MAX/AVG` ignore NULL input and return SQL NULL when a group contains no
non-NULL input. `COUNT(*)` counts every selected row and `COUNT(value)` counts
only non-NULL values.

`COUNT(*)` is also valid as the **only** aggregate; no synthetic value column is
required.

## 3. Multiple aggregate list

Direct and bitmap-direct hash aggregation compile an arbitrary combination of
the built-ins above, with one current restriction: all value-consuming
aggregates in one physical operator share the same int64 input column.

Example:

```go
Aggregates: []expr.Aggregate{
    expr.Sum("value", "sum"),
    expr.Count("rows"),
    expr.CountColumn("value", "values"),
    expr.Min("value", "min"),
    expr.Max("value", "max"),
    expr.Avg("value", "avg"),
}
```

Materialized rows expose the compiled list through `engine.AggregateRow.Aggregates`.
The legacy `Sum` and `Count` fields remain for source compatibility with the
Phase-1 SUM+COUNT query shape.

## 4. NULL value state

The physical vector carries validity where required. An all-NULL group therefore
has states such as:

```text
SUM        invalid
COUNT(*)   N
COUNT(val) 0
MIN        invalid
MAX        invalid
AVG        invalid
```

Tests cover all-NULL groups and table grow/rehash to ensure validity bits and
state arrays move together.

## 5. Adaptive hash-table grow

`aggregate.Int64Vector.Copy` migrates the complete enabled aggregate layout when
a specialized hash table grows. A cardinality hint is therefore an initial
capacity hint only; rehash does not lose SUM/count/min/max/avg state.

## 6. Radix specialization boundary

The current radix kernel intentionally stays specialized to:

```text
SUM(non-null int64 value) + COUNT(*)
```

This is the path measured throughout Phase 0/1. Advanced aggregate layouts and
nullable value inputs are routed to direct/bitmap tables instead of silently
using an incompatible radix state layout.

The intended next step is generated typed radix state layouts, not adding
interface dispatch to the current partition-local loop.

## 7. Future extensions

The layout/state boundary leaves room for:

- multiple value columns in one aggregate operator;
- generated radix layouts for MIN/MAX/AVG and nullable input;
- partial/final aggregate stages;
- spill serialization and merge;
- approximate aggregates such as HLL/Top-K.
