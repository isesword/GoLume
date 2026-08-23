# Phase 2.5 — Lazy Expressions and Columnar Join Pipeline

## 1. Goal

Phase 2.5 closes the gap between a lazy control plane and a composable physical
pipeline. Before this phase, scalar projection expressions were absent and Lazy
Join was terminal. The new execution shape is:

```text
Scan
 -> Filter
 -> WithColumns / SelectExprs
 -> Join
 -> Filter / Projection
 -> GroupBy / Aggregate
```

without introducing row-level reflection or a second execution engine.

## 2. Logical expression projection

`logical.ExprProjection` represents both operations:

```text
SelectExprs:  IncludeInput=false
WithColumns:  IncludeInput=true
```

Each output is an `expr.NamedExpr`.

Phase 2.5 exposes a Go-friendly API:

```go
lf.WithColumns(
    lazy.Col("a").Mul(lazy.Lit(2)).Add(lazy.Col("b")).Alias("score"),
)
```

rather than accepting heterogeneous `...any` arguments.

### Same-call semantics

Expressions in one `WithColumns` are compiled against the input schema before
that node. They do not depend on aliases created by sibling expressions. This is
intentional and deterministic. Chained `WithColumns` nodes can reference an
alias created by a previous node.

## 3. Scalar physical program

`expr.CompileScalar` lowers a logical arithmetic tree once into a fixed program.
The current instruction set handles:

- int64 column load;
- int64 literal;
- add;
- subtract;
- multiply.

Evaluation uses `expr.ScalarWorkspace`, whose registers and validity bitmaps are
reader-local and reused between partitions.

NULL semantics are SQL-style propagation:

```text
NULL + x = NULL
NULL * x = NULL
```

Arithmetic currently uses native Go int64 wrap semantics. Checked arithmetic,
type coercion, casts, decimal/date/string functions are future instruction-set
extensions rather than implicit conversions in Phase 2.5.

## 4. Expression-aware projection pushdown

The optimizer traces dependencies through `ExprProjection`.

Example:

```text
score = a + b
unused_score = c * 2
final output = score
```

Scan projection becomes:

```text
[a, b]
```

and the unused computed expression is removed by the `expression-pruning` rule.

A predicate above a computed expression boundary is intentionally residual:

```text
Filter(score > 10)
  WithColumns(score = a + b)
```

is not rewritten to a source predicate until alias substitution / expression
normalization is implemented. This avoids invalid predicate pushdown.

## 5. Generic physical projection fallback

Logical projection pushdown is valuable even when a source does not implement
`source.PushdownSource`. `lowerSourcePipeline` therefore checks the optimized
Scan projection and adds a passthrough `physical.ProjectSource` when necessary.

This guarantees that downstream Join/Projection/Aggregate sees only required
columns even for in-memory or custom sources.

Native Parquet/Arrow sources still do the stronger optimization and avoid
binding/opening the unneeded source columns in the first place.

## 6. Physical FilterSource and ProjectSource

`physical.FilterSource` and `physical.ProjectSource` implement `source.Source`.
This keeps pipeline composition at batch boundaries:

```text
Source.NewReader
 -> ReadPartition
 -> batch filter/projection
 -> downstream ReadPartition
```

No interface dispatch is added per row.

`FilterSource` compacts specialized vectors into reader-local buffers. Opaque
Arrow vectors can pass through an all-row selection, but arbitrary partial Take
for opaque arrays is deliberately not guessed; such columns must be projected
away or gain a specialized Arrow Take implementation.

`ProjectSource` passes existing vectors by view and only materializes computed
int64 vectors.

## 7. Columnar JoinSource

`physical.JoinSource` upgrades Join from a terminal row-reference operation to a
morsel-producing source.

Preparation performs:

```text
right partitions
 -> retain required build payload
 -> build specialized equality table

left keys
 -> count exact output cardinality per left partition
```

Execution performs:

```text
left partition
 -> probe immutable build table
 -> materialize joined columns into reader-local reusable output buffers
 -> downstream operator
```

The current exact cardinality prepass is a Phase 2.5 simplification. It makes
`source.Partition.Rows` exact, but scans left join keys once during prepare. A
future streaming join will remove this requirement and emit bounded batches
without a cardinality prepass.

### Supported join keys

- one fixed int32/int64/uint32/uint64 key;
- two fixed keys encoded as `hashagg.Key128`;
- one UTF-8 dictionary string key with dictionary-domain normalization.

### Join types

- inner;
- left;
- semi;
- anti.

Duplicate build keys preserve one-to-many semantics. SQL NULL equality semantics
remain strict: a NULL key never matches another NULL key.

## 8. Joined payload materialization

The joined source currently materializes specialized payloads:

- int32/int64/uint32/uint64;
- float64;
- string/binary view;
- dictionary string.

The output batch is reader-local and its backing buffers are reused across
partitions. Right-side build payload and hash-table memory are charged to the
engine `MemoryManager` and released when the prepared physical source closes.

Optimizer dependency analysis is performed before JoinSource construction, so
irrelevant payload columns should be removed before joined payload
materialization.

## 9. PreparedFrame

Aggregate plans retain `PreparedQuery`. Row-producing plans use `PreparedFrame`:

```go
p, err := lf.PrepareFrame(ctx)
frame1, err := p.Execute(ctx)
frame2, err := p.Execute(ctx)
```

Join build state and compiled physical source wrappers are retained once.
Independent executions create independent readers, so concurrent Execute calls
are allowed when underlying sources support concurrent readers.

`Close` waits for active executions and then releases JoinSource reservations.
Executing after close returns `lazy.ErrPreparedFrameClosed`.

`Frame` owns deep-copied specialized batches. Generic opaque Arrow arrays do not
have a universal ownership/Take contract in `memory`, so opaque terminal
materialization is rejected rather than unsafe.

## 10. Aggregate-over-join

The advanced lowering path composes source wrappers below the existing
specialized aggregate engine:

```text
Logical Aggregate
     |
Join / ExprProjection / Filter subtree
     |
lowerSourcePipeline
     |
source.Source
     |
Engine.Prepare
     |
PreparedQuery
```

Therefore existing adaptive hash/radix/SIMD/worker scheduling remains the only
aggregate execution engine.

## 11. Low-materialization terminal Join

`PrepareJoin` / `CollectJoin` remain available. They return logical `RowRef`
pairs and do not materialize payload columns. This is useful for callers that
need match relationships rather than a downstream columnar DAG.

The general Lazy path uses `PrepareFrame` or aggregate `Prepare`, which lowers
Join to `physical.JoinSource`.

## 12. Current limits

Phase 2.5 does not claim:

- arbitrary Arrow Take/materialization for opaque arrays;
- scalar arithmetic beyond int64 `+/-/*`;
- casts/type coercion/decimal/date/string functions;
- expression fusion across multiple projection/filter nodes;
- common-subexpression elimination;
- radix-partitioned large-build JoinSource;
- spillable joins;
- streaming join output without cardinality prepass;
- Sort/Limit/Window;
- ExplainAnalyze.

These are extensions of the current plan/source/operator contracts rather than
reasons to redesign them.
