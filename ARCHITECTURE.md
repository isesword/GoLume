# Architecture

## 1. Layering

Phase 2.8 keeps abstractions at plan and batch boundaries, not inside row loops.
Analyze instrumentation is created only for diagnostic execution and is absent from ordinary hot paths. Ordering operators and runtime feedback extend the same Source/Prepared boundaries rather than adding a row-iterator engine.

```text
                        LazyFrame API
                              |
                              v
                         LogicalPlan
      +-----------------------+------------------------+
      |                       |                        |
 Schema inference       Rule optimizer          Expr dependency
      |                       |                        |
      +-----------+-----------+------------------------+
                  |
        source projection / predicate pushdown
                  |
        +---------+----------+
        |                    |
      Parquet           Arrow RecordBatch
        |                    |
 row-group prune         batch prune
        +---------+----------+
                  |
           Physical Source DAG
   +--------------+---------------+
   |              |               |
FilterSource  ProjectSource    JoinSource
   |              |               |
   +--------------+---------------+
                  |
          +-------+--------+
          |                |
     PreparedDataFrame    PreparedQuery
                           |
                direct / bitmap / partitioned-hash / sorted / spill-hash
                           |
                  scalar / SIMD kernels
```

## 2. Logical control plane

Current logical nodes:

```text
Scan
Filter
Projection
ExprProjection
Join
Aggregate
Sort
TopK
Limit
```

`LazyFrame` is immutable. Transformations do not read data. Planning and source
pushdown happen in `Prepare`, `PrepareResult`, or one-shot Collect helpers.

## 3. Expression system

Boolean predicates use the SQL-three-valued `expr.Program`.
Scalar projections use `expr.ScalarProgram`.

The typed scalar IR supports signed/unsigned numerics, float, UTF-8 string views, date32 and timestamp semantics, explicit Cast, arithmetic and string concat. Projection expressions compile into one shared instruction DAG with compile-time common-subexpression elimination.

Programs are compiled once; `ScalarWorkspace` stores reusable typed vector registers and validity bitmaps per execution stream. This deliberately avoids AST interface dispatch for each row.

## 4. Optimizer

The rule optimizer currently performs:

- filter merge;
- column projection merge/removal;
- projection dependency analysis;
- expression dependency pruning;
- source predicate annotations;
- join input dependency split.

Computed aliases form a conservative predicate-pushdown boundary until alias
substitution analysis is added.

Projection pruning has two levels:

1. native `PushdownSource` implementations avoid source I/O/binding;
2. generic sources receive a passthrough physical ProjectSource fallback.

## 5. Source contract

`source.Source` is the common physical boundary:

```go
Schema()
Stats(ctx)
Partitions(ctx)
NewBatchStream(ctx, ReadOptions)
```

Parquet, Arrow RecordBatch, in-memory data, FilterSource, ProjectSource and
JoinSource all implement this contract. This is what makes aggregate-over-join
possible without a second execution engine.

## 6. Parquet and Arrow

Parquet pushdown:

```text
projection -> requested column chunks only
predicate  -> conservative row-group min/max/null_count pruning
```

Arrow RecordBatch pushdown:

- typed/opaque schema preservation;
- typed value views for common primitives;
- string/binary views;
- UTF-8 dictionary-domain normalization;
- cached int64 batch stats for repeatable whole-batch pruning.

Residual row filters always remain when source metadata only proves possibility.

## 7. Physical Filter/Projection

`FilterSource` compacts selected specialized vectors into reusable reader-local
buffers. `ProjectSource` passes existing vectors by view and materializes only
computed scalar columns.

Both are sources themselves, so plans can compose:

```text
Scan -> FilterSource -> ProjectSource -> JoinSource -> ProjectSource
```

without channel-per-operator scheduling.

## 8. Columnar Join

`JoinSource` retains an immutable right-side build table and required payload.
Each independent reader probes left partitions and writes joined values into its
own reusable output batch.

Key shapes:

```text
fixed single key       -> PairTable encoded Key128
fixed two-column key   -> Key128 + validity mask
dictionary UTF-8       -> DictionaryTable + canonical dictionary codes
```

JoinSource no longer pre-reads probe rows during prepare. The immutable build table tracks maximum duplicate-key multiplicity; left partition metadata and that multiplicity produce safe row upper bounds. Probe data is read exactly when its partition executes.

For fixed-width large joins, Phase 2.7 can radix-partition build/probe keys and assign independent buckets to the engine's persistent worker pool. The automatic policy is deliberately narrow: at least four workers, build >= 1,048,576 rows and probe >= 4,194,304 rows. Smaller workloads remain on the single hash table because pinned four-worker benchmarks showed the medium case to be effectively tied.

`PrepareJoin` and row-reference join results have been removed. All joins use
the same physical stream through `Join(...).Collect()` or `Join(...).Prepare()`.
See [STREAM_MIGRATION.md](STREAM_MIGRATION.md) for breaking API changes.

## 9. Prepared execution

Aggregate terminal:

```text
LazyFrame.PrepareResult
 -> PreparedResult / PreparedQuery
 -> query-owned native aggregate stream
 -> terminal record collector
```

Row terminal:

```text
LazyFrame.Prepare
 -> prepared physical source DAG
 -> reusable Join build / scalar programs
 -> independent native stream per Execute
```

`PreparedDataFrame.Close` waits for active executions before closing retained
physical resources.

## 10. Memory ownership

Hot analytical storage uses primitive contiguous buffers and validity bitmaps.
Strings use offsets + bytes or dictionary codes, not `[]string`.

Join build payload/hash memory is reserved through `memory.Manager`.
Prepared aggregate plans retain compilation and concurrency slots, not input-sized
workspaces. Each execution reserves scan, aggregate-state and output memory before
allocation and releases its owned resources on completion or Close.

`Collect` returns owned copies of specialized vectors. Opaque Arrow values
are intentionally not deep-cloned without an explicit Arrow ownership/gather
operator.

## 11. Aggregate execution

All aggregate terminals use the same native execution layer:

```text
source batch
 -> predicate / selection
 -> native direct / bitmap / partitioned-hash / sorted / spill-hash strategy
 -> specialized hash table
 -> SoA aggregate state
```

Supported aggregate states include SUM, COUNT(*), COUNT(value), MIN, MAX, AVG.
High-cardinality aggregation uses the same native aggregate stream and query-budgeted partition spill when configured. The former input-wide radix execution backend is removed.

## 12. Analyze diagnostics

`LazyFrame.Analyze` / `ExplainAnalyze` perform a diagnostic-only physical lowering. `physical.ProfileSource` wrappers record source/operator calls, input/output rows, decoded output-byte estimates and inclusive time into `observer.OperatorTrace`. Prepare and Execute phases are distinct, which makes Join build reuse visible.

Aggregate terminal execution registers a fused aggregate root so predicate+aggregate work remains visible even when no standalone FilterSource exists. Derived self time subtracts direct-child inclusive time and is diagnostic rather than CPU-cycle accounting when child operators overlap.

Go heap counters are process-wide, so allocation is reported only as whole Prepare/Execute `TotalAlloc` deltas. Ordinary `Collect` and `Prepare` do not install profile wrappers.


## 13. Ordering and row selection

Phase 2.8 adds three composable Source operators:

```text
LimitSource  -> order-preserving global truncation
SortSource   -> full materialization + stable row-index sort
TopKSource   -> bounded heap + winner-partition payload gather
```

Sort and engine-managed Top-K reserve retained state through `memory.Manager`.
Top-K candidate snapshots allocate only when an entry is admitted to the heap;
non-winning input rows compare directly against the current worst candidate.

Opaque Arrow payloads are gathered by coalescing selected row IDs into Arrow
slices and concatenating those slices. Returned owned Arrow arrays are released
through Batch/Frame lifecycle methods.

Aggregate output can be exposed as an internal one-partition Source when a Lazy
DAG continues with Sort/Top-K/Limit after GroupBy/Agg. The ordinary terminal
PreparedQuery aggregate path remains specialized.

## 14. Runtime planning feedback

Successful aggregate executions publish observed row count, selectivity and
cardinality into an engine-local bounded feedback store. Values use an EWMA
(alpha=0.25) and at most 1024 plan signatures are retained.

The next Prepare uses feedback only when the corresponding explicit Hint is
absent. This means runtime learning can refine default CostModel inputs without
overriding user intent. Feedback currently targets aggregate strategy selection;
Join's large-table parallel-radix policy remains independently benchmark-gated.

## 15. Design principle

Prefer end-to-end work avoidance and stable physical ownership over decorative
abstraction. Lazy API must not turn a specialized columnar engine into a row
iterator. Interfaces belong at query/source/operator boundaries; tight loops
remain typed and batch-oriented.
