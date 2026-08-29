# Phase 2.8 — Ordering, Opaque Arrow Take, and Runtime Feedback

Phase 2.8 expands the Lazy physical DAG with global ordering operators and
closes two architectural gaps exposed by earlier phases:

1. row-selection operators must be able to preserve Arrow columns that do not
   yet have a specialized engine vector;
2. the aggregate cost model must be able to learn from successful executions
   instead of relying forever on static cardinality/selectivity defaults.

The implementation keeps the existing rule that abstractions live at plan,
source, batch, and query boundaries. Sort and Top-K compare typed column values;
they do not route every row through `interface{}` values or row maps.

## 1. Lazy API

New immutable plan transformations:

```go
lf := session.ScanParquet("events.parquet")

out := lf.
    Filter(lazy.Col("score").Gt(lazy.Lit(0))).
    Sort(lazy.Desc("score")).
    Limit(100)
```

The optimizer rewrites the final two nodes to:

```text
TOPK k=100 [score desc nulls-last]
```

Explicit Top-K is also available:

```go
lf.TopK(100,
    lazy.Desc("score"),
    lazy.AscNullsFirst("event_time"),
)
```

Ordering keys are intentionally strongly typed at the Go API boundary through
`lazy.SortExpr`; the API does not accept `...any`.

## 2. Logical nodes and optimizer rules

Phase 2.8 adds:

```text
Sort
TopK
Limit
```

Current rules include:

```text
Limit(Limit(input))        -> Limit(min(n1,n2), input)
Limit(TopK(k,input))       -> TopK(min(n,k), input)
Limit(Sort(keys,input))    -> TopK(n,keys,input)
```

Projection dependency analysis includes ordering columns even when they are not
present in the final projection.

Predicate movement is conservative:

- Filter may move through Sort because sorting does not change row membership;
- Filter does not move across Limit or Top-K because doing so can change which
  rows survive the cardinality boundary.

## 3. Global Sort

`physical.SortSource` is a full-materialization operator:

```text
all input partitions
    -> append columnar batches
    -> stable row-index sort
    -> columnar gather
    -> one ordered output partition
```

The implementation appends whole specialized vectors and whole Arrow arrays;
it does not materialize the input one row at a time.

Sort reserves its estimated materialization memory through `memory.Manager`
before reading the full input. A rejected reservation returns
`memory.ErrBudgetExceeded` before unbounded allocation begins.

Sort is currently in-memory. External merge sort and spill runs remain Phase 3
work.

## 4. Top-K

`physical.TopKSource` is not implemented as `Sort + Limit`.

First pass:

```text
input batches
   -> compare ordering keys
   -> bounded worst-first heap
   -> keep K (partition,row,stable ordinal,key snapshot)
```

Second pass:

```text
winner partitions only
   -> gather selected rows
   -> replay winners in requested stable order
   -> materialize K payload rows
```

Complexity:

```text
comparison:      O(N log K)
candidate state: O(K)
payload output:  O(K)
```

The source re-reads partitions containing winners. For a row-preserving lazy
pipeline consisting of Scan, Filter, Projection, and/or ExprProjection, the
first pass is now lowered as a separate key-only child source. Its source
projection contains the ordering keys plus any residual-filter dependencies,
so non-winning partitions do not decode payload columns. The second pass
replays the complete payload pipeline and gathers only winner rows.

Join and aggregate-result inputs deliberately retain the single-input Top-K
path: re-lowering them solely for a narrow key pass would repeat retained join
build or aggregate work and can cost more than the payload I/O it saves.

Top-K candidate snapshots were explicitly optimized so non-winning rows do not
allocate. A candidate is copied only when it first enters the heap or replaces
the current worst entry; retained cell/string storage is reused on replacement.

Top-K also reserves bounded candidate/output state through `memory.Manager` in
engine-managed execution. `NewTopKSource` remains as a compatibility constructor
without explicit accounting; Lazy lowering uses `NewTopKSourceWithManager`.

## 5. Reference Sort vs Top-K benchmark

Validation fixture:

```text
rows     = 262,144
columns  = id:int64, score:int64, payload:int64
query    = ORDER BY score DESC
Top-K    = 100 rows
GOMAXPROCS=1
3 benchmark iterations x 3 samples
```

Median on the shared AMD EPYC validation host:

```text
Global Sort:
  671.099 ms/op
  40,085,936 B/op
  147 allocs/op

Top-K 100:
  14.129 ms/op
  49,288 B/op
  306 allocs/op

latency ratio ~= 47.5x
```

This benchmark is a regression/algorithm-selection result for this host, not a
cross-machine performance claim. It demonstrates why `Sort().Limit(K)` should
lower to bounded Top-K when semantics permit.

## 6. Opaque Arrow row selection

Earlier phases preserved unsupported Arrow types as `memory.OpaqueVector`, but
row compaction could not safely gather them.

Phase 2.8 adds generic Arrow row selection without converting nested values to
Go row objects:

```text
selected row ids
   -> coalesce contiguous runs
   -> array.NewSlice for each run
   -> array.Concatenate when multiple runs exist
   -> owned Arrow array in output Batch
```

This supports payload preservation for types such as:

- list/nested arrays;
- decimal arrays;
- extension arrays;
- other Arrow-native arrays without a specialized engine kernel.

`memory.OpaqueVector.Owned` records whether the Batch owns an Arrow reference.
`Batch.ReleaseOwned` and `Frame.Close` release those references deterministically.
`CloneBatch` retains Arrow values when a returned Frame must outlive its source.

This closes the important distinction between:

```text
"the engine cannot compute arithmetic on this type yet"
```

and:

```text
"the engine cannot preserve/filter/take this type"
```

The latter is now supported.

## 7. Aggregate output can continue through the row DAG

A common Lazy pattern is:

```text
GroupBy
 -> Agg
 -> Sort
 -> Limit
```

The existing terminal aggregate fast path remains unchanged. When downstream row
operators exist, Phase 2.8 wraps the reusable `engine.PreparedQuery` as an
internal one-partition columnar `source.Source` and materializes aggregate rows
into typed vectors.

This lets the same physical operators consume grouped output without creating a
second query engine.

## 8. Runtime feedback

Static defaults are still needed before a plan has ever executed:

```text
selectivity = 0.5
cardinality = 65,536
```

After a successful aggregate execution, the engine can observe:

```text
source rows
selected rows
output groups
```

and derive actual selectivity/cardinality.

Feedback is keyed by a stable Lazy plan signature plus source identity within the
current process. It is used only when the user did not provide an explicit hint.

### EWMA

The first observation is stored exactly. Later observations use:

```text
new = old * 0.75 + observation * 0.25
```

for selectivity and cardinality (cardinality is rounded back to an integer).

This prevents one abnormal execution from instantly replacing the planner's
entire estimate.

### Bounded cache

The feedback store is deliberately bounded:

```text
max plans = 1024
policy    = FIFO eviction for new plan signatures
```

`ResetRuntimeFeedback` clears both observations and eviction order.

### Hint precedence

Explicit query hints always win:

```go
lf.WithHints(expr.Hints{
    Selectivity: 0.25,
    Cardinality: 50000,
})
```

Runtime feedback is still reported diagnostically, but it does not overwrite
those estimates.

## 9. Feedback changes physical strategy

A regression fixture deliberately starts with poor defaults:

```text
rows        = 300,000
partitions  = 4
actual groups = 2
```

Before execution, defaults satisfy the high-cardinality radix rule, so the first
plan chooses radix. After one successful execution reports cardinality 2, the
next `Prepare` consumes the feedback and chooses a non-radix strategy.

This validates the full loop:

```text
Prepare with estimates
 -> Execute
 -> observe actual rows/groups
 -> EWMA feedback
 -> next Prepare
 -> CostModel chooses again
```

Feedback currently refines the aggregate `planner.CostModel`. Join's large-table
parallel-radix threshold is still benchmark-gated using build/probe sizes and is
not yet driven by this feedback store.

## 10. Memory and ownership boundaries

Current ordering behavior:

```text
Limit:
  streaming/order-preserving, O(output) materialization only

Top-K:
  O(K) retained candidate/output state, budget-accounted in Lazy execution

Sort:
  O(N) in-memory materialization, budget-accounted
```

Opaque Arrow output is reference-counted explicitly and must be released through
reader/Batch/Frame lifecycle methods.

Phase 3 will add external sort/spill; Phase 2.8 intentionally returns a memory
budget error rather than silently spilling or silently exceeding the budget.

## 11. Current limitations

Not claimed in Phase 2.8:

- external/spill sort;
- distributed/global merge ordering;
- arbitrary expression sort keys (keys are named columns; compute one with
  `WithColumns` first);
- Top-K key-only first-pass source projection;
- window functions;
- collation-aware string ordering;
- runtime-feedback-driven Join threshold adaptation;
- generic arithmetic kernels for all opaque Arrow types.

These limitations are explicit extension points rather than hidden fallbacks.
