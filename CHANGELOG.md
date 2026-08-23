# Changelog

## Phase 2.8

- Added Lazy Limit, Sort, and bounded-heap Top-K with `Sort + Limit -> TopK` optimization.
- Added generic opaque Arrow row gather/materialization and explicit retained-array ownership.
- Added aggregate-result source lowering so grouped output can feed ordering/row operators.
- Added bounded EWMA runtime feedback for aggregate selectivity/cardinality and CostModel reuse.
- Added memory-budgeted Sort/Top-K execution and algorithm-selection benchmarks.

## Phase 2.7

- Added Analyze-only operator tracing with Prepare/Execute phase separation.
- Added per-operator calls, input/output rows, decoded output bytes, inclusive time and derived self time.
- Added query-level Prepare/Execute allocation deltas to AnalyzeReport.
- Added parallel radix fixed-key build/probe using the persistent engine scheduler.
- Added reader-local parallel radix workspace and fixed-key scratch reuse.
- Added Lazy Join `RadixBits` / `Workers` physical hints.
- Added conservative large-query automatic parallel-radix selection backed by medium/large benchmarks.
- Added `lazy-demo -analyze`.


## Phase 2.6

- Added typed scalar inference, numeric coercion, explicit Cast, float/string/date/timestamp execution.
- Fused projection expressions into one shared instruction DAG with compile-time CSE.
- Added typed predicate comparisons while retaining the int64 SIMD fast path.
- Promoted additional Arrow RecordBatch primitive/time types into specialized vectors.
- Removed JoinSource probe-side cardinality pre-read; prepared joins now use safe row upper bounds.
- Added radix-partitioned fixed-key join plus reusable zero-allocation probe workspace; kept it opt-in after benchmark regression.
- Added `LazyFrame.Analyze` and `ExplainAnalyze`.

## Phase 2.5

- Added lazy `SelectExprs` / `WithColumns` and a compiled nullable int64 scalar program.
- Added expression dependency pruning and generic physical projection fallback.
- Upgraded Join to a columnar `source.Source` that can feed downstream Filter/Projection/Aggregate.
- Added `CollectFrame` / reusable `PrepareFrame` for row-producing Lazy DAGs.
- Retained terminal `PrepareJoin` as the lower-materialization row-reference API.
- Added Phase 2.5 lifecycle/concurrency/memory/benchmark regressions.

## Phase 2.4

- Added Arrow RecordBatch Lazy Source with typed/opaque Arrow field preservation.
- Added cached UTF-8 dictionary normalization, validity/offset normalization, and batch pruning statistics.
- Added logical Join plus terminal PreparedJoin for fixed/composite/dictionary equality keys.
- Added per-input join projection pushdown, memory-accounted reusable build state, and expanded regressions.

## Phase 2.3

- Added immutable `lazy.LazyFrame` planning API.
- Added logical Scan/Filter/Projection/Aggregate plan nodes and schema inference.
- Added rule optimizer with filter/projection simplification.
- Added whole-plan source-column dependency analysis and projection pushdown.
- Added scan predicate pushdown annotations while retaining residual filters.
- Added generic flat INT32/INT64 Parquet source projection.
- Added SQL-3VL-aware Parquet row-group pruning using min/max/null_count.
- Added `Explain`, `ExplainOptimized`, and `ExplainPhysical` diagnostics.
- Added `LazyFrame.Prepare` lowering into existing PreparedQuery execution.
- Added no-filter aggregate lowering through an all-TRUE predicate program.
- Added correctness fixtures proving physical column pruning and row-group pruning.

See `docs/PHASE2_3_LAZY.md` and `docs/LAZY_OPTIMIZER.md`.

## Phase 2.2

- Adaptive grow/rehash for specialized aggregation tables with budget accounting.
- Nullable single/composite GROUP BY semantics including radix validity masks.
- Dictionary-domain and fixed 128-bit composite join primitives.
- Compiled SoA SUM/COUNT/MIN/MAX/AVG direct/bitmap aggregation.
- COUNT(*)-only execution and all-NULL aggregate semantics.
- Strong package/API GoDoc convention checking.

## Phase 2.1

- PreparedQuery and bounded reusable workspace pool.
- SQL three-valued predicate program.
- int32/int64/uint32/dictionary/plain-string/composite group keys.
- uint64 and 128-bit radix aggregation.
- initial int32 hash join primitive.
- in-memory source and Arrow-Go Parquet adapter.
