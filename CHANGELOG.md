# Changelog

## Phase 2.17

- Added an Arrow-shaped `TypeList` schema contract with one specialized
  `item:string` child field and a canonical `StringListVector` representation.
- Preserved parent NULL, valid empty list, NULL child, and valid empty string as
  distinct states through clone, filter, sort, Top-K/Limit Take, projection,
  window materialization, and joins.
- Specialized Arrow `List<String>` ingestion while retaining other nested
  Arrow types as opaque values; InMemory sources now reject malformed offsets
  and schema-nullability violations before execution.
- Added nested byte accounting and scalar-register passthrough so Lazy
  pipelines can own and return list<string> columns safely.
- Kept offset validation and variable-length gather scalar: both are
  dependency/indirection bound and not Auto-SIMD candidates. Existing
  fixed-width companion columns continue through the approved SIMD compact
  kernels; single-byte Split scanning remains a benchmark-gated Phase 2.18
  SIMD candidate.

## Phase 2.16

- Added literal-pattern `RegexMatch` predicates and capture-aware
  `RegexExtract` projections to the expression and Lazy APIs.
- Compile regular expressions into reusable RE2 programs, cap patterns at 64
  KiB, and keep pattern/group metadata out of row-materialized registers.
- Preserve SQL three-valued predicate semantics; extraction returns NULL for
  no match or an unmatched optional capture group.
- Added plain/dictionary string, Unicode, empty-match, invalid-pattern,
  capture-bound, long-input, CSE, and Lazy end-to-end regressions.

## SIMD dispatch and API reset (breaking)

- Replaced generic hot-loop comparison/arithmetic dispatch with generated,
  operation-specialized scalar and SIMD functions; the committed generator has
  a drift-check mode used by structural validation.
- Deleted failed SIMD production paths: 64-bit comparison masks, checked
  integer lane rescans, arm64 widening and AVX2 simulated 64-bit multiply,
  portable bitmap Bool, intermediate Hash128 inputs, and UTF-8 double-pass and
  ASCII-prefix candidates. `ModeSIMD` now falls back honestly to scalar when no
  retained SIMD backend exists.
- Approved operation-specific darwin/arm64 Auto entries for Int32/Uint32
  comparisons and Float64 Sub/Mul/Div. Added a non-SIMD `arm64-unrolled` Hash64
  backend whose capability explicitly reports `HardwareSIMD=false`.
- Added measured Int32 GT planner approvals for the 10/50/90/100% BitmapDirect
  selection buckets; the 1% bucket remains DirectFused.
- Removed the per-64-row DirectFused bitmap allocation and added 1-second x 10
  kernel, selectivity, expression, Hash, UTF-8, Parquet, Lazy, Radix, and Join
  gate evidence under `validation/`.
- Replaced public function-field kernel sets and family-wide capability flags with an immutable, comparable `kernel.Set` handle and exact `KernelID` capability lookup.
- Made `source.ReadOptions.Kernels` mandatory and propagated the same engine-owned handle through Parquet, physical operators, Lazy frames, terminal joins, and radix workspaces.
- Removed all repository compatibility APIs: `Result.Groups`/`engine.Group`, `expr.AggCount`, aggregate `SumCountVector` helpers, sort `Column` alternatives, Parquet `Open`/`Columns`, `ErrStringTableFull`, and the Top-K constructor without a memory manager.
- Removed allocating SIMD tail adapters, centralized canonical SplitMix64 semantics, retained stable dictionary predicate results in `EvalWorkspace`, and added structural/allocation regression tests.
- amd64 Auto remains scalar until native linux/amd64 evidence is available.

See `docs/SIMD_API_MIGRATION.md` and `docs/SIMD_KERNELS.md`.

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
