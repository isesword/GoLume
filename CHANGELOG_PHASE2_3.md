# Phase 2.3 changelog

## Lazy planning

- Added immutable `lazy.LazyFrame` and `lazy.GroupedFrame` builders.
- Added `Collect`, `Prepare`, `Explain`, `ExplainOptimized`, and `ExplainPhysical`.
- Added logical Scan/Filter/Projection/Aggregate nodes and schema inference.

## Projection pushdown

- Added whole-plan column dependency analysis.
- Added `source.PushdownSource` capability contract.
- Parquet reader now opens only projected flat primitive column chunks.
- Added fixture tests proving unused vectors are not materialized.

## Predicate pushdown

- Added scan-level pushed predicates while retaining residual row filters.
- Added conservative Parquet row-group pruning over min/max/null_count.
- Added SQL 3VL truth-set reasoning for comparison/AND/OR/NOT/NULL predicates.
- Added fixture tests proving `pred > 150` prunes one of three row groups.

## Execution integration

- Optimized lazy plans lower into the existing PreparedQuery execution engine.
- Added all-TRUE predicate program for aggregate queries without Filter.
- Added direct Prepared-vs-Lazy Prepared regression benchmark.

## Diagnostics and quality

- Extended explain output with projection, predicate, selected row groups, rows,
  compressed bytes, and physical strategy.
- Full scalar/SIMD/emulated/forced-width/race/vet/GoDoc validation retained.
