# Phase 2.5 changelog

## Added

- `LazyFrame.SelectExprs` and `LazyFrame.WithColumns`.
- Named scalar expressions with `+`, `-`, `*` and aliases.
- Compiled nullable int64 `expr.ScalarProgram` and reusable `ScalarWorkspace`.
- Logical `ExprProjection` node and schema inference.
- Expression-aware projection dependency analysis and unused-expression pruning.
- `physical.ProjectSource` and `physical.FilterSource` batch operators.
- Generic source projection fallback at physical Scan lowering.
- `physical.JoinSource` for columnar joined payload output.
- Joined payload support for fixed numeric, float64, string/binary view and dictionary string columns.
- JoinSource build-side memory accounting and reusable reader-local output buffers.
- Aggregate-over-join physical lowering.
- `Frame`, `CollectFrame`, `PreparedFrame`, concurrent repeated row-pipeline execution and close lifecycle.
- Row-pipeline `ExplainPhysical` diagnostics.
- Phase 2.5 optimizer, lifecycle, concurrency, memory and benchmark regressions.

## Changed

- Lazy physical lowering now composes Scan/Filter/Projection/ExprProjection/Join as `source.Source` wrappers.
- `ExplainPhysical` uses the general row-pipeline path for row-producing LazyFrames; terminal `PreparedJoin.Explain` remains available separately.
- Projection pruning no longer relies exclusively on a native `PushdownSource` implementation.
- Join is no longer terminal for the general Lazy DAG; `PrepareJoin` remains the low-materialization row-reference API.

## Deliberately deferred

- generic type coercion and casts;
- float/string/date/decimal scalar expression functions;
- expression fusion / common-subexpression elimination;
- arbitrary opaque Arrow Take/materialization;
- streaming JoinSource without a left cardinality prepass;
- radix/spillable JoinSource;
- Sort/Limit/Window/ExplainAnalyze.
