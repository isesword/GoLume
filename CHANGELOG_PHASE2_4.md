# Phase 2.4 changelog

## Added

- Arrow RecordBatch source (`source/arrowrecord`).
- `Session.ScanArrow` with retained RecordBatch lifecycle.
- Expanded Arrow-aware logical data types and opaque Arrow vectors.
- Typed RecordBatch bindings for int32/int64/uint32/uint64/float64/string/binary.
- Generic opaque-vector NULL predicate support.
- Source-wide UTF-8 dictionary normalization across RecordBatches.
- Cached normalized dictionary row codes, string offsets and validity bitmaps.
- Cached int64 RecordBatch statistics for conservative whole-batch predicate pruning.
- Logical Join node and schema inference.
- Join-aware projection dependency propagation.
- Lazy terminal inner/left/semi/anti equality joins.
- Single/composite fixed numeric join lowering.
- Cross-domain dictionary string join lowering.
- Prepared build-side join state with memory-budget accounting and retryable initialization.
- Phase 2.4 Arrow/Join tests and release validation logs.

## Changed

- LazyFrame stores builder errors so fluent API failures propagate to Prepare/Collect/Explain.
- ExplainPhysical recognizes terminal Join plans.
- Session closes retained Arrow sources even when backed by a caller-owned Engine.
- Join projection pushdown reads only key/filter columns for terminal row-reference output.
- Arrow source conversion work is moved to Source construction rather than repeated Reader execution.

## Deliberately deferred

- expression projections / WithColumns;
- aggregate-over-join and joined payload streaming;
- radix/spillable physical join;
- arbitrary-type compute kernels for every Arrow type;
- pqarrow RecordReader lowering;
- Sort/Window/Limit;
- ExplainAnalyze.
