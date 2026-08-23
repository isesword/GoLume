# Roadmap

## Completed

### Phase 1

- Arrow-compatible vectors and validity bitmaps.
- Scalar / Go 1.27 portable SIMD dispatch.
- Specialized numeric hash aggregation.
- Radix ownership, persistent workers and Parquet adapter.

### Phase 2.1

- PreparedQuery and reusable budget-accounted workspaces.
- SQL 3VL predicate program.
- Dictionary/plain-string and fixed composite group-by.
- Initial reusable join primitives.

### Phase 2.2

- Adaptive hash grow/rehash.
- Nullable single/composite group keys.
- SUM/COUNT/MIN/MAX/AVG SoA aggregate layouts.
- Dictionary/fixed-128 equality join primitives.
- Repository-level GoDoc checking.

### Phase 2.3 — Lazy planning and Parquet pushdown

- immutable LazyFrame and LogicalPlan;
- schema inference;
- Projection and Predicate Pushdown;
- conservative Parquet row-group pruning;
- Explain / ExplainOptimized / ExplainPhysical;
- Lazy -> PreparedQuery lowering.

### Phase 2.4 — Arrow RecordBatch and Lazy Join

- Arrow RecordBatch typed/opaque source;
- UTF-8 dictionary normalization;
- cached whole-batch pruning stats;
- logical Join and PreparedJoin;
- fixed/composite/dictionary equality joins.

### Phase 2.5 — Expression and composable physical DAG

- SelectExprs / WithColumns;
- compiled scalar program and expression dependency pruning;
- physical FilterSource / ProjectSource;
- columnar JoinSource;
- Join -> Filter/Projection/GroupBy/Agg;
- CollectFrame / PrepareFrame.

### Phase 2.6 — Typed expressions and streaming join

- typed scalar inference / Cast / numeric coercion;
- float/string/date/timestamp execution;
- shared ProjectionProgram with CSE;
- streaming JoinSource without probe-side pre-read;
- radix fixed-key join implementation;
- ExplainAnalyze.

### Phase 2.7 — operator diagnostics and parallel radix join

- operator-level Analyze topology/timing/row counters;
- Prepare/Execute phase separation;
- persistent-worker parallel radix build/probe;
- benchmark-backed large fixed-key auto-selection.

### Phase 2.8 — ordering, opaque Take, runtime feedback

- Lazy Limit / Sort / Top-K;
- nested Limit simplification and Sort+Limit -> Top-K rewrite;
- full-materialization budgeted stable Sort;
- bounded O(K) Top-K with candidate-buffer reuse;
- generic opaque Arrow row gather plus retained-array ownership;
- aggregate-result source for GroupBy/Agg -> Sort/TopK/Limit;
- bounded 1024-plan EWMA runtime feedback;
- aggregate CostModel feedback with explicit Hint precedence;
- Sort/Top-K and feedback strategy regressions.

## Phase 2.9 — richer analytical operators and calibrated planning

Priority order:

1. **String/date expression breadth**
   - contains/prefix/suffix/length;
   - date/timestamp extraction;
   - timezone/unit-aware casts;
   - checked arithmetic modes.

2. **Ordering quality**
   - expression sort keys without mandatory WithColumns;
   - Top-K key-only first-pass source projection;
   - dictionary-aware/collation-aware ordering hooks;
   - multi-worker stable sort partitions followed by merge.

3. **Runtime planning feedback breadth**
   - feed build/probe bytes, skew and duplicate multiplicity into Join planning;
   - platform-calibrated ARM/x86 thresholds;
   - planner confidence/sample decay and source-version invalidation.

4. **Window foundation**
   - partition/order logical specification;
   - row_number/rank/dense_rank;
   - rolling aggregate execution contracts.

## Phase 3 — bounded memory and spill

- query-level shared memory credits;
- spillable group-by partitions;
- spillable/radix joins;
- external sort/merge;
- streaming partial/final aggregation;
- cancellation-safe spill cleanup.

## Phase 4 — user-facing analytical engine

- broader DataFrame facade;
- optional SQL frontend targeting the same LogicalPlan;
- full window expressions;
- Arrow IPC/Flight/ADBC;
- Iceberg/Delta adapters;
- WASM validation after native execution is stable.

## Performance rule

Optimize work avoidance and data movement before micro-kernels. A physical
strategy is not enabled because it is theoretically attractive: it must win an
end-to-end benchmark on representative hardware and data distributions.
