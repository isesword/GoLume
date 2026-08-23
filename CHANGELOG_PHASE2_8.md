# Phase 2.8 changelog

- Added Lazy `Sort`, `TopK`, and `Limit` nodes plus typed ordering expressions.
- Added optimizer rules for nested limits and `Sort + Limit -> TopK`.
- Added projection dependency and safe predicate-boundary handling for ordering nodes.
- Added budget-accounted full materialization `SortSource`.
- Added bounded-heap `TopKSource` with stable ordering and winner-partition rereads.
- Removed per-input-row Top-K candidate allocation; retained candidate buffers are reused.
- Added `memory.OpaqueVector` ownership semantics, generic Arrow gather/take through slice/concatenate, and deterministic Frame cleanup.
- Added Aggregate-result columnar source so `GroupBy/Agg -> Sort/TopK/Limit` can continue through the Lazy DAG.
- Added bounded per-plan EWMA runtime feedback for selectivity/cardinality.
- Added feedback-aware aggregate CostModel preparation with explicit user-hint precedence.
- Added feedback regressions proving physical strategy changes on later prepares.
- Added Sort/Top-K memory budget and ownership regressions.
- Added 262K Sort-vs-Top100 benchmark and validation artifacts.
