# Lazy Optimizer

## Design rules

The Phase 2.3 optimizer is rule based. Rules operate only on immutable logical
plans and never perform row-level execution.

The first rule set is intentionally small and high-value:

1. merge adjacent filters;
2. merge compatible projections;
3. compute required source columns;
4. push predicates toward scans when semantics permit it;
5. annotate scans with projections and predicates;
6. remove no-op projections.

A cost model remains responsible for choosing physical execution strategy after
logical optimization.

## Projection dependency analysis

Required columns are propagated from plan consumers toward each scan.

```text
Aggregate
  keys=[key]
  aggs=[sum(value)]

Projection
  [key, value]

Filter
  pred > 32767

Scan
  [pred, key, value]
```

A projection cannot remove a column that is still required by a lower residual
filter. The source projection is therefore the union of all columns needed
before each value becomes dead.

## Predicate movement

A predicate can move through a column-only projection when every referenced
column remains semantically unchanged. Phase 2.3 does not push predicates
through aggregate nodes.

Filters are merged using SQL `AND`:

```text
Filter(a > 10)
  Filter(b < 20)
```

becomes one residual predicate equivalent to:

```text
(a > 10) AND (b < 20)
```

The scan receives the same predicate as pruning metadata.

## Conservative source pruning

The source-level predicate is an opportunity for pruning, not permission to
remove row-level filtering.

For Parquet metadata, every row group receives a set of possible SQL truth
values. If TRUE is impossible, the row group can be discarded. Otherwise it is
kept.

This conservative rule is important for:

- NULL values;
- incomplete statistics;
- unsupported operators;
- logical OR/NOT combinations;
- metadata whose min/max values are missing.

Unsupported expressions produce `unknown`, which means **keep**, never prune.

## Optimizer/physical separation

The optimizer does not choose `radix`, `bitmap`, or SIMD width. After source
pushdown, the existing physical planner sees:

- source row count after pruning;
- source schema after projection;
- predicate complexity;
- cardinality/selectivity hints;
- key width;
- available workers/kernel capabilities.

This keeps logical rewrites independent from CPU architecture.

## Future rules

The next rule families can be added without changing `LazyFrame` semantics:

- constant folding;
- boolean simplification;
- common subexpression elimination;
- limit pushdown;
- dictionary-aware equality pruning;
- aggregate partial/final lowering;
- join-side selection and join reorder;
- runtime-statistics feedback.
