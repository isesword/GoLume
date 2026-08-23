# Phase 2.8 release manifest

Release: `go-columnar-engine-phase2.8-final`

## New / changed core packages

- `logical/` — Lazy Sort, TopK, and Limit nodes plus explain/schema support.
- `optimizer/` — Limit simplification, Sort+Limit -> Top-K, ordering-key dependency propagation, and conservative Filter boundaries.
- `physical/` — budgeted stable Sort, bounded Top-K, Limit, generic opaque Arrow row gather, and materialization helpers.
- `memory/` — explicit owned opaque Arrow reference lifecycle through Batch/Frame cloning and release.
- `lazy/` — ordering API/lowering, aggregate-result Source, row-DAG ordering composition, and per-plan feedback signatures.
- `engine/` — bounded runtime-feedback store and feedback-aware aggregate preparation/PlanInfo.
- `scripts/` — Phase 2.8 validation coverage and Sort/Top-K benchmark entry.

## Runtime feedback policy

```text
store size    = 1024 plan signatures
smoothing     = EWMA alpha 0.25
eviction      = FIFO for new signatures
apply only if = corresponding user Hint absent
```

Feedback currently refines aggregate selectivity/cardinality inputs. It does not replace the independently benchmarked Join radix threshold.

## Ordering policy

```text
Limit:
  streaming global cardinality boundary

Sort:
  full in-memory materialization
  stable row-index ordering
  MemoryManager reservation

Top-K:
  O(N log K) comparison
  O(K) retained candidates/output
  MemoryManager reservation in Lazy execution
  winner-partition payload reread
```

`Sort(...).Limit(K)` is rewritten to Top-K when the nodes are adjacent after rule optimization.

## Validation

See `VALIDATION.md` and `validation/phase2.8-final/` for:

- scalar/SIMD/width/race/vet/GoDoc matrices;
- opaque nested Arrow row-selection regression;
- Sort/Top-K memory-budget regressions;
- aggregate -> Top-K/Limit execution;
- bounded feedback-store and strategy-change regressions;
- 262K Sort vs Top100 benchmark;
- 1M-row Parquet ExplainAnalyze/correctness output.

## Dependency hygiene

Production `go.mod`:

```text
module gocolumn

go 1.27.0

require github.com/apache/arrow-go/v18 v18.7.0
```

The release archive must not contain sandbox-only dependency files or local filesystem replacements.
