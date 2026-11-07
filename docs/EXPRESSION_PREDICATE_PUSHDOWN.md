# Safe expression-projection predicate pushdown

The optimizer can now attach source-pruning predicates through expression
projections when output names can be resolved to input column identities.
`SelectExpr(expr.Named(expr.Col("x"), "alias"))` followed by a predicate on `alias` can prune
using statistics for `x`. WithColumns-style projections also allow predicates
on unchanged input columns, even when unrelated safe computed outputs exist.

## Semantic boundaries

- The original row-level filter stays above the expression projection. Source
  statistics only prove that whole groups cannot match; retained groups still
  require the original residual predicate.
- Every projection expression must bind successfully and have `MayError=false`.
  An unrelated fallible output blocks crossing, including fallible children
  hidden inside IF, CASE or COALESCE. This prevents pruning from suppressing
  demanded row-value errors. Cancellation, allocation admission and invalid
  source storage remain outside the row-value error effect contract.
- Alias substitution is simultaneous within one projection. For `x := y` and
  `y := x`, a predicate on output `x` resolves to input `y` exactly once. Across
  multiple projections the translation composes once at each boundary.
- Computed replacement names are not passthrough columns. A predicate referencing
  one cannot cross that projection, even when an input column has the same name.
  Arbitrary computed-expression substitution is deliberately unsupported.
- The translated predicate is rebound against the input schema, must be Boolean
  and non-fallible, and retains operand order, guarded structure, native types,
  NULL behavior and NaN behavior. Caller-owned AST slices are not modified.
- This rule does not split conjuncts. If any referenced output is not a direct
  column identity, the entire inherited predicate stops at that boundary.
- Existing Limit, Top-K, window, join and aggregate barriers are unchanged.
  Fallible ordering keys also remain barriers. Filters already below a barrier
  continue to be optimized independently.

Predicate annotation currently precedes dead-output elimination. Consequently,
a fallible output that will later be eliminated can still block this rule.
This is conservative, not a change to the existing unused-output contract.

The `expression-predicate-pushdown` rule marker records a successful translation
at a projection boundary. The usual `predicate-pushdown` marker records scan
annotation. A translation may encounter a later barrier before reaching a scan.

## Performance scope

This stage enables source row-group pruning; it does not relocate row-level
filters or add pruning capabilities to a source. Reduced scanning, expression
work and allocations depend on usable source statistics and selectivity.
No reduction is promised for a source without pruning or for groups that all
remain possible matches. Benchmarks and validation records are under
`validation/expression-predicate-pushdown/`.

The pinned benchmark uses 65,536 rows in 16 ordered Parquet groups, with one
4,096-row group retained and 2,048 final matching rows. Both paths compute an
alias and an additional safe comparison output. Five 200ms samples with
GOMAXPROCS=1 on the shared AMD EPYC 9V74 host produced these medians:

| Plan | Time/op | Allocated bytes/op | Allocations/op | Source rows/op |
| --- | ---: | ---: | ---: | ---: |
| Unoptimized | 2.796 ms | 4,116,578 | 2,377 | 65,536 |
| Optimized | 0.396 ms | 481,939 | 920 | 4,096 |

This fixture scans 93.75% fewer rows and allocates about 88.3% fewer bytes.
Allocated bytes are cumulative Go allocations, not peak resident memory or a
memory-budget guarantee. Timing is descriptive shared-host evidence; the earlier
short smoke benchmark had different timings. The benchmark excludes logical
optimization and file generation but includes source-pushdown application,
physical lowering, reading, expression evaluation and result collection.
