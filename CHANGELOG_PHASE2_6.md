# Phase 2.6 changelog

## Typed expression engine

- Added `Float64Literal`, `StringLiteral`, `Date32Literal`, `TimestampLiteral`.
- Added `/`, string concat, and explicit Cast.
- Added centralized scalar type inference and numeric coercion.
- Added float/string/date/timestamp predicate comparisons.

## Fusion / CSE

- Replaced per-expression projection programs with one `ProjectionProgram`.
- Added compile-time common-subexpression reuse across projection outputs.
- Added one reader-local typed scalar workspace per ProjectSource reader.

## Arrow RecordBatch

- Added typed physical bindings for float32, date/time and timestamp/duration families.
- Removed duplicate OpaqueVector bindings when a specialized vector exists.

## Join

- Removed JoinSource probe-side cardinality pre-read during prepare.
- Added maximum build-key multiplicity tracking for safe output upper bounds.
- Added radix fixed-key join tables and reusable radix probe workspace.
- Added explicit `JoinSourceOptions.RadixBits` experiment control.
- Kept radix disabled by default after the validation benchmark regressed.
- Extended joined payload materialization to semantic date/time/float32 integer families.

## Diagnostics

- Added `LazyFrame.Analyze` and `LazyFrame.ExplainAnalyze`.
