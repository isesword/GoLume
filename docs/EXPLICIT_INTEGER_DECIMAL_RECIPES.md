# Explicit exact-or-error integer arithmetic

These recipes use existing public APIs on baseline `ec773430`; they do not add
an automatic promotion policy or change existing queries. Ordinary mixed
Int64/Uint64 arithmetic still returns Float64 and can lose integer precision.
Convert **both inputs before arithmetic**, not the already rounded result.

```go
left := expr.CastDecimal128(expr.Col("i"), 20, 0)
right := expr.CastDecimal128(expr.Col("u"), 20, 0)
sum := expr.Add(left, right)       // Decimal128(21,0)
difference := expr.Sub(right, left) // Decimal128(21,0)
product := expr.Mul(left, right)   // Decimal128(38,0), checked
```

Decimal128(20,0) represents every original Int64 and Uint64 value exactly.
The sum and difference of any two such inputs fit the inferred result type.
For example Uint64(9007199254740993) minus Int64(9007199254740992) produces
coefficient `1`, precision `21`, scale `0`. The ordinary mixed subtraction
returns Float64 zero; its compatibility behavior is intentionally unchanged.

The guarantee is **an exact result or an error**, not success for every
multiplication. Decimal38 cannot represent all Int64×Uint64 products or
Uint64(max) squared. Such demanded non-NULL results report overflow, never a
rounded Float64 or a silently wrapped Decimal result. Casts do not make arbitrary
long chains overflow-free. Result metadata matters as well as the coefficient.

## Conditions and division

`If`, `Case`, and `Coalesce` require identical Decimal precision and scale in
all result arms. Two explicitly converted Decimal(20,0) inputs can be selected
without losing integer precision. A Decimal(21,0) sum cannot be combined directly
with a Decimal(20,0) fallback; explicitly convert the fallback to Decimal(21,0):

```go
selected := expr.If(condition, sum, expr.CastDecimal128(right, 21, 0))
quotient := expr.DivideDecimal128(left, right, 20, 1)
```

The second expression specifies a decimal result scale, not integer division:
1/2 is coefficient `5` at scale `1`; 1/3 cannot be represented exactly at that
scale and errors. A non-NULL zero divisor errors. These are existing explicit
Decimal division semantics, not a redefinition of ordinary `Div`.

NULL arithmetic remains NULL. An inactive guard does not demand an overflowing
product. Invalid branch metadata still fails binding even if that branch would
not be selected. Conversion/overflow failures are not a blanket reason to swallow
child, storage, cancellation, or memory-budget errors.

## Executable evidence and limits

`expr/decimal_integer_recipe_test.go` is an external-package test using public
compilation and workspace APIs. `ExampleCastDecimal128_mixedIntegerArithmetic`
is executable documentation with checked output. Tests exercise:

- 130-row typed inputs, Int64/Uint64 extrema and values around 2^53, independent
  `math/big` sum/difference oracles, nullable bitmaps, selected and empty results;
- exact small multiplication and independently checked Decimal38 overflow cases;
- condition truth/falsity/NULL, Coalesce fallback, metadata mismatch rejection
  and explicit normalization;
- exact/inexact division and zero divisor, hidden NULL and inactive overflow,
  budget rejection, cancellation and reservation release;
- the unchanged default mixed arithmetic behavior.

Run `go test ./expr -run 'ExplicitDecimalIntegerRecipe|ExampleCastDecimal128_mixedIntegerArithmetic'`.
Repeat with `GOEXPERIMENT=simd` to check compatibility in that build. Requested
SIMD mode and a SIMD-enabled build do **not** mean Decimal executes native SIMD.
No performance or native ARM64 claim is made.

Workspace output is borrowed until the next evaluation or Close. Read it before
reuse, and close the workspace to release its reservations. Selected outputs are
in selected-row order. These expression-level tests do not establish Decimal
support in every aggregation, Join key, spill format or downstream sink; those
endpoints still need separate capability and roundtrip validation. This is a
bounded step in the numeric-promotion roadmap, not completion of the wider plan.
