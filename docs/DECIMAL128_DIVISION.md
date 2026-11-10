# Exact Decimal128 division

`expr.DivideDecimal128(left, right, precision, scale)` and the equivalent
`lazy.DivideDecimal128` divide two native Decimal128 expressions into an explicit
result type. They return `expr.Decimal128DivideExpr`, which supports `Alias`.
Both input precisions/scales may differ. The result precision must be 1..38 and
scale 0..precision. Nullability is the union of the operand nullabilities.

The result represents the exact mathematical quotient. Given coefficients
`a`, `b`, operand scales `sa`, `sb`, and target scale `s`, its coefficient is:

```
a * 10^(s + sb - sa) / b
```

The target is always explicit. Ordinary `expr.Div` / `Col.Div` continue to
reject decimal operands, as do implicit decimal/integer/float/string mixtures,
opaque decimals, invalid metadata, and negative scales. An integer divisor
must be expressed as a typed decimal literal, not an implicit cast.

## Exactness and errors

For a demanded row whose operands are both non-NULL:

1. A zero divisor is an error, including `0 / 0`.
2. If the mathematical coefficient is not an integer, execution returns
   `loses fractional digits`, without rounding or truncating.
3. If the exact coefficient has magnitude at least `10^precision`, execution
   returns an overflow error. Exactness is checked before precision, matching
   the exact rescale contract.

There are no NaN, infinity, saturation, implicit scale reduction, or hidden
rounding modes. `1 / 8` succeeds at scale 3 but fails at scale 2; `1 / 3` fails
at every supported scale. Increasing target precision alone cannot rescue an
insufficient target scale. `0 / nonzero` is exactly zero for every legal target.
Negative quotients retain their sign; zero has a canonical zero coefficient.

```go
// 12.00 / 3 = 4.00, coefficient 400, decimal128(8,2).
expr.DivideDecimal128(
    expr.Decimal128(decimal128.FromI64(1200), 4, 2),
    expr.Decimal128(decimal128.FromI64(3), 1, 0), 8, 2)

// 1 / 8 = 0.125: coefficient 125 at scale 3.
expr.DivideDecimal128(
    expr.Decimal128(decimal128.FromI64(1), 1, 0),
    expr.Decimal128(decimal128.FromI64(8), 1, 0), 3, 3)
```

## NULLs, guards, and planning

A NULL operand makes the quotient NULL, with a cleared coefficient payload.
NULL payload bits are not divided, even if they contain an invalid signed
coefficient. A NULL numerator also suppresses a literal zero divisor's value
error. This does not suppress errors produced while evaluating a child:
children remain eager and left-before-right, just as for existing arithmetic.

IF, CASE, COALESCE, and IN retain their existing demand rules. An undemanded
quotient does not raise a row-value error. Binding still validates every branch,
and any read source is validated independently of row-value guards. AND/OR are
eager; a false sibling cannot hide an evaluated quotient error.

Within an instruction, errors use the first failing row in execution order.
Selected execution reports the selected batch's row number. Across instructions,
existing operand order wins rather than a global smallest-row search. A failed
evaluation returns no successful result and releases its expression reservations.

Division is conservatively `MayError`, including literal `1 / 1`. Optimizers
must preserve its demanded input domain. Earlier empty filters and unused
projections may suppress a quotient; a later false filter cannot erase an
already demanded error. Structural CSE includes ordered operands and both target
metadata fields. Guarded uses keep independent demand domains.

## Fixed-width implementation

The exponent ranges from -38 to 76. A positive exponent grows the numerator to
less than `10^114`; a negative exponent grows the denominator to less than
`10^76`. Six uint64 limbs (384 bits) hold either without overflow. Scaling uses
at most four powers-of-ten chunks that individually fit uint64. Small divisors
use unsigned 128-by-64 division per limb; larger divisors use aligned binary
long division. The complete remainder is checked before narrowing the quotient.
This permits large intermediate numerators that cancel to a valid result.

The successful coefficient path allocates no heap memory and uses no `big.Int`
or floating-point arithmetic. It adds no instruction/register metadata fields.
It is scalar integer code under both scalar and SIMD dispatch; no decimal SIMD
speedup is claimed. The wide-divisor path is a correctness-first baseline.

## Relationship to Polars-style use

This API supports composable column/literal expressions, explicit schema,
filters, conditional guards, and bounded lazy collection/sinks. It intentionally
does not promise Polars decimal arithmetic compatibility. Polars' published
[fixed-scale design](https://pola.rs/posts/polars-in-aggregate-dec25/) permits
rounding to retain its scale. Its
[2.0 SQL rules](https://docs.pola.rs/releases/upgrade/2/)
also distinguish SQL result-scale rules from the expression API. GoLume requires
a caller-selected target and an exactly representable quotient. Rounded
financial allocation or repeating ratios need a separately designed, explicit
rounding API; this change does not choose a rounding convention for the caller.

## Validation scope

An independent `math/big.Int` oracle builds the full scaled rational using both
operand scales rather than production helpers. Tests cover all input/target scale
triples, all target precision/scale metadata, random signed coefficients,
38-digit extrema, exponent -38/76, zero, recurring fractions, exact cancellation,
precision boundaries, and allocation counts. Execution tests cover two-input
NULL/guard combinations, left/right errors, structural identities, selected and
dense evaluation, bitmap boundaries, reused/concurrent workspaces, admission
failures and retries, and source validation. Division-by-one tests also enforce
the exact rescale laws across conditional and streaming paths.
