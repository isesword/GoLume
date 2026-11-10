# Exact Decimal128 rescale

`expr.RescaleDecimal128(value, precision, scale)` and the equivalent
`lazy.RescaleDecimal128` explicitly convert one native Decimal128 value to a
chosen decimal precision/scale without changing its numerical value. The result
can be named with `.Alias(name)`. The expression node is
`expr.Decimal128RescaleExpr`.

This complements the existing exact Add/Sub/Mul and cross-scale comparisons.
[Exact casts](DECIMAL128_CAST.md) additionally support integers and strings.
Generic `CastTo` cannot target Decimal128 without precision/scale; use
`CastDecimal128` for that target, and `CastTo` for decimal-to-integer/string.

## Contract

- Inputs must be native Decimal128 with valid metadata. Integers, floats,
  strings, opaque decimal payloads, and Decimal256 are not coerced.
- Target precision is 1..38; target scale is 0..precision. Binding validates
  source and target metadata even for NULLs and statically dead branches.
- Increasing scale by `d` multiplies the unscaled coefficient by `10^d`.
  The exact target-precision bound is checked **before** multiplication, so an
  overflowing intermediate cannot wrap into an accepted result.
- Decreasing scale by `d` divides the coefficient by `10^d`. Any nonzero
  remainder is an error, not rounding or truncation. Exact divisibility is
  checked before target precision when both could fail on the same row.
- At the same scale, narrowing precision still checks every demanded non-NULL
  coefficient. All successful results satisfy `abs(coefficient) < 10^precision`.
- NULL input produces NULL, with no inspection of its coefficient payload.
  Result nullability is the input's declared nullability.
- Value-dependent failures happen during execution, never by compile-time
  folding. IF, CASE, COALESCE and IN retain their guarded evaluation domains.
  A NULL parent or an eager AND/OR sibling does not suppress a child failure.
- The first failing row within the current instruction is reported. Selected
  execution reports the position within its selected batch, not the original
  source row. Error indices restart for each execution batch. Child expressions
  execute in existing left-before-right order; errors are not globally sorted
  by source row. No partial output is returned by a failed batch evaluation.
  Streaming readers can already have returned earlier successful batches.
- Rescale is conservatively marked `MayError`, including same-type rescale.
  Optimizers preserve its demanded domain. Unused projections may be pruned.
- Semantic errors release expression reservations and close the workspace;
  retryable memory-admission failures retain accurately accounted state.

Examples (coefficients, not displayed decimal strings):

```go
// 123.45 -> coefficient 123450, precision 8, scale 3.
expr.RescaleDecimal128(expr.Decimal128(decimal128.FromI64(12345), 5, 2), 8, 3)

// 12.30 -> coefficient 123, precision 5, scale 1, exactly.
expr.RescaleDecimal128(expr.Decimal128(decimal128.FromI64(1230), 5, 2), 5, 1)

// 12.34 -> scale 1 fails rather than returning 12.3.
expr.RescaleDecimal128(expr.Decimal128(decimal128.FromI64(1234), 5, 2), 5, 1)
```

Rescaling branches explicitly to identical precision/scale allows them to
participate in IF/CASE/COALESCE. There is still no implicit branch coercion.

## Implementation and limitations

Upscaling uses two-limb integer multiplication after its decimal-bound check.
Downscaling uses at most two unsigned 128-by-64 divisions by powers of ten,
checking the remainder after each division. Each divisor fits uint64, including
`10^19`. Negatives are divided as magnitudes and their sign is restored only
for an exact quotient. Valid native coefficients exclude the signed 128-bit
minimum. Production code uses neither `big.Int` nor floating point and allocates
no per-row coefficient scratch.

This is scalar integer code in both scalar and SIMD kernel modes. `math/bits`
is limb arithmetic, not SIMD. SIMD dispatch is not claimed for decimal rescale;
vectorization would require checked lane-wide carries and quotient/remainder
support plus independent performance evidence.

[`DivideDecimal128`](DECIMAL128_DIVISION.md) now provides exact division at
explicit result precision/scale, rejecting nonzero remainders. Ordinary `Div`
still rejects decimals rather than guessing a target or rounding policy. Exact integer/string conversions are available through
[decimal casts](DECIMAL128_CAST.md). Float casts, implicit mixed numeric
coercion, rounding modes, negative scales, and wider decimals remain unsupported. Decimal arithmetic still does not silently reduce scale;
callers can explicitly rescale operands first only when the conversion is exact.

## Test coverage

Tests compare coefficients against an independent `math/big.Int` oracle across
all legal source scale and target precision/scale combinations, random values,
exact powers of ten, signed and precision boundaries, and 19/20/37/38-digit
scale gaps. Integration coverage includes dense/selected execution, NULL poison,
CSE identity, nested guards, source preservation, workspace reuse across batch
sizes, failure ordering, all admission-point retries, concurrent workspaces,
Parquet scans, optimized filters/projections, bounded/ordinary execution,
Collect, streaming, sink/readback, later-batch errors, early close and cancellation.
Validation commands and actual results belong to the accompanying validation
report; this list describes the test cases, not an unexecuted pass claim.
