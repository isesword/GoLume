# Exact Decimal128 casts

`expr.CastDecimal128(value, precision, scale)` and
`expr.TryCastDecimal128(value, precision, scale)` convert native signed/unsigned
integers, strings, or native Decimal128 to an explicit decimal type. Both have
matching `lazy` helpers and support `.Alias(name)`.

For conversions **from** Decimal128, use `expr.CastTo(value, target)` with any
native signed/unsigned integer type or `memory.TypeString`. Use
`expr.TryCastTo(value, integerTarget)` to return NULL for inexact or out-of-range
integer conversions. Generic `CastTo(..., memory.TypeDecimal)` cannot specify
precision/scale and remains unsupported; use the explicit decimal constructor.

## Exactness and types

- Decimal precision is 1..38; scale is 0..precision. Binding validates metadata
  even for NULL inputs and statically dead branches. Unsupported input/target
  types are binding errors, not tolerant NULL results.
- Integer-to-decimal conversion multiplies the coefficient by the target power
  of ten and checks the target precision. All eight native integer widths are
  supported, including the full uint64 range.
- Decimal-to-decimal conversion follows [exact rescale](DECIMAL128_RESCALE.md):
  downscaling must discard only zeros, and the coefficient must fit the target
  precision. There is no implicit rounding or truncation.
- Decimal-to-integer conversion first checks that the fractional remainder is
  zero, then checks the target integer range. Negative values cannot become
  unsigned integers, except that zero is unsigned-compatible.
- Decimal-to-string conversion emits ordinary base-ten notation with the full
  declared scale, including trailing zeros. It emits no exponent or leading
  plus sign and normalizes negative zero. Scale zero emits no decimal point.
- NULL inputs remain NULL without inspecting their coefficient/text payload.
  Tolerant conversion results are nullable.

## String grammar

Input is an optional ASCII `+` or `-`, followed by ASCII digits with at most one
`.`. There must be at least one digit. `1.`, `.1`, and `+.1` are valid. Leading
zeros are valid. Whitespace, exponent notation, underscores, non-ASCII digits,
NaN and infinity are invalid. Excess fractional digits are accepted only when
all discarded digits are zero. Validation covers the entire token, including
very long strings, with bounded coefficient scratch.

On a single failing string, precedence is **syntax, inexactness, overflow**.
For example `999.001x` is a syntax failure; `999.001` converted to `(3,2)` is
inexact even though it would also exceed the target precision.

```go
// coefficient 1230, representing 12.30
expr.CastDecimal128(expr.Str("+12.3000"), 4, 2)

// NULL, since 1.234 cannot be represented exactly with scale 2
expr.TryCastDecimal128(expr.Str("1.234"), 4, 2)

// "12.30", retaining the declared scale
expr.CastTo(expr.CastDecimal128(expr.Str("12.3"), 4, 2), memory.TypeString)

// int64(12); use TryCastTo to turn an inexact/range failure into NULL
expr.CastTo(expr.CastDecimal128(expr.Str("12.00"), 4, 2), memory.TypeInt64)
```

## Evaluation and errors

Strict casts fail on the first demanded non-NULL conversion failure in the
current instruction. Tolerant casts mask **only their own conversion failures**.
They do not catch failures in child expressions, invalid metadata, malformed
storage, cancellation or memory admission. In particular, wrapping a failing
strict cast in a tolerant cast does not make that child safe.

Existing IF, CASE, COALESCE and IN guards retain their selected evaluation
domains. AND/OR are eager and do not protect a failing cast. A filter applied
before a cast can remove offending rows; a later filter must not erase an
already-demanded failure. Optimizers may prune unused projections but retain
observable failure ordering. Value-dependent failures are execution errors,
not constant-folding errors.

Error row indices are relative to the current selected execution batch, not
global source positions. A failed batch produces no partial batch output.
Streaming can return successful earlier batches before a later error. Closing
readers and frames releases owned query memory; early close and cancellation
follow the existing stream contract.

## Materializing integer outputs

`Series.ToAnySlice` returns the corresponding native Go integer type for all
eight signed/unsigned cast outputs, and `nil` for NULLs. `NullCount`, sliced frame
views, streaming batches, and Parquet sink/readback preserve that representation.
No widening cast is necessary. Native Float32 materialization and native-width
struct child access use their corresponding native storage as well.

The older `Int32Chunks`, `Uint32Chunks`, and `Float64Chunks` APIs still assume
widened physical-family storage; this change does not add native-width chunk
APIs or make those older methods accept native narrow vectors. Use
`ToAnySlice` for native narrow output materialization.

## Scope and validation

Float-to-decimal and decimal-to-float casts, implicit mixed numeric coercion,
rounding modes, negative scales, opaque decimal payloads and Decimal256 are not
supported. Decimal casts use fixed-width integer operations in both scalar and
SIMD kernel modes; this is not a claim of SIMD acceleration.

Query tests cover native integer/string/decimal conversion chains, NULLs,
tolerant syntax/inexact/range failures, optimized projection/filter ordering,
dead branches, unused casts, Parquet and in-memory sources, Collect, streaming,
Parquet sink/readback, later-batch failure, early close and cancellation. They
exercise bounded and ordinary execution with scalar and SIMD kernel settings.
Actual validation commands/results should be recorded separately; this list
is coverage description, not a claim that unrun checks passed.
