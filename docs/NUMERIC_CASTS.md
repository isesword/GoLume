# Numeric casts

`CastTo(value, target)` (or `Col("text").Cast(target)`) now parses strings into
Int8/16/32/64, Uint8/16/32/64, and Float32/64. Outputs retain their requested
physical width. Existing numeric casts and the public two-field `Cast` struct
remain compatible.

`TryCastTo(value, target)` and the `TryCast` methods on columns, string literals,
and cast results accept numeric or string inputs and a numeric target. A syntax
or range conversion failure becomes NULL. This is not a general exception
handler: child expression failures, invalid storage, cancellation and budget
failures still propagate. Unsupported input/target combinations fail binding.
The result schema is conservatively nullable, even if every row succeeds.

## String grammar and boundaries

- Integer parsing is base ten with an optional leading sign. Unsigned input
  permits `+`, but rejects every `-` spelling, including `-0`.
- No surrounding whitespace, grouping separators, underscores, hex literals,
  fractional integer notation, or exponent notation for integers is accepted.
- Integer overflow is checked exactly at the requested width without going
  through floating point. For example Uint64 `18446744073709551615` is exact.
- Floats accept decimal notation with an optional sign, decimal point and
  exponent, including `.5`, `1.`, and `01e2`.
- Case-insensitive NaN, Inf and Infinity are accepted, including signed NaN.
  NaN is a valid floating value, not NULL. Signed zero is preserved.
- Floating string overflow produces signed infinity; finite decimals round to the nearest representable floating value
  (ties to even), and sufficiently small magnitudes underflow to signed zero. Float32 parsing rounds directly to Float32 precision.
- NULL inputs stay NULL without inspecting their payloads. Empty strings are
  invalid. Empty batches produce empty typed outputs.

These choices follow the pinned Polars 1.35.2 oracle in
`testdata/fundamentals/polars-1.35.2.json`. Deliberate compatibility distinction:
existing Float64-to-Float32 *numeric* casts still reject infinities and values
outside Float32 range. Their TryCast counterparts return NULL for those values;
new string-to-Float32 casts accept infinity. Finite floating-to-integer numeric
casts retain truncation toward zero with checked bounds.

## Execution and safety

Strict casts are marked as potentially failing before planning; TryCast retains
its children's error effect. Strict and tolerant forms have distinct structural
identities, and guarded expressions do not evaluate inactive rows. Tolerant casts
participate in dependency analysis and liveness-based register reuse. Fixed-width
payloads and nullable bitmaps use existing workspace admission and release.

The scalar parser is the fallback in every kernel mode. There is no new SIMD
performance claim or unmeasured architecture gate. Variable-length, nullable and
dictionary strings use the same exact parser. SIMD candidates require separate
benchmarked, architecture-specific evidence and identical rounding/error rules.

Integer conversion consumes bytes directly. Floating syntax is validated before
calling Go's correctly rounded parser. An exact comparison against the IEEE
round-to-nearest overflow boundary avoids constructing a copied overflow error
for successful infinity output. Long decimal mantissas are normalized with
790 significant digits and a sticky digit, exceeding the precision needed to
distinguish every binary32/64 rounding midpoint. This avoids a Go 1.27 Float32
slow-path issue for long coefficients with compensating exponents. The parser borrows an immutable, read-only string
view synchronously; no view or library error escapes parsing. Invalid input
returns a fixed internal error rather than retaining the input token.

## Validation

`expr/cast_numeric_test.go` covers all ten target widths, integer extrema,
Float32/64 special values, signed zero, invalid grammar, strict/tolerant
structural separation, child errors, dictionaries, guarded rows, empty and
bitmap-tail batches, workspace reuse and release, low-budget rejection and
cancellation. It compares numeric TryCast with existing strict conversion and
performs seeded differential checks against Go's floating parser around exact
IEEE overflow boundaries and long decimal mantissas.

Benchmarks: `BenchmarkStringNumericCast` and `BenchmarkTryCastStringValidity`.
They include valid, invalid, long-valid, long-invalid and overflow cases in view
and dictionary encoding. Run with `-benchmem`; timings are host-specific, not
cross-architecture release gates.
