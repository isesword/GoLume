# Numeric promotion compatibility contract

This records the default expression contract, first verified on `ec773430`.
It does not silently convert existing queries to lossless arithmetic.
For opt-in workspace arithmetic, see
[Explicit exact mixed-integer expressions](EXACT_MIXED_INTEGER_EXPRESSIONS.md).
See `FUNDAMENTALS_SEMANTICS.md` for the broader semantic contract and
`NUMERIC_CASTS.md` for explicit strict and tolerant conversions.

## Arithmetic and branch results are different

For binary `Add`, `Sub`, and `Mul` over the ten primitive numeric types:

| Inputs | Result |
| --- | --- |
| Either input Float32 or Float64 | Float64 |
| Both inputs Uint64 | Uint64 |
| Exactly one input Uint64, other input an integer | Float64 |
| All remaining integer pairs | Int64 |

This is based on declared types, not observed values. Even small mixed Uint64
values select Float64. Float32 + Float32 also selects Float64. Integer division
is not integer quotient: `Div` always returns Float64.

`If`, `Case`, and `Coalesce` preserve an identical branch type, including Int8,
Uint32, and Float32. Different numeric branch types use the arithmetic common
type. Thus Float32 + Float32 returns Float64 but an If with two Float32 arms
returns Float32. A nonselected arm still participates in type inference. For
multiple arms, types are combined in branch order; this document does not claim
an independently redesigned, order-independent least-upper-bound type system.

The new external-package regression declares a literal 10 by 10 result-type
matrix independently of `CommonNumericType` and exercises every ordered pair
through public compilation and workspace evaluation in scalar and requested
SIMD modes. The literal matrix covers Add/Sub/Mul/Div and If/Case/Coalesce
(1,400 evaluations). A separate typed-column matrix repeats these operations
over 130 rows, crossing both 64-bit bitmap boundaries, with varying values,
left-hand NULLs, and true/false/NULL conditions. It checks result types, every
logical validity bit, and every valid result value, including alternate branch
and Coalesce fallback conversions. Requested SIMD is not evidence that native
SIMD executed: unsupported builds/hosts can fall back to scalar kernels.
These are test coverage descriptions, not a claim of a completed test run.

## Precision is not guaranteed by promotion

Binary64 represents all integers through 2^53 exactly, not every larger integer.
The contract regression constructs integer vectors without a floating-point
intermediate. It distinguishes these operations:

- Uint64(2^53 + 1) compared directly with Int64(2^53): greater and not equal.
- The same mixed integer subtraction: Float64 zero, not the mathematical one.
  Each operand is converted before subtraction; 2^53 + 1 rounds to 2^53.
- Uint64(max) converted by a mixed conditional: Float64 2^64.
- Uint64(max) minus Int64(max): Float64 2^63. This particular rounded answer
  equals the exact difference, despite both inputs having rounded.
- Uint64 zero compares greater than Int64(-1), without unsigned wrapping.

Direct integer comparison is a separate domain and does not use the arithmetic
Float64 common type. This does not restore precision already lost by an earlier
arithmetic expression or explicit cast. Floating inputs retain floating-point
comparison semantics; this document makes no blanket exact-comparison claim
for arbitrary integer/float combinations.

NULL arithmetic and comparisons stay NULL; hidden payloads must not affect
the logical result. The regression does not assert that payload memory is
never physically read.
A NULL If condition chooses the else arm. Coalesce selects the first valid
value; selection does not waive the common output conversion.

## What this work does not decide

This compatibility record must not be interpreted as endorsement of lossy
mixed-integer arithmetic for every analytical use case. Opt-in exact expressions are deliberately limited to one mixed Int64/Uint64
operation at a time. Broader automatic promotion and Int128 storage support
are separate decisions, with impacts on type inference, downstream kernels,
budgets, and stored-query behavior.

Decimal, temporal types, arbitrary precision, rounding policies, and generic
nested branches are outside this ten-type matrix. Existing explicit Decimal128
APIs are not a generic automatic widening mode. This step advances roadmap
item 17 but does not complete its broader design decisions. No performance
improvement or native ARM64 validation is claimed.
