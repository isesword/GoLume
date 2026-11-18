# Explicit Decimal128 rounding

> Integration note: this API description preserves the earlier isolated
> implementation's validation references. Those historical checks are not test
> results for the new b33-based candidate. The two math operations are selectively
> ported; existing date/time/window opcodes retain their values and the new private
> Decimal opcodes are appended. No nested-container feature is included here.
> Final combined testing and publication status are reported separately.

`expr.RoundDecimal128(value, precision, scale, mode)` and its `lazy` wrapper
round an existing native Decimal128 value to explicit target metadata. The
fourth argument is mandatory and uses `expr.DecimalRoundingMode`; zero and
unknown enum values are invalid, not defaults. `.Alias(name)` names the result.
No global rounding setting or implicit numeric conversion is introduced.

## Modes

For a reduction in scale, divide the absolute coefficient by the full power of
ten to obtain quotient q and remainder r. If r is zero, no mode changes q.
Otherwise:

| Explicit mode | Increase absolute q by one when |
| --- | --- |
| `DecimalRoundTowardZero` | never |
| `DecimalRoundAwayFromZero` | always |
| `DecimalRoundFloor` | the input is negative |
| `DecimalRoundCeil` | the input is positive |
| `DecimalRoundHalfEven` | above halfway, or exactly halfway and q is odd |
| `DecimalRoundHalfAwayFromZero` | at or above halfway |

The original sign is restored only after this decision. Decimal zero has no
negative-zero representation. Rounding +2.5 to scale0 yields respectively
2,3,2,3,2,3; rounding -2.5 yields -2,-3,-3,-2,-2,-3. Half-even parity concerns
the retained coefficient: 1.245→scale2 is1.24, 1.255→scale2 is1.26, with symmetric
negative results. No floating approximation, epsilon or double rounding is used.

## Precision, errors and NULLs

Source must be native Decimal128 with supported metadata. Target precision must
be1..38 and scale0..precision. Invalid metadata/modes fail binding even in a dead
branch or on all-NULL input. Integer/string inputs require an explicit existing
cast; float, opaque Decimal and Decimal256 are not implicitly converted.

Increasing scale adds exact coefficient zeros; equal scale only checks precision.
A valid explicit mode is still mandatory in either case. Decreasing scale makes
one rounding decision using the complete remainder, including gaps above19digits.
Only the final rounded coefficient is checked against target precision. For
example9.95 rounded half-even to precision2/scale1 produces coefficient100 and
fails overflow; precision3/scale1 can represent10.0. Nothing clamps or saturates.

NULL produces NULL without reading its coefficient. Value-dependent overflow is
an execution error in the demanded domain; conditional guards retain their
existing lazy domains. Binding errors cannot be hidden. The node is conservatively
MayError, including equal/upscale. Batch error rows and earlier successful stream
batches follow the existing exact Decimal expression contract. There is no
TryRound API that silently substitutes NULL for a rounding error.

## Independence from exact operations

`RescaleDecimal128`, decimal casts and `DivideDecimal128` remain exact. They
continue rejecting discarded nonzero digits and inexact quotients. Ordinary
float `Round` is unchanged. This operation rounds an already-represented value;
it cannot make1/3 representable through the exact division API. Decimal addition,
subtraction and multiplication still do not silently round their results.

Window Sum/Avg retain their separately approved exact accumulation and final
nearest-even result semantics. This API adds no configurable window/aggregate
rounding, ordinary group/global Decimal aggregation, Decimal Join/order support,
negative scales or wider decimal representation.

## Execution and verification

Production coefficient arithmetic is fixed-width integer code, with complete
quotient/remainder and no per-row big.Int, floating conversion or heap scratch.
Existing Decimal buffer accounting, ownership, selected gathering and guard
execution are reused. Scalar/requested SIMD tests do not claim a dedicated SIMD
rounding kernel or speedup.

The decimal-round-validation archive records source hashes, independent big.Int
oracle and query tests, review and actual full-gate results. The existing exact
baseline probe documents unchanged rejection; no synthetic new-API stub is used
to manufacture a baseline failure. Hosted CI, native ARM execution, publication
and completion of the broader roadmap item are not implied by local tests.
