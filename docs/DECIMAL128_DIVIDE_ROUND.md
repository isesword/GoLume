# Explicit rounded Decimal128 division

> Integration note: this API description preserves the earlier isolated
> implementation's validation references. Those historical checks are not test
> results for the new b33-based candidate. The two math operations are selectively
> ported; existing date/time/window opcodes retain their values and the new private
> Decimal opcodes are appended. No nested-container feature is included here.
> Final combined testing and publication status are reported separately.

`expr.DivideRoundDecimal128(left, right, precision, scale, mode)` and the matching
`lazy` wrapper divide two native Decimal128 expressions, rounding the exact
quotient once into an explicitly chosen target. All five arguments are required.
The zero rounding-mode value is invalid, including in a dead branch.

For coefficients a,b and input scales sa,sb, the target coefficient is the
rounded mathematical value `a * 10^(scale + sb - sa) / b`. Target precision is
1..38 and scale is 0..precision. Inputs may have different valid metadata.
There is no implicit integer/float/string conversion or opaque Decimal admission.

## Rounding and errors

The six existing DecimalRoundingMode values are reused:

- TowardZero: discard the remainder.
- AwayFromZero: increase magnitude for any nonzero remainder.
- Floor / Ceil: round toward negative / positive infinity respectively.
- HalfEven: nearest, exact ties to an even target coefficient.
- HalfAwayFromZero: nearest, exact ties away from zero.

Signs include a negative denominator. A zero quotient still uses the original
quotient sign for directional rounding; a final zero has no negative-zero form.
Exact quotients are unchanged by every valid mode. Precision is checked after
rounding, including carry; overflow is an error, not saturation or scale change.
A demanded non-NULL zero divisor is an error even for 0/0. A NULL operand gives
NULL and skips the division value error. Child-expression errors are not hidden:
children retain their normal eager, left-before-right evaluation order.

For example, 1/3 into scale2 gives0.33 with HalfEven; -1/3 gives-0.34 with Floor.
9.95 into precision2/scale1 with HalfEven overflows (rounded10.0 needs3 digits).
1251/1000 directly into scale1 with HalfEven is1.3, not1.2: no intermediate
scale2 rounding is performed. This differs from composing two rounding steps.

## Execution and compatibility

The fixed-width implementation retains the complete quotient and remainder in
six uint64 limbs before rounding. Half comparisons use r against denominator-r,
so odd denominators cannot become false ties. Final full-width carry precedes
narrowing and precision checking. No floating-point arithmetic or runtime
big.Int is used. Existing Decimal output buffers and query admission apply;
this adds no new per-row owner or separate budget exemption.

IF/CASE/COALESCE/IN preserve demand domains; binding validates all branches.
Selected evaluation uses the existing native Decimal gather contract and its
compact selected-row error numbering. CSE includes ordered operands, target
metadata and mode. The expression remains MayError because zero and overflow
can fail. It does not imply source predicate pushdown support.

`DivideDecimal128` remains exact and rejects nonzero remainder before checking
precision. Ordinary Div still rejects Decimal. Cast/Rescale defaults, ordinary
aggregates and exact window rounding are unchanged. Decimal aggregation, join,
ordering and window admission are not expanded. This capability is incremental
on the separately verified item18 integration, not a published commit or a
claim that roadmap item18 is complete.

Independent integer-oracle tests and end-to-end Arrow opt-in / Collect / IPC /
Parquet checks are recorded in the companion validation archive. Its final
review and six-phase gate log, not this API description, establish test status.
No throughput improvement, whole-query zero-allocation, hosted CI or native
ARM64 execution is claimed.
