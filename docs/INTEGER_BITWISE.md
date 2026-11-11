# Native-width integer bitwise expressions

`BitAnd`, `BitOr`, `BitXor`, and `BitNot` operate on Int32, Uint32, Int64,
and Uint64 values. Matching fluent methods are available on columns, unary
and binary results, casts, Int64 literals, IF, CASE, and COALESCE expressions.
For example, `expr.Col("flags").BitAnd(expr.I64(3))` retains the input's
integer type when the literal fits.

## Type and NULL contract

- Binary operands must have the same integer type. An Int64 literal on either
  side may convert to the other operand's type only when exactly representable.
  Mixed column widths or signedness require an explicit cast. A negative
  literal cannot become unsigned, and an out-of-range literal is a bind error.
- There is no conversion through Float64. Uint64 high bits and integers beyond
  2^53 remain exact. Signed results use their native two's-complement bit pattern.
- NOT complements every bit at the input's native width, preserving its type.
  AND, OR, and XOR propagate NULL from either input. NOT propagates input NULL.
  Invalid result lanes contain zero; validity determines NULL, not the payload.
- Floats, Boolean, decimal, string, temporal and narrow 8/16-bit inputs are
  rejected. Cast explicitly to a supported type when needed. Shifts are outside
  this implementation. Logical `And`, `Or`, and `Not` are unchanged.

The binder, compiler, public Workspace, selected-row execution and guarded
IF/CASE regions use the existing ownership and budget contracts. Bitwise
operands remain dense, including literal broadcasts; no new temporary buffer
or coercion through an intermediate numeric domain is introduced. Kernel
calls process at most 4,096 rows between cancellation checks.

## Dispatch and scope

The evaluator calls the corresponding native-width `kernel.Set` bitwise
method. Scalar mode and plain builds execute scalar kernels. SIMD builds use
the existing runtime and CPU gates, with scalar fallback when native SIMD is
unavailable. NULL masks and scalar tails remain part of each kernel's contract.
A SIMD capability reports the selected backend, not a promise that every
short input executes vector instructions.

This is an independent implementation on the published baseline, not recovery
of an unpublished candidate. It does not add parser syntax, shifts, implicit
cross-type promotion or arbitrary-width integers.

## Validation and measurement

Use `scripts/validate.sh` for the repository's normal gates. Never force a SIMD
width above the host's detected native maximum. Cross-compilation validates
buildability, not native ARM execution. Any missing original fixture remains
an explicit limitation rather than a full-suite pass.

`BenchmarkBitwiseExpression` compares scalar and auto mode with identical
programs and batches. It includes Workspace evaluation, validity handling,
literal materialization where applicable and result export. Setup and
compilation are outside timing. It reports allocations and retained workspace
bytes as well as elapsed time. Because the baseline has no bitwise API, these
are within-candidate scalar/SIMD measurements, not an old-versus-new speedup.
