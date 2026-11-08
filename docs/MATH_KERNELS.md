# Native math kernels

Math operations use the immutable `kernel.Set` and have individual kernel IDs,
capabilities, and registry entries. Integer operations never detour through
floating-point storage. All ten numeric widths have scalar reference backends.

## Semantics

- Abs, round, floor, and ceil retain the input type. Integer round/floor/ceil are
  identities. Round means zero decimal places with ties to even.
- Signed absolute value of the minimum value wraps to the same minimum value.
  Integer power and minimum-integer division by -1 also wrap, matching the
  pinned Polars 1.35.2 oracle for these newly introduced operations. This does
  not change the error behavior of existing add/subtract/multiply expressions.
- Integer modulo follows the divisor's sign; floor division rounds down. A zero
  integer divisor marks a failing row, allowing the expression layer to emit
  NULL. Negative integer power exponents mark failure for the expression layer
  to report a semantic error. Invalid/inactive input rows never mark failures.
- Float32 and Float64 sqrt/log use IEEE NaN/infinity domains. Sqrt preserves
  negative zero; log of either signed zero is negative infinity. Floating
  floor division is floor(a/b), including IEEE division by zero.
- Floating modulo uses `math.Mod` with a divisor-sign correction. Exact zero
  remainder has the divisor's zero sign. Infinite divisors produce NaN, as in
  the pinned oracle. This intentionally avoids Polars' cancellation-prone
  formula: GoLume returns 1 for 1e20 % 3 and 2 for -1e20 % 3, whereas the pinned
  Polars version returns zero for both. The accurate remainder is tested
  explicitly and is not presented as bitwise Polars compatibility.
- Float32 transcendental reference evaluation uses Go math at float64 precision
  and rounds to float32 storage. Pow uses math.Pow, not exp(y*log(x)). SIMD
  Abs/Sqrt preserve finite results and signed-zero semantics; active NaNs are
  compared by NaN class, not payload or signaling-bit identity, as in the
  existing dense SIMD path. Invalid outputs are always positive zero.
- The caller supplies the combined validity/selection bitmap. Invalid lanes
  are zeroed and not evaluated. Output buffers and failure masks are supplied
  by the caller; kernels allocate no memory. A nil validity mask means dense.

## SIMD dispatch and historical evidence

Portable native SIMD for dense and nullable/selected Float32/Float64 abs and sqrt
is selected by both default `ModeAuto` and diagnostic `ModeSIMD` on SIMD-enabled,
non-emulated runtimes. `ModeScalar` remains the explicit reference. The supported
`./scripts/build.sh` defaults an unset `GOEXPERIMENT` to `simd`; direct Go
commands must opt into the experiment themselves.

Nullable/selected batches expand packed validity bits in vector registers and
replace inactive inputs with positive zero before Abs/Sqrt; invalid negative
values and NaNs never reach the floating operation. All-invalid words are
cleared without reading input, and all-valid words use the dense loop. Nullable
short inputs and tails use one fixed 64-byte stack buffer, bounded integer-bit
copies, and the same vector input mask. This avoids the experimental toolchain
partial-load boundary issue without allocating row-sized scratch. Dense
(nil-mask) tails retain scalar reference execution. The kernels use no
row-sized scratch or heap allocations, support exact in-place output, and leave
the output suffix untouched. Diagnostics describe a selected backend, not a
promise that every invocation vectorizes. Other math operations remain scalar because no retained native SIMD implementation exists.
Kernel Auto selection no longer waits for historical performance admission;
planner strategy decisions retain their separate measurement policy.

There are no approximate pow/log replacements, fake vectorized integer
division, or inaccurate modulo rewrites. Floor/ceil/round remain scalar because
the portable SIMD API has no matching operations; architecture-specific
implementations require separate evidence.

The historical initial linux/amd64 measurement used Go 1.27.3, GOMAXPROCS=1, 65,536 dense
Float64 values, ten 1-second samples per case. Raw output is retained in
`validation/fundamentals/math-kernel-amd64.txt`. Scalar/forced medians were:

| Operation | Scalar ns/op | Forced SIMD ns/op | Ratio |
| --- | ---: | ---: | ---: |
| abs | 81,925 | 15,619 | 5.25x |
| sqrt | 393,602.5 | 41,063.5 | 9.59x |

Both measured paths reported zero allocations. These are historical candidate
measurements, not a remeasurement of the native-preferred default or a claim
about other batch sizes, null densities, Float32, or other architectures.
Those workloads require their own correctness and performance measurements.

## Expression integration

The public expression constructors are `Abs`, `Round`, `Floor`, `Ceil`, `Sqrt`,
`Log`, `Mod`, `FloorDiv`, and `Pow`, with corresponding fluent methods on numeric
expression nodes. For example, `Col("x").Abs().Sqrt()` and
`Col("u").Mod(I64(2))` compile through the same typed kernel set.

Same-type operands retain that type. Mixed unsigned integers promote to the
widest unsigned input. An integral literal that fits the other operand's type
is converted to that native width. Other mixed operands use existing numeric
promotion, except mixed UInt64/signed columns are rejected rather than silently
losing integer precision: callers must explicitly choose a conversion. Sqrt and
log promote integral input to Float64 and retain Float32/Float64 input.

Integer modulo and floor division are nullable even with non-null inputs.
Negative signed integer exponents are marked as potentially erroring for the
optimizer. Guarded If/Case evaluation skips inactive error rows. Input casts
remain ordinary compiled expression nodes, so their storage, liveness,
nullability and error effects follow existing contracts. Math uses existing
Unary/Binary instructions, whose dependency and owner-lifetime analysis already
covers all inputs. Execution checks cancellation between 4096-row kernel chunks.

The round API currently takes no decimal-place parameter; it rounds to integral
values while retaining the input dtype. This is not a claim of supporting every
Polars rounding mode or decimal precision option.

Expression workspace measurements (same rows/sample policy) are retained in
`validation/fundamentals/math-expression-amd64.txt`. Median scalar/forced times:

- abs: 133,147.0 versus 21,965.5 ns/op (6.06x).
- sqrt: 407,432.5 versus 39,263.5 ns/op (10.38x).

Both expression modes use 64 B/op and 1 allocation/op for the existing exported
result view. Measurements were taken on a shared virtualized host. They do not
prove performance of the current default, small-batch/null-heavy workloads, or
native arm64 execution. The current Auto selection is a native-preferred policy,
not a new benchmark result or a blanket speedup guarantee.
