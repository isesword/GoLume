# Independent numeric SIMD review

## Outcome

No remaining correctness defect was found in the reviewed implementation. The
review covered the generated kernels, registration order, bitmap/null handling,
IEEE behavior, integer signedness, and dispatch capability/function consistency.
A dispatch-test blind spot was reported and fixed: native capability flags are
now checked against actual function identity rather than trusted on their own.

## Final short-input re-review

The final generator adds early scalar fallback for inputs shorter than one
runtime SIMD vector. All 40 generated literal/Float64-column comparison guards
were inspected: each calls the matching scalar operation before bitmap/vector
setup, and column length validation remains before the fallback. No semantic
or dispatch defect was found. Independent probes were rebuilt and rerun after
this change at emulated, 128-, 256-, and 512-bit settings; all passed.

The rebuilt kernel test executable also passed
TestFloat64NativeComparisonModesTailsAndNulls,
TestNativeFloatArithmeticBitPatterns, TestExpandedNumericNativeDispatch and
TestFloat32NativeArithmeticModesTailsAndNulls at each of those four settings.
Refreshed disassembly contains the short Float64 scalar-call branches and the
same native packed instructions for full vectors. This review did not rerun
performance benchmarks or infer a speedup from those correctness checks.

## Scope and semantic boundaries

- Newly selected portable kernels: Float64 Add; Float64 literal and column
  GT/GE/LT/LE/EQ; Float32 Add/Sub/Mul/Div; Int8/Int32/Int64/Uint32/Uint64 literal
  GT/GE/LT/LE/EQ. Existing arm64 Int32/Uint32 overrides remain later in the
  registration order; Int32 EQ receives the portable implementation.
- Int16/Uint8/Uint16 literal comparisons and the remaining integer column
  comparisons stay scalar. Checked integer arithmetic and overflow behavior
  are unchanged.
- Float32 arithmetic kernels are available through kernel.Set. Ordinary
  Float32 expressions still promote to Float64 in CommonNumericType and are
  evaluated with their existing conversion semantics; this stage is not an
  end-to-end Float32 expression arithmetic SIMD integration.
- All five floating comparisons are ordered: a NaN operand produces false,
  while positive and negative zero compare equal. NaN remains a valid value;
  validity bits, not NaNs, represent nulls.
- Floating arithmetic preserves the scalar result bits for non-NaNs, including
  signed zero, gradual underflow/subnormals, and infinities. Invalid output
  lanes become positive zero. Exact output aliasing of either input is covered
  by repository tests; partial overlap and bitmap/output aliasing are not a
  newly added guarantee.
- NaN payload/sign bits are not a required arithmetic parity contract. The
  isolated direct SIMD emulation probe produced 20 payload differences while
  matching NaN classification. Normal Set dispatch avoids emulated SIMD and
  retains scalar functions when simd.Emulated() is true.

## Independent execution evidence

Host: linux/amd64, Go 1.27.3. Both probes passed with GODEBUG=simd=0,+128,+256,+512.
See review-independent-probes.txt and the retained probe sources.

1. A 65,536-row floating probe covers the Cartesian product of signed zeros,
   subnormals, normal boundaries, finite extrema, infinities, quiet/signaling
   NaNs, then deterministic random bit patterns. Float32 Add/Sub/Mul/Div and
   Float64 Add match scalar non-NaN result bits. Float64 ordered comparisons
   match scalar Boolean results. Native widths also happened to match NaN bits
   on this host, but no cross-architecture payload promise is inferred.
2. An independent scalar Go oracle checks actual kernel.Set integer dispatch
   for all five integer types and all five comparisons at lengths 0 through
   139, including unaligned input subslices, every bitmap tail, nil/all/mixed/
   all-null validity, dirty output, preserved suffix sentinels, extrema and
   unsigned sign-bit boundaries. Scalar, Auto and SIMD modes all match.
3. Compiled native disassembly confirms packed instructions at 128/256/512:
   VADDPS and VDIVPS for Float32, VADDPD for Float64 Add, ordered VCMPPD for
   Float64 comparisons, VPCMPGTB for Int8, and unsigned SIMD comparisons using
   XOR-sign plus VPCMPGTQ at 128/256 or VPCMPUQ at 512. See
   review-amd64-objdump.txt. The 128/256 unsigned sequence is vector arithmetic,
   not a scalar lane implementation.

The probes are review checks, not performance benchmarks. They do not establish
native arm64 execution or performance. Full repository, race, static and arm64
cross-compilation results are recorded separately by the implementation work.

## Reproduction

With Go 1.27 and dependencies available, from the repository root:

```sh
cp validation/numeric-native/review-simd-ieee-probe.go.txt /tmp/simd-ieee-probe.go
cp validation/numeric-native/review-integer-oracle.go.txt /tmp/simd-integer-oracle.go
for width in 0 +128 +256 +512; do
  GOEXPERIMENT=simd GODEBUG=simd=$width go run /tmp/simd-ieee-probe.go
  GOEXPERIMENT=simd GODEBUG=simd=$width go run /tmp/simd-integer-oracle.go
done
GOEXPERIMENT=simd go test -c -o /tmp/kernel-review.test ./kernel
go tool objdump -s 'gocolumn/kernel.simd(Float32(Add|Div)|Float64(Add|GT|ColumnsGT)|Int8GT|Uint64GT)@simd(128|256|512)$' /tmp/kernel-review.test
```

Only request vector widths supported by the target hardware. The recorded
128/256/512 execution was on the amd64 review host, not arm64.

## Reviewed production source SHA-256

- `internal/cmd/gensimd/main.go`: `add25d5473ce9aec1d5a74a305af918355026a59882727525b171e2fe8e32842`
- `kernel/zz_generated_simd.go`: `96d9a5bc9debe07597f0a5f116dcd0a99096fdeaef50f662d0cd987236ac671d`
- `kernel/scalar.go`: `d5eb6309902110cad1391bbd2760640e72299a255df41ff00a1195e6ba6d16b4`
