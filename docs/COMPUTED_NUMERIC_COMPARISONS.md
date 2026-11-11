# Computed numeric comparisons

The expression evaluator dispatches comparisons of computed numeric registers
through the same `kernel.Set` functions used for source-column comparisons.
This covers arithmetic, cast and conditional results without adding another
SIMD backend or changing expression typing.

## Supported fast path

- `GT`, `GE`, `LT`, `LE` and `EQ` between two dense registers with the exact same
  logical numeric type: Int8/16/32/64, Uint8/16/32/64 or Float32/64.
- A dense Int64 or Float64 register compared with a same-type compact literal,
  on either side. A left literal reverses the physical comparison operation.
  It does not allocate or materialize a broadcast vector.
- Plain builds and `ModeScalar` use the existing scalar kernels. With
  `GOEXPERIMENT=simd`, Auto and SIMD modes retain the kernel set's existing
  hardware guards, backend selection and short-input scalar tails. Calling a
  kernel does not imply that every row or every machine executes SIMD.

The comparison consumes only the logical row prefix. Both operand validity
masks are intersected before dispatch, including active masks inside IF/CASE
regions. The truth bitmap has zero bits for NULL and out-of-range tail rows;
SQL three-valued Boolean composition still uses the separate validity bitmap.
No new workspace storage is required.

## Unchanged fallback

The original row comparator still handles mixed numeric types, two compact
literals, Boolean, decimal, string and temporal domains. In particular, an
integer is not rounded to Float64 to compare it, signed/unsigned bounds remain
exact, and a Float64 literal is not narrowed to a Float32 result. Matching
Float32 vectors can compare directly because their comparison truth is the
same as widening both operands to Float64, including NaN and signed zero.

This is a new implementation on top of the published baseline, not a recovery
of an unpublished candidate. It does not fuse arithmetic with comparison or
change the order, error domain or memory ownership of operand evaluation.

## Verification and measurement

`numeric_comparison_test.go` checks direct dispatch for every numeric type,
all five operators, every length 0–129, unaligned slices, logical prefixes,
invalid rows, dirty tail bits, integer limits and IEEE special values. It also
checks literal orientation, fallback domains and allocation-free dispatch.
`computed_comparison_review_test.go` exercises real computed expressions,
selection, guarded branches, shared outputs, shrinking workspaces, exact
mixed-domain results and budget release.

`BenchmarkComputedNumericComparison` measures the public reusable Workspace,
including operand arithmetic/casts and validity handling. Use the same
benchmark file at the parent revision for a before/after comparison. Within
one revision, the `mode=scalar` and `mode=auto` variants separately show the
impact of selected kernels. Compare allocations and `retained-B` as well as
elapsed time. Input creation and compilation are outside the timed loop.

The repository's `scripts/validate.sh` remains the complete validation entry
point. Runtime-width tests must stay within the host's detected native maximum.
Cross-compilation is not evidence of native ARM execution. Any unavailable
original test fixtures or skipped tests must remain explicit in validation
reports rather than being reported as a full-suite pass.
