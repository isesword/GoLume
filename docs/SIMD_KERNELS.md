# SIMD kernel dispatch

GoLume uses Go 1.27 `simd` and `simd/archsimd` entirely behind `kernel.Set`.
Builds without `GOEXPERIMENT=simd` retain the complete scalar implementation;
SIMD vector types never leave the kernel package.

## Dispatch contract

- `ModeScalar` is the normative scalar baseline.
- `ModeSIMD` selects only SIMD implementations that still exist in production.
  A KernelID without a retained SIMD backend reports and executes scalar; it
  does not select a historical diagnostic implementation.
- `ModeAuto` selects only entries backed by at least ten same-build 1-second
  samples, at least 10% kernel improvement with `p < 0.05`, and no related
  end-to-end regression greater than 3%.
- `KernelCapability` reports the actual implementation. Architecture-unrolled
  scalar code uses `HardwareSIMD=false`, `VectorBits=0`, and an explicit backend
  name.

Run `GOEXPERIMENT=simd go run ./cmd/kernel-info -mode auto` to inspect every
KernelID's backend, vector width, hardware/emulation flags, and Auto/forced
state.

## Operation-specialized generation

`go generate ./kernel` invokes the standard-library-only generator at
`internal/cmd/gensimd`. It emits concrete comparison, arithmetic, and string
length functions without operation parameters, closures, callbacks, or
loop-local operator switches. `go run ./internal/cmd/gensimd -check` is part of
the structural validation and rejects generated drift.

arm64 comparison kernels broadcast the literal outside the loop, use
`archsimd` lane extraction to build each 64-row word directly, combine validity
once, and count the final word once. They do not store masks to a temporary
array for a second lane scan. DirectFused binds the typed comparison at batch
entry and reuses one 64-row transient word.

Checked integer arithmetic is operation-specialized scalar code with exact
first-overflow rows. Int64 scale has dedicated factor `0`, `1`, and `-1` paths
and precomputed legal bounds for other factors; it no longer divides the
product to detect overflow. Float64 preserves IEEE NaN, infinity, and signed
zero behavior.

## darwin/arm64 Auto allowlist

The 2026-08-29 Apple M4 Pro gate approves:

- Int32 `GT/GE/LT/LE` using `archsimd-arm64-mask` (128-bit).
- Uint32 `GT/GE/LT/LE/EQ` using `archsimd-arm64-mask` (128-bit).
- Float64 `Sub/Mul/Div` using `portable-simd` (128-bit on this host).
- Hash64 using `arm64-unrolled`, a four-way native scalar `MUL` pipeline. It is
  deliberately reported as non-SIMD.

The comparison kernel candidates improve 36.22% to 72.38%; Float64 operations
improve 19.30% to 19.61%; Hash64 improves 31.91%. All have ten 1-second samples,
`p=0.000`, and zero kernel allocations. The Float64 expression workload
improves 17.57% (`p=0.000`).

For Int32 GT, 1M-row DirectFused improves 37.09%, 20.09%, 7.18%, 5.57%, and
6.22% at 1/10/50/90/100% selectivity. BitmapDirect improves 30.62%, 16.43%,
12.34%, 16.29%, and 16.30%. Comparing the two SIMD strategies, BitmapDirect
clears 10% only for the 10/50/90/100% buckets, so planner approvals are limited
to those exact `(arm64, 128, compare-int32-gt, bucket)` entries. Their strategy
gains are 12.05%, 11.66%, 12.23%, and 11.92%. The 1% bucket remains
DirectFused because its strategy gain is only 4.40%.

With the final Auto allowlist active, the 1M prepared Parquet radix aggregate,
Prepared Lazy equivalent, and prepared Radix Join improve by approximately
0.52%, 1.47%, and 0.53%; allocations are unchanged. Raw samples, benchstat CSV,
and the complete decision record are under `validation/` and indexed by
`validation/simd-auto-gates.json`.

Run `scripts/bench_hotpaths.sh` to reproduce the focused 1-second x 10 kernel,
Hash, UTF-8, expression, DirectFused, BitmapDirect, and strategy reports. Use
`scripts/bench_e2e.sh` for the Parquet/Lazy/Radix/Join regression set.

## Deleted candidates

The deletion list below describes the generated column-scalar/older kernel
refactor. The later forced-only Int32/Uint32/Float32 column-column prototypes
still use a runtime operator switch and a stored-mask lane scan. They are not
Auto-admitted and must pass their own operation and query gates; the list is
not a claim that every current candidate has the generated implementation shape.

The following failed production implementations were deleted rather than
retained behind forced or diagnostic dispatch:

- generic comparison closures and runtime comparison/arithmetic switches;
- mask stores followed by temporary-array lane scans;
- Int64/Uint64 comparison SIMD candidates;
- checked integer SIMD candidates and per-lane overflow rescans;
- arm64 widening and amd64 AVX2 emulation of 64-bit multiplication for Hash and
  Scale;
- Hash128 intermediate-input arrays and the slower unrolled Hash128 experiment;
- portable SIMD Bool AND/OR/NOT;
- UTF-8 validation-plus-continuation double scans and the arm64 ASCII-prefix
  candidate.

The final arm64 UTF-8 candidate made long ASCII 90.28% faster but regressed the
complete mixed-Chinese expression workload by 4.12% (`p=0.004`). Because this
exceeded the 3% end-to-end limit, its generator output, registration, and helper
were deleted. UTF-8 Length now uses the single-pass scalar `DecodeRune`
semantics for ASCII, Unicode, and malformed input. Dictionary predicate caching
remains the primary string optimization.

The amd64 Hash/compact candidates use AVX-512 native 64-bit multiply or Compress
in `ModeSIMD`. Separate portable column-column prototypes also remain forced-only.
The amd64 Auto allowlist remains entirely scalar until the relevant complete
admission gates pass. AVX2 simulated multiplication is not present.

Ordering, Top-K heap, window execution, substring matching, random hash probes,
opaque Arrow values, and random low-density gathers remain scalar by design.
