# SIMD kernel dispatch

GoLume uses Go 1.27 `simd` and `simd/archsimd` behind `kernel.Set`.
The supported build entry, `./scripts/build.sh`, defaults an unset
`GOEXPERIMENT` to `simd` and builds `./...` when given no arguments. It forwards
explicit build arguments and honors an explicitly set experiment value,
including `GOEXPERIMENT=`, without changing persistent `go env` settings.
Direct `go build`/`go test` commands still require `GOEXPERIMENT=simd` to include
SIMD implementations. SIMD vector types never leave the kernel package.

## Dispatch contract

- `ModeAuto` is the default and prefers existing native SIMD implementations
  wherever the build, runtime hardware, and selected vector width support them.
  Native SIMD is no longer withheld pending a per-operation performance gate.
- `ModeScalar` explicitly selects the complete scalar reference implementation
  for correctness comparisons and diagnostics.
- `ModeSIMD` explicitly selects available native SIMD for diagnostics. It keeps
  the same hardware and emulation guards as Auto; it cannot force unsupported
  instructions or restore deleted candidates.
- Builds without the SIMD experiment, emulated/disabled runtimes, and operations
  without a retained native implementation use scalar. Short inputs, vector
  tails, and nullable/selected math may also execute scalar for correctness.
- `KernelCapability` reports the selected implementation's vector path, not a
  promise that every call vectorizes. Architecture-unrolled scalar code has
  `HardwareSIMD=false`, `VectorBits=0`, and an explicit backend name.
- Planner bitmap/direct strategy approvals are independent of kernel selection.
  Their measured decision policy and existing approvals are unchanged.

Run `GOEXPERIMENT=simd go run ./cmd/kernel-info -mode auto` (or add `-json`) to
inspect selected backends, vector widths, hardware/emulation flags, available
explicit-SIMD backends, and Auto selection. The JSON `kernels` array preserves
existing KernelIDs. Literal comparisons remain `compare-<type>-<op>`;
column-to-column comparisons have a separate `column_comparisons` array and
`compare-columns-<type>-<op>` report IDs. The table prints both groups.
The column rows come from `Set.ColumnComparisonCapability(type, op)` /
`Capabilities.ForColumns(type, op)`, not literal comparison capabilities.
Physical temporal aliases use their Int32/Int64 storage rows.

For compatibility, `auto_admitted` / table `auto` means Auto selects a
non-`scalar` backend. It is not a benchmark-approval or hardware-SIMD flag;
arm64's `arm64-unrolled` Hash64 is non-SIMD and is retained only in Auto, so
its Auto backend can differ from the explicit-SIMD `available_backend`.

## Current native coverage

- Portable native SIMD in Auto and explicit SIMD: literal `GT/GE/LT/LE/EQ`
  for Int8/Int32/Int64/Uint32/Uint64/Float32/Float64; column comparisons for
  Int32/Uint32/Float32/Float64; Float32/Float64 `Add/Sub/Mul/Div`; dense
  Float32/Float64 `Abs/Sqrt`.
- arm64 retains `archsimd-arm64-mask` overrides for literal Int32
  `GT/GE/LT/LE` and Uint32 `GT/GE/LT/LE/EQ`, at 128 bits. Int32 literal EQ now
  uses portable SIMD. The existing Auto Hash64 unroll remains a scalar MUL pipeline.
- Float32 arithmetic kernels preserve native Float32 precision, but ordinary
  expression arithmetic still promotes Float32 operands to Float64. This stage
  does not change result types or claim SIMD acceleration of that expression path.
- Int16/Uint8/Uint16 literal comparisons and Int8/Int16/Int64/Uint8/Uint16/Uint64
  column comparisons remain scalar. Checked integer arithmetic is unchanged.
- amd64 replaces these 35 literal and 20 column comparison entries with
  generated direct mask-to-bitmap kernels at the exact selected runtime width:
  128-bit AVX, 256-bit AVX2, or 512-bit AVX512 (F+CD+BW+DQ+VL). Unsupported
  widths/features retain the portable implementation. `archsimd-amd64-mask`
  reports this backend independently of arithmetic and hash/compaction.
- amd64 Hash64 and Int32/Int64/Uint32/Uint64/Float64 compaction use AVX-512
  only when the runtime reports AVX-512, portable SIMD is non-emulated, and
  the selected native width is at least 512 bits. Narrower widths fall back
  to scalar for these operations; there is no AVX2 multiply emulation.
- Other current operations remain scalar. See the
  [operation matrix](SIMD_OPERATION_MATRIX.md) for the explicit boundaries.

## Operation-specialized generation

`go generate ./kernel` invokes the standard-library-only generator at
`internal/cmd/gensimd`. It emits concrete literal comparison, arithmetic,
and string length functions without operation parameters, closures, callbacks,
or loop-local operator switches. `go run ./internal/cmd/gensimd -check` is
part of structural validation and rejects generated drift.

arm64 literal comparison kernels broadcast once outside the loop, use
`archsimd` lane extraction to build each 64-row word, combine validity once,
and count the final word once. DirectFused binds the typed comparison at batch
entry and reuses one 64-row transient word. The separately maintained
Int32/Uint32/Float32 column-comparison implementations still use an operator
switch and a stored-mask lane scan. Selecting those native implementations
in Auto does not imply they have the generated literal-kernel shape.

amd64 comparison masks are widened to uint64 before shifting into the output
word, avoiding narrow-mask truncation. They do not store masks to scratch arrays
or scan lanes. The 165 generated width/type/operation variants share one generator
and are selected once during Set installation, not through a per-vector type switch.
Scalar fallback below one vector is preserved. This adds fixed generated code;
it does not add per-call heap buffers, and is not a claim about total process RSS.

Portable column32 comparisons now combine both validity bitmaps once per output
word, then mask the result. All-null words can skip value loads; output is cleared
first, so skipped words cannot retain stale bits. The portable fallback is tested
directly in addition to testing the selected architecture backend.

Portable literal and generated Float64 column comparisons use bounded stack
mask storage and combine validity once per bitmap word. Arithmetic writes its
caller-owned output, then zeros invalid rows; both exact input aliases are
supported. There are no heap scratch buffers or widened Float32 arithmetic.
Float comparison predicates are ordered: every predicate is false for NaN,
including EQ, while positive and negative zero compare equal. NaN arithmetic
payload identity is not part of the contract.

Checked integer arithmetic is operation-specialized scalar code with exact
first-overflow rows. Int64 scale has dedicated factor `0`, `1`, and `-1` paths
and precomputed legal bounds for other factors. Float64 preserves IEEE NaN,
infinity, and signed-zero behavior.

## Historical performance evidence

The following records describe earlier versions and the former measured-only
Auto policy. They are not new measurements of the native-preferred policy,
not new native arm64 validation, and not blanket speedup guarantees.

The 2026-08-29 Apple M4 Pro gate approved Int32 literal `GT/GE/LT/LE`, Uint32
literal `GT/GE/LT/LE/EQ`, Float64 `Sub/Mul/Div`, and the non-SIMD Hash64 unroll.
Comparison candidates improved 36.22% to 72.38%; Float64 operations improved
19.30% to 19.61%; Hash64 improved 31.91%. These used ten 1-second samples,
`p=0.000`, and zero kernel allocations. The Float64 expression workload
improved 17.57% (`p=0.000`).

For historical Int32 GT, 1M-row DirectFused improved 37.09%, 20.09%, 7.18%,
5.57%, and 6.22% at 1/10/50/90/100% selectivity. BitmapDirect improved 30.62%,
16.43%, 12.34%, 16.29%, and 16.30%. Comparing the two SIMD strategies,
BitmapDirect cleared 10% only for 10/50/90/100%, producing the existing
`(arm64, 128, compare-int32-gt, bucket)` planner approvals. Their strategy
improvements were 12.05%, 11.66%, 12.23%, and 11.92%; the 1% bucket remained
DirectFused (4.40%). These planner approvals are unchanged by native-preferred
kernel dispatch.

With that historical Auto allowlist, the 1M prepared Parquet radix aggregate,
Prepared Lazy equivalent, and prepared Radix Join improved approximately
0.52%, 1.47%, and 0.53%; allocations were unchanged. Raw samples, benchstat
CSV, and decisions are indexed by `validation/simd-auto-gates.json`.
Run `scripts/bench_hotpaths.sh` for focused kernel/expression/strategy reports
and `scripts/bench_e2e.sh` for Parquet/Lazy/Radix/Join regression measurements.
Fresh results must identify their actual build, host, and workload.

## Deleted implementations and remaining scalar work

The older generated literal-kernel refactor deleted generic comparison
closures/runtime switches, stored-mask lane scans in those kernels,
Int64/Uint64 comparison SIMD, checked integer SIMD with overflow rescans,
arm64 widening/amd64 AVX2 multiply emulation, Hash128 intermediate arrays and
unrolled experiments, portable SIMD Boolean operations, and UTF-8 double-scan
candidates. This history does not describe the separate current column kernels.

The historical arm64 UTF-8 candidate improved long ASCII by 90.28% but
regressed the mixed-Chinese expression workload by 4.12% (`p=0.004`). It was
deleted. UTF-8 Length keeps scalar single-pass `DecodeRune` semantics for
ASCII, Unicode, and malformed input; dictionary predicate caching remains.

Ordering, Top-K heap, window execution, substring matching, random hash-table
probes, opaque Arrow operations, and random gathers have no general native
SIMD implementation. Native-preferred selection does not claim these loops,
all math operations, or an entire query are vectorized.

## Numeric native expansion validation

The [numeric stage report](../validation/numeric-native/README.md) records
native amd64 128/256/512-bit execution, runtime-disabled fallback, dispatch
identity checks, IEEE/boundary tests, allocations, and complete query measurements.
Linux/arm64 validation for this stage is cross-compilation only, not native execution.
Kernel coverage does not imply whole-query acceleration; measured query timings
are mixed and managed-memory/heap allocation comparisons are reported separately.

## Comparison bitmap validation

The [bitmap stage report](../validation/bitmap-native/README.md) separates
independent native safety checks from paired before/after Auto benchmarks.
The native operation counts are unchanged; this stage improves 55 existing
comparison dispatch entries rather than claiming 165 new public operations.
All three amd64 widths and CPU-feature-masked fallback paths were checked.
arm64 is cross-compiled only, and its literal overrides remain unchanged.
