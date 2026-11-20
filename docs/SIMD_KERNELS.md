# SIMD kernel dispatch

GoLume uses Go 1.27 `simd` and `simd/archsimd` behind `kernel.Set`.
The supported build entry, `./scripts/build.sh`, defaults an unset
`GOEXPERIMENT` to `simd` and builds `./...` when given no arguments. It forwards
explicit build arguments and honors an explicitly set experiment value,
including `GOEXPERIMENT=`, without changing persistent `go env` settings.
Direct `go build`/`go test` commands still require `GOEXPERIMENT=simd` to include
SIMD implementations. SIMD vector types never leave the kernel package. Go 1.27 alone does not
enable the experiment (`go.mod` requires Go 1.27.0). The shared native guard
requires non-emulated SIMD and, on amd64, AVX2 even at 128 bits because the
current experimental lowering uses AVX2 broadcasts.

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

- Portable native SIMD comparisons cover all ten primitive numeric types
  (Int8/16/32/64, Uint8/16/32/64, Float32/64), both literal and column-to-column,
  for `GT/GE/LT/LE/EQ`. Float32/Float64 Add/Sub/Mul/Div and dense/masked
  Abs/Sqrt also have native paths.
- arm64 retains 128-bit `archsimd-arm64-mask` overrides for literal Int32
  GT/GE/LT/LE and Uint32 GT/GE/LT/LE/EQ. Int32 EQ uses portable SIMD.
  Auto Hash64 remains a non-SIMD scalar MUL unroll.
- amd64 replaces 50 literal and 50 column comparison entries with direct
  mask-to-bitmap kernels at the exact selected runtime width: 128/256 bits
  require AVX2; 512 bits additionally requires `archsimd.X86.AVX512()`.
  Width/feature mismatch cannot force unsupported instructions.
- Checked Int32/Uint32/Int64/Uint64 Add/Sub/Mul and checked Int64 scale have
  generated amd64 128/256/512-bit and arm64 128-bit native implementations.
  First-overflow and alias semantics remain checked; tails and unsupported
  paths retain scalar work. This is not native coverage for every integer op.
- Widening and mixed arithmetic to Float64 have architecture-specific paths.
  amd64 uses 256-bit conversion kernels when the selected width is at least
  256; Int64/Uint64 conversion additionally requires AVX-512. arm64 supplies
  128-bit conversion paths. Float32 expression arithmetic still returns
  Float64, but eligible paths now use widening kernels rather than being
  universally scalar. Query `Float64WideningCapability` for the source type.
- Masked integer SUM and numeric MIN/MAX have native implementations. Narrow
  integer SUM widens to Int64; Float32/Float64 SUM remains row-ordered scalar.
  Global Float64 AVG batching is not a SIMD reduction, and exceptional IEEE
  inputs/state use the original row updater. See the operation matrix.
- amd64 Hash64 and 32/64-bit numeric compaction (including Float32) require
  AVX-512 and selected width at least 512. Byte/word compaction additionally
  requires VBMI2. Other architectures/widths retain scalar compaction; no AVX2
  Hash64 multiply emulation is installed.
- Native implementation availability is not a whole-query speedup guarantee.
  See the [operation matrix](SIMD_OPERATION_MATRIX.md) for explicit boundaries.

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
or scan lanes. The 300 generated width/type/operation variants share one generator
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
supported. These arithmetic kernels do not allocate heap scratch buffers. Separate
widening kernels serve Float64-result expression paths.
Float comparison predicates are ordered: every predicate is false for NaN,
including EQ, while positive and negative zero compare equal. NaN arithmetic
payload identity is not part of the contract.

Checked integer kernels preserve exact first-overflow rows with native and
scalar paths. Int64 scale retains factor special cases and checked bounds.
Float64 arithmetic preserves IEEE NaN, infinity, and signed-zero semantics;
this does not promise one universal NaN payload across platforms or builds.

## Operation-level reduction benchmarks

`BenchmarkKernelID` executes every registered SUM and MIN/MAX reduction using
one 64-row dense mask per invocation, reported as `64 rows/op`. Other families
retain their 65,536-row fixture. Integer inputs are 0 through 63; floating-point
inputs are 1 through 64. SUM preserves narrow-to-Int64 widening and native-width
wrapping; MIN/MAX retains both return values, including exact floating-point
bits. An independent row-by-row oracle checks both scalar and forced-SIMD
outputs before timing, in addition to the cross-backend equality check.

The earlier unified benchmark omitted the reduction dispatch branch. Its
`sum-*-masked` and `minmax-*-masked` samples measured no kernel work and are
invalid performance evidence. Do not compare those samples with corrected
results or infer a regression from their lower timings. Dedicated masked SUM
and MIN/MAX benchmarks are separate and were not affected by this omission.

This repair changes test/benchmark code only. It grants no new statistical
approval and does not change production dispatch. The dense 64-row workload
does not establish performance for sparse masks, other sizes, or full queries.
Use the dedicated family benchmarks for broader shapes and report the build,
CPU, runtime SIMD width, and actual workload with fresh measurements.

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
candidates. This is deletion history, not the current coverage boundary: later generated
comparison, checked arithmetic, scale and widening implementations described
above supersede several of those former gaps.

The historical arm64 UTF-8 candidate improved long ASCII by 90.28% but
regressed the mixed-Chinese expression workload by 4.12% (`p=0.004`). It was
deleted. UTF-8 Length keeps scalar single-pass `DecodeRune` semantics for
ASCII, Unicode, and malformed input; dictionary predicate caching remains.

Ordering, Top-K heap, window execution, substring matching, random hash-table
probes, opaque Arrow operations, and random gathers have no general native
SIMD implementation. Native-preferred selection does not claim these loops,
all math operations, or an entire query are vectorized.

## Numeric native expansion validation

The historical numeric stage artifact `validation/numeric-native/README.md` records
native amd64 128/256/512-bit execution, runtime-disabled fallback, dispatch
identity checks, IEEE/boundary tests, allocations, and complete query measurements.
Linux/arm64 validation for this stage is cross-compilation only, not native execution.
Kernel coverage does not imply whole-query acceleration; measured query timings
are mixed and managed-memory/heap allocation comparisons are reported separately.

## Comparison bitmap validation

The historical bitmap stage artifact `validation/bitmap-native/README.md` separates
independent native safety checks from paired before/after Auto benchmarks.
That historical stage improved 55 comparison dispatch entries; the current
generated backend contains 100 entries across three widths (300 variants),
not 300 distinct public operations.
That report records checks of all three amd64 widths and CPU-feature-masked
fallback paths for its recorded revision, not a fresh run of this candidate.
arm64 is cross-compiled only, and its literal overrides remain unchanged.

## Narrow masked MIN/MAX

`MinMaxID` and `Set.MinMax{Int8,Int16,Uint8,Uint16}Masked` expose bounded,
allocation-free extrema alongside the 32/64-bit reducers. Inputs are limited to
64 rows, high selection bits are ignored, and an empty selection returns zero
extrema. Native kernels widen exact, in-bounds typed loads into Int32 registers,
where all narrow signed and unsigned values are representable, then use vector
min/max and horizontal reduction. Sparse masks and scalar tails preserve exact
selection semantics. AMD64 extracts both results before clearing upper AVX bits.

Dispatch remains behind existing CPU and SIMD-width guards. The four registry
entries are candidates, not statistical-gate approvals. ARM64 has been
cross-compiled only. Engine batch support preserves narrow output types and
promoted Int64 sums without changing the pure-Int64 legacy query boundary.

## Scope of this documentation audit

The current dispatch descriptions above were checked against the installers in
`kernel/simd_go127.go`, `simd_guard_amd64.go`, generated comparison/checked/scale/
widening files, masked reduction installers, and hash/compaction installers.
This documentation-only review executes no tests or benchmarks. Historical
reports are revision-specific evidence, not proof of current native ARM64 or
hosted CI execution. Cross-compilation cannot establish native behavior.

The historical `validation/numeric-native/README.md` and
`validation/bitmap-native/README.md` artifacts are not included in this frozen
source package. Their paths above are historical references, not available
links or raw-data evidence for this review. Historical measurement figures
are retained as previously documented, not newly reproduced here.
