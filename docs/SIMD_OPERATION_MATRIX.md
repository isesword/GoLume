# SIMD Operation Matrix

This matrix describes current dispatch, not projected coverage or performance.
Refresh it whenever a kernel or production execution path changes.

The supported Go 1.27 build entry is `./scripts/build.sh`. It defaults an unset
`GOEXPERIMENT` to `simd`, honors explicit overrides (including an empty value),
and changes no persistent Go settings. Direct Go commands still need the
experiment explicitly enabled. `ModeAuto` prefers retained native SIMD wherever
available; `ModeScalar` is the reference and `ModeSIMD` is diagnostic selection.
Runtime emulation/disablement and hardware/vector-width guards remain in force.
On amd64 the common native guard requires AVX2 even for 128-bit execution;
512-bit comparison overrides additionally require the AVX-512 feature predicate.

The former P0 baseline was `865f8dd99ccfdb8deaa53584f98577be720ba554` on Go
1.27.0, darwin/arm64, Apple M4 Pro. Its test and benchmark records are historical,
not verification of the current policy change. New native arm64 measurements
and a blanket speedup claim are not supplied by this matrix.

## Reproduction

```sh
go version
go env GOOS GOARCH GOEXPERIMENT
./scripts/build.sh
GOEXPERIMENT= ./scripts/build.sh
GOEXPERIMENT= go test ./kernel ./cmd/kernel-info ./memory ./expr ./engine ./physical
GOEXPERIMENT=simd go test ./kernel ./cmd/kernel-info ./memory ./expr ./engine ./physical
GOEXPERIMENT=simd go run ./cmd/kernel-info -mode scalar
GOEXPERIMENT=simd go run ./cmd/kernel-info -mode auto -json
GOEXPERIMENT=simd go run ./cmd/kernel-info -mode simd
GOEXPERIMENT=simd GODEBUG=simd=0 go test ./kernel ./cmd/kernel-info ./expr ./engine ./physical
```

These are commands to run, not a declaration that every check above passed.
The CLI separates literal KernelIDs (`kernels`) from column-comparison reports
(`column_comparisons`). `compare-int32-gt` and `compare-columns-int32-gt` may
select different backends. Programmatic callers use `Capability(id)` for
literal comparisons and `ColumnComparisonCapability(type, op)` for columns.

## Current operation matrix

All native entries below require a SIMD-enabled, non-emulated runtime. Short
inputs and vector tails may execute scalar. Nullable/selected Abs/Sqrt masks
inactive inputs to positive zero before native floating operations.

| Family | Physical operations | Auto native backend / guard | Remaining scalar boundary |
|---|---|---|---|
| Literal comparison | All ten primitive numeric types GT/GE/LT/LE/EQ | portable SIMD; amd64 direct masks at exact width; arm64 Int32/Uint32 literal overrides | unavailable native runtime, short inputs/tails |
| Column comparison | All ten primitive numeric types GT/GE/LT/LE/EQ | portable SIMD; amd64 direct masks at exact width | unavailable native runtime, short inputs/tails |
| Arithmetic kernels | Float32/Float64 Add/Sub/Mul/Div | portable SIMD | unavailable native runtime, tails |
| Widening / mixed arithmetic | Int8/16/32, Uint8/16/32, Float32 to Float64; Float32-pair arithmetic with Float64 result | amd64 selected width >=256, conversion uses 256 bits; arm64 128 | other widths/architectures, unsupported expression shapes, tails |
| Widening / mixed arithmetic | Int64/Uint64 to Float64 | amd64 selected width >=256 plus AVX-512; arm64 128 | same fallback boundaries; result remains Float64 |
| Checked arithmetic | Int32/Uint32/Int64/Uint64 Add/Sub/Mul | generated amd64 128/256/512 and arm64 128 | other ops/types, unsupported runtime, short/tail/alias fallback; exact overflow retained |
| Unary math | Float32/Float64 Abs/Sqrt | portable SIMD, dense and nullable/selected; mask before math | dense nil-mask tails, unavailable native runtime |
| Other math | integer math, Round/Floor/Ceil/Log/Mod/FloorDiv/Pow | none | typed scalar; no approximate replacements |
| Integer bitwise | Int32/Uint32/Int64/Uint64 AND/OR/XOR/NOT | portable SIMD, dense and nullable; expression/kernel entry points | unavailable native runtime, short/tails; other widths and shifts unsupported |
| Boolean | bitmap AND/OR/NOT | none | word scalar |
| Masked SUM | Int8/Int16/Uint8/Uint16 | architecture-specific widening, amd64 128/256/512 and arm64 128 | <=64 inputs, exact Int64 result; short/tails/sparse scalar work |
| Masked SUM | Int32/Uint32/Int64/Uint64 | portable SIMD, amd64 512-bit override when AVX-512 available | <=64 inputs, native-width wrapping; unsupported runtime/tails |
| Masked SUM | Float32/Float64 | none | ordered scalar floating accumulation |
| Masked MIN/MAX | All ten primitive numeric types | portable 32/64-bit reducers; architecture-specific narrow widening | <=64 inputs; empty mask, short/tails and IEEE handling retain reference semantics |
| Global Float64 AVG | engine batch updater, optional SUM/count lanes | not a SIMD reduction | original-order scalar math; nonfinite state/input or overflow replays original row lane |
| Hash | Hash64 batch | amd64 AVX-512, selected width >=512 | arm64 Auto has non-SIMD unroll; other cases scalar |
| Hash | Hash128 batch | none | scalar |
| Scale | checked Int64 scale | generated amd64 128/256/512 and arm64 128 | unsupported runtime, short/tails; checked factor special cases |
| String length / match | byte/UTF-8 length, equality/prefix/suffix/contains | none | scalar KernelID entries |
| Compact | Int32/Int64/Uint32/Uint64/Float32/Float64 | amd64 AVX-512, selected width >=512 | other architectures/widths, short inputs and operation-specific fallback |
| Compact | Int8/Int16/Uint8/Uint16 | same guard plus AVX512VBMI2 | other architectures/widths/features |
| Parquet decode / payload | typed decode, clone/materialize, key extraction | no general SIMD backend | typed scalar paths; not a kernel SIMD claim |
| Operator-owned loops | hash probes/state updates, merge/sort, Top-K, random gather | no general SIMD backend | algorithm/access-pattern work remains |

Both amd64 wide operations require `archsimd.X86.AVX512()`, non-emulated SIMD,
and a selected vector width of at least 512. AVX-512 hardware alone does not
permit a wide backend when the runtime selects 128 or 256 bits. There is no
AVX2 multiplication emulation. Auto's arm64 hash unroll is reported with
`hardware_simd=false` and `vector_bits=0`, not mislabeled as SIMD.

The integer bitwise expression type, NULL, literal and verification contracts
are documented in [INTEGER_BITWISE.md](INTEGER_BITWISE.md). New bitwise registry
entries remain candidates without historical performance-gate approvals; Auto
follows the native-preferred policy above, not those historical gate states.

## Correctness and evidence rules

- Report each operation and comparison shape independently. A literal
  capability is not evidence for its column-to-column counterpart.
- Auto selection expresses the native-preferred default, not a performance
  certification. `auto_admitted` retains its report name but means selection
  of a non-scalar-named backend; inspect `hardware_simd` for actual SIMD.
- `GODEBUG=simd=0` checks disabled-runtime scalar fallback correctness. The
  captured kernel capability reports do not establish an emulated SIMD backend,
  hardware SIMD availability, or performance.
- The planner's measured bitmap/direct strategy decisions are independent and
  unchanged. Existing strategy approvals still apply only to their recorded
  architecture, width, operation, and selectivity bucket.
- Historical gates used ten same-build one-second samples, at least 10%
  kernel improvement, `p < 0.05`, and no related end-to-end regression above
  3%. Those records remain useful evidence, not the current kernel admission
  policy. New performance claims need new comparable samples and the actual
  scan/expression/filter/aggregate or join/output path being claimed.

## Remaining optimization inventory

The separately maintained portable Int32/Uint32/Float32 column-comparison
kernels are selected in Auto; their operator switch and stored-mask lane scan
have not been rewritten as generated literal kernels. This does not describe
the generated column-comparison implementations for the other numeric types. Remaining operator-owned loops include hash aggregation probe
and state updates, join probes, and merge/sort. Native-width storage and batching
alone do not make these SIMD.

- Capture fresh CPU profiles and allocation/peak-memory measurements for
  numeric projection, filtering, aggregation, join, sort, strings, and Parquet.
- Keep the inventory of production loops bypassing `kernel.Set` current.
- For Polars comparisons, pin the revision and distinguish SIMD from batching
  and parallelism.
- Retain raw final-build kernel and end-to-end samples per target host before
  claiming performance gains or changing planner strategy approvals.

The amd64 comparison mask backend selects only the actual runtime width: AVX2
at both 128 and 256 bits, and the complete archsimd AVX512 feature group
at 512 bits. Feature/width mismatch keeps the portable backend. Native column
and literal masks are packed directly; portable column32 validity is applied
per word rather than per lane. The historical artifact path is `validation/bitmap-native/README.md`; it is
not included in this frozen source package and cannot serve as this review's
raw-data evidence.

## Evidence boundary for this revision

This matrix is a source audit, not a fresh execution report. Native ARM64 and
hosted CI success are not established here; architecture source code and
cross-compilation are not native test results. Consult the operation-specific
capability reports and preserve actual command/toolchain/CPU/width results when
validating a target. Historical stage links apply only to their recorded scope.
