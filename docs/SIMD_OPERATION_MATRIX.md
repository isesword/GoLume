# SIMD Operation Matrix

This matrix describes current dispatch, not projected coverage or performance.
Refresh it whenever a kernel or production execution path changes.

The supported Go 1.27 build entry is `./scripts/build.sh`. It defaults an unset
`GOEXPERIMENT` to `simd`, honors explicit overrides (including an empty value),
and changes no persistent Go settings. Direct Go commands still need the
experiment explicitly enabled. `ModeAuto` prefers retained native SIMD wherever
available; `ModeScalar` is the reference and `ModeSIMD` is diagnostic selection.
Runtime emulation/disablement and hardware/vector-width guards remain in force.

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
go test ./kernel ./cmd/kernel-info ./memory ./expr ./engine ./lazy ./physical
GOEXPERIMENT=simd go test ./kernel ./cmd/kernel-info ./memory ./expr ./engine ./lazy ./physical
GOEXPERIMENT=simd go run ./cmd/kernel-info -mode scalar
GOEXPERIMENT=simd go run ./cmd/kernel-info -mode auto -json
GOEXPERIMENT=simd go run ./cmd/kernel-info -mode simd
GOEXPERIMENT=simd GODEBUG=simd=0 go test ./kernel ./cmd/kernel-info ./expr ./engine ./lazy ./physical
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
| Literal comparison | Int8/Int32/Int64/Uint32/Uint64/Float32/Float64 GT/GE/LT/LE/EQ | portable SIMD; guarded amd64 direct mask extraction; arm64 retains Int32 GT/GE/LT/LE and Uint32 overrides | unavailable native runtime, tails |
| Literal comparison | Int16/Uint8/Uint16 | none | typed scalar entries |
| Column comparison | Int32/Uint32/Float32/Float64 GT/GE/LT/LE/EQ | portable SIMD; guarded amd64 direct mask extraction | unavailable native runtime, tails |
| Column comparison | Int8/Int16/Int64/Uint8/Uint16/Uint64 | none | typed scalar entries |
| Arithmetic kernels | Float32/Float64 Add/Sub/Mul/Div | portable SIMD | unavailable native runtime, tails |
| Expression arithmetic | Float32 operands | promotes to Float64, existing scalar conversion path | native Float32 arithmetic API is not reached |
| Arithmetic | checked integers | none | scalar with existing numerical/overflow semantics |
| Unary math | Float32/Float64 Abs/Sqrt | portable SIMD, dense and nullable/selected inputs; mask before math, bounded fixed-stack nullable tails | dense nil-mask tails, unavailable native runtime |
| Other math | integer math, Round/Floor/Ceil/Log/Mod/FloorDiv/Pow | none | typed scalar; no approximate replacements |
| Integer bitwise | Int32/Uint32/Int64/Uint64 AND/OR/XOR/NOT | portable native SIMD, dense and nullable; expression and kernel.Set entry points | unavailable native runtime, short inputs and tails; other integer widths and shifts unsupported |
| Boolean | bitmap AND/OR/NOT | none | word scalar |
| Hash | Hash64 batch | amd64 AVX-512 with native width >=512 | arm64 Auto retains non-SIMD four-way unroll; other cases scalar |
| Hash | Hash128 batch | none | scalar |
| Scale | checked Int64 scale | none | bounded scalar |
| String length / match | byte/UTF-8 length, equality/prefix/suffix/contains | none | scalar KernelID entries |
| Compact | Int32/Int64/Uint32/Uint64/Float64 | amd64 AVX-512 Compress with native width >=512 | other architectures/widths |
| Compact | Int8/Int16/Uint8/Uint16/Float32 | none | native-width scalar |
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
- `GODEBUG=simd=0` checks disabled/emulated correctness. It cannot establish
  hardware SIMD availability or performance.
- The planner's measured bitmap/direct strategy decisions are independent and
  unchanged. Existing strategy approvals still apply only to their recorded
  architecture, width, operation, and selectivity bucket.
- Historical gates used ten same-build one-second samples, at least 10%
  kernel improvement, `p < 0.05`, and no related end-to-end regression above
  3%. Those records remain useful evidence, not the current kernel admission
  policy. New performance claims need new comparable samples and the actual
  scan/expression/filter/aggregate or join/output path being claimed.

## Remaining optimization inventory

The portable column-comparison kernels are selected in Auto now; their
operator switch and stored-mask lane scan have not been rewritten as generated
literal kernels. Remaining operator-owned loops include hash aggregation probe
and state updates, join probes, and merge/sort. Native-width storage and batching
alone do not make these SIMD.

- Capture fresh CPU profiles and allocation/peak-memory measurements for
  numeric projection, filtering, aggregation, join, sort, strings, and Parquet.
- Keep the inventory of production loops bypassing `kernel.Set` current.
- For Polars comparisons, pin the revision and distinguish SIMD from batching
  and parallelism.
- Retain raw final-build kernel and end-to-end samples per target host before
  claiming performance gains or changing planner strategy approvals.

The amd64 comparison mask backend selects only the actual runtime width: AVX
at 128 bits, AVX2 at 256 bits, and the complete archsimd AVX512 feature group
at 512 bits. Feature/width mismatch keeps the portable backend. Native column
and literal masks are packed directly; portable column32 validity is applied
per word rather than per lane. See the [bitmap report](../validation/bitmap-native/README.md).
