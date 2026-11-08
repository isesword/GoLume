# Contiguous numeric native SIMD expansion

Base: `fa163b3c161e65725145bff003b54939db5148e1`.

This stage fills common contiguous numeric kernel gaps while preserving public
result types and scalar references. It does not claim that every operation or
query is vectorized, or that SIMD makes every workload faster.

## Changes

- Generated native Float64 Add and all five ordered literal and column
  comparisons (GT/GE/LT/LE/EQ).
- Generated native Float32 Add/Sub/Mul/Div, without widening or reciprocal
  approximations. Ordinary Float32 expression arithmetic still promotes to
  Float64 and does not call this native-width kernel API. Its query benchmarks
  are semantic/allocation controls, not evidence of Float32 SIMD query execution.
- Generated native Int8/Int32/Int64/Uint32/Uint64 literal comparisons for all
  five predicates. Int16/Uint8/Uint16 literal entry points remain scalar.
  Existing arm64 Int32/Uint32 overrides are preserved; Int32 EQ now uses portable SIMD.
- Fixed stack mask storage, early scalar fallback before vector setup when
  fewer than one full vector fits, scalar tails, per-word validity for the generated
  comparisons, and positive-zero invalid arithmetic outputs. Exact arithmetic
  input/output aliases are supported. Partial overlap and bitmap/validity aliases
  are not new guarantees.
- Native selection requires the SIMD build and a non-emulated runtime. Scalar
  mode and runtime-disabled builds retain scalar function identities. Existing
  AVX-512 hash/compaction guards and checked-integer overflow semantics are unchanged.

On the native 512-bit amd64 host, Auto reports 53 hardware-SIMD KernelIDs and
20 separate column comparison capabilities, up from 18 and 15. These report
available vector paths, not a guarantee that short inputs enter a vector loop.

## Correctness and native execution

- Complete ordinary and SIMD test suites, complete SIMD race suite, ordinary
  and SIMD vet/build, GoDoc, generated-source consistency, registry consistency,
  and structural checks pass. The final verification log records the exact commands.
- Kernel/expression/CLI checks pass at runtime-disabled and native 128/256/512-bit
  settings. The query matrix also passes at all four settings.
- Literal integer tests cover every length 0..129 plus larger inputs, all five
  operators, signed extrema, unsigned sign-bit boundaries, nil/all/mixed/all-null
  validity, dirty output tails, suffix sentinels, and zero kernel allocations.
- Floating arithmetic tests include 65,537 edge/random bit-pattern rows, full
  signed-zero/NaN/infinity/subnormal/extrema combinations, rounding boundaries,
  tails, both exact aliases, and nullable outputs. Non-NaN results are bit-exact;
  NaN classification, rather than payload identity, is the portable contract.
- All five floating comparisons remain ordered, including NaNs and signed zero.
- Independent review confirms actual packed instructions at all three native
  amd64 widths, not just capability flags. See `review-summary.md`,
  `review-independent-probes.txt`, and `review-amd64-objdump.txt`.
- Linux/arm64 SIMD kernel cross-compilation passes. No native arm64 execution
  or arm64 performance result is claimed for this stage.
- Whole-query coverage is 240 cases: 35 comparisons and five arithmetic cases,
  dense/nullable inputs, three dispatch modes, and 4096+4096+7 source rows.
  Independent results, row identity/order, managed peak parity, complete release,
  and representative allocation parity are checked.

## Measurement scope

The definitive `balanced-*` records use frozen test executables on Intel Xeon
Platinum 8573C linux/amd64, Go 1.27.3, GOMAXPROCS=1, six mode-order permutations,
100 ms per sample. Correctness and compilation finish before timing. The runner
checks exactly 216 kernel and 72 query cases per round. This is exploratory
shared-host evidence, not a dedicated/pinned performance lab or historical
one-second acceptance gate. `kernel-benchmark.txt` and `query-benchmark.txt`
are earlier fixed-order smoke samples; kernel smoke overlapped validation work.
Do not use their timings instead of the balanced run.

Query timing includes stream open/drain/close and excludes fixtures, kernel-set
construction, and expression compilation. `peak_bytes` counts managed execution
workspace, not total process RSS or complete source fixtures. Zero per-call
kernel allocations do not imply zero initialization cost. Whole-query comparisons
include every selected SIMD backend, not an isolated attribution to one new kernel.

The first balanced run found a short-input regression: seven-row Float64
literal calls paid vector/mask setup even though the 512-bit vector cannot fit.
An early scalar fallback removes that setup. The `before-short-fix-*` records
preserve the initial finding; the final `balanced-*` run measures the corrected
implementation. Short calls can still have dispatch overhead and are not
reported as vector execution.

The corrected run contains 72 kernel workloads and 24 complete-query workloads,
each in three modes with six samples (1,296 kernel and 432 query measurements).
Every measured kernel reports zero B/op and allocs/op. Query B/op, allocs/op,
managed-peak medians, and row counts match across modes.

Auto kernel medians are lower in 50 workloads and higher in 22, ranging from
78.8% lower to 96.9% higher than Scalar. The slower extremes are tiny calls,
where native setup/dispatch may dominate and a full vector often does not fit.
Auto query medians are lower in 20 workloads and higher in four, ranging from
17.5% lower to 5.8% higher. These are descriptive short shared-host samples,
not statistical acceptance or a promise of universal speedups. Float32 query
arithmetic remains the promoted-path control described above.

Results and medians are recorded in `measurement-summary.json`; compare raw
samples before drawing workload-specific performance conclusions.

Reproduce with Go 1.27 and dependencies available:

```sh
GOMAXPROCS=1 SIMD_BENCHCOUNT=6 SIMD_BENCHTIME=100ms \
  ./scripts/bench_numeric.sh validation/numeric-repeat
```

## Source and publication boundaries

All available tracked source files were checked against the base tree before
editing. This archive omits historical validation files and the tracked test
binary; no such omissions are published as deletions. The existing local
`testdata/bench-1m.parquet` is the known alternate synthetic fixture from the
previous stage (SHA-256 `115f46c063da52e2b3b8c883522e8d29a4559b410772bc08681a87235d783876`).
It is not changed or published. New query fixtures are generated in memory and
do not depend on it. Build checks use GOFLAGS=-buildvcs=false because this
archive lacks Git metadata; this does not change the supported build script.

Publication is limited to the requested private performance branch. There is
no main merge, release, or broad performance acceptance claim. Next stages are
comparison-mask packing/null processing, followed by measured aggregation and
compaction hotspots, each reviewed and validated separately.
