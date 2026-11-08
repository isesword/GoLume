# Direct comparison bitmaps and wordwise nulls

Baseline: `d12aa9edc62cd9db82777b8cc6379aca431b9306`.

This stage improves existing comparison implementations. It does not add new
public numeric operations, change expression result types, reassociate arithmetic,
or claim that every query becomes faster.

## Implementation

- Generated amd64 direct comparison-mask extraction for the existing 35 literal
  and 20 column comparison entries. Seven literal types and four column types,
  five predicates, and three widths produce 165 concrete functions, not 165 new
  public operations. Native capability counts stay at 53 KernelIDs plus 20 column
  comparisons on the current host.
- Exact selected-width installation: 128-bit AVX, 256-bit AVX2, or 512-bit
  archsimd AVX512 (F+CD+BW+DQ+VL), always with a non-emulated runtime and a
  non-Scalar mode. Unsupported features/widths retain the portable backend.
  The selected comparison backend is `archsimd-amd64-mask`.
- Mask bits are widened to uint64 before shifts and packed directly into the
  output word. These paths avoid scratch-mask stores, per-lane scans, and
  per-vector type/width switches. Early scalar fallback below one vector and
  scalar tails are retained. The fixed-width generator adds code, not per-call
  heap buffers; this is not a claim about total process RSS or binary size.
- Both validity bitmaps are combined once per column output word. All-null words
  skip value loads after logical output words have been cleared. Existing portable
  Int32/Uint32/Float32 column paths also use wordwise validity, independently tested
  so architecture overrides cannot hide fallback regressions.
- Architecture comparison registration follows the portable column installers.
  Existing arm64 literal overrides and hash/compaction guards are preserved.
  Mask16, narrow literal dispatch gaps, and new compaction/reduction kernels are
  outside this stage.

## Correctness and architecture evidence

- Complete ordinary and SIMD test suites, complete SIMD race suite, vet/build in
  both modes, generator/registry/GoDoc checks, and structural checks pass. Exact
  commands and results are in `final-verification.txt`.
- Kernel, expression, CLI and affected complete-query tests pass with runtime
  SIMD disabled and at native 128/256/512-bit widths.
- Independent review directly executes all 165 native functions and the modified
  portable column paths. It covers signed/unsigned extrema, NaNs including a
  signaling encoding, signed zero, infinities, subnormals, asymmetric validity,
  every single set-bit position 0..128, unaligned slices, bitmap/vector tails,
  dirty output and untouched suffix sentinels. No kernel heap allocations are added.
- Explicit hardware-feature masking checks cover disabled AVX/AVX2/AVX512 and
  the corresponding supported fallback widths. This tests dispatch guards on
  this host, not physically older processors.
- Disassembly confirms packed comparison plus movemask/KMOV extraction. See
  `review-verdict.md`, `review-source-sha256.txt`, and `review-objdump.txt`.
- Linux/arm64 SIMD kernel cross-compilation passes. Native arm64 execution and
  performance are not claimed. Portable null-loop changes need native arm64
  measurement before drawing performance conclusions there.
- Existing numeric and column32 query matrices remain intact. A new 60-case
  Float32 comparison matrix checks literal/column shapes, all predicates,
  dense/nullable input, all modes, independent row results/order, managed peak,
  allocation parity, and complete release. Fixtures use 4096+4096+7 rows.
  Float32 arithmetic still promotes to Float64 and remains a control workload.

## Paired measurement scope

`paired-*` compares frozen baseline and final test executables in **Auto mode**,
not Scalar versus SIMD. The source baseline was checked against Git blobs before
freezing; binary hashes and build settings are recorded. Six samples alternate
before/after and after/before order, at GOMAXPROCS=1 and 100 ms per sample, on a
shared Intel Xeon Platinum 8573C linux/amd64 host. Compilation and correctness
checks finish before timing; no other local validation/profile work overlaps it.

There are 124 kernel workloads and 64 complete-query workloads per version,
including short/tail/dense/null cases, all five column32 predicates, and existing
arithmetic controls. The 64 query cases comprise 24 numeric cases (ten unchanged
arithmetic controls) and 40 column32 filter/project cases. New Float32 query
comparisons appear separately in `float32-query-supplement.txt`: six final-build
samples across three modes, not a before/after result, because the frozen baseline
predates that additional harness.

Fixtures, compilation and kernel-set construction are outside query timing;
stream open/drain/close, source/workspace setup, validity, selection, projection
and cleanup are included. B/op and allocs/op measure Go heap allocations.
`peak_bytes` measures managed execution workspace, not fixture memory or total
RSS. Zero per-call kernel allocation does not imply zero initialization cost.
Whole-query changes include all selected backends and are not isolated attribution
to one instruction. Raw samples and benchstat summaries accompany the medians.
Short shared-host results are exploratory, not a dedicated performance acceptance
run or a cross-architecture speedup promise.

## Observed results and caveats

Across 1,488 paired kernel measurements, 117 of 124 workload medians are lower
and seven are higher. Ratios range from 92.7% lower to 12.2% higher; the higher
medians are short calls below a full vector. Nominal benchstat comparisons find
109 improvements, one regression and 14 indistinguishable cases at p<0.05.
The short nullable Int32 case is examined separately in `short-followup-*`:
ten alternating one-second samples per version reproduce a median change from
21.46 ns to 23.47 ns, about +2 ns / +9.3% (nominal p=0.019), with identical
zero allocation and results. Seven rows are below one selected 512-bit vector,
so this is short-path overhead, not failed vector execution. It remains an
explicit tradeoff rather than being hidden by a broad speedup claim.

Across 768 paired query measurements, all 64 medians are lower, ranging from
1.1% to 31.9% lower. Benchstat distinguishes improvements in 29 cases; 35 remain
indistinguishable from noise. All ten unchanged arithmetic controls fall in the
latter group. These are nominal per-workload comparisons without a multiple-test
correction, not proof that every workload benefits. No significant complete-query
regression is observed in these samples.

Every query retains identical B/op, allocs/op, managed-peak medians, and row
counts. Direct kernel allocation tests remain zero. Of 124 paired kernel cases,
122 report zero B/op and allocs/op in both versions. The two legacy million-row
Float32 benchmarks construct `Set` inside the timed subbenchmark without resetting
the timer: their reported B/op includes one-time initialization amortized over a
performance-dependent iteration count. Their lower B/op is **not** evidence of a
per-call memory reduction. Raw figures are retained and this limitation is not
normalized away in `measurement-summary.json`.

The supplementary Float32 complete-query benchmark verifies final Scalar/Auto/
SIMD results and memory separately; it is not an additional baseline comparison.


Reproduce after preserving the baseline kernel/physical test binaries in a
separate directory, using Go 1.27.3 and dependencies:

```sh
GOEXPERIMENT=simd go test -c ./kernel -o BASELINE/kernel.test
GOEXPERIMENT=simd go test -c ./physical -o BASELINE/physical.test
# Switch the source checkout from the stated baseline to the candidate, then:
GOMAXPROCS=1 SIMD_BENCHCOUNT=6 SIMD_BENCHTIME=100ms \
  SIMD_BASELINE_COMMIT=d12aa9edc62cd9db82777b8cc6379aca431b9306 \
  ./scripts/bench_bitmap.sh BASELINE validation/bitmap-repeat
```

## Boundaries and next investigation

This source archive has no Git metadata, so builds use GOFLAGS=-buildvcs=false.
Historical validation omissions and the tracked test binary are not deleted from
the repository. The known local substitute `testdata/bench-1m.parquet` is unchanged
and not published; its SHA256 remains
`115f46c063da52e2b3b8c883522e8d29a4559b410772bc08681a87235d783876`.
The new/paired query fixtures are generated in memory and do not use that file.
The local reviewer executable is also excluded from publication.

Publication remains limited to the requested private performance branch, with
an expected-head check and no force/main merge. Next is a measured investigation
of global/grouped aggregation and projection/compaction. No additional SIMD
implementation is assumed necessary until those profiles identify useful work.
