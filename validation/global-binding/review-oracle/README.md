# Independent global binding review

Reviewed 2026-11-08 UTC against frozen baseline `b105e07a29ebbfaef5d7d3be5857feda999cc1d6`.

## Finding

No actionable correctness or lifetime defect found in the reviewed production change. The separate full-suite verification log now records a pass. The independent focused checks themselves are not a full test pass or a performance measurement; timing evidence is reviewed separately below.

- Eligibility is restricted to global, native, nonempty aggregate lanes, excludes statistics/distinct/quantiles and MIN/MAX/AVG, and permits SUM only for the ten primitive numeric types. Compiler-produced metadata and existing NumericBundle validation supply descriptor shape/type invariants.
- The existing predicate evaluation and bindSortedBatch validation run before descriptor population, including batches with no selected rows. Missing or short vectors/validity keep their existing error path.
- Per-row conversion mirrors nativeAggregateBits and uses the unchanged NumericVector.AddBits call in encounter and lane order. No SIMD reduction, reassociation, Float32 widening, or arithmetic change was introduced.
- COUNT-only descriptors contain validity without typed value pointers. Opaque fallback remains usable even for primitive logical types. Synthetic empty-name COUNT(*) reads return `(0,false)` as before.
- Descriptors are cleared before the next producer call, including interleaved empty/unselected batches, and on cleanup. EOF/source error clears inputBatch bindings. Same source-owned vector headers can be reused with different Values/Validity slices each batch.
- A single exact-capacity descriptor slice is reserved before allocation. On this amd64 build each lane is 112 bytes. Constructor failure cleans earlier reservations without double-releasing the prepared-query owner; the descriptor allocation/reservation is released when the output stream is drained to EOF, explicitly closed, or failed. Source EOF clears borrowed references but may precede final-output consumption and reservation release. There are no row-sized binding copies. This follows the existing managed backing-allocation accounting model, which is not total heap/RSS accounting.
- Grouped and richer aggregation use the existing row reader and state update path. The shared sorted-stream cleanup changes were also inspected for retained group-key and result state dependencies.

## Independent oracle

`oracle_test.go.txt` is overlaid on the existing engine/native_aggregate_benchmark_test.go filename. It introduces no retained `.go` package beneath validation and makes no production edits. Its expected sums/counts are computed row-by-row without aggregate or kernel production helpers. Existing fixture code is used only to construct typed input vectors.

Each run executes 48 mode/seed cases three times (144 queries), plus three repeated opaque COUNT queries: 147 executions per run, 441 across these three passing runs:

- `baseline-default.txt`: frozen baseline, GOEXPERIMENT empty, GOMAXPROCS=1.
- `current-default.txt`: reviewed candidate, GOEXPERIMENT empty, GOMAXPROCS=1.
- `current-simd.txt`: reviewed candidate, GOEXPERIMENT=simd, GOMAXPROCS=1.

The oracle covers Scalar/Auto/forced-SIMD modes; all ten numeric input widths; signed/unsigned wrapping; narrow integer widening; UInt64 values above 2^63; Float32 step rounding and Float64 sequential order; NaN/Inf/signed-zero inputs; dense/sparse/empty selection; changing nil/non-nil validity; row counts 0, 1, 7, 63, 65, 129, 2, 0 with an additional intermediate zero-row batch; source reuse of the same vector headers with changing backing slices; prepared-query reuse; and zero managed retained bytes. NaN results are compared by NaN classification, not payload equality; other valid results use exact bits and types. Input fixture storage is created outside query accounting. The source obeys the configured row bound.

Separate production focused tests were read for exact eligibility rejection, validity-only nested/string/dictionary/date/opaque inputs, synthetic COUNT(*) lane behavior, zero row-reader allocations, validation errors despite a false predicate, explicit Close/cancel/source-error handling, exact descriptor reservation, constructor budget failure rollback, and empty/all-null/filtered identities. Those tests belong to the implementer, not this independent oracle.

## Native disassembly

The local-only `current-simd.test` was compiled with Go 1.27.3 linux/amd64 and GOEXPERIMENT=simd. `bound-reader-objdump.txt` contains no mapaccess or mapassign calls; its only calls are bounds-panic paths. `bound-dispatch-objdump.txt` shows the eligible branch calling the bound reader followed by NumericVector.AddBits, bypassing the existing fallback name lookup. Map calls remain in the fallback branch as intended. This is evidence of lookup removal in the bound path, not a new SIMD implementation or a performance result.

`source-binary-sha256.txt` records exact production/test/helper and binary hashes. `environment.txt` records the actual host/toolchain. The three production hashes were stable throughout the independent runs:

- engine/native_global_binding.go: `916bcb7feb02ec7959cc1360f548788a3f7a418da56efabda33ac61700bc7f04`
- engine/sorted_aggregate_stream.go: `aafda761c3df67ccc099964816864e3c557c70e8363dbc74152f50ca941ce1a0`
- engine/multi_aggregate.go: `ef6a70e77be807e7d00aaad7fd1e7c0e56bfe10b525f45470a8173b7ce924fe8`

## Reproduction and limits

From each target repository root, regenerate the appropriate overlay JSON with its absolute engine/native_aggregate_benchmark_test.go path as the Replace key and the absolute retained oracle_test.go.txt path as the value. Then run:

```
GOEXPERIMENT= GOMAXPROCS=1 go test -overlay=/absolute/path/overlay.json ./engine -run '^TestReviewGlobalBinding' -count=1 -v
GOEXPERIMENT=simd GOMAXPROCS=1 go test -overlay=/absolute/path/overlay.json ./engine -run '^TestReviewGlobalBinding' -count=1 -v
```

Use the repository's Go 1.27.3 toolchain/dependencies. The checked-in overlay copies contain the current workspace paths and need regeneration after relocation. Preserve helper sources as `.go.txt`. Do not publish the local `.test` executable; portable objdump text and its hash suffice.

No race run, complete test-suite run, arm64 execution, arm64 performance result, heap/RSS/leak-duration measurement, or benchmark was performed by this independent reviewer. Separate validation artifacts provide additional checks; final-verification.txt records ordinary/SIMD full suites, full SIMD race, vet/build and repository validation passes, plus an arm64 SIMD engine cross-compile. These are distinct from the independent oracle runs above. No native arm64 execution is claimed.

## Shared harness evidence review before timing

Read-only inspection and independent log/CSV reconciliation confirmed:

- 36 new materialized whole-query timed cases plus six unchanged legacy summary-consumption cases. These two benchmark families have different sinks and must not be pooled as one identical workload.
- 108 unique oracle subtests in both before-correctness.txt and after-correctness.txt. Each subtest executes twice and requires stable peak and zero retained bytes.
- All 108 charged-memory.csv entries match their raw before/after logs. Deltas: 42 entries at +224 bytes, 54 at +448 bytes, 12 excluded controls at zero. Every eligible delta is exactly 112 bytes times lane count, including larger total inputs, and every logged postexecution usage is zero.
- The frozen harness equals engine/global_binding_benchmark_test.go. Candidate production hashes still match those above.
- Requested one documentation correction: the 262,147-row source batch is split at MaxMorselRows=65,536 into five active scan views. The source comment and methodology were corrected before timing. Both binaries were then refrozen using the same comment-corrected harness. Earlier preflight artifacts are explicitly archived under superseded-harness; no stale one-view comment remains in the final harness.

The checks above first occurred before timing and were rechecked against the final refrozen artifacts. Timing evidence, including longer paired follow-up and confirmation, has since been reconciled as described below.


## Final refrozen provenance and initial timing review

Final common harness SHA256 (engine/global_binding_benchmark_test.go and harness.go.txt): `9e7a6c1b112c5f5e33ef4e4b3535637a852e985542738283a2f371648dc454b0`.

Final comparable benchmark binaries, as recorded in paired-environment.txt and before/after provenance:

- Before: `131f78f1b76460e860f52529743573ed8f87ee73a1f89da0917f7a33ef924e46`
- After: `27e51d5b2919f98979c8fceed9f42c48cf87bc2691e83b21a6e9f879bfe118d8`

These are the final timed executables, distinct from the independent oracle/disassembly executable recorded earlier. The production file hashes above remain unchanged. The comment-only harness correction did not alter the independent oracle or production semantics.

Independently parsed all 12 initial round logs: 42 workloads per round and variant, 504 timed samples total, with six complete pairs per workload. Every reported median ns/op, B/op, allocs/op, paired geometric-mean ratio, and exact paired sign-flip value in paired-summary.csv reconciles with those raw logs. paired-order.tsv alternates AB/BA as described. No samples were discarded.

The initial short-target data contains very large timing excursions in both variants, with some paired ratios exceeding 20x and changing direction. Consequently, the initial medians and unadjusted sign-flip values must not establish a precise universal speedup or dismiss regressions. In particular, the excluded MIN/MAX control is slower in all six initial pairs (ratios approximately 1.02 to 1.37), and the tiny selected four-lane case and three other apparent regressions require follow-up rather than an assumption that all unfavorable results are noise.

The longer follow-up protocol includes all five initially >5% regressions plus three headline eligible cases, with six alternating one-second pairs and immediately adjacent before/after measurements per case. Case order reverses each round, frozen executables remain unchanged, and no outliers are discarded. All 96 follow-up samples and all 32 subsequent confirmation samples have now been independently reconciled with their summaries. The confirmation includes eight alternating two-second pairs for the two remaining positive median changes, again without dropping outliers.


## Final timing and profile assessment

The complete evidence supports a narrow lookup-removal benefit for the measured larger eligible queries, with added fixed allocation overhead. It does not establish a universal improvement or prove excluded paths are performance-identical.

Representative longer, immediately adjacent one-second measurements (six pairs):

| Fixture | Before median | After median | Median change | Paired ratio range |
|---|---:|---:|---:|---:|
| Two lanes, dense, 32,771 rows | 2.423 ms | 1.667 ms | -31.2% | 0.632–0.784 |
| Four lanes, dense, 32,771 rows | 3.938 ms | 2.062 ms | -47.6% | 0.442–0.559 |
| Four lanes, nullable, 262,147 rows | 27.743 ms | 16.786 ms | -39.5% | 0.542–0.660 |
| Two lanes, selected, 1,021-row batches, forced-SIMD mode | 0.620 ms | 0.477 ms | -23.1% | 0.726–1.018 |

The first three fixtures favor the candidate in every longer paired sample. These are complete materialized queries, not standalone reductions. Whole-query heap cost rises by approximately 344 B/op for two lanes or 568 B/op for four lanes, and four allocations per execution. The exact managed descriptor increments remain 224/448 bytes; those are different metrics.

The initially unfavorable grouped/statistics medians become -0.8%/-1.8% in the longer follow-up. Tiny selected four-lane queries remain +7.4% there but become +0.8% in the eight-pair two-second confirmation (22.1365 to 22.315 microseconds, paired ratios 0.948–1.031). No benefit for that tiny fixture is established.

The excluded MIN/MAX control is +12.0% initially, +9.9% in the longer follow-up, and +3.3% in the final confirmation (2.7514 to 2.8417 ms). The final paired ratios span 0.881–5.824, with a +29% paired geometric-mean ratio due to a large excursion. The final median does not reproduce the earlier >5% change, but this noisy evidence does not prove absence of a smaller regression. Preserve the full raw series and these limitations rather than describing excluded controls as unchanged or discarding the unfavorable samples. The sign-flip figures are exploratory and unadjusted, not equivalence tests or independent admission thresholds.

Four separate CPU profiles corroborate actual dispatch on the final frozen binaries:

- Eligible global, four lanes/32,771 dense rows: baseline nativeAggregateBits is 26.77% cumulative and runtime.mapaccess1_faststr is 23.42% cumulative. Candidate samples identify globalAggregateLaneBinding.bits at 7.89% cumulative; mapaccess1_faststr is 0.72% cumulative. The source-line profile shows the new bound branch and sequential NumericVector.AddBits call.
- Excluded MIN/MAX: candidate samples still identify nativeAggregateBits (18.22% cumulative) and its name-map path, with no sampled bound-reader branch. The source-line profile corroborates the intended exclusion.

Profile percentages use each profile's own sample total, overlap when cumulative, and cannot be added or treated as absolute per-query speedups. CPU profiles cover calibration, setup and benchmark execution; these short unpaired profiled runs are mechanism evidence, not replacement timings. The disassembly evidence above separately establishes absence of map access in the bound reader.

No new correctness, accounting, lifetime, source-provenance or benchmark-summary blocker was found. The implementation remains narrowly scoped. Larger-query gains are supported in these fixtures; small-query behavior and excluded-control variance remain explicit limitations. All production hashes remain those recorded above.

Final supplemental checks were also reconciled: actual-eligibility.txt has 36 passing cases (16 two-lane and 16 four-lane eligible cases, four ineligible controls). Eligible sorted streams allocate exactly their lane count; excluded MIN/MAX/statistics sorted streams allocate no binding descriptors. Grouped plans are classified ineligible and stay outside the sorted binding implementation. Every opened inspection stream releases its managed charge on Close. This candidate-only overlay is not part of the comparable timed binaries.

The final profile manifest passes all 19 local hash checks, including the exact reproduction method. Raw CPU profiles and test executables are local-only; published evidence is portable text plus identification hashes. Before/after addBundleRow disassembly confirms stack reservation increases from 184 to 192 bytes with the new branch. That supports a plausible small fallback-overhead explanation, but does not establish causality for the noisy MIN/MAX timings.
