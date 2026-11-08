# Whole-query benchmark and memory findings

## Result

The frozen candidate produces repeatable whole-query gains on representative
large eligible global sum/count queries. This is batch-column binding, not a new
SIMD reduction. It preserves sequential floating-point summation and costs a
small, explicit per-execution allocation.

There is no established small-query benefit. The excluded MIN/MAX control has a
remaining small, uncertain slowdown signal; these results do **not** prove every
excluded path is unchanged in performance.

## Compared executables and coverage

- Baseline commit: `b105e07a29ebbfaef5d7d3be5857feda999cc1d6`. The supplied source
  validation record reports 711 files verified and no differences.
- Identical final harness SHA256:
  `9e7a6c1b112c5f5e33ef4e4b3535637a852e985542738283a2f371648dc454b0`.
- Before binary SHA256:
  `131f78f1b76460e860f52529743573ed8f87ee73a1f89da0917f7a33ef924e46`.
- After binary SHA256:
  `27e51d5b2919f98979c8fceed9f42c48cf87bc2691e83b21a6e9f879bfe118d8`.
- Go 1.27.3, GOEXPERIMENT=simd, linux/amd64, Intel Xeon Platinum 8573C,
  GOMAXPROCS=1 and one engine worker. The relevant production hashes were checked
  again after timing and remained unchanged.
- 36 new materialized ExecuteTyped workloads: 32 eligible global cases and four
  excluded controls (two grouped, one MIN/MAX, one First/Last statistics).
- Six unchanged BenchmarkNativeAggregate workloads: two global cases and four
  grouped controls. These existing tests are summary-only, so their timings are
  comparisons within that unchanged benchmark, not interchangeable with the new
  materialized workloads.
- Each final executable passed 108 independent oracle subtests, with two complete
  executions and memory-release checks per subtest. Scalar, Auto and forced-SIMD
  modes are covered. No production reduction helper is used by the oracle.
- A separate, untimed candidate-only overlay inspects actual eligibility and
  opened sorted streams: all 32 eligible cases allocate two or four descriptors.
  The MIN/MAX and statistics sorted controls are opened and verified to allocate
  none. The two grouped controls are classified ineligible and are not opened
  through the sorted-stream helper. Every inspected sorted stream releases its
  charged memory. Its source is `eligibility.go.txt`; it is not part of either
  comparable timed executable.

The final shared harness was rebuilt on both trees after a comment-only
source-batch/morsel clarification. Earlier preflight evidence is explicitly
marked superseded. The 262,147-row single source batch becomes five scan views
at MaxMorselRows=65,536. The 32,771-row single source batch is one view; the
1,021-row source-batch fixture is 33 views.

## Unprofiled timing evidence

All raw observations are retained; no outlier was removed.

1. Initial matrix: six alternating 150 ms pairs for all 42 workloads, 504 samples.
2. Follow-up: six alternating 1 s pairs for all five initial apparent regressions
   above 5%, plus three representative large eligible workloads, 96 samples.
3. Confirmation: eight alternating 2 s pairs for the two remaining positive
   median changes, 32 samples.

Each follow-up/confirmation before-after pair was adjacent, with AB/BA direction
reversed each round. There are 632 unprofiled measurements in total. Compilation,
fixture creation, preparation, oracle checks, budget snapshots and profiles are
outside timed execution. Timed execution includes source/state open, scan,
aggregation, result materialization and source/state close. The prepared cache
is disabled. See `methodology.txt` for the full contract.

Representative longer-run medians:

| Workload | Before | After | Change | Paired evidence |
| --- | ---: | ---: | ---: | --- |
| Two lanes, dense, 32,771 rows, one source batch | 2.423 ms | 1.667 ms | -31.2% | All six pairs faster; after/before 0.632–0.784 |
| Four lanes, dense, 32,771 rows, one source batch | 3.938 ms | 2.062 ms | -47.6% | All six pairs faster; after/before 0.442–0.559 |
| Four lanes, nullable, 262,147 rows | 27.743 ms | 16.786 ms | -39.5% | All six pairs faster; after/before 0.542–0.660 |
| Two lanes, selected, 32,771 rows, 1,021-row batches, forced SIMD | 620.358 µs | 476.823 µs | -23.1% | Initial +69% apparent regression does not reproduce |
| Grouped two-lane control | 4.735 ms | 4.697 ms | -0.8% | Mixed directions; no established improvement |
| Statistics four-lane control | 6.269 ms | 6.158 ms | -1.8% | Mixed directions; no established improvement |
| Four lanes, selected, 63 rows (final confirmation) | 22.136 µs | 22.315 µs | +0.8% | +0.179 µs; only three of eight pairs slower |
| MIN/MAX two-lane control (final confirmation) | 2.751 ms | 2.842 ms | +3.3% | +90.3 µs; six of eight pairs slower; uncertain |

The three large eligible follow-up gains have descriptive, unadjusted exact
paired sign-flip p=0.03125. This is the minimum two-sided value with six pairs,
not a multiple-comparison-adjusted acceptance criterion. Report the effects and
consistent pair directions rather than a universal or hardware-independent
speedup claim.

The tiny selected confirmation has ratios 0.948–1.031 and paired p=0.430. Its
initial +16.4% and first follow-up +7.4% median changes did not persist. The
allocation cost is still real even though a latency penalty was not established.

MIN/MAX requires caution: confirmation ratios span 0.881–5.824, paired geometric
mean is approximately +29%, and paired p=0.219. The +3.3% median is not proof of no
regression, nor does it establish a repeatable material slowdown. Both an added
small fallback cost and scheduling variation remain plausible. A strict narrow
non-regression threshold would need further measurement on a less noisy host.
The initial and follow-up samples also contain large outliers on both variants;
see all ratios and raw data rather than relying only on these medians.

The unchanged global native benchmarks improved 33.2% and 46.3% in the initial
matrix; unchanged grouped native medians ranged from -3.7% to +0.4%. These are
supporting continuity checks, not the headline materialized-query results.

## Explicit memory tradeoff

| Numeric lanes | Added charged peak | Added heap B/op in longer runs | Added allocs/op |
| --- | ---: | ---: | ---: |
| Two | 224 B | 344 B | 4 |
| Four | 448 B | 568 B | 4 |

The new charged descriptor is 112 bytes per lane. The peak delta is the same at
32,771 and 262,147 input rows, and across single/multiple source batches,
nullable/selected input and count-only lanes. Heap differences likewise remain
per execution rather than per row or per batch. Small B/op fluctuations in the
short samples are retained rather than normalized away.

Examples from the exact charged-memory checks:

- Two lanes, 32,771 rows: 13,430 → 13,654 B.
- Two lanes, 262,147 rows: 21,606 → 21,830 B.
- Four lanes, 32,771 rows: 14,868 → 15,316 B.
- Four lanes, 262,147 rows: 23,044 → 23,492 B.

The baseline itself grows 8,176 B between these tail-sized fixtures because of
existing active-morsel scratch, independently of the new descriptors. Excluded
controls have zero charged-peak delta and no systematic heap/allocation change.
Every final oracle execution reports zero query and engine charged bytes after
execution. This is **query-charged peak**, not source-fixture size, total heap
retention, or process RSS. B/op and allocs/op are separate Go heap measurements.

## Matched profiles and actual dispatch

Four separate 2 s CPU profiles were collected only after all unprofiled timing:
matched before/after large four-lane eligible global queries and matched
before/after excluded MIN/MAX queries. Their timings are not merged into the
unprofiled sample sets. Profiles include benchmark calibration, setup, untimed
verification and cleanup as well as execution; exact commands are in
`profiles/method.txt`.

- Eligible global baseline: nativeAggregateBits accounts for 26.77% cumulative
  CPU samples. Candidate: that function has no samples; the typed bound reader
  appears at 7.89% cumulative. addBundleRow share changes from 59.11% to 31.90%.
  Together with the explicit eligibility/descriptor check, this confirms the
  intended hot-path replacement for these fixtures.
- MIN/MAX still runs nativeAggregateBits in both executables (15.60% and 18.22%
  cumulative samples). addBundleRow share is similar, 51.06% versus 50.56%.
  The added nil-check branch has no line-attributed samples. This is too coarse
  to rule out a few-percent branch or register-allocation cost.
- Disassembly shows addBundleRow's stack frame grows from 184 to 192 bytes and
  adds the eligibility branch/spill. This is a possible source of small fallback
  overhead, not proof that it caused the timing difference.

Top reports, annotated sources, disassembly, commands, hashes and profiled run
output are portable text evidence under `profiles/`. Raw `.cpu` profiles and test
executables are local-only and excluded from the published deliverable; retained
hashes and commands allow verification or regeneration. The source and executables
remain frozen; no
production changes were made during measurement or follow-up.

## Files to inspect

- `paired-summary.csv`, `paired-summary.txt`, `paired-analysis.json`, and
  `before-round-*.txt` / `after-round-*.txt`: the initial complete matrix.
- `followup/`: all longer eight-case measurements and summaries.
- `confirmation/`: all final two-case measurements and summaries.
- `charged-memory.csv`: all 108 final before/after peak comparisons.
- `before-correctness.txt`, `after-correctness.txt`: independent oracle and plan
  logs, including repeated memory release.
- `actual-eligibility.txt`, `eligibility.go.txt`, `eligibility-overlay.json`:
  separate actual dispatch/descriptor checks.
- `before-provenance.txt`, `after-provenance.txt`, `*-sources.sha256`,
  `candidate-production.sha256`, `profiles/manifest.sha256`: provenance.
- `harness.go.txt` and `scripts/bench_global_binding.sh`: reproducible shared
  source and three-phase baseline/candidate/measurement runner.
