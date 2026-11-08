# Batch-bound global aggregate columns

Baseline: `b105e07a29ebbfaef5d7d3be5857feda999cc1d6`.

The preceding [profile investigation](../aggregate-compaction-profile/README.md)
found substantial per-row column-name lookup in global native aggregation. This
stage implements a narrow response: bind the current batch's typed vectors and
validity once, then use those bindings in the existing sequential update loop.
It does not introduce SIMD reductions or change numerical semantics.

## Implementation and scope

- The new path requires global native aggregation with nonempty SUM/COUNT lanes.
  SUM supports the existing ten primitive numeric input types. MIN/MAX/AVG,
  statistics, distinct, quantiles and grouped aggregation retain their existing
  readers. COUNT-only lanes bind validity without touching payloads, including
  supported opaque and nested columns.
- Column validation still runs in its original order, even when a predicate
  selects no rows. Each selected row calls the unchanged NumericVector.AddBits
  in the same lane and row order. Float32 rounds at every addition, Float64
  accumulation remains sequential, and existing integer widths/wrap, null and
  empty-input identities are preserved.
- Exact-capacity descriptors are reserved before allocation and reused across
  input batches. Borrowed vector/validity references are cleared before the
  producer can recycle headers and at input EOF. Descriptor storage and its
  reservation are released when the output stream is drained, closed or fails.
  Constructor reservation failure unwinds earlier state and the prepared-query
  ownership correctly. There is no row-sized binding buffer or per-row allocation.
- Native amd64 disassembly confirms that the bound reader contains no mapaccess
  or mapassign calls. The fallback reader retains its existing map lookup.
  This proves lookup removal in the eligible path, not new SIMD instructions.
  Kernel dispatch/capability counts are unchanged.

## Correctness and architecture checks

`final-verification.txt` records a passing complete repository validation script,
ordinary and SIMD full suites, the full SIMD race suite, ordinary/SIMD vet and
builds, generator/registry/GoDoc/structural checks, runtime SIMD disabled and
native 128/256/512-bit tests, and the repository's million-row demo. The known
local substitute Parquet fixture used by that demo is not published; all new
query fixtures are generated in memory.

The SIMD engine test binary also cross-compiles for linux/arm64. No native arm64
execution or performance is claimed. This change is architecture-independent
binding code, but that does not substitute for testing on another architecture.

Focused tests cover eligibility, all ten numeric widths, map-independent reads,
zero per-row allocations, count-only validity, changed vector headers/backing
slices, empty batches, malformed inputs, exact floating-point order, terminal
errors, cancellation, constructor budget failure, and release. An independent
public-query oracle performs 147 executions per build across baseline ordinary,
candidate ordinary and candidate SIMD builds, 441 executions total. Its results
use exact bits except NaN classification. See `review-oracle/README.md` and its
source hashes, logs and disassembly.

The shared full-query harness additionally has 108 oracle cases per executable,
each executed twice, covering Scalar/Auto/forced-SIMD modes, null/selection
patterns, order-sensitive sums, overflow, multiple batches, exceptional floats,
empty inputs, and excluded controls. Both final frozen executables pass. A
comment-only correction distinguishing source batches from active scan views was
applied to both frozen harnesses before any timing; earlier preflight evidence is
retained locally and explicitly marked superseded. Its explanatory NOTICE is
published, while duplicated superseded logs and binaries are excluded.

## Paired query measurement

The same frozen harness is overlaid on the verified baseline source and final
candidate. Six before/after pairs alternate AB/BA order, use GOMAXPROCS=1 and
150 ms targets, and execute frozen binaries without overlapping local validation
or profiling. Host: Intel Xeon Platinum 8573C, Go 1.27.3, linux/amd64. No CPU
affinity is applied; the machine is shared/virtualized.

There are 36 new complete materialized ExecuteTyped workloads and six unchanged
legacy summary-consumption workloads per version per round. They have different
output sinks and are reported separately. Immutable input, engine/plan creation,
the correctness oracle and peak inspection are outside timing. State/source
open, scan, reduction, result consumption and close are timed. The new cases
include two/four mixed numeric lanes, dense/null/selective input, small and large
sizes, multi-batch input, count-only lanes and excluded controls.

Source-batch size is distinct from the active scan view. The 262,147-row source
batch is split into five views by MaxMorselRows=65,536; the 32,771-row one-batch
case fits one view, while 1,021-row source batches require 33 views. See
`methodology.txt`, binary/source manifests and `paired-order.tsv` for details.
Timing results are exploratory shared-host observations, not a universal speedup
or a cross-architecture acceptance claim. Any per-workload significance figures
are descriptive and unadjusted for multiple comparisons.

## Observed timings and qualifications

The initial 504 samples cover all 42 workloads in six pairs. Among the 32 eligible
new query cases, 28 medians are lower and four are higher; all four excluded
controls have higher initial medians. Severe scheduling excursions affect some
samples in both variants, so those short-target medians are not sufficient for
an admission conclusion. The raw series is retained without dropping outliers.

All five initial changes worse than +5%, plus three representative larger
eligible cases, received six longer one-second pairs, measured immediately
before/after for each case with alternating order. The two remaining unfavorable
medians then received eight two-second pairs. This adds 96 follow-up and 32
confirmation samples, for 632 timed observations in total.

| Complete materialized query | Before median | After median | Change | Longer protocol |
|---|---:|---:|---:|---|
| Two lanes, dense, 32,771 rows | 2.423 ms | 1.667 ms | -31.2% | Six one-second pairs |
| Four lanes, dense, 32,771 rows | 3.938 ms | 2.062 ms | -47.6% | Six one-second pairs |
| Four lanes, nullable, 262,147 rows | 27.743 ms | 16.786 ms | -39.5% | Six one-second pairs |
| Four lanes, selected, 63 rows | 22.136 us | 22.315 us | +0.8% | Eight two-second pairs |
| Excluded MIN/MAX, two lanes, 32,771 rows | 2.751 ms | 2.842 ms | +3.3% | Eight two-second pairs |

The first three cases favor the candidate in every longer pair. The initially
unfavorable grouped/statistics cases no longer show higher medians in their
longer follow-up; the forced-SIMD two-lane selected case improves by 23.1% there.
The tiny selected case's earlier +7.4% does not reproduce in its final
confirmation, but no benefit for that fixture is established.

MIN/MAX remains uncertain: its median changes are +12.0%, +9.9%, and +3.3% in
the successive protocols. Final paired ratios range from 0.881 to 5.824, and its
paired geometric-mean ratio is about +29% because of an extreme excursion.
The final samples do not reproduce a >5% median regression, but they also do
not prove absence of a smaller regression. This is not evidence that excluded
paths are performance-identical. Their reader is unchanged, while shared stream
routing and function layout changed. The implementation is admitted for the
measured larger eligible workload improvement with this limitation explicit.

Four separate CPU profiles corroborate the intended mechanism. In the eligible
four-lane query, nativeAggregateBits accounts for 26.77% cumulative samples in
the baseline and has no candidate samples; the bound reader appears at 7.89%.
Map access falls from 23.42% to 0.72% cumulative samples. MIN/MAX still executes
the fallback reader. Its profiles do not establish a cause for the uncertain
timing difference. Profile percentages have different denominators, overlap
when cumulative, include calibration/setup, and are not absolute speedups.

See `paired-summary.csv`, `followup/`, `confirmation/`, `profiles/`, and the final
independent review. All 632 samples, summaries, source identities and memory
rows were independently reconciled. No correctness or lifetime defect was found.

## Memory evidence

Separate untimed executions measure query-charged peak and verify release for
every oracle case and its repeat. All 108 before/after entries reconcile with
raw logs: 42 cases add 224 bytes for two lanes, 54 add 448 bytes for four lanes,
and 12 excluded controls add zero. The new descriptor charge is exactly 112 bytes
per eligible lane on this amd64 build. Its delta stays the same at 32,771 and
262,147 rows. This is additional bounded state, not a memory reduction.

Absolute baseline peak can still grow with input/morsel shape. In these tail
fixtures the existing row-sensitive workspace grows by 8,176 bytes between the
two large sizes, separately from the fixed descriptor delta. Query and engine
charged usage return to zero after each checked execution. Charged peak excludes
fixture/source storage, uncharged heap/runtime overhead and RSS. It is not total
process memory or a long-duration leak proof. B/op and allocs/op from timing are
separate complete-query heap measurements and include allocator size classes
and reservation bookkeeping.

For the longer materialized eligible cases, the measured heap cost is about
+344 B/op for two lanes and +568 B/op for four lanes, with four additional
allocations per execution. The extra cost is per query, not per input row.
Excluded controls have unchanged charged peak and allocation counts; their
short-run B/op samples vary by a few bytes. This stage trades a small, explicitly
charged descriptor buffer and bookkeeping for reduced repeated lookup work.

## Reproduction and publication

Use Go 1.27.3 and the repository dependencies, preserve a baseline source tree,
and run `scripts/bench_global_binding.sh baseline`, then `candidate`, then
`measure`, with GLOBAL_BINDING_BASELINE and GLOBAL_BINDING_BIN_DIR set as the
script describes. The runner refuses to overwrite frozen binaries or measured
rounds. `summarize.py` produces descriptive paired summaries from the raw rounds.

This checkout is a source archive, so builds use GOFLAGS=-buildvcs=false. Baseline
source files were verified against the GitHub tree before freezing. Historical
validation omissions and the tracked old test binary are not deleted. Local
test executables and binary profiles are excluded from publication; textual
evidence and hashes are retained. Changes are limited to the requested private
performance branch with expected-head checks, no force update and no main merge.
