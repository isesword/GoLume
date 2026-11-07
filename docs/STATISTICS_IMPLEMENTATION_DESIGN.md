# Statistics status and exact median/quantile design

## Current scope

This document separates implemented fundamentals from the **design-only** exact
median/quantile milestone. It does not add query APIs or promise those operations
are implemented. Current implementation details and validation limits live in:

- [STATISTICS_STATES.md](STATISTICS_STATES.md): typed sample/population variance
  and standard deviation, fixed-width first/last, resident/partitioned/sorted
  execution and V4 spill. Moment states use exact integer anchors and scaled
  deviations, rather than the simple raw-M2 prototype originally proposed here.
  Endpoints carry stable source ordinals. Arbitrary query-level ddof and
  variable-length string/binary endpoints remain unsupported.
- [NATIVE_AGGREGATION.md](NATIVE_AGGREGATION.md) and
  [MULTI_COLUMN_AGGREGATION.md](MULTI_COLUMN_AGGREGATION.md): independent typed
  lanes, global aggregation, native outputs and existing compatibility limits.
  The old statement that all aggregates share one Int64 column is obsolete.
- [ORDERED_TRANSFORMS.md](ORDERED_TRANSFORMS.md): numeric shift/diff, including
  budgeted future lookahead and cross-batch/partition state, plus FillNull and
  FillNaN convenience constructors. Forward/backward fill and nonnumeric
  shifting remain outside this stage. Numeric shift/diff and value filling are
  implemented; see the linked document for their supported execution paths.
- Exact `n_unique` is implemented in this stage and documented in
  [EXACT_DISTINCT.md](EXACT_DISTINCT.md), including its tuple-sort fallback and
  budget limits. Median/quantile use the design below; they must preserve
  duplicates rather than collapse them as a distinct operator would.

Legacy aggregate structs and codecs must retain their current compatibility.
A new variable-size order-statistic state cannot be squeezed into the old
`aggregate.Int64Result`, or into V4 fixed-size statistics state by reinterpreting
payloads. The typed descriptor/state/output framework is now the integration
point, rather than a prerequisite to build from scratch.

## Exactness and proposed public contract

“Exact” means selecting ranks from all eligible input observations with their
multiplicities. It does not mean arbitrary-precision interpolated output or
bitwise Polars equivalence. No sketch, sampling, approximate quantile, average
of worker medians, or Float64 conversion before rank selection is permitted.

Proposed initial scope: scalar q per requested output, ten numeric input dtypes,
global or grouped reductions, and `median` as q=0.5 with midpoint interpolation.
No string/bool/temporal quantiles or rolling/window quantiles in this milestone.
Several outputs over the same input can share retained values or runs.

Recommended dtype policy is Float32 output for Float32 input and Float64 output
for integers/Float64, for every interpolation mode. This is consistent with the
existing moment output policy and avoids per-mode schema changes. It deliberately
allows rounding of selected UInt64/Int64 values in final floating output; retain
native bits until that final conversion. The exact reference dtype matrix must
be pinned before API implementation. A dtype-preserving rank-selection API, if
wanted, should be a separately named operation rather than an undocumented
change to nearest/lower/higher quantile outputs.

NULLs are omitted. Empty/all-null global input produces one nullable result;
empty grouped input produces no group. NaN remains a valid observation, sorts
after +Inf, and contributes to the non-null count. Any selected NaN produces a
valid NaN, not NULL. Normalized -0/+0 compare equally; preserve deterministic
native payload selection through a source ordinal tie-breaker. NaN payload bits
are not a portable compatibility guarantee; canonicalize NaN output. Counts and
ranks use checked uint64 arithmetic.

## Rank and interpolation definitions

Validate q as a finite Float64 in [0,1] during binding; reject invalid mode/q
before reading input. Treat q as its exact represented binary value when
computing ranks. Do not form `float64(n-1)*q` for large n: above 2^53 that loses
integer ranks. Use bounded integer significand/exponent arithmetic (or an
accounted arbitrary-precision control-plane calculation) for quotient/remainder.

For n>0, let h=(n-1)*q, i=floor(h), j=ceil(h), and t=h-i:

- lower: x[i]; higher: x[j].
- nearest: x[floor(h+0.5)], with half ties toward the higher rank.
- midpoint: x[i] if i=j, otherwise the arithmetic midpoint of x[i], x[j].
- linear: x[i] if i=j or t=0; otherwise (1-t)*x[i]+t*x[j].
- equiprobable: x[max(ceil(n*q)-1,0)], with q=1 selecting n-1.

These are proposed explicit definitions, not inferred from a handful of examples.
Confirm all tie/end cases against pinned Polars before declaring compatibility;
if the reference differs, record the deliberate policy rather than quietly
changing rank computation. For n=1 every mode selects the sole value.

The existing Polars 1.35.2 fixture
`testdata/fundamentals/polars-1.35.2.json` establishes useful examples: `[1,3]`
at q=.25 gives 1 for nearest/lower/equiprobable, 3 for higher, 2 for midpoint,
and 1.5 for linear. Its median is 2. Nulls are omitted; the median of
`[NaN,NaN,null,2]` is NaN. These examples alone are not full interpolation tests.

## Arithmetic, stability and non-finite results

Compute midpoint without overflowing native integer addition. For integer
endpoints, form exact signed magnitude differences or widened intermediate
arithmetic, then perform one documented final floating conversion; do not
subtract unsigned values before comparing them. Linear interpolation must avoid
intermediate overflow when the final value is representable, using scaled or
convex combination arithmetic with a high-precision test reference. Float32
interpolation should use Float64 intermediates and round once at output.

Handle endpoints before arithmetic: t=0/1 returns the corresponding endpoint,
and equal infinities return that infinity. An interior combination of a finite
value and one infinity returns that infinity; -Inf with +Inf returns valid NaN.
A selected NaN returns valid NaN. These are recommended explicit semantics,
including sensible behavior where naive `0*Inf` would corrupt an endpoint.
Observe reference behavior and record any numerical improvements separately.
For equal signed zeros, use stable selected payloads for selection modes;
interpolated zero sign follows a documented arithmetic rule, tested explicitly.

Order comparisons use native integer widths, unsigned comparisons for UInt64,
and one float total-order policy across sort/merge/rank selection. A worker's
completion order must not affect ties or selected bits. Source ordinals break
value ties, remain unique across batches/partitions, and survive spilled runs.
This is especially relevant to ±0. Floating interpolation may require a stated
ULP tolerance, but nullness, ranks and non-finite classification are exact.

## Bounded in-memory and external execution

Resident mode retains typed observations in explicitly sized, budgeted buffers.
Charge capacity before allocation, including replacement overlap, validity/key
metadata, permutation/selection workspaces and output. A typed selection
algorithm may replace full sorting for a few ranks, but needs a bounded
worst-case fallback. Sorting once can serve many quantiles. Sorted group input
bounds active groups, not the size of one group's values.

External mode writes bounded sorted runs of `(group key, typed value, ordinal)`.
Group equality and ordering must follow existing typed key canonicalization.
Null values need no value record, but retain group-existence metadata so an
all-null group still emits its NULL result. Count eligible values per group with
checked arithmetic; do not assume that metadata for every group fits in RAM.
Store counts in budgeted state or sorted count runs merged by group.

After counts are available, compute required ranks once per group, then merge
value runs in group/value/ordinal order and retain only requested endpoints.
Carry duplicate observations unchanged. One hot group can span arbitrarily many
runs without requiring its values in memory. Repartitioning by group hash alone
is insufficient: the hot group will never shrink. External merge sorting is the
bounded fallback; any repartition optimization needs a measurable progress guard.

Use bounded fan-in and multiple merge passes when the run count exceeds memory
or file-descriptor limits. Charge the merge heap, per-run readers, decompression
buffers if any, writer buffer, group/key scratch, selected endpoints and output.
A run manifest must itself be bounded or spillable; a Go slice of all run handles
is not free. Early stopping is permitted only after all needed ranks and group
outputs are established; promptly delete no-longer-needed owned runs.

Two-pass source replay is optional only for a declared stable replayable source.
The general path spools values/counts and works with single-pass sources. Never
re-read an unstable source to obtain counts for previously seen values. Parallel
workers produce compatible typed runs and count contributions, not finalized
quantiles. Schema, dtype, interpolation parameters and codec versions are checked
before merge/restore.

## Memory, disk and failure contract

Use the existing query budget and `memory.QuerySpillStore` with explicit
`SpillOptions{Directory, MaxBytes}`. MaxBytes must be positive; zero does not
mean unlimited disk. Include headers/checksums and simultaneously live old/new
runs in disk admission, not just input payload size. Multi-pass merges may need
replacement-run overlap; fail with `ErrSpillQuotaExceeded` if quota is exhausted.
Do not delete original runs before a replacement is durably committed.

Reservation failures must occur before oversized allocations. An external mode
still needs a minimum working set; report admission failure if that cannot fit.
No automatic unbounded materialization, temporary untracked files, or silent
approximation on resource failure. On cancellation, corrupt/truncated frames,
count overflow, write/sync/rename/read errors and Close, release all query-owned
memory and run handles. Repeated Close is safe. Do not expose partial terminal
results as a successfully completed quantile query. Returned materialized values
must own their data and remain valid after query closure.

Define a new typed order-statistic run codec rather than mutating V1–V4 formats.
Validate lengths, type/key descriptors, counts, ordinal encoding and sortedness
where required. Corruption must not become unchecked indexing or allocation.
Disk quota is shared with other operators in the same query, not independently
recreated per quantile lane.

## API integration and unresolved approval boundary

A no-options `Median(column, alias)` can be additive to the existing three-field
`expr.Aggregate`. Quantile needs q/mode parameters; do not add fields to public
positional-literal structs, hide parameters in alias/column strings, or use a
mutable global registry. Recommended direction: a separate configured aggregate
specification plus additive engine/lazy entry points accepting legacy and
configured specs. Binding normalizes both into immutable typed descriptors.
Aliases do not define state identity; compatible outputs may share storage.

Before implementation, settle the exact name/shape of that additive API and
confirm the uniform floating output and interpolation policy above. This is a
product/API review boundary, not a reason to implement median alone by bypassing
the future quantile contract. This milestone authorizes design only; no new
median/quantile code or publication is implied by this document.

## Future acceptance matrix

1. Pin reference cases for every numeric dtype, mode and q boundary; n=0/1/2,
   odd/even counts, half ties, duplicate values, all-null/all-NaN, mixed ±Inf,
   signed zero, integer extrema and values adjacent above 2^53.
2. Test rank arithmetic independently for synthetic counts near 2^53, MaxInt64
   and MaxUint64 without allocating that many rows; compare exact rational rank
   calculations and validate count overflow.
3. Compare native selection and high-precision interpolation references;
   exercise finite results whose naive midpoint/difference would overflow.
4. Require resident, sorted, parallel and external agreement on groups/ranks,
   nullness and non-finite classification. Vary batches, partitioning, worker
   completion, dictionary domains, selection density and 63/64/65 boundaries.
5. Test one massive hot group, many tiny groups, all-null groups and mixed
   quantile outputs. Bound reader fan-in/manifest memory; force multiple passes.
6. Inject memory/disk admission failures and IO/corruption/cancellation at each
   stage; assert no retained reservations/files and safe close/retry contracts.
7. Test single-pass and unstable sources, schema parity, output ownership and
   legacy API/codec preservation. Distinguish unsupported forced strategies
   from supported ones in explain output and errors.
8. Measure native typed sorting/selection/merge and end-to-end costs by dtype,
   cardinality, skew, null density and spill volume. SIMD candidates must preserve
   total ordering and active-lane safety; scalar reference and per-architecture
   correctness/performance gates precede any Auto dispatch. Exact quantiles are
   not inherently a SIMD reduction.
