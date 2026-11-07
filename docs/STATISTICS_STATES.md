# Fixed-size statistics and query semantics

Fixed-size moments and endpoints are integrated with native typed query results,
lazy collection, resident/partitioned/sorted aggregation and V4 spill state.
Existing reduction behavior and SIMD dispatch gates remain unchanged.

## Moments

`MomentsState` accepts native numeric bits and skips NULL rows. It counts NaN
and infinities as observations, returning a valid NaN when `n > ddof`; otherwise
the result is NULL. The caller supplies ddof explicitly; one is the conventional
sample default, and zero is population variance. Float32 output conversion is a
finalization concern, so intermediate statistics remain Float64.

Integers are centered on their exact first value. Ordered unsigned subtraction
forms signed deltas before converting to floating point, retaining unit-sized
differences near both Int64 and UInt64 limits. Floating values are centered on
their first finite value. For extreme ranges where a centered coordinate would
overflow, the state switches to an absolute convex mean.

Welford/Chan squared deviations are represented as `scale² * scaledM2` rather
than an overflowing raw M2. Merges transport these intermediate values, never
finalized variance. `RestoreMoments` validates external state. Add and Merge
reject count overflow without modifying the prior state. Reset retains type.
These fixed-size states allocate no backing storage; integrating state vectors
must reserve their full capacities and replacement overlap before allocation.

Variance is finalized with exponent-aware scaling. Standard deviation is
computed directly from the scaled state, not by taking the square root of an
already overflowed variance. Finite distributed results still have rounding
differences with encounter and merge order. Compare with appropriate tolerances,
while comparing nullness and non-finite classification exactly.

### Deliberate numerical improvements over the reference

The separately reproducible `scripts/statistics_polars_oracle.py` records seven
observed Polars 1.35.2 cases in
`testdata/fundamentals/statistics-polars-1.35.2.json`. It does not define GoLume's
desired answer where that reference loses information:

- For `[-1e154, 1e154]`, Polars returns infinite population variance and standard
  deviation. Scaled state returns variance approximately `1e308` and std `1e154`.
- For `[-MaxFloat64, MaxFloat64]`, the representable population variance is
  infinite, but population std is finite `MaxFloat64`. The state returns these
  independently; the reference reports infinity for both.
- Three consecutive Int64 or UInt64 values at their maximum have sample
  variance 1 and population variance 2/3. The exact-anchor state preserves that;
  Polars 1.35.2 reports zero after losing unit differences.

Go tests consume those reference observations and assert the deliberate target
differences. Additional exact-rational tests cover integer extrema, merge
permutations, random integer/float inputs, subnormal ranges and overflow.
This is numerical accuracy, not a claim of bitwise Polars compatibility.

## Fixed-width endpoints

`EndpointState` uses a stable `SourceOrdinal{Partition, Row}` where Partition is
the logical source-order partition index, never a worker or hash partition ID.
Row is its original offset before filtering. FIRST/LAST retain boundary NULLs;
they do not search for a non-null value. Merging compares source ordinals, so
worker arrival order does not define the result. Reusing a tracked endpoint
ordinal for a conflicting payload fails. Input producers must ensure uniqueness
and preserve ordinals through sorting and spilling; a batch-local row index or
group ID is insufficient.

The state preserves raw numeric, bool and fixed temporal bits. Variable-length
string/binary endpoints remain unsupported by this foundation and need separately
owned, budgeted payloads. Exact N_UNIQUE similarly needs variable-state and spill
contracts; an unaccounted per-group map is not an acceptable implementation.

## Query integration

`expr` and `lazy` expose `Var`, `Std`, `VarPopulation`, `StdPopulation`, `First`
and `Last`. Sample constructors use ddof=1; population constructors use ddof=0.
Arbitrary ddof is supported by the state primitive but is not yet a query option.
Variance and standard deviation preserve Float32 output for Float32 input, as
in Polars1.35.2, and produce Float64 for integer and Float64 input. Float32
conversion occurs only at finalization; intermediate moment state is Float64.
First/Last preserve numeric, bool or fixed-width temporal dtype. String/binary
endpoints and exact N_UNIQUE are not yet query APIs.

The public three-field Aggregate and six-field legacy Layout are unchanged.
Independent StatisticsLayout lanes contain real moment/endpoint state, including
stats-only queries with no numeric lane. Native ExecuteTyped and lazy columnar
collection expose these results; legacy materialized Execute requires the typed
API. Global empty input emits one row with NULL statistics and zero counts.

Endpoint queries require stable pipeline input order even when the requested
pipeline is relaxed. A checked global row counter is assigned before predicate
selection or hash repartition, with SourceOrdinal{Partition:0, Row:counter}.
This flattening preserves the source's logical partition/row order. Spill runs
carry full ordinals; merge compares them independently of worker arrival order.
Sorted aggregation uses the declared source ordering and does not sort the input.

V4 spill frames numeric and statistics descriptors separately and supports zero
numeric lanes. V1/V2/V3 formats remain unchanged. Full raw moment state and both
endpoints survive spill/restore; restore validates descriptors, payload flags,
endpoint ordering and normalized native bits. State capacities, growth overlap,
codec payload and output allocations participate in the query budget.

Moment recurrences and ordinal comparisons currently use scalar typed kernels.
No SIMD path is forced: accuracy and merge fidelity take precedence, and any
future vector implementation requires per-architecture measurements and semantic
differential tests before a dispatch gate is enabled.

### Diagnostic performance measurement

On the development Linux/amd64 AMD EPYC 9V74 host, three 200ms runs of
`BenchmarkStatisticsAggregate` (4,096 rows, 64 groups, one shared UInt64 lane
serving sample variance/std and first/last, materialized typed results) measured:

- bitmap-direct: 0.635–0.671 ms/op, about 72,083 B/op and 464 allocations/op;
- partitioned-hash with two workers: 0.494–0.536 ms/op, about 123,014 B/op and
  545 allocations/op.

These are end-to-end diagnostic measurements, not an architecture-independent
speedup claim or a SIMD admission gate. The existing anchored moment primitive
also has a zero-allocation benchmark; run both on each target before changing
kernel dispatch.
