# Exact numeric median and quantiles

## Additive API

`expr.AggregateSpec` accepts existing `expr.Aggregate` values and the new
`expr.QuantileSpec`. Existing positional `Aggregate` and `Query` structs are
unchanged. Use `Engine.PrepareConfigured` with `expr.ConfiguredQuery`, then the
normal prepared `ExecuteTyped` or stream API. A bare `AggQuantile` aggregate is
rejected because it lacks options.

```go
q := expr.ConfiguredQuery{
    GroupBy: []string{"category"},
    Aggregates: []expr.AggregateSpec{
        expr.Median("value", "median"),
        expr.Quantile("value", .9, aggregate.QuantileLinear, "p90"),
        expr.Count("rows"),
        expr.NUnique("value", "unique"),
        expr.Var("value", "variance"),
    },
}
prepared, err := engine.PrepareConfigured(ctx, input, q, engine.PrepareOptions{})
```

For lazy queries use `frame.GroupBy("category").AggConfigured(...)` or
`frame.AggConfigured(...)` for global aggregation. `lazy.Median` and
`lazy.Quantile` are convenience constructors. Existing aggregates can be mixed
in the same call. Configured aggregate outputs support downstream filters,
projections, joins, sorts and nested aggregation through ordinary lazy lowering.
Specifications are copied when building/binding a query; changes to the caller's
slice do not alter a prepared plan. No global parameter registry or alias-based
parameter encoding is used.

## Types, nulls and exactness

All ten numeric input types are supported. Float32 input returns Float32; every
other numeric input returns Float64 for every mode. Nulls are ignored. An
empty/all-null global input produces one NULL quantile result, while an empty
grouped input produces no groups. An existing all-null group retains its NULL
output. NaN is a valid observation and sorts after positive infinity. Duplicates
retain their multiplicity. Signed zeros compare equal; source ordinals break
ties deterministically, and rank-selection modes retain the selected zero sign.
NaN payload bits are canonicalized at finalization.

Exact means selecting ranks from every eligible observation, not approximate
sketches or arbitrary-precision output. Native integer bits are ordered before
final floating conversion, preserving UInt64 order beyond 2^53 and MaxInt64.
Selected integer outputs can still round when represented as Float64.
String, bool, temporal and window/rolling quantiles are not provided.

Median is q=.5 with midpoint interpolation. q must be finite in [0,1]. Let
h=(n-1)*q, i=floor(h), j=ceil(h), t=h-i for the non-null count n:

- `QuantileNearest`: rank floor(h+.5), half ties toward the higher rank.
- `QuantileLower`: rank i.
- `QuantileHigher`: rank j.
- `QuantileMidpoint`: x[i] if i=j, otherwise their midpoint.
- `QuantileLinear`: x[i] when i=j, otherwise interpolation with weight t.
- `QuantileEquiprobable`: rank max(ceil(n*q)-1,0).

`PlanQuantile` treats q as its exact represented binary value and multiplies its
significand by the uint64 count using fixed-width integer arithmetic. Rank,
nonzero remainder and half comparison remain exact above 2^53, including
MaxUint64 counts. Only the interpolation weight is rounded to Float64; a fractional weight that
rounds to one is clamped to the next representable value below one. Interpolation
is not an arbitrary-precision function of exact q×count, particularly for
synthetic huge counts.

Float interpolation avoids overflowing an opposite-sign subtraction by using a
compensated convex combination, including the rounding residual of 1-w and
FMA product residuals. This preserves small results near cancellation without
overflowing the endpoint difference. Exact endpoints are returned without evaluating 0*Inf.
Equal infinities remain infinite; an interior finite/infinite interpolation is
infinite; opposite infinities or a selected NaN produce valid NaN. Float32 rounds
once at finalization. Integer interpolation uses exact high/low components and
compensated arithmetic, avoiding signed/unsigned subtraction overflow and lost
small residuals. It remains floating interpolation, not a correctly-rounded
arbitrary-precision guarantee; tests use appropriate finite tolerances.

A deliberate improvement over Polars 1.35.2: the midpoint of MinInt64 and
MaxInt64 is -0.5, whereas the reference returns 0 after prematurely rounding the
endpoints. `scripts/quantile_polars_oracle.py` records 2,738 observed cases in
`testdata/fundamentals/quantile-polars-1.35.2.json`; Go tests explicitly identify
this difference. The reference includes all dtypes/modes, nulls, NaNs, infinities,
duplicates and large integers. Synthetic large-count tests use exact rational
rank calculations rather than allocating enormous inputs.

## Shared exact ordering execution

The common exact tuple sorter serves resident, partitioned-hash, sorted and
spill-requested plans through one serial fallback. A planned parallel strategy
is not a claim of parallel quantile state. Group-key ordering alone cannot bound
a hot group's quantile values. Diagnostics report `exact-global-tuple-sort` and
the shared serial policy.

The input is read once in stable logical source order. Tuples are ordered by
canonical group key, family, lane, native value order key and source ordinal.
Ordinary-row family 0 accumulates numeric/statistics inputs and non-null quantile
counts. Distinct family 1 and owned-byte-endpoint family 2 retain their existing
semantics. Quantile family 3 follows them, so exact group counts are known before
its values are traversed. Equal quantile values have distinct source ordinals;
tuple deduplication cannot collapse their multiplicity. An ordinal record exists
for each selected row, including groups whose quantile inputs are all NULL.

Each quantile input column is stored once per observation even when many q/mode
outputs reference it. Group-local counts, cursors and requested endpoints reset
between groups. There is no unbounded per-group count map, source replay or
retained hot-group value array. Ordinary aggregates, moments, owned string/binary
endpoints and n_unique can share the same pass. Finite moment rounding still
follows its separately documented tolerance contract.

ExecutionBounded quantiles require an explicit spill quota even for global or
already group-ordered input; group ordering cannot bound stored values.
Memory-only mode retains tuple-sort buffers until admission fails; it does not
promise constant memory. With configured spill, bounded runs and pairwise merge
passes support a hot group larger than available memory. Tuple-sort run/segment
metadata remains budgeted in memory and can itself fail admission; this is not
unlimited external capacity. Existing V1–V5 aggregate codecs are unchanged;
quantile tuples use the shared tuple-sort framing/checksums and validated family
payloads rather than repurposing a fixed statistics codec.

## Resource and failure behavior

Rank-state arrays, output validity/bits, scratch growth overlap, sorter buffers,
run metadata and output batches are admitted before allocation. The sorter uses
the existing query-owned spill store and finite `SpillOptions.MaxBytes`, including
live old/new runs during merging. No untracked files, unbounded quota, sampling
fallback or source replay is introduced. Large records, descriptor/length
errors, lane/order-key mismatches, excess/missing quantile observations and
count overflow fail explicitly. Cancellation, quota failure, error and Close
release query reservations and owned runs; spill deletion retries follow the
shared store/sorter contract.

All source vectors are validated before indexing. Counts and sort keys are
computed from valid rows only. The result transport preserves existing public
materialized structs; native `ExecuteTyped` and lazy columnar collection expose
the new outputs. Legacy materialized Execute retains its existing native-result
restriction. Caller-owned materialized values survive query/engine closure.

No new SIMD dispatch gate is enabled. Native comparisons, rank arithmetic and
interpolation use scalar reference paths; the existing engine kernels still
handle predicates. Any vector sort/selection candidate needs separate correctness
and per-architecture performance evidence.

## Validation scope

Focused tests cover exact rank arithmetic, all numeric dtypes, six modes,
reference fixtures, multiple shared outputs, global/grouped/empty/all-null,
mixed ordinary/statistics/distinct endpoints, strategy fallback, actual hot-group
spill, quota exhaustion, cancellation, malformed tuples and cleanup. Lazy tests
cover copying, schema validation, nested/downstream composition and preservation
of legacy-only configured routing. Full integrated gates and independent review
must accompany this stage before publication.

`BenchmarkQuantileRanks` and `BenchmarkQuantileAggregate` provide primitive and
end-to-end diagnostics. Measurements are host-specific and are not SIMD or
cross-architecture performance guarantees.

### Diagnostic measurement on the development host

Three 150ms samples on Linux/amd64 AMD EPYC 9V74, 4,096 UInt64 rows, one global
group, median plus p90, an 8 MiB memory limit and materialized typed output:

- Memory-only: 3.46–5.01 ms/op, approximately 1.79 MB/op and 204–205 allocations.
- Bounded spill: 7.62–7.93 ms/op, approximately 2.82 MB/op and 1,072–1,073 allocations.

Raw samples are in `validation/exact-quantiles/benchmark.txt`. These measure the
shared exact-sort implementation and filesystem cost, not a SIMD speedup or a
regression gate. Smaller budgets can reject required working-set/metadata
admission explicitly; a spill quota alone does not guarantee every memory limit.
