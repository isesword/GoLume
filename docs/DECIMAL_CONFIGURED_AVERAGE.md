# Explicit global native Decimal AVG (N20 candidate)

Use the additive configured aggregate entry point:

```go
frame.AggConfigured(
    lazy.AvgDecimal128("amount", 18, 4, expr.DecimalRoundHalfEven, "mean"),
    lazy.CountColumn("amount", "valid"),
)
```

The matching `expr.AvgDecimal128(column, precision, scale, mode, alias)` produces
an `expr.DecimalAverageSpec` for `expr.ConfiguredQuery` and
`Engine.PrepareConfigured`. A prepared global result can use the existing
`PreparedQuery.ExecuteDecimal` result API. The old positional `expr.Aggregate`,
`expr.Query`, and scalar `TypedValue` layouts do not change. Existing bare `Avg`
is not silently reinterpreted as native Decimal AVG.

This slice is global-only, over root native Decimal columns. Configured AVG may
coexist with native Decimal SUM and existing COUNT(*) / COUNT(root Decimal).
Each AVG has its own explicit target precision (1..38), scale (0..precision),
and one of the six existing expression rounding modes: toward zero, away from
zero, floor, ceil, half even, half away from zero. Mode zero is invalid. Input
scale comes from the validated native column schema; target scale need not equal
it. Options are validated even when filtering leaves no selected input. No
implicit rounding, output scale, float conversion or truncation is introduced.

For exact coefficient sum S, non-NULL count C, input scale a and output scale b,
the output coefficient is the once-rounded value of S * 10^(b-a) / C. NULL input
is ignored. Empty/all-NULL selected input yields one NULL AVG row; COUNT retains
its existing zero behavior. A nonempty exact cancellation yields a valid zero.
The wide sum is never narrowed before division: [M,M] can have legal AVG=M even
when its SUM would overflow precision38. A SUM requested alongside that AVG must
still fail on its own final overflow; AVG does not suppress other lane errors.

The N19 fixed384-bit finalizer performs exact quotient/remainder rounding and
checks target precision after rounding. Per-alias state remains fixed in input
row count and a global plan remains single-worker aggregation. Configured spill
availability does not imply aggregate state spilling. Existing strategy/budget
validation and source/predicate/exchange/output ownership still apply. This is
logical admission accounting, not a proof of total allocator/GC/RSS usage.

Collect, IPC, Parquet and the separate Decimal result interface must preserve
each AVG alias's configured precision/scale and exact coefficient, rather than
forcing precision38/input-scale. Specifications are copied by configured query
builders; later mutation of a caller's slice must not change a saved plan. Plan
explanations include the configured target metadata and mode.

Grouped AVG, nested-child AVG, window AVG, parallel state merging and aggregate
spill are not opened by this change. Ordered/resident grouped SUM/COUNT retain
their previous scopes. Legacy scalar materialization remains unable to represent
native128-bit outputs and is not enlarged or converted to float. No serialized
query transport guarantee is added beyond the existing configured interfaces.

## Verification boundary

The new lazy tests use an independent unbounded integer oracle, all six modes,
positive/negative ties, source/target scale changes, smaller target precision,
NULL/empty/filter-empty cases, aliases and copied specifications. They retain the
64MiB query budget, exercise batch1/64 and one-byte rejection, [M,M] legal AVG
with sibling SUM overflow, canceled/repeated calls, and actual native Parquet
input -> AVG -> IPC/Parquet output with metadata/coefficient inspection. The
Parquet output is re-read through the native reader and checked after session
close; IPC is independently read through Arrow arrays. This is not a claim that
every source.Close failure or sink commit failure has been injected.

The protected repository test remains outside the explicit safe test closure.
Scoped gate results do not assert a full-repository pass, erase historical
failures, or publish this candidate.
