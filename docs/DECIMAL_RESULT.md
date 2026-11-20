# Exact public Decimal results (N16 candidate)

`Engine.ExecuteDecimal(ctx, source, query, options)` and
`PreparedQuery.ExecuteDecimal(ctx, options)` return a new `DecimalResult` for the
existing dedicated global native Decimal SUM and SUM+COUNT plans. This is a new
explicit result interface, not a new aggregate capability or a change to
`TypedValue`, `Result`, numeric codecs or their old admission fees.

```go
result, err := prepared.ExecuteDecimal(ctx, engine.ExecuteOptions{Materialize: true})
if err != nil { /* handle */ }
value := result.Rows[0].Aggregates[0].Value
coefficient, precision, scale, valid := value.AsDecimal128()
// Exact signed128 coefficient; no float or decimal-string intermediary.
```

`DecimalResult.Rows` contains exactly one global row when Materialize is true,
including empty-input queries. Its ordered `DecimalAggregateValue` entries carry
resolved output aliases (an empty query alias becomes `agg_i`), aggregate Kind
and a `DecimalValue`. The latter has Type, Valid, Coefficient, Precision, Scale
and Uint64 fields. SUM uses nullable Decimal(38,input scale); COUNT is valid
Uint64, including zero for empty input. NULL sums retain precision/scale in the
fields, but `AsDecimal128` returns zero,0,0,false for NULL or incompatible dtype.
`AsUint64` rejects NULL or Decimal, never truncating a coefficient.

Rows and copied aliases are caller-owned and remain usable after the source,
prepared query and engine close. Copies from the borrowed stream are admitted
while its state/output ownership is still charged. Returned ordinary Go storage
retains no query lease, following existing eager result ownership; this does
not make a query budget a process-wide heap limit or charge caller retention
forever. Errors return an empty result, not partial materialization.

Materialize=false returns no rows, only Summary. Summary.Rows is nil; selected
rows include predicate-true NULL rows, GroupCount is one, and Checksum is zero,
not a digest of Decimal coefficients. Legacy Execute and ExecuteTyped with
Materialize=true still reject these plans. Their public layouts and typed
accessors remain unchanged. Count-only native Decimal, grouped aggregation,
other aggregate mixtures, arbitrary container Decimal and aggregate state spill
are not authorized by this endpoint.

Independent external-package tests compare both public wrappers to exact
coefficient/count oracles and the existing lazy native result path, including
NativeNested Parquet input, empty/NULL/filter cases and post-close ownership.
Full gates and historical limits must be reported separately. Preparation
metadata, third-party source memory and granular cancellation retain their
previous boundaries; no performance claim is implied.

N17 separately adds [ordered single-Int64-key grouped SUM/COUNT](DECIMAL_GROUPED_SUM.md)
through BatchStream/Lazy only. Global identities, fees and ExecuteDecimal scope
remain unchanged; the new grouped path is not a general hash/spill aggregate.

N18 separately permits [unordered resident Decimal grouping](DECIMAL_RESIDENT_GROUPED_SUM.md)
under explicit finite-budget ExecutionMemoryOnly. N17 ordered behavior is retained;
the new path is O(groups), has no result-order promise and does not expand
ExecuteDecimal beyond global plans.
