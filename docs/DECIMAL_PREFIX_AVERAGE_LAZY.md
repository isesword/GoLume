# Explicit Decimal prefix average (N28)

```go
frame.WithDecimalPrefixAverage(
    "amount", "sequence", "running_average", 18, 4,
    aggregate.DecimalAverageHalfEven,
)
```

The method signature is:

```go
func (f LazyFrame) WithDecimalPrefixAverage(
    column, orderBy, alias string,
    precision, scale int32,
    rounding aggregate.DecimalAverageRoundingMode,
) LazyFrame
```

The rounding argument must be an explicit supported `aggregate.DecimalAverageRoundingMode`; output precision/scale must be valid Decimal128 metadata. This API does not extend `WithWindows`, `RollingAvg`, or their existing admission rules.

## Input and execution contract

- The input is a Scan followed only by direct column Projections, with exactly two columns at the prefix operator: one plain required Int64 order key and one native Decimal128 value. An original wider Scan may be projected to these two columns.
- The source must provide ascending ordering proof for that key and a bounded, non-spilling stream plan. Names alone are not an ordering proof. The operator validates strict ascending order, including batch boundaries; duplicate keys are errors, not peer groups.
- No partition keys, implicit sorting, FILTER, Sort, TopK, Limit, expression projection/rename, joins, or other upstream computations are accepted. The original logical plan is validated before optimization, so a constant-true filter cannot disappear to bypass this restriction.
- Execution must explicitly select MemoryOnly and use a finite session budget. Auto and Bounded are not synonyms for this contract. Runtime child, state and result pages share finite query admission.
- The frame is exactly ROWS UNBOUNDED PRECEDING THROUGH CURRENT ROW. Output retains both input columns and adds a nullable Decimal128 column with the explicit precision/scale. All-NULL prefixes are NULL. Existing six rounding modes apply; no float conversion is used.

## Planning and output ownership

A dedicated logical node carries immutable scalar configuration. Projection pruning retains both input dependencies and the prefix node even when an outer consumer only requests the key or counts rows. Predicates cannot move from above the prefix into its input. Explain includes all configuration, including rounding, precision and scale.

The N27 physical operator remains one whole ordered stream and reports one partition. The lazy adapter neither partitions it nor requests soft page splitting. Existing pipeline byte/row bounds and finite memory admission still apply.

The columnar terminal paths Prepare/Execute, Collect, Batches, Analyze and IPC/Parquet sinks use the existing row-pipeline preparation and ownership rules. Collect reserves retained output before cloning each page and returns owned data; finite MemoryOnly does **not** make Collect constant-memory. Accumulating more output than its budget fails. Batches are borrowed views invalidated on advance/Close. Sinks retain their existing atomic-file completion rules. Native row materialization through the legacy aggregate Result interface is not newly enabled; supported downstream aggregate summaries still execute and validate the prefix rather than eliminating it.

Failure is page-atomic, not whole-stream rollback: an error within a page publishes none of that page, but already consumed earlier pages cannot be withdrawn. Duplicate keys, coefficient precision errors, output rounding overflow, budget exhaustion and cancellation remain errors. Partial consumers may stop before later input is read, as in the existing stream contract.
