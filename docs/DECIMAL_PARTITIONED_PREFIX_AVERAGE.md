# Explicit partitioned Decimal ROWS prefix average

N36 adds independent `logical.PartitionedDecimalPrefixAverage` and
`physical.PartitionedDecimalPrefixAverage` types, without changing the exported
N28 structs or the old unpartitioned API. The Lazy entry is:

```go
frame.WithPartitionedDecimalPrefixAverage(value, partition, order, alias,
    outputPrecision, outputScale, rounding)
```

The physical entry is `NewPartitionedDecimalPrefixAverageSource(input, spec)`.
Its explicit Frame must be ROWS UNBOUNDED PRECEDING THROUGH CURRENT ROW. Output
preserves the three input columns and appends a nullable native Decimal128 alias
with the requested precision/scale. Configuration is validated even for empty
input; the alias must not exist and the three input names must be distinct.

## Admission and ordering

Exactly three operator input fields are admitted: one plain Int64 partition
(nullable or required), one required plain Int64 order column, and one native
Decimal128 value. Lazy lineage permits only Scan and direct column Projection,
not expression projections, filters, sorting, limits, joins or other upstream
operators. A projection may select these three columns from a wider scan. The
original scan must provide the leading composite ordering witness
`partition ASC NULLS FIRST, order ASC`; projection cannot invent that witness.
Lowering restores the proven identity-column witness only after logical
validation. Runtime checks independently verify partition nondecrease, NULL-first
placement and strictly increasing order within each partition, across every page
and empty page. Duplicate order values are allowed in different partitions, never
within a partition. NULL partition payload bits have no semantic identity.

Only finite MemoryOnly execution is accepted by the Lazy contract. Physical
streams require a finite manager and a non-spilling bounded child. There is no
implicit sort, FILTER, spill, Auto selection, general window or aggregate-dispatch
extension. A supplied ordering promise is not blindly trusted at runtime.

## Arithmetic and lifetime

A single signed-256-bit sum/count state is reset only when the partition changes.
NULL values do not contribute; an all-NULL prefix yields NULL. Each output is
rounded once using the existing Decimal average finalizer and explicit rounding
mode. No intermediate Decimal128 SUM finalization or rounded-output feedback is
used. No seen-partition map or cardinality-dependent retained state exists.

A page is published only after all rows in it pass shape, coefficient, ordering,
precision and cancellation checks. A failing page publishes no rows and ends the
stream; previous successful pages are not retracted. Errors, EOF and Close release
page and fixed state charges and close the child. Close errors are propagated;
terminal cleanup also observes cancellation raised during release/child Close.
This is page atomicity, not transactional materialization of the entire stream.
Outputs borrow the input vectors and own only the appended average vector. They
are valid until the next Next call or Close, not beyond either operation.

## Budget and optimizer

Before opening its child, the operator reserves 768 bytes for fixed accumulator,
composite witness and stream bookkeeping. Appended output reserves
`16*rows + 8*ceil(rows/64)` before allocation. The child receives the same finite
ReadOptions manager and retains its whole decoded page simultaneously with the
average output. The output payload cap includes both Int64 inputs, Decimal value,
new Decimal output and present validity bitmaps. Child page boundaries and row
limits are preserved; soft payload-target splitting is not supported.

This is tracked GoLume ownership, not a claim about total process RSS or private
allocations of arbitrary user-defined sources. A source must obey the supplied
manager contract to obtain complete tracked upstream accounting.

Optimizer traversal preserves the node and all three input dependencies even
when its average output is unused. Predicate propagation stops at the node; an
outer filter stays above it. Count-only projections retain a carrier column
rather than producing an invalid empty physical projection. Explain reports the
partition, order, value, alias and full rounding configuration. Legacy N28
behavior and unrelated execution strategies remain unchanged.
