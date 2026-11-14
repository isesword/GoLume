# CountRows after SelectExprs: correctness repair

Originally developed against ec773430; integrated on the Bloom feature branch.

## Cause and limited fix

CountRows has no value-column dependency. `optimizer.pushProjection` correctly
prunes every unused SelectExprs output but formerly retained an empty
ExprProjection when IncludeInput=false. Final schema inference rejects that
invalid node. Pure Int64 inputs reproduce this; it is not a Decimal aggregate
capability restriction.

All expressions are still schema-validated before optimization and bound before
pruning. The existing dead-output contract in EXPRESSION_EVALUATION.md permits
removing unused runtime computations, even fallible casts. Required values,
filter dependencies and group keys remain demanded.

When no expressions remain, IncludeInput=true keeps its existing elimination.
For IncludeInput=false, retain one safe I64(0) expression under the original
first output name. This is an internal row-cardinality carrier, not a requested
output. It prevents hidden source fields, especially native Decimal, from
leaking into downstream schema admission. The existing rule label
`expression-projection-elimination` describes eliminating original work; the
SelectExprs schema barrier is retained. Reoptimization retains the same carrier.
A safe literal entails some work; no performance improvement is claimed.

Two alternatives were investigated, not shipped: returning the child directly
can expose hidden native Decimal fields; a zero-column ordinary Projection is
logically valid but physical lowering rejects it. General zero-column physical
projection support remains out of scope.

## Validation

New optimizer and lazy `countrows_projection_test.go` tests cover:

- empty input, three rows, and 130 rows in two 65-row source batches;
- scalar/requested SIMD modes, aliases, nested projections, limit, filters
  before and after projection, nullable CountColumn and nullable grouping;
- unused runtime-overflow expression removed, missing dependency still rejected,
  demanded CountColumn/filter/group cast reporting its actual overflow;
- hidden native Decimal source with Int64 SelectExprs, outer constant output,
  no externally visible carrier, unchanged original plan and reoptimization;
- mixed UInt64/Int64 explicit Decimal subtraction followed by CountRows=3,
  ordinary and bounded sessions; bounded configuration does not prove spill;
- existing explicit empty Select physical rejection is recorded, not repaired.

Focused ordinary and GOEXPERIMENT=simd commands:

```
go test -json ./optimizer ./lazy -run 'CountRows.*Projection|CountRowsEliminates|UnusedFallibleProjection' -count=1
```

Evidence is in the companion countrows-validation directory: before.log is the
baseline failure, after.log is the early direct-elimination experiment (not final
acceptance), zero-projection-experiment logs record the rejected alternative,
and focused-scalar.jsonl / focused-simd.jsonl are final focused results. Full
repository gates and independent review are tracked separately by the parent.

The historical Decimal downstream archive retains its original observed-error
assertions. The integrated downstream tests explicitly replace those expectations
with correct row counts; the independent repair tests cover the optimizer contract.
Native Decimal value aggregation, Join and sort capabilities are not broadened.
