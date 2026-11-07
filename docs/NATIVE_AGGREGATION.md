# Native numeric aggregation and exact results

SUM, MIN, MAX, AVG and COUNT(column) support Int8/16/32/64, Uint8/16/32/64,
Float32 and Float64 input, independently per column. COUNT(column) also accepts
nonnumeric columns using their validity and row count only, without parsing or
materializing values. Existing group-key support is unchanged in this increment.

## Output types and semantics

- Counts return Uint64.
- SUM widens Int8/Int16/Uint8/Uint16 to Int64. Other inputs preserve their dtype.
- MIN/MAX preserve the input dtype.
- AVG returns Float64 for integers and preserves Float32/Float64 input dtype.
- Empty/all-null SUM/MIN/MAX/AVG remain NULL; counts are zero.
- NaN is a valid value: it contributes to COUNT and propagates through SUM/AVG.
  MIN/MAX ignore NaNs when any non-NaN value exists; all-NaN extrema are NaN.
- Integer SUM wraps in its output width. Float32 SUM rounds after each addition.
- Int64 AVG preserves the existing wrapped-int64-total behavior in both legacy
  and native queries. Adding another input column does not fix or alter that
  known limitation.
- Newly supported types use a separate Float64 total for AVG, avoiding narrow
  integer SUM overflow. This is not compensated or scaled summation: a Float64
  total can overflow even when the mathematical mean is finite. Floating
  encounter/merge order can change rounding, so distributed finite results
  require tolerance rather than a bitwise equivalence claim.

## Materialized API without changing existing result structs

Lazy DataFrame collection returns native typed columns directly. For eager
record results use the additive `Engine.ExecuteTyped` or
`PreparedQuery.ExecuteTyped` API with the existing ExecuteOptions.

```go
result, err := prepared.ExecuteTyped(ctx, engine.ExecuteOptions{Materialize: true})
value := result.Rows[0].Aggregates[0].Value
n, ok := value.AsUint64() // Exact, including values above MaxInt64 and 2^53.
```

TypedValue includes dtype, SQL validity, raw numeric bits and owned string data.
Its AsInt64/AsUint64/AsFloat64/AsBool/AsString accessors reject incompatible dtypes
and NULL rather than silently coercing. Float32 widens exactly through AsFloat64;
Bits retains its original IEEE representation. Unresolved dictionary keys expose
their code/domain; AsString returns false until text is available.

TypedResult.Rows owns materialized rows. TypedResult.Summary contains existing
execution counters; Summary.Rows is always nil. Materialize=false returns only
the summary. Checksum retains its legacy first-int64-input meaning; it is zero
when the first input lane is another dtype. Typed results remain usable after
the prepared query and engine close and retain no query-budget reservations.

AggregateValue and KeyPart have not gained fields, so their existing positional
literals and legacy field values are unchanged. ExecuteTyped also accepts
legacy int64 queries. Existing Execute with Materialize=true rejects newly
supported native queries with an explicit request to use ExecuteTyped or a lazy
DataFrame; it never narrows UInt64 or routes it through Float64. Summary-only
Execute works for native queries.

## Execution, memory and spill

Native lanes share the same stable group IDs and resident, parallel, sorted,
global and radix-spill operators introduced for multi-column int64. Each lane
uses typed structure-of-arrays state. Growth reserves complete replacement
arrays before allocation, including old/new overlap. Typed result collection
shares the same terminal stream, rather than executing the query again, and
charges row/value/string storage while collecting. Errors discard partial
results and release execution-owned reservations.

Native spill uses `aggregate-native-state-v3`, with exact type/layout descriptors,
raw integer/IEEE state bits and a raw Float64 AVG total. Restore validates framing,
types, layouts, flags and lengths before merge; UInt64 and NaN bit patterns are
not converted through floats or interface serialization. Legacy V1 and multi-int64
V2 remain separate. No new SIMD aggregation dispatch is enabled by this increment:
new scatter updates have a scalar reference, while existing predicate and
single-column int64 dispatch remain in place. Architecture-specific SIMD work
requires independent correctness and measured gates.
