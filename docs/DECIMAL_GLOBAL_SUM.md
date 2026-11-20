# Exact global native Decimal SUM (N14 candidate)

`Agg(Sum("amount", "total"))` accepts root native Decimal input. A query may
request several Decimal SUM outputs, including aliases of the same input lane.
Each output alias owns an independent state; repeated inputs are not state-CSE
deduplicated. Every output is nullable Decimal(38, input scale), with explicit plain encoding.
NULL inputs are ignored. Empty or all-NULL input returns one NULL result row,
not zero. Source precision/scale and validity must match their declared schema.

The sum uses a fixed signed256 intermediate and a checked per-lane valid count
bounded by MaxInt64. Selected-row statistics separately count all predicate-true
rows, including NULL inputs, and must fit MaxInt; exceeding that bound has its
own error rather than the valid-value count error. Only the final result is checked against precision38.
Thus [M,M,-M] succeeds if M fits precision38, regardless of batch boundaries or
order, even though a 128-bit partial sum might overflow. A final coefficient
outside the precision38 range is an error, never wraparound, float conversion,
rounding or truncation. Production state does not use per-row big.Int allocation.

N14 permits global queries whose aggregate outputs are all native Decimal SUMs.
N15 additionally permits COUNT(*) and COUNT(root native Decimal) beside at least
one such SUM; see DECIMAL_SUM_COUNT.md for NULL, count-limit and scope details.
Other aggregate mixtures, grouped aggregation, Decimal
AVG/Min/Max, window operations and Decimal children in native containers are not
opened. Existing numeric aggregate protocols and semantics remain unchanged.
Configured AVG output precision/scale/rounding requires a separate API decision.

The original supported output interfaces are BatchStream and lazy Collect/IPC/Parquet;
N16 adds the separate ExecuteDecimal endpoint described below.
Legacy engine Execute/ExecuteTyped scalar materialization (Materialize=true) cannot represent128
bits and explicitly rejects this plan; TypedValue is not silently enlarged,
truncated or converted to float. Prepared schema and supported streaming output
must preserve precision/scale on every execution. Materialize=false may consume
the stream for execution statistics without pretending to return a Decimal scalar.
Its summary Checksum is zero and is not a digest of Decimal values. Direct
resident/hash or spillable aggregate stream entry points reject this plan; use
the dedicated global sorted BatchStream entry point.

Fixed global state can run under ExecutionBounded with a spill directory
configured; that configuration does not require spilling the aggregate state.
This does not authorize grouped/hash/partitioned aggregate spill or ignoring an
explicit incompatible forced strategy. Runtime state and scratch use a conservative admission envelope of
16384 + 2*N*sizeof(DecimalSumState), and output uses 8192 + 1024*N, for N
output aliases. Predicate workspace and input exchange have separate owners.
These charges are not an exact heap/RSS proof: compiled schema and descriptors
belong to the existing prepared-plan lifecycle, not this query reservation. Workers settings do not imply parallel
SUM merging unless the execution path actually implements and tests it.

Independent tests use big.Int coefficient oracles, cancellation-order permutations,
NULL/empty inputs, repeated aliases, multiple scales, overflow and unsupported
mixtures. NativeNested Parquet input, D1 arithmetic and one-row native output can
roundtrip through IPC/Parquet. Historical failures and protected full-repository
restrictions remain; test specifications are not a claim of completed gates.

N15 leaves the pure-SUM admission fees above unchanged. Mixed SUM+COUNT plans
prepay two Uint64 count arrays, adding 16 bytes per output slot. The legacy
Result/TypedValue materialization rejection applies to the entire mixed plan too.

N16 adds the explicit Engine/PreparedQuery `ExecuteDecimal` materialization
endpoint with a separate `DecimalResult`; see [public Decimal results](DECIMAL_RESULT.md).
Legacy Execute/ExecuteTyped materialization restrictions and existing TypedValue
layout remain unchanged. This result API does not expand aggregate eligibility.

N17 separately adds [ordered single-Int64-key grouped SUM/COUNT](DECIMAL_GROUPED_SUM.md)
through BatchStream/Lazy only. Global identities, fees and ExecuteDecimal scope
remain unchanged; the new grouped path is not a general hash/spill aggregate.

N18 separately permits [unordered resident Decimal grouping](DECIMAL_RESIDENT_GROUPED_SUM.md)
under explicit finite-budget ExecutionMemoryOnly. N17 ordered behavior is retained;
the new path is O(groups), has no result-order promise and does not expand
ExecuteDecimal beyond global plans.
