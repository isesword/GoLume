# Native and composite grouping keys

Aggregation keeps the specialized Int32, Int64, Uint32, stable-domain dictionary,
plain single-string and fixed-pair paths. Other supported keys use a single
shared group-ID index and canonical byte keys, including arbitrary-length
composites of native signed/unsigned integers, floats, booleans, temporal values,
strings and binary values. Nested list/struct and opaque Arrow keys are rejected.

Each component has a type, validity flag and byte length before its payload.
Thus null, valid zero and empty text are distinct; embedded NUL bytes and component
boundaries cannot create ambiguous composite keys. All floating NaN payloads form
one group; positive and negative zero form one group. Output uses canonical NaN
and positive zero. Other bits, including UInt64 values above MaxInt64, remain exact.
Temporal units and metadata belong to the fixed query schema.

Dictionary keys on the generic path compare resolved bytes, not unrelated integer
codes. They do not require a global dictionary domain and may use different
codebooks in different input batches. Generic dictionary output is plain string.
The specialized dictionary path still uses its explicit stable-domain contract.

Generic grouping supports resident, partition-owner parallel, adjacent sorted
and bounded radix-spill execution. Sorted execution requires the source's declared
ordering contract: equivalent keys must be adjacent. Row encoding uses reusable
operator-owned scratch, reserved before allocation; each parallel owner has its
own scratch. Index arenas own admitted keys; spill and sorted state do not retain
borrowed input batches. Growth charges the full replacement while the old buffer
is live. Dictionary/string payloads are not coerced through floating point.

Use ExecuteTyped for materialized generic-key rows, or the lazy DataFrame's native
columnar result. Legacy Execute summary-only mode remains available. Legacy
AggregateValue and KeyPart structs remain unchanged. CompiledQuery gained
GenericFields metadata; callers constructing exported compiled metadata must use
keyed literals rather than positional literals.

The canonical encoder, hash and visitor are scalar and allocation-free with
caller-provided capacity. No SIMD speedup is claimed for variable-width framing.
Specialized existing numeric aggregation kernels remain available on their old
paths. Performance evidence for new paths must be interpreted as throughput for
new functionality, not as a comparison against previously unsupported queries.

Columnar output admission also charges one vector descriptor and a conservative
map-entry/growth allowance per output column, in addition to native data,
validity and string buffers. Wide one-row results therefore cannot evade the
budget through metadata-only allocations. String payloads are charged per output
column at their decoded size rather than charging all canonical framing to every
string column.

Group names and effective aggregate output aliases must be unique. Preparation
and lazy schema inference now reject duplicates, including an aggregate alias
that shadows a group key. This closes an inconsistent behavior where typed rows
could represent duplicate names but columnar output reused one named vector and
failed or overwrote values later. Use distinct aliases rather than relying on
that previously ambiguous output.
