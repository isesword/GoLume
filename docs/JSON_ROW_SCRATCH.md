# JSON decode row scratch

JSON decode reuses a single scratch StructVector, Batch, and typed child-vector
metadata within each evaluated batch. Fixed-width children retain one element;
validity allocates on the first NULL and is reset on every subsequent row.
String/list wrapper metadata also stays in this scratch row. This removes the
previous per-row reconstruction of the schema slice, maps, and child vectors.

## Ownership and bounded scratch

AppendStructRow copies each completed scratch row into the owned result before
the next row is decoded. Immediately afterward, scratch string bytes, list child
bytes, child offsets, and child validity are dropped. No variable-capacity
high-water mark is retained per field across rows: alternating large fields must
not turn a largest-row scratch allowance into the sum of historical field maxima.
The scratch row itself is local to this evaluation and is never kept in a
prepared plan, shared between executions, or exported to callers.

The original conservative output and scratch reservations remain unchanged and
are acquired before construction. A replacement keeps the previous result and
its charge until success. Budget rejection, parse/type errors, and cancellation
leave the prior result intact and release the new reservations. This change
reduces allocated bytes and allocator churn; it does not lower the admitted
query budget or claim an exact Go heap/RSS limit.

## Compatibility

The standard-library decoder, unknown-field handling, duplicate-key behavior,
UTF-8 validation, required/nullable field rules, native-width conversions, list
nullability, error row/field order, and cancellation checkpoints are unchanged.
Only the existing scalar and List<String> JSON child schema is supported; this
is not new recursive JSON support or a different parser. SIMD dispatch and
Sum/Avg semantics are unchanged.

## Reproducible measurements

Run the diagnostic benchmarks with a fixed GOMAXPROCS, before and after:

```sh
GOMAXPROCS=1 go test ./expr -run '^$' -bench '^BenchmarkJSONDecodeRowScratch' -benchmem -benchtime=150ms -count=6
GOMAXPROCS=1 go test ./lazy -run '^$' -bench '^BenchmarkJSONRowReusePreparedCollect' -benchmem -benchtime=200ms -count=6
```

On the shared Linux/amd64 AMD EPYC 9V74 development host, six single-worker
prepared-Collect samples with 4,096 mixed JSON rows reduced allocated bytes from
16.381 MiB to 7.726 MiB (-52.83%) and allocations from about 254,801 to 136,036
(-46.61%). Median time was 30.95 ms versus 20.42 ms, with substantial shared-host
noise; this is diagnostic evidence for this workload, not a universal speedup.
One-row and 256-row benchmarks also reduced allocated bytes and allocation count.

The null-row allocation regression fails on the previous implementation (10,567
allocations for 256 rows) and passes with reuse. Tests cover native numeric and
temporal widths, required and nullable fields, parent NULLs, list validity across
64-element boundaries, owned payloads after input mutation, metadata identity,
variable scratch release, prepared execution across batches, budget rejection,
all cancellation checkpoints, error order, and atomic result replacement.

## Empty result contract

A zero-row JSON evaluation retains every declared typed child vector. String
and List<String> children include their required zero offset sentinel, including
the list's child string vector. This lets the result pass StructVector.Validate
and feed CloneStructRange and columnar serialization without missing columns.
No synthetic row is appended and no input JSON is parsed: a required field or a
malformed constant does not cause a value error when there are no input rows.

Empty child metadata is initialized only after the existing output and scratch
reservations succeed. Cancellation or admission failure still leaves the prior
result and its charge intact. Alternating nonempty and empty evaluations does not
carry prior payloads or row counts into the empty result. The nonempty decoder
and its allocation behavior are unchanged. This is a schema-correctness fix,
not a reduction in the admitted memory budget.
