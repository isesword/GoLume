# Owned string/binary First and Last

String/binary endpoints are integrated with native typed results, lazy collection,
resident/partitioned/sorted aggregation, V5 spill and mixed exact-distinct queries.
First/Last preserve their input logical dtype; outputs own plain decoded bytes.

## State and ownership

`aggregate.BytesEndpointState` owns exact endpoint bytes and stable source ordinals.
Valid empty bytes differ from NULL, and boundary NULLs are retained. String values
are compared/copied as raw bytes, without Unicode normalization or UTF-8 rewriting.
Binary values preserve embedded zero and invalid UTF-8 bytes. State snapshots are
borrowed until mutation/Reset/Close; output must acquire its own charged storage.

Each selected payload is admitted before allocation. Replacing both endpoints
admits both complete allocations while the old payloads remain charged, then copies
and commits atomically. A rejected second reservation releases the first one without
changing state. Equal bytes can reuse owned storage while ordinals advance. Reset
and Close release payload reservations. Containing state structs/lane capacities
are separately admitted by the bundle or one-group reducer that owns them.
Oversized values fail with the budget error instead of allocating outside the budget.

Dictionary string input resolves each code using the source column's actual
dictionary; endpoint output owns the resolved bytes. A code without an accessible
matching dictionary fails clearly rather than becoming empty text or code bytes.
String and Binary results retain their logical dtype and use plain owned output.

## Row transactions and retry

A byte-state update is individually atomic, but multiple byte lanes plus numeric
lanes require a stronger row boundary when the resident spill engine retries a row.
The engine preflights every variable payload reservation and copy before committing
any lane. It aborts all prepared replacements if one fails. Only then does it commit
byte lanes and fixed states. Updating numeric counts first would count the retry twice; updating one byte
lane first could move an endpoint despite the row failing. Spill merge has the same
requirement. A terminal one-group streaming reducer need not retry partial state,
but it must close all state on an error and must not emit a partial result.

## Transport and compatibility

V1/V2/V3/V4 framing is preserved exactly. Partial-state transport uses an explicit
V5 frame with separate numeric and statistics descriptor counts. Statistics lanes
include fixed statistics and variable endpoints, distinguished by their dtype. Fixed sections retain their old raw representation.
Each byte endpoint lane encodes Seen/validity flags, both logical ordinals, checked
UInt32 lengths and exact first/last bytes. Decode checks descriptor type, flags, lengths,
truncation, unseen/NULL payload consistency and conflicting equal ordinals before
borrowing payload views. Merge acquires ownership before the borrowed frame expires.
Validate all additions and remaining lengths before slicing; admit the complete
encoded run before allocation. A very large endpoint must not create an unchecked
buffer even when it cannot fit in one run.

The exact-distinct tuple stream adds tag 2 for variable endpoint
payloads, preserving group/lane/ordinal identity and raw bytes. The existing fixed
ordinary-row tag is unchanged; byte lanes have zero placeholders. This serves mixed
distinct/endpoint
queries; non-distinct resident/parallel/sorted execution retains its existing algorithms.
Mixed distinct queries retain the documented shared exact tuple-sort fallback.

## Validation scope

Tests and independent review cover grouped/global empty input; empty valid values versus NULL; binary zero and
invalid UTF-8; input mutation and dictionary lifetime; same values across distinct
dictionaries; first/last boundaries across filtered partitions; stable parallel and
sorted provenance; multiple byte lanes mixed with numeric/moments/distinct; failed
second-lane reservation and spill retry without double-counting; hot large values;
multiple spill runs; corrupt lengths/flags/ordinals; quota/cancellation failures;
early/double Close; output remaining valid after source/state Close; and zero query
budget/temp-file retention after terminal paths.

## Diagnostic performance

On the development Linux/amd64 AMD EPYC 9V74 host, three 200ms runs of
`BenchmarkBytesStatisticsAggregate` (4,096 rows, 64 groups, 32-byte changing
payloads, First+Last and materialized typed output) measured bitmap-direct at
4.71–5.89 ms/op, about 836.5 KB/op and 21,395 allocations/op. Two-worker
partitioned-hash measured 5.48–7.62 ms/op, about 890.1 KB/op and 21,480
allocations/op. Replacement payload copies and reservation tokens dominate this
small workload; parallelism did not establish a speedup. This is a correctness-
and-budget-first baseline, not a performance claim or SIMD dispatch gate. Future
pooling/amortization must preserve transactional replacement overlap and ownership.
No SIMD or planner admission gate was changed for this stage.
