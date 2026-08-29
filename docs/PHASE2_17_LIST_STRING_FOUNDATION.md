# Phase 2.17 — List<String> foundation

Phase 2.17 introduces one deliberately narrow nested type: a variable-length
list whose child is UTF-8 string. It establishes the output contract required
by `Split` and `RegexExtractAll` without prematurely generalizing execution to
arbitrary nested values.

## Schema

`memory.TypeList` uses Arrow-shaped child metadata. The supported form has one
child field named `item` with `TypeString`; parent and child nullability are
independent.

```go
field := memory.ListStringField("tags", true, true)
```

Other child shapes are rejected by validated InMemory ingestion. Arrow
`List<String>` is specialized; `LargeList`, List with non-string children,
Struct, and Map remain source-native opaque values.

## Physical layout

```text
StringListVector
  Offsets   []uint32        // list row -> child row range
  Validity  Bitmap          // parent NULLs
  Values    StringViewVector
    Offsets []uint32        // child row -> byte range
    Data    []byte
    Validity Bitmap         // NULL string elements
```

The canonical representation begins both offset arrays at zero, requires
monotonic offsets, and requires terminal offsets to equal the exact child row
and byte counts. Child data is bounded by the existing 4-GiB uint32 string
contract.

These states remain distinct:

- NULL parent list;
- valid empty list;
- NULL child element;
- valid empty string element.

## Execution coverage

List<String> columns survive owned cloning, projection aliases, Filter compact,
Sort/Top-K/Limit Take, window payload materialization, and inner/outer joins.
Outer joins synthesize a NULL parent list rather than an empty list. Nested
buffers are included in join build-side memory accounting.

Arrow RecordBatch ingestion caches normalized parent and child offsets while
borrowing retained Arrow string bytes. InMemory sources validate offsets and
parent/child nullability before exposing a reader.

## SIMD boundary

No new List-specific SIMD kernel is enabled in Phase 2.17:

- offset validation runs once during source construction and compares adjacent
  values, so vector mask extraction and reduction would dominate this cold
  check;
- List Take/compact is driven by irregular parent offsets and variable byte
  lengths, making it memory-copy and indirection bound;
- fixed-width companion columns continue using the existing approved SIMD
  compact kernels in the same Filter/Take pipelines.

Phase 2.18 will benchmark a dedicated single-byte delimiter scanner for
`Split`. It may enter Auto only if both the scan microbenchmark and end-to-end
Lazy workload beat `bytes.Index`/scalar execution on each target architecture.
