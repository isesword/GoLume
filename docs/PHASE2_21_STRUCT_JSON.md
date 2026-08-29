# Phase 2.21 — bounded Struct and explicit-schema JSON

Phase 2.21 adds a deliberately bounded nested record contract without adding a
second eager execution engine. A `StructVector` contains parent validity plus a
row-aligned child `Batch`. Children may be supported scalar types or the
specialized `List<String>` shape from Phase 2.17. Recursive Struct/List, Map,
dictionary string children, and runtime schema evolution are rejected.

## Explicit JSON decode

```go
target := memory.NewSchema(
    memory.Field{Name: "id", Type: memory.TypeInt64},
    memory.Field{Name: "name", Type: memory.TypeString, Nullable: true},
    memory.ListStringField("tags", true, true),
)

df, err := frame.
    WithColumns(lazy.JSONDecode(lazy.Col("payload"), target).Alias("decoded")).
    Collect(ctx)
```

`JSONDecode` accepts only UTF-8 string input and always uses the supplied
schema. SQL NULL and JSON `null` produce a NULL Struct. Unknown object fields
are ignored. Missing or JSON-null children become NULL when nullable and fail
with a row number and field path otherwise. Malformed JSON, trailing values,
type mismatches, invalid UTF-8, and numeric overflow fail explicitly.

## Execution and lifecycle

Struct payloads participate in clone, Filter/compact, Take, stable Sort/Top-K,
Join materialization, projection, and DataFrame collection. Nested buffers are
included in session memory accounting. `Series.StructChunks` returns borrowed
read-only row views; `ToAnySlice` remains the explicit low-performance copying
escape hatch. DataFrame display formats a valid record as `{field: value}` and
keeps NULL Struct distinct from a valid Struct containing NULL children.

Arrow Struct arrays use a specialized binding only when every child fits this
bounded contract. Arrow buffers remain retained by the source while normalized
offsets, narrow-number widening, booleans, and temporal nanoseconds use the
same established Arrow ingestion rules as top-level columns.
