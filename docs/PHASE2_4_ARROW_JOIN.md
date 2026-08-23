# Phase 2.4 — Arrow RecordBatch and Lazy Join

Phase 2.4 broadens the Lazy DAG without replacing the execution engine built in
Phases 1–2.3. It adds an Arrow-native source boundary and a terminal equality
join lowering path while preserving the same rule optimizer, memory manager,
hash tables, expression evaluator, and prepared-execution lifecycle.

## 1. Architecture

```text
Apache Arrow RecordBatch(s)
          |
          v
 source/arrowrecord
   |       |        \
   |       |         -> arbitrary Arrow arrays kept as OpaqueVector
   |       -> source-wide dictionary normalization
   -> typed vectors for hot primitive/string paths
          |
          v
      LazyFrame
          |
          v
     LogicalPlan
  Scan / Filter / Projection / Aggregate / Join
          |
          v
      Optimizer
   projection pushdown
   predicate annotations
   per-input join pushdown
          |
          +----------------------+
          |                      |
          v                      v
   PreparedQuery            PreparedJoin
          |                      |
          v                      v
 SIMD / hash / radix       pair/dictionary hash join
```

The optimizer remains a control-plane component. No LogicalPlan interface
dispatch is introduced into a row loop.

## 2. Arrow RecordBatch source

`lazy.Session.ScanArrow(records...)` retains the supplied RecordBatches and
exposes them through the existing `source.Source` contract. The session releases
those retains on `Close`.

### Type preservation

Every Arrow field is accepted into the lazy schema. Native fast paths currently
exist for:

- `int32`, `int64`;
- `uint32`, `uint64`;
- `float64`;
- UTF-8 string;
- binary;
- dictionary-encoded UTF-8 string.

Other types (including nested, extension, union, decimals without a specialized
kernel, large/view strings, etc.) are retained as `memory.OpaqueVector` with the
original `arrow.Array` and exact Arrow type string. This is deliberately
stronger than rejecting the schema but weaker than claiming every Arrow type
already has arithmetic/group-by kernels.

An opaque vector still carries a validity bitmap, so generic `IS NULL` and
`IS NOT NULL` semantics work even before a type-specific compute kernel exists.

### Buffer ownership and caching

For native primitive arrays, the engine references Arrow value slices. For
string/binary arrays the byte payload is referenced directly. Data that must be
normalized to the engine's internal conventions is cached once at Source
construction:

- validity bitmaps;
- 32-bit relative string/binary offsets;
- source-wide dictionary values;
- normalized per-row dictionary codes.

Prepared execution therefore does not repeat dictionary code translation or
validity/offset conversion on every scan.

## 3. Dictionary preservation

Input RecordBatches may encode the same logical UTF-8 column with different
local dictionary orders:

```text
batch 0 dictionary: ["a", "b"]
batch 1 dictionary: ["b", "a", "c"]
```

The Arrow source constructs one source-wide domain:

```text
canonical: ["a", "b", "c"]
```

and normalizes the row codes once. Subsequent group-by or dictionary join paths
continue operating on integer codes rather than expanding every row to a Go
string.

Non-UTF8 dictionaries are kept as opaque Arrow arrays until a dedicated
normalizer/kernel is implemented.

## 4. Projection pushdown on Arrow batches

The same logical dependency analysis used by Parquet is applied to Arrow
RecordBatch sources. Only columns required by downstream projection,
predicates, grouping, aggregation, or join keys are bound into each
`memory.Batch`.

This cannot undo memory already owned by the caller's RecordBatch, but it avoids
creating engine vectors and executing downstream work for unrelated columns.

## 5. Predicate pushdown on Arrow batches

Unlike Parquet, an in-memory RecordBatch normally has no persisted row-group
min/max metadata. Phase 2.4 therefore synthesizes conservative `int64`
per-batch statistics when pushdown is prepared and caches them on the source.
The same SQL-three-valued pruning rules used for Parquet decide whether an
entire RecordBatch can produce TRUE.

This optimization is especially useful for repeated `PreparedQuery` execution:
the statistics are computed once while the later executions can skip rejected
batches. It should not be confused with Parquet metadata pruning, which can
avoid decoding data without first scanning the column values.

Residual row-level filters are always retained.

## 6. Lazy Join

A logical `Join` node supports one or two equality keys and these join modes:

- inner;
- left;
- semi;
- anti.

Example:

```go
joined := left.Join(right, lazy.JoinOptions{
    Type:    lazy.InnerJoin,
    LeftOn:  []string{"customer_id"},
    RightOn: []string{"id"},
})

prepared, err := joined.PrepareJoin(ctx)
if err != nil {
    return err
}
defer prepared.Close()

result, err := prepared.Execute(ctx)
```

### Physical key shapes

Phase 2.4 lowers:

- one fixed numeric key (`int32/int64/uint32/uint64`);
- two fixed numeric keys into a 128-bit composite key;
- one dictionary UTF-8 string key.

Composite join NULL semantics are SQL equality semantics: if any key component
is NULL, that row does not match another NULL row.

Dictionary joins normalize different source domains at dictionary cardinality
rather than hashing/comparing strings for every probe row.

### Prepared build side

`PreparedJoin` builds the right-side table on first execution and retains it for
later probes. The build allocation is reserved through the engine memory
manager and released by `Close`. A failed/cancelled first build does not poison
future retries.

### Terminal row-reference output

The Phase 2.4 join lowering is intentionally terminal and returns logical row
references instead of eagerly materializing both payload sides. Consequently,
the optimizer can push source projection down to:

```text
join keys + residual filter columns
```

rather than reading every payload column just to construct join pairs.

Streaming joined payload batches and aggregate-over-join lowering are the next
step; they should share a partition-owned join operator rather than bolt a
second execution engine onto LazyFrame.

## 7. Optimizer boundaries

Projection dependencies are propagated independently into each join input and
join keys are always retained. Predicates already inside either input are
optimized normally.

A predicate above a Join is not blindly pushed into an input. That would be
incorrect for outer joins and requires ownership/null-semantics analysis even
for inner joins. It remains a residual predicate until the optimizer has an
explicit safe rule.

## 8. Explicit non-claims

Phase 2.4 does not yet claim:

- arbitrary Arrow types have compute/group-by kernels;
- expression projection / `WithColumns`;
- aggregate-over-join physical pipelines;
- sort/window/limit lazy nodes;
- statistics-driven build-side reversal;
- radix/spillable physical joins;
- page-level Parquet/Bloom/dictionary pruning;
- full `pqarrow.RecordReader` lowering from Parquet into RecordBatch.

The source and logical contracts are designed so these can be added without
changing the public semantics introduced here.
