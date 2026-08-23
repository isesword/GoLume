# Phase 2.2 — adaptive execution, SQL semantics, and operator hardening

## 1. Scope

Phase 2.2 moves the project from a benchmark-oriented specialized executor toward
a reusable Polars-like Go analytical engine. It focuses on correctness under bad
statistics, richer aggregate/key semantics, reusable physical primitives, and
engineering contracts that can survive later optimizer/streaming work.

The architectural rule remains:

> dynamic interfaces live at planning/operator boundaries; row hot paths stay
> typed, contiguous and specialization-friendly.

## 2. Delivered capabilities

### 2.1 Adaptive aggregation hash tables

`I32Table`, `U32Table`, `U64Table`, `PairTable`, and `StringTable` now treat
cardinality estimates only as initial sizing hints.

```text
insert new group
    |
size + 1 > maxEntries ?
    |
    +-- no --> normal probe/update
    |
    `-- yes
          |
      Reserve(delta retained bytes)
          |
      allocate 2x table
          |
      rehash current generation only
          |
      continue insert
```

Default maximum load factor is 0.70. Grow reservations are charged through the
Prepared workspace and remain charged until that workspace is closed.

### 2.2 SQL NULL group keys

Single-key int32/int64/uint32/dictionary/plain-string tables preserve a dedicated
NULL group distinct from the physical zero value.

Two-column fixed keys use:

```text
Key128 { A, B }
ValidityMask bit0/bit1
```

Invalid payload is canonicalized before hashing. The validity mask is part of
hash/equality identity, including through radix exchange.

### 2.3 Radix fallback under cardinality misprediction

Partition-local radix tables deliberately remain fixed-size/cache-local. If a
local table cannot accommodate underestimated cardinality, radix returns
`radix.ErrLocalTableFull`. Engine then aggregates the already compacted selected
rows through a budget-aware adaptive global table instead of rereading source
partitions.

### 2.4 Built-in aggregate list

Direct and bitmap-direct group-by now use a compiled `aggregate.Layout` and SoA
`aggregate.Int64Vector` state. Supported SQL aggregates:

```text
SUM(value)
COUNT(*)
COUNT(value)
MIN(value)
MAX(value)
AVG(value)
```

All value aggregates currently share one int64 input column. `COUNT(*)` may be
the only aggregate and does not require a synthetic value column.

Radix intentionally remains the measured SUM(non-null value)+COUNT(*)
specialization. Planner rejects forced radix when the aggregate/value layout is
not compatible.

### 2.5 Dictionary string join

`join.DictionaryTable` supports inner/left/semi/anti join.

- Same stable dictionary domain: integer code fast path.
- Different domains: dictionary-level value normalization once, followed by
  integer code translation in the row hot path.
- NULL never matches SQL equality join.
- Duplicate build rows and duplicate dictionary values preserve one-to-many
  semantics.

### 2.6 Fixed 128-bit composite join

`join.PairTable` supports inner/left/semi/anti equality join for fixed `Key128`.
Any NULL key component makes the row non-matching. Duplicate build rows are
stored as row chains rather than collapsed.

### 2.7 PreparedQuery memory ownership

Prepared plan compilation caches:

- schema/predicate validation;
- physical predicate program;
- key shape and dictionary domain metadata;
- source partitions/stats;
- physical strategy;
- reusable workspace pool.

Retained bitmap/hash/radix/string-arena capacity remains charged to the query
memory manager until `PreparedQuery.Close`.

### 2.8 Go documentation policy

Production exported API is checked by `tools/checkgodoc`.

The checker now enforces:

- package documentation (`Package x ...`, or `Command ...` for main packages);
- GoDoc presence for exported declarations;
- exported type/function comments begin with the declared symbol name;
- grouped const/var declarations have group documentation.

## 3. Physical strategy model

```text
                         Prepared Query
                              |
                 predicate/key/aggregate layout
                              |
                              v
                          CostModel
                              |
        +---------------------+--------------------+
        |                     |                    |
 direct-fused          bitmap-direct        radix ownership
        |                     |                    |
 scalar fused        selection bitmap       compact/exchange
        |                     |                    |
 adaptive table       adaptive table       cache-local tables
        +---------------------+--------------------+
                              |
                         materialize
```

The default heuristic keeps the ARM result from Phase 0 in mind: narrow NEON128
bitmap materialization is not automatically better than fused scalar execution.
Wide x86 SIMD, reusable selections, predicate complexity, row count, estimated
cardinality and key width influence strategy selection.

## 4. Correctness contracts

### Dictionary domains

A dictionary code is only meaningful inside its stable semantic domain. Group-by
requires a stable `DictionaryDomain`; join normalizes different domains before
probing.

### Plain strings

Plain string vectors are offset+byte views. Hash aggregation copies only the
first occurrence of a new group key into a workspace-owned arena, so a table
never retains dangling references to reused source batches.

### Join NULL vs GROUP BY NULL

They intentionally differ:

```text
GROUP BY NULL  -> all equivalent NULL keys form one group
NULL = NULL    -> UNKNOWN, therefore no equality-join match
```

### Cardinality hints

Hints influence initial sizing and planner choice, never correctness.

## 5. Final validation in this release

Validated with Go 1.27 and the real 1,048,576-row Parquet fixture:

```text
groups   = 219196
selected = 472475
checksum = 236716301
```

Pass matrix:

```text
scalar build/test             PASS
GOEXPERIMENT=simd             PASS
GODEBUG=simd=0                PASS
GODEBUG=simd=+128             PASS
GODEBUG=simd=+256             PASS
GODEBUG=simd=+512             PASS
race: engine/hashagg/join/... PASS
go vet ./...                  PASS
GoDoc convention checker      PASS
```

On the validation host, the real Parquet Prepared radix demo reached 17.3–18.3
ms steady-state after first workspace creation. Five-run benchmark medians were
approximately 26.77 ms one-shot vs 16.93 ms Prepared, with allocation bytes per
operation reduced from about 118 MB to 36.9 MB. These are regression numbers for
this host, not cross-machine product claims.

## 6. Explicit boundaries after Phase 2.2

Not claimed complete yet:

- plain-string radix group-by;
- generated advanced-aggregate radix layouts;
- aggregate functions consuming multiple distinct value columns;
- radix-partitioned join;
- join node in a generic multi-source logical plan DAG;
- spill implementation;
- pqarrow RecordBatch/projection-pushdown adapter;
- calibrated cost-based optimizer with runtime feedback;
- SQL/DataFrame public facade.

These are the next engine stages; they do not require replacing the current
memory/kernel/hash/radix/Prepared abstractions.
