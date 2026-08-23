# Phase 2.6 — Typed Expressions, Fusion/CSE, Streaming Join, ExplainAnalyze

## Goals

Phase 2.6 extends the Phase 2.5 lazy physical DAG without replacing the proven
aggregate, Parquet, or prepared-execution paths. The release focuses on five
execution-quality improvements:

1. typed scalar inference, coercion, and explicit `Cast`;
2. float/string/date/timestamp expression execution;
3. one fused projection program with compile-time common-subexpression elimination;
4. streaming JoinSource preparation without reading the probe side;
5. executable `Analyze` / `ExplainAnalyze` diagnostics.

A radix-partitioned fixed-key join table is included and tested, but is not the
default physical strategy because the validation host showed a clear single-core
regression versus the existing single hash table.

## Typed expression IR

The expression IR now includes:

```text
column
int64 / float64 / string literal
date32 / timestamp literal
+ - * /
string concat
explicit Cast
Alias
```

`InferScalarType` is the single logical/physical type authority. Current numeric
coercion rules are intentionally simple and explicit:

```text
integer + integer       -> int64
uint64 + uint64         -> uint64
uint64 + signed integer -> float64
integer + float         -> float64
/                       -> float64
```

Date and timestamp values retain semantic `DataType` while using Arrow-compatible
physical integer vectors. Phase 2.6 deliberately does not guess timestamp units
or timezone conversions; those remain field metadata concerns.

## Physical families

Logical types map to a small number of hot physical vector families:

```text
int8/int16/int32/date32/time32      -> []int32
int64/date64/timestamp/time64       -> []int64
uint8/uint16/uint32                 -> []uint32
uint64                              -> []uint64
float32/float64                     -> []float64
string/binary                       -> offsets + bytes / dictionary codes
```

The mapping lets Arrow semantic types enter the expression engine without adding
interface dispatch to row loops.

## Fusion and CSE

`ProjectSource` no longer compiles/evaluates each projected expression
independently. All named expressions compile into one `ProjectionProgram`:

```text
score = a + b
scaled = (a + b) * 2

compile:
load a
load b
add         <- shared register
load 2
mul
```

The `(a+b)` subtree is compiled once and reused. A reader owns one
`ScalarWorkspace`, so computed buffers are retained across batches.

This is compile-time DAG CSE, not runtime JIT. It removes intermediate program
materialization while preserving typed batch execution.

## Typed predicates

The bitmap predicate program supports typed column/literal comparisons for:

- signed/unsigned numeric columns;
- float32/float64;
- UTF-8 string/dictionary string;
- date32;
- timestamp;
- SQL `AND/OR/NOT/IS NULL/IS NOT NULL` three-valued logic.

The existing int64 `column > literal` SIMD fast path is unchanged. Other typed
predicates use scalar typed loops until a benchmark justifies a SIMD kernel.

## Arrow RecordBatch execution

Arrow RecordBatch binding now promotes additional Arrow-native types into typed
vectors once at Source construction:

- int8/int16 -> int32 physical vector;
- uint8/uint16 -> uint32;
- float32 -> float64;
- date32/time32 -> int32;
- date64/timestamp/time64/duration -> int64.

Typed columns no longer keep a duplicate `OpaqueVector` binding. Opaque storage
is reserved for types without a specialized physical vector.

## Streaming JoinSource preparation

Phase 2.5 prepared JoinSource by probing the entire left side once to compute
exact output cardinality. Phase 2.6 removes that prepass.

The build table tracks maximum duplicate-key multiplicity. The source derives a
safe output upper bound from:

```text
left partition rows * max build multiplicity
```

For left joins the multiplier is at least one; semi/anti joins use one. Actual
rows are discovered only when the partition is executed.

Therefore:

```text
PrepareFrame
  -> read/build right side
  -> read left partition metadata only

Execute
  -> read each left partition once
  -> probe
  -> emit joined columnar batch
```

`source.Stats.RowsUpperBound` marks that the prepared row count is a capacity
bound rather than exact cardinality.

## Radix fixed-key join

`join.RadixPairTable` partitions fixed 128-bit keys by hash prefix and builds
independent `PairTable` instances. `RadixProbeWorkspace` retains histogram,
partition, row-index, and matched-row scratch per reader, so repeated probes
allocate zero bytes in the benchmark hot path.

The physical API exposes an explicit experiment switch:

```go
physical.NewJoinSourceWithOptions(...,
    physical.JoinSourceOptions{RadixBits: 8},
)
```

Default `NewJoinSource` remains the single-table strategy. Validation on the
shared AMD EPYC host for 262,144 build rows and 1,048,576 probe rows showed:

```text
single PairTable: ~25-37 ms/op, 0 alloc/op
radix-256:        ~86-118 ms/op, 0 alloc/op
```

That result is why radix join is not enabled automatically. Future work should
combine radix ownership with parallel bucket probing and use runtime statistics
before revisiting the cost model.

## ExplainAnalyze

`LazyFrame.Analyze(ctx)` executes one prepared plan and returns:

- original logical plan;
- optimized plan;
- physical diagnostics;
- prepare duration;
- execute duration;
- output rows;
- aggregate group/selected-row counts when applicable;
- physical aggregate strategy.

`ExplainAnalyze(ctx)` returns the human-readable report.

Aggregate Analyze executes without full group materialization. Row-producing
plans use the public `PreparedFrame` contract and therefore materialize an owned
Frame.

## Compatibility

The following Phase 2.5 API remains valid:

```text
Select(...)
SelectExprs(...)
WithColumns(...)
Collect / Prepare
CollectFrame / PrepareFrame
PrepareJoin
```

The original 1,048,576-row Parquet aggregate remains a release regression and
must continue to return:

```text
groups   = 219196
selected = 472475
checksum = 236716301
```
