# Phase 2.7 — Operator Analyze and Parallel Radix Join

## Goals

Phase 2.7 closes two execution-quality gaps left by Phase 2.6:

1. `ExplainAnalyze` must show where time and rows are spent inside the physical
   operator DAG, not only a query-level duration.
2. radix join must become a real multi-worker ownership strategy before the cost
   model is allowed to select it automatically.

The implementation keeps diagnostic instrumentation out of ordinary hot paths
and keeps single-table join as the default for small and medium workloads.

## Operator-level ExplainAnalyze

`LazyFrame.Analyze` creates an Analyze-only physical lowering. Each physical
`source.Source` node is wrapped by `physical.ProfileSource`; normal
`Collect/Prepare/PrepareFrame` do not create these wrappers.

A trace node records:

```text
operator id
operator kind
operator label/strategy
prepare vs execute phase
reader calls
input row upper bound
actual output rows
decoded output bytes
inclusive time
self time
```

Example:

```text
EXECUTE OPERATORS
id    kind        calls   input_rows   output_rows   output_bytes   inclusive      self           label
op01  scan            8      1048576       1048576       ...         38.5ms         38.5ms       parquet(...)
op02  aggregate       1      1048576        219196           0         47.9ms          9.4ms       radix+predicate-fused
```

### Inclusive and self time

Reader wrappers measure inclusive time. The trace stores physical parent/child
relationships and computes:

```text
self = inclusive - sum(direct child inclusive)
```

Self time is clamped at zero because concurrently running children can overlap.
The value is therefore diagnostic rather than a CPU-cycle accounting guarantee.

### Prepare and execute are separate

Join build-side work happens during physical preparation while probe-side work
happens during execution. The trace therefore stores separate phase statistics:

```text
PREPARE OPERATORS
right scan
join build

EXECUTE OPERATORS
left scan
join probe/materialize
projection
filter
...
```

This separation makes expensive build-side reuse visible when comparing one-shot
execution with `PrepareFrame`.

### Allocation accounting

Go's heap accounting is process-wide. Parallel operators cannot be assigned
exact allocation bytes reliably by reading runtime counters around each operator.
Phase 2.7 therefore reports real `runtime.MemStats.TotalAlloc` deltas for the
whole Prepare and Execute phase:

```text
prepare_alloc=...
execute_alloc=...
```

Per-operator allocation numbers are intentionally not fabricated.

`output_bytes` in the operator table is an estimate of decoded columnar batch
memory emitted by that operator, not compressed Parquet bytes.

## Parallel radix fixed-key join

Phase 2.6 had a correct radix table but the probe was serial and substantially
slower than the single `PairTable`.

Phase 2.7 adds:

```text
RadixPairTable.BuildParallel
RadixPairTable.ProbeParallelWithWorkspace
ParallelRadixProbeWorkspace
```

and the minimal scheduler boundary:

```go
type ParallelExecutor interface {
    MaxWorkers() int
    ParallelFor(ctx context.Context, tasks, parallelism int, fn func(int) error) error
}
```

`engine.Engine` implements this contract by forwarding work to the existing
persistent scheduler pool. The join package therefore does not create its own
worker pool.

## Probe algorithm

For one probe morsel:

```text
fixed keys
   ↓
radix histogram
   ↓
prefix offsets
   ↓
scatter key + original probe-row id
   ↓
┌──────── bucket 0 ──────── worker
├──────── bucket 1 ──────── worker
├──────── ...
└──────── bucket N ──────── worker
   ↓
PairTable probe local to each bucket
   ↓
per-bucket Pair buffers
   ↓
serial callback replay
```

The callback replay is intentionally serial. Callers can keep ordinary append
logic and do not need locks around output materialization.

SQL semantics remain identical to `PairTable` for:

- inner;
- left;
- semi;
- anti;
- duplicate build keys;
- NULL fixed/composite keys.

Row order is not an execution contract without an explicit Sort, so bucket
ownership is free to change physical output order.

## Parallel build

The right/build side is radix-partitioned once. Independent bucket-local
`PairTable` instances are built concurrently, then treated as immutable during
all prepared probes.

The maximum duplicate-key multiplicity is reduced after workers finish and is
used by `JoinSource` to preserve its safe output-row upper-bound contract.

## Reader-local workspace

`ParallelRadixProbeWorkspace` is owned by one join reader. It retains:

```text
histogram counts
prefix starts/cursors
scattered probe rows
scattered Key128 values
per-bucket Pair output slices
summaries
NULL unmatched rows
```

`JoinSource` also now retains fixed-key and validity scratch in the reader rather
than allocating new key/mask slices for every partition.

## Cost-model policy

Phase 2.7 validation on the shared AMD EPYC host showed two distinct regimes.

### Medium workload

```text
build = 262,144
probe = 1,048,576
~50% match
```

Pinned four-worker real Pair output benchmark medians were:

```text
single table       31.141 ms
parallel radix w4  31.618 ms
```

A previous serial radix implementation was around 3x slower; Phase 2.7 parallel ownership removes that large regression, but at this medium size it still does not create a durable win.

This is effectively a tie between single table and parallel radix. The fixed
partition/scatter cost does not justify automatic radix selection.

### Large workload

```text
build = 1,048,576
probe = 4,194,304
~50% match
```

Pinned four-worker samples (`taskset` CPUs 0-3, `GOMAXPROCS=4`) were:

```text
single table:
  281.486 ms
  281.066 ms
  300.330 ms
  median = 281.486 ms

parallel radix w4:
  136.719 ms
  165.957 ms
  140.177 ms
  median = 140.177 ms
```

The median improvement is about 2.01x. At this size the global hash table is large enough that cache locality plus bucket parallelism outweighs exchange cost. An eight-worker diagnostic run was faster still (~117-127 ms), but the automatic four-worker threshold is justified with the pinned four-worker data because the validation VM exposes only five CPUs.

### Automatic selection

The automatic Phase 2.7 policy is deliberately narrower than the theoretical
region:

```text
fixed-width key
workers >= 4
build rows >= 1,048,576
probe rows >= 4,194,304
```

Then:

```text
radix_bits = 8
parallel radix build/probe
```

Otherwise the existing single hash table remains the default.

An explicit Lazy hint can force the experimental path:

```go
left.Join(right, lazy.JoinOptions{
    Type:      lazy.InnerJoin,
    LeftOn:    []string{"id"},
    RightOn:   []string{"id"},
    RadixBits: 8,
    Workers:   8,
})
```

The hints survive optimizer rewriting and lower directly into
`physical.JoinSourceOptions`.

## Why the threshold is conservative

The automatic policy currently uses row counts and worker count only. It does
not yet model:

- key width beyond the fixed-key requirement;
- actual LLC size / NUMA topology;
- build/probe skew;
- duplicate multiplicity;
- memory bandwidth pressure from sibling queries;
- ARM vs x86 behavior.

Phase 2.7 therefore only enables the region that has already shown a clear win.
Cross-architecture measurements can widen or move the threshold later.

## Compatibility

No existing Phase 2.6 API is removed. Normal joins with no physical hints retain
single-table behavior except for the conservative large-query automatic region
when the Lazy engine has at least four workers.

`AnalyzeReport` adds operator and allocation diagnostics without changing
`ExplainAnalyze`'s existing logical/optimized/physical sections.
