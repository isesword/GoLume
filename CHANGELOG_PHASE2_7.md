# Phase 2.7 changelog

## Operator-level ExplainAnalyze

- Added Analyze-only `physical.ProfileSource` wrappers. Normal `Collect`, `Prepare`, and `PrepareFrame` do not carry timing branches in their hot loops.
- Added `observer.OperatorTrace` with separate Prepare and Execute phases.
- Added per-operator calls, input rows, output rows, decoded output-byte estimates, inclusive time, and derived self time.
- Added query-level `runtime.MemStats.TotalAlloc` deltas for Prepare and Execute. Per-operator allocation is intentionally not reported because Go heap counters are process-wide and cannot be attributed reliably across concurrent operators.
- Added aggregate-root tracing so fused predicate + aggregate work remains visible even when there is no standalone physical FilterSource.
- Added `lazy-demo -analyze`.

## Parallel radix fixed-key join

- Added `join.ParallelExecutor`, implemented by `engine.Engine` through its existing persistent scheduler pool.
- Added parallel radix build and probe with bucket ownership; the join package does not create a second worker pool.
- Added `ParallelRadixProbeWorkspace` and reader-local fixed-key/validity scratch reuse.
- Preserved inner/left/semi/anti, duplicate build-key, and NULL equality semantics.
- Added explicit Lazy join hints: `RadixBits` and `Workers`.
- Added conservative automatic selection only for fixed-width large joins with at least four workers.

## Benchmark-backed policy

Pinned four-worker measurements on the validation host:

```text
medium: build=262,144, probe=1,048,576
single table       31.14 ms median
parallel radix-4   31.62 ms median

large: build=1,048,576, probe=4,194,304
single table      281.49 ms median
parallel radix-4  140.18 ms median
```

The medium workload is effectively a tie; the large workload is about 2.01x faster with parallel radix. The automatic threshold therefore remains deliberately narrow instead of treating radix as a universal join strategy.
