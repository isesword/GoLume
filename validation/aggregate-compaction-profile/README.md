# Aggregation and compaction hotspot reconnaissance

Date: 2026-11-08 UTC. Source revision: `7f90fb6b84db6f9f3518baf83eb5a7059c1b3fb9`, verified against the published GitHub tree. This source snapshot has no `.git`; `source-sha256.txt` fingerprints relevant source files and `source-unchanged.txt` confirms they were unchanged during the investigation. No production code was changed.

## Decision

Prioritize **batch-bound typed aggregate-column access**, initially the global native sum/count path. Bind column vectors, validity and immutable lane descriptors once when an input batch is loaded instead of resolving names and types on every row. Keep the existing row order and scalar native accumulation semantics. This is a hypothesis supported by hotspots, not an implemented or measured speedup. It can be bounded by aggregate-lane count with no row-sized state. Any retained binding allocation must be charged and released, must not outlive its borrowed batch, and must be rebound on every batch transition.

Global `nativeAggregateBits` accounts for 33.76% cumulative CPU, while `numericSum` is only 3.07%. Grouped native has analogous overhead, but generic multi-key aggregation spends much more time constructing and looking up keys. These findings do not support mechanically replacing arithmetic with SIMD. Next, independently investigate canonical key-component binding and repeated key-size/encoding work in generic grouping. Preserve collision checks and canonical null/type/value encoding.

The existing eligible filter→project edge already avoids most wide-column materialization. Its remaining bottleneck is conditional expression evaluation. The forced materialized edge spends substantial CPU in **existing** AVX512 Int64 compression. There is no evidence here to admit a new narrow/Float32 compaction kernel.

## Reproduction and environment

- Go 1.27.3 linux/amd64; Intel Xeon Platinum 8573C, AVX512 reported in `/proc/cpuinfo`.
- Toolchain environment: `source /workspace/scratch/ac8e1e118737/numeric-env.sh`.
- `run.sh` executes all five exact workloads serially with `GOEXPERIMENT=simd GOMAXPROCS=1`, 3-second CPU/heap profiles, then five separate 1-second unprofiled benchmark samples each. Each query uses one engine worker. Benchmark calibration can make actual profiled process duration exceed three seconds.
- No CPU affinity was applied. Shared/virtualized host and timing drift remain limitations. `lscpu` could not read its sysfs topology; `/proc/cpuinfo` identified the CPU. `environment.txt` records host/toolchain details.
- `run-memory.sh` uses Go overlays mapping existing benchmark files to retained `*.go.txt` copies. The only changes are query-budget instrumentation, logging, and two input row counts. It then runs a capability helper and `go list ./...`.
- Both scripts resolve the repository root relative to their own location and inherit caller PATH/GOCACHE/GOMODCACHE/GOFLAGS. Optionally set GOLUME_ENV_SETUP to a local environment script. Go 1.27.3 and module dependencies must already be available. run-memory.sh regenerates absolute overlay paths for the current checkout. The retained helper is copied to `.go` only during execution and removed afterward. No retained `.go` files create validation packages.
- Engine benchmark options use `DefaultOptions().KernelMode == kernel.ModeAuto`. Both selection cases explicitly set `opts.Kernels = kernel.New(kernel.ModeAuto)`. The selection suffix **auto** refers to the filter→project execution edge, not a different kernel mode.
- `capabilities.txt` confirms native AVX512/512-bit Int64 compaction; Float32 and Int16 compaction are scalar. Materialized CPU samples also directly identify `compactInt64X8` and archsimd calls.
- No arm64 host, no cross-architecture performance inference. arm64's compaction installer currently does nothing. amd64 int32/int64/uint32/uint64/Float64 use guarded AVX512 compress; narrow integers and Float32 remain scalar.

## Workloads and baseline timing

All input construction and preparation are excluded from benchmark timers. Aggregation plans are prepared once, but execution state is rebuilt for each execution. `ExecuteTyped` uses `Materialize:false`, so these measure summary consumption, not full result-row materialization. All aggregate inputs are nonnull, without a filter. Numbers below are medians and observed ranges over five unprofiled samples, not confidence intervals.

| Workload | Shape | Median ms/op | Range ms/op | B/op | allocs/op |
|---|---|---:|---:|---:|---:|
| native-global | 32,768 rows; UInt64/F64/Int32/F32 sums plus row count; no keys | 4.272 | 3.962–6.298 | 24,282 | 136 |
| native-grouped | same mixed lanes; Int32 key; 4,096 groups | 8.289 | 7.293–10.213 | 1,055,735–1,055,740 | 372 |
| generic-grouped | 32,768 rows; UInt64/String/UInt64 keys; 4,096 groups; Int64 sum/count | 10.766 | 10.398–11.428 | 1,211,883–1,211,893 | 321–322 |
| selection-auto | 8,192 rows; 32 Int64 input columns; 50% selected; two projected expressions | 0.218 | 0.214–0.232 | 321,880 | 276 |
| selection-materialized | same fixture, materialized public filter boundary | 0.693 | 0.677–0.875 | 1,347,480 | 397 |

Native keys are deterministic modulo group count; native UInt64 values exceed 2^63. Generic keys are correlated representations of the same modulo value, not three independent high-cardinality dimensions. Selection is a deterministic contiguous prefix: `c0=row`, `c0<4096`. Only `c1` is needed by the two output expressions, one of which is conditional. Each selection iteration opens and closes a stream over one source batch; allocation results include cold stream/workspace setup, not steady-state multi-batch reuse. Input rows are null-free Int64, not Float32/narrow, scattered, variable-length, nested, or nullable compaction. This deliberately exposes wide-input/narrow-projection work avoidance.

The two selection timings were measured in separate blocks and materialized timings drifted. They support work-avoidance as a priority, not a statistically precise speedup or scalar-versus-SIMD comparison. No production before/after change was measured.

## CPU evidence

Percentages refer to each workload's own CPU sample total. **Flat** is self time; **cumulative** includes descendants. Nested cumulative percentages overlap and must not be summed.

| Workload | Function | Flat | Cumulative |
|---|---|---:|---:|
| native-global | sortedAggregateStream.addBundleRow | 13.04% | 67.77% |
| native-global | nativeAggregateBits | 8.18% | 33.76% |
| native-global | sortedAggregateStream.nextSelected | 22.25% | 24.81% |
| native-global | runtime.mapaccess1_faststr | 0.77% | 24.04% |
| native-global | NumericVector.AddBits | 12.53% | 16.11% |
| native-global | numericSum | 1.79% | 3.07% |
| native-grouped | multiAggregateState.group | 5.24% | 38.27% |
| native-grouped | nativeAggregateBits | 3.64% | 17.08% |
| native-grouped | GroupIndex.find | 10.48% | 12.07% |
| native-grouped | NumericVector.AddBits | 7.74% | 10.93% |
| native-grouped | numericSum | 1.59% | 2.51% |
| generic-grouped | multiAggregateState.group | 3.36% | 77.58% |
| generic-grouped | multiAggregateState.key | 1.57% | 50.22% |
| generic-grouped | canonicalGroupBinding.AppendKey | 7.85% | 36.77% |
| generic-grouped | canonicalGroupBinding.KeySize | 6.28% | 26.01% |
| generic-grouped | runtime.mapaccess2_faststr | 7.17% | 25.56% |
| selection-auto | evaluationState.evaluateConditional | 5.22% | 52.99% |
| selection-auto | gatherSelectedValues[Int64] | 2.43% | 2.43% |
| selection-materialized | compactInt64X8 | 20.29% | 30.57% |
| selection-materialized | evaluationState.evaluateConditional | 2.57% | 17.14% |

`engine/prepared.go` explicitly assigns global aggregation `StrategySorted`, one worker, with reason "global aggregation uses one bounded state". This does not sort rows. In `engine/sorted_aggregate_stream.go`, `nextSelected` advances row position, checks selection, obtains a group key and legacy value binding, then returns one row at a time; batch loading occurs when exhausted. `addBundleRow` subsequently performs the per-lane native reads and updates. A later global-specific batch loop could reduce this row-dispatch overhead, but needs separate cancellation, batch-boundary, filtering, empty-input, and state-lifetime validation. First isolate column binding to keep scope small.

## Allocation and managed-memory evidence

Heap allocation profiles are default-rate sampled **alloc_space**, process-wide over initialization, benchmark calibration, setup, profiling overhead and timed execution. They are allocation totals, not retained/live heap or per-operation counts. Global's relatively small allocation sample includes substantial setup/profiler contamination. Use benchmark B/op/allocs/op above for execution-normalized observations.

Leading allocation-space sites:
- Global: scalarResize[uint64] 22.98%, scalarResize[scalarRegister] 20.84%, newSortedAggregateStream 14.59% flat. CPU optimization is more directly supported than pooling from this profile.
- Native grouped: GroupIndex.grow 51.44%, NewNumericVector 32.22%, GroupIndex.appendKey 12.59% flat. These are repeated per-query state construction/growth, not evidence of a leak.
- Generic grouped: GroupIndex.appendKey 43.89%, GroupIndex.grow 40.64%, NewInt64Vector 12.10% flat.
- Selection auto: scalarResize[Int64] 50.96%, selectRows 10.47%, allocateSelectedColumn 10.35%, scalarResize[int] 10.18% flat.
- Selection materialized: compactLayoutParents 78.05%, scalarResize[Int64] 12.54% flat.

Separate one-execution managed-budget probes at fixed cardinality:

| Workload | Peak at 32,768 rows | Peak at 262,144 rows | Used after execution |
|---|---:|---:|---:|
| native-global | 13,052 B | 21,244 B | 0 B in both |
| native-grouped, 4,096 groups | 618,844 B | 627,036 B | 0 B in both |
| generic-grouped, 4,096 groups | 736,078 B | 875,346 B | 0 B in both |

These observed peaks **increase**, despite fixed cardinality. They do not establish row-independent query memory. A source-derived consistency check: absent predicates compile to Bool(true) (`engine/compile.go:30–33`), whose workspace charges two bitmaps (`expr/workspace.go:18–28`, `expr/scalar_budget.go:142–147`); the maximum morsel is 65,536 rows (`pipeline/options.go:15`). Growing an active batch from 32K to 64K rows adds 2 × (1024−512) × 8 = 8,192 bytes, matching the two native deltas. This is not instrumented high-water attribution. Generic string batch views can additionally allocate rebased offset buffers at nonzero starts; peak overlap makes simple attribution unsafe. Attribution of the generic increase is not proven here; postexecution Snapshot operator/class maps show current usage, not per-operator high-water marks. Fixture/source buffers were created before attaching the query budget and remain resident. Managed peak excludes those buffers, runtime/allocator overhead and uncharged allocations; it is not RSS or total live heap. No RSS measurement was taken. One-execution probes establish release for those runs, not a long-duration leak proof. Their timings and B/op are instrumented first-execution diagnostics, not the baseline numbers.

Selection's existing repeated benchmark reports managed peak 295,552 B for auto versus 1,309,792 B for materialized and checks zero retained bytes after **every** iteration. This is managed workspace reservation evidence, distinct from heap B/op.

## Required correctness and admission gates for the next stage

Preserve exact lane semantics and row order. Float32 sums round after each addition; Float64 sums are sequential. Do not use reassociation, widened Float32 accumulators, FMA substitutions, or lane-parallel partial reductions without an explicitly compatible semantic contract. Preserve integer wrap at the current accumulation width, narrow-input widening behavior, UInt64 bits above 2^63, CountRows versus CountValues, empty/all-null result validity, min/max NaN and signed-zero behavior, and canonical grouped-key equality. Column bindings must not inspect payloads for count-only operations and must honor null bitmaps and borrowed input lifetime.

Before admitting a change, test all native widths, nullable/dense/sparse/empty inputs, cancellation and reuse, multiple batches, overflow, ±0/NaN/±Inf and order-sensitive floating sums; retain bounded reservation failure and release tests. Compare full-query timings on the same workload order with repeated controlled baseline/change samples, as well as native and fallback dispatch modes. This investigation is not a correctness-suite run and not evidence for other architectures, spills, parallel execution, skew, very large cardinality, or materialized result output.

## Files and publication hygiene

- `run.sh`, `run-memory.sh`: exact benchmark/profile and managed-probe commands.
- `*.profile.txt`, `*.bench.txt`: raw outputs; `summary.json`: baseline medians/ranges.
- `*.cpu-flat.txt`, `*.cpu-cum.txt`, `*.alloc.txt`: portable pprof text exports.
- `memory-*.txt`, `overlay-*.json`, `*_benchmark_test.go.txt`: instrumented probe results and source.
- `environment.txt`, `capabilities.txt`, `capabilities.go.txt`, `source-sha256.txt`, `source-unchanged.txt`, `binary-sha256.txt`, `go-list.txt`: provenance/checks.
- Raw `*.cpu`, `*.mem`, and `*.test` files are local-only and should not be published. Their hashes are retained for identification. `go list ./...` passed after temporary Go helper cleanup. This is package-discovery hygiene, not a full test pass.

## Independent review

Independent read-only review reconciled all five timing medians/ranges and hotspot percentages with raw outputs; confirmed both selection strategies use ModeAuto, current Int64 compaction is native AVX512, and query-budget peak differs from heap/RSS. Recommended first follow-up is batch-scoped native-column binding, preserving scalar encounter order and existing error/lifetime/budget contracts. Preserve source/binding validation errors, cancellation, synthetic empty-name COUNT(*) lanes and count-only payload avoidance. Rebind borrowed vectors on each batch; clear stale references at EOF, error and Close. Charge O(lanes) descriptor metadata and roll back cleanly on reservation or binding failure. The reviewer made no production edits or benchmark/test runs; source review is not new correctness or cross-architecture evidence.
