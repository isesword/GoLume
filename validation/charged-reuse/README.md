# Charged memory reuse and shared I32 kernel: WIP evidence

**Not performance-complete.** The combined diagnostic median is 13.338 ms versus
17.309 ms for WIP 24d742d, but remains above the old exact 965e0b33 baseline formal 10.04 ms reference
and 10.341 ms 3% threshold. No merge or full performance acceptance is implied.

## Four-way attribution

Three alternating fixed 300-execution samples per variant, three warmups, explicit
GC before measurement, GOMAXPROCS=4 and cores 5–8. Identical Parquet input, 64 K scan
rows, 256 K aggregate wave, 256 state shards, 64 MiB in-flight limit, original unlimited
query/root limit and Materialize=false. Each execution checks 472475 selected rows,
219196 groups and checksum 236716301. All 3,600 measured executions passed.

- WIP: 17.309 ms wall, 49.856 CPU-ms, 53.710 MB and 11717.9 allocations/op.
- Charged reuse only: 14.599 ms, 42.235 CPU-ms, 32.426 MB and 6273.0 allocations/op.
- Shared kernel only: 16.060 ms, 46.413 CPU-ms, 53.711 MB and 11717.4 allocations/op.
- Combined: 13.338 ms, 38.737 CPU-ms, 32.425 MB and 6273.1 allocations/op.

Combined is 22.9% faster in this diagnostic and uses 22.3% less CPU than WIP.
The idle cache retains 20,872,392 genuinely charged bytes in the reused variants;
all variants return root usage to zero at Prepared.Close. Active query capacity
is admitted in full on each checkout. These are diagnostic samples, not a new
formal 10×1 s gate or a claim that the original gate passed.

`four-way-runs.json` preserves each report; `four-way-summary.json` preserves
medians and individual samples. `control-manifest.json` records exact binary
hashes and source differences from 24d742d. No compiled diagnostic harness is added
to the production test packages.

## Controlled row-kernel comparison

Fixed 300-operation, one-core controls use identical synthetic partitioned input,
slot/state hashes, 641600 probes, 472475 rows and 219196 groups. They do not use the
actual Parquet key order or its two-wave cache behavior. The normalized old row
loop is a test-only reference, not the old executor.

Median CPU-ms: WIP 11.249; prior 9.291; bitmap-v2 8.562; dense borrowed 8.228;
dense live 8.396; normalized old reference 9.349. The borrowed/live dense samples
overlap, so no superiority claim is made between those two. Production uses the
independently reviewed borrowed dense implementation. No extra prehash array,
changed table capacity or changed hash function is involved.

See `hash-runs.json`, `hash-summary.json` and the included text harnesses.

## Remaining CPU and wall-time limits

The combined exact 300-execution profile samples 39.367 CPU-ms/op, close to process
CPU 39.748. Exclusive buckets: hash/update 14.333, allocation/zeroing/GC 6.300,
scan 6.633, classify/scatter 5.533, predicate 2.133, selected append 2.533, other 1.900.
Hash includes lookup 6.533, shared Init/Add about 3.7 and mixing 1.567 CPU-ms. These
are disjoint CPU buckets, not additive wall phases.

The combined profile confirms the dense path: `lookupI32Slot` →
`aggregateSumCountNoGrow` → `AggregateValues` → `aggregatePacked`.
The method name without “dense” is not evidence of bitmap fallback.

The WIP profile was anomalously slower than its stable matrix (23.509 ms and
62.147 CPU-ms). Its raw output is preserved but must not be used to inflate the
claimed improvement. `cpu-profile-decomposition.json` also includes prior old
exact-N evidence for context, not a fresh paired timing inference.

Updated coarse tracing checks 120 executions per variant, original budgets,
exact 262144+210331 flush sizes, a new query owner per execution, stable charged
idle usage, and zero after Close. Combined trace median 12.909 ms. An optimistic
two 256 K-buffer replay gives 10.665 ms (IQR 10.254–12.282), already above the gate
before charging contention for the same four CPUs. First fill 4.277 ms and final
compute 2.762 ms remain exposed. Deleting all Next intervals gives 9.133 ms but also
unrealistically deletes first-wave decode/wait; overlap alone cannot do that.
The independent CPU / 4 lower bound is about 9.684 ms and must still apply.

This rules out treating two-wave overlap alone as a credible gate fix. No overlap
implementation is included. Candidate next investigations are actual-W4 versus
controlled-kernel cache/probe behavior, and a measured reduction of the serial
predicate/selection stage using existing shared kernels. Any follow-up requires
an attributable mechanism and new end-to-end evidence, not another layout toggle.

`stage-analysis.txt` and `two-wave-replay.json` preserve derived bounds and
phase summaries. The full compressed trace is not included in this checkpoint. Phase children are not added to their enclosing
flush; worker elapsed sums are work-wall, not CPU or extra query wall time.

## Correctness and review

Full combined `scripts/validate.sh` passed: scalar, SIMD and forced widths,
SIMD-disabled fallback, broad race checks, vet, generated-kernel/structure checks,
GoDoc and demo. `full-validation.txt` is the original log. Removing only the
Linux diagnostic harness from normal test discovery is separately checked.

Independent scoped reviews verified:

- Stable account ownership, exact hard caps, Attach/Detach/Close, root reclamation
  outside locks, and spill-callback recursion suppression.
- Cache admission credits, actual backing capacities, query/source/context
  isolation, pressure eviction, cancelled/failed-query destruction, and Close
  waiting for an already-unlinked eviction.
- Shared kernel consumed-prefix/growth rollback, all aggregate layouts, NULL
  fallbacks, private borrowed-view lifetime and the non-NULL dense gate.

A Close/eviction race and a test-only cross-platform helper dependency were found
and fixed before this checkpoint. Independent regression/race checks passed.
Darwin/Windows hashagg tests were compile-checked only, not executed.
The dense engine path preserves the existing ≤4096-row context checks; kernels
check cancellation through failed reservations at growth, not per input row.

## Reproduction

Use the repository's required Go toolchain and GOEXPERIMENT=simd. Generate the
standard `testdata/bench-1m.parquet` fixture using the repository fixture workflow;
its SHA-256 must match `control-manifest.json`. Keep test and timing jobs serialized.
Never compare concurrent heavy jobs or change input/budgets to improve a result.

For current combined fixed-count profiling in an isolated checkout:

1. Copy `engine-fixed-count-harness.go.txt` to `engine/reuse_fixed_cpu_test.go`.
2. Build `GOEXPERIMENT=simd go test -c ./engine -o engine-profile.test`.
3. Set GOMAXPROCS=4, W4_PROFILE_N=300, GOCOLUMN_TEST_PARQUET to the verified file,
   W4_PROFILE_REPORT to a fresh JSON path, and optionally W4_PROFILE_PATH for pprof.
4. Run `taskset -c 5,6,7,8 ./engine-profile.test -test.run '^TestW4FixedIterationCPUProfile$' -test.v`.

The WIP control is checkout 24d742d plus exactly the same harness. For reuse-only,
apply the current memory/engine reuse change set to 24d742d, retain its aggregate
and hashagg code, and keep the original AggregateSelectedNullableValues call in
aggregatePacked. For kernel-only, apply current aggregate/api.go and hashagg/i32.go
to 24d742d and only the nullable/layout-gated dense call-site delta, not the cache
or lifecycle changes. The control manifest gives exact hashes/diffs; do not
substitute “new code with cache disabled” for the original WIP control.

For hash-only controls copy the selected/dense harness text to the corresponding
isolated hashagg package. Set GOLUME_HASH_OPS=300, GOLUME_HASH_MODE to native-reset,
native-fresh, dense-reset, dense-fresh or old-reference-reset as supported, and
GOLUME_HASH_REPORT to a fresh JSON path. With GOMAXPROCS=1 and core 5 run
`-test.run '^TestI32FixedHashProfile$' -test.v`. Input and slot/probe checks are
mandatory. Exact historical non-adopted control source archives are not included;
the recorded hashes and results identify those experiments without claiming that
every historical control can be rebuilt from this checkpoint alone.

Text report path fields use relative source/artifact names; measured values are
unchanged. `artifact-provenance.json` records original and published text hashes.
Binary CPU profiles, full compressed traces and diagnostic source archives are
not published. The text harnesses and instructions above support new measurements
of the current implementation; the included reports preserve observed results.

For coarse tracing, run the standalone instrument script with
GOLUME_DIAGNOSTIC_ROOT, GOLUME_WIP_SOURCE and GOLUME_COMBINED_SOURCE set to
isolated paths, then build the two diagnostic engine test binaries and set
W4_DIAG_N=40 and W4_DIAG_OUT. The same input/budget/Close assertions apply.
