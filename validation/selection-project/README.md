# Selection-aware Filter → Project validation

Baseline: `9bbf440d8a221103008aa230fb68fd4996278d61`.
Toolchain: Go 1.27.3 linux/amd64. Shared virtualized host; benchmark timings are
comparative observations, not universal throughput promises.

See `docs/SELECTION_PROJECT.md` for scope, ownership, retry behavior and fallbacks.
Independent implementation review approved after fixing two lifetime edges:
logical guarded-region references are dropped before replacing gathered storage;
borrowed filter indices are dropped before advancing the filter and on termination.
The regression tests remain in the source tree.

## Benchmark methodology

`BenchmarkSelectionProject` compares direct automatic selection handoff with a
concrete-type-hiding wrapper that forces the original materialized filter boundary.
Both use identical inputs, predicates, expressions and public output consumption.
Input has 8,192 rows and either 2 or 32 int64 columns; narrow output expressions
read one column. Cases keep 1%, 50%, 99% or 100% of rows, with and without retaining
input. `includeInput=true` deliberately falls back in the final implementation.
Peak bytes are tracked memory-manager reservations, not process RSS. Allocation
metrics are Go benchmark allocations. Logical source/projected row metrics are not
instrumented kernel visits. Benchmarks include opening and closing one stream per
iteration; they are not steady-state-only numbers.

The full scalar and SIMD matrices use three samples, 300ms and 200ms respectively.
SIMD uses `GOEXPERIMENT=simd` and `kernel.ModeAuto`. The scalar matrix was measured
before switching the benchmark selector from ModeScalar to ModeAuto (equivalent
without the SIMD experiment). They precede the final recursive stale-reference
invalidation and filter-index cleanup; `final-recheck.txt` reruns representative
wide and narrow cases on the final reviewed implementation.

Initial SIMD medians for 32 columns, narrow projection:

| Kept | Materialized | Auto | Ratio |
| --- | ---: | ---: | ---: |
| 1% | 241.0 µs | 52.6 µs | 4.58× |
| 50% | 683.2 µs | 230.9 µs | 2.96× |
| 99% | 843.3 µs | 331.8 µs | 2.54× |
| 100% | 342.8 µs | 346.3 µs | 0.99× |

At 99%, tracked peak reservation fell from 2,566,696 to 557,488 bytes (78.3%).
All-pass paths have identical peak and allocation counts. Full-input fallback
cases retain identical deterministic allocation/peak metrics; timing differences
in those cases illustrate shared-host noise. Narrow two-column partial cases had
much smaller benefits than the wide fixture; optimizer column pruning can make
that the more representative end-to-end case.

## Analytical work, separate from timing

For partial selections in the wide fixture the materialized boundary copies
32 × selected values; the automatic path gathers 1 × selected values. At 99%
(8,110 selected rows), this is 259,520 versus 8,110 values. Existing compaction
kernels scan source-column spans; selected gather reads selected indices only.
Predicate evaluation still sees all 8,192 source rows, and both projection
executors see only the same selected rows. This does not claim a 32× end-to-end
speedup or measured hardware memory traffic. At 100%, both paths borrow without
copying input values.

## Final gates

Raw command output is in `final-validation.txt` and `final-recheck.txt`.
No remote CI run is inferred from local success. This branch does not match the
repository's main/master push trigger, so branch-only publication does not itself
schedule the configured CI workflow.

The first cross-build invocation could not obtain VCS status because this checkout
is a source snapshot without a Git worktree. The raw error is retained. Cross-builds
were rerun with Go's supported `-buildvcs=false` flag; no source or dependency was
changed to bypass compilation checks.

Final reviewed-code SIMD recheck medians (250ms × 3):

| Input width | Kept | Materialized | Auto | Ratio |
| --- | --- | ---: | ---: | ---: |
| 2 | 1% | 43.45 µs | 30.72 µs | 1.41× |
| 2 | 99% | 322.34 µs | 291.31 µs | 1.11× |
| 2 | 100% | 266.62 µs | 273.52 µs | 0.97× |
| 32 | 1% | 265.38 µs | 48.17 µs | 5.51× |
| 32 | 99% | 817.72 µs | 324.56 µs | 2.52× |
| 32 | 100% | 286.84 µs | 287.90 µs | 1.00× |

Passed gates: full scalar and full SIMD tests; SIMD race for expr, physical, lazy,
engine, optimizer, memory and Arrow source; runtime widths disabled/128/256/512;
vet; GoDoc; SIMD generator, registry and structure checks; linux/amd64,
linux/arm64 and darwin/arm64 SIMD cross-builds. Final changed packages, including
independent lifetime regressions, were rerun scalar/SIMD/race after the last fix.
