# Expression-projection predicate pushdown validation

Base: b8ce5dc3ef44dd8fbc4d0b0a92254de8adb65f48, with source and test blob
identities checked against its complete remote tree. Historical logs and generated
benchmark fixtures in the local validation snapshot are not republished.

This stage adds simultaneous passthrough/direct-alias substitution for source
pruning, preserves original residual filters and conservatively blocks any
projection with a potentially fallible output. No new execution framework,
compatibility shims or public API are introduced.

The independent execution suite compares the original logical plan against the
optimized plan, using physical lowering without optimizer invocation for the
reference. It covers NULLs, NaN, alias replacement/swaps/chains, computed-name
barriers, retained conditional errors and statically dead fallible children,
lower guards, exact error strings and real Parquet row-group pruning/residuals.

`full-tests.txt`: full scalar/SIMD suites, vet, GoDoc, generated SIMD/registry
checks and SIMD structure checks.

`race-widths-build.txt`: SIMD race tests for optimizer/lazy/engine/physical/Parquet,
disabled SIMD and forced 128/256/512-bit targeted suites, and Linux/arm64 SIMD
cross-build. ARM64 is compile-only, not execution validation.

`benchmark.txt`: five 200ms scalar execution samples at GOMAXPROCS=1. The fixture
has 65,536 rows in 16 Parquet groups; 4,096 rows are read after pruning and 2,048
rows survive the residual filter. Source-row counts derive from actual source
statistics and the applied pushdown report. See the design document for medians
and limitations. Allocated bytes are not peak resident or managed memory.

Local test results do not imply remote CI execution or merge readiness.

Independent implementation review found no production correctness issue. Its
optimizer regression file additionally checks simultaneous alias identity,
all-or-nothing mixed computed conjuncts, AST slice independence, UInt64/Float32
and dictionary/Arrow binding metadata, existing operator barriers and inner
filters. `final-review-tests.txt` repeats full scalar/SIMD, affected SIMD race,
vet and GoDoc checks after those additions.
