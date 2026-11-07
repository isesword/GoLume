# Expression compilation and dense-owner review

Baseline: `a8cb86b33003390aa64ce0dc3c652b725edd3812` on
`perf/expression-buffer-liveness`. The remote ref was checked before work; all
local Go source files matched its blob IDs. Three local supplemental expression
test files were preserved, reviewed, run, and included in this change.

## Independent review

A separate reviewer inspected both the structural identity change and dense
physical-owner layout, and added independent regressions. No blocking correctness
issue was found. This is bounded review and test coverage, not an exhaustive
proof of all possible expressions or allocation failures.

Structural identity uses complete length-framed node descriptors, with child
identities rather than child serializations. Go map hash collisions cannot merge
different complete keys. Exact float bits, ordered operands, all nested Field
metadata, and explicit type/op parameters remain in identity. A compile-local
bound-node cache prevents repeated descent by lowering. Shared identity tables
across guarded regions do not share register memos or hoist evaluation.

The dense-owner planner preserves exact-type lifetime matching, pinned outputs,
borrowed input handling, and dedicated caches/nested/control state. The runtime
allocates logical views by instruction count and owner records by physical slot
count. First introductions are dense and monotonic; accounting and preparation
visit each owner once. Replacement admission remains transactional: an admission
failure preserves the old program and owners, and a later retry can succeed.
Unplanned private reference programs retain instruction-index ownership.

Added review tests cover every Field metadata component, nested child order,
independently bound equal trees, bounded descriptor size, operand order,
fallible expressions in separate dead/live regions, failed replacement admission,
old-program reuse, retry accounting, compact capacity, and complete release.
The prior supplemental tests provide independent arithmetic/string/regex/Boolean
oracles and planned/unplanned storage comparison. They are broad matrices, not
exhaustive numeric boundary verification.

## Final validation

The complete `scripts/validate.sh` command sequence passed, including GoDoc,
SIMD generation, kernel registry, SIMD structure, scalar and SIMD full repository
tests, runtime SIMD disabled, the script's SIMD width gate, linux/amd64 SIMD build,
repository race suite, vet, and the parquet lazy-demo. SIMD hardware on this
linux/amd64 host includes AVX512 for supported kernels; integer comparison
kernels report the scalar backend, so the script skips its +256/+512 gates.
Separate forced +128/+256/+512 test runs cover kernel/expr/engine/radix,
without claiming every operation has a hardware SIMD implementation.
An additional full SIMD expr race run also passed. macOS/arm64 was not run here. The final record is
`expression-compile-full-validation.txt`.

This materialized checkout has no usable Git metadata and shell scripts do not
retain executable bits. The sequence was therefore run with `GOFLAGS=-buildvcs=false`
and `bash scripts/check_simd_structure.sh`; the repository scripts were not
modified. The first attempt stopped at those environment issues, then the whole
sequence was rerun successfully with these adaptations. The parquet fixture is a
local regenerated fixture rather than the remote binary blob; its demo is not a
byte-for-byte replay of that remote fixture. Missing historical validation logs
and the tracked test binary were not replaced or published.

## Remaining scope

The public binder still expands repeated source subtrees: this change eliminates
recursive whole-subtree identity strings, not all repeated source-DAG work.
Logical register views and instructions remain proportional to the compiled DAG;
variable-width payloads and dedicated state retain their existing allocation
rules. Benchmarks are local microbenchmarks, not end-to-end query speed claims.
