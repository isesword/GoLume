# Foundation safety patch review

Base: `78a9cbb875a632218050a7080526f7109c86ff07` (`main`).
Branch: `fix/foundation-cse-memory-lifecycle`.

## Changes

- Frame scalar CSE keys structurally, including arbitrary column/field names,
  float literal bits, and recursive JSON field metadata. Preserve sharing of
  genuinely identical expressions.
- Reserve bounded join matched-row and probe-key scratch before allocation.
  Preallocate build locations to the capacity included in the reservation.
- Drop all scalar/query workspace references before releasing their memory
  reservations, including workspaces still reachable from an idle pool.
- Add regression tests for the collisions, reservation rejection and release,
  zero allocations on rejected join scratch, and workspace Close/reuse.

## Initial review and verification

Repository CI and `docs/TESTING_INFRA.md` were read. No `AGENTS.md` or
`CONTRIBUTING` file exists in the base tree. Changes received a separate static
review of key framing, schema metadata, memory-manager interfaces, ownership,
cleanup/error paths, and test API compatibility.

Validation ran on Linux/amd64 with official Go 1.27.3 and downloaded module
versions from the existing go.mod/go.sum. No dependencies or toolchain
requirements were changed.

Passed:

- gofmt on all seven changed Go files.
- All nine new regression tests, including zero allocations when join scratch
  reservations are denied.
- `go test ./expr ./engine ./physical -count=1` (four engine fixture tests skip).
- `go vet ./...`.
- SIMD race tests for engine, hashagg, join, radix, scheduler, physical,
  optimizer, memory, source/parquet, and source/arrowrecord.
- GoDoc, SIMD codegen, SIMD structure, and kernel registry consistency checks.
- Scalar suite with only the three failures below explicitly excluded.
- Runtime SIMD-disabled package tests and expr/lazy SIMD race tests, excluding
  the two unavailable lazy fixture tests.
- SIMD +256 kernel/expr/engine/radix tests (engine fixture skips still apply).
- Linux/amd64 SIMD build with `-buildvcs=false`. The initial build with VCS
  stamping failed because this is a connector-materialized source tree, not a
  normal Git checkout; disabling stamping leaves the compiled source unchanged.

The six CSE/Close regression tests were also run against the original main
implementations: all six failed as expected. The fixed implementations were
restored and the nine regression tests passed.

Full `go test ./...` and `GOEXPERIMENT=simd go test ./...` were executed and
**did not pass**. The failures are:

1. `internal/kernelgate.TestGateErrors`: the unchanged fixture marks a kernel
   approved only on darwin/arm64, but this Linux run expects it to be approved
   on the local architecture (`re-gate without force: <nil>`).
2. `lazy.TestLazyParquetAggregateEndToEnd` and
   `lazy.TestLazyPreparedRepeatedExecution`: the 11 MB binary
   `testdata/bench-1m.parquet` cannot be retrieved with the text-only repository
   connector. Four engine fixture tests explicitly skip for the same reason.

These limitations are not suppressed in the full-suite results. The existing
binary fixture and unrelated architecture-sensitive test need verification in
an ordinary checkout before merging. The macOS CI matrix was not run locally.
The workflow is configured for main/master pushes, pull requests, and manual
dispatch. After publishing code commit `27af6c6184b7c38571265a48582650fb1580773c`,
GitHub nevertheless recorded a `BuildFailed` run with `startup_failure` and no
jobs: https://github.com/isesword/GoLume/actions/runs/42174941177 .
This is not a successful remote CI run; its underlying startup reason was not
available in the workflow/job responses.

## Synthetic Parquet follow-up

The original binary could not be retrieved through the text-only connector.
At the user's request, `tools/genfallbackfixture` now generates a separate,
deterministic functional-test substitute. It does **not** reproduce the original
fixture's bytes, distributions, encoding choices, statistics, or performance.
The tracked `testdata/bench-1m.parquet` is unchanged on the remote branch.

Generated artifact:

- Path: `testdata/bench-1m.synthetic.parquet` (local only, not committed).
- Required schema: `pred:int64`, `key:int32`, `value:int64`.
- 1,048,576 rows, 17 Snappy row groups, no dictionary encoding, no nulls.
- 3,566,591 bytes.
- SHA-256: `115f46c063da52e2b3b8c883522e8d29a4559b410772bc08681a87235d783876`.
- Two independent generations produced the same bytes.

The synthetic data is deliberately constructed to satisfy the unchanged
historical summary assertions: 472,475 selected rows, 219,196 groups, and
236,716,301 sum. This alone is not a correctness oracle. The generator also
computes a plain row-loop map and compares **every group's count and sum** with
materialized results from direct-fused, bitmap-direct, and radix execution.
All three comparisons passed. The input includes threshold boundary values,
negative rejected values with disjoint keys, repeated keys across row groups,
and non-round batch/row-group tails.

Generation refuses to overwrite an existing file or symlink; both protections
are tested. Reproduce with:

```sh
go run ./tools/genfallbackfixture
```

During this validation only, the absent local hardcoded fixture path was a
symlink to the generated synthetic file. No existing tests or assertions were changed,
and neither binary data nor the local symlink is included in the commit.

Results with the substitute:

- All six previously blocked tests passed: TestRadixHonorsMemoryBudget,
  TestParquetStrategies (all strategies), TestMaterialize (all strategies),
  TestPreparedQueryRetainsAndReleasesWorkspace,
  TestLazyParquetAggregateEndToEnd, TestLazyPreparedRepeatedExecution.
- Full scalar and SIMD suites ran; the only failing test was the unchanged
  Linux-sensitive `internal/kernelgate.TestGateErrors` described above.
- `GOEXPERIMENT=simd go test -race ./engine ./lazy ./physical` passed with the
  substitute present, including the formerly blocked fixture tests.
- Generator tests, vet, and GoDoc checks passed.

This establishes functional coverage on the new dataset. Original-fixture
reproduction, null/dictionary/compression-distribution coverage, performance
comparability, and a fully green remote CI run remain unverified.

## Remaining risk

Scalar expression budgeting is not complete. Fixed-width and variable-width
string/JSON output paths still need consistent reserve-before-growth ownership
and error propagation. Registers may borrow source buffers or own computed
buffers and can be reused across programs, so capacity alone does not identify
already charged ownership. This patch intentionally does not claim to solve
that broader accounting problem or to make every query strictly memory bounded.

The subsequent expression-accounting implementation and its remaining limits are
recorded in [expression-budget-review.md](expression-budget-review.md). That
follow-up supersedes the scalar-budget gap above; neither report claims an exact
Go heap/RSS cap.
