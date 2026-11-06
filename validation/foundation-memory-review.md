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

## Review and verification

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

## Remaining risk

Scalar expression budgeting is not complete. Fixed-width and variable-width
string/JSON output paths still need consistent reserve-before-growth ownership
and error propagation. Registers may borrow source buffers or own computed
buffers and can be reused across programs, so capacity alone does not identify
already charged ownership. This patch intentionally does not claim to solve
that broader accounting problem or to make every query strictly memory bounded.
