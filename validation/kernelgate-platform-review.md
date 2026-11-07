# Kernel gate platform test correction

Base: `a51c5d2f9117bb4d89220bee5d1d2368ce3c4a7c`.
Branch: `fix/foundation-cse-memory-lifecycle`.

`TestGateErrors` assumed compare-int32-gt was already approved on the executing
platform, but the shared fixture approves it only on darwin/arm64. On Linux/amd64,
StateOn correctly treats the absent architecture entry as candidate, allowing
first approval and returning nil. The old test therefore failed with
`pipeline_test.go:211: re-gate without force: <nil>`.

The test now establishes its approved prerequisite through the normal Gate
pipeline with Force:true and verifies LocalArch and StateApproved. It then
asserts that a non-forced repeat fails before any benchmark execution, and that
a forced repeat executes exactly one benchmark and remains approved. The shared
fixture, production approval logic, actual registry/gates files, and GitHub
Actions configuration are unchanged. All writes performed by this test remain
inside t.TempDir through its fake runner.

The test is platform-neutral for the registry's supported architecture list;
it does not make approval evidence portable across architectures. Only
Linux/amd64 was executed here, not macOS or Windows.

Independent review found no blocker. Validation uses Go 1.27.3 with
GOFLAGS=-buildvcs=false in the connector-materialized tree. Full-suite fixture
tests still use the previously disclosed synthetic Parquet fallback, not the
unavailable original binary. Earlier reports' kernel-gate failure was historical;
this follow-up corrects it under the user's subsequent explicit authorization.

Passed after the correction:

- Targeted kernelgate packages; independent TestGateErrors run 20 times.
- `go test ./... -count=1` (complete scalar suite, no exclusions).
- `GOEXPERIMENT=simd go test ./... -count=1` (complete SIMD suite, no exclusions).
- `go test -race ./internal/kernelgate ./tools/kernelgate -count=1`.
- `go vet ./...` and repository GoDoc conventions.

Remote Actions was not rerun. No merge or deployment was performed.
