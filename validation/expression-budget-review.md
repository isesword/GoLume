# Expression workspace budget review

Branch: `fix/foundation-cse-memory-lifecycle`.
Base: `496548655518e1705d52b666a4305d23fa1f6fda`.

This follow-up supersedes the earlier report's remaining scalar-expression
accounting gap. It does not change the Linux kernel-gate test or CI settings.

## Ownership and reservation policy

- Keep borrowed input views separate from workspace-owned computed registers.
  Reusing a workspace across programs cannot overwrite a borrowed input buffer.
- Reserve register/output metadata, validity, fixed-width values, string offsets
  and payloads, and list payloads before allocating or growing them. Replacement
  reserves old-plus-new overlap before dropping the old allocation and charge.
- Retain reusable buffers and their charges across batches. Reattaching the same
  comparable manager refreshes context without discarding these buffers. Changing
  managers releases old state; non-comparable custom managers conservatively reset.
- Errors, cancellation, stream termination, and explicit Close discard owned
  state and release reservations. Returned vectors are borrowed views, not
  ownership transfers, and must not outlive workspace reuse or Close.
- Temporary string/parser/match allocations use explicit allowances. This is a
  managed live-allocation budget, not an exact Go heap or process RSS cap.
  Allocator size classes, GC-delayed reclamation, runtime/stdlib caches, fixed
  view objects, and reservation bookkeeping are outside exact accounting.
  Synchronous standard-library parsing/transformation calls cannot be interrupted
  midway; cancellation is checked at surrounding instruction/row/field boundaries.

JSON reserves both the new output and decoder scratch before construction,
retaining the old output charge until successful replacement. Its conservative
policy is `2048 + 16*totalRaw + 512*fields + 64*rows*(fields+1)` output bytes and
`8192 + 128*maxRowRaw + 2048*fields` scratch bytes. Raw bytes include unknown JSON
fields. These allowances deliberately trade admission precision for safety; they
are not measurements of encoding/json's internal allocation behavior.

## Validation environment

Linux/amd64, official Go 1.27.3, unchanged module versions. Repository testing
instructions and prior branch standards were followed. No AGENTS.md or
CONTRIBUTING file is present. This connector-materialized tree uses
`GOFLAGS=-buildvcs=false`; no source behavior is changed by disabling VCS stamping.

Fixture-dependent tests use the explicitly labeled generated fallback described
in `foundation-memory-review.md`, not the unavailable original benchmark binary.
No binary or local fixture symlink is included in this commit. Results therefore
do not establish compatibility with the original data distribution or its
performance characteristics.

## Tests and review

New tests cover low-budget numeric/string/JSON projections; repeated batches;
EOF/cancellation/early Close; buffer growth rejection and old-plus-new overlap;
borrowed inputs; program-size churn; manager changes; invalid roots/result types;
Ensure failure propagation; string case/replace/split/regex growth; and JSON
unknown fields, rollback, replacement, overflow, and cancellation. The 1 MiB JSON
rejection test asserts zero allocations before rejection. Warm scalar reuse
asserts no additional manager reservations. The old base fails the new numeric
and string integration tests because it admits tiny budgets and retains no
charge, confirming those regressions exercise the original defect.

Independent review examined alias lifetimes, reserve-before-allocation ordering,
replacement overlap, rollback, cancellation, size arithmetic, and stated scope.
It identified fixes now included and found no remaining blocking issue under the
documented accounting policy.

Passed locally:

- Uncached `go test ./expr ./physical ./lazy` and the added regressions.
- `go vet ./...` and GoDoc conventions.
- SIMD code generation, SIMD structure, and kernel registry consistency checks.
- `GOEXPERIMENT=simd go test -race ./expr ./physical ./lazy ./engine`.
- `GOEXPERIMENT=simd GODEBUG=simd=0 go test ./expr ./physical ./engine ./lazy`.
- `GOOS=linux GOARCH=amd64 GOEXPERIMENT=simd go build ./...`.

Both uncached full scalar and SIMD suites ran. Their only failing test was the
unchanged `internal/kernelgate.TestGateErrors`, at pipeline_test.go:211:
`re-gate without force: <nil>`. That test is explicitly outside this follow-up's
scope and its file is unchanged. Thus this is not a fully green full-suite claim.
GitHub Actions was not re-run or repaired; account-related CI availability is
outside this patch. No merge or deployment was performed.

## Performance observations

Sequential base/candidate runs of `BenchmarkFloat64ExpressionKernelMode`, five
samples at 500ms per sample, showed 176 B/op and 8 allocs/op before versus 64 B/op
and 1 alloc/op after. Scalar medians were 243,808 versus 218,656 ns/op; auto medians
were 230,590 versus 216,204 ns/op. These local samples show no observed regression,
not a cross-platform speed guarantee. Earlier concurrent test/benchmark samples
were noisier and were not used for this comparison.

The new 8192-row warm arithmetic benchmark measured managed/unmanaged modes at
approximately 14 microseconds, both 64 B/op and 1 alloc/op (three samples). JSON
managed mode added two reservation allocations versus unmanaged mode, about
285 KB/op in both; timing variation did not support a speed conclusion. Retained
buffer reuse is protected by deterministic reservation-count tests rather than
flaky wall-clock thresholds.
