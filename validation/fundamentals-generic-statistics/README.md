# Generic keys and fixed-state statistics integration gate

This stage combines generic native/composite grouping with fixed-size Var/Std and
First/Last because they share compiler, state, codec and output dispatch changes.
It does not implement exact n_unique, variable-width First/Last, or median/quantile.

Independent reviews cleared canonical encoding/ownership, grouping paths, native
outputs, stable endpoint ordinals and V4 state after fixing wide-output metadata
admission, duplicate names, Float32 statistics result dtype and low-budget error
classification. Regression tests are included alongside the implementation.

## Validation

The frozen source was layered over the published ordered-transform stage, without
concurrent exact-distinct edits. Linux/amd64 Go 1.27.3 checks passed:

- Full scalar `go test ./...` and full `GOEXPERIMENT=simd go test ./...`.
- SIMD race tests: aggregate, engine, lazy, hashagg, physical and optimizer.
- `go vet ./...`, GoDoc, generated SIMD consistency and kernel registry checks.
- Linux/arm64 SIMD-enabled cross-build (compile only, not hardware execution).
- Pinned seven-case Polars statistics oracle regeneration check.

`full-tests.txt` includes the successful suites and an incorrect initial GoDoc
command path. `structural-checks.txt` records the corrected tools/checkgodoc path
and successful remaining checks. This was a harness path error, not a code test
failure. `vet.txt` is empty on success; `oracle.txt` records the oracle check.

Performance samples and caveats are in ../fundamentals-generic/ and the statistics
semantics document. The legacy specialized hot-loop instructions and allocation
counts are unchanged. Timing noise prevents a universal regression-bound or
speedup claim. Auto SIMD gates are unchanged; canonical framing and stable moment
recurrences remain scalar. No remote CI pass is inferred from these local gates.
