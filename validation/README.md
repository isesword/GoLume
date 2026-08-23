# Validation artifacts

Raw outputs from the final Phase 2.3 release candidate on the isolated
validation host are under `phase2.3-final/`.

- `godoc.log`: package/exported API GoDoc checker.
- `test-scalar.log`: full scalar test suite.
- `test-simd.log`: full `GOEXPERIMENT=simd` suite.
- `test-simd-emulated.log`: SIMD API under `GODEBUG=simd=0`.
- `simd-widths.log`: forced 128/256/512 correctness regression.
- `test-race.log`: race detector over execution, lazy, optimizer, and Parquet source packages.
- `vet.log`: `go vet` output; empty means no diagnostics.
- `lazy-demo.log`: real 1M-row Lazy explain + repeated Prepared execution.
- `benchmark-lazy-overhead.txt`: direct Prepared vs Lazy Prepared execution-layer comparison.

The sandbox uses offline-only Arrow-Go compatibility shims documented under
`tools/offline-poc/`. Production `go.mod` contains no local replacements.
