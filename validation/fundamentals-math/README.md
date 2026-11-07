# Math integration validation

Validated 2026-11-07, linux/amd64, Go 1.27.3, AMD EPYC 9V74 shared host.
An isolated stage contains the reviewed baseline, global aggregates, numeric
casts and math changes, excluding concurrent multi-column/window work.

Passed full scalar and SIMD `go test ./...`; SIMD race tests for expr, kernel,
engine, lazy, optimizer and physical; `go vet ./...`; GoDoc; gensimd -check;
kernelgate check; and the SIMD structure script.

`GOOS=linux GOARCH=arm64 GOEXPERIMENT=simd go build -buildvcs=false ./...`
passed. The first cross-build attempted VCS stamping and failed because this
source snapshot has no Git metadata; the log retains that failure and the
successful unstamped retry. This is compile-only ARM64 coverage, not execution
or performance evidence.

Independent review added exhaustive Int8 modulo/floor-div pairs, 8,193-row mask
and reuse cases, and arbitrary Float32 bit-pattern scalar/SIMD comparisons.
Focused race, checkptr and forced widths 128/256/512 passed.

See docs/MATH_KERNELS.md and validation/fundamentals/math-* for typed semantics
and ten-sample kernel/expression timings. Only dense floating ABS/SQRT have
forced-mode SIMD candidates; Auto gates remain unchanged. Accurate float modulo
and mixed integer promotion differences are explicit. Round currently has zero
decimal places. No claim of universal SIMD or cross-architecture speedup is made.
