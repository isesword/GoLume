# Dense physical-owner metadata

Baseline: `a8cb86b33003390aa64ce0dc3c652b725edd3812` on
`perf/expression-buffer-liveness`. Source verified against remote tree.

## Architecture

The storage planner now assigns dense physical owner IDs, independent of logical
instruction indices. First introductions are monotonic; later references reuse
an existing owner. Type-specific lifetime allocation, guarded-region boundaries,
and pinned result roots are unchanged. Every column/cache/variable-width owner
still retains a dedicated physical record.

The executor allocates logical register views by instruction count and physical
records by owner count. Fixed-capacity admission and preparation traverse first
owner introductions once, rather than inferring physical ownership from equality
with a logical instruction index. Unplanned private programs retain dedicated
instruction-index owners. Replacement reservation still precedes allocation and
release of the previous workspace. No additional runtime mapping allocation or
unbudgeted owner array is introduced.

On this amd64 build each register record is 1,312 bytes. At depth 128 the int64
chain has 130 logical instructions and four physical owners; the Boolean chain
has 129 instructions and three owners. Both eliminate 126 unused physical records
(165,312 retained/managed bytes). Logical views still scale with instruction count.

## Measurements

Command (same environment, separately, before and after):

```
go test ./expr -run '^$' \
  -bench '^BenchmarkExpressionLiveness/(int64|bool)/depth=(8|128)/' \
  -benchtime=150ms -count=3
```

Go 1.27.3, linux/amd64, AMD EPYC 9V74; 65,536 rows. Compilation is outside the
benchmark timer. Raw samples are in `expression-dense-owner-before.txt` and
`expression-dense-owner-after.txt`.

| Depth-128 case | Before ns/op median | After ns/op median | Before B/op median | After B/op median | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| int64 cold | 15,603,987 | 12,365,701 | 1,951,105 | 1,776,376 | 548 | 44 |
| int64 warm | 12,989,429 | 11,509,688 | 64 | 64 | 1 | 1 |
| bool cold | 452,031 | 330,559 | 402,412 | 226,410 | 536 | 32 |
| bool warm | 294,992 | 243,417 | 0 | 0 | 0 | 0 |

Depth-128 managed/retained bytes: int64 1,938,688 to 1,773,376; bool 387,648 to
222,336. The deterministic metadata and allocation reductions support adopting
the change. Three short samples on a shared machine do not establish a general
throughput speedup or isolate every source of timing variation. No end-to-end
query performance claim follows from these samples.

## Verification

- Full `go test ./expr` passed.
- `go test -race ./expr` passed.
- Targeted `TestDense|Test.*Budget|Test.*Close|TestLiveness` passed.
- New dense-owner tests assert monotonically introduced compact IDs, exact
  zero-row metadata, owned capacities, managed/retained equality, row growth and
  shrink reuse, close release, and unplanned dedicated-register compatibility.
- Existing differential/liveness tests cover nested regions, multiple outputs,
  scalar/SIMD kernels, error domains and admission failure/retry/close.

Broader final repository tests and independent review belong to the combined
compiler/runtime change's final validation record.
