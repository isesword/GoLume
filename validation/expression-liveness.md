# Expression buffer liveness validation

Validation date: 2026-11-07 (UTC).

## Result

Last-use reuse reduces fixed-width and Boolean temporary payloads without increasing warm-evaluation allocation counts. For 128-operation chains over 65,536 rows, managed peak falls by 97.19% for int64 and 84.18% for Boolean. Cold-evaluation latency falls by 37.63% and 50.05%, respectively (benchstat p=0.002, six samples). Warm-evaluation timing differences are not statistically significant in this run; this is a memory and cold-start improvement, not evidence of a general warm-throughput speedup.

## Source and host

- Baseline main commit: `de12170760035fb5b47a96e84f835e5f74b68836` (tree `1b82dedf25fcde1c16b57f84d680911b7c3d7ec7`).
- Baseline implementation was copied before applying the liveness change. Identical new benchmark and correctness-test sources were used for both versions.
- Go 1.27.3, linux/amd64; Debian GNU/Linux 13 (trixie), Linux 6.18.57.
- CPU: AMD EPYC 9V74 80-Core Processor; nine logical CPUs available to this container.
- Scalar kernels, `GOMAXPROCS=1`, `-cpu=1`; shared virtualized host, no CPU affinity pinning.

## Method

- Each fixture has 65,536 input rows. Int64 chains repeatedly add one; Boolean chains repeatedly apply NOT. Depths are 8, 32, and 128.
- Input values are deterministic. Int64 input is NULL every fifth row; Boolean input is NULL every seventh row. Input construction and compilation are outside the timed section.
- Warm timing reuses an initialized public `Workspace`. Int64 uses `Evaluate`; Boolean uses compact `EvaluateBool`.
- Cold timing includes workspace creation, first evaluation, retained-capacity inspection, and `Close`. The memory manager is reused, and every iteration must release all reservations.
- Three alternating before/after blocks, two samples per process, `-benchtime=300ms`: six samples per fixture/version. An initial block potentially overlapping unrelated validation was discarded and replaced; only the three retained blocks appear in the raw files.
- `peak-managed-B` is the maximum successful reservation total. `retained-B` is owned workspace capacity after evaluation. They are equal on these fixtures. Neither includes borrowed input, compiled program metadata, or process-wide RSS.
- Go `B/op` and `allocs/op` are separate runtime allocation measurements. Warm int64 results retain the existing 64 B / 1 allocation per evaluation; compact Boolean evaluation remains 0 B / 0 allocations.
- Register metadata still scales with instruction count. The optimization bounds temporary row payloads by simultaneous liveness, not total program size.

## Managed memory and cold allocations

Values below are exact across all six samples. Managed peak is the same for warm and cold cases.

| Type | Depth | Peak before (B) | Peak after (B) | Reduction | Cold B/op before | Cold B/op after | Cold allocs/op before | Cold allocs/op after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| int64 | 8 | 4,818,528 | 1,623,808 | 66.30% | 4,821,648 | 1,626,480 | 98 | 68 |
| int64 | 32 | 17,660,640 | 1,686,784 | 90.45% | 17,675,024 | 1,699,184 | 314 | 164 |
| int64 | 128 | 69,029,088 | 1,938,688 | 97.19% | 69,049,616 | 1,951,088 | 1,178 | 548 |
| bool | 8 | 170,928 | 72,768 | 57.43% | 173,832 | 75,240 | 86 | 56 |
| bool | 32 | 626,736 | 135,744 | 78.34% | 643,464 | 150,504 | 302 | 152 |
| bool | 128 | 2,449,968 | 387,648 | 84.18% | 2,472,840 | 402,408 | 1,166 | 536 |

## Timing and throughput

Medians of six samples. Throughput counts input rows, not arithmetic or Boolean operations. See the benchstat file for confidence intervals and significance tests.

| Type | Depth | State | Before (ms/op) | After (ms/op) | Time change | Before (M rows/s) | After (M rows/s) |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| int64 | 8 | warm | 0.7058 | 0.7197 | +1.97% | 92.86 | 91.07 |
| int64 | 8 | cold | 1.0485 | 0.8755 | -16.50% | 62.51 | 74.92 |
| int64 | 32 | warm | 2.8657 | 2.8214 | -1.55% | 22.88 | 23.23 |
| int64 | 32 | cold | 4.4432 | 2.8140 | -36.67% | 14.76 | 23.29 |
| int64 | 128 | warm | 12.0672 | 10.6673 | -11.60% | 5.43 | 6.14 |
| int64 | 128 | cold | 17.2683 | 10.7699 | -37.63% | 3.80 | 6.09 |
| bool | 8 | warm | 0.1150 | 0.1105 | -3.85% | 570.95 | 593.01 |
| bool | 8 | cold | 0.1376 | 0.1261 | -8.36% | 476.29 | 519.78 |
| bool | 32 | warm | 0.1342 | 0.1402 | +4.43% | 488.30 | 467.57 |
| bool | 32 | cold | 0.2358 | 0.1612 | -31.63% | 278.02 | 406.49 |
| bool | 128 | warm | 0.2439 | 0.2383 | -2.30% | 268.69 | 275.01 |
| bool | 128 | cold | 0.6644 | 0.3318 | -50.05% | 98.66 | 197.49 |

All cold cases improve significantly (p=0.002 except Boolean depth 8, p=0.004). All warm changes are statistically inconclusive (p=0.132–0.589). Host variability remains visible despite serialized benchmark execution; these are not cross-machine performance guarantees.

## Correctness coverage

The added tests cover:

- Deep int64 and Boolean chains, NULL/all-valid transitions, bitmap word boundaries, zero rows, growth and shrink reuse, compact/exported Boolean results, and borrowed-input immutability.
- Ten numeric physical types through repeated narrowing/widening chains.
- Shared DAG intermediates that are not outputs, duplicate input edges, early outputs, duplicate output roots, and output pinning.
- Three-valued Boolean diamonds.
- Conditional-region demand masks and a narrow cast that would fail if unselected rows were evaluated.
- Budget growth rejection, retry at the old size, retry after lifting the limit, cancellation, and release on close.
- Row-scaled workspace payload bounded by simultaneous live values rather than expression depth. This regression fails on the baseline and passes with reuse.

The full expr package and the new liveness tests with SIMD and race instrumentation pass. Baseline correctness tests pass; its new memory-bound assertion fails as expected. Exact output is in `expression-liveness-tests.txt`.

These expression benchmarks and tests do not use any Parquet fixture. A synthetic replacement used for separately run repository-wide Parquet validation does not affect these measurements.

## Reproduction

Use the same Go toolchain and dependencies for both checkouts. Copy the new liveness test/benchmark files to the baseline checkout without changing its implementation. Run each command from the corresponding repository root:

```sh
GOMAXPROCS=1 go test ./expr -run '^$' \
  -bench '^BenchmarkExpressionLiveness$' -benchtime=300ms \
  -count=2 -benchmem -cpu=1
```

Repeat three alternating baseline/changed pairs, append outputs by version, then compare:

```sh
go tool benchstat validation/expression-liveness-before.txt \
  validation/expression-liveness-after.txt
go test ./expr -count=1
GOEXPERIMENT=simd go test ./expr -run '^TestLiveness' -count=1
go test -race ./expr -run '^TestLiveness' -count=1
```

## Files

- `expr/liveness_test.go`: correctness and bounded-payload regressions.
- `expr/liveness_benchmark_test.go`: warm/cold benchmark fixtures.
- `validation/expression-liveness-before.txt`: retained baseline samples.
- `validation/expression-liveness-after.txt`: retained changed samples.
- `validation/expression-liveness-benchstat.txt`: statistical comparison.
- `validation/expression-liveness-tests.txt`: test results, including the expected baseline memory assertion failure.
