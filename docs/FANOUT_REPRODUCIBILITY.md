# Reproducing native shared fan-out measurements

The repository contains the actual measurement fixtures:

- pipeline/perf_fanout_test.go: BenchmarkRealOwnedFanOut (six drain cases).
- pipeline/perf_resources_test.go: TestRealFanOutResources, full rows/checksum,
  source batch metrics, slow branch and reservation/semaphore cleanup checks.
- pipeline/architecture_operator_harness_test.go: real Filter/Project/Drop
  correctness and budget checks, plus a separately named setup-inclusive benchmark.
- pipeline/architecture_operator_execution_bench_test.go:
  BenchmarkArchitecturePhysicalFanOutExecution (three prepared-plan cases).

Do not upload benchmark logs, CPU profiles, binaries, private absolute paths or
local result JSON with this source. Keep those artifacts outside the repository.
Use the same Go 1.27.3 toolchain and build mode for both revisions.

## Commands

From each checked-out repository root:

```sh
GOGC=50 go test ./pipeline -run '^$' -cpu=1 -benchmem -benchtime=1s -count=1 \
  -bench '^(BenchmarkRealOwnedFanOut|BenchmarkArchitecturePhysicalFanOutExecution)$'
go test ./pipeline -run '^(TestRealFanOutResources|TestArchitecturePhysicalFanOut)$' -count=3
go test -race ./pipeline -run '^(TestRealFanOutResources|TestArchitecturePhysicalFanOut)$' -count=1
```

Repeat the benchmark command ten times for both revisions, alternating which
revision runs first each round. Do not run competing benchmarks concurrently.
Compare all samples with benchstat, including B/op, allocs/op, items/branch and
peak-budget-B; do not choose the best run. Benchmark CPU selection is controlled
by `-cpu`, not overridden by the fixtures. Resource tests are nonparallel and
set GOMAXPROCS=1 only for their duration, restoring its previous value afterward.

Both revisions use two branches, 65,536 total rows, one pipeline worker, a 64 KiB
source payload soft target, 64 KiB exchange target, 256 KiB hard morsel maximum,
1 MiB exchange in-flight cap and 256 minimum morsel rows. The large BatchRows
setting is a ceiling, not a promise that the source delivers 65,536-row batches.
The drain source actually emits 8/max8192 Scalar, 13/max5461 String and
10/max7280 StructList batches. Small cases cap source batches at 256 rows.
Prepared operator benchmarks keep fixture construction and plan compilation out
of timing, but include execution, oracle checks and cleanup. Drain timing checks
rows; the separate resource tests check payload checksums/order and cleanup.

For the published baseline b584c1e8957af825969be849dad831a849999e78, install
these same test-only sources in a separate checkout or Go overlay. Do not alter
budgets, source options or production code to make the baseline pass. Its old
accounting is incomplete, so a lower reported budget peak is not a smaller true
allocation bound. The baseline can fail the physical 512 KiB regression; compare
benchmark throughput and the high-budget correctness subtests fairly instead:

```sh
go test ./pipeline -run '^TestArchitecturePhysicalFanOut/[^/]+/query8388608$' -count=1
```

## Resource acceptance

Drain resource tests retain 256 KiB as diagnostic: either success or an explicit
budget failure is allowed, but results and all cleanup invariants are checked.
At 512 KiB, 768 KiB and 1 MiB, every Scalar/String/StructList fast and slow drain
case must succeed. Physical operator resource tests require success at both
512 KiB and 8 MiB; a budget error is not a passing diagnostic at those levels.
Cancellation accompanying another branch's budget failure is distinguished from
an unexplained cancellation. Tests check engine/query balances and bound peaks;
drain tests additionally reacquire full group/global credit after closing.

## Observed local comparison

Ten interleaved one-second samples, linux/amd64 AMD EPYC 9V74, Go 1.27.3,
GOGC=50, one CPU, published baseline versus phase1 guard-shaped sharing:

| Workload | Baseline median | Shared median | benchstat comparison |
| --- | ---: | ---: | --- |
| Drain Scalar, 256 rows | 1.828 ms | 1.628 ms | -10.93%, p<0.001 |
| Drain Scalar, large ceiling | 253.0 us | 256.0 us | NS, p=0.247 |
| Drain String, 256 rows | 2.180 ms | 1.907 ms | -12.53%, p<0.001 |
| Drain String, large ceiling | 716.0 us | 541.9 us | -24.32%, p<0.001 |
| Drain StructList, 256 rows | 14.249 ms | 8.771 ms | -38.45%, p<0.001 |
| Drain StructList, large ceiling | 11.761 ms | 6.730 ms | -42.78%, p<0.001 |
| Physical Scalar | 1.500 ms | 1.574 ms | NS, p=0.075 |
| Physical String | 2.242 ms | 2.183 ms | NS, p=0.315 |
| Physical StructList | 13.053 ms | 9.069 ms | -30.52%, p<0.001 |

NS means no statistically significant difference in this sample, not proof of
identical performance or compliance with every regression threshold. Physical
Scalar's median is higher and remains visible rather than discarded. Shared
large drain item counts are 16/25/46 versus baseline 8/13/10; small counts remain
256. Increased allocation counts are a real cost even where allocated bytes or
time improve. Guard ranges and stricter admission are not relaxed for this test.
Query peaks and per-branch pinned credit peaks are accounting measures, not RSS;
per-branch peaks cannot be summed as unique simultaneous retained storage.

Native ARM, hosted CI and the final full-repository gate outcome must be reported
separately. Missing original parquet fixture tests must be identified as excluded,
not replaced by synthetic data or described as passing. This comparison does not
include the rejected whole-batch or sparse-caller-map designs.
