# Multi-column int64 benchmark evidence

Host: Linux/amd64, AMD EPYC 9V74 virtualized/shared host, Go 1.27.3.
Fixture: 65,536 rows; global, 64 groups, or 4,096 groups; SUM over 1, 2 or 4
input columns plus COUNT(*). Prepared execution, no materialized output,
prepared-state caching disabled, scalar build, one worker, GOMAXPROCS=1.

`matrix-amd64.txt`: five samples of 100 iterations per matrix cell. These are
absolute costs, not speedup comparisons: adding lanes changes the work. Median
milliseconds for two/four lanes were 3.55/5.49 global, 6.07/8.23 at 64 groups,
and 7.23/9.60 at 4,096 groups. High-cardinality generic paths allocate more
because the index and every state lane grow from a conservative initial size.
The generic path is a scalar correctness baseline, not a claim to outperform
the existing specialized single-column kernel.

`legacy-{before,after}-amd64.txt`: ten samples of 100 iterations for the
one-column cells, sequential runs pinned to CPU 8, GOMAXPROCS=1. Before is the
frozen global-aggregate increment; after includes the compact-key optimization
that removes aggregate-state copies from the sorted per-row key path.
`legacy-benchstat.txt` contains the raw comparison.

The shared host was noisy (some timing intervals exceed 40%). The final global
comparison did not detect a significant timing difference (p=0.436); grouped
results favored the after build but are not claimed as speedups. Legacy
allocation counts were unchanged (109 global, 127 low-cardinality, 128
high-cardinality). Global execution added about 129 bytes of fixed metadata per
query, under 0.5%; grouped bytes were unchanged within noise. Initial short
samples exposed a possible global slowdown; compact keys corrected the large
per-row copy before these final measurements.

No new SIMD aggregation dispatch is enabled by this increment. Predicate
kernel dispatch and the existing single-column aggregation kernels remain
unchanged. These measurements do not certify arm64 or universal performance.

Reproduction:

    GOMAXPROCS=1 go test ./engine -run '^$' -bench BenchmarkMultiColumnAggregate -benchmem -count=5 -benchtime=100x
    GOMAXPROCS=1 taskset -c 8 go test ./engine -run '^$' -bench 'BenchmarkMultiColumnAggregate/groups=.*/lanes=1$' -benchmem -count=10 -benchtime=100x
