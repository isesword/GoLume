# Native aggregate baseline

Linux/amd64 shared AMD EPYC 9V74 host, Go 1.27.3, scalar build, GOMAXPROCS=1.
32,768 rows; global, 64 groups or 4,096 groups; two lanes Uint64/Float64 or
four lanes adding Int32/Float32. SUM per lane plus COUNT(*), one worker,
prepared-state caching disabled, summary-only ExecuteTyped. Three samples
of 20 iterations. Raw ns/op, bytes/op and allocations/op are in matrix-amd64.txt.

This is an absolute scalar baseline, not a speedup comparison. Input types and
work differ from the int64 benchmark. High-cardinality growth starts from a
conservative capacity and accounts for all replacement overlap. No new SIMD
aggregation dispatch is enabled; semantic tests also run in SIMD builds to
exercise existing predicate/scan dispatch without claiming vectorized new states.
No arm64 performance or universal speed guarantee is established by these samples.

    GOMAXPROCS=1 go test ./engine -run '^$' -bench BenchmarkNativeAggregate -benchmem -count=3 -benchtime=20x
