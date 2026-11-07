# Global aggregate validation

Validated on 2026-11-07, linux/amd64, Go 1.27.3, AMD EPYC 9V74 shared host.
This increment adds zero-key aggregation; it does not complete typed multi-column
aggregation, parallel global reduction, or new SIMD reduction kernels.

An isolated source snapshot containing only the global increment over
895c2bc8df5761e04b4e30472972f0ccd7577af8 passed:

- `go test ./...`
- `GOEXPERIMENT=simd go test ./...`
- `go test -race ./engine ./lazy ./logical`
- `go vet ./...`
- `go run ./tools/checkgodoc .`

Independent review found no blocking issues and added composition tests for
count-only, limited/empty input, nested aggregates, and post-aggregate filters.
Those tests additionally passed in scalar and SIMD race runs.

The benchmark command was:

```
GOMAXPROCS=1 go test ./engine -run '^$' -bench '^BenchmarkGlobalAggregate$' -benchmem -benchtime=1s -count=3
```

It excludes input construction and preparation, includes a complete execution
and one-row materialization, and checks SUM and COUNT each iteration. Median
results: 1,024 rows 56.5 us / 12,624 B / 116 allocs; 65,536 rows 2.15 ms /
28,762 B / 116 allocs; 1,048,576 rows 33.8 ms / 29,737 B / 131 allocs.
Bytes/op are cumulative allocations, not a peak-residency measurement.
These are initial throughput/allocation observations, not an old-vs-new speedup
or an architecture SIMD gate. No Auto SIMD dispatch was changed.
