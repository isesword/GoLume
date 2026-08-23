# Phase 2.5 final validation summary

- scalar full test: PASS
- GOEXPERIMENT=simd full test: PASS
- simd=0/+128/+256/+512 targeted tests: PASS
- race (lazy/physical/optimizer/engine/join/radix/scheduler/expr/memory): PASS
- go vet: PASS
- GoDoc convention checker: PASS
- 1M-row Parquet regression: 219196 groups / 472475 selected / checksum 236716301
- Join -> computed projection -> Filter -> Aggregate regression: PASS
- PreparedFrame repeat/concurrent/Close/memory lifecycle: PASS
- generic source physical projection fallback: PASS

Join frame benchmark (shared linux/amd64 host):
- one-shot median ~= 12.92 ms/op, ~=407 allocs/op, ~=15.31 MB/op
- PreparedFrame median ~= 8.56 ms/op, 113 allocs/op, ~=10.64 MB/op
- latency ratio ~= 1.51x in this fixture
