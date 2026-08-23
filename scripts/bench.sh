#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

GOEXPERIMENT=simd go test ./engine -run '^$' \
  -bench 'Benchmark(DirectFused|BitmapDirect|RadixW4|PreparedRadixW4)$' \
  -benchmem -benchtime="${BENCHTIME:-1s}" -count="${COUNT:-5}"

GOEXPERIMENT=simd go test ./lazy -run '^$' \
  -bench 'BenchmarkPrepared(LazyParquet|ExecutionLayerComparison)' \
  -benchmem -benchtime="${BENCHTIME:-1s}" -count="${COUNT:-5}"

go test ./physical -run '^$' \
  -bench 'Benchmark(GlobalSort262K|TopK100Of262K)$' \
  -benchmem -benchtime="${ORDER_BENCHTIME:-3x}" -count="${ORDER_COUNT:-3}" -cpu=1
