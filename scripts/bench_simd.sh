#!/usr/bin/env bash
set -euo pipefail

BENCH_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$BENCH_ROOT"

BENCH_ARCH="$(go env GOARCH)"
BENCH_RAW="${1:-validation/simd-kernelid-${BENCH_ARCH}.txt}"
BENCH_CSV="${2:-validation/simd-kernelid-${BENCH_ARCH}.csv}"
BENCH_PATTERN="${3:-^BenchmarkKernelID$}"
BENCH_TIME="${SIMD_BENCHTIME:-1s}"

GOEXPERIMENT=simd go test ./kernel \
	-run '^$' \
	-bench "$BENCH_PATTERN" \
	-benchtime "$BENCH_TIME" -benchmem -count=10 >"$BENCH_RAW"

go tool benchstat -alpha 0.05 -col '/mode' -format csv "$BENCH_RAW" >"$BENCH_CSV"
printf 'raw samples: %s\nbenchstat csv: %s\n' "$BENCH_RAW" "$BENCH_CSV"
