#!/usr/bin/env bash
set -euo pipefail

E2E_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$E2E_ROOT"

E2E_ARCH="$(go env GOARCH)"
E2E_RAW="${1:-validation/simd-e2e-${E2E_ARCH}.txt}"
E2E_CSV="${2:-validation/simd-e2e-${E2E_ARCH}.csv}"

GOEXPERIMENT=simd go test ./engine \
	-run '^$' -bench '^BenchmarkPreparedRadixKernelMode$' \
	-benchmem -count=10 >"$E2E_RAW"

GOEXPERIMENT=simd go test ./lazy \
	-run '^$' -bench '^(BenchmarkPreparedLazyParquetKernelMode|BenchmarkPreparedRadixJoinKernelMode)$' \
	-benchmem -count=10 >>"$E2E_RAW"

go tool benchstat -alpha 0.05 -col '/mode' -format csv "$E2E_RAW" >"$E2E_CSV"
printf 'raw samples: %s\nbenchstat csv: %s\n' "$E2E_RAW" "$E2E_CSV"
