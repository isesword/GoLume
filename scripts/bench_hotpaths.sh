#!/usr/bin/env bash
set -euo pipefail

HOTPATH_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$HOTPATH_ROOT"

HOTPATH_ARCH="$(go env GOARCH)"
HOTPATH_TIME="${SIMD_BENCHTIME:-1s}"
HOTPATH_COUNT="${SIMD_BENCHCOUNT:-10}"
HOTPATH_PREFIX="${1:-validation/simd-refactor-${HOTPATH_ARCH}}"
CANDIDATES='^BenchmarkKernelID/(compare-(int32-(gt|ge|lt|le)|uint32-(gt|ge|lt|le|eq))|arithmetic-float64-(sub|mul|div))$'

GOEXPERIMENT=simd go test ./kernel -run '^$' -bench "$CANDIDATES" \
	-benchtime "$HOTPATH_TIME" -benchmem -count "$HOTPATH_COUNT" >"${HOTPATH_PREFIX}-kernel.txt"
go tool benchstat -alpha 0.05 -col '/mode' -format csv \
	"${HOTPATH_PREFIX}-kernel.txt" >"${HOTPATH_PREFIX}-kernel.csv"

GOEXPERIMENT=simd go test ./kernel -run '^$' -bench '^(BenchmarkHashBackend|BenchmarkUTF8Profiles)$' \
	-benchtime "$HOTPATH_TIME" -benchmem -count "$HOTPATH_COUNT" >"${HOTPATH_PREFIX}-hash-utf8.txt"
go tool benchstat -alpha 0.05 -col '/backend' -format csv \
	"${HOTPATH_PREFIX}-hash-utf8.txt" >"${HOTPATH_PREFIX}-hash.csv"
go tool benchstat -alpha 0.05 -col '/mode' -format csv \
	"${HOTPATH_PREFIX}-hash-utf8.txt" >"${HOTPATH_PREFIX}-utf8.csv"

GOEXPERIMENT=simd go test ./expr -run '^$' \
	-bench '^(BenchmarkFloat64ExpressionKernelMode|BenchmarkUTF8LengthExpressionKernelMode)$' \
	-benchtime "$HOTPATH_TIME" -benchmem -count "$HOTPATH_COUNT" >"${HOTPATH_PREFIX}-expression.txt"
go tool benchstat -alpha 0.05 -col '/mode' -format csv \
	"${HOTPATH_PREFIX}-expression.txt" >"${HOTPATH_PREFIX}-expression.csv"

GOEXPERIMENT=simd go test ./engine -run '^$' -bench '^BenchmarkInt32ComparisonKernelMode$' \
	-benchtime "$HOTPATH_TIME" -benchmem -count "$HOTPATH_COUNT" >"${HOTPATH_PREFIX}-comparison.txt"
go tool benchstat -alpha 0.05 -col '/mode' -format csv \
	"${HOTPATH_PREFIX}-comparison.txt" >"${HOTPATH_PREFIX}-comparison.csv"

awk '/^BenchmarkInt32ComparisonKernelMode\/direct-fused\/.*\/mode=simd-/ {
	gsub("/direct-fused/", "/strategy=direct/"); gsub("/mode=simd", ""); print
}' "${HOTPATH_PREFIX}-comparison.txt" >"${HOTPATH_PREFIX}-strategy.txt"
awk '/^BenchmarkInt32ComparisonKernelMode\/bitmap-direct\/.*\/mode=simd-/ {
	gsub("/bitmap-direct/", "/strategy=bitmap/"); gsub("/mode=simd", ""); print
}' "${HOTPATH_PREFIX}-comparison.txt" >>"${HOTPATH_PREFIX}-strategy.txt"
go tool benchstat -alpha 0.05 -col '/strategy' -format csv \
	"${HOTPATH_PREFIX}-strategy.txt" >"${HOTPATH_PREFIX}-strategy.csv"

printf 'hot-path gate artifacts: %s-*.{txt,csv}\n' "$HOTPATH_PREFIX"
