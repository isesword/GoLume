#!/usr/bin/env bash
set -euo pipefail

STRUCTURE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$STRUCTURE_ROOT"

fail_if_found() {
	local pattern="$1"
	local message="$2"
	shift 2
	local matches
	matches="$(rg -n "$pattern" "$@" || true)"
	if [[ -n "$matches" ]]; then
		printf '%s\n%s\n' "$message" "$matches" >&2
		exit 1
	fi
}

production_go_files() {
	rg --files -g '*.go' -g '!**/*_test.go' -g '!vendor/**'
}

go run ./internal/cmd/gensimd -check

unexpected_new="$(production_go_files | grep -v '^engine/engine.go$' | grep -v '^cmd/kernel-info/main.go$' | xargs rg -n 'kernel\.New\(' || true)"
if [[ -n "$unexpected_new" ]]; then
	printf 'kernel.New is restricted to Engine initialization and cmd/kernel-info:\n%s\n' "$unexpected_new" >&2
	exit 1
fi

fail_if_found '\.\.\.kernel\.Mode|WithKernelMode|syncSummary' \
	'forbidden kernel compatibility/reconstruction API found:' \
	$(production_go_files)

fail_if_found 'Groups[[:space:]]+\[\]Group|AggCount[[:space:]]*=|SumCountVector|NewSumCountVector|SumCountBytesForSlots|ErrStringTableFull|func NewTopKSource\(' \
	'forbidden compatibility API found:' \
	$(production_go_files)

fail_if_found 'type Group struct' \
	'legacy engine.Group API found:' \
	engine

fail_if_found 'type Columns struct|func Open\(path string, cols Columns\)' \
	'legacy Parquet API found:' \
	source/parquet

fail_if_found '^[[:space:]]*Column[[:space:]]+string' \
	'legacy sort-column API found:' \
	logical/plan.go lazy/frame.go

fail_if_found 'type .*CompareFunc func\(.*CompareOp|type .*ArithmeticFunc func\(.*ArithmeticOp' \
	'operation-specialized dispatch must not carry runtime operation parameters:' \
	kernel

fail_if_found '^func compare(I64|U64|F64)\(' \
	'generic runtime-operation comparison helper found in kernel:' \
	kernel

fail_if_found 'blockComparator|compare = func\(base, end int, out memory\.Bitmap\)' \
	'DirectFused must not rebuild a generic comparison closure:' \
	engine

fail_if_found 'simdCompare64|mask64Bits|mask32Bits|recordOverflowLanes|mul64x2|mul64x4|prepareHash128Input|installArchSIMDScale|arm64LongRuneCount|simdArmStringUTF8' \
	'forbidden legacy or widening SIMD hot path found:' \
	kernel

fail_if_found 'utf8\.Valid\(' \
	'UTF-8 length must not perform a validation pre-pass:' \
	kernel

fail_if_found 'installArchSIMDString' \
	'deleted UTF-8 SIMD registration hook found:' \
	kernel

printf 'SIMD structure checks passed\n'
