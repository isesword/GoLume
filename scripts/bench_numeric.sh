#!/usr/bin/env bash
set -euo pipefail

# Run all new numeric kernel/query workloads using frozen executables and all
# six mode orders. This measures performance; it does not change dispatch policy.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PREFIX="${1:-validation/numeric-$(date -u +%Y%m%dT%H%M%SZ)}"
TIME="${SIMD_BENCHTIME:-1s}"
COUNT="${SIMD_BENCHCOUNT:-6}"
if ! [[ "$COUNT" =~ ^[1-9][0-9]*$ ]]; then
  printf 'SIMD_BENCHCOUNT must be a positive integer\n' >&2
  exit 1
fi
if (( COUNT % 6 != 0 )); then
  printf 'Warning: count=%s does not complete all six mode orders\n' "$COUNT" >&2
fi
mkdir -p "$(dirname "$PREFIX")"
if compgen -G "${PREFIX}-*" >/dev/null; then
  printf 'Refusing to overwrite evidence prefix: %s\n' "$PREFIX" >&2
  exit 1
fi
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
digest() {
  if command -v sha256sum >/dev/null; then sha256sum "$@"; else shasum -a 256 "$@"; fi
}
{
  go version
  go env GOOS GOARCH GOAMD64
  uname -sm
  printf 'GODEBUG=%s\nGOMAXPROCS=%s\nbenchtime=%s\ncount=%s\n' "${GODEBUG:-}" "${GOMAXPROCS:-}" "$TIME" "$COUNT"
  printf 'build_GOEXPERIMENT=simd\nmode_order=all-six-permutations\n'
  digest scripts/bench_numeric.sh
  while IFS= read -r file; do digest "$file"; done < <(find kernel physical memory internal/cmd/gensimd -name '*.go' | LC_ALL=C sort)
} >"${PREFIX}-environment.txt"
GOEXPERIMENT=simd go test -c ./kernel -o "$TMP/kernel.test"
GOEXPERIMENT=simd go test -c ./physical -o "$TMP/physical.test"
{
  (cd "$TMP"; digest kernel.test physical.test)
  go version -m "$TMP/kernel.test"
  go version -m "$TMP/physical.test"
  "$TMP/kernel.test" -test.run 'Test(Native|ExpandedNumeric|Float32Native|Float64Native)' -test.v
  "$TMP/physical.test" -test.run '^TestNumericSIMDWholeQuery' -test.v
} >>"${PREFIX}-environment.txt"
orders=("scalar auto forced-simd" "auto forced-simd scalar" "forced-simd scalar auto"
        "forced-simd auto scalar" "auto scalar forced-simd" "scalar forced-simd auto")
printf 'family\tround\tmode\n' >"${PREFIX}-order.tsv"
: >"${PREFIX}-kernel.txt"
: >"${PREFIX}-query.txt"
for ((round=0;round<COUNT;round++)); do
  for mode in ${orders[round % 6]}; do
    printf 'kernel\t%d\t%s\n' "$((round+1))" "$mode" >>"${PREFIX}-order.tsv"
    for pattern in "^BenchmarkNativeFloatArithmetic$/.*/.*/.*/.*/^${mode}$" \
                   "^BenchmarkNativeNumericLiteral$/.*/.*/.*/^${mode}$" \
                   "^BenchmarkNativeFloat64Comparison$/.*/.*/.*/^${mode}$"; do
      "$TMP/kernel.test" -test.run '^$' -test.bench "$pattern" -test.benchtime "$TIME" \
        -test.benchmem -test.count 1 >>"${PREFIX}-kernel.txt"
    done
    printf 'query\t%d\t%s\n' "$((round+1))" "$mode" >>"${PREFIX}-order.tsv"
    "$TMP/physical.test" -test.run '^$' \
      -test.bench "^BenchmarkNumericSIMDWholeQuery$/.*/.*/.*/.*/^mode=${mode}$" \
      -test.benchtime "$TIME" -test.benchmem -test.count 1 >>"${PREFIX}-query.txt"
  done
done
# Full workload counts: 216 kernel and 72 query cases per three-mode round.
for family in kernel query; do
  expected=$((COUNT * 216))
  if [[ "$family" == query ]]; then expected=$((COUNT * 72)); fi
  actual="$(grep -c '^Benchmark.*ns/op' "${PREFIX}-${family}.txt" || true)"
  if [[ "$actual" != "$expected" ]]; then
    printf '%s count mismatch: got %s want %s\n' "$family" "$actual" "$expected" >&2
    exit 1
  fi
done
printf 'complete\n' >"${PREFIX}-complete.txt"
printf 'Numeric evidence: %s-*\n' "$PREFIX"
