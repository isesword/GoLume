#!/usr/bin/env bash
set -euo pipefail

# Compare a frozen pre-change build with this checkout in native Auto mode.
# The baseline executables must be built from the stated baseline source.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
BASELINE="${1:?usage: bench_bitmap.sh baseline-binary-directory [output-prefix]}"
BASELINE="$(cd "$BASELINE" && pwd)"
PREFIX="${2:-validation/bitmap-$(date -u +%Y%m%dT%H%M%SZ)}"
TIME="${SIMD_BENCHTIME:-1s}"
COUNT="${SIMD_BENCHCOUNT:-6}"
for file in kernel.test physical.test; do
  if [[ ! -x "$BASELINE/$file" ]]; then printf 'Missing baseline executable: %s\n' "$BASELINE/$file" >&2; exit 1; fi
done
if ! [[ "$COUNT" =~ ^[1-9][0-9]*$ ]]; then printf 'Invalid SIMD_BENCHCOUNT\n' >&2; exit 1; fi
if (( COUNT % 2 != 0 )); then printf 'Warning: an odd count does not balance before/after orders\n' >&2; fi
mkdir -p "$(dirname "$PREFIX")"
if compgen -G "${PREFIX}-*" >/dev/null; then printf 'Refusing to overwrite %s-*\n' "$PREFIX" >&2; exit 1; fi
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
digest() { if command -v sha256sum >/dev/null; then sha256sum "$@"; else shasum -a 256 "$@"; fi; }
{
 go version; go env GOOS GOARCH GOAMD64; uname -sm
 printf 'baseline_commit=%s\nGODEBUG=%s\nGOMAXPROCS=%s\nbenchtime=%s\ncount=%s\nmode=auto\norder=before-after/after-before\n' "${SIMD_BASELINE_COMMIT:-unspecified}" "${GODEBUG:-}" "${GOMAXPROCS:-}" "$TIME" "$COUNT"
 digest scripts/bench_bitmap.sh
 (cd "$BASELINE"; digest kernel.test physical.test)
 go version -m "$BASELINE/kernel.test"
 go version -m "$BASELINE/physical.test"
} >"${PREFIX}-environment.txt"
GOEXPERIMENT=simd go test -c ./kernel -o "$TMP/kernel.test"
GOEXPERIMENT=simd go test -c ./physical -o "$TMP/physical.test"
{
 (cd "$TMP"; digest kernel.test physical.test)
 go version -m "$TMP/kernel.test"
 go version -m "$TMP/physical.test"
 for directory in "$BASELINE" "$TMP"; do
  "$directory/kernel.test" -test.run .
  "$directory/physical.test" -test.run '^Test(NumericSIMDWholeQuery|Compare32|BitmapFloat32)'
 done
} >>"${PREFIX}-environment.txt"
printf 'round\tversion\n' >"${PREFIX}-order.tsv"
for version in before after; do : >"${PREFIX}-$version-kernel.txt"; : >"${PREFIX}-$version-query.txt"; done
for ((round=0;round<COUNT;round++)); do
 order='before after'; if ((round % 2)); then order='after before'; fi
 for version in $order; do
  directory="$BASELINE"; if [[ "$version" == after ]]; then directory="$TMP"; fi
  printf '%d\t%s\n' "$((round+1))" "$version" >>"${PREFIX}-order.tsv"
  for pattern in '^BenchmarkNativeNumericLiteral$/.*/.*/.*/^auto$' \
                 '^BenchmarkNativeFloat64Comparison$/.*/.*/.*/^auto$' \
                 '^Benchmark(Float32Compare|Float32ColumnCompare)$/^auto$' \
                 '^BenchmarkColumn32ComparisonGate$/.*/.*/.*/^mode=auto$'; do
   "$directory/kernel.test" -test.run '^$' -test.bench "$pattern" -test.benchtime "$TIME" -test.benchmem -test.count 1 >>"${PREFIX}-$version-kernel.txt"
  done
  for pattern in '^BenchmarkNumericSIMDWholeQuery$/.*/.*/.*/.*/^mode=auto$' \
                 '^BenchmarkCompare32FilterProjectKernelMode$/.*/.*/.*/.*/^mode=auto$'; do
   "$directory/physical.test" -test.run '^$' -test.bench "$pattern" -test.benchtime "$TIME" -test.benchmem -test.count 1 >>"${PREFIX}-$version-query.txt"
  done
 done
done
# The frozen baseline matrix has 124 kernel and 64 complete-query Auto cases.
for version in before after; do
 for family in kernel query; do
  cases=124; if [[ "$family" == query ]]; then cases=64; fi
  actual="$(grep -c '^Benchmark.*ns/op' "${PREFIX}-$version-$family.txt" || true)"
  expected=$((COUNT * cases))
  if [[ "$actual" != "$expected" ]]; then printf '%s/%s cases: got %s want %s\n' "$version" "$family" "$actual" "$expected" >&2; exit 1; fi
 done
done
printf 'complete\n' >"${PREFIX}-complete.txt"
printf 'Bitmap evidence: %s-*\n' "$PREFIX"
