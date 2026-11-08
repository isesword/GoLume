#!/usr/bin/env bash
set -euo pipefail

# Full comparison matrix by default. Overrides are exploratory subsets, not
# evidence that the complete matrix passed. Auto now prefers native SIMD.
# All six mode orders are balanced when count is a multiple of six.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PREFIX="${1:-validation/column32-$(go env GOOS)-$(go env GOARCH)-$(date -u +%Y%m%dT%H%M%SZ)}"
TIME="${SIMD_BENCHTIME:-1s}"
COUNT="${SIMD_BENCHCOUNT:-12}"
KERNEL="${COLUMN32_KERNEL_PATTERN:-^BenchmarkColumn32ComparisonGate$}"
QUERY="${COLUMN32_QUERY_PATTERN:-^BenchmarkCompare32FilterProjectKernelMode$}"
if ! [[ "$COUNT" =~ ^[1-9][0-9]*$ ]]; then
  printf 'SIMD_BENCHCOUNT must be a positive integer\n' >&2
  exit 1
fi
if (( COUNT % 6 != 0 )); then
  printf 'Warning: count=%s does not complete a six-order balance cycle\n' "$COUNT" >&2
fi
mkdir -p "$(dirname "$PREFIX")"
if compgen -G "${PREFIX}-*" >/dev/null; then
  printf 'Refusing to overwrite existing evidence prefix: %s\n' "$PREFIX" >&2
  exit 1
fi
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
HASH=sha256sum
if ! command -v "$HASH" >/dev/null; then
  HASH=shasum
fi
digest() {
  if [[ "$HASH" == shasum ]]; then shasum -a 256 "$@"; else sha256sum "$@"; fi
}
find . -type f \( -name '*.go' -o -name go.mod -o -name go.sum \) -print0 |
  LC_ALL=C sort -z >"$TMP/source-files"

{
  go version
  go env GOOS GOARCH GOAMD64
  printf 'GODEBUG=%s\nGOMAXPROCS=%s\n' "${GODEBUG:-}" "${GOMAXPROCS:-}"
  printf 'benchtime=%s\ncount=%s\nkernel=%s\nquery=%s\n' "$TIME" "$COUNT" "$KERNEL" "$QUERY"
  printf 'mode_order=all-six-permutations\n'
  printf 'build_GOEXPERIMENT=simd\n'
  digest scripts/bench_column32.sh
  uname -a
  if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git rev-parse HEAD
    git status --short
  else
    printf 'Source archive: no Git metadata\n'
  fi
  # Record the build inputs even when running a connector-restored archive.
  while IFS= read -r -d '' file; do digest "$file"; done <"$TMP/source-files"
} >"${PREFIX}-environment.txt"

# Freeze both test executables before timing so every mode uses the same build.
GOEXPERIMENT=simd go test -c ./kernel -o "$TMP/kernel.test"
GOEXPERIMENT=simd go test -c ./physical -o "$TMP/physical.test"
{
  (cd "$TMP"; digest kernel.test physical.test)
  go version -m "$TMP/kernel.test"
  go version -m "$TMP/physical.test"
} >>"${PREFIX}-environment.txt"
"$TMP/kernel.test" -test.run '^TestColumn32' -test.v >>"${PREFIX}-environment.txt"
"$TMP/physical.test" -test.run '^TestCompare32' >>"${PREFIX}-environment.txt"
# Each sample is a fresh process from the same frozen binary. -test.skip filters
# the other two modes without rewriting caller-provided benchmark patterns.
orders=("scalar auto simd" "auto simd scalar" "simd scalar auto"
        "simd auto scalar" "auto scalar simd" "scalar simd auto")
printf 'family\tround\tmode\n' >"${PREFIX}-order.tsv"
run_balanced() {
  local family="$1" binary="$2" pattern="$3" skip_prefix="$4"
  local round mode others
  : >"${PREFIX}-$family.txt"
  for ((round = 0; round < COUNT; round++)); do
    for mode in ${orders[round % 6]}; do
      case "$mode" in
        scalar) others='auto|simd' ;;
        auto) others='scalar|simd' ;;
        simd) others='scalar|auto' ;;
      esac
      printf '%s\t%d\t%s\n' "$family" "$((round + 1))" "$mode" >>"${PREFIX}-order.tsv"
      "$binary" -test.run '^$' -test.bench "$pattern" \
        -test.skip "${skip_prefix}mode=($others)$" \
        -test.benchtime "$TIME" -test.benchmem -test.count 1 >>"${PREFIX}-$family.txt"
    done
  done
}
run_balanced kernel "$TMP/kernel.test" "$KERNEL" '^BenchmarkColumn32ComparisonGate$/type=/case=/op=/'
run_balanced query "$TMP/physical.test" "$QUERY" '^BenchmarkCompare32FilterProjectKernelMode$/type=/op=/pct=/nullable=/'
for result in "${PREFIX}-kernel.txt" "${PREFIX}-query.txt"; do
  if ! grep -q '^Benchmark.*ns/op' "$result"; then
    printf 'No measured benchmark rows in %s\n' "$result" >&2
    exit 1
  fi
done

# backend is a diagnostic label, not a row dimension: leaving it in .fullname
# would split scalar and candidate into different rows and hide comparisons.
go tool benchstat -alpha 0.05 -row '.name,/type,/case,/op' -col '/mode' \
  -ignore '/backend' "${PREFIX}-kernel.txt" >"${PREFIX}-kernel-summary.txt"
go tool benchstat -alpha 0.05 -row '.name,/type,/case,/op' -col '/mode' \
  -ignore '/backend' -format csv "${PREFIX}-kernel.txt" >"${PREFIX}-kernel.csv"
go tool benchstat -alpha 0.05 -col '/mode' \
  "${PREFIX}-query.txt" >"${PREFIX}-query-summary.txt"
go tool benchstat -alpha 0.05 -col '/mode' -format csv \
  "${PREFIX}-query.txt" >"${PREFIX}-query.csv"
printf 'Column32 evidence: %s-*.{txt,csv}\nThis script measures performance; it does not change dispatch policy.\n' "$PREFIX"
printf 'complete\n' >"${PREFIX}-complete.txt"
