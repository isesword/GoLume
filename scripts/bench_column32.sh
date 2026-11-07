#!/usr/bin/env bash
set -euo pipefail

# Full admission matrix by default. Overrides are exploratory subsets, not
# evidence that the complete matrix passed. Auto is a fallback control only.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PREFIX="${1:-validation/column32-$(go env GOOS)-$(go env GOARCH)-$(date -u +%Y%m%dT%H%M%SZ)}"
TIME="${SIMD_BENCHTIME:-1s}"
COUNT="${SIMD_BENCHCOUNT:-10}"
KERNEL="${COLUMN32_KERNEL_PATTERN:-^BenchmarkColumn32ComparisonGate$}"
QUERY="${COLUMN32_QUERY_PATTERN:-^BenchmarkCompare32FilterProjectKernelMode$}"
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
"$TMP/kernel.test" -test.run '^$' -test.bench "$KERNEL" \
  -test.benchtime "$TIME" -test.benchmem -test.count "$COUNT" >"${PREFIX}-kernel.txt"
"$TMP/physical.test" -test.run '^$' -test.bench "$QUERY" \
  -test.benchtime "$TIME" -test.benchmem -test.count "$COUNT" >"${PREFIX}-query.txt"
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
printf 'Column32 evidence: %s-*.{txt,csv}\nNo Auto admission is performed by this script.\n' "$PREFIX"
printf 'complete\n' >"${PREFIX}-complete.txt"
