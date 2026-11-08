#!/usr/bin/env bash
set -euo pipefail

# Three explicit phases keep the baseline executable frozen before candidate
# measurement, and allow correctness/build work without starting CPU timings.
# Usage: GLOBAL_BINDING_BASELINE=/absolute/frozen/src \
#        GLOBAL_BINDING_BIN_DIR=/absolute/binaries \
#        scripts/bench_global_binding.sh baseline|candidate|measure [evidence-dir]
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ACTION="${1:?expected baseline, candidate, or measure}"
OUT="${2:-$ROOT/validation/global-binding}"
BASELINE="${GLOBAL_BINDING_BASELINE:?set frozen baseline source directory}"
BIN="${GLOBAL_BINDING_BIN_DIR:?set directory for frozen test executables}"
TIME="${GLOBAL_BINDING_BENCHTIME:-150ms}"
COUNT="${GLOBAL_BINDING_BENCHCOUNT:-6}"
mkdir -p "$OUT" "$BIN"
OUT="$(cd "$OUT" && pwd)"; BIN="$(cd "$BIN" && pwd)"; BASELINE="$(cd "$BASELINE" && pwd)"
export GOMAXPROCS=1
SOURCE="$ROOT/engine/global_binding_benchmark_test.go"
FROZEN="$OUT/harness.go.txt"
if [[ "$ACTION" == baseline ]]; then
  [[ ! -e "$BIN/before.engine.test" && ! -e "$FROZEN" ]] || { echo 'Baseline evidence already exists; refusing to overwrite' >&2; exit 1; }
  cp "$SOURCE" "$FROZEN"
fi
[[ -f "$FROZEN" ]] || { echo 'Run baseline phase first' >&2; exit 1; }
cmp "$SOURCE" "$FROZEN" || { echo 'Harness differs from frozen baseline source' >&2; exit 1; }

freeze() {
  local label="$1" tree="$2"
  [[ ! -e "$BIN/$label.engine.test" ]] || { echo "Refusing to overwrite $label executable" >&2; exit 1; }
  python3 - "$tree" "$FROZEN" "$OUT/$label-overlay.json" <<'PY'
import json,sys
root,source,out=sys.argv[1:]
with open(out,'w') as f: json.dump({'Replace':{root+'/engine/global_binding_benchmark_test.go':source}},f,indent=2)
PY
  (
    cd "$tree"
    GOEXPERIMENT=simd go test -overlay "$OUT/$label-overlay.json" -c ./engine -o "$BIN/$label.engine.test"
    {
      printf 'source_tree=%s\nGOEXPERIMENT=simd\nGOMAXPROCS=1\n' "$tree"
      go version; go env GOOS GOARCH GOAMD64; uname -sm
      sha256sum "$FROZEN" "$BIN/$label.engine.test" "$ROOT/scripts/bench_global_binding.sh"
      go version -m "$BIN/$label.engine.test"
      if [[ -f "$BASELINE/../source-validation.json" ]]; then cat "$BASELINE/../source-validation.json"; fi
    } >"$OUT/$label-provenance.txt"
    find aggregate engine kernel memory pipeline source -name '*.go' -type f | LC_ALL=C sort | xargs sha256sum >"$OUT/$label-sources.sha256"
  )
  "$BIN/$label.engine.test" -test.run '^TestGlobalBindingWholeQueryOracle$' -test.v >"$OUT/$label-correctness.txt"
}

case "$ACTION" in
 baseline) freeze before "$BASELINE" ;;
 candidate) freeze after "$ROOT" ;;
 measure)
  [[ "$COUNT" =~ ^[1-9][0-9]*$ ]] || { echo 'Benchmark count must be positive' >&2; exit 1; }
  [[ -x "$BIN/before.engine.test" && -x "$BIN/after.engine.test" ]] || { echo 'Freeze both executables first' >&2; exit 1; }
  [[ ! -e "$OUT/paired-order.tsv" ]] || { echo 'Paired evidence already exists; refusing overwrite' >&2; exit 1; }
  {
    date -u; go version; uname -a
    printf 'GOMAXPROCS=1\nGOEXPERIMENT=simd\nGODEBUG=%s\nbenchtime=%s\nsamples=%s\n' "${GODEBUG:-}" "$TIME" "$COUNT"
    sha256sum "$BIN/before.engine.test" "$BIN/after.engine.test" "$FROZEN"
    if [[ -f /proc/cpuinfo ]]; then grep -m1 'model name' /proc/cpuinfo || true; fi
  } >"$OUT/paired-environment.txt"
  printf 'round\torder\tvariant\n' >"$OUT/paired-order.tsv"
  for ((round=1;round<=COUNT;round++)); do
    order='before after'; if (( round % 2 == 0 )); then order='after before'; fi
    position=0
    for variant in $order; do
      position=$((position+1))
      printf '%s\t%s\t%s\n' "$round" "$position" "$variant" >>"$OUT/paired-order.tsv"
      "$BIN/$variant.engine.test" -test.run '^$' -test.bench '^Benchmark(GlobalBindingWholeQuery|NativeAggregate)$' \
        -test.benchtime "$TIME" -test.benchmem -test.count 1 >"$OUT/$variant-round-$round.txt"
    done
  done
  for variant in before after; do
    actual="$(grep -h '^BenchmarkGlobalBindingWholeQuery.*ns/op' "$OUT/$variant"-round-*.txt | wc -l)"
    [[ "$actual" -eq "$((COUNT*36))" ]] || { echo "Wrong whole-query sample count: $variant $actual" >&2; exit 1; }
    actual="$(grep -h '^BenchmarkNativeAggregate.*ns/op' "$OUT/$variant"-round-*.txt | wc -l)"
    [[ "$actual" -eq "$((COUNT*6))" ]] || { echo "Wrong legacy sample count: $variant $actual" >&2; exit 1; }
  done
  date -u >"$OUT/paired-complete.txt"
  ;;
 *) echo "Unknown action: $ACTION" >&2; exit 1 ;;
esac
