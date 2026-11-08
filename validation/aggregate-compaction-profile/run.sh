#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
# Prerequisite: Go 1.27.3 with simd experiment and module dependencies available.
# Inherit caller PATH/GOCACHE/GOMODCACHE/GOFLAGS; optionally source a local setup.
if [[ -n "${GOLUME_ENV_SETUP:-}" ]]; then source "$GOLUME_ENV_SETUP"; fi
export GOEXPERIMENT=simd GOMAXPROCS=1
D=validation/aggregate-compaction-profile
run() {
 local name=$1 package=$2 bench=$3
 go test "$package" -run '^$' -bench "$bench" -benchtime=3s -count=1 -benchmem -cpuprofile="$D/$name.cpu" -memprofile="$D/$name.mem" -o "$D/${package##*/}.test" > "$D/$name.profile.txt" 2>&1
 go tool pprof -top -nodecount=35 "$D/$name.cpu" > "$D/$name.cpu-flat.txt"
 go tool pprof -top -cum -nodecount=35 "$D/$name.cpu" > "$D/$name.cpu-cum.txt"
 go tool pprof -top -alloc_space -nodecount=25 "$D/$name.mem" > "$D/$name.alloc.txt"
 go test "$package" -run '^$' -bench "$bench" -benchtime=1s -count=5 -benchmem > "$D/$name.bench.txt" 2>&1
}
run native-global ./engine '^BenchmarkNativeAggregate$/^groups=0$/^lanes=4$'
run native-grouped ./engine '^BenchmarkNativeAggregate$/^groups=4096$/^lanes=4$'
run generic-grouped ./engine '^BenchmarkGenericGroup$/^groups=4096$/^keys=3$'
run selection-auto ./physical '^BenchmarkSelectionProject$/^width32$/^pct50$/^inputfalse$/^auto$'
run selection-materialized ./physical '^BenchmarkSelectionProject$/^width32$/^pct50$/^inputfalse$/^materialized$'
