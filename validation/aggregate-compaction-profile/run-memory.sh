#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
# Prerequisite: Go 1.27.3 with simd experiment and module dependencies available.
# Inherit caller PATH/GOCACHE/GOMODCACHE/GOFLAGS; optionally source a local setup.
if [[ -n "${GOLUME_ENV_SETUP:-}" ]]; then source "$GOLUME_ENV_SETUP"; fi
export GOEXPERIMENT=simd GOMAXPROCS=1
D=validation/aggregate-compaction-profile
python3 - <<'PYTHON'
from pathlib import Path
import json
root = Path.cwd()
d = root / 'validation/aggregate-compaction-profile'
for rows in (32768, 262144):
    names = ('native_aggregate_benchmark_test.go', 'generic_group_benchmark_test.go')
    replace = {str(root/'engine'/name): str(d/f'{rows}-{name}.txt') for name in names}
    (d/f'overlay-{rows}.json').write_text(json.dumps({'Replace': replace}, indent=2))
PYTHON
for rows in 32768 262144; do
 go test -overlay "$D/overlay-$rows.json" ./engine -run '^$' -bench '^Benchmark(NativeAggregate|GenericGroup)$/^groups=(0|4096)$/^(lanes=4|keys=3)$' -benchtime=1x -count=1 -benchmem > "$D/memory-$rows.txt" 2>&1
done
trap 'rm -f "$D/capabilities.go"' EXIT
cp "$D/capabilities.go.txt" "$D/capabilities.go"
go run "$D/capabilities.go" > "$D/capabilities.txt"
rm "$D/capabilities.go"
go list ./... > "$D/go-list.txt"
