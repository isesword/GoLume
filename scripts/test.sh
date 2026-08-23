#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo '== GoDoc =='
go run ./tools/checkgodoc .

echo '== scalar build/tests =='
go test ./...

echo '== SIMD build/tests =='
GOEXPERIMENT=simd go test ./...

echo '== vet =='
go vet ./...

echo '== lazy demo, automatic planner =='
GOEXPERIMENT=simd go run ./cmd/lazy-demo \
  -parquet ./testdata/bench-1m.parquet \
  -workers 4 \
  -repeat 3
