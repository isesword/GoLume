#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

go run ./tools/checkgodoc .
go test ./...
GOEXPERIMENT=simd go test ./...
GOEXPERIMENT=simd GODEBUG=simd=0 go test ./kernel ./expr ./physical ./engine ./lazy ./optimizer ./memory ./source/arrowrecord
GOEXPERIMENT=simd go test -race \
  ./engine ./hashagg ./join ./radix ./scheduler ./lazy ./physical ./optimizer ./memory ./source/parquet ./source/arrowrecord
go vet ./...
GOEXPERIMENT=simd go run ./cmd/lazy-demo \
  -parquet ./testdata/bench-1m.parquet \
  -workers 4 -analyze
