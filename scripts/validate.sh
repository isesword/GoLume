#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

go run ./tools/checkgodoc .
GOEXPERIMENT=simd go run ./internal/cmd/gensimd -check
GOEXPERIMENT=simd go run ./tools/kernelgate check
bash -n ./scripts/bench_hotpaths.sh
./scripts/check_simd_structure.sh
go test ./...
go run ./cmd/kernel-info -mode scalar
GOEXPERIMENT=simd go test ./...
GOEXPERIMENT=simd go run ./cmd/kernel-info -mode auto
GOEXPERIMENT=simd go run ./cmd/kernel-info -mode simd
GOEXPERIMENT=simd GODEBUG=simd=0 go test ./kernel ./expr ./physical ./engine ./lazy ./optimizer ./memory ./source/arrowrecord
GOEXPERIMENT=simd GODEBUG=simd=0 go run ./cmd/kernel-info -mode auto
VALIDATE_ARCH="$(go env GOARCH)"
GOEXPERIMENT=simd GODEBUG=simd=+128 go test ./kernel ./expr ./engine ./radix
if [[ "$VALIDATE_ARCH" == "amd64" ]]; then
	VALIDATE_BITS="$(GOEXPERIMENT=simd go run ./cmd/kernel-info -mode simd | awk '/compare-int64/ {print $3}' | cut -d= -f2)"
	if (( VALIDATE_BITS >= 256 )); then
		GOEXPERIMENT=simd GODEBUG=simd=+256 go test ./kernel ./expr ./engine ./radix
	fi
	if (( VALIDATE_BITS >= 512 )); then
		GOEXPERIMENT=simd GODEBUG=simd=+512 go test ./kernel ./expr ./engine ./radix
	fi
fi
GOOS=linux GOARCH=amd64 GOEXPERIMENT=simd go build ./...
GOEXPERIMENT=simd go test -race \
  ./engine ./hashagg ./join ./radix ./scheduler ./lazy ./physical ./optimizer ./memory ./source/parquet ./source/arrowrecord
go vet ./...
GOEXPERIMENT=simd go run ./cmd/lazy-demo \
  -parquet ./testdata/bench-1m.parquet \
  -workers 4 -analyze
