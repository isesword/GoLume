#!/usr/bin/env bash
# The same gates run locally and in CI. Each phase is independently runnable.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PHASE="${1:-all}"
case "$PHASE" in all|checks|tests|runtime|race|cross|demo) ;; *)
  echo "usage: $0 [all|checks|tests|runtime|race|cross|demo]" >&2; exit 2 ;;
esac
if (( $# > 1 )); then echo 'expected at most one phase' >&2; exit 2; fi

# Do not let local go env -w, workspace files, test filters, CPU flags or a
# cross-compilation target silently change the meaning of a successful gate.
export GOENV=off GOWORK=off GOFLAGS=-buildvcs=false GOEXPERIMENT= GODEBUG=
export GOAMD64=v1 GOARM64=v8.0
unset GOOS GOARCH CGO_ENABLED
HOST_ARCH="$(go env GOHOSTARCH)"
HOST_OS="$(go env GOHOSTOS)"
case "$HOST_ARCH" in amd64|arm64) ;; *) echo "unsupported validation host: $HOST_ARCH" >&2; exit 1 ;; esac
case "$(uname -m)" in x86_64|amd64) MACHINE_ARCH=amd64 ;; aarch64|arm64) MACHINE_ARCH=arm64 ;; *) MACHINE_ARCH=unknown ;; esac
if [[ "$MACHINE_ARCH" != "$HOST_ARCH" ]]; then
  echo "Go host architecture $HOST_ARCH differs from machine $MACHINE_ARCH; native execution required" >&2; exit 1
fi
export GOARCH="$HOST_ARCH" GOOS="$HOST_OS"
if [[ -n "${VALIDATE_EXPECT_ARCH:-}" && "$HOST_ARCH" != "$VALIDATE_EXPECT_ARCH" ]]; then
  echo "runner architecture $HOST_ARCH, expected $VALIDATE_EXPECT_ARCH" >&2; exit 1
fi
if [[ -n "${VALIDATE_EXPECT_OS:-}" && "$HOST_OS" != "$VALIDATE_EXPECT_OS" ]]; then
  echo "runner OS $HOST_OS, expected $VALIDATE_EXPECT_OS" >&2; exit 1
fi
case "${VALIDATE_REQUIRE_NATIVE:-0}" in 0|1) ;; *) echo 'VALIDATE_REQUIRE_NATIVE must be 0 or 1' >&2; exit 2 ;; esac

checks() {
  go run ./tools/checkgodoc .
  GOEXPERIMENT=simd go run ./internal/cmd/gensimd -check
  GOEXPERIMENT=simd go run ./tools/kernelgate check
  for script in scripts/*.sh; do bash -n "$script"; done
  bash scripts/check_simd_structure.sh
  go test -count=1 ./scripts ./cmd/kernel-info
  go vet ./...
  GOEXPERIMENT=simd go vet ./...
}

tests() {
  go test -count=1 ./...
  GOEXPERIMENT=simd go test -count=1 ./...
}

runtime() {
  go run ./cmd/kernel-info -mode scalar -check-scalar -max-native-width
  local bits width flag mode
  bits="$(GOEXPERIMENT=simd go run ./cmd/kernel-info -mode simd -max-native-width)"
  case "$bits" in 0|128|256|512) ;; *) echo "invalid native width: $bits" >&2; return 1 ;; esac
  echo "Native $HOST_OS/$HOST_ARCH SIMD width: $bits"
  if [[ "${VALIDATE_REQUIRE_NATIVE:-0}" == 1 && "$bits" == 0 ]]; then
    echo 'native SIMD required, but only scalar dispatch is available' >&2; return 1
  fi
  # A forced width can execute unsupported instructions. Never force above the
  # detected native maximum, and verify the actual dispatch before testing it.
  for width in 128 256 512; do
    if (( width > bits )); then
      echo "SKIP native SIMD +$width: host maximum is $bits"
      continue
    fi
    for mode in auto simd; do
      GOEXPERIMENT=simd GODEBUG="simd=+$width" go run ./cmd/kernel-info -mode "$mode" -check-native-width "$width" -max-native-width
    done
    GOEXPERIMENT=simd GODEBUG="simd=+$width" go test -count=1 ./...
  done
  # simd=0 checks the package-level switch. CPU masking independently exercises
  # architecture guards; x86 AVX2 is required even for the 128-bit lowering.
  local fallbacks=(simd=0)
  if [[ "$HOST_ARCH" == amd64 ]]; then
    fallbacks+=(cpu.all=off cpu.avx2=off)
    # Only force 128 bits on a CPU that actually supports native SIMD. The
    # masking case must also be safe on older scalar-only amd64 hosts.
    if (( bits >= 128 )); then fallbacks+=(simd=+128,cpu.avx2=off); fi
  fi
  for flag in "${fallbacks[@]}"; do
    for mode in auto simd; do
      GOEXPERIMENT=simd GODEBUG="$flag" go run ./cmd/kernel-info -mode "$mode" -check-scalar -max-native-width
    done
    GOEXPERIMENT=simd GODEBUG="$flag" go test -count=1 ./...
  done
}

race() {
  CGO_ENABLED=1 go test -race -count=1 ./...
  CGO_ENABLED=1 GOEXPERIMENT=simd go test -race -count=1 ./...
}

cross() {
  local arch
  for arch in amd64 arm64; do
    # Compilation is a separate gate, never evidence of native ARM64 execution.
    CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build ./...
    CGO_ENABLED=0 GOOS=linux GOARCH="$arch" GOEXPERIMENT=simd go build ./...
  done
}

demo() {
  GOEXPERIMENT=simd go run ./cmd/lazy-demo -parquet ./testdata/bench-1m.parquet -workers 4 -analyze
}

if [[ "$PHASE" == all ]]; then
  for phase in checks tests runtime race cross demo; do
    echo "== $phase =="
    "$phase"
  done
else
  "$PHASE"
fi
