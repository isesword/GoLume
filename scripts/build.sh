#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Prefer native SIMD without modifying the caller's persistent Go settings.
# An explicitly set value, including an empty string, remains authoritative.
export GOEXPERIMENT="${GOEXPERIMENT-simd}"
if (( $# == 0 )); then
    set -- ./...
fi
exec go build "$@"
