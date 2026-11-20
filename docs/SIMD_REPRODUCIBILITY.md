# Reproducing SIMD dispatch and performance evidence

This is a procedure, not a new measurement or CI pass. Current isolated evidence is amd64-only. Historical ARM measurements quoted in other documents are separate evidence; neither cross-compilation nor these commands proves current native ARM64 execution. Keep the exact source manifest, toolchain, CPU capabilities, raw logs and failed runs with every report.

## Environment and provenance

Run from a verified candidate with an explicit approved file manifest. Use a fresh evidence directory and refuse to overwrite it. Pin the actual installed Go executable, dependencies and cache; do not silently fetch tools. Set the following required variables to your verified Go 1.27.3 executable and prepopulated offline dependency/build caches. Record actual machine-specific paths in private run evidence, not repository documentation:

```sh
: "${GO:?set absolute path to verified Go 1.27.3 executable}"
: "${GOPATH:?set prepopulated offline dependency cache}"
: "${GOCACHE:?set verified build cache}"
export GOPATH GOCACHE
export GOENV=off GOWORK=off GOFLAGS=-buildvcs=false GOTOOLCHAIN=local
export GOPROXY=off GOSUMDB=off GOMAXPROCS=1
export GOAMD64=v1 GOARM64=v8.0
unset GOOS GOARCH CGO_ENABLED
export GOEXPERIMENT= GODEBUG=
"$GO" version
"$GO" env GOHOSTOS GOHOSTARCH GOOS GOARCH GOAMD64 GOEXPERIMENT
uname -a
```

Also record CPU model, affinity, concurrent workloads, power/thermal policy when observable, exact query/fixture/row counts/batch sizes, source SHA and executable SHA. Do not assume setting GOMAXPROCS fixes CPU affinity or eliminates host noise. Hash only approved file paths, never undisclosed or protected files.

## Dispatch, runtime width and fallback

The following are narrow commands, not the broad repository validation script:

```sh
GOEXPERIMENT= "$GO" run ./cmd/kernel-info -mode scalar -check-scalar -max-native-width
GOEXPERIMENT=simd "$GO" run ./cmd/kernel-info -mode auto
GOEXPERIMENT=simd "$GO" run ./cmd/kernel-info -mode simd
bits=$(GOEXPERIMENT=simd "$GO" run ./cmd/kernel-info -mode simd -max-native-width)
case "$bits" in 0|128|256|512) ;; *) exit 1;; esac
for width in 128 256 512; do
  if [ "$width" -gt "$bits" ]; then echo "SKIP width=$width hostmax=$bits"; continue; fi
  for mode in auto simd; do
    GOEXPERIMENT=simd GODEBUG="simd=+$width" "$GO" run ./cmd/kernel-info -mode "$mode" -check-native-width "$width" -max-native-width
  done
  GOEXPERIMENT=simd GODEBUG="simd=+$width" "$GO" test -count=1 ./kernel
 done
for mode in auto simd; do
  GOEXPERIMENT=simd GODEBUG=simd=0 "$GO" run ./cmd/kernel-info -mode "$mode" -check-scalar -max-native-width
 done
GOEXPERIMENT=simd GODEBUG=simd=0 "$GO" test -count=1 ./kernel
if [ "$("$GO" env GOHOSTARCH)" = amd64 ]; then
  for mask in cpu.all=off cpu.avx2=off; do
    for mode in auto simd; do
      GOEXPERIMENT=simd GODEBUG="$mask" "$GO" run ./cmd/kernel-info -mode "$mode" -check-scalar -max-native-width
    done
    GOEXPERIMENT=simd GODEBUG="$mask" "$GO" test -count=1 ./kernel
  done
  if [ "$bits" -ge 128 ]; then
    GOEXPERIMENT=simd GODEBUG='simd=+128,cpu.avx2=off' "$GO" run ./cmd/kernel-info -mode simd -check-scalar -max-native-width
  fi
fi
```

The CPU masks above are amd64-specific. Never force a width above detected hardware support. A `-check-native-width` success reports the maximum selected width, not that every KernelID uses that width. Save the full capability listing too. ModeSIMD may legitimately retain scalar implementations for particular operations; verify the operation actually timed. Kernel-only tests are not full query validation.

## Structure scripts and restricted validation

`scripts/check_simd_structure.sh` checks generated dispatch freshness and forbidden patterns, not timing or numerical equivalence. It discovers production Go paths and also searches several named directories. For strict manifest-only audits, reproduce its checks over explicitly approved files instead of silently broadening discovery. `go run ./internal/cmd/gensimd -check` is the generator consistency check, not a native-runtime proof.

Do not run `scripts/validate.sh` wholesale in a protected-test environment. Its checks/tests/runtime/race phases invoke broad `./...`, including repeated runtime-width tests. Likewise `scripts/bench.sh` and `scripts/bench_e2e.sh` invoke package-wide Lazy builds. Any permitted Lazy execution must instead use the reviewed literal production/test file allowlist. This document does not amend or bypass those restrictions.

## Repeatable performance, without changing semantics

After correctness passes, serialize timings away from other heavy tasks. Narrow examples (review data/CPU cost before execution):

```sh
GOEXPERIMENT=simd GODEBUG= "$GO" test ./kernel -run '^$' \
  -bench '^BenchmarkKernelID$' -benchtime=1s -benchmem -count=10
GOEXPERIMENT=simd GODEBUG= "$GO" test ./engine -run '^$' \
  -bench '^BenchmarkGlobalBindingWholeQuery$' -benchtime=300ms -benchmem -count=5
```

Redirect each run to a unique raw file and preserve exit status. If the pinned tool environment already supplies benchstat, use `"$GO" tool benchstat -alpha 0.05 -col '/mode' -format csv RAW`; otherwise preserve raw samples and report the unavailable analysis tool, do not download silently. These commands time different scopes: KernelID microkernels versus complete prepared source consumption and typed materialization. Do not turn a microkernel ratio into a whole-query claim.

For before/after code comparison, `scripts/bench_global_binding.sh` has useful controls: freeze identical harness text, immutable executables, correctness first, paired alternating execution order, environment/binary provenance and expected sample counts. Its baseline/candidate actions require genuine frozen trees and do not invent a historical baseline. Its source-discovery hashing and hardcoded expected benchmark counts need review against the current approved manifest and harness before use. The simpler bench scripts overwrite named output files and do not all normalize environment, so create fresh paths and explicitly record inherited flags.

For the test-controlled Float64 AVG experiment, both arms use the identical AVG request. Forced row fallback is a test-only internal switch, not a public execution mode. Both arms retain binding admission costs. Finite ordinary inputs and negative controls (tiny batches, batch1, nonfinite values, finite overflow invoking conservative fallback) must be reported; do not hide regressions behind a large-input median. Compare complete IEEE output bits against original row execution first. No claim that Scalar kernel mode itself disables batch aggregation.

## Three distinct memory measurements

- **Go benchmark B/op and allocs/op:** cumulative Go allocations per measured operation, not live bytes, retained memory or total process RSS. Setup outside the timer is excluded.
- **Query charged peak:** maximum outstanding tracked reservations for that query. State whether sampled in a separate warmup query or during timed execution. A metric named `peak-charged-B` in the existing global binding benchmark comes from pre-timer verification, not the maximum of timed repetitions. Excludes uncharged immutable fixtures and external allocations unless specifically integrated.
- **RSS/high-water mark:** process/OS measurement, including runtime, stacks, caches, executable pages and potentially unrelated work. Running `/usr/bin/time -v` around a frozen test executable, if available, measures process high-water RSS, not per-query memory. Wrapping `go test` also includes build/subprocess effects and must not be compared as query memory.

Never add peaks from separate stages and call them simultaneous; use shared-query snapshots to prove overlap. Query Used=0 after Close proves tracked reservation release, not absence of every heap allocation or external leak.

## Long-term regression guard: not yet established by these scripts

Repeated samples and benchstat are measurement tools, not a persistent regression service. A credible guard still needs a stable native runner/CPU, frozen fixture and semantics, paired baseline artifacts, declared noise budget and regression threshold, reviewed statistics, allocation/query-peak budgets, retained failed runs, and an owner for triage. Timing on a changing shared host cannot serve as a reliable hard threshold. Existing validate commands primarily enforce correctness/build/runtime; do not infer continuous performance or memory thresholds, ARM execution, or CI acceptance without actual run evidence.
