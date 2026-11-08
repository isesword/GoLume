# Native SIMD-preferred defaults

Base: `addb233d5541a3aea8c2fbce5e238250cb12ad29`.

This stage makes `ModeAuto` prefer every retained native SIMD backend that the
current build and hardware can execute. It is a dispatch policy change, not a
claim that every operation is vectorized or every workload becomes faster.
The planner's bitmap/direct strategy approvals are unchanged.

## Implementation

- Auto now selects portable Float32 literal comparisons, Int32/Uint32/Float32
  column comparisons, Float64 Sub/Mul/Div, and dense Float32/Float64 Abs/Sqrt.
- Native amd64 Hash64 and supported fixed-width compaction retain both AVX-512
  and at-least-512-bit runtime guards. Existing arm64 literal comparison
  implementations are preserved; its unrolled Hash64 remains honestly non-SIMD.
- Explicit Scalar, experiment-disabled builds, runtime emulation, absent
  implementations, insufficient hardware, tails, and nullable math retain
  correct scalar behavior. Deleted implementations are not resurrected.
- Column-comparison capabilities are separate from literal KernelIDs. The CLI
  reports all 50 column operations independently. On this native amd64 host,
  Auto selects 18 hardware-SIMD KernelIDs and 15 column-comparison operations.
- Generated Float32 comparison masks use fixed stack storage. Column32 and
  Float32 comparison tests assert zero allocations in the kernel calls.
- `scripts/build.sh` defaults an unset experiment to SIMD, honors explicit
  overrides and forwards build arguments, without persistent Go configuration.

## Validation

- Full ordinary `go test ./...`: pass.
- Full `GOEXPERIMENT=simd go test ./...`: pass.
- Full `GOEXPERIMENT=simd go test -race ./...`: pass.
- Ordinary and SIMD `go vet ./...`, GoDoc, generated-source consistency,
  registry consistency, and SIMD structure checks: pass.
- Complete kernel and CLI suites with runtime emulation and 128/256/512-bit
  settings: pass. Native host reports amd64, non-emulated 512-bit vectors.
- Both complete builds: pass with `GOFLAGS=-buildvcs=false`, because this
  source archive has no usable Git metadata. The build entry itself
  does not change that flag for normal repository checkouts.
- Linux/arm64 SIMD kernel test cross-compilation: pass. No native arm64 execution
  or new arm64 performance result is claimed.
- Query coverage: 120 filter/project cases across two types, five operators,
  four selectivity/null profiles and three dispatch modes, plus 40 fixture
  invariant tests. Inputs cover equality, signed/unsigned boundaries, and
  4096 + 4096 + 7 source rows.
- Benchmark runner smoke: exactly 240 kernel and 120 query cases, six samples
  per case. Its six mode permutations and skip filters were checked.

The current source, JSON oracle fixtures, and validation registry were restored
and checked against the base commit's Git blob identities before intentional
changes. One stale source file absent from the base was removed from the local
restored copy; it is not a repository deletion.

The local `testdata/bench-1m.parquet` resolves to the existing synthetic fixture,
not the base commit's original binary. Full-suite results use that fixture.
Its SHA256 is
`115f46c063da52e2b3b8c883522e8d29a4559b410772bc08681a87235d783876`
(3,566,591 bytes). Neither it nor the tracked `kernel.test` binary is changed;
the tracked test binary is never used. The new comparison matrices construct
their own fixtures and do not depend on that Parquet file.

## Observed results

The complete run contains 80 kernel workloads and 40 query workloads, each
measured in three modes with six samples. All kernel scenarios retain zero
reported allocations. Query allocation and managed-peak medians are identical
across the three modes; projected/source row counts also agree.

Auto kernel medians are lower in 70 workloads and higher in 10. The higher
medians are the short-input cases, where this 512-bit host has no complete SIMD
vector; the largest is Uint32 GE at 73.5% higher.
Query medians are mixed: 27 lower and 13 higher, ranging from 27.7% lower to
22.0% higher. Most query differences are not statistically distinguishable
in these short, noisy samples. These are descriptive observations, not
workload-wide speedup/regression guarantees; see the raw samples and benchstat
reports. No kernel improvement is extrapolated into a query claim.

## Measurement scope

`balanced-*` records use frozen test binaries on this Intel Xeon Platinum
8573C linux/amd64 host, `GOMAXPROCS=1`, six order-balanced samples per case,
and 100 ms per sample. The runner completes compilation and correctness checks before timing.
This is a shared host, not a dedicated/pinned performance lab. Short samples are exploratory, not the historical ten one-second gate.

Kernel measurements cover all eight input profiles and all five operations
for both 32-bit integer types. Query measurements cover sparse, medium, dense
and nullable outputs. Auto now uses SIMD rather than serving as a scalar
fallback control. Whole-query changes include the complete selected dispatch
set and cannot isolate one kernel's contribution.

`peak_bytes` measures managed execution workspace, not source fixtures,
kernel-set construction, the complete process heap or RSS. Fixtures,
expression compilation and kernel-set construction are outside the query timer.
Zero kernel allocations therefore do not mean zero engine-initialization cost.

The runner records source/binary hashes, full command settings and every mode
order. Its complete marker means execution finished, not automatic policy
approval. A longer repeat is available with:

```sh
GOMAXPROCS=1 SIMD_BENCHCOUNT=12 SIMD_BENCHTIME=1s \
  ./scripts/bench_column32.sh validation/column32-native
```

No main-branch merge, release, or claim of repository-wide performance acceptance
is part of this stage.
