# SIMD and execution API migration

This release intentionally has no compatibility wrappers. Every execution path
must receive one engine-owned `kernel.Set`; constructing a new set from a mode
inside a source or operator is an error in design and is rejected by
`scripts/check_simd_structure.sh`.

## Source readers

Before:

```go
reader, err := src.NewReader(ctx)
```

After:

```go
kernels := engine.KernelSet()
reader, err := src.NewReader(ctx, source.ReadOptions{Kernels: kernels})
```

The zero `ReadOptions` value returns an error. Wrappers must pass the received
options unchanged. Join preparation and timestamp decoding use the same handle.

## Results and aggregates

- Read materialized groups only from `engine.Result.Rows` as
  `[]engine.AggregateRow`; `Result.Groups` and `engine.Group` no longer exist.
- Use `expr.AggCountRows`; the `expr.AggCount` alias no longer exists.
- Allocate aggregate state with `aggregate.NewInt64Vector(slots, layout)` and
  finalize a slot with `Result(slot)`. The `SumCountVector` aliases and direct
  `Sum`/`Count` accessors no longer exist.

## Ordering and Parquet

- `logical.SortKey` and `lazy.SortExpr` require `Expr`. Direct-column ordering
  is written as `Expr: expr.Col(name)`; `lazy.Asc`/`Desc` already construct it.
- Open Parquet sources with `parquet.OpenFile(path)` and project by column name.
  Index-based `Columns` and `Open` were removed.
- Use `physical.NewTopKSourceWithManager`, passing the query memory manager or
  explicit `nil` in standalone diagnostics.

## Build and dispatch policy

The module requires Go 1.27. SIMD implementations are selected by the
`goexperiment.simd` build tag, not by the Go version alone. Direct build/test
commands need `GOEXPERIMENT=simd`; without the experiment, `installSIMD` is a
no-op and the scalar implementations remain installed. `scripts/build.sh`
defaults an **unset** experiment to `simd`, but honors an explicitly empty
`GOEXPERIMENT=`. It does not modify persistent `go env` settings.

Capabilities are queried with `Set.Capability(KernelID)`. A result for one
operation says nothing about another operation in the same family:

- `ModeScalar` retains the scalar reference implementations.
- `ModeAuto` prefers retained native implementations when the build, runtime
  hardware, and operation-specific width guards permit them. It is **not** a
  per-operation benchmark allowlist, and amd64 Auto is not universally scalar.
- `ModeSIMD` selects available retained native implementations for diagnostics,
  subject to the same safety guards. It cannot force unavailable instructions;
  unsupported operations remain scalar.

The common native guard rejects emulated SIMD. On amd64 it additionally requires
AVX2, including at a selected width of 128 bits: current experimental lowering
uses AVX2 broadcasts even for that width. Generated amd64 comparison overrides
accept the exact runtime-selected width: 128 or 256 bits with AVX2, or 512 bits
with the runtime AVX-512 feature predicate. Width selection alone is not a CPU
safety check. Hash64 and wide compaction have separate AVX-512 and selected-width
(at least 512 bits) guards; byte/word compaction additionally requires VBMI2.
These are operation-specific conditions, not a promise of equal coverage at all
three widths or of vectorizing every call. Short inputs, tails, and some masks
may use scalar work inside an installed native kernel.

An architecture-specific non-scalar backend is not necessarily hardware SIMD.
In particular, arm64 Auto retains the `arm64-unrolled` Hash64 backend with
`HardwareSIMD=false`; explicit SIMD does not install that Auto-only unroll.
Use the reported backend, `HardwareSIMD`, and `VectorBits` together rather than
interpreting the mode name as proof of execution. Column-to-column comparison
capabilities are separate from literal comparison capabilities.

For current operation coverage and remaining scalar boundaries, see
[SIMD kernel dispatch](SIMD_KERNELS.md) and the
[operation matrix](SIMD_OPERATION_MATRIX.md). Planner bitmap/direct strategy
approvals are a separate decision policy; native-preferred kernel selection does
not itself change those approvals or establish whole-query acceleration.

### Source of this contract

The build switch and common guard are implemented in
[`simd_go127.go`](../kernel/simd_go127.go),
[`simd_disabled.go`](../kernel/simd_disabled.go), and
[`simd_guard_amd64.go`](../kernel/simd_guard_amd64.go).
The width-specific comparison guard is in
[`zz_generated_simd_amd64.go`](../kernel/zz_generated_simd_amd64.go).
Hash/compaction guards are in
[`hash_simd_amd64.go`](../kernel/hash_simd_amd64.go) and
[`compact_simd_amd64.go`](../kernel/compact_simd_amd64.go);
the Auto-only arm64 unroll is in
[`hash_simd_arm64.go`](../kernel/hash_simd_arm64.go).
The experiment default is defined by [`build.sh`](../scripts/build.sh).

### Historical measurements are not current dispatch gates

The darwin/arm64 comparison, Float64 arithmetic, and non-SIMD Hash64 decisions
recorded in `validation/simd-auto-gates.json` describe the former measured-only
Auto policy. They remain historical evidence for their recorded versions,
hardware, workloads, and sampling conditions, not today's kernel allowlist.
They do not establish current native ARM64 execution, hosted CI success, all
runtime widths/fallbacks, or long-running performance and memory stability.
Cross-compilation is not native execution. Fresh performance claims require
recorded source/toolchain, host CPU, selected backends/widths, workload, and
measurements; this documentation correction runs no tests or benchmarks.
