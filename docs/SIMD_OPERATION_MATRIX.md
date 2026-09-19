# SIMD Operation Matrix

This is the P0 baseline for the current GoLume tree. It records dispatch as
implemented, not projected coverage. Refresh it whenever a kernel or execution
path changes.

- Baseline commit: `865f8dd99ccfdb8deaa53584f98577be720ba554`
- Toolchain: `go1.27.0`
- Host: `darwin/arm64`, Apple M4 Pro
- Build experiment: `GOEXPERIMENT` is empty by default; SIMD commands below
  explicitly enable `GOEXPERIMENT=simd`.
- Verification: `go test ./...`, targeted SIMD tests, and targeted
  `GODEBUG=simd=0` tests passed after the P0 documentation and first P1/P2
  changes. Full CPU profiles and fresh one-second benchmark artifacts are
  still pending.

## Reproduction

```sh
go version
go env GOOS GOARCH GOEXPERIMENT
go test ./kernel ./memory ./expr ./engine ./lazy ./physical
GOEXPERIMENT=simd go test ./kernel ./memory ./expr ./engine ./lazy ./physical
GOEXPERIMENT=simd go run ./cmd/kernel-info -mode scalar
GOEXPERIMENT=simd go run ./cmd/kernel-info -mode auto
GOEXPERIMENT=simd go run ./cmd/kernel-info -mode simd
GOEXPERIMENT=simd GODEBUG=simd=0 go test ./kernel ./expr ./engine ./lazy ./physical
```

The repository's current Auto policy is architecture-specific. On the
validated darwin/arm64 host it admits Int32/Uint32 comparison, Float64
Sub/Mul/Div, and an `arm64-unrolled` Hash64 backend. Hash64 is deliberately
reported as non-hardware-SIMD. linux/amd64 has no Auto approval without native
benchmark evidence.

## Current operation matrix

| Family | Physical operations | Current SIMD candidate | Auto policy | Main production callers |
|---|---|---|---|---|
| Comparison | Int32 GT/GE/LT/LE | arm64 mask, 128-bit | darwin/arm64 exact IDs | predicate compiler, filter |
| Comparison | Uint32 GT/GE/LT/LE/EQ | arm64 mask, 128-bit | darwin/arm64 exact IDs | predicate compiler, filter |
| Comparison | Int64/Uint64/Float64 | scalar (literal and column-column) | scalar pending native evidence | predicate compiler, filter |
| Comparison | Int8/Int16/Uint8/Uint16 | native-width scalar `kernel.Set` entry, including column-column | scalar; no Auto admission | predicate compiler, filter |
| Comparison | Float32 | portable SIMD forced literal/column-column candidate plus scalar | scalar Auto pending gate | predicate compiler, filter |
| Arithmetic | Float64 Sub/Mul/Div | portable SIMD | darwin/arm64 exact IDs | expression projection |
| Arithmetic | checked integer arithmetic | scalar | scalar | expression projection |
| Boolean | bitmap AND/OR/NOT | word scalar | scalar | predicate registers |
| Hash | Hash64 batch | four-way native scalar unroll | darwin/arm64 approved, non-SIMD | aggregate/partition preparation |
| Hash | Hash128 batch | scalar | scalar | pair-key aggregation/join |
| Scale | checked Int64 scale | bounded scalar | scalar | temporal/unit conversion |
| String length | byte and UTF-8 length | scalar | scalar | string expressions |
| String match | equality/prefix/suffix/contains | formal scalar `KernelID` entries | scalar pending SIMD evidence | predicate string filters |
| Compact | fixed-width compaction | scalar or arch-specific candidate | evidence required per ID | filter materialization |
| Parquet decode | Float32 + IntLogicalType 8/16 | native 4/1/2-byte scalar decode | scalar; no SIMD admission | lazy scan output |
| Join payload/key | native narrow integer vectors | native scalar clone/materialize and key extraction | Float32 keys, pair/nullable spill paths pending | join output materialization |

## Evidence rules

- A KernelID is independent: one passing operation does not approve its whole
  type family.
- Auto admission requires the current gate policy: ten same-build one-second
  samples, at least 10% kernel improvement, `p < 0.05`, and no related
  end-to-end regression above 3%.
- `GODEBUG=simd=0` is an emulation/disabled-runtime correctness check; it is
  not evidence that hardware SIMD is available.
- End-to-end evidence must include the real scan, expression, filter,
  aggregate/join, and output path for the workload being claimed.

## P0 follow-up inventory

Float32 comparison now has an independent `KernelID`, literal and column-column
portable SIMD forced candidates, and a formal scalar backend. Int32 and Uint32
also have forced column-column candidates; all other physical widths use typed
scalar column-column entries. Auto admission remains operation-specific and is
not inferred from forced-mode correctness.

The current bypass inventory is narrowed to the remaining operator-owned loops:
`hashagg/{i32,u64,pair,string}.go` table probe/state updates, `join/{pair,dictionary}.go`
probe loops, and `physical/order_stream.go` merge/sort loops. Native numeric
pass-through, filter compact, take gather, and expression alias paths now use
the native maps; these operator loops remain scalar until their access pattern
and end-to-end cost are measured.

- [ ] Capture fresh CPU profiles and allocation/peak-memory measurements for
  numeric projection, filtering, aggregation, join, sort, strings, and Parquet.
- [ ] Record every production loop that bypasses `kernel.Set`.
- [ ] Pin the Polars comparison revision and link the corresponding compute
  kernel sources, distinguishing SIMD from batching and parallelism.
- [ ] Add fresh final-build benchmark artifacts under `validation/` before
  changing any Auto allowlist.
