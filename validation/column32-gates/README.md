# Int32/Uint32 column-column SIMD evidence foundation

Base: `733ec424a86858afa335bdae5a4c2ab2a5c40221`.

This stage adds correctness gates, whole-query benchmarks and a reproducible
runner. It does **not** change production dispatch or admit any SIMD candidate
to Auto. All production Go files remain unchanged.

## Validation

- Go 1.27.3, linux/amd64, AMD EPYC 9V74, native SIMD 512 bits (not emulated).
- Full scalar and full SIMD `go test ./...`: pass.
- `go vet ./...` and SIMD structural/codegen checks: pass.
- Race checks for kernel/expr/physical: pass; SIMD Column32 gate race: pass.
- Column32 correctness with emulation and 128/256/512-bit settings: pass.
- SIMD kernel test cross-compilation for linux/arm64 and darwin/arm64: pass.
  No native arm64 execution or performance is claimed.
- 240 kernel and 48 full-query benchmark smoke cases pass. Single-iteration
  smoke results are correctness evidence only, not performance evidence.
- Independent review found no blocking correctness problem. The runner's
  grouping, source/build fingerprints and evidence overwrite protections were
  checked; scope/attribution limits are documented.

All restored source files were checked against the base commit's Git blob
identities. The retained local `testdata/bench-1m.parquet` is an older synthetic
fixture, not the base commit's binary fixture; full tests used that local file.
It is not changed in this commit. New Column32 benchmarks construct their own
fixtures and do not use that file. The tracked `kernel.test` binary was not used.

## Performance scope

The adjacent `column32-amd64-subset-*` artifacts contain ten one-second samples
for GT/EQ on both types: streaming and nullable-tail kernels, and 50%-match
queries with/without nulls. Every compared mode uses the same compiled executable.
The query executable was resumed separately after a launcher edit interrupted
the initial script, and its SHA256 matched the originally recorded executable.

These are exploratory subset measurements on a shared host, not an admission
record. Early Int32 streaming samples overlapped another test build; later
kernel/query samples did not overlap agent test workloads. Fixed mode order,
incomplete sparse/dense/operator coverage, and absence of a native arm64 gate
are additional reasons not to promote Auto. Rerun the complete matrix on an
idle native target with balanced mode ordering before making an admission
decision. A kernel win alone does not prove query speedup or isolated attribution.

In this subset, forced SIMD median kernel time was 29–56% lower, and the eight
whole-query cases were 7.3–13.8% lower (`p <= 0.002`, ten samples each). Kernel
allocations remained zero. Query allocations and managed workspace peaks were
unchanged. These promising results justify broader native measurement, not a
blanket default enablement. In particular, the two early streaming results have
the contention limitation noted above and need clean repetition.

Run `bash scripts/bench_column32.sh` for the full matrix. See
`docs/SIMD_CANDIDATE_DECISIONS.md` for controls and interpretation. Explicit
prefixes must be new; a `-complete.txt` marker is emitted only on successful
completion of the runner. No such full-run marker is claimed for this resumed
exploratory subset.
