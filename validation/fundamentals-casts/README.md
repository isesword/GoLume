# Numeric cast validation

Validated 2026-11-07 with Go 1.27.3 on linux/amd64 (AMD EPYC 9V74 shared host).
The isolated stage includes the semantic baseline and global aggregate increment,
but no concurrent math or multi-column aggregation work.

Passed:

- `go test ./...`
- `GOEXPERIMENT=simd go test ./...`
- `GOEXPERIMENT=simd go test -race ./expr ./engine ./lazy ./optimizer ./physical`
- `go vet ./...`
- `go run ./tools/checkgodoc .`
- `GOEXPERIMENT=simd go run ./internal/cmd/gensimd -check`
- `GOEXPERIMENT=simd go run ./tools/kernelgate check`

Independent review added strict/TryCast fixture comparisons, decimal threshold
and long-decimal regressions. Observed validation includes 360 pinned Polars
cases under both strictness modes, 100,000 generated ordinary decimals at both
float widths, and exact-rational long midpoint/boundary comparisons including
subnormals and overflow. Focused checkptr and race tests passed.

Two adversarial findings were fixed before this stage was frozen: successful
float overflow formerly constructed copied strconv errors, and Go 1.27.3's
Float32 parser misread some long compensated mantissas. Exact overflow-boundary
classification plus bounded decimal normalization avoid these issues. The parser
never returns a borrowed input string or library parse error to the caller.

The allocation matrix uses 4,096-row reusable expression workspaces and both
view and dictionary inputs. Valid, invalid, long-valid, long-invalid and overflow
cases all measured 64 B/op and 1 allocation/op after warmup. `allocations.txt`
contains short three-iteration diagnostic samples: they establish allocation
behavior, not statistically reliable throughput or a SIMD release gate.

No new parser SIMD backend or Auto dispatch gate is claimed. Cancellation is
checked between rows; a single exceptionally long token is parsed synchronously.
