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

## Dispatch policy

Capabilities are queried with `Set.Capability(KernelID)`. A result for one
operation says nothing about another operation in the same family. `ModeSIMD`
forces only retained production SIMD backends; unsupported IDs execute and
report scalar instead of selecting historical diagnostic code. `ModeAuto`
contains only entries with committed benchmark evidence. darwin/arm64 currently
approves the exact comparison, Float64 arithmetic, and architecture-unrolled
Hash64 entries recorded in `validation/simd-auto-gates.json`; amd64 remains all
scalar in Auto until native linux/amd64 evidence exists.
