# Explicit exact mixed-integer expressions

Use `expr.AddInt128`, `expr.SubInt128`, or `expr.MulInt128` to opt in for one
operation. One operand must be declared Int64 and the other Uint64. Either
order is accepted; subtraction preserves the written order. The result is
always signed binary Int128, independent of the values. Default `Add`, `Sub`
and `Mul` are unchanged; mixed Int64/Uint64 still promotes to Float64.

```go
legacy := expr.Sub(expr.Col("u"), expr.Col("i"))       // Float64
exact := expr.SubInt128(expr.Col("u"), expr.Col("i")) // Int128
```

For Uint64(2^53+1) and Int64(2^53), the two expressions return zero and one
respectively. Even MinInt64 × MaxUint64 fits a signed Int128. Its magnitude
has 39 decimal digits, so it does not fit Decimal128(38,0). The exact
operators do not convert either input through Float64 or Decimal128.

## Execution and scope

This is a workspace expression API, not a new materialized column type.
Bind and compile against a schema with `expr.Compile` or
`expr.CompileExpressions`, then evaluate with `expr.Workspace`. The returned
`expr.ScalarValue` has `Type == memory.TypeInt128`. Read the borrowed
`Int128.Values` and validity bitmap before the next evaluation or Close.
Each signed value is represented as two limbs, with mathematical value
`High * 2^64 + Low`; `High` is signed and `Low` unsigned.

NULL inputs produce NULL results. Exact expressions can be result arms in
IF, CASE and COALESCE when all value arms have the same Int128 type. As
elsewhere, binding validates every arm; runtime evaluates only demanded
rows and branches. A demanded child error is not an arithmetic fallback.
Other operand types, including existing Int128 results as operands of
these three entry points, are rejected rather than coerced. This first version
does not define widening chains, overflow semantics for Int128 operands,
exact division, or any change to same-type integer arithmetic.

Logical and physical column projections do not materialize Int128. Sort and
TopK do not accept Int128 keys.
Aggregates, Join, Arrow/Parquet and spill formats gain no Int128 support.
The specialized expressions use scalar arithmetic in both build modes;
a SIMD build/run is a compatibility check, not evidence of a native SIMD
implementation.
