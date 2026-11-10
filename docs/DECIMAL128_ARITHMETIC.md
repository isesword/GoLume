# Exact Decimal128 arithmetic

Native Decimal128 represents `coefficient * 10^-scale` with signed 128-bit
coefficients, precision 1..38, and scale 0..precision. `expr.Add`, `expr.Sub`,
and `expr.Mul` (including the lazy expression builders) accept two native
Decimal128 operands. They never implicitly convert a decimal to float or
coerce an integer/string/float operand to decimal.

## Result types

Given operands `(p1, s1)` and `(p2, s2)`:

- Add/subtract: `s = max(s1, s2)`,
  `p = min(38, max(p1 - s1, p2 - s2) + s + 1)`.
- Multiply: `s = s1 + s2`, `p = min(38, p1 + p2)`.
  Binding rejects `s > 38`, even in a dead branch or for NULL/zero literals.
- The result is nullable if either operand is nullable.

Capping precision at 38 does **not** round, truncate coefficients, or reduce
scale. Each demanded, non-NULL result must satisfy `abs(coefficient) < 10^p`.
If it does not, execution returns a checked arithmetic overflow with the
operator, result precision/scale, and the first failing row in the current
execution batch. Selected evaluation reports the row within the selected
batch, as other checked arithmetic does. There is no partial successful result
from an evaluation that fails. Semantic failure closes its expression workspace
and releases its reservations.

For example, adding two `(38, 38)` operands retains scale 38. A sum with
coefficient `10^38` fails instead of being rounded to a smaller scale. Conversely,
subtracting `(10^37, scale 0)` and `(10^38 - 1, scale 1)` succeeds with coefficient
1 at scale 1. The aligned intermediate need not fit the result precision.

Multiplication uses `p1 + p2` because the strict operand bounds already imply
`abs(a * b) < 10^(p1 + p2)`; no extra carry digit is needed. These are GoLume's
explicit exact-result rules, not a claim of matching another database's decimal
promotion or rounding conventions.

## NULLs, guards, and error ordering

- A NULL operand makes that arithmetic result NULL. Its coefficient payload is
  ignored, including noncanonical/out-of-range payload bits.
- IF, CASE, COALESCE, and IN retain their existing demand/short-circuit rules.
  An overflow in a branch or candidate that is not evaluated is suppressed.
- Arithmetic children retain eager left-before-right program order. A NULL
  parent does not suppress an error in an evaluated child. Ordinary SQL AND/OR
  remain eager, so a false sibling does not hide an arithmetic error.
- Within one arithmetic instruction, rows are checked in increasing execution
  order. Across instructions, existing expression order is preserved; this is
  not a global smallest-row error selection.
- Binding checks all operand metadata and unsupported operations before data
  execution, including in dead branches. Source-vector validation still checks
  valid coefficients and schema agreement independently of value-level guards.
- Arithmetic is marked `MayError`. Optimizers must preserve its demanded input
  domain; a later empty filter must not erase an earlier demanded overflow.
  Unused projections may still be pruned.

## Implementation and scope

Same-scale add/subtract uses checked two-limb signed arithmetic. Cross-scale
add/subtract and multiplication use stack-resident 256-bit integer magnitudes,
then check the exact final coefficient against its declared precision. This
also permits cancellation after alignment. No per-row `big.Int` allocation or
floating-point conversion is used in production; tests use `math/big.Int` as
an independent oracle.

The implementation is scalar integer arithmetic and runs identically under
scalar and SIMD kernel selections. `math/bits` supplies limb multiply/carry
operations; this is not a native SIMD decimal kernel and no SIMD speedup is
claimed. Wider intermediates and checked 128-bit lane carries require separate
kernel design and benchmarking before a vectorized fast path is appropriate.

Explicit exact precision/scale conversion is available through
[`RescaleDecimal128`](DECIMAL128_RESCALE.md), including scale reduction only
when it discards no nonzero digits. Division, generic decimal casts, rounding,
mixed numeric coercion, negative scales, wider decimals, decimal aggregation, and
conditional branches with differing decimal precision/scale remain outside
this change. Opaque Decimal128/Decimal256 inputs without supported native
metadata remain rejected by arithmetic.

## Validation coverage

The arithmetic-specific tests cover independent integer-oracle checks across
all scale pairs 0..38, random coefficients, both signs, precision boundaries,
128-bit carries/overflow, cross-scale cancellation, NULL payloads, guards,
left/right error order, empty and bitmap-boundary batches, dense and selected
execution, repeated workspace reuse, concurrent workspaces, and memory
admission failure/retry.

Parquet integration tests cover optimized filters and projections, ordinary
and bounded execution, scalar and SIMD kernel modes, Collect, batch streaming,
Parquet sink/readback, demanded overflow, unused/dead expressions, early close,
and cancellation. Cross-compilation, where performed, is build evidence only,
not native execution on that architecture.
