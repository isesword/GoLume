# Resident native Decimal128 Sort (N10 candidate)

Resident stable Sort accepts top-level native Decimal128 keys and payloads.
Each key column has fixed precision 1..38 and scale 0..precision. Comparison is
exact signed coefficient ordering within that column, without floating-point
conversion, subtraction overflow, rounding or cross-column scale alignment.
Multiple key columns compare lexicographically; ordinary scalar keys may be mixed
with Decimal keys. Existing direction, NULL placement and stable-tie rules apply.

Arrow input still requires explicit NativeDecimal128; it composes with
NativeNested. Zero/default options retain opaque transport. Native arithmetic
before Sort, owned Collect, IPC/native Arrow re-entry and native Parquet transport
retain coefficients and precision/scale within their existing reader profiles.
N11 adds root Decimal to explicit NativeNested Parquet input, so mixed bounded
containers plus root Decimal can be read back natively. Independent Arrow checks
and default flat Decimal reads remain separate controls. See
[Parquet root Decimal](PARQUET_ROOT_DECIMAL.md) for reader scope and budget limits.
NULL payload bits are not values.
Existing bounded container payload shapes do not expand, and Decimal children
inside native Struct/List remain unsupported. This native profile restriction
must not be applied to an entire opaque Arrow payload merely because its schema
retains child descriptions. Existing default opaque Struct transport may carry
Decimal children or deeper Arrow structures without becoming native nested
Decimal execution; scalar-key Sort keeps that transport contract. Opaque
payloads do not become Decimal keys or acquire generic IPC/native export support.
Runtime storage, not the presence of schema Children alone, distinguishes these
paths.

This is resident full-materialization Sort, not bounded TopK or external sort.
Join, windows and value/group aggregation retain their admission restrictions.
Root Decimal TopK is added separately in N12 and explicit bounded external Sort
in N13 (DECIMAL_EXTERNAL_SORT.md). Shared low-level coefficient
support must not implicitly authorize those consumers. Ordinary native input
support does not mean generic opaque Arrow IPC export is supported.

Owned source copies, metadata, comparison descriptors, ordinal/run arrays,
input-to-sorted output overlap and borrowed output headers require admission.
The original input may close before output is consumed; borrowed views expire
at the next read or Close, while owned Collect survives upstream closure. A
budget failure or cancellation is terminal and releases execution ownership.
No unbounded fallback or performance/SIMD acceleration is claimed.

`lazy/decimal_resident_sort_test.go` specifies mixed maximum-depth container
fixtures, independent big.Int stable ordering, p1/p38 and s0/s38, signed extrema,
values beyond binary64 precision, NULL poison, serial/parallel modes, direction
and NULL placement, source-close and borrow lifetime, IPC/Parquet re-entry,
finite RAM and cancellation. N10 changed only resident Sort boundary tests; N12 separately updates the
TopK boundary. Other negative cases and original budgets remain.
The N9 195-row/64 MiB regression is unchanged. Tests are acceptance specifications;
current gate results and retained historical failures are reported separately.

N12 extends root native Decimal support to TopK and permits the existing
`Sort(...).Limit(n)` rewrite, including derived Decimal keys. See
[Decimal TopK](DECIMAL_TOPK.md). Both strategies validate native Decimal input;
spill and other consumer boundaries remain independent. Unresolved schema/key
capability is not guessed.

N14 separately adds global all-native-Decimal SUM queries only; see
[exact global SUM](DECIMAL_GLOBAL_SUM.md). Other aggregate/group/window
boundaries described above remain, rather than a general Decimal aggregate opening.

N15 additionally allows COUNT(*) and COUNT(root native Decimal) beside at least
one global native Decimal SUM; see [SUM with COUNT](DECIMAL_SUM_COUNT.md).
This does not open grouped or arbitrary mixed aggregates.
