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
The full mixed nested-plus-root-Decimal Parquet file can be written and checked
with the independent Arrow reader, but GoLume NativeNested Parquet input still
rejects root Decimal. Native Decimal/key-only Parquet files use the existing
default reader. This slice does not add the mixed native-reader decoder.
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
Native Decimal TopK, configured external/spill Sort, Join, windows and value/group
aggregation retain their admission restrictions. Shared low-level coefficient
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
finite RAM and cancellation. Existing Decimal boundary tests change only the
resident Sort contracts; their other negative cases and original budgets remain.
The N9 195-row/64 MiB regression is unchanged. Tests are acceptance specifications;
current gate results and retained historical failures are reported separately.

`Sort(...).Limit(n)` remains a resident Sort followed by Limit when visible
payload or a computed sort key is native Decimal. The optimizer must not
replace it with the unsupported Decimal TopK operator. This guard does not
make the query K-bounded: resident Sort still owns and sorts all input rows.
Direct TopK remains rejected; ordinary scalar/opaque/bounded-container rewrites
retain their existing behavior. Unresolved schema/key capability is not guessed.
