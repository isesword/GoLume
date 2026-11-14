# Explicit mixed-integer Decimal: downstream support boundaries

Originally verified on `ec773430`; integrated with Bloom and the CountRows fix. This is a capability verification of existing public APIs,
not a new implementation, implicit promotion change or performance result.
The input query starts with typed Int64/Uint64 columns, converts each to
Decimal128(20,0), and subtracts them. Its result is nullable Decimal128(21,0),
including exact values outside the Int64 and binary64 exact integer domains.

`lazy/decimal_downstream_recipe_test.go` uses public lazy, memory codec/store,
and Arrow reader APIs. Expected coefficients are computed from the original
integer inputs with `math/big`, never by floating conversion or by reusing
production Decimal arithmetic. The 130-row input crosses bitmap boundaries.

## Verified matrix

| Consumer | Observed result | Scope |
| --- | --- | --- |
| Collect / CollectBatches | Exact coefficient, precision 21, scale 0 and NULLs preserved | Ordinary and bounded sessions; scalar/requested SIMD |
| SinkParquet -> ScanParquet -> Collect | Exact native Decimal roundtrip | All 130 rows and metadata checked |
| SinkIPC -> Arrow IPC file reader | Exact Arrow Decimal128 roundtrip | Arrow coefficient, logical NULL and type precision/scale checked |
| Arrow record re-entry through ScanArrow, then native CastDecimal128 | Explicit rejection: native precision required, source field has precision 0 | Transport preservation does not imply native expression re-entry support |
| Global Sum / Min / Max / Avg on result | Explicit rejection: Decimal does not support numeric reductions | No automatic Float64 reduction |
| CountColumn on result / GroupBy Decimal with Count | Explicit native Decimal aggregation rejection | Not evidence that the coefficient codec lacks support |
| Inner Join on Decimal key | Explicit native Decimal execution rejection | No native-key equality Join claim |
| Inner Join on Int64 key retaining Decimal payload | Same explicit rejection | Non-Decimal keys do not make this payload path supported |
| Sort on Decimal key / Int64 key retaining Decimal payload | Explicit native Decimal execution rejection | Includes bounded sessions with spill configured |
| Public ColumnarRun codec -> real QuerySpillStore file -> Restore -> Decode | Exact Decimal coefficient, metadata and NULLs preserved | Codec/store only; confirmed nonzero committed disk bytes and file, then cleanup/accounting release |
| CountRows after expression projection | Exact row count | Decimal projection and pure Int64 control; no Decimal value aggregation claim |

The consumer rejection checks run both ordinary and bounded execution with a
finite memory limit and explicit spill quota. Rejection before materialization
**does not exercise an operator's external/spill algorithm**. The distinct codec
roundtrip test writes and restores an actual file, but it does not establish
spill-capable Decimal aggregation, Join or sorting.

## Important distinctions

- `physical/decimal_boundary.go` rejects native Decimal schemas for Join,
  ordering and window operators; callers in `physical/join.go` and `order.go`
  enforce that boundary even for Decimal payload rather than key columns.
- `engine/compile.go` rejects native Decimal aggregation inputs. Numeric
  reduction validation can reject earlier with a more specific error.
- `memory/spill_columnar.go` supports native Decimal transport independently
  of these operator admission restrictions.
- `source/arrowrecord/source.go` maps Arrow Decimal to a Decimal/Opaque field
  without native precision/scale in this baseline. Therefore an IPC file that
  preserves its Arrow Decimal type is not proof that ScanArrow supplies a
  native Decimal vector usable by the explicit arithmetic APIs. Parquet's
  native Decimal scan path is separately tested and succeeds here.
- CountRows previously failed with an invalid empty expression projection,
  including with pure Int64 input. The integrated optimizer repair preserves
  cardinality and the SelectExprs schema boundary; these tests now assert the
  correct row count. This does not enable Decimal value aggregation.

## Limits and follow-up

The tests do not claim coverage of every aggregate kind, every Join mode, all
spill codecs, dictionary encodings, nested payloads, arbitrary Decimal scales,
all output compression modes or consumer combinations. Arrow transport checks
are not a direct lazy-to-Arrow in-memory API claim. Requested SIMD can use
scalar Decimal implementation. Native ARM64 and performance are not inferred.

This work does not change runtime code or defaults. The generic CountRows defect is fixed separately from native Decimal consumer
support. Next work should design Arrow native re-entry versus Opaque transport. Extending
operator acceptance requires typed execution, budgeting and roundtrip tests;
removing admission checks alone would be unsafe.
