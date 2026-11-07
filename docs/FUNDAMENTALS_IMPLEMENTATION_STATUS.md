# Fundamentals implementation status

This branch implements the approved fundamentals sequence in reviewed increments.
It has not been merged into the default branch. The documents below define each
API's supported types, execution contract and deliberate compatibility limits.

## Delivered

1. **Semantic baseline:** pinned Polars 1.35.2 observations for nulls, NaNs, empty
   inputs, overflow and types. Existing behavior is not silently replaced by every
   reference observation. See [FUNDAMENTALS_SEMANTICS.md](FUNDAMENTALS_SEMANTICS.md).
2. **Aggregation:** global and typed multi-column aggregation, native/composite
   keys, resident/partitioned/sorted/spill paths and exact typed outputs. See
   [NATIVE_AGGREGATION.md](NATIVE_AGGREGATION.md),
   [MULTI_COLUMN_AGGREGATION.md](MULTI_COLUMN_AGGREGATION.md) and
   [GENERIC_GROUP_KEYS.md](GENERIC_GROUP_KEYS.md).
3. **Parsing:** strict string-to-numeric conversion and TryCast with explicit
   invalid/overflow handling. Tests include long malformed and compensated
   decimal inputs, allocation checks and independent reference cases.
4. **Math:** typed abs, round, floor, ceil, modulo/floor division, power, square
   root and logarithm. Native scalar references remain available. Only measured
   eligible abs/sqrt SIMD paths were added; automatic gates were not widened.
5. **Statistics:** sample/population variance and standard deviation; fixed-width
   and owned string/binary First/Last; exact NUnique; exact numeric Median/Quantile
   with six interpolation modes. See [STATISTICS_STATES.md](STATISTICS_STATES.md),
   [VARIABLE_ENDPOINTS_DESIGN.md](VARIABLE_ENDPOINTS_DESIGN.md),
   [EXACT_DISTINCT.md](EXACT_DISTINCT.md) and
   [EXACT_QUANTILES.md](EXACT_QUANTILES.md).
6. **Ordered operations:** numeric Shift/Diff, ForwardFill/BackwardFill and
   FillNull/FillNaN value helpers. See [ORDERED_TRANSFORMS.md](ORDERED_TRANSFORMS.md).

## Important limits and compatibility

- This is not full Polars API or bitwise numerical compatibility. Counts return
  UInt64. SQL-style empty SUM and existing Int64 AVG wrapped-sum behavior remain
  documented compatibility choices. Improved integer/extreme-range moments and
  quantile interpolation can deliberately differ from the pinned reference.
- Round currently rounds to integral values; arbitrary decimal-place rounding is
  not added. Query variance exposes sample/population choices, not arbitrary ddof.
- Shift/Diff and directional fills currently accept the ten numeric widths.
  Nonnumeric ordered transforms and rolling/window quantiles are not implemented.
- Distinct and quantile queries use a shared serial tuple-sort fallback, not a
  specialized parallel speedup. Mixed ordinary rows are carried through sorting.
  Native/composite ordinary and fixed-state aggregation retain their separate
  resident, partitioned, sorted and spill implementations.
- Exact operators do not approximate or exceed admitted memory silently. Very
  large segment manifests can exhaust the memory budget; quotas include
  simultaneously live spill objects. Long backward-fill null runs may fail their
  explicit memory budget rather than retaining unbounded input.
- Legacy aggregate/query positional structs and earlier spill codecs are kept.
  Quantile parameters use additive configured APIs. Exported compiled metadata
  gained fields; use keyed literals. Duplicate output names are rejected early.
- SIMD speedups are measured only where the hardware and operation permit them.
  Scalar framing, recurrence, ordering and fallback paths remain intentional.
  ARM64 was cross-built, not benchmarked or executed on ARM hardware here.

## Verification and evidence

Each published increment was frozen separately from concurrent work and reviewed.
Final gates cover full scalar and GOEXPERIMENT=simd suites, relevant race suites,
vet, GoDoc, generated-kernel/registry checks and Linux/arm64 cross-builds. Additional
SIMD-disabled and 128/256/512-bit correctness checks are recorded. Polars fixtures
are pinned and regenerated; independent rational, malformed-frame, quota,
cancellation, cleanup and retry tests cover behavior beyond that reference.

Evidence lives under validation/fundamentals-* and validation/exact-quantiles/.
Benchmarks there are scoped measurements on a shared Linux/amd64 host, not universal
speedup or regression guarantees. Local verification is not a claim that remote
CI ran, and no default-branch merge is performed by these changes.
