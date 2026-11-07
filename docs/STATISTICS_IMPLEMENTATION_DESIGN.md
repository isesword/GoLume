# Statistics implementation status and remaining extensions

The initial separate median/quantile design has now been implemented as an
additive configured aggregation stage. [EXACT_QUANTILES.md](EXACT_QUANTILES.md)
is the current API, semantic, resource and execution contract. It documents the
shared serial tuple-sort fallback and its bounded-resource limitations; this is
not a claim of specialized parallel/SIMD quantile execution.

## Implemented fundamentals

- [STATISTICS_STATES.md](STATISTICS_STATES.md): typed sample/population variance
  and standard deviation, fixed-width and owned string/binary first/last,
  resident/partitioned/sorted execution and V4/V5 spill. Moment states use exact integer anchors and scaled
  deviations, rather than the simple raw-M2 prototype originally proposed here.
  Endpoints carry stable source ordinals. String/binary payloads are owned and
  budgeted separately. Arbitrary query-level ddof remains unsupported.
- [NATIVE_AGGREGATION.md](NATIVE_AGGREGATION.md) and
  [MULTI_COLUMN_AGGREGATION.md](MULTI_COLUMN_AGGREGATION.md): independent typed
  lanes, global aggregation, native outputs and existing compatibility limits.
  The old statement that all aggregates share one Int64 column is obsolete.
- [ORDERED_TRANSFORMS.md](ORDERED_TRANSFORMS.md): numeric shift/diff and
  forward/backward filling, with admitted future lookahead and cross-batch state,
  plus FillNull/FillNaN value helpers. Nonnumeric shifting and directional filling
  remain outside the current typed window implementation.
- Exact `n_unique` is implemented in this stage and documented in
  [EXACT_DISTINCT.md](EXACT_DISTINCT.md), including its tuple-sort fallback and
  budget limits. Exact quantiles share its sorter while preserving duplicates.

- Exact numeric Median/Quantile use typed native ordering, exact uint64 rank
  calculations, six interpolation modes, nullable floating outputs and one-pass
  tuple ordering. Multiple requested quantiles share each input lane. Empty
  group existence, source ordinals and duplicate multiplicity are preserved.
  Existing Query/Aggregate positional layouts and V1–V5 codecs are unchanged.

## Remaining extensions, not part of this implementation

- Arbitrary query-level ddof, nonnumeric shift/fill and rolling/window quantiles.
- Dtype-preserving integer rank-selection APIs (quantile outputs are floating).
- SIMD or parallel sorting admission, requiring independent architecture gates.
- More efficient shared selection for small resident groups or many requested
  probabilities. Current correctness uses one common exact ordering path.
- Spillable run manifests for cases where admitted in-memory segment metadata
  is the limiting resource. Current behavior is explicit budget failure, never
  unaccounted growth.

The original design's product choices are resolved in EXACT_QUANTILES.md:
AggConfigured/PrepareConfigured are additive; Float32 input returns Float32 and
other numeric input returns Float64; nearest/lower/higher/midpoint/linear/
equiprobable are explicitly defined. No additional user decision is needed for
these implemented, reversible API choices. Future expansions remain separate.
