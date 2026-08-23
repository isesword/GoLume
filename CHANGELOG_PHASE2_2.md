# Phase 2.2 Changelog

## Adaptive aggregation

- Added load-factor-driven growth to int32, uint32, uint64, fixed 128-bit, and plain-string aggregation tables.
- Cardinality hints now control initial sizing only.
- Added `hashagg.TableOptions` and coarse-grained `ReserveFunc` for growth memory accounting.
- Added budget-aware grow error propagation through Engine hot paths.

## NULL semantics

- Added SQL NULL group support for single-key aggregation.
- Added validity-mask identity for fixed two-column direct/bitmap/radix aggregation.
- Added NULL materialization through `engine.KeyPart.Valid` / `engine.Group.KeyValid`.
- Added nullable single-key radix handling without conflating NULL with zero.

## Radix robustness

- Added `radix.ErrLocalTableFull` instead of local-table panic/full-loop behavior.
- Added adaptive global fallback using already compacted selected rows when cardinality estimates are badly wrong.
- Fallback workspace is retained and budget-accounted like other Prepared scratch.

## Join

- Hardened int32 build-table capacity against underestimated cardinality hints.
- Added dictionary-string hash join.
- Added same-domain code fast path.
- Added dictionary-level normalization for different domains.
- Added duplicate dictionary value canonicalization and NULL no-match semantics.
- Added fixed 128-bit composite inner/left/semi/anti hash join with duplicate build-row chains and NULL no-match semantics.
- Composite join build capacity now grows from actual build rows when the hint is underestimated.

## Aggregates

- Added reusable `aggregate.State` lifecycle.
- Added SUM, COUNT(*), COUNT(value), MIN, MAX, AVG built-ins.
- Added compiled `aggregate.Layout` + SoA `Int64Vector` state and integrated it into specialized hash aggregation tables.
- Direct/bitmap group-by now supports SUM, COUNT(*), COUNT(value), MIN, MAX, and AVG combinations on one int64 value column.
- Added SQL NULL result validity for SUM/MIN/MAX/AVG and regression tests for all-NULL groups.
- Added COUNT(*)-only execution without a synthetic value column.
- Added grow/rehash tests proving advanced aggregate state and validity survive capacity changes.
- Kept radix explicitly specialized to non-NULL SUM+COUNT(*) and reject incompatible forced plans.

## Engineering quality

- Added package documentation for core public packages.
- Added GoDoc to all exported production declarations and enforce name-prefix/package-doc conventions.
- Added `tools/checkgodoc` static verification tool.
- Removed stale pre-NULL materialization helper.
- Added `scripts/validate.sh` and updated test documentation.
