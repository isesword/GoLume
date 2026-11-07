# Exact distinct aggregation

`expr.NUnique(column, alias)` and `lazy.NUnique(column, alias)` count distinct
values exactly. Repeated outputs over the same input share one lane. Use
`ExecuteTyped` or a DataFrame for materialized results.

## Semantics

- Output is non-nullable UInt64, matching GoLume's other counts. Polars 1.35.2
  defaults to UInt32; this output-width difference is deliberate.
- Empty global input produces one row containing zero. Empty keyed input has no
  groups. NULL counts as one distinct value, so an all-NULL group returns one.
- All NaN bit patterns compare equal, and positive/negative zero compare equal.
- Native integers preserve all bits, including UInt64 values above MaxInt64.
- Supported inputs are native numeric, boolean, temporal, string and binary
  columns. Dictionary strings compare resolved bytes, including across changing
  codebooks. Empty bytes, embedded zero bytes and NULL remain distinct.
- Unsupported nested/opaque distinct inputs fail preparation. Ordinary
  COUNT(column) in a mixed query still supports validity-only inputs.

## Execution and budgets

Queries containing NUnique currently use one shared exact tuple-sort fallback,
including when the selected policy is resident, parallel or sorted. This is not
an accelerated parallel distinct implementation. `Plan().Strategy.Reason` and
stream diagnostics disclose the effective algorithm. Existing queries without
NUnique retain their specialized operators and aggregate-record layout.

Each selected source row is visited once. Canonical group bytes prefix two
record families: ordinary numeric/statistical input records with unique logical
source ordinals, and distinct `(lane, canonical value)` records. The sorter
retains ordinary row multiplicity while globally deduplicating distinct tuples
across every run. One fixed-size group accumulator consumes the sorted output.
First/Last keep source order even when the source pipeline offers relaxed order.
No source replay, unbounded distinct set, sum of overlapping local counts, or
unbounded result join is used. Sorting values within a group handles a single
high-cardinality hot group.

Mixed queries carry ordinary input rows through sorting rather than reducing
all of them into partial states first. This trades extra sort/disk work for a
simple exact one-pass implementation. Distinct-only input likewise includes an
ordinal record per row. Performance measurements are descriptive, not a claim
that this fallback is faster than specialized ordinary aggregation.

Reservations cover tuple buffers, row descriptors, growth overlap, merge
buffers, retained keys, one-group states and output vectors before allocation.
Configured disk quotas apply to intermediate runs too. Spill storage is shared
with the query but owned sorter objects are deleted on EOF/Close. Memory-only
policy never spills, even if the engine has a disk quota configured. Without
spill, growing exact state must fit the query memory budget; sorted group keys
alone do not make unbounded distinct cardinality constant-memory.

Segment handle metadata is budgeted. The current SpillManager represents each
physical segment with an in-memory handle, so extremely many segments can reach
the memory cap even when disk quota remains. This fails admission rather than
silently exceeding the budget. A persistent on-disk handle manifest would be a
separate future improvement. Memory admission is an owned-data accounting
contract, not a strict process RSS cap; source-owned immutable buffers and Go
runtime overhead follow the repository's existing memory contract.

All string/binary values are copied before input reuse. Exact counts are checked
for UInt64 overflow. Cancellation, malformed tuple records, exhausted quota and
allocation failures terminate the stream and run cleanup. Failed object deletion
is reported and can be retried by Close.

## SIMD

The existing compiled predicate dispatch can use the configured kernels. Exact
variable-length tuple ordering/dedup remains scalar. No inaccurate float SIMD
comparison or unmeasured architecture gate is enabled for this operation.
