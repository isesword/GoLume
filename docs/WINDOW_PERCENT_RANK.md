# Partition-aware PercentRank

> Integration note: scope and semantics below are retained from the reviewed
> feature candidate. Stage-local test results and unpublished-state wording
> are historical; see [integration validation](DATE_WINDOW_INTEGRATION.md) for
> this branch update and its remaining limits.

`lazy.PercentRank(alias)` adds non-null Float64 analytic output through
`WithWindows`. Within a partition of N rows, its value is zero if N=1 and
otherwise (rank-1)/(N-1). Rank is one-based and peer-aware, with gaps.
All rows, including NULL ordering keys, count toward N. Peer comparison uses
the existing ordering contract, including NULL, NaN, signed zero, multiple keys,
descending order and stable ties. Integer positions are formed before Float64
division; counts beyond 2^53 do not imply exact rational results.

Explicit ORDER BY remains required. Frame is ignored as for Rank, and Column
must be empty. Empty input preserves a typed empty result. The subsequent [CumeDist extension](CUME_DIST.md) adds inclusive peer-end
fractions with a second monotone decode cursor. PR-only execution retains its
single-cursor behavior; see that document for the additional resource boundary.

## Compatibility and scope

Logical and physical WindowFunction values are appended. Existing WindowExpr,
WindowExpression and WindowSourceOptions shapes are unchanged. Old queries that
do not use PercentRank bypass its preparation and retain existing behavior.

The initial profile is flat native scalar payload: booleans, signed/unsigned
numeric widths, floats, date/time/duration physical types, strings and binary.
Numeric/temporal/bool fields use plain encoding; string/binary fields may use
plain, view or dictionary encoding. Constant and unknown encodings are rejected.
Decimal, opaque and nested payload are not claimed. The new query validates this
profile at preparation rather than silently dropping unsupported metadata.
Existing unsupported mixed frames do not become supported.

All window outputs still use original input values, including when an output
alias replaces a partition key, ordering key or value column. For bounded paths,
a private collision-checked Float64 column carries the ratio; a private copied
expression uses zero-offset Shift. The existing future/rolling evaluator runs on
that augmented original input, and private columns are removed. No user expression
or source schema is mutated.

## Three execution paths

- Resident captures actual typed capacities through the resident sort accumulator,
  without using Stats as an allocation plan. A separately admitted result-only
  batch computes every mixed expression before any original input alias changes;
  the captured owner then receives the result vectors without copying the whole
  payload. Original capture capacities remain charged. Only PercentRank queries
  use this path; the legacy reader is unchanged.
  Resident still evaluates the complete input before its first output. It then
  emits original-order borrowed pages honoring ReadOptions.BatchRows (a zero
  request uses the PR-only output default 1024, not the global native-scan
  DefaultBatchRows of 8192), with independent admitted headers/offsets/validity. The full owner remains
  live through the last page until the next Next or Close. This reduces actual
  downstream sink copies; it is not a spill or input-bounded algorithm.
  Empty input emits one typed empty page. Cancellation at page boundaries clears
  the previous borrow and all execution owners. Fixed 8MiB sink validation remains
  recorded separately against the final-source tests; no budget has been raised.
- Ordered input is captured one partition at a time and replayed after N is known.
  The source is opened once; Stats is not treated as an exact partition count.
  The first result is delayed until the partition ends.
- External execution keeps the original chain: private ordinal, partition/order
  external sort, partition replay window, ordinal restore sort, ordinal removal.
  Output row order is the original input order, not the internal sorting order.

The new ordered strategy is reported as `ordered-partition-spool-replay`, with
spillable rather than input-independent bounded capability. Resident remains
resident and does not claim to spool. External describes its middle replay phase.

## Resource boundaries

Spool pages use admitted encoding and replay buffers. Streaming execution requires a positive finite memory limit. Without configured disk,
the complete partition's retained representation must fit the hard memory budget.
With disk, pages spill through the existing query store. Metadata still grows
with live pages; no metadata paging or arbitrary-partition O(1) guarantee is made.
Budget exhaustion remains an error, not a hidden unbounded fallback.

At a partition boundary inside an upstream batch, the unread tail still borrows
the upstream owner during replay. Encoding overlaps input ownership; restored
payload overlaps decode, and replay/output may overlap restore-sort state.
These are real simultaneous costs. Clearing vector headers is not equivalent to
freeing their owners. Close/error/cancellation must release pages, references
and leases without closing a shared query store owned elsewhere.

Independent diagnostics distinguish partition spool pages/bytes/partitions,
front-sort spill runs/bytes and restore-sort spill runs/bytes. Query aggregate
spill bytes alone are not evidence of all three phases.

## Fixed validation, not presumed success

The physical streaming resource case was frozen before running:
262144 rows, four Int64 inputs, one partition, peers of three, batch size 1024,
4MiB hard memory and 256MiB disk. Ordered and external are success targets;
the same ordered input without disk and resident materialization at 4MiB are
resource-failure cases. Failures are retained rather than tuning these numbers.
A separate 64MiB physical test copies streamed coefficients into an explicitly
charged test buffer; that is not engine Collect validation.

Lazy has separate actual Collect and streaming cases, including its fixed
32768-row variable-width profile. Small matrices cover all three paths, NULL,
NaN, ties, partitions, mixed functions, source-buffer reuse and alias replacement.
Resource/corruption/cancellation review and regression results are recorded
separately; this document does not assert they have passed.

This isolated extension does not restore the missing final candidate, implement
historical missing GROUPS/First/Last work, expand deferred deep nested
spill metadata, complete item 18, or unblock the original protected-file gate.
