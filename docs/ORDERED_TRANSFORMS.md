# Ordered shift, difference and value filling

This increment adds numeric `lazy.Shift(column, periods, alias)` and
`lazy.Diff(column, periods, alias)` expressions for `WithWindows`. An explicit
`NewWindow(partitionKeys, ordering)` is required. Results retain original input
row order, length and partition membership; equal ordering keys retain stable
input order. These are window operations, not per-batch scalar functions.

Positive periods read earlier rows, negative periods read later rows, and zero
reads the current row. Exposed positions are NULL. Diff is current minus shifted
value and is NULL if either endpoint is NULL. Shift preserves all ten numeric
physical widths, including UInt64 values above 2^53 and Float32. Diff preserves
signed and floating widths; UInt8/16/32 promote to Int16/32/64. UInt64 diff uses
Int64, with an endpoint above MaxInt64 becoming NULL, matching pinned Polars
observations even when the mathematical difference would fit. Signed subtraction
wraps at its output width. NaNs remain valid values, not NULL sentinels.

The existing four-field logical and physical window expression structs are
unchanged. Constructors encode the offset as an equal, finite ROWS start/end
bound. Minimum int64 period is rejected, because its magnitude is not representable.
Nonnumeric shift/diff inputs are currently rejected explicitly.

## Execution limits

- Resident windows support both positive and negative periods.
- Ordered bounded and external-sort windows support both directions. Positive
  and zero periods use the existing ordered state.
  State is a pre-admitted ring of `periods + 1` numeric cells per expression;
  state persists across batches and resets at partition boundaries. Huge offsets
  fail admission before ring allocation. Offset zero uses one cell.
- Negative periods use a delayed, owned input queue. Payload and node metadata
  are reserved before `CloneBatchRange` copies data. Admission recursively counts
  native/Arrow child descriptors and schema fields, with a conservative four-times
  payload envelope for append capacity and temporary growth overlap. Copied setup
  descriptors and hidden output headers are also admitted before allocation. This
  is deliberately conservative rather than an exact retained-byte measurement. The queue retains at most
  the largest future offset plus two child batches (in addition to the child
  stream's own current buffer), and rejects insufficient memory without a silent
  full-input materialization. At EOF or a partition boundary, absent future
  values become NULL. Cancellation/error/Close release retained batches.
- Future outputs are computed from original input before requested aliases can
  overwrite columns, then carried in collision-free private columns to the
  ordinary evaluator. Those columns never appear in output. No replay or
  concurrent-reader assumption is used; past and future expressions may coexist.
- Existing ranking and aggregate-window behavior is unchanged. This increment
  does not implement forward/backward fill, string shifting, or arbitrary
  following-frame bounded aggregates.

## Value fills

`expr.FillNull(value, replacement)` lowers to Coalesce. It changes NULL only,
not NaN. `expr.FillNaN(value, replacement)` lowers to guarded If expressions and
changes NaN only, retaining NULL. Non-floating comparable values pass through
FillNaN unchanged. Both helpers retain the existing Coalesce/If binding policy;
they are convenience constructors, not a new type-preserving cast policy.
Use a replacement explicitly cast to the input dtype when preserving its width
is required. An unused replacement branch is not evaluated; child errors retain
the existing conditional-expression semantics. The expression engine's existing
conditional and equality kernels handle execution; no new SIMD gate is claimed.

Tests cover ten numeric dtypes, nulls, zero/past/future offsets, source-batch and
partition boundaries, original-order restoration, resident/ordered/external
agreement for supported offsets, UInt64 endpoint checks, huge-offset budget
failure and cleanup, and NULL/NaN fill separation. Additional future-path tests cover hidden-name collisions, alias replacement,
mixed past/future windows, EOF tails, small batches, cancellation, input failure,
admission failure and retained-state cleanup.
