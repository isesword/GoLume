# Empty offsets at clone and spill boundaries

`StringViewVector` permits zero rows with no offsets, including the child of a
valid empty `List<String>` or an all-NULL list column. List parent offsets still
require `rows + 1` entries. A missing child vector is not an empty list.

`CloneBatchRange` owns normalized offsets, including a `{0}` child sentinel for
zero child rows. It preserves parent/element validity, so NULL and an empty
non-NULL list remain distinct. A raw contiguous list clone preserves physical
values hidden under NULL parents; the existing nonempty Struct row-gather path
may elide them while preserving logical NULLs. A zero-length Struct clone keeps
its child bindings, types and dictionary metadata instead of dropping them.

`EstimateBatchRangeBytes` includes the owned sentinel. Endpoint and validity
bounds are checked without allocating; string/list sizing stays constant-time
per column because admission callers repeatedly probe overlapping ranges.
Cloning also checks selected string/list interior offsets before copying.
Estimation is not full input validation: malformed interior offsets can receive
an estimate and then fail cloning. Callers must release admission on failure.
Existing nonempty Struct gather semantics and general malformed-input validation
outside these range helpers are unchanged.

The columnar encoder accepts missing offsets only for zero-row strings and list
children, writing the existing wire-format sentinel and charging its four bytes
before allocation. No wire version change is required. Missing list parent or
dictionary sentinels, nonempty missing children, invalid physical offsets and
non-nullability violations remain errors. Decoder offset-count checks remain
strict, including zero child rows requiring exactly one wire offset. Neither
cloning nor encoding mutates source offsets or borrows the copied sentinel.

The production callers covered by regression tests are Exchange cloning,
future-window lookahead and one-pass TopK snapshots. Tests cover nil and explicit
sentinels, zero-length ranges, bitmap boundaries, Struct<List>, nonzero bases,
NULL masks, exact/denied codec budgets, corrupted decode cleanup, consumer EOF,
early Close, cancellation, upstream failure and every reservation-denial point.
