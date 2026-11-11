# Arrow Boolean streams and windows

Arrow RecordBatch Boolean fields use the existing engine `TypeBool` contract:
`Uint32Vector` values containing 0 or 1, with a separate parent validity bitmap.
`false` is a valid value. NULL payload slots are emitted as zero and remain NULL.
The adapter respects both Arrow array bit offsets and the record's logical row
count, including zero-length records and hidden tails of longer arrays.

## Bounded conversion

The retained source keeps packed Arrow Boolean arrays. Each `Next` converts
only the rows selected for that output batch, rather than caching a widened
value array for the whole record. Projection excludes unselected Boolean
columns from both decoding and scan reservations. Independent streams have
independent decoded value buffers; source records remain alive until every
open stream closes, reaches EOF, or fails, even after source `Close`.

- `BatchRows` limits output rows.
- `BatchBytes` and the scan memory manager account for four decoded bytes per
  Boolean row, plus any newly allocated validity or other column metadata.
- Payload hard/soft limits count the native output representation, not the
  full packed Arrow backing. A row may exceed the soft target but never the
  positive hard limit.
- EOF, cancellation, failures, and `Close` release the output reservation.
  Stream output remains borrowed until the next read or close; clone it to
  retain it independently.

Source-owned Arrow buffers and the adapter's existing cached validity and
nested-column metadata are not charged as new scan buffers. Existing struct
Boolean child normalization remains source-wide. This is bounded top-level
value decoding, not a hard heap/RSS cap or a new nested import implementation.
Non-Boolean opaque, dictionary, and extension arrays retain their existing
representation and sizing rules.

## Window behavior and error handling

Public lazy rolling Count accepts imported Boolean inputs in resident,
ordered-state, and external-sort modes. Count uses validity, so both `true`
and `false` count. Alias replacement reads the original source for all
expressions and leaves only the new output binding. Original Boolean columns
can still be collected and exported to Arrow after the window.

The direct physical window API already supports Boolean partition/order keys;
regressions cover NULL/false/true partitions, peer ranking, numeric directional
fills, and Count aliases across records and small batches. The public logical
window API still rejects Boolean partition/order keys. Numeric-only
Shift/Diff/Fill/Min/Max payload restrictions and Sum/Avg arithmetic are unchanged.

Resident materialization's typed-opaque fallback processes every remaining
column and increments each row instead of returning early. Later column and
child-stream errors propagate, and retained Arrow slices are released on all
materialization/concatenation failure paths. Lookahead sizing recognizes
Arrow's typed-nil absent dictionary data, still counts real dictionaries, and
returns an error for nil input data rather than panicking.

Tests use generated in-memory Arrow data, checked allocators, explicit memory
budgets, independent streams, repeated execution, and all three physical
window modes. The original Parquet fixture is not synthesized or downloaded;
fixture-dependent test coverage must be reported separately when it is absent.
