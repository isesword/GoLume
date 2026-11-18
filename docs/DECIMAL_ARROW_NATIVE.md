# Explicit native Arrow Decimal128 input

Legacy entry points retain their existing opaque Decimal behavior:

- `arrowrecord.NewSource(records...)`
- `session.ScanArrow(records...)`
- either new entry point with zero-valued options

To resume exact Decimal expressions on Arrow/IPC output, opt in per source:

```go
src, err := arrowrecord.NewSourceWithOptions(
    arrowrecord.Options{NativeDecimal128: true}, records...)
// Or let the session own the retained source:
lf, err := session.ScanArrowWithOptions(
    lazy.ArrowScanOptions{NativeDecimal128: true}, records...)
```

`lazy.ArrowScanOptions` aliases `arrowrecord.Options`. `FromSource` preserves
whatever schema the supplied source already has; it does not reinterpret it.
Native and legacy sources can coexist in one session. There is no new global
engine flag, schema-only source constructor, or eager reader-draining API.
IPC readers supply RecordBatches to these same constructors. At least one
record is required, as before; a zero-row record carries an empty typed schema.

## Selection and validation

The option selects only top-level Arrow Decimal128 fields. Precision must be
1..38 and scale 0..precision. Invalid metadata is rejected even for empty or
all-NULL records. Valid coefficients must fit declared precision; non-nullable
fields cannot contain logical NULLs. Actual array type must agree with schema.
NULL coefficient bits are ignored. No rounding, rescaling, float conversion or
Decimal256 narrowing occurs.

Decimal32/64/256, dictionary-wrapped and nested Decimal forms retain their
existing mapping and admission behavior. This is not a claim that every nested
form is supported. Unsupported Decimal128 precision/scale under opt-in is an
error, not a silent return to opaque input. Use the legacy entry point when
opaque transport is intended.

One option-aware top-level field mapper feeds source schema and binding.
Recursive field mapping is unchanged. Native fields retain exact precision,
scale, nullability and an agreeing Arrow type spelling. `memory.ArrowSchema`
can export the native Decimal128 field back to its Arrow type. Native expression
binding sees precision/scale; the legacy precision-zero rejection remains.

## Ownership and budgets

The source retains immutable Arrow records, restricted to logical record rows.
Nonzero Arrow slices and physical column tails follow the existing source
contract: hidden rows do not enter coefficient/NULL validation. Native vectors
borrow the Arrow Decimal128 coefficient storage; the types are already aliases.
Only stream-local vector headers are created. Caller records/readers can be
released after successful construction. An open stream retains its owner even
after source Close; opening a new stream on a closed source is rejected.

Existing batchview slicing retains precision/scale and accounts for owned
validity normalization. An aligned slice may borrow its validity bitmap without
a reservation; a subsequent unaligned slice can require admission. Failure or
cancellation closes the affected stream and releases its owned state. A new
stream can still be opened on an unclosed source. Early Close is idempotent.

Construction has no context/manager parameter, as before. Retained Arrow input
buffers and construction-time metadata/validation are not claimed to be charged
to the query budget. The option adds no per-row coefficient-copy cache or
unbudgeted execution payload allocation.

## Boundaries unchanged

Native Decimal arithmetic retains its existing explicit exact semantics.
Decimal aggregate, Join, ordering and window admission restrictions are not
removed. Choosing native input can therefore reject an operation that previously
transported an opaque payload. This is why the conversion is explicit rather
than a default change. No SIMD performance improvement is claimed.

The former downstream verification remains true for the default ScanArrow.
The new opt-in path closes IPC re-entry without rewriting that legacy contract.

## Evidence

Adapter tests in `source/arrowrecord/decimal_test.go` cover default opaque
behavior, native slices/NULLs/metadata, empty schemas, invalid metadata/value
rejection, partial-construction cleanup, physical-tail exclusion, Decimal256
transport, nested mapping non-expansion, bounded validity admission failure,
cancellation, concurrent streams and retained-source lifetime. The exact lazy
IPC re-entry and session tests are in `lazy/decimal_arrow_test.go`.

This implementation was ported onto the published b33 candidate without the
unrelated nested-container options. The lazy test fixtures are self-contained
copies of the existing downstream Decimal fixtures. Historical candidate tests
do not establish current validation; current gate results are reported separately.
