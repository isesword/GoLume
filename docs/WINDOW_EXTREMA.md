# Rolling Min/Max: native result types

`lazy.RollingMin` and `lazy.RollingMax` preserve the input numeric type:
Int8/16/32/64, Uint8/16/32/64, and Float32/64. The physical result vector now
matches that schema in resident, ordered-state, and external-sort execution.
There is no widened result representation or compatibility fallback.

Previously Int8/16 results were written into Int32 vectors, Uint8/16 into
Uint32 vectors, and Float32 into Float64 vectors while the schema still
advertised the original type. Collect or the external restore sort could
therefore fail when consuming the result. Consumers must use the schema's
native-width vector. Explicitly cast the result when a wider type is wanted.

The direct physical constructors now reject Min/Max output fields whose name,
type, or nullability disagrees with the expression. Supply the expression's
alias, the exact input type, and `Nullable: true`. Even a non-nullable input
can have an empty ROWS frame.

## Value semantics

- NULL input rows are ignored. Empty and all-NULL frames return NULL.
- Integer extrema compare integers directly, including the full Int64 and
  Uint64 domains. They do not round through Float64.
- Floating-point NaN sorts above every non-NaN value. Min chooses a non-NaN
  value when one is present; Max chooses NaN when any is present.
- Equal values keep the earliest value still in the frame. This includes
  positive and negative zero. NaN payload preservation is not a public
  guarantee.
- Resident execution supports the existing ROWS frames. Bounded execution
  additionally supports all valid combinations of finite PRECEDING, CURRENT
  ROW, and FOLLOWING endpoints. Existing UNBOUNDED PRECEDING to CURRENT ROW
  frames are preserved. See [finite ROWS execution](WINDOW_BOUNDED_ROWS.md).

## Storage and lifetime

Extrema share the native-width allocation and scalar write helpers already
used by Shift/Diff and directional fill. Resident execution allocates each
result once and writes partition results into disjoint original-row slots.
Ordered execution allocates each batch's output before processing rows; its
rolling state retains numeric scalar copies, not pointers into child batches.
Both paths detach a replaced alias before allocating its output, so writing
the result does not mutate its input vector or validity bitmap. Output and
rolling-state memory remain covered by the existing window reservations and
stream cleanup contract.

Alias removal clears every physical column map, including List/Struct maps.
The resident path releases any replaced owned Arrow payload gathered by
`takeRows`; ordered and look-ahead paths only detach borrowed bindings and
leave release responsibility with the input owner.

This change does not alter Sum/Avg accumulation, RollingCount validity, Arrow
boolean normalization, or the accepted input domain of Min/Max.

## Regression coverage

The tests cover all ten numeric types and three execution strategies, actual
physical vector bindings and public Series values, cross-batch partitions,
empty/all-NULL frames, alias replacement, exact 64-bit endpoints, NaN,
infinities, signed zero, invalid physical output fields, cancellation, and
budget cleanup. Run `go test ./physical ./lazy -run WindowExtrema` for the
focused suite, and use `scripts/validate.sh` for the repository gates.

Missing external fixtures must be reported as validation limits, not replaced
with fabricated data or counted as passing tests.
