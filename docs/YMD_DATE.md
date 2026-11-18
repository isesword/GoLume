# Typed Gregorian date construction

> Provenance: this document preserves the original isolated implementation-stage
> notes. Stage-local validation, opcode/layout comparisons and "unpublished" or
> next-priority statements are historical, not the status of the requested
> publication. See [date/time integration scope](DATE_TIME_INTEGRATION_SCOPE.md).

`expr.DateFromYMD(year, month, day)` and
`expr.TryDateFromYMD(year, month, day)` construct ordinary Date32 directly from
three Int64 columns or expressions. This supports structured calendar imports
without building and parsing strings. Inputs are Gregorian year, month 1..12,
and day of month. The proleptic Gregorian calendar includes year zero and negative
years. No timezone, civil resolver, implicit numeric conversion or Timestamp
nanosecond restriction is involved.

Leap years are divisible by 4, except centuries not divisible by 400. Thus 2000,
0 and -400 admit February 29; 1900, 2100 and -100 do not. Invalid dates are
rejected, never silently rolled into the next month.

## Error order and range

For each demanded non-NULL row:
1. Month outside 1..12 or day outside 1..31 is `ErrYMDDate`.
2. Year outside the internal inclusive [-6000000, 6000000] arithmetic envelope
   is `ErrYMDDateRange`, checked before conversion to Go int.
3. Construct UTC midnight and compare its exact year/month/day back to the
   inputs. Any normalization (including illegal February 29) is `ErrYMDDate`.
4. The epoch-day coefficient must fit signed int32, else `ErrYMDDateRange`.

For example, an extreme year with month zero fails step 1. An extreme year
with February 30 fails the year protection before calendar construction.
A valid year inside the envelope with February 30 fails step 3.

The envelope does not exclude any Date32 date. Even the conservative distance
(6000000 - 1971) years at a minimum of 365 days each exceeds 2^31 days; the
negative side is farther from the 1970 epoch. Its years fit a 32-bit Go int and
UTC seconds remain below 2e14 in magnitude. UTC midnight seconds divide exactly
by 86400, also before 1970. No nanosecond conversion is used.

Strict construction returns either row error. Try turns only these two local
error classes into NULL, without modifying ISO or civil error classifications.
All child evaluation, storage, cancellation and budget errors propagate.
Wrong input types remain binding errors, including in dead branches.

## Execution and compatibility

A new `YMDDateExpr` holds Year, Month, Day and private tolerance state. Existing
public struct shapes, Op values and instruction fields stay unchanged. A new
tail scalar opcode reuses A/B/C and the existing tolerance slot. Binding,
dependency analysis, CSE, rewrites and liveness include all three ordered inputs.
Strict and Try have different CSE identities.

After type/bitmap validation, each row checks cancellation, active status and all
three validities before any demanded payload access. A NULL input yields NULL
and can hide an undemanded malformed payload, not an error from evaluating a
child. Final cancellation is checked too.

Date32 output uses existing admitted 4-byte-per-capacity-element storage plus
validity and workspace metadata. No additional per-row owned storage or new
spill encoding is introduced. Output remains borrowed until the next evaluation
or Close. The new opcode is not selection-whitelisted; filtered evaluation uses
the existing fallback, not a new selected fast path.

Tests cover calendar examples, leap centuries and negative years, invalid/range
ordering, CSE and rewrite identity, NULL/guards, batch growth/shrink, real initial
and warm reservation failures with retry, cancellation, and independent
three-input owner/storage review. Lazy Date32 IPC/Parquet/spill validation is
separate. Source alone is not evidence that tests passed.

## Scope and next priority

This adds a common structured-date input operation, not completion of item 18.
It does not recover the missing later candidate or unblock the protected original
test gate. Existing resource failures and unpublished state remain unchanged.

The next high-value window investigation is PercentRank/CumeDist with a complete
partition design. Both need total partition cardinality, and CumeDist also needs
peer-end position. A resident-only formula is not a bounded/external execution
solution: replay or partition storage, actual memory/disk accounting, one-shot
sources, cancellation and original-order restoration require explicit design.
This date increment does not broaden the deferred deep spill metadata work.
