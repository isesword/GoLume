# Typed ISO week-date construction

> Provenance: this document preserves the original isolated implementation-stage
> notes. Stage-local validation, opcode/layout comparisons and "unpublished" or
> next-priority statements are historical, not the status of the requested
> publication. See [date/time integration scope](DATE_TIME_INTEGRATION_SCOPE.md).

This isolated extension adds `expr.DateFromISOWeek(year, week, weekday)` and
`expr.TryDateFromISOWeek(year, week, weekday)`. All three inputs must be Int64.
The result is ordinary Date32 (signed 32-bit days since 1970-01-01), not a
Timestamp. There is no timezone, resolver, implicit conversion or normalization.
Pair the ISO year with the ISO week: Gregorian year is not interchangeable.

The proleptic Gregorian calendar includes year zero and negative years.
Monday is weekday 1 and Sunday is 7. ISO week 1 contains January 4; a year has
52 or 53 weeks. For example, 2020-W53-5 is 2021-01-01, whereas 2021-W53-1 is
invalid. This is distinct from existing ISO extraction, string parsing, epoch-day
literals, civil date/clock composition and calendar arithmetic.

## Validation and range

For a demanded, non-NULL row, validation order is fixed:
1. Week must be 1..53 and weekday 1..7, else `ErrISOWeekDate`.
2. ISO year must be inside the inclusive internal envelope [-6000000, 6000000],
   else `ErrISOWeekDateRange`. This check precedes conversion to Go int.
3. Construct from the Monday of January 4's week, then verify the resulting ISO
   year, week and weekday. A nonexistent week 53 is `ErrISOWeekDate`.
4. The computed epoch-day coefficient must fit int32, else
   `ErrISOWeekDateRange`.

The envelope is only arithmetic protection, not a separate restricted public
date domain. It covers every Date32 date and its ISO week-year: an ISO year
differs from its Gregorian year by at most one. Even the conservative positive
distance of (6000000 - 1971) Gregorian years, at at least 365 days per year,
exceeds 2^31 epoch days. The negative side has a larger distance from 1970.
Thus no representable Date32 ISO year lies outside this envelope. Year values
fit 32-bit Go int, and seconds at these bounds are below 2e14, safely inside
int64. The implementation uses whole UTC midnights and seconds, not nanoseconds.
Division by 86400 is exact, including before the epoch.

Independent boundary facts:
- Date32 minimum: -5877641-W26-2; the preceding Monday is outside range.
- Date32 maximum: 5881580-W28-5; the following Saturday is outside range.
- Gregorian 0000-01-01: ISO -1-W52-6, epoch day -719528.

Strict construction fails on either row error. Try turns only these two new
error classes into NULL; it does not broaden any old civil error classifier.
Child errors, invalid storage, cancellation and budget failure still propagate.
The binding phase rejects wrong types even in an inactive branch.

## Execution and compatibility

A new independent `ISOWeekDateExpr` holds Year, Week and Weekday plus private
tolerance state. Old public structs, Op values and instruction fields do not
change. A tail-appended scalar opcode uses existing A/B/C operands and tolerance
storage. Analysis, binding, CSE, rename and liveness include all three children.
CSE distinguishes strict and Try and all ordered operands.

Types and bitmap lengths are checked first. Each row checks cancellation,
then active status, then all input validities, before accessing any demanded
payload. Any NULL input yields NULL and can hide an undemanded malformed payload
or invalid calendar value, but never suppresses child expression evaluation.
Cancellation is also checked after the final row.

The existing workspace admits and charges actual Date32 data (4 bytes per
capacity element), validity and workspace metadata. No per-row owned buffers,
new spill format or uncharged fallback allocations are introduced. Borrowed
results last until the next evaluation or Close; physical owners never overwrite
a live input. The new opcode is not selection-whitelisted: filtered execution
uses the existing fallback. No fast selected execution is claimed.

Date32 remains compatible with existing Arrow, Parquet and fixed-width consumers;
consumer verification is separate from expression verification. No timestamp
range or timezone metadata is imposed on Date32.

Tests cover calendar facts, invalid domains, CSE/rewrites, three-input lifetime,
NULL/guard behavior, grow/shrink, real admission failures and retry, cancellation
and independent boundary/storage review. Validation results are recorded
separately, not presumed from test source. This work neither restores the missing
later candidate nor completes item 18 or the blocked original repository gate.
All prior resource, protected-file and unpublished-state restrictions remain.
