# Local wall-clock Time64 extraction

> Provenance: this document preserves the original isolated implementation-stage
> notes. Stage-local validation, opcode/layout comparisons and "unpublished" or
> next-priority statements are historical, not the status of the requested
> publication. See [date/time integration scope](DATE_TIME_INTEGRATION_SCOPE.md).

`expr.TimeOfDayInZone(timestamp, zone)` returns the local wall-clock `h:m:s.n`
encoded as **Time64 nanoseconds**, not Int64, Timestamp or Duration. The value is
`((hour * 60 + minute) * 60 + second) * 1e9 + nanosecond`, in `[0, 86400e9)`.
All arithmetic in that encoding uses int64, including on 32-bit targets.

This is not elapsed time since local midnight. On a spring DST transition,
local 03:00 yields 10,800,000,000,000 even if only two real hours passed since
midnight. No `Sub(midnight)` calculation is used. Both instants in a repeated
01:30 produce the same wall-clock value. The calendar date, timezone identity
and offset are deliberately absent from Time64; retain the Timestamp when instant
identity is needed. This cannot by itself invert DateInZone or a civil formatter.

The operation accepts Timestamp plus any valid existing captured TimeZone handle,
including loader and raw-TZif handles. It is instant-to-local extraction, so no
civil fold/gap policy or raw-only resolver restriction applies. Negative Unix
nanoseconds, Timestamp endpoints, fractional seconds at nanosecond precision and historical offset
seconds are preserved. Clock fields come from the captured Location, not from
adding an offset to int64 nanoseconds or taking a remainder of a negative epoch.

The new function guarantees the one-day range for its outputs. It does **not**
change existing engine Time64 input semantics, which accept the existing signed
physical values without a new global time-of-day range check. Existing Arrow
Time64 microseconds inputs still normalize to nanoseconds. No Timestamp, Duration,
integer cast or old calendar extraction changes meaning.

A tail logical Op reuses the existing ZonedExtractExpr shape, strong zone identity,
rewrites, dependency analysis, liveness and scalar opcode. Only the new part binds
to computed plain TypeTime64 with inherited nullability; all earlier extraction
parts still return Int64. No public struct field, instruction field or spill format
is added. Existing selected dense execution already supports the fixed-width
opcode and Time64's eight-byte payload. NULL/guard semantics, charged Int64 backing
buffers, validity and selected gather storage are retained, as is borrowed result
lifetime until next evaluation or Close. The query accounting excludes timezone
plan setup and is not an RSS limit.

Supported downstream machinery already includes Time64 collection, Arrow export
as `time64[ns]`, Arrow input, IPC, typed keys/order and columnar spill. Their actual
integration verification must use logical Time64 metadata as well as coefficients;
relabeling an Int64 result is not acceptance. This does not add nested List<TIME>,
calendar arithmetic, a new Window frame, or general native Parquet TIME reading.

In this recovered earlier baseline, Parquet TIME writing exists, but the native
reader still rejects TIME. The Arrow writer uses INT64 TIME(ns) with
`isAdjustedToUTC=false`, suitable for a timezone-free wall clock. An independent
Arrow Parquet readback can verify a sink's output without falsely claiming the
engine native-read closure. The later missing native TIME/ListTIME work and its
resource failures remain separate and unresolved in this baseline.

Targeted tests cover hardcoded real-zone wall-clock values, DST gap/fold examples,
negative epoch and Timestamp boundaries, extreme accepted offset magnitudes,
constants, NULL/batch reuse, selected execution, CSE/snapshot/rewrite, liveness,
old UTC behavior, budget admission failure/retry and cancellation. Validation
outcomes are recorded separately. This isolated continuation is not recovery of
the missing final source candidate or completion of roadmap item 18. Original
protected-file, resource, race/platform and publication limits remain in force.
