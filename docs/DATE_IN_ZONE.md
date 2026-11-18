# Explicit local calendar dates

> Provenance: this document preserves the original isolated implementation-stage
> notes. Stage-local validation, opcode/layout comparisons and "unpublished" or
> next-priority statements are historical, not the status of the requested
> publication. See [date/time integration scope](DATE_TIME_INTEGRATION_SCOPE.md).

`expr.DateInZone(timestamp, zone)` and `lazy.DateInZone` return the calendar date
of a UTC-nanosecond Timestamp under a captured TimeZone. The output is native
Date32: Gregorian days since1970-01-01, not YYYYMMDD or a midnight Timestamp.
A valid zone is mandatory; zero handles and non-Timestamp inputs fail binding,
including in dead branches. NULL input remains NULL.

The operation maps an instant to a date. It never interprets an ambiguous civil
time, chooses a DST occurrence, or constructs midnight in the target zone.
It uses the already captured rules and does not reload system timezone data.
Old UTC Cast/TryCast and six InZone component extractors are unchanged.

## Representation and grouping

The selected local year/month/day is encoded as a timezone-free Date32 ordinal.
Thus two equal dates derived from different zones compare and group equally.
Keep a separate zone label if source-zone identity matters to the result.
The output is Plain Date32 with inherited nullability; Timestamp source metadata
is not retained as the output logical type. Arrow export is date32[day].

For local-day grouping, project DateInZone(...).Alias("local_date") before
GroupBy("local_date"). This reuses the existing Date32 grouping path; it does
not introduce an expression-valued GroupBy API or claim a performance advantage.

## Boundaries and ownership

Negative epochs and Timestamp extremes are supported. Encoding the selected
calendar date must not call UnixNano for midnight: that midnight can lie outside
the input Timestamp nanosecond range while the date remains representable.
No timezone offset is added directly to an int64 nanosecond value. UTC midnight
in Unix seconds is used only to encode the selected date, not to reinterpret it.

Date32 values occupy the existing fixed-width query buffers. Captured timezone
rules remain plan-owned and outside Workspace admission accounting, as for the
six component extractors. Cancellation and selected evaluation follow the
existing expression contracts. A mixed nested program still uses its established
fallback; this operator does not grant nested nodes a selected fast path.

The validation archive records fixed-rule DST/skipped-date/extreme tests,
independent expected dates, actual grouping/export results, final hashes,
independent review and complete gate status. This feature does not add civil-time
parsing, zone-preserving Arrow Timestamp metadata, date-to-instant conversion,
calendar arithmetic, or complete the full roadmap item18.

The separate raw-captured civil-text resolver is documented in
[CIVIL_TIME.md](CIVIL_TIME.md). It does not turn DateInZone into a local-midnight
constructor or enable calendar arithmetic.
