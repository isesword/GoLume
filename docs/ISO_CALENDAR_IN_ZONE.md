# Explicit ISO calendar and UTC offset extraction

> Provenance: this document preserves the original isolated implementation-stage
> notes. Stage-local validation, opcode/layout comparisons and "unpublished" or
> next-priority statements are historical, not the status of the requested
> publication. See [date/time integration scope](DATE_TIME_INTEGRATION_SCOPE.md).

This feature is an isolated extension of the recovered civil-time / local-midnight
line. It does not recover the missing later candidate or complete item 18.

The following expressions take a UTC nanosecond Timestamp and an explicit captured
`TimeZone` handle, and return plain Int64 with input nullability:

- `expr.ISOWeekYearInZone`: local ISO week-numbering year, which can differ from the
  Gregorian `YearInZone` at New Year.
- `expr.ISOWeekInZone`: local ISO week number, 1 through 53. Pair this with
  `ISOWeekYearInZone`, not Gregorian year.
- `expr.ISOWeekdayInZone`: local weekday, Monday=1 through Sunday=7.
- `expr.UTCOffsetInZone`: the offset at that instant, in signed seconds east of UTC.
  Negative means west. This preserves historical second-level offsets, such as
  Paris's +561 seconds, without rounding to whole minutes or hours.

For example, 2021-01-01 belongs to ISO year 2020, week 53. At
2021-01-04T00:00:00Z, UTC has Monday of ISO 2021-W01 while New York still has
Sunday of ISO 2020-W53. A repeated local time during a DST fold has the same ISO
calendar parts for both instants, but may have different UTC offsets. No fold/gap
policy is needed: the input is already an unambiguous instant.

Both existing `LoadTimeZone` snapshots and `TimeZoneFromTZData` snapshots work.
This is not an inverse `CivilTimeResolver` operation and does not inherit that
resolver's raw-only provenance requirement. Names do not trigger a reload or
supply cache identity. A compiled plan retains its immutable rule snapshot;
independent handles remain distinct, even if their display labels match.

The implementation uses the existing `ZonedExtractExpr` shape, strong zone
identity, dependency/rename handling and scalar opcode. Only four logical `Op`
values are appended; old values and ordinary unzoned Unary semantics are unchanged.
Existing year/month/day/hour/minute/second extraction, UTC casts, date grouping
and local-midnight resolution are unchanged. This feature does not add calendar
addition or named-zone metadata transport.
There is no implicit local timezone and no timezone metadata on the Int64 output.

Both ordinary and selected dense execution use the existing fixed-width execution
path. Selection indexes retain the existing strictly increasing, in-range contract.
NULLs and conditional domains use the existing expression machinery. Output Int64
buffers and validity storage are charged by the existing workspace manager;
selected gather/descriptor costs are also retained. There is no new per-row owned
allocation, instruction field or spill format. Results remain borrowed until the
next evaluation or Close. Timezone setup is still plan setup rather than query
workspace accounting, and this is not a claim about total process memory.

The targeted tests cover fixed independent ISO calendar facts, UTC year boundaries,
53-week years, New York and Apia local-date differences, both fold instants, Paris
historical seconds, nanosecond limits, NULL/batch reuse, selected execution,
identity and rewrites, legacy UTC behavior, actual initial reservation failures
and retry, and pre/in-progress/final cancellation. Focused validation outcomes are
recorded separately; no unexecuted checks or full repository gate are declared
passed here. Earlier protected-file, resource, race, platform and missing-final-
baseline limitations remain unchanged. This adds neither a RANGE/GROUPS window
frame nor general nested timezone support.
