# Date32 calendar conversions

This slice makes Date32 a usable calendar type across expressions, Lazy execution,
Arrow/Parquet input and Parquet output. It does not introduce parameterized time
units, named time zones, local civil timestamps or daylight-saving rules.

## Type and conversion contract

Date32 is a signed int32 count of days since 1970-01-01 in the proleptic Gregorian
calendar. Astronomical year numbering is used: year 0000 exists and is 1 BCE.
Leap-year rules apply before and after the epoch, including negative years.
The full physical domain spans -5877641-06-23 through +5881580-07-11.

`expr.CastTo(value, memory.TypeDate32)` supports:

- String: parse an exact ISO calendar date. Invalid syntax, impossible dates and
  dates outside the int32 day domain are errors.
- Int32/Int64: interpret the integer as epoch days, with int32 range checking.
- Timestamp: select its UTC calendar day by floor-dividing nanoseconds by
  86,400,000,000,000. Thus -1 ns is 1969-12-31, not 1970-01-01.
- Date32: preserve the physical day value and validity.

A Date32 can be cast to Int32/Int64 (epoch days), String (calendar text), or
Timestamp (UTC midnight in nanoseconds). Timestamp output is checked before
multiplication. Only day counts -106751 through 106751 have representable UTC
midnight timestamps. A nanosecond timestamp can belong to the preceding day
-106752 even though that day's midnight lies outside int64 nanoseconds. Therefore
Timestamp -> Date32 -> Timestamp is deliberately not total at the lower boundary.
This is UTC day selection, not a time-zone conversion or a claim of local-calendar
semantics.

`expr.TryCastTo` supports the same Date32-related conversions **except String
output**. Conversion syntax/range failures become NULL. Date32-to-String cannot
fail for a valid native value, so use `CastTo` for formatting. Existing numeric and
native Decimal128 TRY_CAST support remains. String -> Timestamp/Duration TRY_CAST
is not introduced by this change.

Float, unsigned integer, Date64, Time32, Time64, Duration and Decimal128 conversions
to Date32 are not inferred: unsupported pairs fail at bind time. Use explicit,
checked intermediate conversions only when their physical meaning is intended.

## ISO grammar

Every supported date has one accepted and emitted canonical spelling:

- Years 0000..9999: four digits, for example `0000-02-29`, `1970-01-01`.
- Negative years: minus and at least four digits, for example `-0001-01-01` and
  `-5877641-06-23`.
- Positive years above 9999: plus and at least five digits, for example
  `+10000-01-01` and `+5881580-07-11`.
- Extended years have no extra leading zeros. Negative zero and `+1970` are
  rejected. Month and day always have two digits.

No whitespace trimming, alternate separators, locale inference, timestamp
suffix, timezone suffix, partial date, or silent calendar normalization occurs.
For example 1900-02-29 is rejected; 2000-02-29 is accepted. This intentionally
bounded parser is not Polars' full format-string/lenient parsing API. It performs
bounded work even for an extremely long invalid token, without copying that token
into an error. Formatting and parsing cover the entire Date32 domain, not only
Go's four-digit time parsing range or the narrower nanosecond timestamp range.

## Observable change and migration

**Breaking:** Date32 -> String previously emitted a decimal day count. It now emits
ISO calendar text. For example day -1 changes from `"-1"` to `"1969-12-31"`.
There is no compatibility flag or parallel legacy formatter.

To explicitly request the physical count as text:

```go
physicalText := expr.CastTo(
    expr.CastTo(expr.Col("date"), memory.TypeInt32),
    memory.TypeString,
)
```

Both edges of this expression are supported and tested. `Series.ToAnySlice` and
`Date32Chunks` continue exposing native epoch-day integers; no implicit Go
`time.Time` allocation is added. Timestamp -> String retains its existing physical
nanosecond text behavior in this bounded slice.

Example:

```go
dates := expr.TryCastTo(expr.Col("date_text"), memory.TypeDate32)
result := frame.SelectExprs(
    dates.Alias("date"),
    expr.CastTo(dates, memory.TypeString).Alias("iso_date"),
    expr.TryCastTo(dates, memory.TypeTimestamp).Alias("utc_midnight"),
)
```

## NULL, errors and execution

NULL input produces NULL output without reading its payload. TRY_CAST suppresses
only its own conversion failures. It does not suppress child-expression errors,
invalid live string offsets/dictionary codes, memory-budget failures or
cancellation. Shared checked string-cast reads now apply to Date32, numeric and
Decimal128 conversion paths; malformed storage is never an invalid-date token.
Failed conversion payload slots are zeroed, and validity buffers remain correct
across empty, shrinking and growing batch reuse.

Strict errors identify the first demanded failing row. Selected fixed-width
execution reports rows in the dense selected domain, following existing executor
semantics. String expressions use ordinary batch/guarded evaluation; this change
does not expand the selected-input capability whitelist. CASE/IF/COALESCE/IN retain
their existing guarded-demand semantics. AND/OR remain eager with respect to
errors. Dead projections may be removed, and filter ordering must preserve
errors from already-demanded expressions. Strict Date32 -> Timestamp and
String -> Date32 are marked may-error during binding; Timestamp -> Date32 is total.

No new opcode, dtype tag, schema field, physical vector or alternate evaluation
path is introduced. Existing native vectors, registers, allocator accounting,
constant handling, guards, CSE and sink adapters are reused.

## Current temporal limitations

As of the 3ac88f9 baseline:

- Timestamp and Duration execution uses int64 nanoseconds. Arrow s/ms/us and
  Parquet ms/us timestamps are normalized with overflow checking. Original units
  are not parameterized executable dtypes as in Polars.
- Arrow Timestamp accepts UTC, Etc/UTC or an empty timezone. Named non-UTC zones
  are rejected. An empty source timezone is treated within the engine's existing
  UTC-nanosecond contract; naive/aware distinction is not retained in execution.
- Parquet local (`isAdjustedToUTC=false`) timestamps are rejected.
- Date64 remains a separate millisecond physical type; this slice does not
  redefine its existing arithmetic or add Date64 calendar casts.
- Arrow Time32/Time64 source values currently retain their physical input values,
  while native export selects ms/ns. Correct unit normalization and full time-of-day
  validation are a separate unresolved foundation task, not solved here.
- Existing timestamp text parsing/formatting and timezone policy are unchanged.

## Tests and performance interpretation

Tests include independent integer Gregorian-era text oracles, every day of a
400-year cycle across year zero, 100,000 deterministic samples across int32,
1,024 dates at each domain endpoint, explicit leap/syntax/range cases, and
big.Int timestamp floor/range checks. Additional tests exercise native/dictionary
strings, SQL NULL payloads, constants, conditional demand, selected fixed-width
execution, workspace reuse, budget fault injection and cancellation. Lazy tests
cover in-memory/Arrow/Parquet sources, Collect/stream, Parquet sink roundtrips,
scalar/SIMD dispatch settings and bounded/unbounded execution.

`BenchmarkDate32Calendar` measures parser/formatter helpers.
`BenchmarkDate32CastWorkspace` measures warmed expression evaluation on 4,096 rows.
They are diagnostic benchmarks, not source-to-sink throughput results. The new
calendar parsing/formatting and conversions use scalar Go code. SIMD-enabled
runs validate integration with existing dispatch; they are not evidence of a new
Date32 SIMD kernel or a SIMD speedup. There is no before/after speedup comparison
for conversions that did not exist before, and changed formatting semantics must
not be presented as a like-for-like baseline benchmark.
