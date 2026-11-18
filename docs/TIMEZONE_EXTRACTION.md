# Explicit time-zone calendar extraction

> Provenance: this document preserves the original isolated implementation-stage
> notes. Stage-local validation, opcode/layout comparisons and "unpublished" or
> next-priority statements are historical, not the status of the requested
> publication. See [date/time integration scope](DATE_TIME_INTEGRATION_SCOPE.md).

The existing `Year/Month/Day/Hour/Minute/Second` APIs keep their UTC behavior.
Use a captured `TimeZone` explicitly when a Timestamp instant should be viewed
in another zone:

```go
zone, err := expr.LoadTimeZone("America/New_York")
if err != nil {
    return err
}
localHour := expr.HourInZone(expr.Col("ts"), zone).Alias("local_hour")
```

This is an API fragment inside a function that handles errors. Matching `lazy`
wrappers are available. Construct a handle once and reuse it for all six
`YearInZone`, `MonthInZone`, `DayInZone`, `HourInZone`, `MinuteInZone` and
`SecondInZone` expressions. These accept only native Timestamp (Unix nanoseconds)
and return Int64 with the input's nullability. Date32/Date64 are not implicitly
reinterpreted as instants. An invalid zero-value handle is a binding error, even
on a dead branch. NULL timestamps remain NULL without inspecting their payload.

## Captured rules and reproducibility

`LoadTimeZone(name)` invokes Go's standard zone loader once. The name must be
nonempty valid UTF-8, at most 255 bytes, without NUL; `Local` is rejected and
explicit `UTC` is allowed. The standard loader validates available named zones.
Its configured data sources and versions may differ across machines. This API
does not promise a reload of the standard library's cached search configuration.
Neither binding nor row execution performs another lookup or file read.

For a pinned rule snapshot, use `TimeZoneFromTZData(name, data)` with supplied
TZif bytes. It copies at most 1 MiB, checks supported v1/v2/v3 section bounds,
rejects leap-second records, and delegates calendar-rule interpretation to Go.
Before parsing, each section must have 1..256 local-time types; every referenced
abbreviation must terminate within 255 bytes plus NUL, with at most 64 KiB
expanded abbreviation storage per section. The tail after the final section,
including delimiters, is limited to 1024 bytes for v1/v2/v3. These engineering
bounds prevent repeated abbreviation allocation and long per-lookup POSIX-tail
scans; they are not a claim that the whole plan fits a 1 MiB query budget.
The label does not prove geographic origin or an IANA release; even a label
`UTC` uses the supplied rules. A TZif format version is not a tzdb release.
Keep the original bytes and Go/toolchain version with a reproducible job.
`Name()` returns the label, not a rule-identity or release guarantee.

The explicit-bytes preflight is not applied to the standard name loader, whose
resource and leap-record handling follow Go. It would be incorrect to claim
all underlying host rule files were inspected or certified leap-free. Both
expressions operate on POSIX instants and return second components 0 through59,
not a leap-second60 representation. Use explicit, leap-free TZif for that strict
input guarantee rather than relying on host configuration.

A handle exposes neither a mutable Location pointer nor rule buffers. Copies
share the same immutable state; each successful constructor creates a distinct
state, even for equal names/data. Compilation uses strong-reference handle
identity plus the component, not a name or a digest alone. Reusing one handle
permits CSE; different snapshots are conservatively not merged. Instructions
retain state throughout their lifetime. No cross-process serialized cache
identity or automatic rule refresh is promised. Display digests are provenance,
not the authoritative equality mechanism.

## Instants, DST and limits

Conversion is instant→local calendar fields, which is single-valued. During a
fall-back fold, two different instants may produce the same local hour/minute;
during a spring-forward gap, no instant produces the skipped local times. No
policy is needed to choose between two instants because local text is not input.
Historical second offsets and non-hour transitions are not rounded away.
Timestamp minimum/maximum values are converted without adding an offset to the
nanosecond integer, avoiding integer overflow. Negative fractional epochs use
correct second/nanosecond decomposition.

This feature does not parse ambiguous civil times, retain named-zone Timestamp
metadata end-to-end, change timestamp formatting, enable named-zone Arrow input,
change UTC defaults, add civil-day rolling windows, or change the approved
window Sum/Avg exact accumulation and final rounding semantics.

## Resources and validation

Rule loading/parsing is synchronous configuration work outside Workspace budget
and cancellation contracts. The name-loader path follows standard-library
resource behavior; only the explicit-bytes path has the 1 MiB input cap.
Immutable state is plan-owned and remains alive while handles/plans reference it.
Per-row Int64 results follow existing buffer admission, selected gathering,
guard, cancellation and ownership rules; no per-row zone loading occurs.

The timezone-extract-validation archive records exact fixture provenance,
hard-coded DST/calendar expectations, same-name/different-rule cache tests,
focused scalar/SIMD evidence, independent review and full-gate results. Requested
SIMD does not imply a dedicated timezone SIMD kernel or speedup. Local tests do
not imply hosted CI, native ARM execution, publication or full roadmap item18
completion.

For the separate explicit civil-text-to-instant operation, see
[CIVIL_TIME.md](CIVIL_TIME.md). That resolver requires raw `TimeZoneFromTZData`
capture and mandatory fold/gap policies; it does not change these single-valued
instant-to-calendar extractors or make ordinary `LoadTimeZone` handles resolvers.
