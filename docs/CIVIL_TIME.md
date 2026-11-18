# Explicit civil-time resolution

> Provenance: this document preserves the original isolated implementation-stage
> notes. Stage-local validation, opcode/layout comparisons and "unpublished" or
> next-priority statements are historical, not the status of the requested
> publication. See [date/time integration scope](DATE_TIME_INTEGRATION_SCOPE.md).

`CivilTimeResolver` interprets local calendar text under captured timezone rules
and returns the existing UTC-nanosecond Timestamp representation. It does not
introduce a timezone-bearing Timestamp type or change UTC casts/extraction.

## Capture rules explicitly

A resolver requires a `TimeZoneFromTZData` handle and mandatory policies. Supply
actual TZif bytes, for example from a pinned IANA distribution. No filename or
zone name is loaded by the resolver. Existing `LoadTimeZone` handles, including
`LoadTimeZone("UTC")`, are rejected because they do not retain the raw rule bytes
needed for a complete offset catalogue. A raw capture labelled "UTC" still uses
its supplied rules; the label is not a shortcut or an IANA-version guarantee.

```go
// Inside a function; tzif contains the caller's chosen TZif bytes.
zone, err := expr.TimeZoneFromTZData("America/New_York", tzif)
if err != nil {
    return err
}
resolver, err := expr.NewCivilTimeResolver(ctx, zone, expr.CivilResolveOptions{
    Fold: expr.FoldReject,
    Gap:  expr.GapReject,
})
if err != nil {
    return err
}
instant, err := resolver.Parse(ctx, "2024-01-15T09:30:00")
if err != nil {
    return err
}
_ = instant // UTC Unix nanoseconds, not a local-wall-clock integer.
parsed := expr.ParseCivilTimestamp(expr.Col("local_text"), resolver).Alias("ts")
_ = parsed
```

`lazy.ParseCivilTimestamp` and `lazy.TryParseCivilTimestamp` wrap the expression
constructors. A resolver is immutable; copies share identity and captured rules.
Separate constructions have separate identities even for equal labels/bytes.
No rule refresh or system lookup occurs during binding or row evaluation.

## Exact text grammar and policies

Input is ASCII `YYYY-MM-DDTHH:MM:SS`, optionally followed by a dot and one through
nine fractional digits. Years are four digits, including0000. Dates must be valid
Gregorian dates; hours0..23, minutes/seconds0..59. Whitespace, offsets, `Z`, timezone
abbreviations, signed/extended years, comma fractions, leap-second60,24:00 and
normalization of invalid dates are not accepted. Length is19..29 bytes. Nine-digit
fractions are exact, not rounded.

Fold policy must explicitly be `FoldReject`, `FoldEarlier` or `FoldLater`.
Earlier/Later select the minimum/maximum matching UTC instant, not a DST flag.
More than two candidates are handled. `GapReject` is the sole supported gap policy:
nonexistent local times are errors, never silently shifted by an hour or a day.
Zero-valued policies are invalid.

All matching instants participate in the fold decision before the selected
instant is checked for int64-nanosecond representability. Out-of-range candidates
are not first removed to make an ambiguous civil time appear unambiguous. Valid
civil dates outside Timestamp range therefore remain valid syntax but may fail
resolution with a range error.

## Strict, tolerant and guarded evaluation

`ParseCivilTimestamp` accepts native String input and returns Timestamp with
inherited nullability. `TryParseCivilTimestamp` always has nullable output and
converts only these row-value errors to NULL:

- `ErrCivilSyntax`: invalid text grammar or calendar fields.
- `ErrCivilGap`: no matching instant.
- `ErrCivilFold`: multiple matches under FoldReject.
- `ErrCivilRange`: the selected instant is outside Timestamp range.

`ErrCivilConfiguration`, malformed demanded String storage, resource errors and
context cancellation are not converted to NULL. The public strict `resolver.Parse`
API returns errors recognizable with `errors.Is`. NULL query input remains NULL
without parsing its payload. Invalid resolver configuration fails binding, even
in a dead branch. Existing expression short-circuit rules still apply: an entirely
unexecuted branch is skipped, while source admission may validate storage earlier.
Strict parsing declares its row-error domain to the optimizer; Try still inherits
errors of its child expression.

String dependencies retain materialized Filter/Project execution. This feature
does not add a selected-String fast path. Public expression results are borrowed
until the next Evaluate or workspace Close, not independently retained values.
Use the existing owned collection contracts when a result must outlive that scope.

## Completeness, resources and reproducibility

The catalogue includes both TZif blocks' local-time offsets and any possible
footer standard/daylight offsets. It does not guess a one-hour/day search window
or rely on ZoneBounds always advancing. Every candidate is round-tripped through
the same captured Go Location, so unused catalogue offsets do not create results.
No second transition/calendar engine is implemented.

The existing raw-capture bounds apply: at most1MiB TZif,256 types per block and a
1024-byte footer, with the existing supported versions/abbreviation/leap-record
restrictions. The catalogue has at most514 offset occurrences before deduplication.
Fixed scratch plus the exact immutable index contain at most4112 bytes of offset
entries simultaneously, apart from descriptors and retained rule storage. Invalid
or unsupported footer prefixes are configuration errors, not truncated catalogues;
unused or invalid calendar-rule suffixes can contribute harmless extra candidates
when their complete offset prefix is understood.

Construction is explicit plan setup outside Workspace accounting. It retains the
already captured rules; it does not promise a whole-plan/RSS bound or asynchronous
preemption of Go's calendar lookup. Context checks cover bounded construction and
candidate work. Per-row results use existing admitted Timestamp/validity buffers;
there is no per-row candidate slice or rule loading. Very long invalid strings
are length-rejected before making a copied parser string.

For reproducibility keep the exact TZif bytes/hash and Go version. A zone name or
TZif format version alone is not a tzdb release. Default `Cast`/`TryCast` RFC3339 and
no-offset-as-UTC behavior, `DateInZone`, six component extractors and Arrow timezone
admission remain unchanged. Date-to-local-midnight conversion, calendar-day/month
arithmetic, gap shifting and named-zone metadata transport are not supplied here.

## Verification scope

See the matching civil-time-validation candidate report for verified scenarios,
exact hashes and gate results. This bounded feature does not complete roadmap
item18 and is not a performance, native ARM execution or publication claim.
