# Explicit local-calendar day offsets (N35 isolated candidate)

`expr.AddCalendarDays(timestamp, days, resolver)` and its Lazy spelling return
Timestamp after adding signed Int64 days to the local Gregorian date, preserving
hour, minute, second and nanosecond. This is not `days * 24 hours`: crossing a DST
change can produce a 23-hour, 25-hour or half-hour-shifted elapsed interval.
The captured resolver supplies both the extraction rules and the target fold/gap
policy; no system lookup occurs per row.

Zero days returns the exact original instant after context/configuration checks.
Thus adding zero during a fold does not reject or choose a different occurrence.
Nonzero shifts use the existing resolver's candidate selection before Timestamp
range checking. GapReject does not shift nonexistent dates or clocks forward.
Huge Int64 offsets are checked before addition, with safe Date32 day arithmetic;
final values remain the existing int64-nanosecond Timestamp.

Strict evaluation inherits operand nullability. Try always permits NULL and only
converts civil range/gap/fold errors to NULL; child, configuration, storage,
budget and cancellation failures propagate. Inputs must be Timestamp and Int64;
Duration is deliberately not accepted as a number of calendar days. Both NULL
operands and conditional demand masks retain existing evaluation semantics.

This introduces no month/year interval policy, dynamic timezone column, new
nested temporal transport or rolling window. Existing UTC operations are unchanged.
Resolver setup remains plan-owned; output and workspace follow existing charged,
borrowed expression lifetimes. Existing opcodes are unchanged; one opcode is appended.

Validation is recorded separately under the N35 checkpoint. Focused tests do not
close historic full-gate/protected-file, race, platform or List<TIME> resource
failures. N35 is based on N33 and is not an N34 integration or publication.
