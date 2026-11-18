# Typed local date and wall-clock composition

> Provenance: this document preserves the original isolated implementation-stage
> notes. Stage-local validation, opcode/layout comparisons and "unpublished" or
> next-priority statements are historical, not the status of the requested
> publication. See [date/time integration scope](DATE_TIME_INTEGRATION_SCOPE.md).

`expr.ComposeCivilTimestamp(date, clock, resolver)` combines an explicit Date32
calendar date and Time64 nanosecond wall clock using a captured CivilTimeResolver.
`TryComposeCivilTimestamp` has the same inputs and makes only civil row errors
NULL. Standalone `resolver.ResolveDateTime(ctx, days, clockNanos)` exposes the same
resolution without building an expression. These are not implicit UTC casts.

Date32 is a signed epoch-day ordinal. Clock must satisfy `0 <= clock < 86400e9`;
negative values and 24:00 are rejected with `ErrCivilClock`, never wrapped or
carried into another date. This restriction is local to this operation and does
not tighten the existing engine's general Time64 input domain. No integer column
or Time32 value is accepted as an implicit nanosecond clock.

The date is decoded using safe 64-bit seconds, then clock components are resolved
as local civil fields. The implementation neither adds an elapsed duration to
local midnight nor converts days directly into nanoseconds. Such shortcuts are
wrong across DST gaps/folds and near Timestamp bounds. Existing resolver rules
remain authoritative: raw-captured TZif provenance, mandatory fold policy and
GapReject. Loader TimeZone handles used for instant extraction cannot directly
replace a CivilTimeResolver.

The resolver validates nil context, cancellation, configuration and clock domain
in that order. It then checks all matching captured offsets against local fields,
selects according to FoldReject/Earlier/Later, and only then checks the chosen
Timestamp's nanosecond range. Extreme Date32 values are valid dates but can return
ErrCivilRange. No string parsing or formatting is used to compose the fields.

Strict outputs inherit either input's nullability; Try outputs are nullable.
Either NULL input makes the row NULL before either payload is read. Active-domain
masking and demanded-storage validation are retained. This does not introduce
lazy evaluation between the two child expressions: a failing child cast still
fails, even if the other child eventually yields NULL. Try never swallows child,
configuration, storage, budget or cancellation errors. Its row-error policy adds
ErrCivilClock locally to the existing civil row errors without changing the old
civilRowError helper. Strict binding declares intrinsic MayError; both modes
inherit child error domains.

The new CivilComposeExpr has two ordered children and an independent tail opcode.
It reuses instruction A/B and the existing resolver/tolerant slots; existing public
struct shapes, opcode values and instruction size do not change. Dependency,
rewrite, CSE and liveness track both inputs. Strong resolver identity, both ordered
children and mode distinguish CSE entries. No new selection whitelist entry is
added; filtered queries use the existing materialization fallback.

The output is ordinary UTC-nanosecond Timestamp with computed metadata, backed by
the existing charged Int64 vector and validity bitmap. There is no new buffer,
spill format or per-row string allocation. Resolver setup remains outside query
workspace accounting, and borrowed-result lifetime remains unchanged. Budget
failures use the existing retry contract; cancellation/error closes the workspace.

DateInZone plus TimeOfDayInZone supplies the date and clock fields of an instant,
but composition is a **policy-governed reverse**, not a universal round trip. Two
fold instants have identical local fields. FoldReject errors; Earlier/Later chooses
one and cannot recover which original instant was discarded. Preserve the original
Timestamp when instant identity matters. Existing string parsing, midnight
resolution, calendar arithmetic and UTC casts are unchanged.

Timestamp-only downstream results can reuse existing Arrow/IPC and native Parquet
readback, order and spill. Keeping original TIME columns in a Parquet result still
hits this earlier baseline's native TIME-reader rejection; that separate capability
is not repaired here. No List<TIME> or general nested support is claimed.

Tests target date/clock typing, independent timezone/endpoint oracles, clock domain,
fold/gap/range, NULL/guards, dual-child liveness/CSE/rewrite, child-error propagation,
admission failure/retry and cancellation. The coordinator records actual outcomes;
this document does not declare unexecuted validation passed. This is an isolated
continuation, not recovery of the missing final candidate or completion of item 18.
All previous protected-file, resource, race/platform and publication limits remain.
