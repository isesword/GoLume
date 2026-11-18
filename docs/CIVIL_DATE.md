# Explicit Date32 local midnight

> Provenance: this document preserves the original isolated implementation-stage
> notes. Stage-local validation, opcode/layout comparisons and "unpublished" or
> next-priority statements are historical, not the status of the requested
> publication. See [date/time integration scope](DATE_TIME_INTEGRATION_SCOPE.md).

This is an isolated extension of the earlier 1,894-file civil-time candidate,
not a recovery of the later 2,206-file candidate or a claim that item 18 is complete.

`CivilTimeResolver.ResolveDate(ctx, days)` resolves a Date32 epoch-day ordinal
into the UTC nanosecond Timestamp corresponding to local 00:00:00, using the
resolver's captured TZif bytes and mandatory fold/gap policies. `expr.DateAtMidnight`
and `expr.TryDateAtMidnight` apply the operation to Date32 columns. Their input is
not a Timestamp or an untyped integer. The result is ordinary Timestamp, not a
new named-zone type or a Date32 value with timezone metadata.

The operation is different from both `DateInZone(timestamp, zone)` (instant to
local calendar date) and the existing Date32-to-Timestamp cast (UTC midnight).
Neither old operation changes. It is also not calendar day/month addition:
there is no time-of-day preservation, rolling interval or end-of-month policy.

All int32 Date32 ordinals are supported as input dates. The implementation uses
64-bit seconds to encode civil fields, then resolves every matching captured
offset and checks the local fields against the actual Location. It does not
multiply days into nanoseconds or assume a local day lasts 24 hours. Midnight may
be nonexistent, ambiguous or outside the Timestamp range. Apia's skipped
2011-12-30 is an error, not silently advanced to the next day. FoldReject,
FoldEarlier and FoldLater retain the existing resolver semantics. Candidate
selection precedes nanosecond range checking, including for dates near the
Timestamp endpoints. Try converts only civil row errors to NULL; configuration,
child evaluation, demanded malformed storage, budget and context errors propagate.

The input NULL bitmap and conditional execution domain are respected; unneeded
rows are not resolved. Strict output inherits input nullability; Try output is
nullable. Strict binding declares its intrinsic error domain so optimizer
rewrites cannot filter away a required midnight error. Both modes retain child
error domains. Resolver copies share CSE identity, independently created resolvers
do not, and text parsing and midnight resolution use distinct operation identities.

The new expression has a separate node and tail opcode. No existing public
structure changes shape, no existing opcode is renumbered, and no instruction
field or spill format is added. The instruction reuses the existing strong
resolver handle and tolerant flag. Evaluation uses the existing charged Int64
output and validity buffers, with no per-row string formatting. Resolver setup
remains plan setup outside the query budget, as in CIVIL_TIME.md. Output is still
borrowed according to Workspace's existing lifetime, not newly owned.

Selection fast paths are not enabled by this change: filtered execution uses the
existing materialization fallback. Ordinary Timestamp downstream consumers and
transport are reused; this does not extend arbitrary nested TIME/Decimal support.

The tests in civil_date*_test.go and optimizer/civil_date_test.go target this
scope, existing civil behavior, NULL/guards, compact constants, error classes,
identity/renaming, actual initial admission failures/retry and cancellation.
They do not substitute for the protected-file-blocked whole repository gate,
previous resource failures, unfinished race/platform checks or final-baseline
recovery. Validation outcomes must be recorded separately by the coordinator;
this document does not assert that unexecuted tests passed.
