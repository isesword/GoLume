# Explicit local civil formatting

> Provenance: this document preserves the original isolated implementation-stage
> notes. Stage-local validation, opcode/layout comparisons and "unpublished" or
> next-priority statements are historical, not the status of the requested
> publication. See [date/time integration scope](DATE_TIME_INTEGRATION_SCOPE.md).

`expr.FormatCivilTimestamp(value, zone)` formats a UTC nanosecond Timestamp using
an explicit captured `TimeZone`. Every non-NULL value is a plain String of exactly
29 ASCII bytes: `YYYY-MM-DDTHH:MM:SS.fffffffff`. Fractions always contain nine digits,
including trailing zeroes. There is no `Z`, numeric offset, abbreviation or label.

**This is wall-clock display text, not instant serialization.** Two instants in a
DST fold can produce identical strings. Parsing that text requires an explicit
resolver policy and cannot generally recover the original instant. For example,
New York's two 2024-11-03 01:30 instants both produce
`2024-11-03T01:30:00.000000000`. Keep the original Timestamp (or an explicit offset)
when instant identity matters. No arbitrary layout or Try formatting API is added.

Both existing loader snapshots and raw-TZif `TimeZone` handles are accepted. This
operation does not inherit `CivilTimeResolver`'s raw-only rule. Immutable captured
rules determine output; names do not cause a reload. Existing UTC Timestamp casts,
RFC3339 behavior, calendar extraction and parsing are unchanged.

All valid Timestamp/TimeZone combinations fit the four-digit year format: the
nanosecond Timestamp range combined with accepted signed-int32-second TZif offsets
(and bounded footer offsets) stays between local 1609-09-02 and 2330-05-01. A defensive
year check protects this invariant against internal corruption or future loader
extensions; there is no normal user row-range error and no new Try behavior. The
expression inherits its child's error domain and nullability. Output metadata is
plain String, not Timestamp metadata or named-zone metadata.

A separate expression node and appended physical opcode reuse the existing
instruction TimeZone handle. Existing public struct shapes and instruction size do
not change. CSE uses a distinct formatting tag and strong captured-zone identity;
renames preserve the handle. String output is not added to selected fast paths;
filtered execution uses the existing materialization fallback.

The formatter writes one fixed 29-byte stack scratch array, then calls the existing
charged byte-buffer append path. It does not allocate an intermediate formatted
heap string. Output offsets, validity, and retained byte capacity are admitted by
the existing workspace manager. Growth retains the old capacity's charge while
admitting the new capacity; this is not merely a 29-bytes-per-row estimate. Total
String offsets retain the existing UInt32 limit. A budget failure can occur after
partial internal output, but no successful result is published; retry resets data,
offsets and validity and reuses the workspace's normal failure semantics.

Each row and final publication boundary checks cancellation. NULL/inactive rows
produce no payload and do not read their Timestamp value. Demanded malformed
Timestamp storage fails instead of panicking or yielding fabricated text. Results
retain the usual borrowed lifetime until next evaluation/Close. Query accounting
is not a hard heap/RSS bound and does not move timezone plan setup into Workspace.

Targeted tests cover fixed real-zone and endpoint oracles, negative nanoseconds,
DST gaps/folds, Paris second offsets, maximum accepted offset magnitudes, constants,
NULLs, batch reuse, guards, identity/rewrite and old UTC behavior. They probe actual
initial and warm-growth reservations one by one, including all-NULL-to-valid growth,
retry, warm reuse, cancellation and malformed demanded storage. Outcomes are recorded
separately by the coordinator; this document does not claim tests already passed.
This is an isolated continuation, not recovery of the missing final candidate.
Original protected-file, resource, race, platform and publication limitations remain.
