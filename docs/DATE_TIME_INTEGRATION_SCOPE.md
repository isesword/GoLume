# Date/time integration scope

This integration selectively ports the date/time portion of the archived
civil-time/standard-deviation candidate onto the published source baseline
`3313770e0af1c24e89e2670ac3b0d40302365cdf`. It is not an overwrite of the
archived cumulative tree and does not restore the missing later candidate.

The included prerequisites are captured TimeZone rules, explicit civil-time
resolution and parsing, and zoned instant extraction. The added operations cover
local Date32 midnight, ISO calendar fields and UTC offset, civil formatting,
Time64 wall-clock extraction, date/clock composition, ISO week-date construction,
and Gregorian year/month/day construction. Default UTC casts and extraction
remain unchanged.

The shared expression changes add only these date/time node cases to dependency
analysis, binding, compilation, evaluation, structural identity, liveness and
optimizer renaming. Existing published scalar opcode values remain unchanged;
the new private date/time opcodes are appended. Their numeric values are not
copied from the cumulative candidate, which also contained unrelated additions.
The private instruction adds resolver, tolerance and timezone fields relative to
the published baseline. Statements in earlier per-operation notes about reusing
an existing instruction slot refer to their own intermediate parent, not this
published baseline. Existing public structure shapes remain unchanged.

This port does not include new Decimal rounding/division-rounding, ListGet,
StructGet/StructFrom or recursive/nested container features from the cumulative
archive. It preserves published Decimal and ListLength logic. Date/time lazy
constructors that formerly shared frame.go are placed in timezone.go; this does
not change their signatures.

The per-operation documents retain historical development-stage context.
Their old "unpublished", next-priority and validation statements are not a
current publication status or new test result. Integration validation and remote
publication require separate verification of the final combined source.
Static review alone does not establish test success. Full-repository/protected
source restrictions and retained resource/race/platform limitations remain;
focused checks cannot clear those gates or establish completion of item 18.
