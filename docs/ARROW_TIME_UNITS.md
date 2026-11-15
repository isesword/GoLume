# Arrow Time32/Time64 canonical input units

The native engine represents Time32 in milliseconds and Time64 in nanoseconds.
Arrow output uses those same units. On Arrow record input, legal Time32 seconds
are multiplied exactly by 1,000; milliseconds are unchanged. Legal Time64
microseconds are multiplied exactly by 1,000; nanoseconds are unchanged.
No division, rounding or precision loss is introduced. Negative values preserve
their sign; this repair does not add a separate time-of-day range restriction.

## Errors and NULLs

The conversion is checked against the native Int32/Int64 storage range. A valid
visible value whose scaled coefficient overflows causes source construction to
fail with its column/row context, rather than wrap or clamp. Illegal Time32/64
unit metadata is rejected, including empty or all-NULL arrays. Payload beneath
a NULL value or NULL Struct parent is not converted and cannot trigger overflow;
this does not excuse malformed storage or invalid metadata.

Source construction precedes lazy expression evaluation. A downstream filter,
If/Case/Coalesce branch or unused projection does not suppress a source-admission
error. This is not guard-dependent conversion. Existing logical-prefix slicing
excludes physical tail rows; Arrow slices retain their actual offset semantics.

## Metadata, scope and lifecycle

`Field.ArrowType` retains the original Arrow spelling as source provenance,
following its existing contract. It does not change the internal canonical unit
of a native Time32/Time64 vector. Native export derives units from the logical
engine type. Timestamp and Duration retain their existing normalization rules;
this repair adds neither named timezones nor civil-time/DST parsing.

The same existing cache helper serves supported top-level and bounded Struct
children. Dictionary/opaque and unsupported nested forms retain their existing
mapping and admission; no new recursive or dictionary-native type support is
claimed. The conversion uses the constructor's existing owned physical cache.
The constructor has no query manager/context, so this cache and conversion are
not newly claimed as query-budget charged or cancellable. Existing stream query
budget/cancellation and retained-owner lifetimes remain unchanged.

## Regression evidence

The unchanged baseline incorrectly kept Time32 seconds and Time64 microseconds
coefficients while export relabeled them milliseconds/nanoseconds. Baseline
adapter and lazy failing tests are retained separately from final results in
the arrow-time-validation archive. The independent source/stream/Collect/export
probe reproduces the issue without any pending StructGet feature.

Exact verification outcomes, review, source hashes and gate logs accompany the
candidate. Local validation does not imply hosted CI, native ARM execution,
publication, a performance improvement or completion of roadmap item 18.

Parquet capabilities remain separate: successful Time32/Time64 writing does not
mean the existing native Parquet reader accepts time logical types. Duration
writer support is not added. Tests record those rejection boundaries instead of
claiming an unsupported end-to-end path; Timestamp has its own supported closure.
