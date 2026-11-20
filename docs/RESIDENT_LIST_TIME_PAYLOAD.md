# Resident Sort: native local TIME list payload

## Evidence and scope

N37 investigates a new synthetic reproduction, not the unavailable historical
16 MiB resident / 64 MiB ordered workload. On the unmodified N36 baseline, three
synthetic native Parquet cases decoded successfully and selected `resident-stable`,
but the sorted sink failed with `requires string child "item", got "item":time64`.
The tiny 16-row case failed under 16 MiB as well. These were type-dispatch errors,
not ResourceError evidence or proof that a larger historical workload fits.

The first barrier was resident metadata planning, which treated every non-Int64
list as List<String>. The shared accumulator storage classifier made the same
assumption. Native local TIME(ns, false) lists already decode into Int64ListVector;
their schema, not the backing Go values, distinguishes them from List<Int64>.

This change admits only canonical top-level `memory.IsListTime64` payloads in
resident Sort with existing scalar ordering keys. Their original logical schema
is retained, including parent/element nullability and local nanosecond TIME type.
There are no list comparisons, expressions, new nesting profiles, automatic
spill fallback, or general codec changes.

## Admission and validation

Sort with `Spillable: true` and both TopK input entry points explicitly reject
native TIME list schemas. This prevents the shared accumulator classification
change from accidentally admitting another consumer. The old opaque Arrow
contract is left unchanged. Native TIME lists under Struct or another List remain
unsupported. Combining a native TIME root with an extended nested payload sibling
is explicitly rejected rather than bypassing the existing nested graph validator.

Every delivered resident input batch, including zero- and one-row batches, is
validated before copying. Required checks include canonical schema, Int64 list
storage, no conflicting list/opaque binding, parent Plain/View and child Plain
encodings, exact offsets/child length and validity bounds. Required parent and
required child NULLs are errors. Valid elements of valid parent rows must satisfy
`0 <= nanoseconds < 86_400_000_000_000`. Payload under a NULL parent or NULL child
has no TIME meaning and is not range-checked. Empty valid lists remain distinct
from NULL parents. Input values are copied into owned resident storage before the
source is advanced or closed.

## Ownership and budgets

Metadata counts and descriptor sizes use the existing Int64List storage plan.
The accumulator's existing reserve-before-grow implementation owns offsets,
parent validity, child values and child validity. Stable permutation and bounded
output ranges continue through the existing resident typed gather path. All
source, resident input, row ordinals, output and temporary overlap remain subject
to their existing manager reservations; there is no budget bypass or estimate
based on source Stats. Cancellation and failure use the existing terminal cleanup.

This change does not globally relax `ValidateListInt64`, `ValidateList`, or the
spill wire codec. The legacy unbudgeted batch accumulator is deliberately not
expanded. New synthetic round-trip, metadata/NULL, budget and cancellation evidence
must be distinguished from the missing historical workload when reporting results.
