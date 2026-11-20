# N38: depth-parameterized repeated List transport

This implementation refactors the checked array transport to actual depth two
or three with fixed maximum-three-axis descriptors. The legacy
`MaxNestedListDepth = 2` remains an explicit consumer ceiling;
`MaxRepeatedListDepth = 3` bounds the new checked transport. It does not represent
arbitrary nesting support. Existing depth-one primitive lists retain their own
storage, and depth-two callers retain their API and shape semantics.

`NestedListField` may wrap a primitive List or an existing two-axis List named
`item`. The terminal remains native Int64 or String. Existing permitted outer
Struct ancestors are retained. List/Struct alternation, new leaf types and deeper
axes are not admitted. Validation checks shape and all offsets, including hidden
NULL spans; required-value checks only traverse visible ancestor domains.

## Explicit consumers

NativeNested Parquet planning, direct projection, checked ArrayNode borrow/range,
owned clone/take, Arrow conversion and the existing output sinks use actual depth.
Sort, TopK (including narrow key input), expression container binding/ListGet and
public columnar spill entry points explicitly enforce the old two-axis ceiling.
Changing a common validator therefore does not grant their depth-three admission.
The internal selected-array sizing traversal is shared with owner validation, but
public spill schema gates remain unchanged at depth two; no new wire protocol or
spill execution is claimed.

## Bounded decoder and ownership

The Parquet planner walks at most three standard LIST wrappers and records each
axis's definition thresholds. Repetition levels are checked against actual depth,
with per-axis present/slot state, preventing continuation after a NULL/empty
placeholder. Ancestor witnesses cannot change inside a parent row.

Stage and output buffers track fixed-size per-axis row counts/capacities and an
independent terminal string byte capacity. Every growth sizes all list offsets,
validity words and terminal storage first. A complete replacement is reserved
while the old allocation remains live, copied, then the old lease is released.
Existing hard per-row staging and total batch policies remain in force. Oversized
individual rows fail rather than being split internally or bypassing the budget.
Parent pages may split at existing batch boundaries. Metadata remains bounded by
the source's conservative per-array charge; no unbounded axis map or cache exists.

Owned clone/take first validates and counts every selected visible axis into fixed
arrays, reserves the complete independent tree, then allocates/copies. Hidden NULL
spans are canonicalized. Range/borrow computes every axis descriptor and validity
rebase charge. Arrow limits count every visible axis and terminal string byte
span, rather than treating Child.Child as a universal leaf. Existing allocator
accounting excludes unrelated codec/Arrow-private RSS as before.

## Evidence status

Baseline synthetic Arrow-written Parquet List³ Int64/String inputs (required and
optional axes) were created successfully but NativeNested opening failed with
`two-level LIST terminal must be primitive`. That actual red is preserved. This
source snapshot is implementation-first, not a claim that native scan, IPC,
Parquet round-trip, budgets or old depth-two regressions have passed. Those claims
require the separately retained focused and cumulative results.

## Provenance compatibility

The old depth-two checked profile retains its existing ArrowType provenance.
Depth-three transport uses canonical metadata: the independently checked native
Arrow mapper clears provenance only along each new three-axis path and its outer
Struct ancestors. Unrelated depth-two sibling fields are unchanged. This ensures
consumer depth gates cannot mistake native three-axis Arrow input for an opaque
payload. The axis guard does not impose a new total-depth rejection on legacy
opaque Struct metadata; existing typed validators still enforce native structure
limits. Legacy depth-two invalid-continuation diagnostics are retained.
