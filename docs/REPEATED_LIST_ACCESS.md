# Checked repeated List access

N39 extends the existing `ListLength` and `ListGet` expressions, not the general
expression profile. Their value operand may be a checked three-axis List over
Int64 or String. Length returns the outer length. Get returns an independently
owned nullable two-axis List; the existing Get chain can then reach a scalar.
NULL input/index, negative index, and out-of-range index retain NULL semantics.
Indices retain exact integer handling. No new public expression API is added.

## Admission and effects

Only the value operand of these two operations receives the depth-three binder
exception. Index expressions and every other operation retain their own existing
admission. StructGet, List comparisons, Sort, TopK (including key input), and
spill remain unchanged. Direct column transport retains N38's separate contract.
MayError propagates from children; invalid storage, cancellation, and memory
admission are not newly classified as value errors. Active-row evaluation still
validates full storage shape while enforcing required values in the effective
visible parent domain, before selection. Inactive guarded rows do not evaluate
value-error-producing children.

## One nullable selection owner

`TakeNullableArrayRowsOwned` accepts one or two repeated axes and uses the same
bounded recursive count/copy implementation as ordinary nested Clone and Take.
There is no second per-type copier and no borrowed result. Fixed arrays bound
axis descriptors to the checked maximum. Duplicate selections are counted in
full with checked arithmetic before allocation. A synthetic -1 increments only
the output root count and never dereferences input offsets or values.

This low-level operation is deliberately **root-optional selection**: both a
selected real NULL root and -1 produce NULL even when the supplied root field
is required. It is not a general required-root validator. Output root metadata
is nullable; required descendants remain strict beneath selected valid roots.
All source spans, including hidden NULL storage, must be structurally sound;
hidden payload bytes/values are not copied. Ordinary Clone/Take keep their
strict required-root and nonnegative-index contracts. Expression evaluation
first checks the original visible input field/domain, and does not loosen its
required metadata before that check.

The selection owner reserves complete copied metadata, per-axis offsets and
validity, terminal vectors and String bytes before allocating owned storage.
The caller owns and accounts for the source and selection-index slice; expression
scratch indices and effective masks are separately reserved by the workspace.
Old and new register owners coexist through successful replacement, then the
old reservation is released. Returned ArrayNode storage lives with the workspace
register lease; workspace reset/Close releases it. Physical publication retains
its existing borrowed-output lifetime, rather than extending it to arbitrary
caller retention. Allocation/cancellation failures release newly acquired leases.

## Limits

This does not add deeper List support, new leaves, arbitrary List/Struct mixing,
new operators, sorting, or spill codecs. It does not certify whole-process RSS
or unaccounted caller allocations. Tests must establish actual chained execution,
owned output and source-poison independence, guarded/CSE reuse, required-domain
checks, finite exact/-1 admission, and failure/cancellation cleanup.
