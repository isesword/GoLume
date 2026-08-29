# Phase 2.18 — List-producing string expressions

Phase 2.18 adds `Split` and `RegexExtractAll` to the fused scalar projection
engine. Both operators produce the specialized `List<String>` representation
introduced in Phase 2.17; neither materializes `[][]string` or `[]any`.

## Semantics

`Split(value, delimiter)` accepts a row-dependent UTF-8 delimiter. NULL in
either operand produces a NULL parent list. An empty delimiter splits a
non-empty value into Unicode code points. Literal separators preserve empty
elements at the beginning, end, and between adjacent separators. Consequently,
an empty value split by a non-empty separator produces `[""]`, while an empty
value split by an empty separator produces `[]`.

`RegexExtractAll(value, pattern, group)` requires literal pattern and group
metadata. Group zero selects the whole match. A NULL input produces a NULL
list, no matches produce a valid empty list, and an unmatched optional capture
produces a NULL child element without changing match cardinality. RE2 syntax,
the 64-KiB pattern bound, compiled-program reuse, and projection CSE are shared
with `RegexMatch` and `RegexExtract`.

## Memory and SIMD boundary

List result sizes are preflighted before buffers grow. Parent offsets, child
offsets, data, and validity are charged to the session memory manager and held
by the projection workspace until its reader closes. Budget failure returns
`memory.ErrBudgetExceeded` without retaining a reservation.

The constant single-byte delimiter path uses `bytes.IndexByte`. On Apple M4
Pro, the checked-in benchmark measured it at roughly 1.92 microseconds for the
test payload, statistically indistinguishable from `bytes.Index`. No custom
SIMD kernel enters Auto mode because the required stable end-to-end advantage
was not demonstrated. Dynamic delimiters, empty-delimiter Unicode decoding,
and RE2 matching remain scalar; fixed-width companion columns continue using
the existing approved SIMD kernels.
