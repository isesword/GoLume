# Empty Struct gathers

`TakeStructRows` now returns schema-complete child storage for a zero-row
selection. `CloneBatch` inherits the fix when copying a zero-row Struct.
The generic and bounded Join gather paths follow the same declared-field
selection rules for zero and positive row counts:

- Keep every supported declared scalar or List<String> child, with zero rows.
- Keep each scalar's physical type and the encoding used by `AppendStructRow`.
- Empty strings own one zero offset. Empty lists own a parent zero offset and
  an empty string child with its own zero offset.
- Do not create a NULL row, copy hidden row payload, or allocate validity words.
- Use `Strings` before the dictionary fallback when both maps contain the
  same declared string child. Do not copy undeclared runtime child columns.
- The fallback dictionary keeps its encoding and domain and owns a copy of its
  complete offsets/data table, even when no indices are selected.

Top-level Fields storage is copied. Nested Field.Children metadata retains the
existing immutable-schema sharing contract. Row data, empty offset sentinels,
and fallback dictionary buffers do not alias the source. Existing positive-row
NULL propagation and copied dictionary ownership are unchanged.

## Budget admission

The bounded Join copier and its preflight now both initialize and account for
empty children. An empty string needs 4 bytes of owned offsets; an empty
List<String> needs 8. Dictionary tables remain fully charged, as do existing
per-child metadata allowances. Fixed-width values and validity require zero
buffer bytes when zero rows are selected. Tests check exact buffer capacities,
metadata charges, failed first/second reservations, cancellation, and release.
The historic conservative full-payload lower bound is unchanged. These are
operator admission contracts, not an RSS or Go allocator hard-limit guarantee.

The Join spill stream already skips empty selections before gather admission;
that fast path is unchanged. Direct bounded gathers now produce valid empty
Structs instead of relying on the broken shape of the generic gather.

## Boundaries

This does not add recursive Struct, Decimal, Binary, or opaque Struct children.
`CloneStructRange` keeps its existing zero-length behavior, which copies all
runtime child maps rather than only declared fields. This patch does not change
that separate range-copy contract or its conservative estimates.

Ordinary scalar/List<String> output is checked through Struct validation,
columnar serialization/deserialization, and Arrow export. Runtime dictionary
fallback is checked separately through ownership and Arrow export; this patch
does not expand the spill codec's schema/physical-encoding support. Generic
copy helpers are not promoted to full malformed-source validators.
