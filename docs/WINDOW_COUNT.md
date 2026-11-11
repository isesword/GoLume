# Rolling count contract

`RollingCount(column, frame, alias)` returns a non-nullable `int64` count of
non-NULL parent rows in an inclusive, partition-local ROWS frame.
`RollingCountRows(frame, alias)` counts every row in that frame. Empty frames
and all-NULL value frames return zero, not NULL.

## Parent validity

- A valid boolean counts once, including `false`. The native boolean backing
  is `Uint32Vector`; boolean payloads are not used as the count predicate.
- A native `list<string>` counts once when its parent is valid. Empty lists and
  lists whose elements are all NULL count once. A NULL list does not count.
- A native struct counts once when its parent is valid, even if all children
  are NULL. Child validity never overrides a NULL parent.
- An absent validity bitmap means all parent rows are valid.
- Existing numeric, temporal, string, binary, dictionary, and source-native
  opaque validity handling is preserved. For boolean/list/struct fields with
  no native binding, the opaque vector's parent bitmap remains the fallback.
  This does not add new Arrow boolean conversion or nested-type support.

## Frames and execution modes

Both count functions validate bounds during logical schema inference and at
the direct physical constructor. Unknown bounds, negative finite offsets,
offsets on CURRENT ROW or unbounded bounds, and a start ordered after the end
are errors, including on empty input. Direct physical count results must use
the expression alias and a non-nullable, plain `int64` field. CountRows accepts
no column; CountValues requires an existing input column.

The existing ROWS ordering and clipping rules are unchanged: zero PRECEDING
and FOLLOWING offsets equal CURRENT ROW; finite offsets retain their full
`int64` range before mode-specific capacity checks. Resident execution supports
preceding/following frames and clips them to the partition, including empty
frames. Existing unbounded sentinel semantics are preserved.

Ordered and external-sort execution still require aggregate frames ending at
CURRENT ROW and starting at CURRENT ROW, N PRECEDING, or UNBOUNDED PRECEDING.
Finite retained state is subject to platform capacity and memory budgets.
External-sort execution requires its existing finite memory and spill quotas.
This change neither broadens bounded FOLLOWING support nor changes the
resident algorithm's time complexity.

## Aliases and ownership

Expressions in a single window read the same original input, even if an
earlier expression replaces their source column. Replacing a boolean, list,
struct, or another input type leaves only the result's `Int64Vector` binding.
Resident execution releases replaced owned materializations; ordered execution
detaches borrowed aliases without releasing or mutating the child input.
Partition resets, input-order restoration, and repeated execution retain their
existing contracts.

## Scope and validation

Regression tests cover resident, ordered, and external-sort modes; chunk and
partition boundaries; current, bounded-preceding, and unbounded-preceding
frames; following and empty resident frames; NULL/non-NULL/empty inputs;
repeated execution; typed aliases and public Series values; and invalid
physical and logical contracts. Independent tests exercise the same behavior
through direct physical sources.

This change does not alter Sum/Avg arithmetic, numeric overflow behavior, or
Arrow boolean import/export. The original Parquet fixture is not replaced or
downloaded; fixture-dependent tests must be reported separately when absent.
