# Unified bounded native Parquet entry point

`Options.NativeNested` provides a schema-planned entry point for the supported
native nested shapes. It is the only native Parquet option.
The name does not mean arbitrary recursive Parquet or Arrow schemas are supported.

## Top-level List composition

One or more top-level standard three-level Lists may have Int64 or UTF8 String
elements, in any combination. The additional bounded List<flat Struct> profile
is described in LIST_STRUCT.md. Two-level Int64/String Lists are described in
[NESTED_LIST.md](NESTED_LIST.md). Existing thirteen fixed primitive sibling types
are allowed. Each List and its elements have independent nullability. NULL and
empty Lists, NULL elements and empty Strings remain distinct. Field names are
structural identities, not dotted-path guesses.

A shared typed-cursor coordinator assembles one logical row across all selected
Lists. This is not a zip of independently advancing streams. One owner controls
fixed-column consumption, row counts, output lifetime and the global budget.
Typed Int64/String storage operations retain their own element representations,
with String byte and offset capacities accounted separately.

Default flat reading is unchanged. Earlier experimental shape-specific flags
are not part of this API.

## Bounded recursive Struct composition

NativeNested uses the shared coordinator for Structs up to depth eight with fixed,
String, List<Int64>, List<String>, bounded flat Struct-item Lists and checked
two-level List children,
including multiple Structs mixed
with top-level fixed/String/List fields. Parent presence is checked across all
leaves before a row commits. See PARQUET_NATIVE_FOREST.md for the expanded
original one-level composition contract, and RECURSIVE_STRUCT.md for depth,
consumer and resource boundaries.

Structs deeper than eight, three List edges, Maps, recursive Struct/List item
children outside the explicit profiles and additional element types
remain outside this scope. Unsupported fields are rejected during complete
schema planning, not silently bypassed by later projection. Whole top-level
projection preserves each selected field's semantics and schema snapshots.
Shared fixed-reading utilities remain behind the unified forest coordinator.

## Atomic ownership and bounded native accounting

Each cursor retains its own levels, lookahead and partial row. Borrowed String
page bytes must become owned before that reader advances. Completed earlier
columns are not re-read when a later column pauses. Native accounting sums all
output and stage capacities plus every in-flight replacement, rather than giving
each column an independent copy of the budget. Hard row payload includes every
column; String sentinel offsets and NULL/empty element slots still count.

All variable columns prepare and copy replacements before fixed siblings are
consumed. The final context check precedes a non-failing commit of every column.
Failures release temporary candidates without publishing a partial row; a
complete prefix can be returned with pending state preserved. Lease transfers
move all associated buffers only at the collective commit. Cancellation and
terminal errors release all owners.

Existing page limits and page/dictionary/codec/metadata exclusions remain.
Multiple live column pages do not become covered process RSS merely because the
native buffers share a budget. No performance improvement, arbitrary nesting or
completion of roadmap item18 is claimed. Actual compatibility, mixed-column,
budget and final-gate evidence belongs to the accompanying validation archive.
