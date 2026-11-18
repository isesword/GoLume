# Bounded two-level native Lists

The shared `ArrayNode` profile represents exactly `List<List<Int64>>` and
`List<List<String>>`. It is not arbitrary recursive container support. Either
shape may appear beneath up to eight ordinary Struct ancestors and alongside
multiple new and previously supported roots. The two List edges do not consume
the eight-Struct limit: at most ten containers occur on these paths. These are
explicit engineering limits, not Parquet or Arrow format limits.

## States and admission

R0 indexes outer rows, R1 indexes inner Lists, and R2 indexes terminal elements.
NULL outer List, empty outer List, NULL inner List, empty inner List, NULL terminal
and valid empty String are distinct. NULL inner Lists retain an R1 slot. Outer
Struct masks index R0 only; they are not reused as R1/R2 masks.

`NestedListField`, `NewSchemaWithNestedListsChecked`, `ArrayNode` and the owned
Range/Take APIs are checked entry points. Schema depth/cycles are checked before
recursive snapshots. Canonical native input has zero-base offsets and exact
terminal lengths on every axis; owned copies and source views normalize their
ranges. Hidden values can relax required-value constraints but never malformed
offsets, short storage or invalid graph shape. Required checks for a selection
apply to selected rows and their visible ancestors. Entirely unexecuted expression
branches retain the existing short-circuit contract.

Old `ValidateList`, `NewSchemaChecked` and valid-acyclic `NewSchema` assumptions
are not silently expanded. `Batch.Arrays` is one shared registry for both terminal
types, not one map per type permutation. Primitive Lists and flat StructLists keep
their existing representations and acceptance scopes.

## Consumer matrix

| Operation | Supported boundary |
| --- | --- |
| InMemory, owned Collect, ToAnySlice | Both shapes, outer Structs, mixed roots |
| Arrow input | Explicit `arrowrecord.Options.NativeNested` |
| Arrow/IPC export and readback | Checked export; NativeNested readback |
| ListLength | Counts immediate inner List slots, including NULL inner Lists |
| ListGet | Owned nullable inner primitive List; a second ListGet selects its terminal |
| StructGet | May return the new List or an enclosing owned Struct |
| Filter and projection | Materialized shared compaction; no selected-expression expansion |
| Scalar-key bounded external Sort | Owned initial gather, query-local spill and shared merge |
| Parquet input | Explicit `parquet.Options.NativeNested`, standard three-level LIST wrappers on both axes |
| Parquet output | Shared streaming SinkParquet, row groups and optional sharding |

Arrow NativeNested includes the already supported native container profiles but
does not implicitly enable Decimal. `NativeDecimal128` composes for top-level
Decimal only; nested Decimal remains rejected. Existing Arrow options/defaults
keep their scopes. Dictionary String terminals normalize to views; dictionary
tables containing any NULL entry are explicitly unsupported, even if unused.
Arrow's signed-int32 List and String offset limits are checked at each axis before
builders allocate.

Nested keys, join/window/group payload support,
selected expression execution, StructFrom/JSON construction of this new shape,
third List edges, List<Struct<List>> and additional terminals are not enabled.
Projection that removes unsupported payload can still use existing operators.

## Ownership, spill and budgets

Owned Range/Take/Collect survive source closure and do not alias source payload.
A public expression result is borrowed until the next Evaluate or workspace
Close. Direct operator replacement retains its old owner on failure where that
API promises it; public Evaluate may already have invalidated its old borrowed
view. Filter/reader output expires on the next call. These are different lifetimes.

The query-local positional columnar codec uses revision 4 only when the schema
contains this profile. Previous schemas retain their revision 2 or 3 bytes and
fees. Decode requires the revision appropriate for the supplied schema. This is
not a persistent-file migration guarantee. All three axes have independent
counts, offsets and validity, checked against remaining payload before allocation.

Owned copies charge both List sentinels/bitmaps and terminal capacity/data before
allocation. Thousands of empty inner Lists still consume R1 storage even if R2
is empty. Range estimators bound a permutation, not arbitrary repetition of that
range. Tree gather estimates are conservative and can overcount hidden payload
or separate owner bases. Merge growth includes retained old plus replacement
capacity; failure is terminal, not a rollback/retryable row transaction. Close
releases every retained reservation.

## Unified Parquet reading

The existing coordinator assembles whole R0 rows across all columns. A bounded
two-entry repetition stack interprets rep0 as a new R0, rep1 as a new R1 and rep2
as a terminal continuation in the current R1. Definition thresholds distinguish
NULL/empty placeholders at each layer. Invalid continuations are rejected.
A paid tuple and partial row survive a budget prefix without advancing the reader
or losing borrowed page bytes. String data becomes owned before the reader
advances. Every candidate is prepared before fixed siblings/ancestor consensus
and the final whole-row commit. EOF reconciles physical levels and row starts.

NativeNested remains the only native Parquet option and gains this profile.
The zero-option flat path remains unchanged. New adapters add a conservative 16KiB metadata
envelope each, separately from payload capacity and existing physical-leaf and
Struct metadata charges. Multiple selected roots share one global peak budget.
V1 can split a repeated row across pages; V2 writer behavior may retain the row
in one page. A small page-size setting alone is not evidence of a cross-page row.

## Output and resource limits

SinkParquet reuses Arrow export, pqarrow, shared managed allocation, staging and
sharding rather than a shape-specific writer or whole-result Collect. Atomic
publication and allocator-error semantics are those documented in
[LIST_STRUCT_PARQUET_OUTPUT.md](LIST_STRUCT_PARQUET_OUTPUT.md): rename can commit
before a later directory-sync error, no-overwrite checks are not concurrent
atomic no-replace, and cancellation racing after the final check cannot revoke
publication. File-byte targets are soft logical estimates, not compressed limits.

Accounting covers engine-owned buffers and allocator-backed writer allocations,
not all third-party Parquet page/dictionary/codec/temporary slices, source setup,
GC or process RSS. Arrow constructor normalization caches are retained-source
setup outside query-manager accounting. Public no-context Arrow export is checked
at allocation/call boundaries, not every appended element. Query view preflight,
axis normalization, owned copies and new merge paths have explicit cancellation
checks; no abandoned asynchronous codec goroutine is used.

This is an isolated N6 candidate on frozen N5. Current-candidate validation
is required; historical archive reports do not establish acceptance. The planned
checks include independent Arrow and native readback, all eight nullability
combinations, Struct depth eight, empty output, sharding and spill-to-sink.
Protected full-repository gate restrictions and prior failures remain in force.
No performance improvement, publication or complete roadmap item18 is implied.

Resident Sort and TopK consumer extensions are documented separately in
RESIDENT_NESTED_SORT.md and NESTED_TOPK.md; no new container profile is implied.
