# Bounded native List<Struct> (N6 candidate)

Isolated integration candidate on the frozen N5 parent; historical archive test
reports do not establish acceptance of this integration.

The admitted profile is a standard List with a nonempty flat Struct item. Item
fields may be Int8/16/32/64, Uint8/16/32/64, Float32/64, Bool, Date32, Timestamp or
String. Item Struct/List children, Decimal, Map, Opaque and other types remain
unsupported. Multiple such Lists may coexist with existing supported columns,
including beneath outer Structs. The total Struct depth limit is eight, counting
the item Struct; it is not reset at the List edge. There is only one repeated
edge on a schema path. This is not arbitrary nested-type closure.

NULL List, valid empty List, NULL item, and present item with NULL children are
separate states. Offsets address element rows, while outer ancestor validity
addresses parent rows. Required-value checks use the appropriate visible domain;
malformed storage is rejected even when its values are hidden. Completely
unevaluated expression branches retain their existing short-circuit semantics.

## Admission and owned values

The standalone `ListStructField`, `NewSchemaWithStructListsChecked`,
`StructListVector` and owned range/Take APIs are explicit. The older primitive
`ValidateList` and `NewSchemaChecked` do not silently admit this new shape.
Owned results have canonical offsets and independent payload/schema storage.
NULL parents contribute no copied elements. Nullable items retain their position.

`Batch.StructLists`, InMemory, owned Collect and ordinary `ToAnySlice` values
support the profile. Arrow input requires `Options.NativeStructList`; this
option includes the necessary outer Struct ancestry and may coexist with the
existing recursive-Struct and top-level-Decimal opt-ins. Defaults are unchanged.
Dictionary Strings normalize into owned views. Dictionary tables with any NULL
entry are explicitly unsupported, including unreferenced entries. IPC export and
opt-in readback are supported; export rejects data exceeding Arrow's signed
32-bit List/String offset limits before builder allocation.

`ListLength`, `ListGet` and `StructGet` support the new owned results. ListGet
keeps existing zero-based index, NULL, negative and out-of-range semantics;
a Struct item can be passed to StructGet. Selected expression execution,
StructFrom/JSON construction of the new shape and nested keys are not expanded.
Expression replacement failure retains the previous owner where that API promises
it; stream output instead expires on the next call. These are different contracts.

## Spill and ordering

Columnar query-local spill uses version3 for StructLists without two-level Lists.
Schemas containing two-level Lists use version4 (NESTED_LIST.md); schemas with
neither profile retain their unchanged version2 wire layout. Old
readers reject the new version; decoder version/schema mismatches reject. This
is not a persistent-file migration guarantee.

Bounded external Sort may carry the profile as payload under scalar keys through
initial gathering, multiple actual runs, merge and owned Collect. N8 resident
Sort and N9 TopK add separately checked scalar-key consumers. Struct/List keys, unrelated
join/window/group consumers and selected expressions are not enabled by this
change. A failed merge accumulator cannot publish or be reused; Close releases it.

## Unified Parquet input

`Options.NativeNested` admits the profile within the existing shared planner and
row coordinator. NativeNested remains the only Parquet native option; earlier
experimental shape-specific options are not part of this API. Each Struct-list cursor groups its physical leaves, requiring agreement
on repetition level, parent/List presence and item presence for every element,
not merely equal final lengths. Child validity may differ legitimately.

One paid pending tuple per leaf preserves borrowed page data until copying.
All child buffers are admitted together before an element commits; all logical
columns join the existing complete-row transaction. Prefix cropping uses parent
row count for offsets and terminal element count for item storage. Projection
keeps complete top-level fields; names containing dots remain literal identities.

SinkParquet admits this same profile through the existing streaming writer,
including multiple row groups and optional sharding. Current integration tests
cover NativeNested and independent Arrow reading of written values and schema.
Nested item Structs/Lists remain rejected by this flat-item profile. Separate
two-level primitive Lists use the ArrayNode profile (NESTED_LIST.md). See
LIST_STRUCT_PARQUET_OUTPUT.md for atomic publication and resource limits.

## Accounting and verification limits

Charges cover owned containers, schema snapshots, parent offsets/validity, item
and child storage, String bytes, and simultaneously live replacement buffers.
New reader groups add conservative metadata envelopes of8192 bytes per group and
8192 per physical child leaf; old shapes keep their original charges. Exact-capacity
owned copies and conservative gather floors are not minimum-heap claims.

Source construction, third-party page/dictionary/codec allocations, allocator/GC
behavior and process RSS retain their documented exclusions. Metadata walks,
elements and32KiB copy chunks have cancellation checks, not asynchronous codec
preemption. No performance improvement or native ARM execution is claimed.

The validation archive distinguishes A owned foundation, B consumers, C real
spill and D native reading, including corrected test-evidence classifications.
Current integration tests and review remain required; historical failures and
protected full-repository gate restrictions remain in force. This feature does not complete roadmap item18.

N9 extends scalar-key TopK to this existing payload profile; see NESTED_TOPK.md
for stable-replay and one-pass snapshot lifetime/peak-budget boundaries.
