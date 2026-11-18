# Native Parquet bounded recursive forest (N4 candidate)

This is an isolated implementation candidate, not a publication or test-pass claim.
It builds on the frozen N3 bounded recursive parent. Historical validation reports
for earlier snapshots do not validate this integration.

## Explicit entry point

```go
src, err := parquet.OpenFileWithOptions(path, parquet.Options{NativeNested: true})
```

`Options{}` delegates to the existing `OpenFile`. The default flat reader, its
Decimal handling, statistics and Bloom/page pruning are unchanged. NativeNested
is the only native option: earlier experimental shape-specific flags are not
part of this API. Stored Arrow schema metadata does not expand native support.

## Bounded schema

NativeNested accepts a forest of top-level fixed primitives, UTF-8 String,
standard three-level List<Int64> or List<String>, and nonempty recursive Structs
up to memory.MaxStructDepth (8, counting the root as depth one). Multiple Structs and top-level
siblings may coexist. Fixed primitives are ten numeric types, Bool, Date32 and
Timestamp. Timestamp conversion follows the existing Parquet binding semantics.

Struct depth above eight, nested item Structs, List<List>, Map, nonstandard repeated schemas,
TIME and Decimal are rejected in this native mode. This is not arbitrary generic
nested support. The complete schema is validated before projection; projection
cannot hide unsupported fields. Projection selects complete top-level fields.
A literal dotted name is not an implicit nested path.

## Parent masks and ownership

Parent and child slots are explicit: equal child names in different Structs and
a literal dotted top-level name remain independent. All selected leaves of one
Struct must agree on current-row parent presence. Different Structs may have
different NULL masks. NULL Struct, present Struct with NULL list, empty list,
NULL element and valid empty String remain distinct. Malformed definition or
repetition levels are rejected even below a NULL parent.

All columns participate in a shared row transaction, not independently zipped
streams. Variable values are staged, reserved and copied before fixed siblings
advance; ancestor witnesses and cancellation are checked before a non-failing
commit. A list's next-row lookahead cannot replace the current ancestor witness.
Borrowed scalar String tuples are not fake single-element lists, and their
reader cannot advance while those bytes remain pending.

A recoverable budget boundary returns only complete rows and preserves the
pending row's completed columns, partial stages and borrowed tuple. Errors and
cancellation release owned buffers and close readers. Cropping updates Batch,
Struct and Struct.Values row counts, masks, scalar lengths and list offsets.

## Budget and pushdown boundaries

Owned metadata, parent/child headers, outputs, stages and replacement candidates
are included in native admission. Old and new buffers overlap during replacement
and both count. The hard-row payload is summed across columns. String bytes and
element capacities are distinct. NULL parents do not eliminate header charges.

Parquet page, dictionary and codec allocations remain outside the owned native
buffer ledger. This is not a process-RSS limit or asynchronous codec preemption.

Native projection retains predicate dependencies and validates Bool predicates;
the engine still evaluates the residual predicate. A native pushdown report is
not proof that filtering occurred. Prepared partition order and subset are
preserved. The default flat pushdown path is unchanged.

## Validation required before promotion

Run the integrated source/parquet scalar and SIMD suites, explicitly safe lazy
entry-point/consumer tests, focused race tests and default flat/Decimal/Bloom/
CountRows regressions. Retain original budget boundary, prefix/retry, lease
identity, cancellation, malformed-level and parent-consensus assertions. Test
fixed-only and scalar-final rows as well as mixed lists, dictionary/plain pages,
multiple row groups and small batch sizes. Earlier shape-specific flag rejection
cases are no longer API tests; bounded-forest rejection cases remain required.

Historical full-suite failures and access restrictions remain unresolved by
this implementation. No full-repository gate pass is claimed.

Deep node/edge metadata is admitted separately beyond depth one, preserving
existing depth-one budget constants. Each leaf has a bounded ancestor-ID chain;
required and optional nodes retain distinct identities even when their definition
thresholds match. See RECURSIVE_STRUCT.md for checked ownership and IO boundaries.

## Explicit flat List<Struct> profile

NativeNested also admits standard List<nonempty flat Struct> with fourteen leaf
kinds (the thirteen fixed kinds above plus String). Item children cannot contain
Struct/List/Decimal. Total Struct depth includes the item and remains eight;
there is only one repeated edge per path. See LIST_STRUCT.md.

One grouped cursor requires per-element repetition and ancestor/List/item
presence agreement across every physical leaf, with parent and element offsets
on distinct axes. New groups add 8192/group + 8192/item-leaf metadata; existing
profiles keep their old charges. Whole bundles enter the shared row transaction.
SinkParquet remains explicitly rejected for StructLists. IPC export/readback does
not establish Parquet writer support; no subsequent sink checkpoint is included.
