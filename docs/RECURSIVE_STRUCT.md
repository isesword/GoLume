# Bounded recursive native Structs (N3 candidate)

Isolated integration candidate. Historical archive results do not validate this
N2-based integration; current tests and review must complete before promotion.

The native recursive Struct limit is `memory.MaxStructDepth == 8`, counting a
root Struct as depth one. This is an engineering resource boundary, not a Parquet
or Arrow format limit. Lists still contain only Int64 or String elements:
List<Struct> is available only through the separate explicit flat-item profile
described in LIST_STRUCT.md. List<List>, Map and nested Decimal remain unsupported.
The primitive-list recursive profile itself is unchanged. This feature
does not establish arbitrary recursive type closure or recursive keys.

## Checked memory and owned results

`memory.NewSchemaChecked` validates the bounded tree before taking a deep
snapshot. Cyclic/deeper child graphs are rejected. The existing no-error
`NewSchema` retains its valid-acyclic input contract; it is not newly claimed to
be cycle-safe. The earlier shallow validator is not globally relaxed, so consumers
such as StructFrom and JSON that have not been extended still reject recursion.

Explicit recursive validators separate structural validity from the visible
value domain. Every descendant buffer/offset is structurally checked even under
NULL ancestors; required values are checked only where every ancestor is present.
Shared subgraphs are treated as logical occurrences when copied and charged.

Recursive owned range and selected-copy operations admit all new storage before
returning it, release reservations on failure, and retain independent payloads.
Collect uses the recursive owned path only for recursive schemas, preserving the
older flat/depth-one path and budget. StructGet remains a literal direct-child
operation, but can return an owned Struct so calls can be chained. Parent NULLs
propagate into the returned child's validity without changing descendant schema
nullability. Struct-dependent expression evaluation on selected batches remains
unsupported; internal sort payload gathering is a separately checked path.

## Arrow, IPC and ordinary values

Arrow input requires explicit `Options.NativeRecursiveStruct` through the
existing options-aware entry point. Default Arrow admission remains one-level.
The option can coexist with NativeDecimal128 for top-level Decimal; nested
Decimal remains rejected. Recursive IPC output is supported through checked
schema/storage export; IPC readback uses the same Arrow opt-in.

Dictionary String input in this opt-in is normalized into owned String views.
Dictionary tables containing any NULL entry, including an unreferenced entry,
are explicitly unsupported. Such tables can be valid Arrow; rejecting them is a
supported-subset boundary, not a claim that the input is corrupt. Required-value
checks still respect all ancestors. Owned collected recursive columns can be
read as ordinary nested values through ToAnySlice.

## Spill and sort payloads

Scalar-key sorting can carry recursive Struct payload through real columnar
spill runs, merge, readback and owned Collect. Codec metadata and payload checks
carry the complete ancestor domain. Decode checks depth and metadata admission
before owned allocations, and cancellation remains distinguishable from corrupt
input. Internal recursive selected-copy has its own admission and ownership
contract; it does not make recursive sort/join/group keys supported or broaden
selected expression admission.

## Native Parquet

`Options.NativeNested` extends the shared planner/coordinator to depth eight.
Within Structs, the Parquet terminal set remains the existing thirteen fixed
primitive types, UTF8 String, List<Int64> and List<String>. Multiple recursive
roots and top-level leaves may coexist. NativeNested is the only Parquet native
option. Earlier experimental shape-specific flags are not part of this API.
Zero options and default flat Decimal handling remain unchanged.

Each leaf carries ancestor IDs and cumulative optional-definition thresholds.
Current-row witnesses for every ancestor are distinct from next-row lookahead.
All variable and fixed leaves agree on every shared ancestor before a whole row
commits. NULL ancestors cannot hide malformed repetition/definition levels.
Whole-top projection includes all descendants of selected Structs; dotted names
are literal field names, not implicit projection paths.

Every parent bitmap is charged once. Deep node/edge metadata adds conservative
fees only for depth two and beyond; existing depth-one tight budgets are retained.
Layout and prefix cropping update all nested child rows together. Variable
stages, borrowed tuples, existing outputs and every replacement share one live
budget and atomic commit, not per-column independent quotas.

## Resource and verification limits

New recursive copying and sort gathering deliberately use conservative charges,
including some hidden String payload and overlapping estimate envelopes. These
are not minimal required bytes. Constructor metadata, third-party Parquet page/dictionary/codec
allocations, allocator/GC overhead and process RSS retain their documented
exclusions. Width validation can be quadratic; bounded depth is not a throughput
or width-performance guarantee. Cancellation checkpoints do not imply codec
preemption.

Validation must separately cover A memory foundation, B consumers, C actual
spill and D reader checks on this integration. Historical failures and protected
full-repository gate restrictions remain in force. Cross-compilation
is not native ARM64 execution. No formal performance improvement, hosted CI pass
or completion of roadmap item18 is claimed.
