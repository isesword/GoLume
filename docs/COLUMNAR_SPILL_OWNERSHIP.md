# Columnar spill ownership and admission

`DecodeColumnarRun` owns the reconstructed batch until `DecodedColumnarRun.Close`.
Its reservation is the encoded payload length **plus** the schema-dependent
metadata charge returned by `ColumnarRunDecodeMetadataBytes`. The full original
payload charge is retained. Restoring the spill file and decoding it are separate
owners and their reservations overlap while both objects are live.

## Sparse decoded batches

A decoded batch has only the typed maps needed by its schema. Other typed maps
are nil. This is intentionally different from `NewBatch`, which returns reusable
writable maps for every supported type. Reading or ranging over an absent map is
safe; assigning a new column to one is not. Do not treat a decoded batch as an
extensible writable result builder. Use a separately owned `NewBatch` for that
purpose, with admission for any materialized data.

Decoded bindings are borrowed and read-only. Calling `ClearColumns` voluntarily
relinquishes the bindings. All borrowed maps, vectors, fields, and slice aliases
become invalid when the owner closes. Close clears the graph still reachable
from its batch before returning its reservation. It cannot clear an arbitrary
vector or slice copy that a caller removed and retained beyond the borrow's
lifetime. The input schema's names, Arrow type strings, and nested field metadata
remain borrowed immutable data; Close does not mutate that schema.

The production decode consumers are spill join restores and external-sort
cursors. Spill join temporarily binds the decoded maps to the caller's batch and
returns the original empty map containers before closing the owner, including
when the next call uses another destination. Absent typed maps borrow the
caller's existing empty maps, without a compatibility copy or extra allocation.
Once a join build is completely loaded, its decoded batches independently own
all data needed for probing. That successful branch cannot fall back to
repartitioning. It therefore deletes consumed build files and releases their
reference arrays before materializing output. Each probe file is likewise deleted
only after its independent decode succeeds. Failed deletion remains owned by the
spill store for cleanup; decoded reservations stay charged until their actual
use ends. Failed build admission retains input files for skew/repartition fallback.

External-sort cursors keep decoded batches internal and pre-admit metadata for
every simultaneously live cursor. Their merge/output accumulators independently
pre-admit sparse maps, descriptors, owned outer Fields arrays, backing capacities,
and old/new buffer overlap. The output owner restores the caller's original
empty map containers before destroying its graph and releasing its reservation.
Absent types reuse the caller's empty maps; a zero-valued destination can still
have nil maps for absent types. Borrowed output is not an extensible result builder.

## External-sort initial gathers

Initial-run gathering admits the actual runtime bindings it will copy, not just
columns advertised by the schema. Repeated aliases receive separate destination
descriptors and buffers and are charged separately. The ordinary inmemory stream
projects root bindings to its advertised schema before sorting; custom Sources
can expose additional root bindings. Valid wide declared Structs still require
independent gather metadata, even when the source already paid for its own graph.

A zero-allocation range plan counts selected buffer capacities, typed-map
containers and entries, column and List child descriptors, copied outer
Struct.Fields arrays, sorting indices, key bindings, and the stable ordinal.
The original range-based sort allowance remains a lower bound. The plan does not
impose join's separate full-source-payload floor on every small sort chunk.
Copying uses exact capacities, including validity introduced by List and Struct
canonicalization. Encoder headroom also covers that introduced validity and the
ordinal, before gathering starts. The whole borrowed input graph is validated
for shapes, cycles and depth before recursive range statistics are evaluated.

The initial owner holds partially built state immediately, including a copied
extra Uint64 binding displaced by the internal ordinal. That extra is not
projected away to reduce its fee, and no source vector is overwritten. Once
encoding has produced an independent payload, the owner clears its vectors,
maps, Fields, indices and key bindings before returning workspace credit. The
encoded payload remains charged through the file write and is released before
registering the new run's reference array. Failed file deletion remains the
query store's responsibility; it does not keep an already-consumed gather alive.

Gathered Struct children follow the runtime Struct.Fields list, as in ordinary
takeRows. Other child-map entries are validated but are not falsely described as
copied output. Top-level dictionary tables and domains are borrowed only while
the source is still live; nested dictionary fallback copies its table buffers.
Unadvertised all-NULL runtime dictionaries may omit their table, with arbitrary
hidden codes; any valid row still needs a valid dictionary entry. This opt-in
keeps the strict join validation and declared-dictionary codec rules unchanged.
Copied Fields arrays are shallow: names and Children remain borrowed immutable
metadata. The source's own input allocations and borrowed binding containers
remain the Source's responsibility. On an invalid input graph or child error,
the sorter detaches its borrowed header without recursively destroying that
untrusted graph, then closes the child owner.

Opaque runtime extras are explicitly rejected before gathering. Ordinary
unbounded takeRows can gather Arrow extras, but this bounded path has no proven
Arrow allocation plan; this is a deliberate restriction, not complete Arrow
support. Native Decimal runtime extras can be copied, but advertised Decimal
Sort remains subject to its existing public execution rejection. Gathered Struct
children remain the supported scalar/string/List<String> forms, not arbitrary
recursive, Decimal or Binary children. Unsupported representations return an
error rather than being silently omitted.

## What the metadata charge covers

The plan covers Batch headers, owner/reservation overhead, active typed map
containers and bindings, concrete vector descriptors, List element descriptors,
Struct child batches, and owned outer Field arrays. Constructors use capacity
hints and exact-capacity value/data buffers. Planning and successful validation
allocate no scratch collections. Unsupported schema shapes are rejected before
owned containers are allocated; partial decode failures use the same cleanup as
Close.

The map allowance is a conservative engine-accounting convention based on the
supported Go runtime, not an assertion of exact heap usage or an absolute bound
for every possible future map implementation. The code documents its units and
checks all size arithmetic. It does not charge the same bytes twice merely
because restore and decode share a schema. Engine budget accounting is distinct
from allocator size classes, stacks, runtime metadata, GC timing, and RSS.

## Custom Source batches and bounded join gathers

The Source interface does not require runtime typed maps to contain only fields
in `Source.Schema()`. Bounded spill join therefore validates and admits its
actual gather objects, including schema-extra map bindings. Several names that
point to one source vector are charged separately because gathering creates a
separate output vector for each name. The selected encoder still serializes only
schema fields; extra columns are not silently projected away to avoid paying
for the gather that is actually performed.

The gather preflight retains the prior full-source-payload charge as a lower
bound and adds runtime descriptor/container accounting. Selected value buffers
are preallocated to planned capacities so appending cannot exceed admission.
Struct output follows the runtime vector's `Fields` list; unrelated child maps
are validated and included in the retained source-payload lower bound, but are
not falsely described as copied output. Names and nested field metadata remain
borrowed. Native decimal vectors retain values, validity, precision, and scale.

Opaque Arrow take/concatenate does not yet have a proven bounded workspace plan.
Bounded join rejects any such runtime binding, including schema-extra fields,
before gathering or writing. Ordinary unbounded `takeRows` retains its Arrow
behavior. Unsupported or malformed shapes fail explicitly rather than being
omitted. The runtime-object walk accepts at most 64 nested Struct.Values edges,
including fields not selected for output; deeper trees and cycles are rejected
before admission or copying. A custom Source remains responsible for admission and lifetime of its
own input objects; this gather charge covers the additional engine-owned copy,
not the source's pre-existing storage.

## Finite-budget limits

No fixed budget guarantees success for arbitrary schema width, path length,
dictionary size, run count, or fanout. Unsupported objects and a single working
set that cannot fit are explicitly rejected. A failed admission must not be
reported as a successful morsel or leak reservations. Earlier successfully
returned morsels are not transactional and are not rolled back. The implementation does not
raise query limits, omit live objects from accounting, or promise fixed RSS.
