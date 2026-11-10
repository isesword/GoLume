# Resident Sort and replay TopK ownership

Resident ordering does not use source statistics as an allocation plan. Sort
materializes actual input rows into an owned accumulator. Replay TopK admits
only actual heap capacity and copied ordering cells, then accumulates selected
payload rows during its second pass. An arbitrarily large K with a small input
therefore does not allocate K slots.

## Dictionary semantics

String and binary dictionaries are interned by their referenced bytes. Source
codes and domain strings are not evidence of value identity. Unreferenced
entries, duplicate entries, prefix/suffix storage, and invalid codes hidden by
NULL rows are not copied. Each distinct selected value is copied once into a
normalized table, and output indices are remapped. An admitted open-addressed
index avoids scanning the dictionary once per row. Hash matches are checked by
bytes. Finished tables receive a content-derived domain.

The output source schema clears dictionary domains and does not implement the
source-level DictionaryProvider contract. Forwarding the original table after
remapping would incorrectly promise that its codes still describe the output.
Logical fields and dictionary encodings are preserved.

## Allocation admission

The shared typed accumulator admits metadata, validity buffers, offsets, data,
row values, dictionary indexes, and domain construction before allocation.
Growth accounts the complete new backing while the old backing remains live.
Sort additionally admits comparator bindings, ordinal arrays, and parallel merge
run arrays. Parallel merge allocates its exact result capacity rather than an
unplanned append sequence. Replay TopK admits replacement key bytes before
releasing the evicted key, so old/new overlap cannot escape the limit.

Resident workspaces use one charged credit node per actual growth. External
sort retains its existing small independently releasable credit chunks. Both
modes reserve the same payload plus bookkeeping before creating a credit node.
Unlimited managers still account live storage. A query budget, when present,
is the owner rather than a separate source-constructor manager.

Reservations represent explicit capacities and conservative object-graph
metadata envelopes, not exact Go allocator size classes, GC liveness, process
RSS, or arbitrary user-defined Arrow implementation overhead. High-water
capacity credit may remain admitted until the resident phase ends. Failed or
cancelled operations clear owned references before returning reservations.

## Output lifetime and opaque Arrow data

Typed output is borrowed until the next Next call or Close. Independent output
vector descriptors are invalidated before their storage owner is released.
ReleaseOwned on an output borrower does not release the resident owner.

Opaque Arrow arrays retain their full backing, including parent buffers,
children and dictionary arrays, not merely the logical row slice. Repeated
ArrayData identity is charged once per opaque field. An explicit-capacity,
seeded open-addressed index avoids scanning all earlier sources for each row.
Hash collisions still compare full identities, so equal payload contents or
shared child buffers do not merge distinct ArrayData roots. Each occupied entry
owns one retained reference; rehashing transfers that ownership without changing
reference counts. The entire replacement table is admitted while the old table
is still live, and its high-water credit remains held until owner close.

Reordered output keeps the original source identity instead of the newly sliced
row's Data identity. Per-row Arrow slice metadata and owner arrays are admitted
separately. Nil backing and custom ArrayData implementations that Arrow's own
slicer cannot handle are rejected before slicing or retaining buffers. This does
not add a general custom Arrow ArrayData slicing contract.

Opaque output is emitted
one row per batch in sorted order; consumers must not assume one result batch.
This avoids generic Arrow Concatenate, whose dictionary-unification and
non-buffer metadata allocations do not expose a complete bounded plan.

The existing one-pass snapshot TopK path is unchanged. Generic Window and merge
join accumulation, generic CloneBatchRange/type boundaries, external-sort
compaction, and native decimal execution policy are separate work.
