# List<Struct> Parquet output

`LazyFrame.SinkParquet` accepts the existing bounded List<flat Struct> profile
without another flag or a shape-specific writer. It reuses checked Arrow export,
the current pqarrow writer, managed allocator and streaming sink. It does not
Collect the entire result or enable recursive item fields, nested keys, default
resident Sort or TopK. The separate two-level primitive List profile is documented
in NESTED_LIST.md and shares this sink lifecycle. The precise fourteen-type/depth-eight
profile remains in LIST_STRUCT.md.

NULL List, empty List, NULL item and present all-NULL-child item are preserved,
including beneath outer Structs. Timestamp units, signed/unsigned annotations,
field order and nullability are part of the output contract, not just values.
Arrow signed32 List/String offset limits are checked before builder allocation.
Files can be read by NativeNested or independent Arrow/Parquet readers.

## Row groups, sharding and empty results

`RowGroupRows` controls row-group sizing. `MaxRowsPerFile` is a hard row limit;
`TargetBytesPerFile` remains a soft uncompressed logical-payload planning target,
not a compressed-file byte cap. A first oversized row may exceed the target.
An empty stream still creates a schema-bearing empty file, including one part
for sharded output. Tiny row groups may increase the cost of a subsequent Collect;
streamed writing does not promise that collecting the entire output fits the
same budget.

## Cancellation and publication

The sink writes to staging and closes/syncs it before rename. Failures before
publication clean staging and preserve an existing overwrite target. Cancellation
is checked after writer.Close and immediately before commit, fixing the previous
case where a cancellation without a subsequent allocation could still commit.
This is not an atomic cancellation/rename transaction: cancellation racing after
that last check does not revoke an already committed file.

Successful rename is the publication point. A later parent-directory durability
sync error can return an error after the result is already published; callers
must not interpret every error as proof that the target is unchanged. Existing
no-overwrite checks are not a concurrent atomic no-replace guarantee. Sharded
output retains its existing overwrite restrictions.

## Resource boundary

The query allocator admits Arrow buffers and writer allocations that actually
use that allocator, and releases them through the existing owner lifecycle.
This does not account for every third-party allocation. In particular, the
reviewed writer allocates a temporary ByteArray descriptor slice proportional
to the String leaf element count using ordinary Go allocation. Other slice
bookkeeping, source construction, codec internals, allocator/GC and process RSS
are not an asserted total-memory bound.

The allocator retains its first actual reservation error for the lifetime of a
failed sink. Factory, Write and Close preserve that error identity together with
any encoder error, even if the native writer internally converts an allocation
panic into a generic error. Cleanup still runs; the failed sink is not reusable.
Unrelated panics are not silently swallowed.

Synchronous encoding is not asynchronously preempted: no goroutine is abandoned
while it still accesses a record. Checks and allocation failures are observed at
the documented execution boundaries. Tests distinguish actual public low-budget
and real-writer cancellation from controlled factory/Write/Close failure probes
through the shared sink seam; those probes are not claims about every native
codec failure site. No performance improvement is claimed.
