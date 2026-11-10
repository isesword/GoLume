# Bounded aggregate spill metadata

A finite query budget covers live aggregate state, spill/restore buffers and the
metadata that keeps disk files reachable. A file is not free just because its
payload resides on disk. The query store admits its handle, full directory/path
strings, codec metadata and handle map before writing. The operator additionally
owns a private handle and admitted reference arrays. These fees are unchanged.

## Incremental partial-state compaction

Initial spilling publishes one run per nonempty radix partition. Keeping every
partial run until EOF can exhaust metadata even when repeated groups have a
small final state. The input consumer now tries to merge the oldest two runs of
each partition at safe boundaries after the spill callback has fully returned.
The old table, partition directory and temporary publication array have ended
before compaction begins. No compaction runs while an input table remains live.
Each invocation makes at most one attempt per partition, not an unbounded loop.

Compaction uses the existing merge and raw-state codec paths. It does not
finalize values or add output groups. The replacement stays in the same radix
partition and retains the unmodified suffix order. Repeated oldest-prefix
folding preserves the original per-group merge order; FIRST/LAST continue to use
their encoded SourceOrdinal. Selected-row, checksum and group counters do not
change. Diagnostics include actual committed extra spill I/O.

The fixed scratch owners, callback/reader descriptors and temporary table
headers have a separate metadata reservation. State buffers, keys, restored
payloads, encoded payloads, file handles, maps and new reference capacity retain
their own admissions. All reservations made during the locked mutation use
non-reentrant growth managers. A compacted encoded payload is limited to half
the aggregate state target, so a later whole-run restore has a bounded target.
The codecs size the complete payload before this limit is applied and before
allocation. This target does not replace actual query-budget checks.

## Commit, rollback and failed cleanup

Before publication, the old reference array and files are authoritative. Restore,
merge, encode or admission failure discards the temporary state. A replacement
file is deleted if its reference array cannot be admitted. Memory/state pressure
or a pure disk-quota admission failure can safely skip this optional attempt.
The existing EOF radix path remains available. Actual I/O, corruption,
cancellation and cleanup failures are not converted into success, including
when joined with a resource error.

A disk cap may fit the two input files but not a third temporary copy. Skipping
that compaction preserves queries whose final merge can delete both inputs
before writing the output. The disk cap itself is never raised or bypassed.

After the new file and full replacement array are admitted, the partition owner
is moved atomically. Only then are the two old files deleted. If a delete fails,
the new run remains the sole authoritative state; rolling back to partly deleted
old runs would be incorrect. The store retains all undeleted files and their
real path/handle/quota charges for cleanup. The old array can be released once
its references have ended, independently of the store's surviving file owners.

Failed attempts need not return total usage to the entry value. For example,
creating the optional file can expand the query handle map from 8 to 16 or 16 to
32 slots. Deleting that temporary file leaves the larger map genuinely live.
Its high-water charge remains until the store has no live handles. This is not
a leaked file or permission to remove a still-live capacity charge.

## Consuming merge and repartition owners

EOF merge and recursive repartition take a pointer to the unique source Slice,
move it locally and clear the caller. Production callers pass owners by address,
not copies of their Values views. After successful source deletion, the source
array is released before output admission or the next recursive generation.
Restore readers end their payload/header borrows before their restored owner
closes. Errors after consuming an owner are terminal for reading; stream/query
cleanup retains responsibility for every undeleted file.

## Scale boundaries

This is not an arbitrary-input, fixed-memory guarantee. The input optimization
needs one fitting two-run union. Wide state, large keys or a large union may
exceed the state or compacted-payload target; insufficient shared-query headroom
can also prevent the temporary working set. The attempt then leaves its sources
intact. Continued input can still exhaust metadata before EOF, where the existing
radix fallback would otherwise apply. A single huge source row or restore object
must fit its actual admitted phase.

Private store nodes, the query handle map, uncompressed input references,
recursive child references and eagerly persisted output references still scale
with simultaneously live runs. Extremely long directories consume real memory.
Arbitrarily many output runs, such as one run per group, may correctly fail a
small budget. Query-map high-water capacity does not shrink after each deletion.
Radix recursion remains depth-limited and cannot promise to separate arbitrary
hash collisions. Moving only operator references to disk would not make the
underlying file registries constant-size.

The existing hard memory and disk limits remain mandatory. This patch does not
claim complete accounting for every unrelated aggregate/source allocation, nor
fixed process RSS, allocator overhead or future Go map implementations. A failed
resource admission remains preferable to silently dropping live ownership.
