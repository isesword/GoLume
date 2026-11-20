# One-pass TopK for top-level three-axis List payloads

The existing TopK constructor admits canonical top-level three-axis Lists over
Int64 or String as payload alongside existing scalar ordering keys. The full
payload schema selects a one-pass snapshot strategy even when both sources
advertise stable replay. The same strategy predicate is used by StreamPlan and
NewBatchStream. The key input is schema/key-checked but never opened on this
route. Narrow key inputs remain legal. A depth-three key-input-only schema with
a shallow full input remains rejected. K=0 opens neither source.

This is not depth-three replay support. Depth-three Lists under Struct parents,
mixed native TIME, new leaves, List ordering keys, and spill remain outside the
contract. Old shallow, masked, and opaque routes keep their prior strategy and
admission. In particular opaque Arrow List metadata is not a canonical native
profile, so its old route is not silently changed.

## Reused strict owner and bounded overlap

The existing container snapshot stream validates each delivered full batch
before considering any heap winner. This includes zero-row batches and losing
rows: malformed offsets, required NULLs and metadata are not skipped merely
because the scalar key would lose. Hidden payload and structural semantics
remain those of the strict resident nested validator. Native three-axis payload
combined with opaque runtime siblings is rejected. N39 nullable selection is
not used.

Every admitted candidate uses an independent strict resident accumulator and
workspace. Original input ordinals break ties stably. Replacement retains the
old worst candidate until the new candidate is fully admitted, so atomic
replacement can hold K+1 snapshots. Failure terminates and cleans up rather
than exposing a partially replaced winner set. Per-candidate workspaces are
released on eviction instead of accumulating a high-water mark across the
input. Heap backing growth admits old and new arrays simultaneously.

Source batches coexist with candidate state. Output materializes at most 64
winners per page (or the smaller configured BatchRows), not all K at once. Each
snapshot stays charged until its append succeeds; the page overlaps all still
live winners. Borrowed output descriptors, rebased offsets and bitmaps use the
existing recursive budget calculation. N40's actual-axis retained metadata
charge includes all four nodes. This slice adds no owner, payload copy, quota
increase, or blanket extra header allowance.

Input Close and its errors occur before publication. Source poison cannot
change owned candidates. Previous output is invalidated on Next/Close; its
page and remaining candidates are then released through the terminal owner.
Variable-width rows remain variable-sized: K bounds candidate count, not bytes.
A single large String payload or K+1 overlap can legitimately exceed a finite
budget. Accounted buffers are not a claim of whole-process RSS accounting.

## Acceptance evidence

Real native Parquet scan, Sort+Limit lowering, actual strategy diagnostics,
borrowed typed output and IPC/Parquet round trips must agree with independent
stable TopK results. Separate checks cover replayable and one-shot inputs,
key-input open counters, K boundaries, losing invalid rows, source poison,
replacement/page overlap, exact/minus-one budgets and fault/cancel cleanup.
Constructor success alone is not end-to-end acceptance.
