# Bounded multi-pass Decimal partial-run merge

This internal primitive extends the N29 state protocol and N30 single-pass merge with logical-run scheduling. It does not connect to the query dispatcher, enable Bounded grouped aggregation, sort arbitrary rows, or call SUM Finalize.

## API and hard contract

`mergeDecimalPartialRunsMultiPass(ctx, store, metadata, inputs []decimalPartialLogicalRun, manager)` returns `(*multipassDecimalPartialRun, error)`. Each input descriptor borrows `pages []*decimalPartialFile` in logical order. Successful output exposes `pages`, `pageCount` and `passes`; callers must close its owner. Error results with a nonnil owner are cleanup-only.

- At most 64 initial logical runs, with at most 256 input pages in total. Total-page admission uses subtraction before addition to avoid integer overflow.
- Every page has positive committed physical handle size at most 64 KiB, checked before any restore, and at most 256 accepted records. Hence at most 65,536 accepted input records.
- All inputs belong to the destination finite QuerySpillStore and have identical input precision/scale. Duplicate file pointers or handle IDs, including aliases across different runs, are rejected.
- NULL sorts first; valid keys use signed Int64 ascending order. Each logical input is strictly ordered and unique both inside pages and across page boundaries. Empty pages and empty logical runs are allowed. Cross-page validation remembers the last actual record across empty pages.
- Zero CountRows records are arithmetic identities, not proof of group existence. Final merged zero-row records are omitted. Positive-row all-NULL groups remain.
- Input records retain N29 count/state validation. No intermediate or final precision Finalize occurs here, so wide states may cancel in later passes.

The finite merger manager accounts for newly allocated merger state. Existing input descriptors, file owners and their metadata remain caller-owned/accounted. This is not an independent certification of all query memory.

## Logical progress and pages

Each pass merges consecutive groups of up to eight logical runs. The next logical-run count is `ceil(current/8)`, not the physical page count. Thus completely disjoint keys still terminate: 64 logical runs become 8 and then 1 even if all 256 pages remain. Every group produces a single logical run represented by an ordered page span; at most 256 records are packed in each output page. Same-key heads of every participating run are combined before publication to the buffer. Strict uniqueness within an input makes at most one contribution per logical run possible; the following cursor advance validates the next page before another record can be consumed.

Output pages never split a key. Page-count conservation follows because each group's packed output count is no larger than its input record count; `ceil(sum records/256)` is at most the number of input pages. Each generation therefore has at most 256 pages. Output also has at most 256 pages; no unbounded manifest is created.

An initial single logical run undergoes one copying/validation pass, preserving borrowed ownership and identity filtering. Nonzero input run count consisting entirely of empty runs still follows the same pass schedule and produces no pages. Zero initial logical runs produce an empty owned result with zero passes. `passes` counts completed physical generation transitions, including the single-run copying pass.

## Memory admission and cursors

One reservation precedes allocation of fixed manifests for two generations, all 64 run spans per generation, eight cursors, the 256-record output buffer, output descriptor and 512-entry ownership registry, plus bookkeeping. It uses the fixed structures' unsafe.Sizeof and is conservatively retained until the output owner closes, including on cleanup-only errors.

Each active cursor retains at most one decoded page. Moving to the next page closes the preceding typed owner first. N29 Restore temporarily holds its bounded wire payload and a newly allocated typed page; the other cursors retain at most seven pages. A malicious page count may cause more than 256 typed records to be allocated before post-restore rejection, but the 64 KiB physical precheck bounds that temporary allocation. The existing store validates the actual file length against its store-held handle before payload allocation.

Output encoding, temporary wire buffers, file owners and handle metadata use the N29 budgeted SpillWithMetadata path. All coexistence is charged; no map, growing cache or unbounded page slice is created by the merger.

## File ownership, disk peaks and errors

Borrowed input files are never deleted, including on success. Owned intermediate/output files are registered immediately, including live owners returned by a failed/cancelled N29 spill. At most 256 owned old-generation pages and 256 new-generation pages coexist. Registry slots are reused only after successful deletion. No third generation starts if deletion of an old page fails.

An entire new generation is committed before retiring the previous owned generation. The same finite store accounts for borrowed inputs plus all still-live intermediate and output files. Exact disk requirements are measured from actual physical files/store usage, not inferred only from payload length.

Cancellation is checked during admission, page validation, cursor advances, merge iterations, flush, old-generation deletion and before final publication. A late corrupt or out-of-order page may be detected after earlier outputs have been written; failure cleanup still deletes all generated files and leaves borrowed input files intact. Cleanup joins errors and returns a live owner if any deletion fails. Its `close(ctx)` retries every remaining registered file, preserves unsuccessful entries, and releases the fixed reservation only when every owned file is deleted. Successful close is idempotent. No failed result is consumable and no partial output is published as success.

## Deliberate limits

This is bounded multi-pass infrastructure for at most the hard input limits above, not arbitrary-scale external aggregation. It has no group-table spill trigger, row ingestion, global query strategy admission, end-of-group executor contract, AVG extension or final result sink. N29/N30 and existing query guards remain unchanged.
