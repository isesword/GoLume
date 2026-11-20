# Bounded Decimal partial-run merge

Internal single-pass primitive. Not connected to query execution.

## Audited boundary

N29 `engine/decimal_spill_state.go:restoreDecimalPartialRun` uses `memory/spill.go:QuerySpillStore.Restore`, which allocates a complete payload after reserving its payload+codec length, then allocates a separate typed records owner. This is not a streaming cursor. N30 explicitly bounds that mechanism rather than claiming arbitrary-run streaming.

## Hard scope and ordering

Fixed constants: at most 8 input files, at most 64 KiB of committed physical bytes per input (checked with handle.Bytes before Restore), at most 256 records per decoded input. Each file is one sorted unique run. Ordering is NULL first, then signed Int64 ascending; only one NULL record may exist in each run. All files must have the same explicit input precision/scale and live handles, and must belong to the destination QuerySpillStore so its finite quota includes all inputs and outputs. Repeated owner or store+handle ID is rejected. An oversized file is rejected before payload restore; a checksum-valid file with too many records is rejected after bounded restore. At most 2048 input records exist. Empty runs are allowed. Zero-row identity records may participate in merging, but a final CountRows=0 record is not emitted because it does not establish group existence. Positive-row all-NULL groups are retained.

Output is an ordered sequence of at most 8 N29 files, each at most 256 records. Same-key contributions across all current inputs are merged before adding a record to a page. Page boundaries therefore cannot split a key; all nonempty pages are globally strictly increasing. No Finalize is called. Input runs need not occupy disjoint key ranges. No implicit sorting, duplicate repair or change to old N29 encoding.

## Internal API

`mergeDecimalPartialRuns(ctx context.Context, store *memory.QuerySpillStore, metadata decimalPartialMetadata, inputs []*decimalPartialFile, manager memory.Manager) (*mergedDecimalPartialRun, error)`

`mergedDecimalPartialRun` owns a fixed array of 8 output file pointers, page count, and an accounting lease. `close(ctx) error` deletes each owned page, preserves failed handles for retry and releases its lease only after all pages close. Access is internal/borrowed via fields; no uncharged append/map/cache.

Inputs and their files remain caller-owned on success and failure. Existing input-file owners and metadata remain accounted by their caller; the finite merger manager accounts for new merger allocations, not an independently certified total of all query memory. The merger only borrows them for this call and closes its restored typed copies. Caller must not mutate/close input files concurrently. Output is published to caller only after all validation/merging/page writes and final context check. If cleanup fails during a failed operation, return the retained output owner together with joined error, never drop its handles. A failed result is cleanup-only, not consumable. Inputs are never implicitly deleted, so the disk peak includes all input files plus committed output pages. Finite store and finite manager required, also on empty inputs.

## Memory proof and admission

Before creating the merger owner, reserve a conservative fixed bookkeeping allowance plus storage for the fixed input-owner/index arrays and output-file-pointer array, and one 256-record output buffer. Use exact unsafe.Sizeof for fixed-size constants, not unaccounted growable slices. Restore inputs serially. Each Restore temporarily holds one bounded wire payload alongside previously restored typed inputs and its new typed copy. Retain at most 8 typed owners, each <=256 records; close them on every exit. Output page flush uses existing N29 budgeted encoder and SpillWithMetadata, so typed inputs + output buffer + encoded page + file-owner/store metadata are simultaneously charged. The fixed owner/workspace reservation is conservatively retained until output-owner close, even after the merge workspace becomes unreachable. This deliberately over-reserves rather than undercounts borrowed output lifetime. Fixed local record candidates do not create heap caches. Cancellation checks cover input preflight, order validation, merge iteration, flush and final publication.

An adversarial encoded count may cause a bounded typed allocation before the 256-count check, but the 64KiB physical cap bounds that allocation as well. No arbitrary-size decode allocation is reachable through this merger. Existing QuerySpillStore validates real file length equals store-held, validated handle bytes before payload allocation.

## Disk and failure contract

Use only existing SpillWithMetadata via N29 spill helper. No input deletion while writing. Exact quota/-1 tests must measure actual handle.Bytes/store.Used, not assume payload size equals physical file size. Failed Reserve/Restore/merge/count overflow/corruption/write/cancellation cleans temporary memory and output files, joining cleanup errors and retaining retryable output owner if necessary. Delete retry is explicit.

## Deliberate limits

This is one bounded merge invocation, not a multi-pass merge scheduler or a grouped-spill executor. Output pages describe one sorted logical run, but this API does not promise arbitrary page-list input or arbitrary fan-in in a future pass. The pages are separately valid bounded N29 runs and may only be reused within this invocation's same hard file-count/byte/record limits. A future scheduler must retain global page ordering/exhaustion and group-completion proofs. No dispatcher, Bounded admission, group table, spill guard, AVG or end-of-group Finalize integration is changed.

## Files

Production is isolated in `engine/decimal_run_merge.go`. The N29 protocol and all existing query admission guards remain unchanged. Independent reference, resource and lifecycle tests validate this boundary.
