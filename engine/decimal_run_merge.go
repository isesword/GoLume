package engine

import (
	"context"
	"errors"
	"fmt"
	"unsafe"

	"gocolumn/memory"
)

const decimalMergeMaxInputs = 8
const decimalMergeMaxFileBytes = 64 << 10
const decimalMergePageRecords = 256

// mergedDecimalPartialRun owns one bounded, ordered sequence of partial pages.
// A nonnil result accompanying an error is cleanup-only, never consumable.
type mergedDecimalPartialRun struct {
	pages     [decimalMergeMaxInputs]*decimalPartialFile
	pageCount int
	lease     memory.Reservation
}

func (r *mergedDecimalPartialRun) close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("decimal merge close requires context")
	}
	var result error
	for i, page := range r.pages {
		if page == nil {
			continue
		}
		if err := page.close(ctx); err != nil {
			result = errors.Join(result, err)
		} else {
			r.pages[i] = nil
		}
	}
	if result == nil {
		r.pageCount = 0
		if r.lease != nil {
			r.lease.Release()
			r.lease = nil
		}
	}
	return result
}

func decimalPartialKeyLess(a, b decimalPartialRecord) bool {
	if a.KeyValid != b.KeyValid {
		return !a.KeyValid
	}
	return a.KeyValid && a.Key < b.Key
}

// mergeDecimalPartialRuns is a single bounded merge, not an executor or a
// multi-pass scheduler. Input files remain borrowed and unchanged on all exits.
func mergeDecimalPartialRuns(ctx context.Context, store *memory.QuerySpillStore, metadata decimalPartialMetadata, inputs []*decimalPartialFile, manager memory.Manager) (*mergedDecimalPartialRun, error) {
	if ctx == nil || store == nil || store.Limit() <= 0 || manager == nil || manager.Limit() <= 0 {
		return nil, fmt.Errorf("decimal merge requires context, finite store and finite manager")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := metadata.validate(); err != nil {
		return nil, err
	}
	if len(inputs) > decimalMergeMaxInputs {
		return nil, fmt.Errorf("decimal merge fan-in exceeds %d", decimalMergeMaxInputs)
	}
	for i, input := range inputs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if input == nil || input.store != store || input.handle == nil || input.metadata != metadata {
			return nil, fmt.Errorf("decimal merge input is closed, belongs to another store or metadata mismatches")
		}
		if input.handle.Bytes() <= 0 || input.handle.Bytes() > decimalMergeMaxFileBytes {
			return nil, fmt.Errorf("decimal merge input exceeds physical byte bound")
		}
		for j := 0; j < i; j++ {
			if inputs[j] == input || (inputs[j].store == input.store && inputs[j].handle.ID() == input.handle.ID()) {
				return nil, fmt.Errorf("decimal merge duplicate input handle")
			}
		}
	}
	// The fixed state, page records and descriptors are admitted before allocation.
	type mergeWork struct {
		decoded   [decimalMergeMaxInputs]*decodedDecimalPartialRun
		positions [decimalMergeMaxInputs]int
		records   [decimalMergePageRecords]decimalPartialRecord
	}
	charge := int64(unsafe.Sizeof(mergedDecimalPartialRun{})) + int64(unsafe.Sizeof(mergeWork{})) + 256
	lease, err := manager.Reserve(ctx, charge)
	if err != nil {
		return nil, err
	}
	out := &mergedDecimalPartialRun{lease: lease}
	work := &mergeWork{}
	defer func() {
		for _, run := range work.decoded {
			run.close()
		}
	}()
	fail := func(err error) (*mergedDecimalPartialRun, error) {
		if cleanup := out.close(context.Background()); cleanup != nil {
			return out, errors.Join(err, cleanup)
		}
		return nil, err
	}
	// Validate every complete bounded input before committing any output file.
	for i, input := range inputs {
		run, err := restoreDecimalPartialRun(ctx, input, manager)
		if err != nil {
			return fail(err)
		}
		work.decoded[i] = run
		if len(run.records) > decimalMergePageRecords {
			return fail(fmt.Errorf("decimal merge input exceeds record bound"))
		}
		for j := range run.records {
			if err := ctx.Err(); err != nil {
				return fail(err)
			}
			if j > 0 && !decimalPartialKeyLess(run.records[j-1], run.records[j]) {
				return fail(fmt.Errorf("%w: decimal merge input is not strictly ordered and unique", memory.ErrSpillCorrupt))
			}
		}
	}
	used := 0
	flush := func() error {
		if used == 0 {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if out.pageCount >= decimalMergeMaxInputs {
			return fmt.Errorf("decimal merge output page bound exceeded")
		}
		page, err := spillDecimalPartialRun(ctx, store, metadata, work.records[:used], manager)
		// Spill may return a live retryable owner together with an error.
		if page != nil {
			out.pages[out.pageCount] = page
			out.pageCount++
		}
		if err != nil {
			return err
		}
		clear(work.records[:used])
		used = 0
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		selected := -1
		for i := range inputs {
			run, pos := work.decoded[i], work.positions[i]
			if pos >= len(run.records) {
				continue
			}
			if selected < 0 || decimalPartialKeyLess(run.records[pos], work.decoded[selected].records[work.positions[selected]]) {
				selected = i
			}
		}
		if selected < 0 {
			break
		}
		next := work.decoded[selected].records[work.positions[selected]]
		work.positions[selected]++
		for i := range inputs {
			if i == selected {
				continue
			}
			run, pos := work.decoded[i], work.positions[i]
			if pos >= len(run.records) {
				continue
			}
			candidate := run.records[pos]
			if !decimalPartialKeyLess(next, candidate) && !decimalPartialKeyLess(candidate, next) {
				if err := mergeDecimalPartialRecord(ctx, &next, metadata, metadata, candidate); err != nil {
					return fail(err)
				}
				work.positions[i]++
			}
		}
		// An empty arithmetic identity cannot establish that a group existed.
		if next.CountRows == 0 {
			continue
		}
		work.records[used] = next
		used++
		if used == decimalMergePageRecords {
			if err := flush(); err != nil {
				return fail(err)
			}
		}
	}
	if err := flush(); err != nil {
		return fail(err)
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	return out, nil
}
