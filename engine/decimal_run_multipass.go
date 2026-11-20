package engine

import (
	"context"
	"errors"
	"fmt"
	"unsafe"

	"gocolumn/memory"
)

const decimalMultiMaxRuns = 64
const decimalMultiMaxPages = 256

// decimalPartialLogicalRun borrows an ordered page sequence from its caller.
// Every record, including page boundaries, must be strictly key-increasing.
type decimalPartialLogicalRun struct{ pages []*decimalPartialFile }

// multipassDecimalPartialRun owns all remaining generated files. A result
// returned with an error is cleanup-only, not a partially consumable run.
type multipassDecimalPartialRun struct {
	pages     [decimalMultiMaxPages]*decimalPartialFile
	pageCount int
	passes    int
	cleanup   [2 * decimalMultiMaxPages]*decimalPartialFile
	lease     memory.Reservation
}

func (r *multipassDecimalPartialRun) close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("decimal multipass close requires context")
	}
	var result error
	for i, f := range r.cleanup {
		if f == nil {
			continue
		}
		if err := f.close(ctx); err != nil {
			result = errors.Join(result, err)
		} else {
			r.cleanup[i] = nil
		}
	}
	if result == nil {
		clear(r.pages[:])
		r.pageCount = 0
		if r.lease != nil {
			r.lease.Release()
			r.lease = nil
		}
	}
	return result
}

func (r *multipassDecimalPartialRun) remember(f *decimalPartialFile) {
	// At most 256 old and 256 new files coexist. The writer checks the new
	// generation bound before each spill, so this fixed registry cannot fill.
	for i := range r.cleanup {
		if r.cleanup[i] == nil {
			r.cleanup[i] = f
			return
		}
	}
	panic("decimal multipass ownership bound violated")
}

func (r *multipassDecimalPartialRun) discard(ctx context.Context, f *decimalPartialFile) error {
	if err := f.close(ctx); err != nil {
		return err
	}
	for i := range r.cleanup {
		if r.cleanup[i] == f {
			r.cleanup[i] = nil
			break
		}
	}
	return nil
}

type decimalMultiSpan struct{ start, count int }
type decimalMultiCursor struct {
	span     decimalMultiSpan
	page     int
	position int
	decoded  *decodedDecimalPartialRun
	last     decimalPartialRecord
	hasLast  bool
}
type decimalMultiWork struct {
	pages   [2][decimalMultiMaxPages]*decimalPartialFile
	runs    [2][decimalMultiMaxRuns]decimalMultiSpan
	cursors [decimalMergeMaxInputs]decimalMultiCursor
	buffer  [decimalMergePageRecords]decimalPartialRecord
}

func (c *decimalMultiCursor) close() { c.decoded.close(); c.decoded = nil }

func (c *decimalMultiCursor) ready(ctx context.Context, pages *[decimalMultiMaxPages]*decimalPartialFile, manager memory.Manager) error {
	for c.decoded == nil || c.position >= len(c.decoded.records) {
		c.close()
		if err := ctx.Err(); err != nil {
			return err
		}
		if c.page >= c.span.count {
			return nil
		}
		file := pages[c.span.start+c.page]
		c.page++
		run, err := restoreDecimalPartialRun(ctx, file, manager)
		if err != nil {
			return err
		}
		c.decoded = run
		c.position = 0
		if len(run.records) > decimalMergePageRecords {
			return fmt.Errorf("decimal multipass input page exceeds record bound")
		}
		for _, record := range run.records {
			if err := ctx.Err(); err != nil {
				return err
			}
			if c.hasLast && !decimalPartialKeyLess(c.last, record) {
				return fmt.Errorf("%w: decimal multipass logical run is not strictly ordered and unique", memory.ErrSpillCorrupt)
			}
			c.last = record
			c.hasLast = true
		}
	}
	return nil
}

// mergeDecimalPartialRunsMultiPass merges bounded logical runs, not individual
// page counts. It never finalizes SUM and does not enable query spill execution.
func mergeDecimalPartialRunsMultiPass(ctx context.Context, store *memory.QuerySpillStore, metadata decimalPartialMetadata, inputs []decimalPartialLogicalRun, manager memory.Manager) (*multipassDecimalPartialRun, error) {
	if ctx == nil || store == nil || store.Limit() <= 0 || manager == nil || manager.Limit() <= 0 {
		return nil, fmt.Errorf("decimal multipass requires context, finite store and finite manager")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := metadata.validate(); err != nil {
		return nil, err
	}
	if len(inputs) > decimalMultiMaxRuns {
		return nil, fmt.Errorf("decimal multipass logical run bound exceeded")
	}
	total := 0
	for _, input := range inputs {
		if len(input.pages) > decimalMultiMaxPages-total {
			return nil, fmt.Errorf("decimal multipass total page bound exceeded")
		}
		total += len(input.pages)
	}
	// Input slices are caller-owned. All copied manifests, cursors, registry,
	// page buffer and output descriptor storage are admitted before allocation.
	charge := int64(unsafe.Sizeof(multipassDecimalPartialRun{})) + int64(unsafe.Sizeof(decimalMultiWork{})) + 256
	lease, err := manager.Reserve(ctx, charge)
	if err != nil {
		return nil, err
	}
	out := &multipassDecimalPartialRun{lease: lease}
	work := &decimalMultiWork{}
	defer func() {
		for i := range work.cursors {
			work.cursors[i].close()
		}
	}()
	fail := func(err error) (*multipassDecimalPartialRun, error) {
		if cleanup := out.close(context.Background()); cleanup != nil {
			return out, errors.Join(err, cleanup)
		}
		return nil, err
	}
	offset := 0
	for i, input := range inputs {
		work.runs[0][i] = decimalMultiSpan{start: offset, count: len(input.pages)}
		for _, f := range input.pages {
			if err := ctx.Err(); err != nil {
				return fail(err)
			}
			if f == nil || f.store != store || f.handle == nil || f.metadata != metadata {
				return fail(fmt.Errorf("decimal multipass input store, metadata or lifetime invalid"))
			}
			if f.handle.Bytes() <= 0 || f.handle.Bytes() > decimalMergeMaxFileBytes {
				return fail(fmt.Errorf("decimal multipass input physical page bound exceeded"))
			}
			for j := 0; j < offset; j++ {
				other := work.pages[0][j]
				if f == other || f.handle.ID() == other.handle.ID() {
					return fail(fmt.Errorf("decimal multipass duplicate input handle"))
				}
			}
			work.pages[0][offset] = f
			offset++
		}
	}
	if len(inputs) == 0 {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		return out, nil
	}
	current, runCount, pageCount := 0, len(inputs), total
	borrowed := true
	for {
		next := 1 - current
		clear(work.pages[next][:])
		clear(work.runs[next][:])
		nextPages, nextRuns := 0, 0
		for first := 0; first < runCount; first += decimalMergeMaxInputs {
			if err := ctx.Err(); err != nil {
				return fail(err)
			}
			n := min(decimalMergeMaxInputs, runCount-first)
			for i := range work.cursors {
				work.cursors[i].close()
				work.cursors[i] = decimalMultiCursor{}
			}
			for i := 0; i < n; i++ {
				work.cursors[i].span = work.runs[current][first+i]
			}
			start, used := nextPages, 0
			flush := func() error {
				if used == 0 {
					return nil
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				if nextPages >= decimalMultiMaxPages {
					return fmt.Errorf("decimal multipass output page bound exceeded")
				}
				page, err := spillDecimalPartialRun(ctx, store, metadata, work.buffer[:used], manager)
				if page != nil {
					out.remember(page)
					work.pages[next][nextPages] = page
					nextPages++
				}
				if err != nil {
					return err
				}
				clear(work.buffer[:used])
				used = 0
				return nil
			}
			for {
				if err := ctx.Err(); err != nil {
					return fail(err)
				}
				selected := -1
				for i := 0; i < n; i++ {
					c := &work.cursors[i]
					if err := c.ready(ctx, &work.pages[current], manager); err != nil {
						return fail(err)
					}
					if c.decoded == nil {
						continue
					}
					if selected < 0 || decimalPartialKeyLess(c.decoded.records[c.position], work.cursors[selected].decoded.records[work.cursors[selected].position]) {
						selected = i
					}
				}
				if selected < 0 {
					break
				}
				c := &work.cursors[selected]
				record := c.decoded.records[c.position]
				c.position++
				for i := 0; i < n; i++ {
					if i == selected {
						continue
					}
					other := &work.cursors[i]
					if other.decoded == nil {
						continue
					}
					candidate := other.decoded.records[other.position]
					if !decimalPartialKeyLess(record, candidate) && !decimalPartialKeyLess(candidate, record) {
						if err := mergeDecimalPartialRecord(ctx, &record, metadata, metadata, candidate); err != nil {
							return fail(err)
						}
						other.position++
					}
				}
				if record.CountRows == 0 {
					continue
				}
				work.buffer[used] = record
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
			work.runs[next][nextRuns] = decimalMultiSpan{start: start, count: nextPages - start}
			nextRuns++
		}
		// Only a completely committed new generation can replace owned old pages.
		// Initial pages always stay borrowed. A failed delete preserves the registry.
		if !borrowed {
			for i := 0; i < pageCount; i++ {
				if err := ctx.Err(); err != nil {
					return fail(err)
				}
				if err := out.discard(ctx, work.pages[current][i]); err != nil {
					return fail(err)
				}
			}
		}
		out.passes++
		if nextRuns == 1 {
			if err := ctx.Err(); err != nil {
				return fail(err)
			}
			copy(out.pages[:], work.pages[next][:nextPages])
			out.pageCount = nextPages
			return out, nil
		}
		// Strict progress follows ceil(logicalRuns/8), even for disjoint keys whose
		// physical page count does not decrease.
		current, runCount, pageCount, borrowed = next, nextRuns, nextPages, false
	}
}
