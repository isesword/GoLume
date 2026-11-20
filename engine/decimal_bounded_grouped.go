package engine

import (
	"context"
	"errors"
	"fmt"
	"unsafe"

	"gocolumn/aggregate"
	"gocolumn/memory"
)

const decimalBoundedBatchRows = 256
const decimalBoundedMaxRows = 65536
const decimalBoundedMaxBatches = 4096

// DecimalBoundedInputRow is one nullable Int64 key and native Decimal128 value.
type DecimalBoundedInputRow struct {
	Key        int64
	KeyValid   bool
	Value      memory.Decimal128
	ValueValid bool
}

// DecimalBoundedInputBatch is an engine-owned fixed-capacity destination. Input
// implementations fill only Records[:Rows] and must set metadata on every batch.
type DecimalBoundedInputBatch struct {
	Rows             int
	Precision, Scale int32
	Records          [decimalBoundedBatchRows]DecimalBoundedInputRow
}

// DecimalBoundedInput fills fixed typed batches, not arbitrary borrowed slices.
// ExecuteDecimalBoundedGrouped takes ownership of every nonnil input immediately
// and calls Close once, including when admission fails. Calls are single-consumer.
type DecimalBoundedInput interface {
	Next(context.Context, *DecimalBoundedInputBatch) (bool, error)
	Close() error
}

// DecimalBoundedAggregateMode selects explicit final outputs from one wide state.
// Counts are always included. Zero preserves the original SUM/count contract.
type DecimalBoundedAggregateMode uint8

const (
	// DecimalBoundedSumCounts emits SUM, COUNT(value) and COUNT(rows).
	DecimalBoundedSumCounts DecimalBoundedAggregateMode = iota
	// DecimalBoundedAverageCounts emits AVG and counts without narrowing SUM.
	DecimalBoundedAverageCounts
	// DecimalBoundedSumAverageCounts emits both SUM and AVG, and both counts.
	DecimalBoundedSumAverageCounts
)

// DecimalBoundedGroupedOptions explicitly selects this limited spill executor.
// Manager and Store must be finite; TableCapacity is 1..256 or zero for a
// conservative budget-based choice. No existing query strategy is changed.
type DecimalBoundedGroupedOptions struct {
	Manager                        memory.Manager
	Store                          *memory.QuerySpillStore
	Precision, Scale               int32
	TableCapacity                  int
	Mode                           DecimalBoundedAggregateMode
	AveragePrecision, AverageScale int32
	AverageRounding                aggregate.DecimalAverageRoundingMode
}

// DecimalBoundedOutputRow contains selected nullable SUM/AVG and nonnull counts.
// Unselected values are zero and invalid; Mode identifies which outputs exist.
type DecimalBoundedOutputRow struct {
	Key                   int64
	KeyValid              bool
	Sum                   memory.Decimal128
	SumValid              bool
	Average               memory.Decimal128
	AverageValid          bool
	CountValid, CountRows uint64
}

// DecimalBoundedOutputBatch is borrowed until the next Next call or Close.
// Precision/Scale describe SUM (38/input scale), including NULLs. Average
// metadata is explicit in AVG modes and zero when AVG is not selected.
type DecimalBoundedOutputBatch struct {
	Rows                           int
	Precision, Scale               int32
	Records                        [decimalBoundedBatchRows]DecimalBoundedOutputRow
	Mode                           DecimalBoundedAggregateMode
	AveragePrecision, AverageScale int32
}

// DecimalBoundedGroupedStats records actual input, flush and merge activity.
type DecimalBoundedGroupedStats struct {
	TableCapacity, InputRows, InputBatches, Flushes, MergePasses, Groups int
}

// DecimalBoundedGroupedResult owns spill files and its output buffer. A nonnil
// result accompanying an execution error is cleanup-only; Close retries deletes.
// Stats is informational. Next and Close are single-consumer, not concurrent.
type DecimalBoundedGroupedResult struct {
	Stats        DecimalBoundedGroupedStats
	initial      [decimalMultiMaxRuns]*decimalPartialFile
	initialCount int
	merged       *multipassDecimalPartialRun
	metadata     decimalPartialMetadata
	mode         DecimalBoundedAggregateMode
	average      aggregate.DecimalAverageOptions
	manager      memory.Manager
	lease        memory.Reservation
	output       DecimalBoundedOutputBatch
	nextPage     int
	closed       bool
	terminal     error
}

type decimalBoundedWork struct {
	input DecimalBoundedInputBatch
	runs  [decimalMultiMaxRuns]decimalPartialLogicalRun
	pages [decimalMultiMaxRuns][1]*decimalPartialFile
}

// Close invalidates borrowed output and deletes all remaining owned files.
// Failed deletion retains ownership and accounting for a later Close retry.
func (r *DecimalBoundedGroupedResult) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.output = DecimalBoundedOutputBatch{}
	r.closed = true
	if ctx == nil {
		return fmt.Errorf("decimal bounded Close requires context")
	}
	var err error
	if r.merged != nil {
		if e := r.merged.close(ctx); e != nil {
			err = errors.Join(err, e)
		} else {
			r.merged = nil
		}
	}
	for i, f := range r.initial {
		if f == nil {
			continue
		}
		if e := f.close(ctx); e != nil {
			err = errors.Join(err, e)
		} else {
			r.initial[i] = nil
		}
	}
	if err == nil && r.lease != nil {
		r.lease.Release()
		r.lease = nil
	}
	return err
}

// Next returns the next native typed batch. Every call invalidates the previous
// borrowed batch, including EOF and errors. Errors perform retryable cleanup.
func (r *DecimalBoundedGroupedResult) Next(ctx context.Context) (*DecimalBoundedOutputBatch, bool, error) {
	if r == nil {
		return nil, false, fmt.Errorf("nil decimal bounded result")
	}
	r.output = DecimalBoundedOutputBatch{}
	fail := func(e error) (*DecimalBoundedOutputBatch, bool, error) {
		r.terminal = e
		e = errors.Join(e, r.Close(context.Background()))
		return nil, false, e
	}
	if r.terminal != nil {
		return nil, false, r.terminal
	}
	if r.closed {
		return nil, false, fmt.Errorf("decimal bounded result closed")
	}
	if ctx == nil {
		return fail(fmt.Errorf("decimal bounded Next requires context"))
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if r.merged == nil || r.nextPage >= r.merged.pageCount {
		return nil, false, nil
	}
	page, err := restoreDecimalPartialRun(ctx, r.merged.pages[r.nextPage], r.manager)
	if err != nil {
		return fail(err)
	}
	defer page.close()
	if len(page.records) > len(r.output.Records) {
		return fail(fmt.Errorf("decimal bounded output page exceeds bound"))
	}
	r.output.Precision, r.output.Scale = 38, r.metadata.Scale
	r.output.Mode = r.mode
	r.output.AveragePrecision, r.output.AverageScale = r.average.Precision, r.average.Scale
	for i, record := range page.records {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		value, err := r.finalize(record)
		if err != nil {
			return fail(err)
		}
		r.output.Records[i] = value
	}
	rows := len(page.records)
	page.close() // Release may trigger cancellation; check before publication.
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	r.output.Rows = rows
	r.nextPage++
	return &r.output, true, nil
}

// finalize is shared by whole-result validation and batch publication. AVG-only
// deliberately never calls Finalize: its exact sum need not fit precision 38.
func (r *DecimalBoundedGroupedResult) finalize(record decimalPartialRecord) (DecimalBoundedOutputRow, error) {
	out := DecimalBoundedOutputRow{Key: record.Key, KeyValid: record.KeyValid, CountValid: record.CountValid, CountRows: record.CountRows}
	if r.mode != DecimalBoundedAverageCounts {
		value, valid, err := record.Sum.Finalize()
		if err != nil {
			return DecimalBoundedOutputRow{}, err
		}
		out.Sum, out.SumValid = value, valid
	}
	if r.mode != DecimalBoundedSumCounts {
		value, valid, err := record.Sum.FinalizeAverage(r.average)
		if err != nil {
			return DecimalBoundedOutputRow{}, err
		}
		out.Average, out.AverageValid = value, valid
	}
	return out, nil
}

// ExecuteDecimalBoundedGrouped explicitly runs unordered single-key Decimal
// SUM and/or explicit AVG, plus both counts, with finite memory and disk. It finalizes
// every resulting group before returning success, never just the first page.
// This entry point does not adapt Source, PreparedQuery, Auto or Lazy APIs.
func ExecuteDecimalBoundedGrouped(ctx context.Context, input DecimalBoundedInput, opts DecimalBoundedGroupedOptions) (result *DecimalBoundedGroupedResult, err error) {
	if input == nil {
		return nil, fmt.Errorf("decimal bounded execution requires input")
	}
	defer func() {
		err = errors.Join(err, input.Close())
		if ctx != nil {
			err = errors.Join(err, ctx.Err())
		}
		if err != nil && result != nil {
			result.terminal = err
			if cleanup := result.Close(context.Background()); cleanup != nil {
				err = errors.Join(err, cleanup)
			} else {
				result = nil
			}
		}
	}()
	if ctx == nil || opts.Manager == nil || opts.Manager.Limit() <= 0 || opts.Store == nil || opts.Store.Limit() <= 0 {
		return nil, fmt.Errorf("decimal bounded execution requires context, finite manager and finite store")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	metadata := decimalPartialMetadata{Precision: opts.Precision, Scale: opts.Scale}
	if err := metadata.validate(); err != nil {
		return nil, err
	}
	var average aggregate.DecimalAverageOptions
	switch opts.Mode {
	case DecimalBoundedSumCounts:
		if opts.AveragePrecision != 0 || opts.AverageScale != 0 || opts.AverageRounding != 0 {
			return nil, fmt.Errorf("%w: SUM-only mode has AVG configuration", aggregate.ErrDecimalAverageOptions)
		}
	case DecimalBoundedAverageCounts, DecimalBoundedSumAverageCounts:
		average = aggregate.DecimalAverageOptions{InputScale: opts.Scale, Precision: opts.AveragePrecision, Scale: opts.AverageScale, Mode: opts.AverageRounding}
		var empty aggregate.DecimalSumState
		if _, _, err := empty.FinalizeAverage(average); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("decimal bounded aggregate mode invalid")
	}
	if opts.TableCapacity < 0 || opts.TableCapacity > decimalBoundedBatchRows {
		return nil, fmt.Errorf("decimal bounded table capacity must be 0..256")
	}
	fixedBytes := int64(unsafe.Sizeof(DecimalBoundedGroupedResult{})) + int64(unsafe.Sizeof(decimalBoundedWork{})) + 256
	lease, err := opts.Manager.Reserve(ctx, fixedBytes)
	if err != nil {
		return nil, err
	}
	result = &DecimalBoundedGroupedResult{metadata: metadata, mode: opts.Mode, average: average, manager: opts.Manager, lease: lease}
	work := &decimalBoundedWork{}
	capacity := opts.TableCapacity
	width := int64(unsafe.Sizeof(decimalPartialRecord{}))
	if capacity == 0 {
		// Leave fixed merge descriptors, eight typed pages and two physical-page
		// allowances available. This is conservative, not a guarantee for all files.
		headroom := int64(unsafe.Sizeof(multipassDecimalPartialRun{})) + int64(unsafe.Sizeof(decimalMultiWork{})) + 256 + 8*(decimalMergePageRecords*width+256) + 2*decimalMergeMaxFileBytes
		available := opts.Manager.Limit() - opts.Manager.Used()
		if available <= headroom {
			return result, memory.ErrBudgetExceeded
		}
		capacity = int(min(int64(decimalBoundedBatchRows), (available-headroom)/width))
		if capacity < 1 {
			return result, memory.ErrBudgetExceeded
		}
	}
	tableLease, err := opts.Manager.Reserve(ctx, int64(capacity)*width)
	if err != nil {
		return result, err
	}
	defer tableLease.Release()
	table := make([]decimalPartialRecord, capacity)
	used := 0
	result.Stats.TableCapacity = capacity
	flush := func() error {
		if used == 0 {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if result.initialCount >= decimalMultiMaxRuns {
			return fmt.Errorf("decimal bounded initial run limit 64 exceeded")
		}
		// In-place insertion sort uses no unaccounted comparator closures or maps.
		for i := 1; i < used; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			value, j := table[i], i
			for j > 0 && decimalPartialKeyLess(value, table[j-1]) {
				table[j] = table[j-1]
				j--
			}
			table[j] = value
		}
		file, err := spillDecimalPartialRun(ctx, opts.Store, metadata, table[:used], opts.Manager)
		if file != nil {
			i := result.initialCount
			result.initial[i] = file
			work.pages[i][0] = file
			work.runs[i].pages = work.pages[i][:]
			result.initialCount++
		}
		if err != nil {
			return err
		}
		result.Stats.Flushes++
		clear(table[:used])
		used = 0
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		work.input = DecimalBoundedInputBatch{}
		ok, err := input.Next(ctx, &work.input)
		if err != nil {
			return result, err
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if !ok {
			break
		}
		result.Stats.InputBatches++
		if result.Stats.InputBatches > decimalBoundedMaxBatches {
			return result, fmt.Errorf("decimal bounded input batch limit 4096 exceeded")
		}
		batch := &work.input
		if batch.Rows < 0 || batch.Rows > decimalBoundedBatchRows || batch.Precision != metadata.Precision || batch.Scale != metadata.Scale {
			return result, fmt.Errorf("decimal bounded batch rows or precision/scale mismatch")
		}
		if batch.Rows > decimalBoundedMaxRows-result.Stats.InputRows {
			return result, fmt.Errorf("decimal bounded input row limit 65536 exceeded")
		}
		// Validate the full delivered batch before changing aggregate state.
		for i := 0; i < batch.Rows; i++ {
			if err := ctx.Err(); err != nil {
				return result, err
			}
			row := batch.Records[i]
			if row.ValueValid && !memory.Decimal128FitsPrecision(row.Value, metadata.Precision) {
				return result, fmt.Errorf("decimal bounded input coefficient exceeds precision")
			}
		}
		result.Stats.InputRows += batch.Rows
		for i := 0; i < batch.Rows; i++ {
			if err := ctx.Err(); err != nil {
				return result, err
			}
			row := batch.Records[i]
			slot := -1
			for j := 0; j < used; j++ {
				if table[j].KeyValid == row.KeyValid && (!row.KeyValid || table[j].Key == row.Key) {
					slot = j
					break
				}
			}
			if slot < 0 {
				if used == capacity {
					if err := flush(); err != nil {
						return result, err
					}
				}
				slot = used
				used++
				table[slot].KeyValid = row.KeyValid
				if row.KeyValid {
					table[slot].Key = row.Key
				}
			}
			record := &table[slot]
			record.CountRows++ // Hard total-input-row admission is far below uint64 overflow.
			if row.ValueValid {
				if err := record.Sum.Add(row.Value); err != nil {
					return result, err
				}
			}
			record.CountValid = record.Sum.Count()
		}
	}
	if err := flush(); err != nil {
		return result, err
	}
	merged, err := mergeDecimalPartialRunsMultiPass(ctx, opts.Store, metadata, work.runs[:result.initialCount], opts.Manager)
	result.merged = merged
	if err != nil {
		return result, err
	}
	result.Stats.MergePasses = merged.passes
	// Preflight every final group before any result handle is published.
	for i := 0; i < merged.pageCount; i++ {
		page, err := restoreDecimalPartialRun(ctx, merged.pages[i], opts.Manager)
		if err != nil {
			return result, err
		}
		for _, record := range page.records {
			if err := ctx.Err(); err != nil {
				page.close()
				return result, err
			}
			if _, err := result.finalize(record); err != nil {
				page.close()
				return result, err
			}
			result.Stats.Groups++
		}
		page.close()
	}
	// The merger borrowed initial runs. This executor owns them and retires them
	// only after the complete merged output passes final precision validation.
	for i, file := range result.initial {
		if file == nil {
			continue
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if err := file.close(ctx); err != nil {
			return result, err
		}
		result.initial[i] = nil
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	return result, nil
}
