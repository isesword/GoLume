package lazy

import (
	"context"
	"errors"
	"fmt"
	"unsafe"

	"gocolumn/aggregate"
	"gocolumn/engine"
	"gocolumn/expr"
	"gocolumn/logical"
	"gocolumn/memory"
	parquetsource "gocolumn/source/parquet"
)

// DecimalBoundedTerminalOptions borrows a finite query budget and spill store.
// The caller must keep both open until result Close (including retries) finishes.
// Only concrete Parquet scans and the documented fixed-width row plans qualify.
type DecimalBoundedTerminalOptions struct {
	Budget                         *memory.QueryBudget
	Store                          *memory.QuerySpillStore
	TableCapacity                  int
	Mode                           engine.DecimalBoundedAggregateMode
	AveragePrecision, AverageScale int32
	AverageRounding                aggregate.DecimalAverageRoundingMode
}

// DecimalBoundedTerminalResult owns generated files, not the supplied budget or
// store. A nonnil result accompanying an error is cleanup-only. Calls are serial.
type DecimalBoundedTerminalResult struct {
	Stats  engine.DecimalBoundedGroupedStats
	result *engine.DecimalBoundedGroupedResult
	input  *decimalBoundedBatchInput
	budget *memory.QueryBudget
	base   context.Context
	lease  memory.Reservation
	closed bool
}

// Close invalidates output and retries generated-file deletion. It never closes
// the caller's budget or store, even after EOF or when deletion fails.
func (r *DecimalBoundedTerminalResult) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.closed = true
	var inputErr error
	if r.input != nil {
		inputErr = r.input.Close()
	}
	var resultErr error
	if r.result != nil {
		resultErr = r.result.Close(ctx)
	}
	if resultErr == nil && r.lease != nil {
		r.lease.Release()
		r.lease = nil
	}
	return errors.Join(inputErr, resultErr)
}

// Next returns an engine-native borrowed output batch in NULL-first key order.
// The preceding batch expires on every Next or Close, including errors and EOF.
func (r *DecimalBoundedTerminalResult) Next(ctx context.Context) (*engine.DecimalBoundedOutputBatch, bool, error) {
	if r == nil {
		return nil, false, fmt.Errorf("nil decimal bounded terminal result")
	}
	fail := func(err error) (*engine.DecimalBoundedOutputBatch, bool, error) {
		return nil, false, errors.Join(err, r.Close(context.Background()))
	}
	if r.closed {
		return fail(fmt.Errorf("decimal bounded terminal result closed"))
	}
	if ctx == nil {
		return fail(fmt.Errorf("decimal bounded terminal Next requires context"))
	}
	if q := memory.QueryBudgetFromContext(ctx); q != nil && q != r.budget {
		return fail(fmt.Errorf("decimal bounded terminal context has different query budget"))
	}
	if err := r.base.Err(); err != nil {
		return fail(err)
	}
	if r.budget.Snapshot().Closed {
		return fail(memory.ErrQueryBudgetClosed)
	}
	if r.result == nil {
		return fail(fmt.Errorf("decimal bounded terminal has no consumable result"))
	}
	batch, ok, err := r.result.Next(memory.WithQueryBudget(ctx, r.budget))
	if err != nil {
		return fail(err)
	}
	if err := r.base.Err(); err != nil {
		return fail(err)
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if r.budget.Snapshot().Closed {
		return fail(memory.ErrQueryBudgetClosed)
	}
	return batch, ok, nil
}

type decimalBoundedBatchInput struct {
	reader           *BatchReader
	key, value       string
	precision, scale int32
	keys             Int64Chunk
	values           Decimal128Chunk
	row, rows        int
	closeErr         error
}

func (s *decimalBoundedBatchInput) Close() error {
	s.keys = Int64Chunk{}
	s.values = Decimal128Chunk{}
	if s.reader != nil {
		s.closeErr = s.reader.Close()
		s.reader = nil
	}
	return s.closeErr
}

func (s *decimalBoundedBatchInput) Next(ctx context.Context, dst *engine.DecimalBoundedInputBatch) (bool, error) {
	if s.reader == nil {
		return false, fmt.Errorf("decimal bounded upstream reader closed")
	}
	for s.row >= s.rows {
		s.keys = Int64Chunk{}
		s.values = Decimal128Chunk{}
		s.row, s.rows = 0, 0
		if !s.reader.Next(ctx) {
			if err := s.reader.Err(); err != nil {
				return false, fmt.Errorf("decimal bounded upstream: %w", err)
			}
			return false, nil
		}
		view := s.reader.Batch()
		if view.Len() < 0 || view.Len() > 65536 {
			return false, fmt.Errorf("decimal bounded upstream batch exceeds 65536 rows")
		}
		key, err := view.Column(s.key)
		if err != nil {
			return false, err
		}
		value, err := view.Column(s.value)
		if err != nil {
			return false, err
		}
		keys, err := key.Int64Chunks()
		if err != nil {
			return false, err
		}
		values, err := value.Decimal128Chunks()
		if err != nil {
			return false, err
		}
		if len(keys) != 1 || len(values) != 1 || keys[0].Len() != int(view.Len()) || values[0].Len() != int(view.Len()) || values[0].Precision() != s.precision || values[0].Scale() != s.scale {
			return false, fmt.Errorf("decimal bounded upstream typed chunk shape/metadata mismatch")
		}
		s.keys, s.values, s.rows = keys[0], values[0], int(view.Len())
	}
	dst.Rows = min(len(dst.Records), s.rows-s.row)
	dst.Precision, dst.Scale = s.precision, s.scale
	for i := 0; i < dst.Rows; i++ {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		key, keyValid, err := s.keys.Value(s.row + i)
		if err != nil {
			return false, err
		}
		value, valueValid, err := s.values.Value(s.row + i)
		if err != nil {
			return false, err
		}
		dst.Records[i] = engine.DecimalBoundedInputRow{Key: key, KeyValid: keyValid, Value: value, ValueValid: valueValid}
	}
	s.row += dst.Rows
	return true, nil
}

func decimalBoundedFilterGuard(e expr.Expr, remaining *int) error {
	if *remaining <= 0 {
		return fmt.Errorf("decimal bounded filter exceeds 64 expression nodes")
	}
	*remaining--
	switch n := e.(type) {
	case expr.Column:
		if len(n.Name) == 0 || len(n.Name) > 128 {
			return fmt.Errorf("decimal bounded column name exceeds bound")
		}
	case expr.Int64Literal, expr.BoolLiteral, expr.Decimal128Literal:
	case expr.Binary:
		switch n.Op {
		case expr.OpGT, expr.OpGE, expr.OpLT, expr.OpLE, expr.OpEQ, expr.OpAnd, expr.OpOr:
		default:
			return fmt.Errorf("decimal bounded filter binary operation unsupported")
		}
		if err := decimalBoundedFilterGuard(n.Left, remaining); err != nil {
			return err
		}
		return decimalBoundedFilterGuard(n.Right, remaining)
	case expr.Unary:
		switch n.Op {
		case expr.OpNot, expr.OpIsNull, expr.OpIsNotNull:
		default:
			return fmt.Errorf("decimal bounded filter unary operation unsupported")
		}
		return decimalBoundedFilterGuard(n.Value, remaining)
	default:
		return fmt.Errorf("decimal bounded filter expression %T unsupported", e)
	}
	return nil
}

func decimalBoundedPlanGuard(plan logical.Plan) error {
	remaining := 64
	for depth := 0; depth < 16; depth++ {
		switch n := plan.(type) {
		case *logical.Scan:
			if n == nil {
				return fmt.Errorf("nil decimal bounded scan")
			}
			src, ok := n.Source.(*parquetsource.Source)
			if !ok || src == nil {
				return fmt.Errorf("decimal bounded lazy terminal requires concrete Parquet source")
			}
			if err := src.ValidateDecimalBoundedTerminalInput(); err != nil {
				return err
			}
			if n.PushedPredicate != nil {
				return fmt.Errorf("decimal bounded scan annotations unsupported")
			}
			schema := src.Schema()
			if len(schema.Fields) == 0 || len(schema.Fields) > 8 {
				return fmt.Errorf("decimal bounded source requires 1..8 fixed native fields")
			}
			for _, field := range schema.Fields {
				if len(field.Name) == 0 || len(field.Name) > 128 || field.Encoding != memory.EncodingPlain || len(field.Children) != 0 || field.DictionaryDomain != "" {
					return fmt.Errorf("decimal bounded source field metadata unsupported")
				}
				switch field.Type {
				case memory.TypeInt64:
				case memory.TypeDecimal:
					if err := field.ValidateDecimal128(); err != nil {
						return err
					}
				default:
					return fmt.Errorf("decimal bounded source supports only Int64 and native Decimal fields")
				}
			}
			if len(n.Projection) > 8 {
				return fmt.Errorf("decimal bounded scan projection exceeds bound")
			}
			return nil
		case *logical.Filter:
			if n == nil {
				return fmt.Errorf("nil decimal bounded filter")
			}
			if err := decimalBoundedFilterGuard(n.Predicate, &remaining); err != nil {
				return err
			}
			plan = n.Input
		case *logical.Projection:
			if n == nil || len(n.Columns) == 0 || len(n.Columns) > 8 {
				return fmt.Errorf("decimal bounded projection requires 1..8 columns")
			}
			for _, name := range n.Columns {
				if len(name) == 0 || len(name) > 128 {
					return fmt.Errorf("decimal bounded projection name exceeds bound")
				}
			}
			plan = n.Input
		default:
			return fmt.Errorf("decimal bounded lazy input plan %T unsupported", plan)
		}
	}
	return fmt.Errorf("decimal bounded lazy plan exceeds 16 nodes")
}

// CollectDecimalBounded executes a dedicated explicit spill aggregation over a
// narrow Parquet Scan/Filter/direct-Projection row plan. It uses CollectBatches
// with exactly opts.Budget and converts borrowed typed chunks in fixed 256-row
// steps. It does not modify ordinary GroupBy, Agg, Collect, Lazy or Auto dispatch.
func (f LazyFrame) CollectDecimalBounded(ctx context.Context, key, value string, opts DecimalBoundedTerminalOptions) (out *DecimalBoundedTerminalResult, err error) {
	if ctx == nil || opts.Budget == nil || opts.Budget.Limit() <= 0 || opts.Store == nil || opts.Store.Limit() <= 0 {
		return nil, fmt.Errorf("decimal bounded lazy terminal requires context, finite query budget and store")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if q := memory.QueryBudgetFromContext(ctx); q != nil && q != opts.Budget {
		return nil, fmt.Errorf("decimal bounded terminal context has different query budget")
	}
	if opts.Budget.Snapshot().Closed {
		return nil, memory.ErrQueryBudgetClosed
	}
	if f.err != nil {
		return nil, f.err
	}
	if f.session == nil || f.session.engine == nil {
		return nil, fmt.Errorf("decimal bounded lazy frame has no session")
	}
	if err := decimalBoundedPlanGuard(f.plan); err != nil {
		return nil, err
	}
	if key == value || key == "" || value == "" || len(key) > 128 || len(value) > 128 {
		return nil, fmt.Errorf("decimal bounded key/value names invalid")
	}
	schema, err := f.Schema()
	if err != nil {
		return nil, err
	}
	k, ok := schema.Field(key)
	if !ok || k.Type != memory.TypeInt64 {
		return nil, fmt.Errorf("decimal bounded key must be visible Int64")
	}
	v, ok := schema.Field(value)
	if !ok {
		return nil, fmt.Errorf("decimal bounded value column not visible")
	}
	if err := v.ValidateDecimal128(); err != nil {
		return nil, err
	}
	if opts.TableCapacity < 0 || opts.TableCapacity > 256 {
		return nil, fmt.Errorf("decimal bounded table capacity must be 0..256")
	}
	switch opts.Mode {
	case engine.DecimalBoundedSumCounts:
		if opts.AveragePrecision != 0 || opts.AverageScale != 0 || opts.AverageRounding != 0 {
			return nil, aggregate.ErrDecimalAverageOptions
		}
	case engine.DecimalBoundedAverageCounts, engine.DecimalBoundedSumAverageCounts:
		var state aggregate.DecimalSumState
		if _, _, err := state.FinalizeAverage(aggregate.DecimalAverageOptions{InputScale: v.DecimalScale, Precision: opts.AveragePrecision, Scale: opts.AverageScale, Mode: opts.AverageRounding}); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("decimal bounded aggregate mode invalid")
	}
	pipeline := f.session.engine.PipelineOptions()
	if err := pipeline.Validate(); err != nil {
		return nil, err
	}
	if pipeline.Workers != 1 || pipeline.MaxMorselRows > 65536 {
		return nil, fmt.Errorf("decimal bounded terminal requires exactly one worker and at most 65536 morsel rows")
	}
	// Fixed adapter/view metadata allowance, separate from full decoded buffers
	// and filter/engine state, which are admitted by their own shared-q owners.
	charge := int64(unsafe.Sizeof(DecimalBoundedTerminalResult{})) + int64(unsafe.Sizeof(decimalBoundedBatchInput{})) + (256 << 10)
	lease, err := opts.Budget.Operator("DecimalBoundedTerminal", memory.MemoryWorkspace).Reserve(ctx, charge)
	if err != nil {
		return nil, err
	}
	out = &DecimalBoundedTerminalResult{budget: opts.Budget, base: ctx, lease: lease}
	defer func() {
		err = errors.Join(err, ctx.Err())
		if err != nil && out != nil {
			cleanup := out.Close(context.Background())
			err = errors.Join(err, cleanup)
			if out.lease == nil {
				out = nil
			}
		}
	}()
	shared := memory.WithQueryBudget(ctx, opts.Budget)
	reader, err := f.Select(key, value).CollectBatches(shared, BatchOptions{TargetBatchBytes: 1 << 20, MaxBatchBytes: 1 << 20, MaxInFlightBytes: 8 << 20})
	if err != nil {
		return out, fmt.Errorf("decimal bounded upstream open: %w", err)
	}
	adapter := &decimalBoundedBatchInput{reader: reader, key: key, value: value, precision: v.DecimalPrecision, scale: v.DecimalScale}
	out.input = adapter
	if reader.budget != opts.Budget || reader.ownedBudget {
		return out, fmt.Errorf("decimal bounded upstream did not borrow the exact query budget")
	}
	result, err := engine.ExecuteDecimalBoundedGrouped(shared, adapter, engine.DecimalBoundedGroupedOptions{Manager: opts.Budget, Store: opts.Store, Precision: v.DecimalPrecision, Scale: v.DecimalScale, TableCapacity: opts.TableCapacity, Mode: opts.Mode, AveragePrecision: opts.AveragePrecision, AverageScale: opts.AverageScale, AverageRounding: opts.AverageRounding})
	out.result = result
	if result != nil {
		out.Stats = result.Stats
	}
	if err != nil {
		return out, fmt.Errorf("decimal bounded terminal execution: %w", err)
	}
	return out, nil
}
