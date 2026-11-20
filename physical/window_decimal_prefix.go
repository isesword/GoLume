package physical

import (
	"context"
	"errors"
	"fmt"
	"math"

	"gocolumn/aggregate"
	"gocolumn/memory"
	"gocolumn/source"
)

// DecimalPrefixAverage specifies the physical-only Decimal128 ROWS AVG
// transport. The input schema contains exactly a required ascending Int64 key
// and one native Decimal128 value. Duplicate keys are rejected (not treated as
// RANGE peers or silently reordered).
type DecimalPrefixAverage struct {
	Column, OrderBy, Alias string
	Frame                  WindowFrame
	Precision, Scale       int32
	Rounding               aggregate.DecimalAverageRoundingMode
}

type decimalPrefixAverageSource struct {
	input              source.Source
	schema             memory.Schema
	key, value, result memory.Field
	average            aggregate.DecimalAverageOptions
}

// NewDecimalPrefixAverageSource constructs the finite-budget physical operator
// for ROWS UNBOUNDED PRECEDING THROUGH CURRENT ROW. It adds no sort, filter,
// partitioning, spill or materialization. The child supplies globally ordered
// batches and the operator verifies strict monotonicity across every boundary.
// The finite execution budget is provided through ReadOptions.MemoryManager.
// Soft payload targeting is unsupported: this operator preserves child page boundaries.
func NewDecimalPrefixAverageSource(input source.Source, spec DecimalPrefixAverage) (source.Source, error) {
	if input == nil || spec.Column == "" || spec.OrderBy == "" || spec.Alias == "" || spec.Column == spec.OrderBy ||
		spec.Frame != (WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}}) {
		return nil, fmt.Errorf("decimal prefix AVG requires value, distinct direct order key, output alias and canonical ROWS prefix")
	}
	schema := input.Schema()
	if len(schema.Fields) != 2 {
		return nil, fmt.Errorf("decimal prefix AVG requires exactly the order and Decimal value columns")
	}
	key, k := schema.Field(spec.OrderBy)
	val, v := schema.Field(spec.Column)
	if !k || key.Type != memory.TypeInt64 || key.Nullable || key.Encoding != memory.EncodingPlain || key.DictionaryDomain != "" || len(key.Children) != 0 || key.ArrowType != "" || key.DecimalPrecision != 0 || key.DecimalScale != 0 || !v {
		return nil, fmt.Errorf("decimal prefix AVG requires a plain non-null Int64 order key and Decimal128 value")
	}
	if err := val.ValidateDecimal128(); err != nil {
		return nil, err
	}
	if _, exists := schema.Field(spec.Alias); exists {
		return nil, fmt.Errorf("decimal prefix AVG output alias already exists")
	}
	provider, ok := input.(source.OrderingProvider)
	if !ok {
		return nil, fmt.Errorf("decimal prefix AVG requires a proven ascending Int64 ordering")
	}
	ordering := provider.Ordering()
	if len(ordering) == 0 || ordering[0].Name != spec.OrderBy || ordering[0].Descending {
		return nil, fmt.Errorf("decimal prefix AVG requires a proven ascending Int64 ordering")
	}
	plan := source.StreamPlanOf(input)
	if !decimalPrefixChildBounded(plan) {
		return nil, fmt.Errorf("decimal prefix AVG requires a non-spilling bounded child")
	}
	avg := aggregate.DecimalAverageOptions{InputScale: val.DecimalScale, Precision: spec.Precision, Scale: spec.Scale, Mode: spec.Rounding}
	if _, _, err := (aggregate.DecimalSumState{}).FinalizeAverage(avg); err != nil {
		return nil, err
	}
	result, err := memory.Decimal128Field(spec.Alias, true, spec.Precision, spec.Scale)
	if err != nil {
		return nil, err
	}
	fields := append(append([]memory.Field(nil), schema.Fields...), result)
	return &decimalPrefixAverageSource{input: input, schema: memory.NewSchema(fields...), key: key, value: val, result: result, average: avg}, nil
}

func decimalPrefixChildBounded(n source.StreamPlanNode) bool {
	if n.Class != source.StreamPipeline && n.Class != source.StreamBounded {
		return false
	}
	for _, child := range n.Children {
		if !decimalPrefixChildBounded(child) {
			return false
		}
	}
	return true
}

func (s *decimalPrefixAverageSource) Schema() memory.Schema {
	return memory.NewSchema(s.schema.Fields...)
}
func (s *decimalPrefixAverageSource) Stats(ctx context.Context) (source.Stats, error) {
	st, err := s.input.Stats(ctx)
	st.Bytes = 0
	st.Partitions = 1
	return st, err
}
func (s *decimalPrefixAverageSource) Partitions(ctx context.Context) ([]source.Partition, error) {
	st, err := s.Stats(ctx)
	if err != nil {
		return nil, err
	}
	return []source.Partition{{ID: 0, Rows: st.Rows}}, nil
}
func (s *decimalPrefixAverageSource) Ordering() []source.OrderingColumn {
	return []source.OrderingColumn{{Name: s.key.Name}}
}
func (s *decimalPrefixAverageSource) StreamPlan() source.StreamPlanNode {
	return source.StreamPlanNode{Operator: "DecimalPrefixAverage", Class: source.StreamBounded, Detail: "strict ascending Int64; ROWS prefix; memory-only", Children: []source.StreamPlanNode{source.StreamPlanOf(s.input)}}
}

// Explicit budget charge covers the fixed accumulator, order witness and
// stream-owned bookkeeping. Child scan and output use the same manager.
const decimalPrefixStateBytes int64 = 512

func (s *decimalPrefixAverageSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	if ctx == nil {
		return nil, fmt.Errorf("decimal prefix AVG context is nil")
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.BatchPayloadTargetBytes != 0 {
		return nil, fmt.Errorf("decimal prefix AVG does not split child pages for a soft payload target")
	}
	opts = opts.Normalize()
	if opts.MemoryManager == nil || opts.MemoryManager.Limit() <= 0 {
		return nil, fmt.Errorf("decimal prefix AVG requires an explicit finite memory manager")
	}
	lease, err := opts.MemoryManager.Reserve(ctx, decimalPrefixStateBytes)
	if err != nil {
		return nil, fmt.Errorf("decimal prefix AVG state: %w", err)
	}
	child, err := source.OpenBatchStream(ctx, s.input, opts)
	if err != nil {
		lease.Release()
		return nil, err
	}
	return &decimalPrefixAverageStream{src: s, base: ctx, opts: opts, child: child, input: memory.NewBatch(), stateLease: lease}, nil
}

type decimalPrefixAverageStream struct {
	src                    *decimalPrefixAverageSource
	base                   context.Context
	opts                   source.ReadOptions
	child                  source.BatchStream
	input                  *memory.Batch
	stateLease, pageLease  memory.Reservation
	sum                    aggregate.DecimalSumState
	prev                   int64
	havePrev, done, closed bool
}

func (s *decimalPrefixAverageStream) check(ctx context.Context) error {
	if err := s.base.Err(); err != nil {
		return err
	}
	if ctx == nil {
		return fmt.Errorf("decimal prefix AVG Next context is nil")
	}
	return ctx.Err()
}

func decimalPrefixRowBytes(rows int) (bytes int64, words int, err error) {
	if rows < 0 || int64(rows) > (math.MaxInt64-256)/50 {
		return 0, 0, memory.ErrBudgetExceeded
	}
	words = int((int64(rows) + 63) / 64)
	return 16*int64(rows) + 8*int64(words), words, nil
}

func (s *decimalPrefixAverageStream) nextChild(ctx context.Context) (bool, error) {
	if s.base.Done() == nil {
		return s.child.Next(ctx, s.input)
	}
	childCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.base, cancel)
	defer cancel()
	defer stop()
	return s.child.Next(childCtx, s.input)
}

func (s *decimalPrefixAverageStream) Next(ctx context.Context, dst *memory.Batch) (ok bool, err error) {
	if dst == nil {
		return false, errors.Join(fmt.Errorf("decimal prefix AVG destination is nil"), s.Close())
	}
	dst.ClearColumns()
	if dst.Int64s == nil || dst.Decimals == nil {
		return false, errors.Join(fmt.Errorf("decimal prefix AVG destination requires initialized batch maps"), s.Close())
	}
	if s.closed {
		return false, source.ErrBatchStreamClosed
	}
	if s.done {
		return false, nil
	}
	s.releasePage()
	defer func() {
		if err != nil || !ok {
			dst.ClearColumns()
			err = errors.Join(err, s.finish())
		}
	}()
	if err = s.check(ctx); err != nil {
		return false, err
	}
	s.input.ClearColumns()
	ok, err = s.nextChild(ctx)
	if err != nil {
		return false, err
	}
	if err = s.check(ctx); err != nil || !ok {
		return false, err
	}
	rows := s.input.Rows
	if rows < 0 || rows > s.opts.BatchRows {
		return false, fmt.Errorf("decimal prefix AVG input exceeded row bound: %w", memory.ErrBudgetExceeded)
	}
	bytes, words, e := decimalPrefixRowBytes(rows)
	if e != nil {
		return false, e
	}
	key := s.input.Int64s[s.src.key.Name]
	value := s.input.Decimals[s.src.value.Name]
	if key == nil || len(key.Values) != rows || key.Encoding != memory.EncodingPlain || key.Validity != nil && len(key.Validity) < words ||
		value == nil || len(value.Values) != rows || value.Precision != s.src.value.DecimalPrecision || value.Scale != s.src.value.DecimalScale ||
		value.Validity != nil && len(value.Validity) < words {
		return false, fmt.Errorf("decimal prefix AVG input vector disagrees with schema")
	}
	if bytes > s.opts.BatchBytes {
		return false, fmt.Errorf("decimal prefix AVG output bytes: %w", memory.ErrBudgetExceeded)
	}
	if max := s.opts.BatchPayloadBytes; max > 0 {
		payload := bytes + 24*int64(rows)
		if key.Validity != nil {
			payload += 8 * int64(words)
		}
		if value.Validity != nil {
			payload += 8 * int64(words)
		}
		if payload > max {
			return false, fmt.Errorf("decimal prefix AVG output payload: %w", memory.ErrBudgetExceeded)
		}
	}
	s.pageLease, err = s.opts.MemoryManager.Reserve(ctx, bytes)
	if err != nil {
		return false, fmt.Errorf("decimal prefix AVG output: %w", err)
	}
	result := &memory.Decimal128Vector{Values: make([]memory.Decimal128, rows), Validity: make(memory.Bitmap, words), Precision: s.src.result.DecimalPrecision, Scale: s.src.result.DecimalScale}
	for row, current := range key.Values {
		if err = s.check(ctx); err != nil {
			return false, err
		}
		if !key.IsValid(row) || s.havePrev && current <= s.prev {
			return false, fmt.Errorf("decimal prefix AVG requires non-null strictly ascending keys (including across batches)")
		}
		s.prev, s.havePrev = current, true
		if value.IsValid(row) {
			v := value.Values[row]
			if !memory.Decimal128FitsPrecision(v, value.Precision) {
				return false, fmt.Errorf("decimal prefix AVG input coefficient outside declared precision")
			}
			if err = s.sum.Add(v); err != nil {
				return false, err
			}
		} else if !s.src.value.Nullable {
			return false, fmt.Errorf("decimal prefix AVG required value contains NULL")
		}
		v, valid, e := s.sum.FinalizeAverage(s.src.average)
		if e != nil {
			return false, fmt.Errorf("decimal prefix AVG at ordered key %d: %w", current, e)
		}
		if valid {
			result.Values[row] = v
			memory.Set(result.Validity, row)
		}
	}
	if err = s.check(ctx); err != nil {
		return false, err
	}
	// Publish borrowed input and new output together only after the batch succeeds.
	dst.Int64s[s.src.key.Name] = key
	dst.Decimals[s.src.value.Name] = value
	dst.Decimals[s.src.result.Name] = result
	dst.Rows = rows
	return true, nil
}
func (s *decimalPrefixAverageStream) releasePage() {
	if s.pageLease != nil {
		s.pageLease.Release()
		s.pageLease = nil
	}
}
func (s *decimalPrefixAverageStream) finish() error {
	if s.done {
		return nil
	}
	s.done = true
	s.releasePage()
	s.stateLease.Release()
	s.stateLease = nil
	s.input.ClearColumns()
	s.input = nil
	child := s.child
	s.child = nil
	return child.Close()
}
func (s *decimalPrefixAverageStream) Close() error {
	s.closed = true
	return s.finish()
}
