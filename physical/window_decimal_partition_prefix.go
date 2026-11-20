package physical

import (
	"context"
	"errors"
	"fmt"

	"gocolumn/aggregate"
	"gocolumn/memory"
	"gocolumn/source"
)

// PartitionedDecimalPrefixAverage specifies the physical-only Decimal128 ROWS AVG
// transport. Input contains a nullable Int64 partition, required Int64 order,
// and native Decimal128 value. Ordering is strict within each partition.
type PartitionedDecimalPrefixAverage struct {
	Column, PartitionBy, OrderBy, Alias string
	Frame                               WindowFrame
	Precision, Scale                    int32
	Rounding                            aggregate.DecimalAverageRoundingMode
}

type decimalPartitionPrefixAverageSource struct {
	input                         source.Source
	schema                        memory.Schema
	key, partition, value, result memory.Field
	average                       aggregate.DecimalAverageOptions
}

// NewPartitionedDecimalPrefixAverageSource constructs the finite-budget physical operator
// for ROWS UNBOUNDED PRECEDING THROUGH CURRENT ROW. It adds no sort, filter,
// spill or materialization. The child supplies globally ordered
// batches and the operator verifies strict monotonicity across every boundary.
// The finite execution budget is provided through ReadOptions.MemoryManager.
// Soft payload targeting is unsupported: this operator preserves child page boundaries.
func NewPartitionedDecimalPrefixAverageSource(input source.Source, spec PartitionedDecimalPrefixAverage) (source.Source, error) {
	if input == nil || spec.Column == "" || spec.OrderBy == "" || spec.Alias == "" || spec.Column == spec.OrderBy || spec.PartitionBy == "" || spec.PartitionBy == spec.Column || spec.PartitionBy == spec.OrderBy ||
		spec.Frame != (WindowFrame{Start: WindowBound{Kind: WindowUnboundedPreceding}, End: WindowBound{Kind: WindowCurrentRow}}) {
		return nil, fmt.Errorf("partitioned decimal prefix AVG requires value, distinct direct order key, output alias and canonical ROWS prefix")
	}
	schema := input.Schema()
	if len(schema.Fields) != 3 {
		return nil, fmt.Errorf("partitioned decimal prefix AVG requires exactly partition, order and Decimal value columns")
	}
	partition, found := schema.Field(spec.PartitionBy)
	if !found || partition.Type != memory.TypeInt64 || partition.Encoding != memory.EncodingPlain || partition.DictionaryDomain != "" || len(partition.Children) != 0 || partition.ArrowType != "" || partition.DecimalPrecision != 0 || partition.DecimalScale != 0 {
		return nil, fmt.Errorf("partition key must be plain Int64")
	}
	key, k := schema.Field(spec.OrderBy)
	val, v := schema.Field(spec.Column)
	if !k || key.Type != memory.TypeInt64 || key.Nullable || key.Encoding != memory.EncodingPlain || key.DictionaryDomain != "" || len(key.Children) != 0 || key.ArrowType != "" || key.DecimalPrecision != 0 || key.DecimalScale != 0 || !v {
		return nil, fmt.Errorf("partitioned decimal prefix AVG requires a plain non-null Int64 order key and Decimal128 value")
	}
	if err := val.ValidateDecimal128(); err != nil {
		return nil, err
	}
	if _, exists := schema.Field(spec.Alias); exists {
		return nil, fmt.Errorf("partitioned decimal prefix AVG output alias already exists")
	}
	provider, ok := input.(source.OrderingProvider)
	if !ok {
		return nil, fmt.Errorf("partitioned decimal prefix AVG requires a proven ascending Int64 ordering")
	}
	ordering := provider.Ordering()
	if len(ordering) < 2 || ordering[0].Name != spec.PartitionBy || ordering[0].Descending || ordering[0].NullsLast || ordering[1].Name != spec.OrderBy || ordering[1].Descending {
		return nil, fmt.Errorf("partitioned decimal prefix AVG requires a proven ascending Int64 ordering")
	}
	plan := source.StreamPlanOf(input)
	if !decimalPrefixChildBounded(plan) {
		return nil, fmt.Errorf("partitioned decimal prefix AVG requires a non-spilling bounded child")
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
	return &decimalPartitionPrefixAverageSource{input: input, schema: memory.NewSchema(fields...), key: key, partition: partition, value: val, result: result, average: avg}, nil
}

func (s *decimalPartitionPrefixAverageSource) Schema() memory.Schema {
	return memory.NewSchema(s.schema.Fields...)
}
func (s *decimalPartitionPrefixAverageSource) Stats(ctx context.Context) (source.Stats, error) {
	st, err := s.input.Stats(ctx)
	st.Bytes = 0
	st.Partitions = 1
	return st, err
}
func (s *decimalPartitionPrefixAverageSource) Partitions(ctx context.Context) ([]source.Partition, error) {
	st, err := s.Stats(ctx)
	if err != nil {
		return nil, err
	}
	return []source.Partition{{ID: 0, Rows: st.Rows}}, nil
}
func (s *decimalPartitionPrefixAverageSource) Ordering() []source.OrderingColumn {
	return []source.OrderingColumn{{Name: s.partition.Name}, {Name: s.key.Name}}
}
func (s *decimalPartitionPrefixAverageSource) StreamPlan() source.StreamPlanNode {
	return source.StreamPlanNode{Operator: "PartitionedDecimalPrefixAverage", Class: source.StreamBounded, Detail: "partition ASC NULL-first; within-partition strict order; ROWS prefix; memory-only", Children: []source.StreamPlanNode{source.StreamPlanOf(s.input)}}
}

// Explicit budget charge covers the fixed accumulator, order witness and
// stream-owned bookkeeping. Child scan and output use the same manager.
const decimalPartitionPrefixStateBytes int64 = 768

func (s *decimalPartitionPrefixAverageSource) NewBatchStream(ctx context.Context, opts source.ReadOptions) (source.BatchStream, error) {
	if ctx == nil {
		return nil, fmt.Errorf("partitioned decimal prefix AVG context is nil")
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if opts.BatchPayloadTargetBytes != 0 {
		return nil, fmt.Errorf("partitioned decimal prefix AVG does not split child pages for a soft payload target")
	}
	opts = opts.Normalize()
	if opts.MemoryManager == nil || opts.MemoryManager.Limit() <= 0 {
		return nil, fmt.Errorf("partitioned decimal prefix AVG requires an explicit finite memory manager")
	}
	lease, err := opts.MemoryManager.Reserve(ctx, decimalPartitionPrefixStateBytes)
	if err != nil {
		return nil, fmt.Errorf("partitioned decimal prefix AVG state: %w", err)
	}
	child, err := source.OpenBatchStream(ctx, s.input, opts)
	if err != nil {
		lease.Release()
		return nil, err
	}
	return &decimalPartitionPrefixAverageStream{src: s, base: ctx, opts: opts, child: child, input: memory.NewBatch(), stateLease: lease}, nil
}

type decimalPartitionPrefixAverageStream struct {
	src                    *decimalPartitionPrefixAverageSource
	base                   context.Context
	opts                   source.ReadOptions
	child                  source.BatchStream
	input                  *memory.Batch
	stateLease, pageLease  memory.Reservation
	sum                    aggregate.DecimalSumState
	prev                   int64
	previousPartition      int64
	previousPartitionValid bool
	havePrev, done, closed bool
}

func (s *decimalPartitionPrefixAverageStream) check(ctx context.Context) error {
	if err := s.base.Err(); err != nil {
		return err
	}
	if ctx == nil {
		return fmt.Errorf("partitioned decimal prefix AVG Next context is nil")
	}
	return ctx.Err()
}

func (s *decimalPartitionPrefixAverageStream) nextChild(ctx context.Context) (bool, error) {
	if s.base.Done() == nil {
		return s.child.Next(ctx, s.input)
	}
	childCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.base, cancel)
	defer cancel()
	defer stop()
	return s.child.Next(childCtx, s.input)
}

func (s *decimalPartitionPrefixAverageStream) Next(ctx context.Context, dst *memory.Batch) (ok bool, err error) {
	if dst == nil {
		return false, errors.Join(fmt.Errorf("partitioned decimal prefix AVG destination is nil"), s.Close())
	}
	dst.ClearColumns()
	if dst.Int64s == nil || dst.Decimals == nil {
		return false, errors.Join(fmt.Errorf("partitioned decimal prefix AVG destination requires initialized batch maps"), s.Close())
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
			err = errors.Join(err, s.finish(), s.check(ctx))
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
		return false, fmt.Errorf("partitioned decimal prefix AVG input exceeded row bound: %w", memory.ErrBudgetExceeded)
	}
	bytes, words, e := decimalPrefixRowBytes(rows)
	if e != nil {
		return false, e
	}
	partition := s.input.Int64s[s.src.partition.Name]
	key := s.input.Int64s[s.src.key.Name]
	value := s.input.Decimals[s.src.value.Name]
	if partition == nil || len(partition.Values) != rows || partition.Encoding != memory.EncodingPlain || partition.Validity != nil && len(partition.Validity) < words || key == nil || len(key.Values) != rows || key.Encoding != memory.EncodingPlain || key.Validity != nil && len(key.Validity) < words ||
		value == nil || len(value.Values) != rows || value.Precision != s.src.value.DecimalPrecision || value.Scale != s.src.value.DecimalScale ||
		value.Validity != nil && len(value.Validity) < words {
		return false, fmt.Errorf("partitioned decimal prefix AVG input vector disagrees with schema")
	}
	if bytes > s.opts.BatchBytes {
		return false, fmt.Errorf("partitioned decimal prefix AVG output bytes: %w", memory.ErrBudgetExceeded)
	}
	if max := s.opts.BatchPayloadBytes; max > 0 {
		payload := bytes + 32*int64(rows)
		if partition.Validity != nil {
			payload += 8 * int64(words)
		}
		if key.Validity != nil {
			payload += 8 * int64(words)
		}
		if value.Validity != nil {
			payload += 8 * int64(words)
		}
		if payload > max {
			return false, fmt.Errorf("partitioned decimal prefix AVG output payload: %w", memory.ErrBudgetExceeded)
		}
	}
	s.pageLease, err = s.opts.MemoryManager.Reserve(ctx, bytes)
	if err != nil {
		return false, fmt.Errorf("partitioned decimal prefix AVG output: %w", err)
	}
	result := &memory.Decimal128Vector{Values: make([]memory.Decimal128, rows), Validity: make(memory.Bitmap, words), Precision: s.src.result.DecimalPrecision, Scale: s.src.result.DecimalScale}
	for row, current := range key.Values {
		if err = s.check(ctx); err != nil {
			return false, err
		}
		partValid := partition.IsValid(row)
		part := partition.Values[row]
		if !partValid {
			part = 0
		}
		if !partValid && !s.src.partition.Nullable {
			return false, fmt.Errorf("required partition contains NULL")
		}
		same := s.havePrev && partValid == s.previousPartitionValid && (!partValid || part == s.previousPartition)
		if s.havePrev && ((!partValid && s.previousPartitionValid) || (partValid && s.previousPartitionValid && part < s.previousPartition)) {
			return false, fmt.Errorf("partition keys must be ascending NULL-first")
		}
		if !key.IsValid(row) || same && current <= s.prev {
			return false, fmt.Errorf("partitioned decimal prefix AVG requires non-null strictly ascending keys (including across batches)")
		}
		if !same {
			s.sum = aggregate.DecimalSumState{}
		}
		s.previousPartition, s.previousPartitionValid = part, partValid
		s.prev, s.havePrev = current, true
		if value.IsValid(row) {
			v := value.Values[row]
			if !memory.Decimal128FitsPrecision(v, value.Precision) {
				return false, fmt.Errorf("partitioned decimal prefix AVG input coefficient outside declared precision")
			}
			if err = s.sum.Add(v); err != nil {
				return false, err
			}
		} else if !s.src.value.Nullable {
			return false, fmt.Errorf("partitioned decimal prefix AVG required value contains NULL")
		}
		v, valid, e := s.sum.FinalizeAverage(s.src.average)
		if e != nil {
			return false, fmt.Errorf("partitioned decimal prefix AVG at ordered key %d: %w", current, e)
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
	dst.Int64s[s.src.partition.Name] = partition
	dst.Int64s[s.src.key.Name] = key
	dst.Decimals[s.src.value.Name] = value
	dst.Decimals[s.src.result.Name] = result
	dst.Rows = rows
	return true, nil
}
func (s *decimalPartitionPrefixAverageStream) releasePage() {
	if s.pageLease != nil {
		s.pageLease.Release()
		s.pageLease = nil
	}
}
func (s *decimalPartitionPrefixAverageStream) finish() error {
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
func (s *decimalPartitionPrefixAverageStream) Close() error {
	s.closed = true
	return s.finish()
}
