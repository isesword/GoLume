package engine

import (
	"context"
	"errors"
	"fmt"
	"gocolumn/expr"
	"gocolumn/memory"
	"gocolumn/source"
	"math"
	"strings"
	"unsafe"
)

// DecimalValue represents only a native Decimal128 SUM/configured AVG or a Uint64 COUNT.
// Invalid Decimal values retain precision/scale with a zero coefficient.
// Uint64 is never a low-word representation of Coefficient.
type DecimalValue struct {
	Type             memory.DataType
	Valid            bool
	Coefficient      memory.Decimal128
	Precision, Scale int32
	Uint64           uint64
}

func (v DecimalValue) AsDecimal128() (memory.Decimal128, int32, int32, bool) {
	if !v.Valid || v.Type != memory.TypeDecimal {
		return memory.Decimal128{}, 0, 0, false
	}
	return v.Coefficient, v.Precision, v.Scale, true
}
func (v DecimalValue) AsUint64() (uint64, bool) {
	if !v.Valid || v.Type != memory.TypeUint64 {
		return 0, false
	}
	return v.Uint64, true
}

// Alias is the resolved output name, including agg_i for an unnamed output.
type DecimalAggregateValue struct {
	Alias string
	Kind  expr.AggregateKind
	Value DecimalValue
}
type DecimalAggregateRow struct{ Aggregates []DecimalAggregateValue }

// DecimalResult is a caller-owned global result with no keys. Materialize=true
// produces exactly one row, including on empty input; false produces no rows.
// Values and cloned aliases outlive query/engine Close and retain no leases.
// Admission covers construction, not perpetual retention in the caller's heap.
type DecimalResult struct {
	Rows    []DecimalAggregateRow
	Summary Result
}

func (p *PreparedQuery) ExecuteDecimal(ctx context.Context, opts ExecuteOptions) (DecimalResult, error) {
	var result DecimalResult
	summary, e := p.executeResults(ctx, opts, true, nil, &result)
	if e != nil {
		return DecimalResult{}, e
	}
	result.Summary = summary
	return result, nil
}
func (e *Engine) ExecuteDecimal(ctx context.Context, src source.Source, q expr.Query, opts ExecuteOptions) (result DecimalResult, err error) {
	p, err := e.Prepare(ctx, src, q, PrepareOptions{MaxConcurrency: 1})
	if err != nil {
		return DecimalResult{}, err
	}
	defer func() {
		err = errors.Join(err, p.Close())
		if err != nil {
			result = DecimalResult{}
		}
	}()
	summary, err := p.executeResults(ctx, opts, false, nil, &result)
	if err != nil {
		return DecimalResult{}, err
	}
	result.Summary = summary
	return result, nil
}

type decimalResultCollector struct {
	query   *PreparedQuery
	result  *DecimalResult
	manager memory.Manager
	lease   memory.Reservation
}

func (c *decimalResultCollector) close() {
	if c.lease != nil {
		c.lease.Release()
		c.lease = nil
	}
}
func (c *decimalResultCollector) append(ctx context.Context, b *memory.Batch) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	fields := c.query.executionSchema.Fields
	n := len(fields)
	if b == nil || b.Rows != 1 || len(c.result.Rows) != 0 || c.lease != nil {
		return fmt.Errorf("Decimal result requires exactly one global output row")
	}
	per := int64(unsafe.Sizeof(DecimalAggregateValue{}))
	base := int64(unsafe.Sizeof(DecimalAggregateRow{})) + 512
	if int64(n) > (math.MaxInt64-base)/per {
		return memory.ErrBudgetExceeded
	}
	bytes := base + int64(n)*per
	sums, counts := 0, 0
	for i, f := range fields {
		if i%64 == 0 {
			if e := ctx.Err(); e != nil {
				return e
			}
		}
		if int64(len(f.Name)) > math.MaxInt64-bytes {
			return memory.ErrBudgetExceeded
		}
		bytes += int64(len(f.Name))
		switch f.Type {
		case memory.TypeDecimal:
			v := b.Decimals[f.Name]
			if v == nil || len(v.Values) != 1 || v.Precision != f.DecimalPrecision || v.Scale != f.DecimalScale || v.Validity != nil && len(v.Validity) < 1 {
				return fmt.Errorf("Decimal result binding mismatch %q", f.Name)
			}
			if v.IsValid(0) && !v.Values[0].FitsInPrecision(f.DecimalPrecision) {
				return fmt.Errorf("Decimal result coefficient overflow")
			}
			sums++
		case memory.TypeUint64:
			v := b.Uint64s[f.Name]
			if v == nil || len(v.Values) != 1 || v.Validity != nil && len(v.Validity) < 1 || !v.IsValid(0) {
				return fmt.Errorf("COUNT result binding mismatch %q", f.Name)
			}
			counts++
		default:
			return fmt.Errorf("unsupported exact result field %s", f.Type)
		}
	}
	if len(b.Decimals) != sums || len(b.Uint64s) != counts {
		return fmt.Errorf("Decimal result unexpected output bindings")
	}
	lease, e := c.manager.Reserve(ctx, bytes)
	if e != nil {
		return e
	}
	c.lease = lease
	row := DecimalAggregateRow{Aggregates: make([]DecimalAggregateValue, n)}
	rows := make([]DecimalAggregateRow, 1)
	for i, f := range fields {
		if i%64 == 0 {
			if e := ctx.Err(); e != nil {
				return e
			}
		}
		v := DecimalValue{Type: f.Type}
		if f.Type == memory.TypeDecimal {
			src := b.Decimals[f.Name]
			v.Valid = src.IsValid(0)
			v.Precision = f.DecimalPrecision
			v.Scale = f.DecimalScale
			if v.Valid {
				v.Coefficient = src.Values[0]
			}
		} else {
			v.Valid = true
			v.Uint64 = b.Uint64s[f.Name].Values[0]
		}
		row.Aggregates[i] = DecimalAggregateValue{Alias: strings.Clone(f.Name), Kind: c.query.cq.Aggregates[i].Kind, Value: v}
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	rows[0] = row
	c.result.Rows = rows
	return nil
}

// The source has already been closed by the global stream before its output.
// This consumer additionally confirms EOF and checks late cancellation; the
// shared executor closes the stream and revokes results on any closing error.
func collectDecimalStream(ctx context.Context, stream source.BatchStream, c *decimalResultCollector) (AggregateStreamStats, error) {
	decimal, ok := stream.(*decimalGlobalStream)
	if !ok {
		return AggregateStreamStats{}, fmt.Errorf("ExecuteDecimal requires dedicated Decimal stream")
	}
	b := &memory.Batch{}
	seen := false
	for {
		ok, e := stream.Next(ctx, b)
		if e != nil {
			return AggregateStreamStats{}, e
		}
		if !ok {
			break
		}
		if seen {
			return AggregateStreamStats{}, fmt.Errorf("Decimal stream emitted multiple rows")
		}
		if e = c.append(ctx, b); e != nil {
			return AggregateStreamStats{}, e
		}
		seen = true
	}
	if !seen {
		return AggregateStreamStats{}, fmt.Errorf("Decimal stream omitted global row")
	}
	if e := ctx.Err(); e != nil {
		return AggregateStreamStats{}, e
	}
	return AggregateStreamStats{SelectedRows: decimal.selected, Groups: 1}, nil
}
