package physical

import (
	"context"
	"fmt"
	"gocolumn/memory"
	"gocolumn/source"
)

// ConfiguredWindowExpression associates a physical expression with its bucket count.
// Ordinary window functions require Buckets=0; NTILE requires Buckets>0.
type ConfiguredWindowExpression struct {
	Expr    WindowExpression
	Buckets int64
}

// NewConfiguredWindowSourceWithOptions validates positional configuration and selects a window execution path.
func NewConfiguredWindowSourceWithOptions(input source.Source, partitionBy []string, orderBy []SortKey, configured []ConfiguredWindowExpression, outputFields []memory.Field, manager memory.Manager, options WindowSourceOptions) (source.Source, error) {
	expressions := make([]WindowExpression, len(configured))
	buckets := make([]int64, len(configured))
	for i, e := range configured {
		expressions[i] = e.Expr
		buckets[i] = e.Buckets
	}
	return newWindowSourceConfigured(input, partitionBy, orderBy, expressions, buckets, outputFields, manager, options)
}

// windowNTileValue uses zero-based position. Avoid position*buckets and the
// potentially overflowing q+1 when a single bucket spans MaxInt64 rows.
func windowNTileValue(position, total, buckets int64) (int64, error) {
	if total <= 0 || position < 0 || position >= total || buckets <= 0 {
		return 0, fmt.Errorf("invalid NTILE position, total or buckets")
	}
	q, r := total/buckets, total%buckets
	if q == 0 {
		return position + 1, nil
	}
	if r == 0 {
		return position/q + 1, nil
	}
	// r>0 implies q<total, and r*(q+1)<=total, so both operations fit.
	cut := r * (q + 1)
	if position < cut {
		return position/(q+1) + 1, nil
	}
	return r + (position-cut)/q + 1, nil
}

func (s *WindowSource) evaluateExpression(ctx context.Context, input *memory.Batch, schema memory.Schema, output *memory.Batch, rows []int, index int) error {
	e := s.expressions[index]
	if e.Function != WindowNTile {
		return evaluateWindowExpression(ctx, input, schema, output, rows, s.orderBy, e)
	}
	if len(s.buckets) != len(s.expressions) || s.buckets[index] <= 0 {
		return fmt.Errorf("missing NTILE configuration")
	}
	values := windowI64Values(output, e.Alias, input.Rows)
	for i, row := range rows {
		if err := everyWindowRows(ctx, i); err != nil {
			return err
		}
		v, err := windowNTileValue(int64(i), int64(len(rows)), s.buckets[index])
		if err != nil {
			return err
		}
		values[row] = v
	}
	output.Int64s[e.Alias] = &memory.Int64Vector{Values: values}
	return ctx.Err()
}

func canonicalNTileField(f memory.Field) bool {
	return f.Encoding == memory.EncodingPlain && f.DictionaryDomain == "" && f.ArrowType == "" && f.DecimalPrecision == 0 && f.DecimalScale == 0 && len(f.Children) == 0
}
