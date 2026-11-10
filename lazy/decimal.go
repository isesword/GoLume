package lazy

import (
	"gocolumn/expr"
	"gocolumn/memory"
)

// Decimal128Chunk is a borrowed native decimal coefficient chunk. Precision
// and scale are carried alongside the coefficient and must not be discarded.
type Decimal128Chunk struct {
	chunkBase
	vector *memory.Decimal128Vector
}

// Len returns the number of rows in the chunk.
func (c Decimal128Chunk) Len() int { return c.chunkBase.length() }

// Precision returns the declared maximum coefficient digits.
func (c Decimal128Chunk) Precision() int32 { return c.vector.Precision }

// Scale returns the decimal fractional digit count.
func (c Decimal128Chunk) Scale() int32 { return c.vector.Scale }

// Value returns an exact unscaled coefficient, validity, and lifecycle/bounds
// errors. The numerical value is coefficient * 10^-Scale().
func (c Decimal128Chunk) Value(i int) (memory.Decimal128, bool, error) {
	if err := c.check(i); err != nil {
		return memory.Decimal128{}, false, err
	}
	row := c.start + i
	return c.vector.Values[row], c.vector.IsValid(row), nil
}

// Decimal128Chunks returns exact zero-copy native decimal128 chunks. Opaque
// Arrow decimals and wider/negative-scale types require a separate adapter.
func (s Series) Decimal128Chunks() ([]Decimal128Chunk, error) {
	if err := s.field.ValidateDecimal128(); err != nil {
		return nil, err
	}
	if s.state == nil {
		return nil, ErrDataFrameClosed
	}
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	spans, err := s.lockedSpans()
	if err != nil {
		return nil, err
	}
	out := make([]Decimal128Chunk, 0, len(spans))
	for _, span := range spans {
		v, err := span.batch.Decimal(s.field.Name)
		if err != nil {
			return nil, err
		}
		if err = v.ValidateField(s.field, span.batch.Rows); err != nil {
			return nil, err
		}
		out = append(out, Decimal128Chunk{chunkBase: chunkBase{s.state, span.start, span.end}, vector: v})
	}
	return out, nil
}

// Decimal128 constructs an exact typed decimal literal from an unscaled
// coefficient, precision, and scale. Invalid values fail during binding.
func Decimal128(value memory.Decimal128, precision, scale int32) expr.Decimal128Literal {
	return expr.Decimal128(value, precision, scale)
}

// NullDecimal128 constructs a decimal NULL with explicit precision and scale.
func NullDecimal128(precision, scale int32) expr.Decimal128NullLiteral {
	return expr.NullDecimal128(precision, scale)
}

// RescaleDecimal128 changes a native decimal's precision/scale exactly. NULLs
// are preserved; overflow and discarded nonzero digits are execution errors.
func RescaleDecimal128(value expr.Expr, precision, scale int32) expr.Decimal128RescaleExpr {
	return expr.RescaleDecimal128(value, precision, scale)
}
