package expr

import (
	"fmt"
	"gocolumn/memory"
	"math/bits"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
)

func validateDecimalColumn(batch *memory.Batch, name string, field *memory.Field) error {
	v, err := batch.Decimal(name)
	if err != nil {
		return err
	}
	if batch.Rows < 0 || len(v.Values) < batch.Rows {
		return fmt.Errorf("decimal column %q is shorter than batch", name)
	}
	view := *v
	view.Values = view.Values[:batch.Rows]
	if err := view.ValidateField(*field, batch.Rows); err != nil {
		return fmt.Errorf("decimal column %q: %w", name, err)
	}
	return nil
}

// Decimal128Literal is an exact unscaled coefficient with explicit decimal
// metadata. Its value is Value * 10^-Scale. Binding rejects invalid metadata
// and out-of-precision coefficients; it never rounds or converts through float.
type Decimal128Literal struct {
	Value            memory.Decimal128
	Precision, Scale int32
}

func (Decimal128Literal) exprNode() {}

// String returns an unambiguous coefficient, precision, and scale spelling.
func (v Decimal128Literal) String() string {
	return fmt.Sprintf("DECIMAL128(%s,%d,%d)", v.Value.BigInt().String(), v.Precision, v.Scale)
}

// Decimal128 constructs an exact typed literal from its unscaled coefficient.
// For example, coefficient 12345 with precision 5 and scale 2 means 123.45.
// Native decimal comparisons require two decimal operands, possibly with
// different precisions/scales; implicit numeric/string conversion is rejected.
// EQ/LT/LE/GT/GE and SQL IN use exact equality/ordering. Add/Sub align to
// max(scale); Mul adds scales (at most 38). The inferred result precision is
// capped at 38 and overflow is checked per demanded non-NULL row. No operation
// implicitly reduces scale or rounds. RescaleDecimal128 explicitly changes
// precision/scale and rejects any loss of digits. Division, generic casts,
// mixed numeric operands, and conditional branches with different
// precision/scale remain unsupported.
func Decimal128(value memory.Decimal128, precision, scale int32) Decimal128Literal {
	return Decimal128Literal{Value: value, Precision: precision, Scale: scale}
}

// Alias names a decimal literal output.
func (v Decimal128Literal) Alias(name string) NamedExpr { return Named(v, name) }

// Decimal128NullLiteral is a typed decimal NULL with explicit precision/scale.
type Decimal128NullLiteral struct{ Precision, Scale int32 }

func (Decimal128NullLiteral) exprNode() {}

// String returns the typed NULL spelling including precision and scale.
func (v Decimal128NullLiteral) String() string {
	return fmt.Sprintf("NULL_DECIMAL128(%d,%d)", v.Precision, v.Scale)
}

// NullDecimal128 constructs a typed decimal NULL. Metadata is checked at bind
// time, including in dead branches. Null(TypeDecimal) remains unsupported
// because it cannot specify the required precision/scale.
func NullDecimal128(precision, scale int32) Decimal128NullLiteral {
	return Decimal128NullLiteral{Precision: precision, Scale: scale}
}

// Alias names a typed decimal NULL output.
func (v Decimal128NullLiteral) Alias(name string) NamedExpr { return Named(v, name) }

// compareDecimal128 compares validated native decimal values exactly. Valid
// coefficients have magnitude < 10^38 and scale differences are at most 38,
// so alignment needs at most 76 decimal digits (< 2^253). A 256-bit magnitude
// is sufficient without overflow, division, rounding, or per-row allocation.
// The common same-scale case needs only the signed 128-bit comparison.
func compareDecimal128(a memory.Decimal128, aScale int32, b memory.Decimal128, bScale int32) int {
	if aScale == bScale {
		return a.Cmp(b)
	}
	as, bs := a.Sign(), b.Sign()
	if as < bs {
		return -1
	}
	if as > bs {
		return 1
	}
	if as == 0 {
		return 0
	}
	if as < 0 {
		a, b = a.Negate(), b.Negate()
	}
	av, bv := [4]uint64{a.LowBits(), uint64(a.HighBits())}, [4]uint64{b.LowBits(), uint64(b.HighBits())}
	if aScale < bScale {
		av = decimalMagnitudeProduct(a, decimal128.GetScaleMultiplier(int(bScale-aScale)))
	} else {
		bv = decimalMagnitudeProduct(b, decimal128.GetScaleMultiplier(int(aScale-bScale)))
	}
	for i := 3; i >= 0; i-- {
		if av[i] < bv[i] {
			return -as
		}
		if av[i] > bv[i] {
			return as
		}
	}
	return 0
}

// decimalMagnitudeProduct multiplies nonnegative 128-bit magnitudes using
// four 64x64 products. Carries are propagated before adding the next product.
func decimalMagnitudeProduct(a, b memory.Decimal128) (out [4]uint64) {
	x, y := [2]uint64{a.LowBits(), uint64(a.HighBits())}, [2]uint64{b.LowBits(), uint64(b.HighBits())}
	for i, u := range x {
		for j, v := range y {
			hi, lo := bits.Mul64(u, v)
			k := i + j
			var carry uint64
			out[k], carry = bits.Add64(out[k], lo, 0)
			out[k+1], carry = bits.Add64(out[k+1], hi, carry)
			for k += 2; k < len(out); k++ {
				out[k], carry = bits.Add64(out[k], 0, carry)
			}
		}
	}
	return out
}
