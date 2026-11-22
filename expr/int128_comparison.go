package expr

import (
	"cmp"
	"fmt"
	"gocolumn/memory"
)

// Int128Literal is an exact signed binary 128-bit literal. It is an expression
// value, not a storage column or a Decimal coefficient.
type Int128Literal struct {
	Value memory.Int128
}

func (Int128Literal) exprNode() {}

// I128 constructs an Int128 literal from two's-complement limbs: high is the
// signed upper limb and low is the unsigned lower limb. For example, I128(0, 1)
// is one and I128(-1, ^uint64(0)) is minus one. Int128 comparisons require both
// operands to have type Int128; no implicit promotion is performed.
func I128(high int64, low uint64) Int128Literal {
	return Int128Literal{Value: memory.Int128{High: high, Low: low}}
}

// String returns a typed canonical decimal spelling for plans and diagnostics.
func (v Int128Literal) String() string {
	var scratch [40]byte
	return "INT128(" + string(formatInt128Decimal(&scratch, v.Value)) + ")"
}

// Alias names the Int128 workspace result; ordinary batch projection does not
// materialize Int128.
func (v Int128Literal) Alias(name string) NamedExpr { return Named(v, name) }

func int128ComparisonOp(op Op) bool {
	switch op {
	case OpGT, OpGE, OpLT, OpLE, OpEQ:
		return true
	default:
		return false
	}
}

func evalInt128Comparison(out, a, b *scalarRegister, op Op, rows int) error {
	if a.typ != memory.TypeInt128 || b.typ != memory.TypeInt128 ||
		len(a.i128) < rows || len(b.i128) < rows || !int128ComparisonOp(op) {
		return fmt.Errorf("invalid int128 comparison")
	}
	// Existing Boolean execution has already intersected SQL validity and the
	// active region masks. Only demanded non-NULL pairs inspect their values.
	for row := range rows {
		if row&4095 == 0 {
			if err := out.checkContext(); err != nil {
				return err
			}
		}
		if !isValidRegister(out, row) {
			continue
		}
		c := cmp.Compare(a.i128[row].High, b.i128[row].High)
		if c == 0 {
			c = cmp.Compare(a.i128[row].Low, b.i128[row].Low)
		}
		if compareI64(int64(c), 0, op) {
			memory.Set(out.Truth, row)
		}
	}
	return out.checkContext()
}
