package expr

import (
	"fmt"
	"gocolumn/memory"
	"math/bits"
)

// AddInt128 adds one int64 and one uint64 operand without conversion to float.
// The binary int128 result is available through expression workspaces only.
func AddInt128(left, right Expr) Binary { return Binary{Op: OpAddInt128, Left: left, Right: right} }

// SubInt128 subtracts two operands in their written order, one int64 and one
// uint64. The exact binary int128 result is available in expression workspaces.
func SubInt128(left, right Expr) Binary { return Binary{Op: OpSubInt128, Left: left, Right: right} }

// MulInt128 multiplies one int64 and one uint64 operand. Every single product
// for this operand pair fits signed binary int128, even at both extrema.
func MulInt128(left, right Expr) Binary { return Binary{Op: OpMulInt128, Left: left, Right: right} }

func isInt128Op(op Op) bool {
	return op == OpAddInt128 || op == OpSubInt128 || op == OpMulInt128
}

func mixedInt64Uint64(a, b memory.DataType) bool {
	return a == memory.TypeInt64 && b == memory.TypeUint64 || a == memory.TypeUint64 && b == memory.TypeInt64
}

func supportsInt128Child(e Expr) bool {
	switch n := e.(type) {
	case IfExpr, CaseExpr, CoalesceExpr:
		return true
	case Unary:
		return n.Op == OpIsNull || n.Op == OpIsNotNull
	default:
		return false
	}
}

func producesInt128(e Expr) bool {
	switch n := e.(type) {
	case Binary:
		return isInt128Op(n.Op)
	case NullLiteral, IfExpr, CaseExpr, CoalesceExpr:
		return true
	default:
		return false
	}
}

func evalInt128Binary(out, a, b *scalarRegister, op Op, rows int) error {
	if !mixedInt64Uint64(a.typ, b.typ) || !isInt128Op(op) {
		return fmt.Errorf("invalid mixed integer operands or operator for int128")
	}
	signed, unsigned := a, b
	signedFirst := a.typ == memory.TypeInt64
	if !signedFirst {
		signed, unsigned = b, a
	}
	for row := 0; row < rows; row++ {
		if row&4095 == 0 {
			if err := out.checkContext(); err != nil {
				return err
			}
		}
		if !isValidRegister(out, row) {
			out.i128[row] = memory.Int128{}
			continue
		}
		out.i128[row] = mixedInt128Value(signed.i64[row], unsigned.u64[row], op, signedFirst)
	}
	return nil
}

// All sums, differences and individual products of an int64 and a uint64 fit
// signed int128. That proof is specific to the checked input type pair.
func mixedInt128Value(s int64, u uint64, op Op, signedFirst bool) memory.Int128 {
	sign := uint64(s >> 63)
	var hi, lo, carry uint64
	switch op {
	case OpAddInt128:
		lo, carry = bits.Add64(uint64(s), u, 0)
		hi = sign + carry
	case OpSubInt128:
		if signedFirst {
			lo, carry = bits.Sub64(uint64(s), u, 0)
			hi = sign - carry
		} else {
			lo, carry = bits.Sub64(u, uint64(s), 0)
			hi = 0 - sign - carry
		}
	case OpMulInt128:
		magnitude := uint64(s)
		if s < 0 {
			magnitude = 0 - magnitude // Includes MinInt64 without signed overflow.
		}
		hi, lo = bits.Mul64(magnitude, u)
		if s < 0 {
			lo, carry = bits.Add64(^lo, 1, 0)
			hi = ^hi + carry
		}
	}
	return memory.Int128{High: int64(hi), Low: lo}
}
