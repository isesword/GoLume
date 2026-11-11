package kernel

import (
	"fmt"
	"gocolumn/memory"
)

// BitwiseOp identifies a type-preserving integer bitwise operation.
type BitwiseOp uint8

const (
	// BitwiseAnd computes the bitwise intersection.
	BitwiseAnd BitwiseOp = iota + 1
	// BitwiseOr computes the bitwise union.
	BitwiseOr
	// BitwiseXor computes the bitwise exclusive union.
	BitwiseXor
	// BitwiseNot complements every bit and ignores the right input.
	BitwiseNot
)

// String returns the stable bitwise operation name.
func (op BitwiseOp) String() string {
	switch op {
	case BitwiseAnd:
		return "and"
	case BitwiseOr:
		return "or"
	case BitwiseXor:
		return "xor"
	case BitwiseNot:
		return "not"
	}
	return fmt.Sprintf("bitwise(%d)", op)
}

// BitwiseID returns the identifier for a supported integer bitwise kernel.
func BitwiseID(typ memory.DataType, op BitwiseOp) (KernelID, bool) {
	if op < BitwiseAnd || op > BitwiseNot {
		return KernelID{}, false
	}
	switch typ {
	case memory.TypeInt32, memory.TypeUint32, memory.TypeInt64, memory.TypeUint64:
		return KernelID{kind: KernelBitwise, typ: typ, op: uint8(op)}, true
	default:
		return KernelID{}, false
	}
}

type bitwiseDispatch struct {
	int32  func([]int32, []int32, []int32, memory.Bitmap, BitwiseOp)
	uint32 func([]uint32, []uint32, []uint32, memory.Bitmap, BitwiseOp)
	int64  func([]int64, []int64, []int64, memory.Bitmap, BitwiseOp)
	uint64 func([]uint64, []uint64, []uint64, memory.Bitmap, BitwiseOp)
}

func installBitwiseScalar(d *dispatch) {
	d.bitwise.int32 = scalarBitwise[int32]
	d.bitwise.uint32 = scalarBitwise[uint32]
	d.bitwise.int64 = scalarBitwise[int64]
	d.bitwise.uint64 = scalarBitwise[uint64]
}

// BitwiseInt32 computes a type-preserving operation over left. Binary inputs
// must have equal lengths; Not ignores right (which may be nil). out must hold
// len(left) elements. Nil validity means all valid; otherwise it must cover
// left. Invalid output rows are zero and validity is not modified. Exact
// in-place operation on either input is supported; partial overlap is not.
// Invalid operations or lengths panic before modifying output.
func (s Set) BitwiseInt32(left, right, out []int32, validity memory.Bitmap, op BitwiseOp) {
	s.d.bitwise.int32(left, right, out, validity, op)
}

// BitwiseUint32 computes a type-preserving operation over left. Binary inputs
// must have equal lengths; Not ignores right (which may be nil). out must hold
// len(left) elements. Nil validity means all valid; otherwise it must cover
// left. Invalid output rows are zero and validity is not modified. Exact
// in-place operation on either input is supported; partial overlap is not.
// Invalid operations or lengths panic before modifying output.
func (s Set) BitwiseUint32(left, right, out []uint32, validity memory.Bitmap, op BitwiseOp) {
	s.d.bitwise.uint32(left, right, out, validity, op)
}

// BitwiseInt64 computes a type-preserving operation over left. Binary inputs
// must have equal lengths; Not ignores right (which may be nil). out must hold
// len(left) elements. Nil validity means all valid; otherwise it must cover
// left. Invalid output rows are zero and validity is not modified. Exact
// in-place operation on either input is supported; partial overlap is not.
// Invalid operations or lengths panic before modifying output.
func (s Set) BitwiseInt64(left, right, out []int64, validity memory.Bitmap, op BitwiseOp) {
	s.d.bitwise.int64(left, right, out, validity, op)
}

// BitwiseUint64 computes a type-preserving operation over left. Binary inputs
// must have equal lengths; Not ignores right (which may be nil). out must hold
// len(left) elements. Nil validity means all valid; otherwise it must cover
// left. Invalid output rows are zero and validity is not modified. Exact
// in-place operation on either input is supported; partial overlap is not.
// Invalid operations or lengths panic before modifying output.
func (s Set) BitwiseUint64(left, right, out []uint64, validity memory.Bitmap, op BitwiseOp) {
	s.d.bitwise.uint64(left, right, out, validity, op)
}
