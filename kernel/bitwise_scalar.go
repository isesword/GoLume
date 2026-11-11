package kernel

import "gocolumn/memory"

type bitwiseInteger interface {
	~int32 | ~uint32 | ~int64 | ~uint64
}

func prepareBitwise(left, right, out int, validity memory.Bitmap, op BitwiseOp) {
	if op < BitwiseAnd || op > BitwiseNot {
		panic("kernel: unsupported bitwise operation")
	}
	if (op != BitwiseNot && left != right) || out < left {
		panic("kernel: bitwise length mismatch")
	}
	if validity != nil && len(validity) < (left+63)/64 {
		panic("kernel: bitwise validity too small")
	}
}

func scalarBitwise[T bitwiseInteger](left, right, out []T, validity memory.Bitmap, op BitwiseOp) {
	prepareBitwise(len(left), len(right), len(out), validity, op)
	for i := range left {
		if !validAt(validity, i) {
			out[i] = 0
			continue
		}
		switch op {
		case BitwiseAnd:
			out[i] = left[i] & right[i]
		case BitwiseOr:
			out[i] = left[i] | right[i]
		case BitwiseXor:
			out[i] = left[i] ^ right[i]
		case BitwiseNot:
			out[i] = ^left[i]
		}
	}
}
