package kernel

import (
	"gocolumn/memory"
	"math"
)

type mathSigned interface {
	~int8 | ~int16 | ~int32 | ~int64
}
type mathUnsigned interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64
}
type mathFloat interface{ ~float32 | ~float64 }

func scalarMathFloatUnary[T mathFloat](in, out []T, validity memory.Bitmap, op MathUnaryOp) {
	if len(out) < len(in) {
		panic("kernel: math output too small")
	}
	for i, v := range in {
		if !validAt(validity, i) {
			out[i] = 0
			continue
		}
		x := float64(v)
		switch op {
		case MathAbs:
			out[i] = T(math.Abs(x))
		case MathRound:
			out[i] = T(math.RoundToEven(x))
		case MathFloor:
			out[i] = T(math.Floor(x))
		case MathCeil:
			out[i] = T(math.Ceil(x))
		case MathSqrt:
			out[i] = T(math.Sqrt(x))
		case MathLog:
			out[i] = T(math.Log(x))
		default:
			panic("kernel: unsupported float unary math operation")
		}
	}
}

func scalarMathFloatBinary[T mathFloat](left, right, out []T, validity memory.Bitmap, op MathBinaryOp) {
	if len(left) != len(right) || len(out) < len(left) {
		panic("kernel: math length mismatch")
	}
	for i, a := range left {
		if !validAt(validity, i) {
			out[i] = 0
			continue
		}
		x, y := float64(a), float64(right[i])
		switch op {
		case MathMod:
			// Truncated remainder followed by a sign correction avoids the catastrophic
			// cancellation of x-floor(x/y)*y for very different finite magnitudes.
			r := math.Mod(x, y)
			if math.IsInf(y, 0) {
				r = math.NaN()
			}
			if r != 0 && !math.IsNaN(r) && math.Signbit(r) != math.Signbit(y) {
				r += y
			}
			if r == 0 {
				r = math.Copysign(0, y)
			}
			out[i] = T(r)
		case MathFloorDiv:
			out[i] = T(math.Floor(x / y))
		case MathPow:
			out[i] = T(math.Pow(x, y))
		default:
			panic("kernel: unsupported float binary math operation")
		}
	}
}

func scalarMathSignedAbs[T mathSigned](in, out []T, validity, failures memory.Bitmap) int {
	prepareIntegerArithmetic(len(in), len(in), len(in), len(out), failures)
	count := 0
	for i, v := range in {
		if !validAt(validity, i) {
			out[i] = 0
			continue
		}
		if v < 0 {
			v = -v
		}
		out[i] = v
	}
	return count
}

func scalarMathSignedBinary[T mathSigned](left, right, out []T, validity, failures memory.Bitmap, op MathBinaryOp) int {
	prepareIntegerArithmetic(len(left), len(left), len(right), len(out), failures)
	count := 0
	for i, a := range left {
		if !validAt(validity, i) {
			out[i] = 0
			continue
		}
		b, failed := right[i], false
		switch op {
		case MathMod:
			if b == 0 {
				failed = true
				out[i] = 0
			} else {
				r := a % b
				if r != 0 && (r < 0) != (b < 0) {
					r += b
				}
				out[i] = r
			}
		case MathFloorDiv:
			if b == 0 {
				failed = true
				out[i] = 0
			} else {
				q, r := a/b, a%b
				// Go defines MinInt / -1 as wrapping MinInt.
				if r != 0 && (r < 0) != (b < 0) {
					q--
				}
				out[i] = q
			}
		case MathPow:
			if b < 0 {
				failed = true
				out[i] = 0
			} else {
				acc, base, n := T(1), a, b
				for n != 0 {
					if n&1 != 0 {
						acc *= base
					}
					n >>= 1
					if n != 0 {
						base *= base
					}
				}
				out[i] = acc
			}
		default:
			panic("kernel: unsupported signed math operation")
		}
		if failed {
			count++
			if failures != nil {
				memory.Set(failures, i)
			}
		}
	}
	return count
}

func scalarMathUnsignedBinary[T mathUnsigned](left, right, out []T, validity, failures memory.Bitmap, op MathBinaryOp) int {
	prepareIntegerArithmetic(len(left), len(left), len(right), len(out), failures)
	count := 0
	for i, a := range left {
		if !validAt(validity, i) {
			out[i] = 0
			continue
		}
		b, failed := right[i], false
		switch op {
		case MathMod:
			if b == 0 {
				failed = true
				out[i] = 0
			} else {
				out[i] = a % b
			}
		case MathFloorDiv:
			if b == 0 {
				failed = true
				out[i] = 0
			} else {
				out[i] = a / b
			}
		case MathPow:
			acc, base, n := T(1), a, b
			for n != 0 {
				if n&1 != 0 {
					v := acc * base
					acc = v
				}
				n >>= 1
				if n != 0 {
					v := base * base
					base = v
				}
			}
			out[i] = acc
		default:
			panic("kernel: unsupported unsigned math operation")
		}
		if failed {
			count++
			if failures != nil {
				memory.Set(failures, i)
			}
		}
	}
	return count
}
