package kernel

import (
	"gocolumn/memory"
	"math"
	"testing"
)

func TestIndependent32EachFailure(t *testing.T) {
	s := New(ModeSIMD)
	for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul} {
		for pos := 0; pos < 193; pos++ {
			a, b, o := make([]int32, 193), make([]int32, 193), make([]int32, 193)
			u, v, w := make([]uint32, 193), make([]uint32, 193), make([]uint32, 193)
			valid, over := memory.Bitmap{^uint64(0), ^uint64(0), ^uint64(0), ^uint64(0)}, memory.Bitmap{0, 0, 0, 0}
			a[pos], b[pos], u[pos], v[pos] = math.MaxInt32, 2, math.MaxUint32, 2
			if op == ArithmeticSub {
				a[pos], b[pos], u[pos], v[pos] = math.MinInt32, 1, 0, 1
			}
			if f := s.Int32Arithmetic(a, b, o, valid, over, op); f != pos {
				t.Fatalf("signed %v pos%d got%d", op, pos, f)
			}
			for row := 0; row < 256; row++ {
				if memory.IsSet(over, row) != (row == pos) {
					t.Fatalf("signed bitmap pos%d row%d", pos, row)
				}
			}
			if f := s.Uint32Arithmetic(u, v, w, valid, over, op); f != pos {
				t.Fatalf("unsigned %v pos%d got%d", op, pos, f)
			}
			for row := 0; row < 256; row++ {
				if memory.IsSet(over, row) != (row == pos) {
					t.Fatalf("unsigned bitmap pos%d row%d", pos, row)
				}
			}
			valid[pos/64] &= ^(uint64(1) << uint(pos%64))
			if f := s.Int32Arithmetic(a, b, o, valid, over, op); f != -1 || o[pos] != 0 {
				t.Fatal("masked signed", pos, f)
			}
			if f := s.Uint32Arithmetic(u, v, w, valid, over, op); f != -1 || w[pos] != 0 {
				t.Fatal("masked unsigned", pos, f)
			}
		}
	}
}
func TestIndependent32SignedMulBoundary(t *testing.T) {
	s := New(ModeSIMD)
	vals := []int32{math.MinInt32, math.MinInt32 + 1, -1073741824, -65537, -65536, -46341, -46340, -3, -2, -1, 0, 1, 2, 3, 46340, 46341, 65535, 65536, 1073741823, 1073741824, math.MaxInt32}
	for _, a := range vals {
		for _, b := range vals {
			for offset := 0; offset < 16; offset++ {
				x, y, o := make([]int32, 67), make([]int32, 67), make([]int32, 67)
				for i := range x {
					x[i] = vals[(i+offset)%len(vals)]
					y[i] = vals[(i*3+offset)%len(vals)]
				}
				x[offset], y[offset] = a, b
				over := memory.Bitmap{0, 0}
				wantFirst := -1
				got := s.Int32Arithmetic(x, y, o, nil, over, ArithmeticMul)
				for i := range x {
					exact := int64(x[i]) * int64(y[i])
					fail := exact < math.MinInt32 || exact > math.MaxInt32
					if fail && wantFirst < 0 {
						wantFirst = i
					}
					if o[i] != int32(exact) || memory.IsSet(over, i) != fail {
						t.Fatalf("a%d b%d row%d product%d output%d overflow%t", x[i], y[i], i, exact, o[i], memory.IsSet(over, i))
					}
				}
				if got != wantFirst {
					t.Fatal(got, wantFirst)
				}
			}
		}
	}
}
