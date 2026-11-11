package kernel

import (
	"fmt"
	"gocolumn/memory"
	"math/rand"
	"slices"
	"testing"
)

// Reference truth table operates one bit at a time, independently of the
// implementation's word operators (including signed complement semantics).
func bitwiseReference[T bitwiseInteger](a, b T, op BitwiseOp) T {
	var out uint64
	for bit := uint(0); bit < 64; bit++ {
		x, y := uint64(a)>>bit&1 != 0, uint64(b)>>bit&1 != 0
		keep := false
		switch op {
		case BitwiseAnd:
			keep = x && y
		case BitwiseOr:
			keep = x || y
		case BitwiseXor:
			keep = x != y
		case BitwiseNot:
			keep = !x
		}
		if keep {
			out |= 1 << bit
		}
	}
	return T(out)
}

func bitwiseValidity(n, pattern int) memory.Bitmap {
	if pattern == 0 {
		return nil
	}
	v := make(memory.Bitmap, (n+63)/64)
	for i := range v {
		switch pattern {
		case 1:
			v[i] = ^uint64(0)
		case 2:
			v[i] = 0
		case 3:
			v[i] = 0xaaaaaaaaaaaaaaaa
		case 4:
			v[i] = 0x8000000000000001
		case 5:
			if i%2 == 0 {
				v[i] = ^uint64(0)
			}
		case 6:
			v[i] = 0x719dd2ac5e03a4b1
		}
	}
	return v
}

func testBitwiseType[T bitwiseInteger](t *testing.T, fn func([]T, []T, []T, memory.Bitmap, BitwiseOp)) {
	rng := rand.New(rand.NewSource(194))
	lengths := []int{255, 256, 257, 1025}
	for n := 0; n <= 130; n++ {
		lengths = append(lengths, n)
	}
	for _, n := range lengths {
		for offset := 0; offset < 4; offset++ {
			aa, bb, oo := make([]T, n+offset+2), make([]T, n+offset+2), make([]T, n+offset+2)
			a, b, o := aa[offset+1:offset+1+n], bb[offset+1:offset+1+n], oo[offset+1:offset+1+n]
			for i := range a {
				a[i] = T(rng.Uint64())
				b[i] = T(rng.Uint64())
			}
			// Explicit signed minima/maxima, zero, all bits, and alternating bits.
			for i, x := range []uint64{0, ^uint64(0), 1 << 63, 1 << 31, 0x7fffffff, 0x7fffffffffffffff, 0xaaaaaaaaaaaaaaaa, 0x5555555555555555} {
				if i < n {
					a[i] = T(x)
					b[i] = T(^x)
				}
			}
			for pattern := 0; pattern < 7; pattern++ {
				v := bitwiseValidity(n, pattern)
				saved := slices.Clone(v)
				for op := BitwiseAnd; op <= BitwiseNot; op++ {
					want := make([]T, n)
					for i := range want {
						if validAt(v, i) {
							want[i] = bitwiseReference(a[i], b[i], op)
						}
					}
					for i := range oo {
						oo[i] = T(91)
					}
					right := b
					if op == BitwiseNot {
						right = nil
					}
					fn(a, right, o, v, op)
					if !slices.Equal(o, want) {
						t.Fatalf("n=%d offset=%d pattern=%d op=%s", n, offset, pattern, op)
					}
					if oo[offset] != 91 || oo[len(oo)-1] != 91 {
						t.Fatal("output sentinel overwritten")
					}
					if !slices.Equal(v, saved) {
						t.Fatal("validity mutated")
					}
					if offset == 0 { // Both supported exact in-place aliases.
						ac, bc := slices.Clone(a), slices.Clone(b)
						fn(ac, bc, ac, v, op)
						if !slices.Equal(ac, want) {
							t.Fatalf("left alias %s n=%d", op, n)
						}
						ac, bc = slices.Clone(a), slices.Clone(b)
						fn(ac, bc, bc, v, op)
						if !slices.Equal(bc, want) {
							t.Fatalf("right alias %s n=%d", op, n)
						}
					}
				}
			}
		}
	}
	a, b, o := make([]T, 257), make([]T, 257), make([]T, 257)
	wide := make([]T, 260)
	for i := range wide {
		wide[i] = 91
	}
	fn(a, b, wide, nil, BitwiseAnd)
	if wide[257] != 91 || wide[258] != 91 || wide[259] != 91 {
		t.Fatal("oversized output suffix changed")
	}

	for pattern := 0; pattern < 7; pattern++ {
		v := bitwiseValidity(len(a), pattern)
		for op := BitwiseAnd; op <= BitwiseNot; op++ {
			if alloc := testing.AllocsPerRun(10, func() { fn(a, b, o, v, op) }); alloc != 0 {
				t.Fatalf("%s allocations: %v", op, alloc)
			}
		}
	}
	for _, c := range []struct {
		left, right, out int
		v                memory.Bitmap
		op               BitwiseOp
	}{
		{1, 0, 1, nil, BitwiseAnd}, {1, 1, 0, nil, BitwiseOr}, {65, 65, 65, memory.Bitmap{1}, BitwiseXor},
		{0, 0, 0, nil, 0}, {0, 0, 0, nil, 255}, {2, 2, 2, nil, 255},
	} {
		func() {
			for i := range o {
				o[i] = 91
			}
			defer func() {
				for _, x := range o {
					if x != 91 {
						t.Fatal("invalid request mutated output before panic")
					}
				}
				if recover() == nil {
					t.Fatalf("expected panic: %+v", c)
				}
			}()
			fn(a[:c.left], b[:c.right], o[:c.out], c.v, c.op)
		}()
	}
}

func TestBitwise(t *testing.T) {
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		s := New(mode)
		t.Run(mode.String()+"/Int32", func(t *testing.T) { testBitwiseType(t, s.BitwiseInt32) })
		t.Run(mode.String()+"/Uint32", func(t *testing.T) { testBitwiseType(t, s.BitwiseUint32) })
		t.Run(mode.String()+"/Int64", func(t *testing.T) { testBitwiseType(t, s.BitwiseInt64) })
		t.Run(mode.String()+"/Uint64", func(t *testing.T) { testBitwiseType(t, s.BitwiseUint64) })
	}
}

func TestBitwiseIDs(t *testing.T) {
	ids := KernelIDs()
	for _, typ := range []memory.DataType{memory.TypeInt32, memory.TypeUint32, memory.TypeInt64, memory.TypeUint64} {
		for op := BitwiseAnd; op <= BitwiseNot; op++ {
			id, ok := BitwiseID(typ, op)
			if !ok || !slices.Contains(ids, id) || id.String() != fmt.Sprintf("bitwise-%s-%s", typ, op) {
				t.Fatal(id, ok)
			}
			cap, ok := New(ModeScalar).Capability(id)
			if !ok || cap.HardwareSIMD || cap.Backend != "scalar" {
				t.Fatal(id, cap, ok)
			}
		}
	}
	for _, typ := range []memory.DataType{memory.TypeInt8, memory.TypeUint16, memory.TypeFloat32, memory.TypeFloat64, memory.TypeBool} {
		if _, ok := BitwiseID(typ, BitwiseAnd); ok {
			t.Fatal("unsupported type", typ)
		}
	}
	for _, op := range []BitwiseOp{0, 255} {
		if _, ok := BitwiseID(memory.TypeInt64, op); ok {
			t.Fatal(op)
		}
	}
	if KernelBitwise.String() != "bitwise" {
		t.Fatal(KernelBitwise)
	}
}

func benchmarkBitwiseType[T bitwiseInteger](b *testing.B, fn func([]T, []T, []T, memory.Bitmap, BitwiseOp), n, width int, v memory.Bitmap, op BitwiseOp) {
	a, c, o := make([]T, n), make([]T, n), make([]T, n)
	for i := range a {
		a[i] = T(i * 19)
		c[i] = T(i * 31)
	}
	b.SetBytes(int64(n * width))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		fn(a, c, o, v, op)
	}
}
func BenchmarkBitwise(b *testing.B) {
	for _, mode := range []Mode{ModeScalar, ModeAuto} {
		s := New(mode)
		for _, n := range []int{31, 1024, 65536} {
			for _, pattern := range []int{0, 3} {
				for op := BitwiseAnd; op <= BitwiseNot; op++ {
					name := fmt.Sprintf("%s/n=%d/validity=%d/%s", mode, n, pattern, op)
					v := bitwiseValidity(n, pattern)
					b.Run(name+"/Int32", func(b *testing.B) { benchmarkBitwiseType(b, s.BitwiseInt32, n, 4, v, op) })
					b.Run(name+"/Uint32", func(b *testing.B) { benchmarkBitwiseType(b, s.BitwiseUint32, n, 4, v, op) })
					b.Run(name+"/Int64", func(b *testing.B) { benchmarkBitwiseType(b, s.BitwiseInt64, n, 8, v, op) })
					b.Run(name+"/Uint64", func(b *testing.B) { benchmarkBitwiseType(b, s.BitwiseUint64, n, 8, v, op) })
				}
			}
		}
	}
}

// A scalar/SIMD equality check alone would accept two no-op fixture arms.
// Check every bitwise benchmark ID against independent expected values.
func TestBitwiseKernelIDFixture(t *testing.T) {
	f := newKernelBenchFixture(129)
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		set := New(mode)
		for _, typ := range []memory.DataType{memory.TypeInt32, memory.TypeUint32, memory.TypeInt64, memory.TypeUint64} {
			for op := BitwiseAnd; op <= BitwiseNot; op++ {
				id, _ := BitwiseID(typ, op)
				f.outputs[0].clear()
				f.invoke(set, id, 0)
				for i := 0; i < f.rows; i++ {
					var got, want uint64
					switch typ {
					case memory.TypeInt32:
						got, want = uint64(f.outputs[0].i32[i]), uint64(bitwiseReference(f.i32a[i], f.i32b[i], op))
					case memory.TypeUint32:
						got, want = uint64(f.outputs[0].u32[i]), uint64(bitwiseReference(f.u32a[i], f.u32b[i], op))
					case memory.TypeInt64:
						got, want = uint64(f.outputs[0].i64[i]), uint64(bitwiseReference(f.i64a[i], f.i64b[i], op))
					case memory.TypeUint64:
						got, want = f.outputs[0].u64[i], bitwiseReference(f.u64a[i], f.u64b[i], op)
					}
					if got != want {
						t.Fatalf("%s %s row=%d got=%x want=%x", mode, id, i, got, want)
					}
				}
			}
		}
	}
}
