package kernel

import (
	"fmt"
	"gocolumn/memory"
	"math"
	"math/big"
	"math/rand/v2"
	"slices"
	"testing"
)

type checkedInteger64 interface{ ~int64 | ~uint64 }

func checkedReference[T checkedInteger64](a, b T, op ArithmeticOp, signed bool) (T, bool) {
	x, y := new(big.Int), new(big.Int)
	if signed {
		x.SetInt64(int64(a))
		y.SetInt64(int64(b))
	} else {
		x.SetUint64(uint64(a))
		y.SetUint64(uint64(b))
	}
	switch op {
	case ArithmeticAdd:
		x.Add(x, y)
	case ArithmeticSub:
		x.Sub(x, y)
	case ArithmeticMul:
		x.Mul(x, y)
	}
	failed := !x.IsUint64()
	if signed {
		failed = !x.IsInt64()
	}
	var v T
	switch op {
	case ArithmeticAdd:
		v = a + b
	case ArithmeticSub:
		v = a - b
	case ArithmeticMul:
		v = a * b
	}
	return v, failed
}

func checkInteger64Reference[T checkedInteger64](t *testing.T, a, b []T, signed bool, run func([]T, []T, []T, memory.Bitmap, memory.Bitmap, ArithmeticOp) int) {
	t.Helper()
	n := len(a)
	for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul} {
		for _, maskKind := range []string{"nil", "dense", "alternating", "sparse", "none"} {
			var valid memory.Bitmap
			if maskKind != "nil" {
				valid = make(memory.Bitmap, memory.WordsFor(n))
				for i := range valid {
					switch maskKind {
					case "dense":
						valid[i] = ^uint64(0)
					case "alternating":
						valid[i] = 0xaaaaaaaaaaaaaaaa
					case "sparse":
						valid[i] = 1 << uint(i%64)
					}
				}
			}
			want := make([]T, n)
			first := -1
			failures := make(memory.Bitmap, memory.WordsFor(n))
			for i := range a {
				if bitmapWord(valid, i/64)&(1<<uint(i%64)) == 0 {
					continue
				}
				value, failed := checkedReference(a[i], b[i], op, signed)
				want[i] = value
				if failed {
					memory.Set(failures, i)
					if first < 0 {
						first = i
					}
				}
			}
			for _, alias := range []string{"none", "left", "right"} {
				for _, withMask := range []bool{false, true} {
					left, right := slices.Clone(a), slices.Clone(b)
					out := make([]T, n+1)
					out[n] = 123
					switch alias {
					case "left":
						left = append(left, 123)
						out = left
						left = left[:n]
					case "right":
						right = append(right, 123)
						out = right
						right = right[:n]
					}
					var over memory.Bitmap
					if withMask {
						over = make(memory.Bitmap, len(failures)+1)
						for i := range over {
							over[i] = ^uint64(0)
						}
					}
					got := run(left, right, out, valid, over, op)
					if got != first || !slices.Equal(out[:n], want) || out[n] != 123 {
						t.Fatalf("n=%d signed=%t op=%s validity=%s alias=%s bitmap=%t first=%d want=%d values_match=%t canary=%v", n, signed, op, maskKind, alias, withMask, got, first, slices.Equal(out[:n], want), out[n])
					}
					if withMask && (!slices.Equal(over[:len(failures)], failures) || over[len(failures)] != ^uint64(0)) {
						t.Fatalf("overflow bitmap n=%d signed=%t op=%s mask=%s: %x want %x", n, signed, op, maskKind, over, failures)
					}
				}
			}
		}
	}
}

func TestCheckedInteger64BigReference(t *testing.T) {
	edges := []uint64{0, 1, 2, 3, 0x7fffffff, 0x80000000, 0xffffffff, 0x100000000, 0x100000001, 0x7fffffffffffffff, 0x8000000000000000, 0x8000000000000001, 0xffffffff00000000, 0xfffffffffffffffe, ^uint64(0)}
	rng := rand.New(rand.NewPCG(31256, 98726))
	for _, n := range []int{0, 1, 2, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 1025} {
		a, b := make([]uint64, n), make([]uint64, n)
		sa, sb := make([]int64, n), make([]int64, n)
		for i := range a {
			if i < len(edges)*len(edges) {
				a[i] = edges[i%len(edges)]
				b[i] = edges[(i/len(edges))%len(edges)]
			} else {
				a[i] = rng.Uint64()
				b[i] = rng.Uint64()
				if i%3 == 0 {
					a[i] >>= 32
				}
				if i%7 == 0 {
					b[i] >>= 32
				}
			}
			sa[i], sb[i] = int64(a[i]), int64(b[i])
		}
		for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
			set := New(mode)
			t.Run(fmt.Sprintf("rows%d/%s", n, mode), func(t *testing.T) {
				checkInteger64Reference(t, a, b, false, set.Uint64Arithmetic)
				checkInteger64Reference(t, sa, sb, true, set.Int64Arithmetic)
			})
		}
	}
}

func TestCheckedInteger64EachOverflowRow(t *testing.T) {
	const n = 131
	for _, mode := range []Mode{ModeAuto, ModeSIMD} {
		set := New(mode)
		for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul} {
			for row := range n {
				a, b, out := make([]int64, n), make([]int64, n), make([]int64, n)
				ua, ub, uo := make([]uint64, n), make([]uint64, n), make([]uint64, n)
				for i := range a {
					a[i], b[i], ua[i], ub[i] = 2, 1, 2, 1
				}
				switch op {
				case ArithmeticAdd:
					a[row], b[row], ua[row], ub[row] = math.MaxInt64, 1, math.MaxUint64, 1
				case ArithmeticSub:
					a[row], b[row], ua[row], ub[row] = math.MinInt64, 1, 0, 1
				case ArithmeticMul:
					a[row], b[row], ua[row], ub[row] = math.MinInt64, -1, math.MaxUint64, 2
				}
				if got := set.Int64Arithmetic(a, b, out, nil, nil, op); got != row {
					t.Fatalf("signed %s lane %d got %d", op, row, got)
				}
				if got := set.Uint64Arithmetic(ua, ub, uo, nil, nil, op); got != row {
					t.Fatalf("unsigned %s lane %d got %d", op, row, got)
				}
				mask := make(memory.Bitmap, memory.WordsFor(n))
				for i := range mask {
					mask[i] = ^uint64(0)
				}
				mask[row/64] &^= 1 << uint(row%64)
				if got := set.Int64Arithmetic(a, b, out, mask, nil, op); got != -1 || out[row] != 0 {
					t.Fatalf("signed inactive overflow %s row %d got %d", op, row, got)
				}
				if got := set.Uint64Arithmetic(ua, ub, uo, mask, nil, op); got != -1 || uo[row] != 0 {
					t.Fatalf("unsigned inactive overflow %s row %d got %d", op, row, got)
				}
			}
		}
	}
}

func TestCheckedInteger64PartialAliases(t *testing.T) {
	for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul} {
		for _, shift := range []int{-3, -1, 1, 3} {
			for _, rightAlias := range []bool{false, true} {
				const n = 131
				backing := make([]int64, n+6)
				for i := range backing {
					backing[i] = int64(i*37 + 1)
				}
				wantBacking := slices.Clone(backing)
				other := make([]int64, n)
				for i := range other {
					other[i] = int64(i%5 - 2)
				}
				source := backing[3 : 3+n]
				dest := backing[3+shift : 3+shift+n]
				wantSource := wantBacking[3 : 3+n]
				wantDest := wantBacking[3+shift : 3+shift+n]
				a, b, wa, wb := source, other, wantSource, other
				if rightAlias {
					a, b, wa, wb = other, source, other, wantSource
				}
				want := New(ModeScalar).Int64Arithmetic(wa, wb, wantDest, nil, nil, op)
				got := New(ModeSIMD).Int64Arithmetic(a, b, dest, nil, nil, op)
				if got != want || !slices.Equal(backing, wantBacking) {
					t.Fatalf("partial alias op=%s shift=%d right=%t", op, shift, rightAlias)
				}
			}
		}
	}
}

func TestCheckedInteger64NoAllocations(t *testing.T) {
	const n = 4097
	a, b, out := make([]int64, n), make([]int64, n), make([]int64, n)
	ua, ub, uo := make([]uint64, n), make([]uint64, n), make([]uint64, n)
	valid, over := make(memory.Bitmap, memory.WordsFor(n)), make(memory.Bitmap, memory.WordsFor(n))
	for i := range valid {
		valid[i] = 0xeeeeeeeeeeeeeeee
	}
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		set := New(mode)
		for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul} {
			if got := testing.AllocsPerRun(100, func() {
				set.Int64Arithmetic(a, b, out, valid, over, op)
				set.Uint64Arithmetic(ua, ub, uo, valid, over, op)
			}); got != 0 {
				t.Fatalf("mode=%s op=%s allocations=%g", mode, op, got)
			}
		}
	}
}

func BenchmarkCheckedInteger64(b *testing.B) {
	for _, n := range []int{1024, 65536} {
		a, r, out := make([]int64, n), make([]int64, n), make([]int64, n)
		ua, ur, uo := make([]uint64, n), make([]uint64, n), make([]uint64, n)
		for i := range a {
			a[i], r[i] = int64(i*11-123), int64(i%5-2)
			ua[i], ur[i] = uint64(i*11+123), uint64(i%5)
		}
		for _, kind := range []string{"dense", "null50", "null99"} {
			var valid memory.Bitmap
			if kind != "dense" {
				valid = make(memory.Bitmap, memory.WordsFor(n))
				for i := range valid {
					if kind == "null50" {
						valid[i] = 0xaaaaaaaaaaaaaaaa
					} else {
						valid[i] = 1
					}
				}
			}
			over := make(memory.Bitmap, memory.WordsFor(n))
			for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul} {
				for _, mode := range []Mode{ModeScalar, ModeAuto} {
					set := New(mode)
					b.Run(fmt.Sprintf("Int64/%s/%s/rows%d/%s", op, kind, n, mode), func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							set.Int64Arithmetic(a, r, out, valid, over, op)
						}
					})
					b.Run(fmt.Sprintf("Uint64/%s/%s/rows%d/%s", op, kind, n, mode), func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							set.Uint64Arithmetic(ua, ur, uo, valid, over, op)
						}
					})
				}
			}
		}
	}
}
