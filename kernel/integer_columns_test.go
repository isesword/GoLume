package kernel

import (
	"fmt"
	"gocolumn/memory"
	"math/rand/v2"
	"slices"
	"testing"
)

type integerColumnNumber interface {
	~int8 | ~int16 | ~int64 | ~uint8 | ~uint16 | ~uint64
}
type integerColumnCall[T integerColumnNumber] func([]T, []T, memory.Bitmap, memory.Bitmap, memory.Bitmap, CompareOp) int
type integerColumnRunner[T integerColumnNumber] struct {
	name string
	call integerColumnCall[T]
}

func integerColumnRunners[T integerColumnNumber](bind func(Set) integerColumnCall[T]) []integerColumnRunner[T] {
	var calls []integerColumnRunner[T]
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		calls = append(calls, integerColumnRunner[T]{fmt.Sprint(mode), bind(New(mode))})
	}
	return calls
}

// The oracle compares the original physical types. In particular uint64 never
// passes through signed integers or float64, which would lose ordering or bits.
func integerColumnMatch[T integerColumnNumber](a, b T, op CompareOp) bool {
	switch op {
	case CompareGT:
		return a > b
	case CompareGE:
		return a >= b
	case CompareLT:
		return a < b
	case CompareLE:
		return a <= b
	case CompareEQ:
		return a == b
	}
	panic("bad comparison")
}
func integerColumnValidity(n int, kind string) memory.Bitmap {
	if kind == "nil" {
		return nil
	}
	v := make(memory.Bitmap, (n+63)/64)
	for i := range v {
		switch kind {
		case "full":
			v[i] = ^uint64(0)
		case "mixed":
			v[i] = 0xa953c1ef72648b05
		case "even":
			v[i] = 0x5555555555555555
		case "odd":
			v[i] = 0xaaaaaaaaaaaaaaaa
		}
	}
	return v
}
func integerColumnCheck[T integerColumnNumber](t *testing.T, edges []T, calls []integerColumnRunner[T]) {
	t.Helper()
	lengths := []int{257, 1025, 4099}
	for n := 0; n <= 129; n++ {
		lengths = append(lengths, n)
	}
	rng := rand.New(rand.NewPCG(831, 427))
	for _, n := range lengths {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			// Different element offsets expose accidental aligned loads and shared indexes.
			a, b := make([]T, n+1)[1:], make([]T, n+3)[3:]
			for i := range a {
				if i < len(edges)*len(edges) {
					a[i], b[i] = edges[i/len(edges)], edges[i%len(edges)]
				} else {
					a[i], b[i] = T(rng.Uint64()), T(rng.Uint64())
					if i%3 == 0 {
						b[i] = a[i]
					}
				}
			}
			ac, bc := slices.Clone(a), slices.Clone(b)
			for _, ak := range []string{"nil", "full", "mixed", "even", "odd", "null"} {
				for _, bk := range []string{"nil", "full", "mixed", "even", "odd", "null"} {
					av, bv := integerColumnValidity(n, ak), integerColumnValidity(n, bk)
					avc, bvc := slices.Clone(av), slices.Clone(bv)
					for op := CompareGT; op <= CompareEQ; op++ {
						words := (n + 63) / 64
						want := make(memory.Bitmap, words+2)
						want[words], want[words+1] = 0xdeadbeef12345678, 0xabcdef0123456789
						count := 0
						for i := range a {
							if (av == nil || av[i/64]&(uint64(1)<<uint(i%64)) != 0) && (bv == nil || bv[i/64]&(uint64(1)<<uint(i%64)) != 0) && integerColumnMatch(a[i], b[i], op) {
								want[i/64] |= uint64(1) << uint(i%64)
								count++
							}
						}
						for _, runner := range calls {
							got := make(memory.Bitmap, len(want))
							for i := range got {
								got[i] = ^uint64(0)
							}
							copy(got[words:], want[words:])
							if c := runner.call(a, b, av, bv, got, op); c != count || !slices.Equal(got, want) {
								t.Fatalf("backend=%s op=%s valid=%s/%s count=%d want=%d bitmap=%x want=%x", runner.name, op, ak, bk, c, count, got, want)
							}
						}
					}
					if !slices.Equal(av, avc) || !slices.Equal(bv, bvc) {
						t.Fatal("validity mutated")
					}
				}
			}
			if !slices.Equal(a, ac) || !slices.Equal(b, bc) {
				t.Fatal("input mutated")
			}
		})
	}
	// One match at every row proves lane-to-bitmap ordering across several words.
	const n = 257
	for op := CompareGT; op <= CompareEQ; op++ {
		for row := 0; row < n; row++ {
			a, b := make([]T, n+1)[1:], make([]T, n+3)[3:]
			for i := range a {
				switch op {
				case CompareGT, CompareGE, CompareEQ:
					a[i], b[i] = 0, 1
				default:
					a[i], b[i] = 1, 0
				}
			}
			switch op {
			case CompareGT:
				a[row], b[row] = 1, 0
			case CompareLT:
				a[row], b[row] = 0, 1
			default:
				a[row], b[row] = 1, 1
			}
			for _, runner := range calls {
				out := make(memory.Bitmap, (n+63)/64+1)
				for i := range out {
					out[i] = ^uint64(0)
				}
				if c := runner.call(a, b, nil, nil, out, op); c != 1 {
					t.Fatalf("one-hot backend=%s op=%s row=%d count=%d", runner.name, op, row, c)
				}
				for wi, w := range out {
					want := uint64(0)
					if wi == row/64 {
						want = uint64(1) << uint(row%64)
					}
					if wi == len(out)-1 {
						want = ^uint64(0)
					}
					if w != want {
						t.Fatalf("one-hot backend=%s op=%s row=%d word=%d got=%x want=%x", runner.name, op, row, wi, w, want)
					}
				}
			}
		}
	}
	for _, runner := range calls {
		for op := CompareGT; op <= CompareEQ; op++ {
			for _, pair := range [][2]int{{0, 1}, {1, 0}, {64, 65}, {65, 64}} {
				func() {
					defer func() {
						if recover() == nil {
							t.Errorf("backend=%s op=%s lengths=%v did not panic", runner.name, op, pair)
						}
					}()
					runner.call(make([]T, pair[0]), make([]T, pair[1]), nil, nil, make(memory.Bitmap, 2), op)
				}()
			}
		}
	}
}
func TestIntegerColumnComparisons(t *testing.T) {
	t.Run("int8", func(t *testing.T) {
		integerColumnCheck(t, []int8{-128, -127, -1, 0, 1, 126, 127}, integerColumnRunners(func(s Set) integerColumnCall[int8] { return s.CompareInt8Columns }))
	})
	t.Run("int16", func(t *testing.T) {
		integerColumnCheck(t, []int16{-32768, -32767, -1, 0, 1, 32766, 32767}, integerColumnRunners(func(s Set) integerColumnCall[int16] { return s.CompareInt16Columns }))
	})
	t.Run("int64", func(t *testing.T) {
		integerColumnCheck(t, []int64{-1 << 63, -1<<63 + 1, -1<<53 - 1, -1, 0, 1, 1<<53 + 1, 1<<63 - 2, 1<<63 - 1}, integerColumnRunners(func(s Set) integerColumnCall[int64] { return s.CompareInt64Columns }))
	})
	t.Run("uint8", func(t *testing.T) {
		integerColumnCheck(t, []uint8{0, 1, 127, 128, 129, 254, 255}, integerColumnRunners(func(s Set) integerColumnCall[uint8] { return s.CompareUint8Columns }))
	})
	t.Run("uint16", func(t *testing.T) {
		integerColumnCheck(t, []uint16{0, 1, 32767, 32768, 32769, 65534, 65535}, integerColumnRunners(func(s Set) integerColumnCall[uint16] { return s.CompareUint16Columns }))
	})
	t.Run("uint64", func(t *testing.T) {
		integerColumnCheck(t, []uint64{0, 1, 1<<53 - 1, 1 << 53, 1<<53 + 1, 1<<63 - 1, 1 << 63, 1<<63 + 1, 1<<64 - 2, 1<<64 - 1}, integerColumnRunners(func(s Set) integerColumnCall[uint64] { return s.CompareUint64Columns }))
	})
}
