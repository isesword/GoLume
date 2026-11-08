package kernel

import (
	"fmt"
	"gocolumn/memory"
	"math/bits"
	"math/rand"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
)

var column32GateOps = []CompareOp{CompareGT, CompareGE, CompareLT, CompareLE, CompareEQ}
var column32GateModes = []struct {
	name string
	mode Mode
}{
	{"scalar", ModeScalar}, {"auto", ModeAuto}, {"simd", ModeSIMD},
}

type column32GateCompare[T ~int32 | ~uint32] func([]T, []T, memory.Bitmap, memory.Bitmap, memory.Bitmap, CompareOp) int

func column32GateValidity(n int, kind string) memory.Bitmap {
	if kind == "nil" {
		return nil
	}
	v := make(memory.Bitmap, memory.WordsFor(n))
	for i := range v {
		switch kind {
		case "all":
			v[i] = ^uint64(0) // Deliberately dirty validity tail.
		case "even":
			v[i] = 0x5555555555555555
		case "mixed":
			v[i] = bits.RotateLeft64(0xd15ea5e0c93b76a1, i%64)
		}
	}
	return v
}

func column32GateMatch[T ~int32 | ~uint32](a, b T, op CompareOp) bool {
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
	panic("invalid comparison")
}

// Check all bitmap tails and common SIMD lane boundaries, including inputs too
// short for one vector. Full cross products of edge values precede random data.
func TestColumn32ComparisonGate(t *testing.T) {
	t.Run("int32", func(t *testing.T) {
		column32CheckGate(t, []int32{-1 << 31, -1<<31 + 1, -65536, -1, 0, 1, 65536, 1<<31 - 2, 1<<31 - 1},
			func(s Set) column32GateCompare[int32] { return s.CompareInt32Columns })
	})
	t.Run("uint32", func(t *testing.T) {
		column32CheckGate(t, []uint32{0, 1, 65535, 65536, 1<<31 - 1, 1 << 31, 1<<31 + 1, 1<<32 - 2, 1<<32 - 1},
			func(s Set) column32GateCompare[uint32] { return s.CompareUint32Columns })
	})
}

func column32CheckGate[T ~int32 | ~uint32](t *testing.T, edges []T, bind func(Set) column32GateCompare[T]) {
	lengths := make([]int, 130)
	for i := range lengths {
		lengths[i] = i
	}
	lengths = append(lengths, 255, 256, 257, 1023, 1024, 1025, 4099)
	sets := []Set{New(ModeScalar), New(ModeAuto), New(ModeSIMD)}
	calls := []column32GateCompare[T]{bind(sets[0]), bind(sets[1]), bind(sets[2])}
	for _, n := range lengths {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(n) + 71))
			left, right := make([]T, n), make([]T, n)
			for i := range left {
				if i < len(edges)*len(edges) {
					left[i], right[i] = edges[i/len(edges)], edges[i%len(edges)]
				} else {
					left[i], right[i] = T(rng.Uint32()), T(rng.Uint32())
					if i%3 == 0 {
						right[i] = left[i]
					}
				}
			}
			leftCopy, rightCopy := slices.Clone(left), slices.Clone(right)
			words := memory.WordsFor(n)
			for _, ak := range []string{"nil", "all", "none", "even", "mixed"} {
				for _, bk := range []string{"nil", "all", "none", "even", "mixed"} {
					av, bv := column32GateValidity(n, ak), column32GateValidity(n, bk)
					avCopy, bvCopy := slices.Clone(av), slices.Clone(bv)
					for _, op := range column32GateOps {
						want := make(memory.Bitmap, words+2)
						want[words], want[words+1] = 0xfeedfacecafeabcd, 0x0123456789abcdef
						count := 0
						for i := range left {
							// Independent bit indexing rather than the kernel's validity helpers.
							aok := av == nil || av[i/64]&(uint64(1)<<uint(i%64)) != 0
							bok := bv == nil || bv[i/64]&(uint64(1)<<uint(i%64)) != 0
							if aok && bok && column32GateMatch(left[i], right[i], op) {
								want[i/64] |= uint64(1) << uint(i%64)
								count++
							}
						}
						for mi, call := range calls {
							got := make(memory.Bitmap, words+2)
							for i := range got {
								got[i] = ^uint64(0)
							}
							copy(got[words:], want[words:])
							actual := call(left, right, av, bv, got, op)
							if actual != count || !slices.Equal(got, want) {
								t.Fatalf("mode=%s op=%s validity=%s/%s count=%d want=%d bitmap=%x want=%x", column32GateModes[mi].name, op, ak, bk, actual, count, got, want)
							}
							// Reuse the dirty result for an all-null call: old matches must clear.
							nulls := make(memory.Bitmap, words)
							if actual = call(left, right, nulls, nulls, got, op); actual != 0 {
								t.Fatalf("all-null reuse count=%d", actual)
							}
							for wi := 0; wi < words; wi++ {
								if got[wi] != 0 {
									t.Fatalf("stale output at word %d", wi)
								}
							}
							if !slices.Equal(got[words:], want[words:]) {
								t.Fatal("oversized output suffix changed")
							}
						}
					}
					if !slices.Equal(av, avCopy) || !slices.Equal(bv, bvCopy) {
						t.Fatal("validity mutated")
					}
				}
			}
			if !slices.Equal(left, leftCopy) || !slices.Equal(right, rightCopy) {
				t.Fatal("input mutated")
			}
		})
	}
	// Allocation checks include vectors, bitmap tails and mixed validity.
	left, right := make([]T, 1025), make([]T, 1025)
	valid, out := column32GateValidity(1025, "mixed"), make(memory.Bitmap, memory.WordsFor(1025))
	for mi, call := range calls {
		for _, op := range column32GateOps {
			if allocs := testing.AllocsPerRun(100, func() { call(left, right, valid, valid, out, op) }); allocs != 0 {
				t.Fatalf("mode=%s op=%s allocations=%g", column32GateModes[mi].name, op, allocs)
			}
		}
	}
}

// Compare dispatch identities independently of capability reporting so a
// scalar fallback is never mislabeled as selected native code.
func column32GateBackend(s Set, unsigned bool, op CompareOp) string {
	scalar := New(ModeScalar)
	var selected, reference any
	if unsigned {
		selected, reference = s.d.compareUint32Columns[op], scalar.d.compareUint32Columns[op]
	} else {
		selected, reference = s.d.compareInt32Columns[op], scalar.d.compareInt32Columns[op]
	}
	if reflect.ValueOf(selected).Pointer() == reflect.ValueOf(reference).Pointer() {
		return "scalar"
	}
	if strings.Contains(runtime.FuncForPC(reflect.ValueOf(selected).Pointer()).Name(), ".simdAMD") {
		return "archsimd-amd64-mask"
	}
	return "portable-simd"
}

// An intentionally bounded matrix: streaming all-op cases, plus short/tail,
// sparse, dense, equal and nullable cases. Modes are benchstat /mode keys.
func BenchmarkColumn32ComparisonGate(b *testing.B) {
	b.Run("type=int32", func(b *testing.B) {
		column32BenchmarkGate(b, false, func(s Set) column32GateCompare[int32] { return s.CompareInt32Columns })
	})
	b.Run("type=uint32", func(b *testing.B) {
		column32BenchmarkGate(b, true, func(s Set) column32GateCompare[uint32] { return s.CompareUint32Columns })
	})
}

func column32BenchmarkGate[T ~int32 | ~uint32](b *testing.B, unsigned bool, bind func(Set) column32GateCompare[T]) {
	cases := []struct {
		name     string
		n        int
		density  string
		validity string
	}{
		{"stream", 1 << 20, "mixed", "nil"},
		{"bitmap", 1 << 16, "mixed", "all"},
		{"nullable-tail", (1 << 16) + 3, "mixed", "mixed"},
		{"sparse", 1 << 16, "sparse", "nil"},
		{"dense", 1 << 16, "dense", "nil"},
		{"equal", 1 << 16, "equal", "nil"},
		{"short", 7, "mixed", "nil"},
		{"tail", 65, "mixed", "mixed"},
	}
	for _, tc := range cases {
		b.Run("case="+tc.name, func(b *testing.B) {
			left, right := make([]T, tc.n), make([]T, tc.n)
			// Values span both sides of the unsigned sign bit. Relation distributions
			// are built explicitly so "sparse" means ~1% GT and EQ is independently dense.
			for i := range left {
				lo, hi := T(uint32(i)*2654435761), T(uint32(i)*2246822519+1)
				if lo > hi {
					lo, hi = hi, lo
				}
				if lo == hi {
					lo, hi = T(0), T(1)
				}
				switch tc.density {
				case "equal":
					left[i], right[i] = lo, lo
				case "sparse":
					left[i], right[i] = lo, hi
					if i%100 == 0 {
						left[i], right[i] = hi, lo
					}
				case "dense":
					left[i], right[i] = hi, lo
					if i%100 == 0 {
						left[i], right[i] = lo, hi
					}
				default:
					switch i % 3 {
					case 0:
						left[i], right[i] = lo, hi
					case 1:
						left[i], right[i] = hi, lo
					case 2:
						left[i], right[i] = lo, lo
					}
				}
			}
			av, bv := column32GateValidity(tc.n, tc.validity), column32GateValidity(tc.n, tc.validity)
			if tc.validity == "mixed" {
				bv = column32GateValidity(tc.n, "even")
			}
			out := make(memory.Bitmap, memory.WordsFor(tc.n))
			for _, op := range column32GateOps {
				for _, mode := range column32GateModes {
					set := New(mode.mode)
					name := fmt.Sprintf("op=%s/mode=%s/backend=%s", op, mode.name, column32GateBackend(set, unsigned, op))
					b.Run(name, func(b *testing.B) {
						call := bind(set)
						b.SetBytes(int64(tc.n) * 8)
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							call(left, right, av, bv, out, op)
						}
					})
				}
			}
		})
	}
}

// Auto and explicit SIMD select the same native column implementations.
// Scalar mode and unavailable native hardware retain the reference backend.
func column32CheckDispatch(t *testing.T, nativeBackend string) {
	t.Helper()
	for _, mode := range column32GateModes {
		set := New(mode.mode)
		want := "scalar"
		if mode.mode != ModeScalar {
			want = nativeBackend
		}
		for _, unsigned := range []bool{false, true} {
			for _, op := range column32GateOps {
				typ := memory.TypeInt32
				if unsigned {
					typ = memory.TypeUint32
				}
				capability, ok := set.ColumnComparisonCapability(typ, op)
				if !ok || capability.Backend != want || capability.HardwareSIMD != (want != "scalar") {
					t.Fatalf("mode=%s type=%s op=%s capability=%+v ok=%t want=%s", mode.name, typ, op, capability, ok, want)
				}
				if got := column32GateBackend(set, unsigned, op); got != want {
					t.Fatalf("mode=%s unsigned=%t op=%s backend=%s want=%s", mode.name, unsigned, op, got, want)
				}
			}
		}
		t.Logf("mode=%s column32_backend=%s", mode.name, want)
	}
}
