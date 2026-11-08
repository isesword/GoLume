package kernel

import (
	"gocolumn/internal/hashmix"
	"gocolumn/memory"
	"math"
	"math/rand/v2"
	"reflect"
	"testing"
)

func TestFloat32ComparisonScalarSemantics(t *testing.T) {
	values := []float32{float32(math.NaN()), -0, 1.5, 2.5, float32(math.Inf(1))}
	valid := memory.Bitmap{0b11101}
	for _, op := range []CompareOp{CompareGT, CompareGE, CompareLT, CompareLE, CompareEQ} {
		out := make(memory.Bitmap, memory.WordsFor(len(values)))
		got := New(ModeScalar).CompareFloat32(values, valid, out, 1.5, op)
		want := 0
		ref := make(memory.Bitmap, len(out))
		for i, v := range values {
			if !memory.IsSet(valid, i) {
				continue
			}
			match := false
			switch op {
			case CompareGT:
				match = v > 1.5
			case CompareGE:
				match = v >= 1.5
			case CompareLT:
				match = v < 1.5
			case CompareLE:
				match = v <= 1.5
			case CompareEQ:
				match = v == 1.5
			}
			if match {
				memory.Set(ref, i)
				want++
			}
		}
		if got != want || !reflect.DeepEqual(out, ref) {
			t.Fatalf("op %s got (%d,%v), want (%d,%v)", op, got, out, want, ref)
		}
	}
}

func TestNativeWidthCompactionPreservesSelectionOrder(t *testing.T) {
	sel := memory.Bitmap{0b101101}
	s := New(ModeScalar)
	if got := s.CompactInt8([]int8{-3, -2, -1, 0, 1, 2}, sel, make([]int8, 4)); got != 4 {
		t.Fatalf("int8 count=%d", got)
	}
	i16 := make([]int16, 4)
	if got := s.CompactInt16([]int16{1, 2, 3, 4, 5, 6}, sel, i16); got != 4 || !reflect.DeepEqual(i16, []int16{1, 3, 4, 6}) {
		t.Fatalf("int16=(%d,%v)", got, i16)
	}
	u8 := make([]uint8, 4)
	if got := s.CompactUint8([]uint8{1, 2, 3, 4, 5, 6}, sel, u8); got != 4 || !reflect.DeepEqual(u8, []uint8{1, 3, 4, 6}) {
		t.Fatalf("uint8=(%d,%v)", got, u8)
	}
	u16 := make([]uint16, 4)
	if got := s.CompactUint16([]uint16{1, 2, 3, 4, 5, 6}, sel, u16); got != 4 || !reflect.DeepEqual(u16, []uint16{1, 3, 4, 6}) {
		t.Fatalf("uint16=(%d,%v)", got, u16)
	}
	f32 := make([]float32, 4)
	if got := s.CompactFloat32([]float32{1, 2, 3, 4, 5, 6}, sel, f32); got != 4 || !reflect.DeepEqual(f32, []float32{1, 3, 4, 6}) {
		t.Fatalf("float32=(%d,%v)", got, f32)
	}
}

func TestKernelCapabilitiesAreExactAndAutoPrefersNative(t *testing.T) {
	auto := New(ModeAuto)
	if !auto.Valid() || auto.Mode() != ModeAuto || auto.Arch() == "" {
		t.Fatalf("invalid auto set: valid=%t mode=%s arch=%q", auto.Valid(), auto.Mode(), auto.Arch())
	}
	for _, id := range KernelIDs() {
		capability, ok := auto.Capability(id)
		if !ok {
			t.Fatalf("auto capability missing for %s", id)
		}
		if capability.Backend == "scalar" {
			if capability.HardwareSIMD || capability.Emulated || capability.VectorBits != 0 {
				t.Fatalf("inconsistent scalar capability %s=%+v", id, capability)
			}
			continue
		}
		if capability.HardwareSIMD {
			if capability.Emulated || capability.VectorBits <= 0 {
				t.Fatalf("inconsistent native capability %s=%+v", id, capability)
			}
		} else if auto.Arch() != "arm64" || id != KernelHash64Batch || capability.Backend != "arm64-unrolled" {
			t.Fatalf("unknown non-vector backend %s=%+v", id, capability)
		}
	}
	if _, ok := auto.Capability(KernelID{}); ok {
		t.Fatal("invalid KernelID must not fall back to scalar capability")
	}
	if _, ok := ArithmeticID(memory.TypeInt64, ArithmeticDiv); ok {
		t.Fatal("unsupported integer division must not have a KernelID")
	}
	dateID, _ := ComparisonID(memory.TypeDate32, CompareGT)
	int32ID, _ := ComparisonID(memory.TypeInt32, CompareGT)
	timestampID, _ := ArithmeticID(memory.TypeTimestamp, ArithmeticAdd)
	int64ID, _ := ArithmeticID(memory.TypeInt64, ArithmeticAdd)
	if dateID != int32ID || timestampID != int64ID {
		t.Fatalf("physical aliases diverged: date=%s int32=%s timestamp=%s int64=%s", dateID, int32ID, timestampID, int64ID)
	}
	forced := New(ModeSIMD)
	intMul, _ := ArithmeticID(memory.TypeInt32, ArithmeticMul)
	for _, id := range []KernelID{intMul, KernelStringByteLength} {
		capability, ok := forced.Capability(id)
		if !ok || capability.Backend != "scalar" {
			t.Fatalf("forced scalar fallback %s=%+v ok=%t", id, capability, ok)
		}
	}
}

func TestNativeCompactKernelIDsHaveScalarCapabilities(t *testing.T) {
	s := New(ModeScalar)
	for _, id := range []KernelID{KernelCompactInt8, KernelCompactInt16, KernelCompactInt32, KernelCompactInt64, KernelCompactUint8, KernelCompactUint16, KernelCompactUint32, KernelCompactUint64, KernelCompactFloat32, KernelCompactFloat64} {
		capability, ok := s.Capability(id)
		if !ok || capability.Backend != "scalar" || capability.HardwareSIMD || capability.VectorBits != 0 {
			t.Fatalf("compact capability %s=%+v ok=%t", id, capability, ok)
		}
	}
}

func TestKernelIDsHaveNoDuplicatePhysicalEntries(t *testing.T) {
	seen := make(map[KernelID]struct{})
	for _, id := range KernelIDs() {
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate kernel id %s", id)
		}
		seen[id] = struct{}{}
	}
}

func TestStringMatchKernelIDsHaveScalarCapabilities(t *testing.T) {
	set := New(ModeScalar)
	for _, id := range []KernelID{KernelStringEqual, KernelStringPrefix, KernelStringSuffix, KernelStringContains} {
		capability, ok := set.Capability(id)
		if !ok || capability.Backend != "scalar" {
			t.Fatalf("id=%s capability=%+v ok=%t", id, capability, ok)
		}
	}
}

func TestCompareInt64ModesLengthsOpsAndNulls(t *testing.T) {
	lengths := []int{0, 1, 2, 3, 15, 16, 17, 31, 32, 33, 63, 64, 65, 131}
	ops := []CompareOp{CompareGT, CompareGE, CompareLT, CompareLE, CompareEQ}
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		set := New(mode)
		for _, rows := range lengths {
			values := make([]int64, rows)
			validity := make(memory.Bitmap, memory.WordsFor(rows))
			for row := range values {
				values[row] = int64(row%11 - 5)
				if row%5 != 1 {
					memory.Set(validity, row)
				}
			}
			for _, op := range ops {
				want := make(memory.Bitmap, memory.WordsFor(rows))
				got := make(memory.Bitmap, memory.WordsFor(rows))
				wn := New(ModeScalar).CompareInt64(values, validity, want, 1, op)
				gn := set.CompareInt64(values, validity, got, 1, op)
				if wn != gn || !equalBitmap(want, got) {
					t.Fatalf("mode=%d rows=%d op=%d got=(%d,%x) want=(%d,%x) caps=%+v", mode, rows, op, gn, got, wn, want, set.Capabilities())
				}
			}
		}
	}
}

func TestAllTypedComparisonsMatchScalar(t *testing.T) {
	for _, rows := range []int{0, 1, 2, 3, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 131} {
		validity := make(memory.Bitmap, memory.WordsFor(rows))
		i32, u32 := make([]int32, rows), make([]uint32, rows)
		i64, u64 := make([]int64, rows), make([]uint64, rows)
		f64 := make([]float64, rows)
		for row := 0; row < rows; row++ {
			if row%7 != 0 {
				memory.Set(validity, row)
			}
			i32[row], i64[row] = int32(row-65), int64(row-65)
			u32[row], u64[row] = uint32(row), uint64(row)
			f64[row] = float64(row-65) / 3
		}
		if rows > 5 {
			f64[2], f64[3], f64[4], f64[5] = math.NaN(), math.Inf(1), math.Inf(-1), math.Copysign(0, -1)
		}
		for _, mode := range []Mode{ModeAuto, ModeSIMD} {
			set := New(mode)
			for _, op := range []CompareOp{CompareGT, CompareGE, CompareLT, CompareLE, CompareEQ} {
				scalar := New(ModeScalar)
				assertCompare(t, rows, func(out memory.Bitmap) int { return scalar.CompareInt32(i32, validity, out, 3, op) }, func(out memory.Bitmap) int { return set.CompareInt32(i32, validity, out, 3, op) })
				assertCompare(t, rows, func(out memory.Bitmap) int { return scalar.CompareInt64(i64, validity, out, 3, op) }, func(out memory.Bitmap) int { return set.CompareInt64(i64, validity, out, 3, op) })
				assertCompare(t, rows, func(out memory.Bitmap) int { return scalar.CompareUint32(u32, validity, out, 63, op) }, func(out memory.Bitmap) int { return set.CompareUint32(u32, validity, out, 63, op) })
				assertCompare(t, rows, func(out memory.Bitmap) int { return scalar.CompareUint64(u64, validity, out, 63, op) }, func(out memory.Bitmap) int { return set.CompareUint64(u64, validity, out, 63, op) })
				assertCompare(t, rows, func(out memory.Bitmap) int { return scalar.CompareFloat64(f64, validity, out, 1, op) }, func(out memory.Bitmap) int { return set.CompareFloat64(f64, validity, out, 1, op) })
			}
		}
	}
}

func TestTypedComparisonValidityExtremesAndTailPollution(t *testing.T) {
	const rows = 65
	i32, i64 := make([]int32, rows), make([]int64, rows)
	u32, u64 := make([]uint32, rows), make([]uint64, rows)
	f64 := make([]float64, rows)
	for row := 0; row < rows; row++ {
		i32[row], i64[row], u32[row], u64[row], f64[row] = int32(row), int64(row), uint32(row), uint64(row), float64(row)
	}
	allNull := memory.EnsureBitmap(nil, rows)
	for _, validity := range []memory.Bitmap{nil, allNull} {
		set := New(ModeSIMD)
		for _, test := range []struct {
			name string
			got  func(memory.Bitmap) int
			want func(memory.Bitmap) int
		}{
			{"int32", func(out memory.Bitmap) int { return set.CompareInt32(i32, validity, out, 32, CompareGE) }, func(out memory.Bitmap) int { return New(ModeScalar).CompareInt32(i32, validity, out, 32, CompareGE) }},
			{"int64", func(out memory.Bitmap) int { return set.CompareInt64(i64, validity, out, 32, CompareGE) }, func(out memory.Bitmap) int { return New(ModeScalar).CompareInt64(i64, validity, out, 32, CompareGE) }},
			{"uint32", func(out memory.Bitmap) int { return set.CompareUint32(u32, validity, out, 32, CompareGE) }, func(out memory.Bitmap) int { return New(ModeScalar).CompareUint32(u32, validity, out, 32, CompareGE) }},
			{"uint64", func(out memory.Bitmap) int { return set.CompareUint64(u64, validity, out, 32, CompareGE) }, func(out memory.Bitmap) int { return New(ModeScalar).CompareUint64(u64, validity, out, 32, CompareGE) }},
			{"float64", func(out memory.Bitmap) int { return set.CompareFloat64(f64, validity, out, 32, CompareGE) }, func(out memory.Bitmap) int { return New(ModeScalar).CompareFloat64(f64, validity, out, 32, CompareGE) }},
		} {
			got, want := memory.Bitmap{^uint64(0), ^uint64(0)}, memory.Bitmap{0, 0}
			gn, wn := test.got(got), test.want(want)
			if gn != wn || !equalBitmap(got, want) || got[1]&^uint64(1) != 0 {
				t.Fatalf("%s validity_nil=%t got=(%d,%x) want=(%d,%x)", test.name, validity == nil, gn, got, wn, want)
			}
		}
	}
}

func TestFloatArithmeticBoolAndHashKernels(t *testing.T) {
	set := New(ModeSIMD)
	left := []float64{1, 2, 3, math.NaN(), math.Inf(1)}
	right := []float64{4, 5, 6, 7, 2}
	validity := memory.EnsureBitmap(nil, len(left))
	for _, row := range []int{0, 2, 3, 4} {
		memory.Set(validity, row)
	}
	for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul, ArithmeticDiv} {
		want, got := make([]float64, len(left)), make([]float64, len(left))
		New(ModeScalar).Float64Arithmetic(left, right, want, validity, op)
		set.Float64Arithmetic(left, right, got, validity, op)
		for i := range want {
			if math.Float64bits(want[i]) != math.Float64bits(got[i]) && !(math.IsNaN(want[i]) && math.IsNaN(got[i])) {
				t.Fatalf("op=%d row=%d got=%v want=%v", op, i, got[i], want[i])
			}
		}
	}

	aTruth, aValid := memory.EnsureBitmap(nil, 70), memory.EnsureBitmap(nil, 70)
	bTruth, bValid := memory.EnsureBitmap(nil, 70), memory.EnsureBitmap(nil, 70)
	for row := 0; row < 70; row++ {
		if row%2 == 0 {
			memory.Set(aTruth, row)
		}
		if row%3 != 0 {
			memory.Set(aValid, row)
		}
		if row%5 == 0 {
			memory.Set(bTruth, row)
		}
		if row%7 != 0 {
			memory.Set(bValid, row)
		}
	}
	for _, test := range []struct {
		name   string
		fn     boolBinaryFunc
		scalar boolBinaryFunc
	}{{"and", set.BoolAnd, scalarBoolAnd}, {"or", set.BoolOr, scalarBoolOr}} {
		wantT, wantV := memory.EnsureBitmap(nil, 70), memory.EnsureBitmap(nil, 70)
		gotT, gotV := memory.EnsureBitmap(nil, 70), memory.EnsureBitmap(nil, 70)
		test.scalar(aTruth, aValid, bTruth, bValid, wantT, wantV, 70)
		test.fn(aTruth, aValid, bTruth, bValid, gotT, gotV, 70)
		if !equalBitmap(wantT, gotT) || !equalBitmap(wantV, gotV) {
			t.Fatalf("%s mismatch", test.name)
		}
	}

	keys, hashes := make([]uint64, 257), make([]uint64, 257)
	for i := range keys {
		keys[i] = rand.Uint64()
	}
	set.Hash64Batch(keys, hashes)
	for i, key := range keys {
		if hashes[i] != scalarHash64(key) {
			t.Fatalf("hash row=%d", i)
		}
	}
}

func TestHashGoldenAndNullablePairNormalization(t *testing.T) {
	keys := []uint64{0, 1, math.MaxUint64, 0x0123456789abcdef}
	want64 := []uint64{0xe220a8397b1dcdaf, 0x910a2dec89025cc1, 0xe4d971771b652c20, 0x157a3807a48faa9d}
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		got := make([]uint64, len(keys))
		New(mode).Hash64Batch(keys, got)
		for i := range got {
			if got[i] != want64[i] {
				t.Fatalf("mode=%s hash64 row=%d got=%#x want=%#x", mode, i, got[i], want64[i])
			}
		}
	}
	a := []uint64{0, 1, math.MaxUint64, 123}
	b := []uint64{0, 2, math.MaxUint64, 456}
	valid := []uint8{0, 3, 1, 2}
	want128 := []uint64{0xe220a8397b1dcdaf, 0x12f4eeb73ced4b8e, 0xb4d055fcf2cbbd7b, 0x7cf3f6be18585d7e}
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		got := make([]uint64, len(a))
		New(mode).Hash128Batch(a, b, valid, got)
		for i := range got {
			if got[i] != want128[i] || got[i] != hashmix.Pair(a[i], b[i], valid[i]) {
				t.Fatalf("mode=%s hash128 row=%d got=%#x want=%#x", mode, i, got[i], want128[i])
			}
		}
	}
	randomA, randomB, randomValid := make([]uint64, 257), make([]uint64, 257), make([]uint8, 257)
	for i := range randomA {
		randomA[i], randomB[i], randomValid[i] = rand.Uint64(), rand.Uint64(), uint8(rand.Uint32())&3
	}
	wantRandom, gotRandom := make([]uint64, len(randomA)), make([]uint64, len(randomA))
	New(ModeScalar).Hash128Batch(randomA, randomB, randomValid, wantRandom)
	New(ModeSIMD).Hash128Batch(randomA, randomB, randomValid, gotRandom)
	for i := range wantRandom {
		if gotRandom[i] != wantRandom[i] {
			t.Fatalf("random hash128 row=%d got=%#x want=%#x", i, gotRandom[i], wantRandom[i])
		}
	}
}

func TestBooleanThreeValueTruthTablesAndTailMask(t *testing.T) {
	type value struct{ truth, valid bool }
	values := []value{{false, false}, {false, true}, {true, true}}
	set := New(ModeSIMD)
	for _, a := range values {
		for _, b := range values {
			at, av, bt, bv := make(memory.Bitmap, 1), make(memory.Bitmap, 1), make(memory.Bitmap, 1), make(memory.Bitmap, 1)
			if a.truth {
				at[0] = 1
			}
			if a.valid {
				av[0] = 1
			}
			if b.truth {
				bt[0] = 1
			}
			if b.valid {
				bv[0] = 1
			}
			for _, test := range []struct {
				name      string
				got, want boolBinaryFunc
			}{{"and", set.BoolAnd, scalarBoolAnd}, {"or", set.BoolOr, scalarBoolOr}} {
				gotT, gotV, wantT, wantV := memory.Bitmap{^uint64(0)}, memory.Bitmap{^uint64(0)}, memory.Bitmap{0}, memory.Bitmap{0}
				test.got(at, av, bt, bv, gotT, gotV, 1)
				test.want(at, av, bt, bv, wantT, wantV, 1)
				if gotT[0] != wantT[0] || gotV[0] != wantV[0] || gotT[0]&^uint64(1) != 0 || gotV[0]&^uint64(1) != 0 {
					t.Fatalf("%s a=%+v b=%+v got=(%b,%b) want=(%b,%b)", test.name, a, b, gotT[0], gotV[0], wantT[0], wantV[0])
				}
			}
		}
		truth, validity := memory.Bitmap{0}, memory.Bitmap{0}
		if a.truth {
			truth[0] = 1
		}
		if a.valid {
			validity[0] = 1
		}
		gotT, gotV, wantT, wantV := memory.Bitmap{^uint64(0)}, memory.Bitmap{^uint64(0)}, memory.Bitmap{0}, memory.Bitmap{0}
		set.BoolNot(truth, validity, gotT, gotV, 1)
		scalarBoolNot(truth, validity, wantT, wantV, 1)
		if gotT[0] != wantT[0] || gotV[0] != wantV[0] {
			t.Fatalf("not a=%+v got=(%b,%b) want=(%b,%b)", a, gotT[0], gotV[0], wantT[0], wantV[0])
		}
	}
}

func TestCheckedIntegerArithmeticMatchesScalarAndReportsFirstOverflow(t *testing.T) {
	for _, rows := range []int{0, 1, 3, 15, 16, 17, 63, 64, 65, 131} {
		validity := memory.EnsureBitmap(nil, rows)
		i32a, i32b := make([]int32, rows), make([]int32, rows)
		u32a, u32b := make([]uint32, rows), make([]uint32, rows)
		i64a, i64b := make([]int64, rows), make([]int64, rows)
		u64a, u64b := make([]uint64, rows), make([]uint64, rows)
		for row := 0; row < rows; row++ {
			if row%5 != 1 {
				memory.Set(validity, row)
			}
			i32a[row], i32b[row] = int32(row*19), int32(row%7-3)
			u32a[row], u32b[row] = uint32(row*19), uint32(row%7)
			i64a[row], i64b[row] = int64(row*19), int64(row%7-3)
			u64a[row], u64b[row] = uint64(row*19), uint64(row%7)
			if row%13 == 0 {
				i32a[row], i32b[row] = math.MaxInt32, 2
				u32a[row], u32b[row] = math.MaxUint32, 2
				i64a[row], i64b[row] = math.MaxInt64, 2
				u64a[row], u64b[row] = math.MaxUint64, 2
			}
		}
		set := New(ModeSIMD)
		for _, op := range []ArithmeticOp{ArithmeticAdd, ArithmeticSub, ArithmeticMul} {
			assertIntegerArithmetic(t, rows, func(out []int32, overflow memory.Bitmap) int {
				return New(ModeScalar).Int32Arithmetic(i32a, i32b, out, validity, overflow, op)
			}, func(out []int32, overflow memory.Bitmap) int {
				return set.Int32Arithmetic(i32a, i32b, out, validity, overflow, op)
			})
			assertIntegerArithmetic(t, rows, func(out []uint32, overflow memory.Bitmap) int {
				return New(ModeScalar).Uint32Arithmetic(u32a, u32b, out, validity, overflow, op)
			}, func(out []uint32, overflow memory.Bitmap) int {
				return set.Uint32Arithmetic(u32a, u32b, out, validity, overflow, op)
			})
			assertIntegerArithmetic(t, rows, func(out []int64, overflow memory.Bitmap) int {
				return New(ModeScalar).Int64Arithmetic(i64a, i64b, out, validity, overflow, op)
			}, func(out []int64, overflow memory.Bitmap) int {
				return set.Int64Arithmetic(i64a, i64b, out, validity, overflow, op)
			})
			assertIntegerArithmetic(t, rows, func(out []uint64, overflow memory.Bitmap) int {
				return New(ModeScalar).Uint64Arithmetic(u64a, u64b, out, validity, overflow, op)
			}, func(out []uint64, overflow memory.Bitmap) int {
				return set.Uint64Arithmetic(u64a, u64b, out, validity, overflow, op)
			})
		}
	}
}

func TestCheckedIntegerArithmeticReportsEveryForcedSIMDLane(t *testing.T) {
	const rows = 17
	set := New(ModeSIMD)
	for overflowRow := 0; overflowRow < 16; overflowRow++ {
		i32a, i32b, i32out := make([]int32, rows), make([]int32, rows), make([]int32, rows)
		u32a, u32b, u32out := make([]uint32, rows), make([]uint32, rows), make([]uint32, rows)
		i64a, i64b, i64out := make([]int64, rows), make([]int64, rows), make([]int64, rows)
		u64a, u64b, u64out := make([]uint64, rows), make([]uint64, rows), make([]uint64, rows)
		for row := 0; row < rows; row++ {
			i32a[row], i32b[row], u32a[row], u32b[row] = int32(row), 1, uint32(row), 1
			i64a[row], i64b[row], u64a[row], u64b[row] = int64(row), 1, uint64(row), 1
		}
		i32a[overflowRow], u32a[overflowRow] = math.MaxInt32, math.MaxUint32
		i64a[overflowRow], u64a[overflowRow] = math.MaxInt64, math.MaxUint64
		for name, got := range map[string]int{
			"int32":  set.Int32Arithmetic(i32a, i32b, i32out, nil, nil, ArithmeticAdd),
			"uint32": set.Uint32Arithmetic(u32a, u32b, u32out, nil, nil, ArithmeticAdd),
			"int64":  set.Int64Arithmetic(i64a, i64b, i64out, nil, nil, ArithmeticAdd),
			"uint64": set.Uint64Arithmetic(u64a, u64b, u64out, nil, nil, ArithmeticAdd),
		} {
			if got != overflowRow {
				t.Fatalf("%s lane=%d first overflow=%d", name, overflowRow, got)
			}
		}
	}
}

func TestFixedWidthCompactionMatchesSelectedRows(t *testing.T) {
	for _, rows := range []int{0, 1, 7, 8, 15, 16, 17, 63, 64, 65, 131} {
		selection := memory.EnsureBitmap(nil, rows)
		values := make([]int64, rows)
		want := make([]int64, 0, rows)
		for row := range values {
			values[row] = int64(row*17 - 9)
			if row%5 == 0 || row%7 == 1 {
				memory.Set(selection, row)
				want = append(want, values[row])
			}
		}
		for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
			got := make([]int64, len(want))
			n := New(mode).CompactInt64(values, selection, got)
			if n != len(want) {
				t.Fatalf("mode=%s rows=%d selected=%d want=%d", mode, rows, n, len(want))
			}
			for row := range want {
				if got[row] != want[row] {
					t.Fatalf("mode=%s rows=%d output=%d got=%d want=%d", mode, rows, row, got[row], want[row])
				}
			}
		}
	}
}

func TestAllFixedWidthCompactionTypesAndDensities(t *testing.T) {
	const rows = 131
	for _, selection := range []memory.Bitmap{nil, memory.EnsureBitmap(nil, rows)} {
		for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
			set := New(mode)
			assertCompactEqual(t, rows, selection, set.CompactInt32, scalarInt32Compact)
			assertCompactEqual(t, rows, selection, set.CompactInt64, scalarInt64Compact)
			assertCompactEqual(t, rows, selection, set.CompactUint32, scalarUint32Compact)
			assertCompactEqual(t, rows, selection, set.CompactUint64, scalarUint64Compact)
			assertCompactEqual(t, rows, selection, set.CompactFloat64, scalarFloat64Compact)
		}
	}
	for _, every := range []int{1, 2, 10, 100} {
		selection := memory.EnsureBitmap(nil, rows)
		for row := 0; row < rows; row++ {
			if row%every == 0 {
				memory.Set(selection, row)
			}
		}
		for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
			set := New(mode)
			assertCompactEqual(t, rows, selection, set.CompactInt32, scalarInt32Compact)
			assertCompactEqual(t, rows, selection, set.CompactInt64, scalarInt64Compact)
			assertCompactEqual(t, rows, selection, set.CompactUint32, scalarUint32Compact)
			assertCompactEqual(t, rows, selection, set.CompactUint64, scalarUint64Compact)
			assertCompactEqual(t, rows, selection, set.CompactFloat64, scalarFloat64Compact)
		}
	}
}

func assertCompactEqual[T ~int32 | ~int64 | ~uint32 | ~uint64 | ~float64](t *testing.T, rows int, selection memory.Bitmap, gotFn, wantFn func([]T, memory.Bitmap, []T) int) {
	t.Helper()
	values, got, want := make([]T, rows), make([]T, rows), make([]T, rows)
	for i := range values {
		values[i] = T(i*17 + 3)
	}
	gn, wn := gotFn(values, selection, got), wantFn(values, selection, want)
	if gn != wn {
		t.Fatalf("compact count=%d want=%d", gn, wn)
	}
	for i := 0; i < wn; i++ {
		if got[i] != want[i] {
			t.Fatalf("compact row=%d got=%v want=%v", i, got[i], want[i])
		}
	}
}

func TestNonAlignedKernelTailsAllocateZero(t *testing.T) {
	const rows = 131
	set := New(ModeSIMD)
	validity, selection, overflow := memory.EnsureBitmap(nil, rows), memory.EnsureBitmap(nil, rows), memory.EnsureBitmap(nil, rows)
	a, b, out := make([]int64, rows), make([]int64, rows), make([]int64, rows)
	compactOut := make([]int64, rows)
	for row := 0; row < rows; row++ {
		a[row], b[row] = int64(row+1), 2
		if row%3 != 0 {
			memory.Set(validity, row)
		}
		if row%5 == 0 {
			memory.Set(selection, row)
		}
	}
	if got := testing.AllocsPerRun(100, func() { set.Int64Arithmetic(a, b, out, validity, overflow, ArithmeticAdd) }); got != 0 {
		t.Fatalf("arithmetic allocations=%v", got)
	}
	if got := testing.AllocsPerRun(100, func() { copy(out, a); set.ScaleInt64(out, validity, 3) }); got != 0 {
		t.Fatalf("scale allocations=%v", got)
	}
	if got := testing.AllocsPerRun(100, func() { set.CompactInt64(a, selection, compactOut) }); got != 0 {
		t.Fatalf("compact allocations=%v", got)
	}
}

func TestInt64ScaleReportsExactOverflowLane(t *testing.T) {
	for _, rows := range []int{1, 2, 3, 7, 8, 9, 15, 16, 17, 65} {
		for overflowRow := 0; overflowRow < min(rows, 16); overflowRow++ {
			values := make([]int64, rows)
			for row := range values {
				values[row] = int64(row + 1)
			}
			values[overflowRow] = math.MaxInt64
			want, got := append([]int64(nil), values...), append([]int64(nil), values...)
			wantRow := New(ModeScalar).ScaleInt64(want, nil, 2)
			gotRow := New(ModeSIMD).ScaleInt64(got, nil, 2)
			if gotRow != overflowRow || wantRow != overflowRow {
				t.Fatalf("rows=%d lane=%d got row=%d scalar=%d", rows, overflowRow, gotRow, wantRow)
			}
			for row := range want {
				if got[row] != want[row] {
					t.Fatalf("rows=%d lane=%d row=%d got=%d want=%d", rows, overflowRow, row, got[row], want[row])
				}
			}
		}
	}
}

func TestInt64ScaleFactorAndBoundaryMatrix(t *testing.T) {
	tests := []struct {
		name     string
		values   []int64
		validity memory.Bitmap
		factor   int64
		want     []int64
		row      int
	}{
		{"zero", []int64{7, -9}, nil, 0, []int64{0, 0}, -1},
		{"one", []int64{7, -9}, nil, 1, []int64{7, -9}, -1},
		{"minus-one-overflow", []int64{7, math.MinInt64, 9}, nil, -1, []int64{-7, math.MinInt64, 9}, 1},
		{"positive-bound", []int64{math.MaxInt64 / 1000, math.MaxInt64/1000 + 1, 5}, nil, 1000, []int64{(math.MaxInt64 / 1000) * 1000, math.MaxInt64/1000 + 1, 5}, 1},
		{"negative-bound", []int64{math.MaxInt64 / -1000, math.MaxInt64/-1000 - 1, 5}, nil, -1000, []int64{(math.MaxInt64 / -1000) * -1000, math.MaxInt64/-1000 - 1, 5}, 1},
		{"min-factor", []int64{0, 1, -1}, nil, math.MinInt64, []int64{0, math.MinInt64, -1}, 2},
		{"max-factor", []int64{1, 2}, nil, math.MaxInt64, []int64{math.MaxInt64, 2}, 1},
		{"nullable", []int64{3, math.MinInt64, 5}, memory.Bitmap{0b101}, -1, []int64{-3, math.MinInt64, -5}, -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
				got := append([]int64(nil), test.values...)
				if row := New(mode).ScaleInt64(got, test.validity, test.factor); row != test.row {
					t.Fatalf("mode=%s row=%d want=%d", mode, row, test.row)
				}
				if len(got) != len(test.want) {
					t.Fatalf("mode=%s length=%d want=%d", mode, len(got), len(test.want))
				}
				for row := range got {
					if got[row] != test.want[row] {
						t.Fatalf("mode=%s row=%d got=%d want=%d", mode, row, got[row], test.want[row])
					}
				}
			}
		})
	}
}

func TestHashBatchSupportsExactInPlaceAliasing(t *testing.T) {
	a := []uint64{0, 1, math.MaxUint64, 7, 11, 13, 17, 19, 23}
	b := []uint64{29, 31, 37, 41, 43, 47, 53, 59, 61}
	valid := []uint8{0, 1, 2, 3, 3, 2, 1, 0, 3}
	want64 := make([]uint64, len(a))
	want128 := make([]uint64, len(a))
	New(ModeScalar).Hash64Batch(a, want64)
	New(ModeScalar).Hash128Batch(a, b, valid, want128)
	for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
		inPlace64 := append([]uint64(nil), a...)
		New(mode).Hash64Batch(inPlace64, inPlace64)
		left := append([]uint64(nil), a...)
		New(mode).Hash128Batch(left, b, valid, left)
		right := append([]uint64(nil), b...)
		New(mode).Hash128Batch(a, right, valid, right)
		for row := range a {
			if inPlace64[row] != want64[row] || left[row] != want128[row] || right[row] != want128[row] {
				t.Fatalf("mode=%s row=%d hash64=%x left=%x right=%x want64=%x want128=%x", mode, row, inPlace64[row], left[row], right[row], want64[row], want128[row])
			}
		}
	}
}

func TestUTF8LengthASCIIUnicodeAndMalformedInputs(t *testing.T) {
	values := [][]byte{
		{}, []byte("short ascii"), []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789--long-ascii"),
		[]byte("Go语言🙂"), append([]byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789--"), []byte("语言")...),
		{0x80}, {0xc2}, {0xe0, 0x80, 0x80}, {0xed, 0xa0, 0x80}, {0xf4, 0x90, 0x80, 0x80},
	}
	offsets := make([]uint32, len(values)+1)
	var data []byte
	for row, value := range values {
		data = append(data, value...)
		offsets[row+1] = uint32(len(data))
	}
	for _, op := range []CompareOp{CompareGT, CompareGE, CompareLT, CompareLE, CompareEQ} {
		for literal := int64(0); literal <= 70; literal += 5 {
			want, got := memory.EnsureBitmap(nil, len(values)), memory.EnsureBitmap(nil, len(values))
			wn := New(ModeScalar).CompareStringLength(offsets, data, nil, want, literal, op, true)
			gn := New(ModeSIMD).CompareStringLength(offsets, data, nil, got, literal, op, true)
			if wn != gn || !equalBitmap(want, got) {
				t.Fatalf("op=%s literal=%d got=(%d,%x) want=(%d,%x)", op, literal, gn, got, wn, want)
			}
		}
	}
}

func TestStringMatchKernelPreservesValidityAndTail(t *testing.T) {
	offsets := []uint32{0, 5, 9, 14, 14}
	data := []byte("alphabetaomega")
	valid := memory.Bitmap{0b1011}
	wants := map[StringMatchOp]uint64{StringEqual: 0, StringPrefix: 1, StringSuffix: 3, StringContains: 3}
	for _, op := range []StringMatchOp{StringEqual, StringPrefix, StringSuffix, StringContains} {
		got := make(memory.Bitmap, 1)
		selected := New(ModeScalar).CompareStringMatch(offsets, data, valid, got, []byte("a"), op)
		if selected != memory.CountBitmap(got) || got[0] != wants[op] {
			t.Fatalf("op=%s selected=%d bitmap=%x", op, selected, got)
		}
	}
}

func TestStringMatchKernelSharedBufferAndNonZeroOffsets(t *testing.T) {
	// The offsets address a shared data buffer with a non-zero logical start.
	data := []byte("prefix|alpha|beta|🙂|suffix")
	offsets := []uint32{7, 12, 17, 22, 22}
	valid := memory.Bitmap{0b1111}
	got := make(memory.Bitmap, 1)
	selected := New(ModeScalar).CompareStringMatch(offsets, data, valid, got, []byte("a"), StringContains)
	if selected != 2 || got[0]&3 != 3 || got[0]&4 != 0 {
		t.Fatalf("selected=%d bitmap=%x", selected, got)
	}
}

func TestStringLengthComparisonMatchesScalar(t *testing.T) {
	values := [][]byte{[]byte(""), []byte("ascii"), []byte("Go语言"), []byte{0xff, 'x'}, []byte("🙂🙂🙂"), []byte("tail")}
	offsets := make([]uint32, len(values)+1)
	var data []byte
	validity := memory.EnsureBitmap(nil, len(values))
	for row, value := range values {
		data = append(data, value...)
		offsets[row+1] = uint32(len(data))
		if row != 4 {
			memory.Set(validity, row)
		}
	}
	for _, utf8Length := range []bool{false, true} {
		for _, op := range []CompareOp{CompareGT, CompareGE, CompareLT, CompareLE, CompareEQ} {
			want, got := memory.EnsureBitmap(nil, len(values)), memory.EnsureBitmap(nil, len(values))
			wn := New(ModeScalar).CompareStringLength(offsets, data, validity, want, 4, op, utf8Length)
			gn := New(ModeSIMD).CompareStringLength(offsets, data, validity, got, 4, op, utf8Length)
			if wn != gn || !equalBitmap(want, got) {
				t.Fatalf("utf8=%t op=%d got=(%d,%x) want=(%d,%x)", utf8Length, op, gn, got, wn, want)
			}
		}
	}
}

func assertIntegerArithmetic[T comparable](t *testing.T, rows int, scalar, selected func([]T, memory.Bitmap) int) {
	t.Helper()
	want, got := make([]T, rows), make([]T, rows)
	wantOverflow := memory.EnsureBitmap(nil, rows)
	gotOverflow := memory.EnsureBitmap(nil, rows)
	wantFirst := scalar(want, wantOverflow)
	gotFirst := selected(got, gotOverflow)
	if wantFirst != gotFirst || !equalBitmap(wantOverflow, gotOverflow) {
		t.Fatalf("rows=%d first=%d want=%d overflow=%x want=%x", rows, gotFirst, wantFirst, gotOverflow, wantOverflow)
	}
	for row := range want {
		if got[row] != want[row] {
			t.Fatalf("rows=%d row=%d got=%v want=%v", rows, row, got[row], want[row])
		}
	}
}

func assertCompare(t *testing.T, rows int, scalar, selected func(memory.Bitmap) int) {
	t.Helper()
	want, got := make(memory.Bitmap, memory.WordsFor(rows)), make(memory.Bitmap, memory.WordsFor(rows))
	wn, gn := scalar(want), selected(got)
	if wn != gn || !equalBitmap(want, got) {
		t.Fatalf("got=(%d,%x) want=(%d,%x)", gn, got, wn, want)
	}
}

func equalBitmap(a, b memory.Bitmap) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func BenchmarkInt64Compare(b *testing.B) {
	values := make([]int64, 1<<16)
	for i := range values {
		values[i] = int64(i)
	}
	out := make(memory.Bitmap, memory.WordsFor(len(values)))
	for _, mode := range []struct {
		name string
		mode Mode
	}{{"mode=scalar", ModeScalar}, {"mode=simd", ModeSIMD}} {
		set := New(mode.mode)
		b.Run(mode.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				set.CompareInt64(values, nil, out, 32767, CompareGT)
			}
		})
	}
}

func BenchmarkHash64Batch(b *testing.B) {
	keys := make([]uint64, 1<<16)
	out := make([]uint64, len(keys))
	for i := range keys {
		keys[i] = uint64(i)*0x9e3779b97f4a7c15 + 17
	}
	for _, mode := range []struct {
		name string
		mode Mode
	}{{"mode=scalar", ModeScalar}, {"mode=simd", ModeSIMD}} {
		set := New(mode.mode)
		b.Run(mode.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				set.Hash64Batch(keys, out)
			}
		})
	}
}

func BenchmarkFloat64Arithmetic(b *testing.B) {
	left, right := make([]float64, 1<<16), make([]float64, 1<<16)
	out := make([]float64, len(left))
	for i := range left {
		left[i], right[i] = float64(i), float64(i+1)
	}
	for _, mode := range []struct {
		name string
		mode Mode
	}{{"mode=scalar", ModeScalar}, {"mode=simd", ModeSIMD}} {
		set := New(mode.mode)
		b.Run(mode.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				set.Float64Arithmetic(left, right, out, nil, ArithmeticMul)
			}
		})
	}
}

func BenchmarkInt64Arithmetic(b *testing.B) {
	left, right := make([]int64, 1<<16), make([]int64, 1<<16)
	out := make([]int64, len(left))
	for i := range left {
		left[i], right[i] = int64(i), int64(i+1)
	}
	for _, mode := range []struct {
		name string
		mode Mode
	}{{"mode=scalar", ModeScalar}, {"mode=simd", ModeSIMD}} {
		set := New(mode.mode)
		b.Run(mode.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				set.Int64Arithmetic(left, right, out, nil, nil, ArithmeticAdd)
			}
		})
	}
}

func BenchmarkBoolAnd(b *testing.B) {
	const rows = 1 << 20
	a, av := memory.EnsureBitmap(nil, rows), memory.EnsureBitmap(nil, rows)
	bt, bv := memory.EnsureBitmap(nil, rows), memory.EnsureBitmap(nil, rows)
	out, outv := memory.EnsureBitmap(nil, rows), memory.EnsureBitmap(nil, rows)
	for i := range a {
		a[i], av[i], bt[i], bv[i] = uint64(i)*0x9e3779b97f4a7c15, ^uint64(i*3), uint64(i*7), ^uint64(i*11)
	}
	for _, mode := range []struct {
		name string
		mode Mode
	}{{"mode=scalar", ModeScalar}, {"mode=simd", ModeSIMD}} {
		set := New(mode.mode)
		b.Run(mode.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				set.BoolAnd(a, av, bt, bv, out, outv, rows)
			}
		})
	}
}

func BenchmarkInt64Scale(b *testing.B) {
	base := make([]int64, 1<<16)
	values := make([]int64, len(base))
	for i := range base {
		base[i] = int64(i + 1)
	}
	for _, mode := range []struct {
		name string
		mode Mode
	}{{"mode=scalar", ModeScalar}, {"mode=simd", ModeSIMD}} {
		set := New(mode.mode)
		b.Run(mode.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				copy(values, base)
				set.ScaleInt64(values, nil, 1000)
			}
		})
	}
}

func BenchmarkStringUTF8Length(b *testing.B) {
	const rows = 1 << 14
	offsets := make([]uint32, rows+1)
	data := make([]byte, 0, rows*64)
	for row := 0; row < rows; row++ {
		for i := 0; i < 16; i++ {
			data = append(data, "Go语言"...)
		}
		offsets[row+1] = uint32(len(data))
	}
	out := memory.EnsureBitmap(nil, rows)
	for _, mode := range []struct {
		name string
		mode Mode
	}{{"mode=scalar", ModeScalar}, {"mode=simd", ModeSIMD}} {
		set := New(mode.mode)
		b.Run(mode.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				set.CompareStringLength(offsets, data, nil, out, 63, CompareGT, true)
			}
		})
	}
}
