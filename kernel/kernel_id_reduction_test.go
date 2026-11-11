package kernel

import (
	"fmt"
	"math"
	"slices"
	"testing"

	"gocolumn/memory"
)

// Empty dispatch tables make calls to any wrong function fail. The selected
// function checks the precise inputs and returns distinct sentinels, so these
// tests prove that invoke calls each registered reduction and keeps its results.
func TestKernelBenchReductionDispatch(t *testing.T) {
	f := newKernelBenchFixture(65)
	f.reductionMask = 0x8123456789abcdef
	for _, id := range KernelIDs() {
		if id.kind != KernelReduction {
			continue
		}
		t.Run(id.String(), func(t *testing.T) {
			set := Set{d: &dispatch{}}
			calls := 0
			want := [2]uint64{17, 0}
			switch id.op {
			case 1:
				switch id.typ {
				case memory.TypeInt8:
					set.d.sumInt8Masked = kernelBenchSumSpy[int8, int64](t, f.i8a[:64], f.reductionMask, &calls)
				case memory.TypeInt16:
					set.d.sumInt16Masked = kernelBenchSumSpy[int16, int64](t, f.i16a[:64], f.reductionMask, &calls)
				case memory.TypeUint8:
					set.d.sumUint8Masked = kernelBenchSumSpy[uint8, int64](t, f.u8a[:64], f.reductionMask, &calls)
				case memory.TypeUint16:
					set.d.sumUint16Masked = kernelBenchSumSpy[uint16, int64](t, f.u16a[:64], f.reductionMask, &calls)
				case memory.TypeInt32:
					set.d.sumInt32Masked = kernelBenchSumSpy[int32, int32](t, f.i32a[:64], f.reductionMask, &calls)
				case memory.TypeInt64:
					set.d.sumInt64Masked = kernelBenchSumSpy[int64, int64](t, f.i64a[:64], f.reductionMask, &calls)
				case memory.TypeUint32:
					set.d.sumUint32Masked = kernelBenchSumSpy[uint32, uint32](t, f.u32a[:64], f.reductionMask, &calls)
				case memory.TypeUint64:
					set.d.sumUint64Masked = kernelBenchSumSpy[uint64, uint64](t, f.u64a[:64], f.reductionMask, &calls)
				default:
					t.Fatalf("no dispatch assertion for %s", id)
				}
			case 2:
				want = [2]uint64{17, 29}
				switch id.typ {
				case memory.TypeInt8:
					set.d.minMaxInt8Masked = kernelBenchMinMaxSpy(t, f.i8a[:64], f.reductionMask, &calls)
				case memory.TypeInt16:
					set.d.minMaxInt16Masked = kernelBenchMinMaxSpy(t, f.i16a[:64], f.reductionMask, &calls)
				case memory.TypeUint8:
					set.d.minMaxUint8Masked = kernelBenchMinMaxSpy(t, f.u8a[:64], f.reductionMask, &calls)
				case memory.TypeUint16:
					set.d.minMaxUint16Masked = kernelBenchMinMaxSpy(t, f.u16a[:64], f.reductionMask, &calls)
				case memory.TypeInt32:
					set.d.minMaxInt32Masked = kernelBenchMinMaxSpy(t, f.i32a[:64], f.reductionMask, &calls)
				case memory.TypeInt64:
					set.d.minMaxInt64Masked = kernelBenchMinMaxSpy(t, f.i64a[:64], f.reductionMask, &calls)
				case memory.TypeUint32:
					set.d.minMaxUint32Masked = kernelBenchMinMaxSpy(t, f.u32a[:64], f.reductionMask, &calls)
				case memory.TypeUint64:
					set.d.minMaxUint64Masked = kernelBenchMinMaxSpy(t, f.u64a[:64], f.reductionMask, &calls)
				case memory.TypeFloat32:
					set.d.minMaxFloat32Masked = kernelBenchMinMaxSpy(t, f.f32a[:64], f.reductionMask, &calls)
					want = [2]uint64{uint64(math.Float32bits(17)), uint64(math.Float32bits(29))}
				case memory.TypeFloat64:
					set.d.minMaxFloat64Masked = kernelBenchMinMaxSpy(t, f.f64a[:64], f.reductionMask, &calls)
					want = [2]uint64{math.Float64bits(17), math.Float64bits(29)}
				default:
					t.Fatalf("no dispatch assertion for %s", id)
				}
			default:
				t.Fatalf("no dispatch assertion for %s", id)
			}
			f.outputs[1].clear()
			f.invoke(set, id, 1)
			if calls != 1 || f.outputs[1].reduction != want {
				t.Fatalf("calls=%d, results=%x; want 1, %x", calls, f.outputs[1].reduction, want)
			}
		})
	}
}

func kernelBenchSumSpy[T comparable, R int32 | int64 | uint32 | uint64](t *testing.T, want []T, mask uint64, calls *int) func([]T, uint64) R {
	return func(got []T, gotMask uint64) R {
		t.Helper()
		*calls++
		if !slices.Equal(got, want) || gotMask != mask {
			t.Fatalf("wrong input or mask: len=%d mask=%x", len(got), gotMask)
		}
		return 17
	}
}

func kernelBenchMinMaxSpy[T kernelBenchNumber](t *testing.T, want []T, mask uint64, calls *int) func([]T, uint64) (T, T) {
	return func(got []T, gotMask uint64) (T, T) {
		t.Helper()
		*calls++
		if !slices.Equal(got, want) || gotMask != mask {
			t.Fatalf("wrong input or mask: len=%d mask=%x", len(got), gotMask)
		}
		return 17, 29
	}
}

func TestKernelBenchReductionOracle(t *testing.T) {
	masks := []uint64{0, 1, 1 << 63, 0xaaaaaaaaaaaaaaaa, 0x5555555555555555, 0x8123456789abcdef, ^uint64(0)}
	for _, rows := range []int{0, 1, 7, 31, 32, 63, 64, 65} {
		f := newKernelBenchFixture(rows)
		for row := 0; row < rows; row++ {
			// Include signed/unsigned extremes and overflow, not just the easy positive
			// 64-row sequence timed by the benchmark. Floats stay exactly representable.
			f.i8a[row] = int8(row*97 - 128)
			f.i16a[row] = int16(row*1877 - 32768)
			f.u8a[row] = uint8(row*131 + 255)
			f.u16a[row] = uint16(row*991 + 65535)
			f.i32a[row] = math.MaxInt32 - int32(row*97)
			f.i64a[row] = math.MinInt64 + int64(row*193)
			f.u32a[row] = math.MaxUint32 - uint32(row*277)
			f.u64a[row] = math.MaxUint64 - uint64(row*389)
			f.f32a[row] = float32(row%7 - 3)
			f.f64a[row] = float64(row%11 - 5)
		}
		for _, mask := range masks {
			f.reductionMask = mask
			for _, id := range KernelIDs() {
				if id.kind != KernelReduction {
					continue
				}
				for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
					t.Run(fmt.Sprintf("%s/%s/rows=%d/mask=%x", id, mode, rows, mask), func(t *testing.T) {
						f.outputs[0].clear()
						f.invoke(New(mode), id, 0)
						if err := f.checkReduction(id, 0); err != nil {
							t.Fatal(err)
						}
					})
				}
			}
		}
	}
}

func TestKernelBenchReductionRejectsNoop(t *testing.T) {
	f := newKernelBenchFixture(1 << 16)
	for _, id := range KernelIDs() {
		if id.kind != KernelReduction {
			continue
		}
		t.Run(id.String(), func(t *testing.T) {
			f.outputs[0].clear()
			f.outputs[1].clear()
			// This is the original failure: equal empty snapshots were accepted.
			if err := f.outputs[0].diff(f.outputs[1]); err != nil {
				t.Fatal(err)
			}
			if err := f.checkReduction(id, 0); err == nil {
				t.Fatal("independent oracle accepted the no-op result")
			}
			for slot, mode := range []Mode{ModeScalar, ModeSIMD} {
				f.invoke(New(mode), id, slot)
				if err := f.checkReduction(id, slot); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.outputs[0].diff(f.outputs[1]); err != nil {
				t.Fatal(err)
			}
			for part := 0; part < 2; part++ {
				f.outputs[0].reduction[part] ^= 1
				if f.checkReduction(id, 0) == nil || f.outputs[0].diff(f.outputs[1]) == nil {
					t.Fatalf("lost result bit %d was accepted", part)
				}
				f.outputs[0].reduction[part] ^= 1
			}
		})
	}
}

func TestKernelBenchReductionOracleKnownResults(t *testing.T) {
	if got := kernelBenchNarrowSumOracle([]int8{-128, 127}, 3); got != math.MaxUint64 {
		t.Fatalf("signed widening: %x", got)
	}
	if got := kernelBenchNarrowSumOracle([]uint8{255, 255}, 3); got != 510 {
		t.Fatalf("unsigned widening: %x", got)
	}
	if got := kernelBenchSumOracle([]int32{math.MaxInt32, math.MaxInt32, 3}, 7); got != 1 {
		t.Fatalf("int32 wrapping: %x", got)
	}
	if got := kernelBenchSumOracle([]int64{math.MaxInt64, math.MaxInt64, 3}, 7); got != 1 {
		t.Fatalf("int64 wrapping: %x", got)
	}
	if got := kernelBenchSumOracle([]uint32{math.MaxUint32, 2}, 3); got != 1 {
		t.Fatalf("uint32 wrapping: %x", got)
	}
	if got := kernelBenchSumOracle([]uint64{math.MaxUint64, 2}, 3); got != 1 {
		t.Fatalf("uint64 wrapping: %x", got)
	}
}

func TestKernelBenchReductionFloatBits(t *testing.T) {
	for _, allNaN := range []bool{false, true} {
		f := newKernelBenchFixture(4)
		f.f32a = []float32{math.Float32frombits(0x7fc00011), math.Float32frombits(0xffc00022), 0, math.Float32frombits(1 << 31)}
		f.f64a = []float64{math.Float64frombits(0x7ff8000000000011), math.Float64frombits(0xfff8000000000022), 0, math.Copysign(0, -1)}
		if allNaN {
			f.reductionMask = 3
		}
		for _, typ := range []memory.DataType{memory.TypeFloat32, memory.TypeFloat64} {
			id, _ := MinMaxID(typ)
			var want [2]uint64
			if typ == memory.TypeFloat32 {
				want = [2]uint64{1 << 31, 0}
				if allNaN {
					want = [2]uint64{0xffc00022, 0xffc00022}
				}
			} else {
				want = [2]uint64{1 << 63, 0}
				if allNaN {
					want = [2]uint64{0xfff8000000000022, 0xfff8000000000022}
				}
			}
			if got := f.reductionOracle(id); got != want {
				t.Fatalf("oracle %s: %x != %x", id, got, want)
			}
			for _, mode := range []Mode{ModeScalar, ModeAuto, ModeSIMD} {
				f.invoke(New(mode), id, 0)
				if got := f.outputs[0].reduction; got != want {
					t.Fatalf("%s %s: %x != %x", id, mode, got, want)
				}
			}
		}
	}
}

func TestKernelBenchUnsupportedReductionPanics(t *testing.T) {
	for _, id := range []KernelID{
		{kind: KernelReduction, typ: memory.TypeFloat64, op: 1},
		{kind: KernelReduction, typ: memory.TypeBool, op: 2},
		{kind: KernelReduction, typ: memory.TypeInt64, op: 3},
		{kind: KernelKind(255)},
	} {
		t.Run(fmt.Sprintf("%d/%d/%d", id.kind, id.typ, id.op), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("unsupported reduction/kind silently succeeded")
				}
			}()
			newKernelBenchFixture(64).invoke(New(ModeScalar), id, 0)
		})
	}
}
