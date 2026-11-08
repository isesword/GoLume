package physical

import (
	"context"
	"errors"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"testing"
)

func TestReviewCompactNativeNulls(t *testing.T) {
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
		for _, n := range []int{0, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 129, 257, 1025} {
			for pattern := 0; pattern < 6; pattern++ {
				src := memory.NewBatch()
				src.Rows = n
				valid := make(memory.Bitmap, memory.WordsFor(n))
				sel := make(memory.Bitmap, memory.WordsFor(n))
				pvalid := make(memory.Bitmap, memory.WordsFor(n))
				gvalid := make(memory.Bitmap, memory.WordsFor(n))
				rows := []int{}
				for row := 0; row < n; row++ {
					if row%5 != 0 {
						memory.Set(valid, row)
					}
					if row%7 != 0 {
						memory.Set(pvalid, row)
					}
					if row%11 != 0 {
						memory.Set(gvalid, row)
					}
					if pattern == 0 || pattern >= 3 || row%3 == 1 {
						memory.Set(sel, row)
						rows = append(rows, row)
					}
				}
				if pattern == 0 {
					sel = nil
					valid = nil
				}
				if pattern == 2 {
					clear(valid)
				}
				if n > 0 && sel != nil {
					sel[len(sel)-1] |= ^memory.TailMask(n)
				}
				a := make([]int8, n)
				b := make([]uint8, n)
				c := make([]int16, n)
				d := make([]uint16, n)
				e := make([]float32, n)
				special := []uint32{0, 0x80000000, 0x7f800001, 0xffc00017, 0x7f800000, 0xff800000, 1, 0x80000001, 0x7fffff, 0x00800000, 0x7f7fffff}
				for i := range a {
					a[i] = int8(i * 97)
					b[i] = uint8(i * 113)
					c[i] = int16(i * 997)
					d[i] = uint16(i * 1237)
					e[i] = math.Float32frombits(special[i%len(special)])
				}
				src.Int8s["a"] = &memory.Int8Vector{Values: a, Validity: valid}
				src.Uint8s["b"] = &memory.Uint8Vector{Values: b, Validity: valid}
				src.Int16s["c"] = &memory.Int16Vector{Values: c, Validity: valid}
				src.Uint16s["d"] = &memory.Uint16Vector{Values: d, Validity: valid}
				src.Float32s["e"] = &memory.Float32Vector{Values: e, Validity: valid}
				var parents *compactParent
				if pattern >= 3 {
					parents = &compactParent{validity: pvalid, parent: &compactParent{validity: gvalid}}
				}
				dst := memory.NewBatch()
				for repeat := 0; repeat < 2; repeat++ {
					if err := compactSelectedParents(context.Background(), dst, src, rows, sel, kernel.New(mode), parents); err != nil {
						t.Fatal(err)
					}
					for i, row := range rows {
						wantValid := (valid == nil || memory.IsSet(valid, row)) && (parents == nil || parents.valid(row))
						for _, got := range []bool{dst.Int8s["a"].IsValid(i), dst.Uint8s["b"].IsValid(i), dst.Int16s["c"].IsValid(i), dst.Uint16s["d"].IsValid(i), dst.Float32s["e"].IsValid(i)} {
							if got != wantValid {
								t.Fatalf("mode%s n%d pattern%d row%d validity", mode, n, pattern, row)
							}
						}
						av, bv, cv, dv, ev := a[row], b[row], c[row], d[row], math.Float32bits(e[row])
						if parents != nil && !wantValid {
							av = 0
							bv = 0
							cv = 0
							dv = 0
							ev = 0
						}
						if dst.Int8s["a"].Values[i] != av || dst.Uint8s["b"].Values[i] != bv || dst.Int16s["c"].Values[i] != cv || dst.Uint16s["d"].Values[i] != dv || math.Float32bits(dst.Float32s["e"].Values[i]) != ev {
							t.Fatalf("mode%s n%d pattern%d row%d values", mode, n, pattern, row)
						}
					}
				}
			}
		}
	}
}

func TestNativeCompactWorkspaceBudget(t *testing.T) {
	const n = 257
	ctx := context.Background()
	src := memory.NewBatch()
	src.Reset(n)
	valid := make(memory.Bitmap, memory.WordsFor(n))
	selection := make(memory.Bitmap, memory.WordsFor(n))
	rows := []int{}
	for row := 0; row < n; row++ {
		if row%5 != 0 {
			memory.Set(valid, row)
		}
		if row%3 == 0 {
			memory.Set(selection, row)
			rows = append(rows, row)
		}
	}
	src.Int8s["a"] = &memory.Int8Vector{Values: make([]int8, n), Validity: valid}
	src.Uint8s["b"] = &memory.Uint8Vector{Values: make([]uint8, n), Validity: valid}
	src.Int16s["c"] = &memory.Int16Vector{Values: make([]int16, n), Validity: valid}
	src.Uint16s["d"] = &memory.Uint16Vector{Values: make([]uint16, n), Validity: valid}
	src.Float32s["e"] = &memory.Float32Vector{Values: make([]float32, n), Validity: valid}
	bytes, _, err := compactLayout(src, nil, rows, false, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeAuto, kernel.ModeSIMD} {
		kernels := kernel.New(mode)
		low := compactWorkspace{manager: memory.NewBudgetManager(bytes - 1), bounded: true}
		if _, err := low.compact(ctx, src, rows, selection, kernels); !errors.Is(err, memory.ErrBudgetExceeded) || low.batch != nil || low.manager.Used() != 0 {
			t.Fatal("failed budget admission allocated output")
		}
		low.release()
		manager := memory.NewBudgetManager(bytes)
		w := compactWorkspace{manager: manager, bounded: true}
		out, err := w.compact(ctx, src, rows, selection, kernels)
		if err != nil {
			t.Fatal(err)
		}
		if manager.Used() != bytes || memory.BatchBytes(out) != bytes {
			t.Fatal("output capacity charge mismatch")
		}
		if got := testing.AllocsPerRun(20, func() {
			if _, err := w.compact(ctx, src, rows, selection, kernels); err != nil {
				panic(err)
			}
		}); got != 0 {
			t.Fatalf("warm allocations=%v", got)
		}
		pointer := &out.Float32s["e"].Values[0]
		for row := n / 2; row < n; row++ {
			selection[row/64] &= ^(uint64(1) << uint(row%64))
		}
		smallerRows := rows[:0]
		for row := 0; row < n/2; row += 3 {
			smallerRows = append(smallerRows, row)
		}
		smaller, err := w.compact(ctx, src, smallerRows, selection, kernels)
		if err != nil {
			t.Fatal(err)
		}
		if &smaller.Float32s["e"].Values[0] != pointer || manager.Used() != bytes {
			t.Fatal("shrinking output did not reuse capacity")
		}
		w.release()
		if manager.Used() != 0 {
			t.Fatal("output charge leaked")
		}
		// Restore the input selection for the next dispatch mode.
		rows = rows[:0]
		for row := 0; row < n; row += 3 {
			memory.Set(selection, row)
			rows = append(rows, row)
		}
	}
}
