package expr

import (
	"context"
	"reflect"
	"slices"
	"testing"

	"gocolumn/memory"
)

// Exercise the same scratch owner first as an allocated result, then as a
// borrowed input, and finally as an allocated result again. Input headers must
// never replace retained owned capacities, even after metadata reallocations.
func TestRegisterViewsReviewScratchOwnerIsolation(t *testing.T) {
	expressions := []Expr{I64(19), Col("x"), Add(Col("x"), I64(3)), livenessChain(Col("x"), 24, false), Col("c"), Not(Col("c")), Col("x"), I64(23)}
	programs := make([]Program, len(expressions))
	for i, e := range expressions {
		var err error
		programs[i], err = Compile(livenessSchema(), e)
		if err != nil {
			t.Fatal(err)
		}
	}
	manager := memory.NewBudgetManager(0)
	w := evaluationState{regionScratch: true, ctx: context.Background(), manager: manager}
	defer w.Close()
	for _, rows := range []int{65, 1, 130, 0, 64, 257, 3} {
		for i, p := range programs {
			b := livenessBatch(rows, true)
			original := slices.Clone(b.Int64s["x"].Values)
			validity := slices.Clone(b.Int64s["x"].Validity)
			mask := make(memory.Bitmap, memory.WordsFor(rows))
			for row := range rows {
				if row%3 != 0 {
					memory.Set(mask, row)
				}
			}
			if err := evaluateInstructionsActive(p.instructions, b, &w, testKernelSet(), mask); err != nil {
				t.Fatal(err)
			}
			r := w.regs[p.root]
			for row := range rows {
				valid := row%3 != 0
				if i != 0 && i != 7 {
					if i == 4 || i == 5 {
						valid = valid && b.Uint32s["c"].IsValid(row)
					} else {
						valid = valid && b.Int64s["x"].IsValid(row)
					}
				}
				if isValidRegister(r, row) != valid {
					t.Fatalf("program=%d rows=%d row=%d validity", i, rows, row)
				}
				if !valid {
					continue
				}
				if i == 4 || i == 5 {
					want := b.Uint32s["c"].Values[row] != 0
					if i == 5 {
						want = !want
					}
					if memory.IsSet(r.Truth, row) != want {
						t.Fatal("boolean value")
					}
				} else {
					want := original[row]
					switch i {
					case 0:
						want = 19
					case 2:
						want += 3
					case 3:
						want += 24
					case 7:
						want = 23
					}
					got, err := registerInt64(r, row)
					if err != nil || got != want {
						t.Fatalf("program=%d row=%d got=%d want=%d err=%v", i, row, got, want, err)
					}
				}
			}
			if !slices.Equal(original, b.Int64s["x"].Values) || !slices.Equal(validity, b.Int64s["x"].Validity) {
				t.Fatal("mutated input")
			}
			if manager.Used() != w.MemoryBytes() {
				t.Fatalf("accounting: %d != %d", manager.Used(), w.MemoryBytes())
			}
			for _, r := range w.borrowed[len(w.borrowed):cap(w.borrowed)] {
				if !reflect.ValueOf(r).IsZero() {
					t.Fatal("stale borrowed tail")
				}
			}
		}
	}
	w.Close()
	if manager.Used() != 0 {
		t.Fatal("leaked scratch reservations")
	}
}

func TestRegisterViewsReviewMaskedLiteralExport(t *testing.T) {
	for _, e := range []Expr{I64(-17), F64(0.25), Date32(3), Str("雪"), Str("")} {
		p, err := Compile(memory.NewSchema(), e)
		if err != nil {
			t.Fatal(err)
		}
		var w evaluationState
		for _, rows := range []int{65, 1, 0, 129, 3} {
			b := memory.NewBatch()
			b.Rows = rows
			mask := make(memory.Bitmap, memory.WordsFor(rows))
			for row := range rows {
				if row%3 == 1 {
					memory.Set(mask, row)
				}
			}
			if err = evaluateInstructionsActive(p.instructions, b, &w, testKernelSet(), mask); err != nil {
				t.Fatal(err)
			}
			v, err := w.outputValue(p.root, rows)
			if err != nil {
				t.Fatal(err)
			}
			duplicate, err := w.outputValue(p.root, rows)
			if err != nil {
				t.Fatal(err)
			}
			got := constantReviewValues(v, rows).([]any)
			if !reflect.DeepEqual(got, constantReviewValues(duplicate, rows)) {
				t.Fatal("duplicate export changed value")
			}
			for row := range rows {
				if (got[row] != nil) != (row%3 == 1) {
					t.Fatalf("rows=%d row=%d export lost mask", rows, row)
				}
			}
		}
		w.Close()
	}
}

// Fixed-workspace headers share one allocation but must expose disjoint
// capacities. Cleanup and selected-view invalidation iterate full capacities.
func TestRegisterViewsReviewCoallocatedHeaderBoundaries(t *testing.T) {
	p, err := CompileExpressions(livenessSchema(), []NamedExpr{Named(Col("x"), "x"), Named(Col("c"), "c"), Named(Add(Col("x"), I64(1)), "sum")})
	if err != nil {
		t.Fatal(err)
	}
	manager := memory.NewBudgetManager(0)
	w := evaluationState{ctx: context.Background(), manager: manager}
	defer w.Close()
	for _, rows := range []int{65, 1, 130} {
		if err = evaluateInstructions(p.instructions, livenessBatch(rows, true), &w, testKernelSet()); err != nil {
			t.Fatal(err)
		}
		if cap(w.owned) != storageOwnerCount(p.instructions) || cap(w.borrowed) != borrowedRegisterCount(p.instructions) {
			t.Fatal("header capacity crosses ownership boundary")
		}
		owners := append([]scalarRegister(nil), w.owned...)
		w.clearSelectedViews()
		for i := range owners {
			if w.owned[i].typ != owners[i].typ || len(w.owned[i].i64) != len(owners[i].i64) || len(w.owned[i].Truth) != len(owners[i].Truth) || w.owned[i].acquire == nil {
				t.Fatal("clearing borrowed headers changed owner")
			}
		}
		if manager.Used() != w.MemoryBytes() {
			t.Fatalf("coallocated accounting: %d != %d", manager.Used(), w.MemoryBytes())
		}
	}
	w.Close()
	w.Close()
	if manager.Used() != 0 {
		t.Fatal("coallocated close leaked")
	}
}
