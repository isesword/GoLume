package expr

import (
	"context"
	"gocolumn/kernel"
	"gocolumn/memory"
	"math"
	"testing"
)

func TestMathReviewLargeMasksAndReuse(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeInt64, Nullable: true}, memory.Field{Name: "b", Type: memory.TypeInt64, Nullable: true})
	p, err := CompileExpressions(schema, []NamedExpr{Named(Col("a").Mod(Col("b")), "m"), Named(Col("a").FloorDiv(Col("b")), "d"), Named(If(GE(Col("b"), I64(0)), Col("a").Pow(Col("b")), I64(7)), "p")})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []kernel.Mode{kernel.ModeScalar, kernel.ModeSIMD, kernel.ModeAuto} {
		mgr := memory.NewBudgetManager(0)
		w, err := NewWorkspace(context.Background(), p, mgr)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range []int{8193, 1, 4096, 4097, 63, 65, 0, 8193} {
			batch := memory.NewBatch()
			batch.Rows = n
			a, b := make([]int64, n), make([]int64, n)
			av, bv := make(memory.Bitmap, memory.WordsFor(n)), make(memory.Bitmap, memory.WordsFor(n))
			for i := range a {
				a[i] = int64(i%11) - 5
				b[i] = int64(i%5) - 2
				if i%7 != 0 {
					memory.Set(av, i)
				}
				if i%13 != 0 {
					memory.Set(bv, i)
				}
			}
			batch.Int64s["a"] = &memory.Int64Vector{Values: a, Validity: av}
			batch.Int64s["b"] = &memory.Int64Vector{Values: b, Validity: bv}
			got, err := w.Evaluate(context.Background(), batch, kernel.New(mode))
			if err != nil {
				t.Fatal(mode, n, err)
			}
			for i := range a {
				valid := i%7 != 0 && i%13 != 0 && b[i] != 0
				for j := 0; j < 2; j++ {
					if got[j].Int64.IsValid(i) != valid {
						t.Fatalf("mode=%v n=%d row=%d output=%d validity", mode, n, i, j)
					}
				}
				if valid {
					q, r := a[i]/b[i], a[i]%b[i]
					if r != 0 && (r < 0) != (b[i] < 0) {
						q--
						r += b[i]
					}
					if got[0].Int64.Values[i] != r || got[1].Int64.Values[i] != q {
						t.Fatal("wrong quotient/remainder", i)
					}
				}
			}
		}
		w.Close()
		if mgr.Used() != 0 {
			t.Fatal("leak", mgr.Used())
		}
	}
}

func TestMathReviewFloat32DenseSIMD(t *testing.T) {
	schema := memory.NewSchema(memory.Field{Name: "a", Type: memory.TypeFloat32})
	p, err := CompileExpressions(schema, []NamedExpr{Named(Col("a").Abs(), "a"), Named(Col("a").Sqrt(), "s")})
	if err != nil {
		t.Fatal(err)
	}
	batch := memory.NewBatch()
	batch.Rows = 8193
	v := make([]float32, batch.Rows)
	for i := range v {
		v[i] = math.Float32frombits(uint32(i) * 2654435761)
	}
	batch.Float32s["a"] = &memory.Float32Vector{Values: v}
	var scalar, vector evaluationState
	defer scalar.Close()
	defer vector.Close()
	want, err := evaluateProjectionTest(p, batch, &scalar, kernel.New(kernel.ModeScalar))
	if err != nil {
		t.Fatal(err)
	}
	got, err := evaluateProjectionTest(p, batch, &vector, kernel.New(kernel.ModeSIMD))
	if err != nil {
		t.Fatal(err)
	}
	for j := range want {
		for i, x := range want[j].Float32.Values {
			y := got[j].Float32.Values[i]
			if math.IsNaN(float64(x)) && math.IsNaN(float64(y)) {
				continue
			}
			if math.Float32bits(x) != math.Float32bits(y) {
				t.Fatalf("output=%d row=%d scalar=%x SIMD=%x", j, i, math.Float32bits(x), math.Float32bits(y))
			}
		}
	}
}
